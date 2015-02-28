package org.apache.lucene.search;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.Bits;

/**
 * Expert: the Weight for BooleanQuery, used to
 * normalize, score and explain these queries.
 *
 * @lucene.experimental
 */
public class BooleanWeight extends Weight {
  /** The Similarity implementation. */
  protected Similarity similarity;
  protected final BooleanQuery query;
  protected ArrayList<Weight> weights;
  protected int maxCoord;  // num optional + num required
  private final boolean disableCoord;
  private final boolean needsScores;
  private final float coords[];

  public BooleanWeight(BooleanQuery query, IndexSearcher searcher, boolean needsScores, boolean disableCoord) throws IOException {
    super(query);
    this.query = query;
    this.needsScores = needsScores;
    this.similarity = searcher.getSimilarity();
    weights = new ArrayList<>(query.clauses().size());
    for (int i = 0 ; i < query.clauses().size(); i++) {
      BooleanClause c = query.clauses().get(i);
      Weight w = searcher.createWeight(c.getQuery(), needsScores && c.isScoring());
      weights.add(w);
      if (c.isScoring()) {
        maxCoord++;
      }
    }
    
    // precompute coords (0..N, N).
    // set disableCoord when its explicit, scores are not needed, no scoring clauses, or the sim doesn't use it.
    coords = new float[maxCoord+1];
    Arrays.fill(coords, 1F);
    coords[0] = 0f;
    if (maxCoord > 0 && needsScores && disableCoord == false) {
      // compute coords from the similarity, look for any actual ones.
      boolean seenActualCoord = false;
      for (int i = 1; i < coords.length; i++) {
        coords[i] = coord(i, maxCoord);
        seenActualCoord |= (coords[i] != 1F);
      }
      this.disableCoord = seenActualCoord == false;
    } else {
      this.disableCoord = true;
    }
  }

  @Override
  public float getValueForNormalization() throws IOException {
    float sum = 0.0f;
    for (int i = 0 ; i < weights.size(); i++) {
      // call sumOfSquaredWeights for all clauses in case of side effects
      float s = weights.get(i).getValueForNormalization();         // sum sub weights
      if (query.clauses().get(i).isScoring()) {
        // only add to sum for scoring clauses
        sum += s;
      }
    }

    sum *= query.getBoost() * query.getBoost();             // boost each sub-weight

    return sum ;
  }

  public float coord(int overlap, int maxOverlap) {
    if (overlap == 0) {
      // special case that there are only non-scoring clauses
      return 0F;
    } else if (maxOverlap == 1) {
      // LUCENE-4300: in most cases of maxOverlap=1, BQ rewrites itself away,
      // so coord() is not applied. But when BQ cannot optimize itself away
      // for a single clause (minNrShouldMatch, prohibited clauses, etc), it's
      // important not to apply coord(1,1) for consistency, it might not be 1.0F
      return 1F;
    } else {
      // common case: use the similarity to compute the coord
      return similarity.coord(overlap, maxOverlap);
    }
  }

  @Override
  public void normalize(float norm, float topLevelBoost) {
    topLevelBoost *= query.getBoost();                  // incorporate boost
    for (Weight w : weights) {
      // normalize all clauses, (even if non-scoring in case of side affects)
      w.normalize(norm, topLevelBoost);
    }
  }

  @Override
  public Explanation explain(LeafReaderContext context, int doc) throws IOException {
    final int minShouldMatch = query.getMinimumNumberShouldMatch();
    ComplexExplanation sumExpl = new ComplexExplanation();
    sumExpl.setDescription("sum of:");
    int coord = 0;
    float sum = 0.0f;
    boolean fail = false;
    int matchCount = 0;
    int shouldMatchCount = 0;
    Iterator<BooleanClause> cIter = query.clauses().iterator();
    for (Iterator<Weight> wIter = weights.iterator(); wIter.hasNext();) {
      Weight w = wIter.next();
      BooleanClause c = cIter.next();
      if (w.scorer(context, context.reader().getLiveDocs()) == null) {
        if (c.isRequired()) {
          fail = true;
          Explanation r = new Explanation(0.0f, "no match on required clause (" + c.getQuery().toString() + ")");
          sumExpl.addDetail(r);
        }
        continue;
      }
      Explanation e = w.explain(context, doc);
      if (e.isMatch()) {
        if (c.isScoring()) {
          sumExpl.addDetail(e);
          sum += e.getValue();
          coord++;
        } else if (c.isRequired()) {
          Explanation r = new Explanation(0f, "match on required clause, product of:");
          r.addDetail(new Explanation(0f, Occur.FILTER + " clause"));
          r.addDetail(e);
          sumExpl.addDetail(r);
        } else if (c.isProhibited()) {
          Explanation r =
            new Explanation(0.0f, "match on prohibited clause (" + c.getQuery().toString() + ")");
          r.addDetail(e);
          sumExpl.addDetail(r);
          fail = true;
        }
        if (!c.isProhibited()) {
          matchCount++;
        }
        if (c.getOccur() == Occur.SHOULD) {
          shouldMatchCount++;
        }
      } else if (c.isRequired()) {
        Explanation r = new Explanation(0.0f, "no match on required clause (" + c.getQuery().toString() + ")");
        r.addDetail(e);
        sumExpl.addDetail(r);
        fail = true;
      }
    }
    if (fail) {
      sumExpl.setMatch(Boolean.FALSE);
      sumExpl.setValue(0.0f);
      sumExpl.setDescription
        ("Failure to meet condition(s) of required/prohibited clause(s)");
      return sumExpl;
    } else if (shouldMatchCount < minShouldMatch) {
      sumExpl.setMatch(Boolean.FALSE);
      sumExpl.setValue(0.0f);
      sumExpl.setDescription("Failure to match minimum number "+
                             "of optional clauses: " + minShouldMatch);
      return sumExpl;
    }
    
    sumExpl.setMatch(0 < matchCount);
    sumExpl.setValue(sum);
    
    final float coordFactor = disableCoord ? 1.0f : coord(coord, maxCoord);
    if (coordFactor == 1.0f) {
      return sumExpl;                             // eliminate wrapper
    } else {
      ComplexExplanation result = new ComplexExplanation(sumExpl.isMatch(),
                                                         sum*coordFactor,
                                                         "product of:");
      result.addDetail(sumExpl);
      result.addDetail(new Explanation(coordFactor,
                                       "coord("+coord+"/"+maxCoord+")"));
      return result;
    }
  }

  /** Try to build a boolean scorer for this weight. Returns null if {@link BooleanScorer}
   *  cannot be used. */
  // pkg-private for forcing use of BooleanScorer in tests
  BooleanScorer booleanScorer(LeafReaderContext context, Bits acceptDocs) throws IOException {
    List<BulkScorer> optional = new ArrayList<BulkScorer>();
    Iterator<BooleanClause> cIter = query.clauses().iterator();
    for (Weight w  : weights) {
      BooleanClause c =  cIter.next();
      BulkScorer subScorer = w.bulkScorer(context, acceptDocs);
      if (subScorer == null) {
        if (c.isRequired()) {
          return null;
        }
      } else if (c.isRequired()) {
        // TODO: there are some cases where BooleanScorer
        // would handle conjunctions faster than
        // BooleanScorer2...
        return null;
      } else if (c.isProhibited()) {
        // TODO: there are some cases where BooleanScorer could do this faster
        return null;
      } else {
        optional.add(subScorer);
      }
    }

    if (optional.size() == 0) {
      return null;
    }

    if (query.minNrShouldMatch > optional.size()) {
      return null;
    }

    return new BooleanScorer(this, disableCoord, maxCoord, optional, Math.max(1, query.minNrShouldMatch));
  }

  @Override
  public BulkScorer bulkScorer(LeafReaderContext context, Bits acceptDocs) throws IOException {
    final BooleanScorer bulkScorer = booleanScorer(context, acceptDocs);
    if (bulkScorer != null) { // BooleanScorer is applicable
      // TODO: what is the right heuristic here?
      final long costThreshold;
      if (query.minNrShouldMatch <= 1) {
        // when all clauses are optional, use BooleanScorer aggressively
        // TODO: is there actually a threshold under which we should rather
        // use the regular scorer?
        costThreshold = -1;
      } else {
        // when a minimum number of clauses should match, BooleanScorer is
        // going to score all windows that have at least minNrShouldMatch
        // matches in the window. But there is no way to know if there is
        // an intersection (all clauses might match a different doc ID and
        // there will be no matches in the end) so we should only use
        // BooleanScorer if matches are very dense
        costThreshold = context.reader().maxDoc() / 3;
      }

      if (bulkScorer.cost() > costThreshold) {
        return bulkScorer;
      }
    }
    return super.bulkScorer(context, acceptDocs);
  }

  @Override
  public Scorer scorer(LeafReaderContext context, Bits acceptDocs) throws IOException {
    // initially the user provided value,
    // but if minNrShouldMatch == optional.size(),
    // we will optimize and move these to required, making this 0
    int minShouldMatch = query.minNrShouldMatch;

    List<Scorer> required = new ArrayList<>();
    // clauses that are required AND participate in scoring, subset of 'required'
    List<Scorer> requiredScoring = new ArrayList<>();
    List<Scorer> prohibited = new ArrayList<>();
    List<Scorer> optional = new ArrayList<>();
    Iterator<BooleanClause> cIter = query.clauses().iterator();
    for (Weight w  : weights) {
      BooleanClause c =  cIter.next();
      Scorer subScorer = w.scorer(context, acceptDocs);
      if (subScorer == null) {
        if (c.isRequired()) {
          return null;
        }
      } else if (c.isRequired()) {
        required.add(subScorer);
        if (c.isScoring()) {
          requiredScoring.add(subScorer);
        }
      } else if (c.isProhibited()) {
        prohibited.add(subScorer);
      } else {
        optional.add(subScorer);
      }
    }
    
    // scorer simplifications:
    
    if (optional.size() == minShouldMatch) {
      // any optional clauses are in fact required
      required.addAll(optional);
      requiredScoring.addAll(optional);
      optional.clear();
      minShouldMatch = 0;
    }
    
    if (required.isEmpty() && optional.isEmpty()) {
      // no required and optional clauses.
      return null;
    } else if (optional.size() < minShouldMatch) {
      // either >1 req scorer, or there are 0 req scorers and at least 1
      // optional scorer. Therefore if there are not enough optional scorers
      // no documents will be matched by the query
      return null;
    }

    // we don't need scores, so if we have required clauses, drop optional clauses completely
    if (!needsScores && minShouldMatch == 0 && required.size() > 0) {
      optional.clear();
    }
    
    // three cases: conjunction, disjunction, or mix
    
    // pure conjunction
    if (optional.isEmpty()) {
      return excl(req(required, requiredScoring, disableCoord), prohibited);
    }
    
    // pure disjunction
    if (required.isEmpty()) {
      return excl(opt(optional, minShouldMatch, disableCoord), prohibited);
    }
    
    // conjunction-disjunction mix:
    // we create the required and optional pieces with coord disabled, and then
    // combine the two: if minNrShouldMatch > 0, then it's a conjunction: because the
    // optional side must match. otherwise it's required + optional, factoring the
    // number of optional terms into the coord calculation
    
    Scorer req = excl(req(required, requiredScoring, true), prohibited);
    Scorer opt = opt(optional, minShouldMatch, true);

    // TODO: clean this up: it's horrible
    if (disableCoord) {
      if (minShouldMatch > 0) {
        return new ConjunctionScorer(this, Arrays.asList(req, opt), Arrays.asList(req, opt), 1F);
      } else {
        return new ReqOptSumScorer(req, opt);          
      }
    } else if (optional.size() == 1) {
      if (minShouldMatch > 0) {
        return new ConjunctionScorer(this, Arrays.asList(req, opt), Arrays.asList(req, opt), coord(requiredScoring.size()+1, maxCoord));
      } else {
        float coordReq = coord(requiredScoring.size(), maxCoord);
        float coordBoth = coord(requiredScoring.size() + 1, maxCoord);
        return new BooleanTopLevelScorers.ReqSingleOptScorer(req, opt, coordReq, coordBoth);
      }
    } else {
      if (minShouldMatch > 0) {
        return new BooleanTopLevelScorers.CoordinatingConjunctionScorer(this, coords, req, requiredScoring.size(), opt);
      } else {
        return new BooleanTopLevelScorers.ReqMultiOptScorer(req, opt, requiredScoring.size(), coords); 
      }
    }
  }

  /** Create a new scorer for the given required clauses. Note that
   *  {@code requiredScoring} is a subset of {@code required} containing
   *  required clauses that should participate in scoring. */
  private Scorer req(List<Scorer> required, List<Scorer> requiredScoring, boolean disableCoord) {
    if (required.size() == 1) {
      Scorer req = required.get(0);

      if (needsScores == false ||
          (requiredScoring.size() == 1 && (disableCoord || maxCoord == 1))) {
        return req;
      } else {
        return new BooleanTopLevelScorers.BoostedScorer(req, coord(requiredScoring.size(), maxCoord));
      }
    } else {
      return new ConjunctionScorer(this, required, requiredScoring,
                                   disableCoord ? 1.0F : coord(requiredScoring.size(), maxCoord));
    }
  }
  
  private Scorer excl(Scorer main, List<Scorer> prohibited) throws IOException {
    if (prohibited.isEmpty()) {
      return main;
    } else if (prohibited.size() == 1) {
      return new ReqExclScorer(main, prohibited.get(0));
    } else {
      float coords[] = new float[prohibited.size()+1];
      Arrays.fill(coords, 1F);
      return new ReqExclScorer(main, new DisjunctionSumScorer(this, prohibited, coords, false));
    }
  }
  
  private Scorer opt(List<Scorer> optional, int minShouldMatch, boolean disableCoord) throws IOException {
    if (optional.size() == 1) {
      Scorer opt = optional.get(0);
      if (!disableCoord && maxCoord > 1) {
        return new BooleanTopLevelScorers.BoostedScorer(opt, coord(1, maxCoord));
      } else {
        return opt;
      }
    } else {
      float coords[];
      if (disableCoord) {
        // sneaky: when we do a mixed conjunction/disjunction, we need a fake for the disjunction part.
        coords = new float[optional.size()+1];
        Arrays.fill(coords, 1F);
      } else {
        coords = this.coords;
      }
      if (minShouldMatch > 1) {
        return new MinShouldMatchSumScorer(this, optional, minShouldMatch, coords);
      } else {
        return new DisjunctionSumScorer(this, optional, coords, needsScores);
      }
    }
  }
}
