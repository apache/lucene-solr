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
import java.util.Set;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.similarities.Similarity;

/**
 * Expert: the Weight for BooleanQuery, used to
 * normalize, score and explain these queries.
 */
final class BooleanWeight extends Weight {
  /** The Similarity implementation. */
  final Similarity similarity;
  final BooleanQuery query;
  
  final ArrayList<Weight> weights;
  final int maxCoord;  // num optional + num required
  final boolean disableCoord;
  final boolean needsScores;
  final float coords[];

  BooleanWeight(BooleanQuery query, IndexSearcher searcher, boolean needsScores, boolean disableCoord) throws IOException {
    super(query);
    this.query = query;
    this.needsScores = needsScores;
    this.similarity = searcher.getSimilarity(needsScores);
    weights = new ArrayList<>();
    int i = 0;
    int maxCoord = 0;
    for (BooleanClause c : query) {
      Weight w = searcher.createWeight(c.getQuery(), needsScores && c.isScoring());
      weights.add(w);
      if (c.isScoring()) {
        maxCoord++;
      }
      i += 1;
    }
    this.maxCoord = maxCoord;
    
    // precompute coords (0..N, N).
    // set disableCoord when its explicit, scores are not needed, no scoring clauses, or the sim doesn't use it.
    coords = new float[maxCoord+1];
    Arrays.fill(coords, 1F);
    coords[0] = 0f;
    if (maxCoord > 0 && needsScores && disableCoord == false) {
      // compute coords from the similarity, look for any actual ones.
      boolean seenActualCoord = false;
      for (i = 1; i < coords.length; i++) {
        coords[i] = coord(i, maxCoord);
        seenActualCoord |= (coords[i] != 1F);
      }
      this.disableCoord = seenActualCoord == false;
    } else {
      this.disableCoord = true;
    }
  }

  @Override
  public void extractTerms(Set<Term> terms) {
    int i = 0;
    for (BooleanClause clause : query) {
      if (clause.isScoring() || (needsScores == false && clause.isProhibited() == false)) {
        weights.get(i).extractTerms(terms);
      }
      i++;
    }
  }

  @Override
  public float getValueForNormalization() throws IOException {
    float sum = 0.0f;
    int i = 0;
    for (BooleanClause clause : query) {
      // call sumOfSquaredWeights for all clauses in case of side effects
      float s = weights.get(i).getValueForNormalization();         // sum sub weights
      if (clause.isScoring()) {
        // only add to sum for scoring clauses
        sum += s;
      }
      i += 1;
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
    List<Explanation> subs = new ArrayList<>();
    int coord = 0;
    float sum = 0.0f;
    boolean fail = false;
    int matchCount = 0;
    int shouldMatchCount = 0;
    Iterator<BooleanClause> cIter = query.iterator();
    for (Iterator<Weight> wIter = weights.iterator(); wIter.hasNext();) {
      Weight w = wIter.next();
      BooleanClause c = cIter.next();
      Explanation e = w.explain(context, doc);
      if (e.isMatch()) {
        if (c.isScoring()) {
          subs.add(e);
          sum += e.getValue();
          coord++;
        } else if (c.isRequired()) {
          subs.add(Explanation.match(0f, "match on required clause, product of:",
              Explanation.match(0f, Occur.FILTER + " clause"), e));
        } else if (c.isProhibited()) {
          subs.add(Explanation.noMatch("match on prohibited clause (" + c.getQuery().toString() + ")", e));
          fail = true;
        }
        if (!c.isProhibited()) {
          matchCount++;
        }
        if (c.getOccur() == Occur.SHOULD) {
          shouldMatchCount++;
        }
      } else if (c.isRequired()) {
        subs.add(Explanation.noMatch("no match on required clause (" + c.getQuery().toString() + ")", e));
        fail = true;
      }
    }
    if (fail) {
      return Explanation.noMatch("Failure to meet condition(s) of required/prohibited clause(s)", subs);
    } else if (matchCount == 0) {
      return Explanation.noMatch("No matching clauses", subs);
    } else if (shouldMatchCount < minShouldMatch) {
      return Explanation.noMatch("Failure to match minimum number of optional clauses: " + minShouldMatch, subs);
    } else {
      // we have a match
      Explanation result = Explanation.match(sum, "sum of:", subs);
      final float coordFactor = disableCoord ? 1.0f : coord(coord, maxCoord);
      if (coordFactor != 1f) {
        result = Explanation.match(sum * coordFactor, "product of:",
            result, Explanation.match(coordFactor, "coord("+coord+"/"+maxCoord+")"));
      }
      return result;
    }
  }

  /** Try to build a boolean scorer for this weight. Returns null if {@link BooleanScorer}
   *  cannot be used. */
  // pkg-private for forcing use of BooleanScorer in tests
  BooleanScorer booleanScorer(LeafReaderContext context) throws IOException {
    List<BulkScorer> optional = new ArrayList<BulkScorer>();
    Iterator<BooleanClause> cIter = query.iterator();
    for (Weight w  : weights) {
      BooleanClause c =  cIter.next();
      BulkScorer subScorer = w.bulkScorer(context);
      
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

    if (query.getMinimumNumberShouldMatch() > optional.size()) {
      return null;
    }

    return new BooleanScorer(this, disableCoord, maxCoord, optional, Math.max(1, query.getMinimumNumberShouldMatch()), needsScores);
  }

  @Override
  public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {
    final BooleanScorer bulkScorer = booleanScorer(context);
    if (bulkScorer != null) { // BooleanScorer is applicable
      // TODO: what is the right heuristic here?
      final long costThreshold;
      if (query.getMinimumNumberShouldMatch() <= 1) {
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
    return super.bulkScorer(context);
  }

  @Override
  public Scorer scorer(LeafReaderContext context) throws IOException {
    // initially the user provided value,
    // but if minNrShouldMatch == optional.size(),
    // we will optimize and move these to required, making this 0
    int minShouldMatch = query.getMinimumNumberShouldMatch();

    List<Scorer> required = new ArrayList<>();
    // clauses that are required AND participate in scoring, subset of 'required'
    List<Scorer> requiredScoring = new ArrayList<>();
    List<Scorer> prohibited = new ArrayList<>();
    List<Scorer> optional = new ArrayList<>();
    Iterator<BooleanClause> cIter = query.iterator();
    for (Weight w  : weights) {
      BooleanClause c =  cIter.next();
      Scorer subScorer = w.scorer(context);
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

      if (needsScores == false) {
        return req;
      }

      if (requiredScoring.isEmpty()) {
        // Scores are needed but we only have a filter clause
        // BooleanWeight expects that calling score() is ok so we need to wrap
        // to prevent score() from being propagated
        return new FilterScorer(req) {
          @Override
          public float score() throws IOException {
            return 0f;
          }
          @Override
          public int freq() throws IOException {
            return 0;
          }
        };
      }
      
      float boost = 1f;
      if (disableCoord == false) {
        boost = coord(1, maxCoord);
      }
      if (boost == 1f) {
        return req;
      }
      return new BooleanTopLevelScorers.BoostedScorer(req, boost);
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
