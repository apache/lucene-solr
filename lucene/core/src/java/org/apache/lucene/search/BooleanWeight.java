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
package org.apache.lucene.search;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.Bits;

/**
 * Expert: the Weight for BooleanQuery, used to
 * normalize, score and explain these queries.
 */
final class BooleanWeight extends Weight {
  /** The Similarity implementation. */
  final Similarity similarity;
  final BooleanQuery query;
  
  final ArrayList<Weight> weights;
  final boolean needsScores;

  BooleanWeight(BooleanQuery query, IndexSearcher searcher, boolean needsScores, float boost) throws IOException {
    super(query);
    this.query = query;
    this.needsScores = needsScores;
    this.similarity = searcher.getSimilarity(needsScores);
    weights = new ArrayList<>();
    for (BooleanClause c : query) {
      Weight w = searcher.createWeight(c.getQuery(), needsScores && c.isScoring(), boost);
      weights.add(w);
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
  public Explanation explain(LeafReaderContext context, int doc) throws IOException {
    final int minShouldMatch = query.getMinimumNumberShouldMatch();
    List<Explanation> subs = new ArrayList<>();
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
      return Explanation.match(sum, "sum of:", subs);
    }
  }

  @Override
  public Matches matches(LeafReaderContext context, int doc) throws IOException {
    final int minShouldMatch = query.getMinimumNumberShouldMatch();
    List<Matches> matches = new ArrayList<>();
    int shouldMatchCount = 0;
    Iterator<Weight> wIt = weights.iterator();
    Iterator<BooleanClause> cIt = query.clauses().iterator();
    while (wIt.hasNext()) {
      Weight w = wIt.next();
      BooleanClause bc = cIt.next();
      Matches m = w.matches(context, doc);
      if (bc.isProhibited()) {
        if (m != null) {
          return null;
        }
      }
      if (bc.isRequired()) {
        if (m == null) {
          return null;
        }
        matches.add(m);
      }
      if (bc.getOccur() == Occur.SHOULD) {
        if (m != null) {
          matches.add(m);
          shouldMatchCount++;
        }
      }
    }
    if (shouldMatchCount < minShouldMatch) {
      return null;
    }
    return Matches.fromSubMatches(matches);
  }

  static BulkScorer disableScoring(final BulkScorer scorer) {
    return new BulkScorer() {

      @Override
      public int score(final LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
        final LeafCollector noScoreCollector = new LeafCollector() {
          FakeScorer fake = new FakeScorer();

          @Override
          public void setScorer(Scorer scorer) throws IOException {
            collector.setScorer(fake);
          }

          @Override
          public void collect(int doc) throws IOException {
            fake.doc = doc;
            collector.collect(doc);
          }
        };
        return scorer.score(noScoreCollector, acceptDocs, min, max);
      }

      @Override
      public long cost() {
        return scorer.cost();
      }
    };
  }

  // Return a BulkScorer for the optional clauses only,
  // or null if it is not applicable
  // pkg-private for forcing use of BooleanScorer in tests
  BulkScorer optionalBulkScorer(LeafReaderContext context) throws IOException {
    List<BulkScorer> optional = new ArrayList<BulkScorer>();
    Iterator<BooleanClause> cIter = query.iterator();
    for (Weight w  : weights) {
      BooleanClause c =  cIter.next();
      if (c.getOccur() != Occur.SHOULD) {
        continue;
      }
      BulkScorer subScorer = w.bulkScorer(context);

      if (subScorer != null) {
        optional.add(subScorer);
      }
    }

    if (optional.size() == 0) {
      return null;
    }

    if (query.getMinimumNumberShouldMatch() > optional.size()) {
      return null;
    }

    if (optional.size() == 1) {
      return optional.get(0);
    }

    return new BooleanScorer(this, optional, Math.max(1, query.getMinimumNumberShouldMatch()), needsScores);
  }

  // Return a BulkScorer for the required clauses only,
  // or null if it is not applicable
  private BulkScorer requiredBulkScorer(LeafReaderContext context) throws IOException {
    BulkScorer scorer = null;

    Iterator<BooleanClause> cIter = query.iterator();
    for (Weight w  : weights) {
      BooleanClause c =  cIter.next();
      if (c.isRequired() == false) {
        continue;
      }
      if (scorer != null) {
        // we don't have a BulkScorer for conjunctions
        return null;
      }
      scorer = w.bulkScorer(context);
      if (scorer == null) {
        // no matches
        return null;
      }
      if (c.isScoring() == false && needsScores) {
        scorer = disableScoring(scorer);
      }
    }
    return scorer;
  }

  /** Try to build a boolean scorer for this weight. Returns null if {@link BooleanScorer}
   *  cannot be used. */
  BulkScorer booleanScorer(LeafReaderContext context) throws IOException {
    final int numOptionalClauses = query.getClauses(Occur.SHOULD).size();
    final int numRequiredClauses = query.getClauses(Occur.MUST).size() + query.getClauses(Occur.FILTER).size();
    
    BulkScorer positiveScorer;
    if (numRequiredClauses == 0) {
      positiveScorer = optionalBulkScorer(context);
      if (positiveScorer == null) {
        return null;
      }

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

      if (positiveScorer.cost() < costThreshold) {
        return null;
      }

    } else if (numRequiredClauses == 1
        && numOptionalClauses == 0
        && query.getMinimumNumberShouldMatch() == 0) {
      positiveScorer = requiredBulkScorer(context);
    } else {
      // TODO: there are some cases where BooleanScorer
      // would handle conjunctions faster than
      // BooleanScorer2...
      return null;
    }

    if (positiveScorer == null) {
      return null;
    }

    List<Scorer> prohibited = new ArrayList<>();
    Iterator<BooleanClause> cIter = query.iterator();
    for (Weight w  : weights) {
      BooleanClause c =  cIter.next();
      if (c.isProhibited()) {
        Scorer scorer = w.scorer(context);
        if (scorer != null) {
          prohibited.add(scorer);
        }
      }
    }

    if (prohibited.isEmpty()) {
      return positiveScorer;
    } else {
      Scorer prohibitedScorer = prohibited.size() == 1
          ? prohibited.get(0)
          : new DisjunctionSumScorer(this, prohibited, false);
      if (prohibitedScorer.twoPhaseIterator() != null) {
        // ReqExclBulkScorer can't deal efficiently with two-phased prohibited clauses
        return null;
      }
      return new ReqExclBulkScorer(positiveScorer, prohibitedScorer.iterator());
    }
  }

  @Override
  public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {
    final BulkScorer bulkScorer = booleanScorer(context);
    if (bulkScorer != null) {
      // bulk scoring is applicable, use it
      return bulkScorer;
    } else {
      // use a Scorer-based impl (BS2)
      return super.bulkScorer(context);
    }
  }

  @Override
  public Scorer scorer(LeafReaderContext context) throws IOException {
    ScorerSupplier scorerSupplier = scorerSupplier(context);
    if (scorerSupplier == null) {
      return null;
    }
    return scorerSupplier.get(Long.MAX_VALUE);
  }

  @Override
  public boolean isCacheable(LeafReaderContext ctx) {
    if (weights.size() > TermInSetQuery.BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD) {
      // Disallow caching large boolean queries to not encourage users
      // to build large boolean queries as a workaround to the fact that
      // we disallow caching large TermInSetQueries.
      return false;
    }
    for (Weight w : weights) {
      if (w.isCacheable(ctx) == false)
        return false;
    }
    return true;
  }

  @Override
  public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
    int minShouldMatch = query.getMinimumNumberShouldMatch();

    final Map<Occur, Collection<ScorerSupplier>> scorers = new EnumMap<>(Occur.class);
    for (Occur occur : Occur.values()) {
      scorers.put(occur, new ArrayList<>());
    }

    Iterator<BooleanClause> cIter = query.iterator();
    for (Weight w  : weights) {
      BooleanClause c =  cIter.next();
      ScorerSupplier subScorer = w.scorerSupplier(context);
      if (subScorer == null) {
        if (c.isRequired()) {
          return null;
        }
      } else {
        scorers.get(c.getOccur()).add(subScorer);
      }
    }

    // scorer simplifications:
    
    if (scorers.get(Occur.SHOULD).size() == minShouldMatch) {
      // any optional clauses are in fact required
      scorers.get(Occur.MUST).addAll(scorers.get(Occur.SHOULD));
      scorers.get(Occur.SHOULD).clear();
      minShouldMatch = 0;
    }
    
    if (scorers.get(Occur.FILTER).isEmpty() && scorers.get(Occur.MUST).isEmpty() && scorers.get(Occur.SHOULD).isEmpty()) {
      // no required and optional clauses.
      return null;
    } else if (scorers.get(Occur.SHOULD).size() < minShouldMatch) {
      // either >1 req scorer, or there are 0 req scorers and at least 1
      // optional scorer. Therefore if there are not enough optional scorers
      // no documents will be matched by the query
      return null;
    }

    // we don't need scores, so if we have required clauses, drop optional clauses completely
    if (!needsScores && minShouldMatch == 0 && scorers.get(Occur.MUST).size() + scorers.get(Occur.FILTER).size() > 0) {
      scorers.get(Occur.SHOULD).clear();
    }

    return new Boolean2ScorerSupplier(this, scorers, needsScores, minShouldMatch);
  }

}
