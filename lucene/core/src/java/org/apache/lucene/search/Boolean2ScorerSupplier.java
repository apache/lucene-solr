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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.stream.Stream;

import org.apache.lucene.search.BooleanClause.Occur;

final class Boolean2ScorerSupplier extends ScorerSupplier {

  private final Weight weight;
  private final Map<BooleanClause.Occur, Collection<ScorerSupplier>> subs;
  private final ScoreMode scoreMode;
  private final int minShouldMatch;
  private long cost = -1;

  Boolean2ScorerSupplier(Weight weight,
      Map<Occur, Collection<ScorerSupplier>> subs,
      ScoreMode scoreMode, int minShouldMatch) {
    if (minShouldMatch < 0) {
      throw new IllegalArgumentException("minShouldMatch must be positive, but got: " + minShouldMatch);
    }
    if (minShouldMatch != 0 && minShouldMatch >= subs.get(Occur.SHOULD).size()) {
      throw new IllegalArgumentException("minShouldMatch must be strictly less than the number of SHOULD clauses");
    }
    if (scoreMode.needsScores() == false && minShouldMatch == 0 && subs.get(Occur.SHOULD).size() > 0
        && subs.get(Occur.MUST).size() + subs.get(Occur.FILTER).size() > 0) {
      throw new IllegalArgumentException("Cannot pass purely optional clauses if scores are not needed");
    }
    if (subs.get(Occur.SHOULD).size() + subs.get(Occur.MUST).size() + subs.get(Occur.FILTER).size() == 0) {
      throw new IllegalArgumentException("There should be at least one positive clause");
    }
    this.weight = weight;
    this.subs = subs;
    this.scoreMode = scoreMode;
    this.minShouldMatch = minShouldMatch;
  }

  private long computeCost() {
    OptionalLong minRequiredCost = Stream.concat(
        subs.get(Occur.MUST).stream(),
        subs.get(Occur.FILTER).stream())
        .mapToLong(ScorerSupplier::cost)
        .min();
    if (minRequiredCost.isPresent() && minShouldMatch == 0) {
      return minRequiredCost.getAsLong();
    } else {
      final Collection<ScorerSupplier> optionalScorers = subs.get(Occur.SHOULD);
      final long shouldCost =
          ScorerUtil.costWithMinShouldMatch(
              optionalScorers.stream().mapToLong(ScorerSupplier::cost),
              optionalScorers.size(),
              minShouldMatch);
      return Math.min(minRequiredCost.orElse(Long.MAX_VALUE), shouldCost);
    }
  }

  @Override
  public long cost() {
    if (cost == -1) {
      cost = computeCost();
    }
    return cost;
  }

  @Override
  public Scorer get(long leadCost) throws IOException {
    Scorer scorer = getInternal(leadCost);
    if (scoreMode == ScoreMode.TOP_SCORES &&
          subs.get(Occur.SHOULD).isEmpty() && subs.get(Occur.MUST).isEmpty()) {
      // no scoring clauses but scores are needed so we wrap the scorer in
      // a constant score in order to allow early termination
      return scorer.twoPhaseIterator() != null ?
          new ConstantScoreScorer(weight, 0f, scoreMode, scorer.twoPhaseIterator()) :
            new ConstantScoreScorer(weight, 0f, scoreMode, scorer.iterator());
    }
    return scorer;
  }

  private Scorer getInternal(long leadCost) throws IOException {
    // three cases: conjunction, disjunction, or mix
    leadCost = Math.min(leadCost, cost());

    // pure conjunction
    if (subs.get(Occur.SHOULD).isEmpty()) {
      return excl(req(subs.get(Occur.FILTER), subs.get(Occur.MUST), leadCost), subs.get(Occur.MUST_NOT), leadCost);
    }

    // pure disjunction
    if (subs.get(Occur.FILTER).isEmpty() && subs.get(Occur.MUST).isEmpty()) {
      return excl(opt(subs.get(Occur.SHOULD), minShouldMatch, scoreMode, leadCost), subs.get(Occur.MUST_NOT), leadCost);
    }

    // conjunction-disjunction mix:
    // we create the required and optional pieces, and then
    // combine the two: if minNrShouldMatch > 0, then it's a conjunction: because the
    // optional side must match. otherwise it's required + optional

    if (minShouldMatch > 0) {
      Scorer req = excl(req(subs.get(Occur.FILTER), subs.get(Occur.MUST), leadCost), subs.get(Occur.MUST_NOT), leadCost);
      Scorer opt = opt(subs.get(Occur.SHOULD), minShouldMatch, scoreMode, leadCost);
      return new ConjunctionScorer(weight, Arrays.asList(req, opt), Arrays.asList(req, opt));
    } else {
      assert scoreMode.needsScores();
      return new ReqOptSumScorer(
          excl(req(subs.get(Occur.FILTER), subs.get(Occur.MUST), leadCost), subs.get(Occur.MUST_NOT), leadCost),
          opt(subs.get(Occur.SHOULD), minShouldMatch, scoreMode, leadCost), scoreMode);
    }
  }

  /** Create a new scorer for the given required clauses. Note that
   *  {@code requiredScoring} is a subset of {@code required} containing
   *  required clauses that should participate in scoring. */
  private Scorer req(Collection<ScorerSupplier> requiredNoScoring, Collection<ScorerSupplier> requiredScoring, long leadCost) throws IOException {
    if (requiredNoScoring.size() + requiredScoring.size() == 1) {
      Scorer req = (requiredNoScoring.isEmpty() ? requiredScoring : requiredNoScoring).iterator().next().get(leadCost);

      if (scoreMode.needsScores() == false) {
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
          public float getMaxScore(int upTo) throws IOException {
            return 0f;
          }
        };
      }

      return req;
    } else {
      List<Scorer> requiredScorers = new ArrayList<>();
      List<Scorer> scoringScorers = new ArrayList<>();
      for (ScorerSupplier s : requiredNoScoring) {
        requiredScorers.add(s.get(leadCost));
      }
      for (ScorerSupplier s : requiredScoring) {
        Scorer scorer = s.get(leadCost);
        scoringScorers.add(scorer);
      }
      if (scoreMode == ScoreMode.TOP_SCORES && scoringScorers.size() > 1) {
        Scorer blockMaxScorer = new BlockMaxConjunctionScorer(weight, scoringScorers);
        if (requiredScorers.isEmpty()) {
          return blockMaxScorer;
        }
        scoringScorers = Collections.singletonList(blockMaxScorer);
      }
      requiredScorers.addAll(scoringScorers);
      return new ConjunctionScorer(weight, requiredScorers, scoringScorers);
    }
  }

  private Scorer excl(Scorer main, Collection<ScorerSupplier> prohibited, long leadCost) throws IOException {
    if (prohibited.isEmpty()) {
      return main;
    } else {
      return new ReqExclScorer(main, opt(prohibited, 1, ScoreMode.COMPLETE_NO_SCORES, leadCost));
    }
  }

  private Scorer opt(Collection<ScorerSupplier> optional, int minShouldMatch,
      ScoreMode scoreMode, long leadCost) throws IOException {
    if (optional.size() == 1) {
      return optional.iterator().next().get(leadCost);
    } else {
      final List<Scorer> optionalScorers = new ArrayList<>();
      for (ScorerSupplier scorer : optional) {
        optionalScorers.add(scorer.get(leadCost));
      }

      // Technically speaking, WANDScorer should be able to handle the following 3 conditions now
      // 1. Any ScoreMode (with scoring or not)
      // 2. Any minCompetitiveScore ( >= 0 )
      // 3. Any minShouldMatch ( >= 0 )
      //
      // However, as WANDScorer uses more complex algorithm and data structure, we would like to
      // still use DisjunctionSumScorer to handle exhaustive pure disjunctions, which may be faster
      if (scoreMode == ScoreMode.TOP_SCORES || minShouldMatch > 1) {
        return new WANDScorer(weight, optionalScorers, minShouldMatch, scoreMode);
      } else {
        return new DisjunctionSumScorer(weight, optionalScorers, scoreMode);
      }
    }
  }

}
