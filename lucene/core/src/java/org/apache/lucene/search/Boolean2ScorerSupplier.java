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
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.stream.Stream;

import org.apache.lucene.search.BooleanClause.Occur;

final class Boolean2ScorerSupplier extends ScorerSupplier {

  private final BooleanWeight weight;
  private final Map<BooleanClause.Occur, Collection<ScorerSupplier>> subs;
  private final boolean needsScores;
  private final int minShouldMatch;
  private long cost = -1;

  Boolean2ScorerSupplier(BooleanWeight weight,
      Map<Occur, Collection<ScorerSupplier>> subs,
      boolean needsScores, int minShouldMatch) {
    if (minShouldMatch < 0) {
      throw new IllegalArgumentException("minShouldMatch must be positive, but got: " + minShouldMatch);
    }
    if (minShouldMatch != 0 && minShouldMatch >= subs.get(Occur.SHOULD).size()) {
      throw new IllegalArgumentException("minShouldMatch must be strictly less than the number of SHOULD clauses");
    }
    if (needsScores == false && minShouldMatch == 0 && subs.get(Occur.SHOULD).size() > 0
        && subs.get(Occur.MUST).size() + subs.get(Occur.FILTER).size() > 0) {
      throw new IllegalArgumentException("Cannot pass purely optional clauses if scores are not needed");
    }
    if (subs.get(Occur.SHOULD).size() + subs.get(Occur.MUST).size() + subs.get(Occur.FILTER).size() == 0) {
      throw new IllegalArgumentException("There should be at least one positive clause");
    }
    this.weight = weight;
    this.subs = subs;
    this.needsScores = needsScores;
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
      final long shouldCost = MinShouldMatchSumScorer.cost(
          optionalScorers.stream().mapToLong(ScorerSupplier::cost),
          optionalScorers.size(), minShouldMatch);
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
    // three cases: conjunction, disjunction, or mix
    leadCost = Math.min(leadCost, cost());

    // pure conjunction
    if (subs.get(Occur.SHOULD).isEmpty()) {
      return excl(req(subs.get(Occur.FILTER), subs.get(Occur.MUST), leadCost), subs.get(Occur.MUST_NOT), leadCost);
    }

    // pure disjunction
    if (subs.get(Occur.FILTER).isEmpty() && subs.get(Occur.MUST).isEmpty()) {
      return excl(opt(subs.get(Occur.SHOULD), minShouldMatch, needsScores, leadCost), subs.get(Occur.MUST_NOT), leadCost);
    }

    // conjunction-disjunction mix:
    // we create the required and optional pieces, and then
    // combine the two: if minNrShouldMatch > 0, then it's a conjunction: because the
    // optional side must match. otherwise it's required + optional

    if (minShouldMatch > 0) {
      Scorer req = excl(req(subs.get(Occur.FILTER), subs.get(Occur.MUST), leadCost), subs.get(Occur.MUST_NOT), leadCost);
      Scorer opt = opt(subs.get(Occur.SHOULD), minShouldMatch, needsScores, leadCost);
      return new ConjunctionScorer(weight, Arrays.asList(req, opt), Arrays.asList(req, opt));
    } else {
      assert needsScores;
      return new ReqOptSumScorer(
          excl(req(subs.get(Occur.FILTER), subs.get(Occur.MUST), leadCost), subs.get(Occur.MUST_NOT), leadCost),
          opt(subs.get(Occur.SHOULD), minShouldMatch, needsScores, leadCost));
    }
  }

  /** Create a new scorer for the given required clauses. Note that
   *  {@code requiredScoring} is a subset of {@code required} containing
   *  required clauses that should participate in scoring. */
  private Scorer req(Collection<ScorerSupplier> requiredNoScoring, Collection<ScorerSupplier> requiredScoring, long leadCost) throws IOException {
    if (requiredNoScoring.size() + requiredScoring.size() == 1) {
      Scorer req = (requiredNoScoring.isEmpty() ? requiredScoring : requiredNoScoring).iterator().next().get(leadCost);

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

      return req;
    } else {
      List<Scorer> requiredScorers = new ArrayList<>();
      List<Scorer> scoringScorers = new ArrayList<>();
      for (ScorerSupplier s : requiredNoScoring) {
        requiredScorers.add(s.get(leadCost));
      }
      for (ScorerSupplier s : requiredScoring) {
        Scorer scorer = s.get(leadCost);
        requiredScorers.add(scorer);
        scoringScorers.add(scorer);
      }
      return new ConjunctionScorer(weight, requiredScorers, scoringScorers);
    }
  }

  private Scorer excl(Scorer main, Collection<ScorerSupplier> prohibited, long leadCost) throws IOException {
    if (prohibited.isEmpty()) {
      return main;
    } else {
      return new ReqExclScorer(main, opt(prohibited, 1, false, leadCost));
    }
  }

  private Scorer opt(Collection<ScorerSupplier> optional, int minShouldMatch,
      boolean needsScores, long leadCost) throws IOException {
    if (optional.size() == 1) {
      return optional.iterator().next().get(leadCost);
    } else {
      final List<Scorer> optionalScorers = new ArrayList<>();
      for (ScorerSupplier scorer : optional) {
        optionalScorers.add(scorer.get(leadCost));
      }
      if (minShouldMatch > 1) {
        return new MinShouldMatchSumScorer(weight, optionalScorers, minShouldMatch);
      } else {
        return new DisjunctionSumScorer(weight, optionalScorers, needsScores);
      }
    }
  }

}
