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
import java.util.Collection;

import org.apache.lucene.util.InPlaceMergeSorter;
import org.apache.lucene.util.MathUtil;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Utility class to propagate scoring information in {@link BooleanQuery}, which
 * compute the score as the sum of the scores of its matching clauses.
 * This helps propagate information about the maximum produced score
 */
final class MaxScoreSumPropagator {

  /**
   * Return an array which, at index i, stores the sum of all entries of
   * {@code v} except the one at index i.
   */
  private static double[] computeSumOfComplement(float[] v) {
    // We do not use subtraction on purpose because it would defeat the
    // upperbound formula that we use for sums.
    // Naive approach would be O(n^2), but we can do O(n) by computing the
    // sum for i<j and i>j and then sum them.
    double[] sum1 = new double[v.length];
    for (int i = 1; i < sum1.length; ++i) {
      sum1[i] = sum1[i - 1] + v[i - 1];
    }

    double[] sum2 = new double[v.length];
    for (int i = sum2.length - 2; i >= 0; --i) {
      sum2[i] = sum2[i + 1] + v[i + 1];
    }

    double[] result = new double[v.length];
    for (int i = 0; i < result.length; ++i) {
      result[i] = sum1[i] + sum2[i];
    }
    return result;
  }

  private final int numClauses;
  private final Scorer[] scorers;
  private final double[] sumOfOtherMaxScores;

  MaxScoreSumPropagator(Collection<? extends Scorer> scorerList) throws IOException {
    numClauses = scorerList.size();
    scorers = scorerList.toArray(new Scorer[numClauses]);

    // We'll need max scores multiple times so we cache them
    float[] maxScores = new float[numClauses];
    for (int i = 0; i < numClauses; ++i) {
      scorers[i].advanceShallow(0);
      maxScores[i] = scorers[i].getMaxScore(NO_MORE_DOCS);
    }
    // Sort by decreasing max score
    new InPlaceMergeSorter() {
      @Override
      protected void swap(int i, int j) {
        Scorer tmp = scorers[i];
        scorers[i] = scorers[j];
        scorers[j] = tmp;
        float tmpF = maxScores[i];
        maxScores[i] = maxScores[j];
        maxScores[j] = tmpF;
      }

      @Override
      protected int compare(int i, int j) {
        return Float.compare(maxScores[j], maxScores[i]);
      }
    }.sort(0, scorers.length);

    sumOfOtherMaxScores = computeSumOfComplement(maxScores);
  }

  void advanceShallow(int target) throws IOException {
    for (Scorer s : scorers) {
      if (s.docID() < target) {
        s.advanceShallow(target);
      }
    }
  }

  float getMaxScore(int upTo) throws IOException {
    double maxScore = 0;
    for (Scorer s : scorers) {
      if (s.docID() <= upTo) {
        maxScore += s.getMaxScore(upTo);
      }
    }
    return scoreSumUpperBound(maxScore);
  }

  void setMinCompetitiveScore(float minScore) throws IOException {
    if (minScore == 0) {
      return ;
    }
    // A double that is less than 'minScore' might still be converted to 'minScore'
    // when casted to a float, so we go to the previous float to avoid this issue
    float minScoreDown = Math.nextDown(minScore);
    for (int i = 0; i < numClauses; ++i) {
      double sumOfOtherMaxScores = this.sumOfOtherMaxScores[i];
      float minCompetitiveScore = getMinCompetitiveScore(minScoreDown, sumOfOtherMaxScores);
      if (minCompetitiveScore <= 0) {
        // given that scorers are sorted by decreasing max score, next scorers will
        // have 0 as a minimum competitive score too
        break;
      }
      scorers[i].setMinCompetitiveScore(minCompetitiveScore);
    }
  }

  /**
   * Return the minimum score that a Scorer must produce in order for a hit to
   * be competitive.
   */
  private float getMinCompetitiveScore(float minScoreSum, double sumOfOtherMaxScores) {
    assert numClauses > 0;
    if (minScoreSum <= sumOfOtherMaxScores) {
      return 0f;
    }

    // We need to find a value 'minScore' so that 'minScore + sumOfOtherMaxScores <= minScoreSum'
    // TODO: is there an efficient way to find the greatest value that meets this requirement?
    float minScore = (float) (minScoreSum - sumOfOtherMaxScores);
    int iters = 0;
    while (scoreSumUpperBound(minScore + sumOfOtherMaxScores) > minScoreSum) {
      // Important: use ulp of minScoreSum and not minScore to make sure that we
      // converge quickly.
      minScore -= Math.ulp(minScoreSum);
      // this should converge in at most two iterations:
      //  - one because of the subtraction rounding error
      //  - one because of the error introduced by sumUpperBound
      assert ++iters <= 2 : iters;
    }
    return Math.max(minScore, 0f);
  }

  private float scoreSumUpperBound(double sum) {
    if (numClauses <= 2) {
      // When there are only two clauses, the sum is always the same regardless
      // of the order.
      return (float) sum;
    }

    // The error of sums depends on the order in which values are summed up. In
    // order to avoid this issue, we compute an upper bound of the value that
    // the sum may take. If the max relative error is b, then it means that two
    // sums are always within 2*b of each other.
    // For conjunctions, we could skip this error factor since the order in which
    // scores are summed up is predictable, but in practice, this wouldn't help
    // much since the delta that is introduced by this error factor is usually
    // cancelled by the float cast.
    double b = MathUtil.sumRelativeErrorBound(numClauses);
    return (float) ((1.0 + 2 * b) * sum);
  }
}
