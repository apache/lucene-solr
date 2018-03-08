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

import org.apache.lucene.util.MathUtil;

/**
 * Utility class to propagate scoring information in {@link BooleanQuery}, which
 * compute the score as the sum of the scores of its matching clauses.
 * This helps propagate information about the maximum produced score
 */
final class MaxScoreSumPropagator {

  private final int numClauses;
  private final Scorer[] scorers;

  MaxScoreSumPropagator(Collection<? extends Scorer> scorerList) throws IOException {
    numClauses = scorerList.size();
    scorers = scorerList.toArray(new Scorer[numClauses]);
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
