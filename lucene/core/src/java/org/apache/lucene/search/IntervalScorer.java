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

class IntervalScorer extends Scorer {

  private final IntervalIterator intervals;
  private final String field;
  private final DocIdSetIterator approximation;
  private final LeafSimScorer simScorer;

  protected IntervalScorer(Weight weight, String field, DocIdSetIterator approximation, IntervalIterator intervals, LeafSimScorer simScorer) {
    super(weight);
    this.intervals = intervals;
    this.approximation = approximation;
    this.simScorer = simScorer;
    this.field = field;
  }

  @Override
  public int docID() {
    return approximation.docID();
  }

  @Override
  public float score() throws IOException {
    float freq = 0;
    do {
      freq += intervals.score();
    } while (intervals.nextInterval() != Intervals.NO_MORE_INTERVALS);
    return simScorer.score(docID(), freq);
  }

  @Override
  public IntervalIterator intervals(String field) {
    if (this.field.equals(field))
      return new IntervalIterator() {
        boolean started = false;

        @Override
        public int start() {
          return intervals.start();
        }

        @Override
        public int end() {
          return intervals.end();
        }

        @Override
        public int innerWidth() {
          return intervals.innerWidth();
        }

        @Override
        public boolean reset(int doc) throws IOException {
          // inner iterator already reset() in TwoPhaseIterator.matches()
          started = false;
          return true;
        }

        @Override
        public int nextInterval() throws IOException {
          if (started == false) {
            started = true;
            return start();
          }
          return intervals.nextInterval();
        }
      };
    return null;
  }

  @Override
  public DocIdSetIterator iterator() {
    return TwoPhaseIterator.asDocIdSetIterator(twoPhaseIterator());
  }

  @Override
  public TwoPhaseIterator twoPhaseIterator() {
    return new TwoPhaseIterator(approximation) {
      @Override
      public boolean matches() throws IOException {
        return intervals.reset(approximation.docID()) && intervals.nextInterval() != Intervals.NO_MORE_INTERVALS;
      }

      @Override
      public float matchCost() {
        return 0;
      }
    };
  }

  @Override
  public float getMaxScore(int upTo) throws IOException {
    return Float.POSITIVE_INFINITY;
  }


}
