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
import java.util.Arrays;

/**
 * A {@link BulkScorer}-backed scorer.
 */
public class BulkScorerWrapperScorer extends Scorer {

  private final BulkScorer scorer;

  private int i = -1;
  private int doc = -1;
  private int next = 0;

  private final int[] docs;
  private final int[] freqs;
  private final float[] scores;
  private int bufferLength;

  /** Sole constructor. */
  public BulkScorerWrapperScorer(Weight weight, BulkScorer scorer, int bufferSize) {
    super(weight);
    this.scorer = scorer;
    docs = new int[bufferSize];
    freqs = new int[bufferSize];
    scores = new float[bufferSize];
  }

  private void refill(int target) throws IOException {
    bufferLength = 0;
    while (next != DocIdSetIterator.NO_MORE_DOCS && bufferLength == 0) {
      final int min = Math.max(target, next);
      final int max = min + docs.length;
      next = scorer.score(new LeafCollector() {
        Scorer scorer;
        @Override
        public void setScorer(Scorer scorer) throws IOException {
          this.scorer = scorer;
        }
        @Override
        public void collect(int doc) throws IOException {
          docs[bufferLength] = doc;
          freqs[bufferLength] = scorer.freq();
          scores[bufferLength] = scorer.score();
          bufferLength += 1;
        }
      }, null, min, max);
    }
    i = -1;
  }

  @Override
  public float score() throws IOException {
    return scores[i];
  }

  @Override
  public int freq() throws IOException {
    return freqs[i];
  }

  @Override
  public int docID() {
    return doc;
  }

  @Override
  public DocIdSetIterator iterator() {
    return new DocIdSetIterator() {
      @Override
      public int docID() {
        return doc;
      }

      @Override
      public int nextDoc() throws IOException {
        return advance(docID() + 1);
      }

      @Override
      public int advance(int target) throws IOException {
        if (bufferLength == 0 || docs[bufferLength - 1] < target) {
          refill(target);
        }

        i = Arrays.binarySearch(docs, i + 1, bufferLength, target);
        if (i < 0) {
          i = -1 - i;
        }
        if (i == bufferLength) {
          return doc = DocIdSetIterator.NO_MORE_DOCS;
        }
        return doc = docs[i];
      }

      @Override
      public long cost() {
        return scorer.cost();
      }
    };
  }

}
