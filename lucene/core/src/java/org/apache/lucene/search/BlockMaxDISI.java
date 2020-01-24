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

/**
 * {@link DocIdSetIterator} that skips non-competitive docs by checking
 * the max score of the provided {@link Scorer} for the current block.
 * Call {@link #setMinCompetitiveScore(float)} in order to give this iterator the ability
 * to skip low-scoring documents.
 * @lucene.internal
 */
public class BlockMaxDISI extends DocIdSetIterator {
  protected final Scorer scorer;
  private final DocIdSetIterator in;
  private float minScore;
  private float maxScore;
  private int upTo = -1;

  public BlockMaxDISI(DocIdSetIterator iterator, Scorer scorer) {
    this.in = iterator;
    this.scorer = scorer;
  }

  @Override
  public int docID() {
    return in.docID();
  }

  @Override
  public int nextDoc() throws IOException {
    return advance(docID()+1);
  }

  @Override
  public int advance(int target) throws IOException {
    int doc = advanceImpacts(target);
    return in.advance(doc);
  }

  @Override
  public long cost() {
    return in.cost();
  }

  public void setMinCompetitiveScore(float minScore) {
    this.minScore = minScore;
  }

  private void moveToNextBlock(int target) throws IOException {
    upTo = scorer.advanceShallow(target);
    maxScore = scorer.getMaxScore(upTo);
  }

  private int advanceImpacts(int target) throws IOException {
    if (minScore == -1 || target == NO_MORE_DOCS) {
      return target;
    }

    if (target > upTo) {
      moveToNextBlock(target);
    }

    while (true) {
      if (maxScore >= minScore) {
        return target;
      }

      if (upTo == NO_MORE_DOCS) {
        return NO_MORE_DOCS;
      }

      target = upTo + 1;

      moveToNextBlock(target);
    }
  }
}
