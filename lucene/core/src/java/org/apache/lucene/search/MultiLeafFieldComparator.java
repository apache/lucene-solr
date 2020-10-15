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

final class MultiLeafFieldComparator implements LeafFieldComparator {

  private final LeafFieldComparator[] comparators;
  private final int[] reverseMul;
  // we extract the first comparator to avoid array access in the common case
  // that the first comparator compares worse than the bottom entry in the queue
  private final LeafFieldComparator firstComparator;
  private final int firstReverseMul;

  MultiLeafFieldComparator(LeafFieldComparator[] comparators, int[] reverseMul) {
    if (comparators.length != reverseMul.length) {
      throw new IllegalArgumentException("Must have the same number of comparators and reverseMul, got "
          + comparators.length + " and " + reverseMul.length);
    }
    this.comparators = comparators;
    this.reverseMul = reverseMul;
    this.firstComparator = comparators[0];
    this.firstReverseMul = reverseMul[0];
  }

  @Override
  public void setBottom(int slot) throws IOException {
    for (LeafFieldComparator comparator : comparators) {
      comparator.setBottom(slot);
    }
  }

  @Override
  public int compareBottom(int doc) throws IOException {
    int cmp = firstReverseMul * firstComparator.compareBottom(doc);
    if (cmp != 0) {
      return cmp;
    }
    for (int i = 1; i < comparators.length; ++i) {
      cmp = reverseMul[i] * comparators[i].compareBottom(doc);
      if (cmp != 0) {
        return cmp;
      }
    }
    return 0;
  }

  @Override
  public int compareTop(int doc) throws IOException {
    int cmp = firstReverseMul * firstComparator.compareTop(doc);
    if (cmp != 0) {
      return cmp;
    }
    for (int i = 1; i < comparators.length; ++i) {
      cmp = reverseMul[i] * comparators[i].compareTop(doc);
      if (cmp != 0) {
        return cmp;
      }
    }
    return 0;
  }

  @Override
  public void copy(int slot, int doc) throws IOException {
    for (LeafFieldComparator comparator : comparators) {
      comparator.copy(slot, doc);
    }
  }

  @Override
  public void setScorer(Scorable scorer) throws IOException {
    for (LeafFieldComparator comparator : comparators) {
      comparator.setScorer(scorer);
    }
  }

  @Override
  public void setHitsThresholdReached() throws IOException {
    // this is needed for skipping functionality that is only relevant for the 1st comparator
    firstComparator.setHitsThresholdReached();
  }

  @Override
  public DocIdSetIterator competitiveIterator() throws IOException {
    // this is needed for skipping functionality that is only relevant for the 1st comparator
    return firstComparator.competitiveIterator();
  }
}
