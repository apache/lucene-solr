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

package org.apache.lucene.queries.intervals;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.search.DocIdSetIterator;

abstract class ConjunctionIntervalIterator extends IntervalIterator {

  final DocIdSetIterator approximation;
  final List<IntervalIterator> subIterators;
  final float cost;

  ConjunctionIntervalIterator(List<IntervalIterator> subIterators) {
    this.approximation = ConjunctionDISI.intersectIterators(subIterators);
    this.subIterators = subIterators;
    float costsum = 0;
    for (IntervalIterator it : subIterators) {
      costsum += it.matchCost();
    }
    this.cost = costsum;
  }

  @Override
  public int docID() {
    return approximation.docID();
  }

  @Override
  public int nextDoc() throws IOException {
    int doc = approximation.nextDoc();
    reset();
    return doc;
  }

  @Override
  public int advance(int target) throws IOException {
    int doc = approximation.advance(target);
    reset();
    return doc;
  }

  protected abstract void reset() throws IOException;

  @Override
  public long cost() {
    return approximation.cost();
  }

  @Override
  public final float matchCost() {
    return cost;
  }

}
