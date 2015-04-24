package org.apache.lucene.search;
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

import java.io.IOException;

/**
 * A {@link DocIdSetIterator} which is a disjunction of the approximations of
 * the provided iterators.
 * @lucene.internal
 */
public class DisjunctionDISIApproximation<Iter extends DocIdSetIterator>
extends DocIdSetIterator {

  final DisiPriorityQueue<Iter> subIterators;
  final long cost;

  public DisjunctionDISIApproximation(DisiPriorityQueue<Iter> subIterators) {
    this.subIterators = subIterators;
    long cost = 0;
    for (DisiWrapper<Iter> w : subIterators) {
      cost += w.cost;
    }
    this.cost = cost;
  }

  @Override
  public long cost() {
    return cost;
  }

  @Override
  public int docID() {
   return subIterators.top().doc;
  }

  @Override
  public int nextDoc() throws IOException {
    DisiWrapper<Iter> top = subIterators.top();
    final int doc = top.doc;
    do {
      top.doc = top.approximation.nextDoc();
      top = subIterators.updateTop();
    } while (top.doc == doc);

    return top.doc;
  }

  @Override
  public int advance(int target) throws IOException {
    DisiWrapper<Iter> top = subIterators.top();
    do {
      top.doc = top.approximation.advance(target);
      top = subIterators.updateTop();
    } while (top.doc < target);

    return top.doc;
  }
}


