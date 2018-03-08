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
import java.util.List;

abstract class ConjunctionIntervalIterator implements IntervalIterator {

  protected final List<IntervalIterator> subIterators;

  final DocIdSetIterator approximation;
  final float cost;

  ConjunctionIntervalIterator(List<IntervalIterator> subIterators) {
    this.subIterators = subIterators;
    float costsum = 0;
    List<DocIdSetIterator> approximations = new ArrayList<>();
    for (IntervalIterator it : subIterators) {
      costsum += it.cost();
      approximations.add(it.approximation());
    }
    this.cost = costsum;
    this.approximation = ConjunctionDISI.intersectIterators(approximations);

  }

  @Override
  public final DocIdSetIterator approximation() {
    return approximation;
  }

  @Override
  public final float cost() {
    return cost;
  }

  @Override
  public String toString() {
    return approximation.docID() + ":[" + start() + "->" + end() + "]";
  }
}
