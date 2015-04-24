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

/**
 * Wrapper used in {@link DisiPriorityQueue}.
 * @lucene.internal
 */
public class DisiWrapper<Iter extends DocIdSetIterator> {
  public final Iter iterator;
  public final long cost;
  public int doc; // the current doc, used for comparison
  public DisiWrapper<Iter> next; // reference to a next element, see #topList

  // An approximation of the iterator, or the iterator itself if it does not
  // support two-phase iteration
  public final DocIdSetIterator approximation;
  // A two-phase view of the iterator, or null if the iterator does not support
  // two-phase iteration
  public final TwoPhaseIterator twoPhaseView;
  
  public int lastApproxMatchDoc; // last doc of approximation that did match
  public int lastApproxNonMatchDoc; // last doc of approximation that did not match

  public DisiWrapper(Iter iterator) {
    this.iterator = iterator;
    this.cost = iterator.cost();
    this.doc = -1;
    this.twoPhaseView = TwoPhaseIterator.asTwoPhaseIterator(iterator);
      
    if (twoPhaseView != null) {
      approximation = twoPhaseView.approximation();
    } else {
      approximation = iterator;
    }
    this.lastApproxNonMatchDoc = -2;
    this.lastApproxMatchDoc = -2;
  }
}

