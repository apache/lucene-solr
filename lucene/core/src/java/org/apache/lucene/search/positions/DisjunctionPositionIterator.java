package org.apache.lucene.search.positions;

/**
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

import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.positions.IntervalQueue.IntervalRef;

/**
 * DisjunctionPositionIterator based on minimal interval semantics for OR
 * operator
 * 
 * <a href=
 * "http://vigna.dsi.unimi.it/ftp/papers/EfficientAlgorithmsMinimalIntervalSemantics"
 * >"Efficient Optimally Lazy Algorithms for Minimal-Interval Semantic</a>
 * 
 * @lucene.experimental
 */
// nocommit - javadoc
public final class DisjunctionPositionIterator extends BooleanPositionIterator {

  public DisjunctionPositionIterator(Scorer scorer, Scorer[] subScorers)
      throws IOException {
    super(scorer, subScorers, new IntervalQueueOr(subScorers.length));
  }

  void advance() throws IOException {
    final IntervalRef top = queue.top();
    PositionInterval interval = null;
    if ((interval = iterators[top.index].next()) != null) {
      top.interval = interval;
      queue.updateTop();
    } else {
      queue.pop();
    }
  }

  @Override
  public PositionInterval next() throws IOException {
    while (queue.size() > 0 && queue.topContainsQueueInterval()) {
      advance();
    }
    if (queue.size() == 0) {
      return null;
    }
    queue.updateQueueInterval();
    return queue.queueInterval; // TODO support payloads
  }

  @Override
  public PositionIntervalIterator[] subs(boolean inOrder) {
    return iterators;
  }

  @Override
  public void collect() {
    collector.collectComposite(scorer, queue.queueInterval, currentDoc);
    iterators[queue.top().index].collect();
  }

  @Override
  public int advanceTo(int docId) throws IOException {
    queue.clear();
    int minAdvance = NO_MORE_DOCS;
    for (int i = 0; i < iterators.length; i++) {
      if (iterators[i].docID() < docId) {
        minAdvance = Math.min(minAdvance, iterators[i].advanceTo(docId));
      } else {
        minAdvance = Math.min(minAdvance, iterators[i].docID());
      }
    }
    if (minAdvance == NO_MORE_DOCS) {
      return NO_MORE_DOCS;
    }
    for (int i = 0; i < iterators.length; i++) {
      if (iterators[i].docID() == minAdvance) {
        queue.add(new IntervalRef(iterators[i].next(), i));
      }
    }
    return currentDoc = minAdvance;
  }

}