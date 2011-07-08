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
 * ConjuctionPositionIterator based on minimal interval semantics for AND
 * operator
 * 
 * <a href=
 * "http://vigna.dsi.unimi.it/ftp/papers/EfficientAlgorithmsMinimalIntervalSemantics"
 * >"Efficient Optimally Lazy Algorithms for Minimal-Interval Semantic</a>
 * 
 */
@SuppressWarnings("serial")
public final class ConjunctionPositionIterator extends BooleanPositionIterator {
  private final IntervalQueueAnd queue;
  public ConjunctionPositionIterator(Scorer scorer, Scorer[] subScorers) throws IOException {
    super(scorer, subScorers, new IntervalQueueAnd(subScorers.length));
    queue = (IntervalQueueAnd) super.queue; // avoid lots of casts?
  }

  void advance() throws IOException {
    final IntervalRef top = queue.top();
    PositionInterval interval = null;
    if ((interval = iterators[top.index].next()) != null) {
      top.interval = interval;
      queue.updateRightExtreme(interval);
      queue.updateTop();
    } else {
      queue.pop();
    }
  }

  @Override
  public PositionInterval next() throws IOException {
    if (docId != scorer.docID()) {
      docId = scorer.docID();
      queue.reset();
      for (int i = 0; i < iterators.length; i++) {
        final PositionInterval interval = iterators[i].next();
        if (interval == null) {
          return null;
        }
        queue.updateRightExtreme(interval);
        queue.add(new IntervalRef(interval, i));
      }
    }
    if (queue.size() != iterators.length) {
      return null;
    }
    while (queue.topContainsQueueInterval()) {
      advance();
      if (queue.size() != iterators.length)
        return null;
    }
    do {
      queue.updateQueueInterval();
      advance();
      if (queue.size() != iterators.length)
        break;
    } while (queue.topContainsQueueInterval());
    return queue.queueInterval; // TODO support payloads
  }
  
}