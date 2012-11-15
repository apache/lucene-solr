package org.apache.lucene.search.intervals;

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
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.intervals.IntervalQueue.IntervalRef;

import java.io.IOException;

/**
 * DisjunctionPositionIterator based on minimal interval semantics for OR
 * operator
 * 
 * <a href=
 * "http://vigna.dsi.unimi.it/ftp/papers/EfficientAlgorithmsMinimalIntervalSemantics"
 * >"Efficient Optimally Lazy Algorithms for Minimal-Interval Semantics"</a>
 * 
 * @lucene.experimental
 */
public final class DisjunctionIntervalIterator extends IntervalIterator {

  private final IntervalQueue queue;
  private final IntervalIterator[] iterators;

  /**
   * Creates a new DisjunctionIntervalIterator over a set of IntervalIterators
   * @param scorer the parent Scorer
   * @param collectIntervals <code>true</code> if intervals will be collected
   * @param intervals the IntervalIterators to iterate over
   * @throws IOException if a low-level I/O error is encountered
   */
  public DisjunctionIntervalIterator(Scorer scorer, boolean collectIntervals, IntervalIterator... intervals)
      throws IOException {
    super(scorer, collectIntervals);
    this.iterators = intervals;
    queue = new IntervalQueueOr(intervals.length);
  }

  private void advance() throws IOException {
    final IntervalRef top = queue.top();
    Interval interval = null;
    if ((interval = iterators[top.index].next()) != null) {
      top.interval = interval;
      queue.updateTop();
    } else {
      queue.pop();
    }
  }

  @Override
  public Interval next() throws IOException {
    while (queue.size() > 0 && queue.top().interval.begin <= queue.currentCandidate.begin) {
      advance();
    }
    if (queue.size() == 0) {
      return null;
    }
    queue.updateCurrentCandidate();
    return queue.currentCandidate; // TODO support payloads
  }

  @Override
  public IntervalIterator[] subs(boolean inOrder) {
    return iterators;
  }

  @Override
  public void collect(IntervalCollector collector) {
    assert collectIntervals;
    collector.collectComposite(scorer, queue.currentCandidate, docID());
    iterators[queue.top().index].collect(collector);
  }

  @Override
  public int scorerAdvanced(int docId) throws IOException {
    queue.reset();
    for (int i = 0; i < iterators.length; i++) {
      if (iterators[i].docID() <= docId) {
        int scorerAdvanced = iterators[i].scorerAdvanced(docId);
        //assert iterators[i].docID() == scorerAdvanced : " " + iterators[i];
      }
      if (iterators[i].docID() == docId) {
        Interval interval = iterators[i].next();
        if (interval != null)
          queue.add(new IntervalRef(interval, i));
      }
    }
    return this.docID();
  }

  @Override
  public int matchDistance() {
    return iterators[queue.top().index].matchDistance();
  }

}