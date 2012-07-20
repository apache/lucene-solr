package org.apache.lucene.search.positions;

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
public final class DisjunctionIntervalIterator extends BooleanIntervalIterator {

  public DisjunctionIntervalIterator(Scorer scorer, boolean collectPositions, IntervalIterator... intervals)
      throws IOException {
    super(scorer, intervals, new IntervalQueueOr(intervals.length), collectPositions);
  }

  void advance() throws IOException {
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
    assert collectPositions;
    collector.collectComposite(scorer, queue.currentCandidate, docID());
    iterators[queue.top().index].collect(collector);
  }

  @Override
  public int scorerAdvanced(int docId) throws IOException {
    queue.reset();
    for (int i = 0; i < iterators.length; i++) {
      int scorerAdvanced =  iterators[i].scorerAdvanced(docId);
      assert iterators[i].docID() == scorerAdvanced : " " + iterators[i];

      if (scorerAdvanced == docId) {
        queue.add(new IntervalRef(iterators[i].next(), i));
      }
    }
    return this.docID();
  }

  @Override
  public int matchDistance() {
    return iterators[queue.top().index].matchDistance();
  }

}