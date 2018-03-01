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

import org.apache.lucene.util.PriorityQueue;

abstract class DisjunctionIntervalIterator implements IntervalIterator {

  protected final PriorityQueue<IntervalIterator> queue;

  IntervalIterator current;

  DisjunctionIntervalIterator(int iteratorCount) {
    this.queue = new PriorityQueue<IntervalIterator>(iteratorCount) {
      @Override
      protected boolean lessThan(IntervalIterator a, IntervalIterator b) {
        return a.end() < b.end() || (a.end() == b.end() && a.start() >= b.start());
      }
    };
  }

  @Override
  public int start() {
    return current.start();
  }

  @Override
  public int end() {
    return current.end();
  }

  @Override
  public int innerWidth() {
    return current.innerWidth();
  }

  protected abstract void fillQueue(int doc) throws IOException;

  @Override
  public boolean reset(int doc) throws IOException {
    queue.clear();
    fillQueue(doc);
    current = null;
    return queue.size() > 0;
  }

  @Override
  public int nextInterval() throws IOException {
    if (current == null) {
      current = queue.top();
      return current.start();
    }
    int start = current.start(), end = current.end();
    while (queue.size() > 0 && contains(queue.top(), start, end)) {
      IntervalIterator it = queue.pop();
      if (it != null && it.nextInterval() != IntervalIterator.NO_MORE_INTERVALS) {
        queue.add(it);
      }
    }
    if (queue.size() == 0) {
      current = IntervalIterator.EMPTY;
      return IntervalIterator.NO_MORE_INTERVALS;
    }
    current = queue.top();
    return current.start();
  }

  private boolean contains(IntervalIterator it, int start, int end) {
    return start >= it.start() && start <= it.end() && end >= it.start() && end <= it.end();
  }

}
