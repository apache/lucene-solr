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
import org.apache.lucene.search.positions.IntervalQueue.IntervalRef;
import org.apache.lucene.util.PriorityQueue;

/**
 * Abstract base class for calculating minimal spanning intervals with Queues.
 * @see IntervalQueueAnd
 *  
 * @lucene.experimental
 * @lucene.internal
 */
abstract class IntervalQueue extends PriorityQueue<IntervalRef> {
  /**
   * The current interval spanning the queue
   */
  final Interval currentCandidate = new Interval(
      Integer.MIN_VALUE, Integer.MIN_VALUE, -1, -1);
  
  /**
   * Creates a new {@link IntervalQueue} with a fixed size
   * @param size the size of the queue
   */
  public IntervalQueue(int size) {
    super(size);
  }
  
  /**
   * Clears and resets the queue to its initial values;
   */
  void reset() {
    clear();
    currentCandidate.reset();
  }

  /**
   * Called by the consumer each time the head of the queue was updated
   */
  abstract void updateCurrentCandidate();

  /**
   * Holds a reference to an interval and its index.
   */
  final static class IntervalRef {
    Interval interval;
    final int index;

    IntervalRef(Interval interval, int index) {
      super();
      this.interval = interval;
      this.index = index;
    }
  }

}