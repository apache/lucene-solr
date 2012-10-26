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

/**
 * Queue class for calculating minimal spanning conjunction intervals
 * @lucene.experimental
 */ 
final class IntervalQueueAnd extends IntervalQueue {
  
  /** the current right extreme positions of the queue */
  int rightExtreme = Integer.MIN_VALUE;
  /** the current right extreme offset of the queue */
  int rightExtremeOffset = Integer.MIN_VALUE;
  /** the current right extreme begin position*/
  int rightExtremeBegin;  
  /** the end of the internval on top of the queue*/
  int currentTopEnd;
  
  /**
   * Creates a new {@link IntervalQueueAnd} with a fixed size
   * @param size the size of the queue
   */
  IntervalQueueAnd(int size) {
    super(size);
  }

  @Override
  void reset () {
    super.reset();
    rightExtreme = Integer.MIN_VALUE;
    rightExtremeOffset = Integer.MIN_VALUE;
  }
  
  /**
   * Updates the right extreme of this queue if the end of the given interval is
   * greater or equal than the current right extreme of the queue.
   * 
   * @param intervalRef the interval to compare
   */
  void updateRightExtreme(IntervalRef intervalRef) {
    final Interval interval = intervalRef.interval;
    if (rightExtreme <= interval.end) {
      rightExtreme = interval.end;
      rightExtremeOffset = interval.offsetEnd;
      rightExtremeBegin = interval.begin;
    }
  }
 
  @Override
  void updateCurrentCandidate() {
    final IntervalRef top = top();
    Interval interval = top.interval;
    currentCandidate.begin = interval.begin;
    currentCandidate.offsetBegin = interval.offsetBegin;
    currentCandidate.end = rightExtreme;
    currentCandidate.offsetEnd = rightExtremeOffset;
    currentTopEnd = interval.end;
        
  }
  
  @Override
  protected boolean lessThan(IntervalRef left, IntervalRef right) {
    final Interval a = left.interval;
    final Interval b = right.interval;
    return a.begin < b.begin || (a.begin == b.begin && a.end > b.end) || a.offsetBegin < b.offsetBegin;
  }
  
}
