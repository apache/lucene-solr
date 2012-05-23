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
import org.apache.lucene.search.positions.IntervalQueue.IntervalRef;
import org.apache.lucene.search.positions.PositionIntervalIterator.PositionInterval;
import org.apache.lucene.util.PriorityQueue;

/**
 * 
 * @lucene.experimental
 */
// nocommit - javadoc
abstract class IntervalQueue extends PriorityQueue<IntervalRef> {
  final PositionInterval queueInterval = new PositionInterval(
      Integer.MIN_VALUE, Integer.MIN_VALUE);

  public void reset() {
    clear();
  }

  abstract public boolean topContainsQueueInterval();

  abstract public void updateQueueInterval();

  public IntervalQueue(int size) {
    super(size);
  }

  final static class IntervalRef {
    PositionInterval interval;
    int index;

    IntervalRef(PositionInterval interval, int index) {
      super();
      this.interval = interval;
      this.index = index;
    }
  }

}