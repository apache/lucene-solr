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

import org.apache.lucene.search.positions.PositionIntervalIterator.PositionInterval;
/**
 * 
 * @lucene.experimental
 */ // nocommit - javadoc/
final class IntervalQueueAnd extends IntervalQueue {

  int rightExtreme = Integer.MIN_VALUE;
  int rightExtremeOffset = Integer.MIN_VALUE;
  int rightExtremeOrd = Integer.MIN_VALUE; // the ord of the queues right extreme - ordered case! 
  
  public IntervalQueueAnd(int size) {
    super(size);
  }

  public void reset () {
    super.reset();
    currentCandidate.begin = Integer.MIN_VALUE;
    currentCandidate.end = Integer.MIN_VALUE;
    rightExtreme = Integer.MIN_VALUE;
    rightExtremeOffset = Integer.MIN_VALUE;
    rightExtremeOrd = Integer.MIN_VALUE;
  }

  public void updateRightExtreme(IntervalRef ref) {
    if (rightExtreme < ref.interval.end) {
      rightExtreme = ref.interval.end;
      rightExtremeOrd = ref.ord;
    }
    
    rightExtremeOffset = Math.max(rightExtremeOffset, ref.interval.offsetEnd);
  }
  
  public boolean topContainsQueueInterval() {
    PositionInterval interval = top().interval;
    return interval.begin <= currentCandidate.begin
        && currentCandidate.end <= rightExtreme;
  }
 
  public void updateCurrentCandidate() {
    PositionInterval interval = top().interval;
    currentCandidate.begin = interval.begin;
    currentCandidate.offsetBegin = interval.offsetBegin;
    currentCandidate.end = rightExtreme;
    currentCandidate.offsetEnd = rightExtremeOffset;
  }
  
  @Override
  protected boolean lessThan(IntervalRef left, IntervalRef right) {
    final PositionInterval a = left.interval;
    final PositionInterval b = right.interval;
    return a.begin < b.begin || (a.begin == b.begin && a.end >= b.end);
  }
}
