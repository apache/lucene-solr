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
 * 
 * @lucene.experimental
 */ // nocommit - javadoc/
final class IntervalQueueAnd extends IntervalQueue {

  int rightExtreme = Integer.MIN_VALUE;
  int rightExtremeOffset = Integer.MIN_VALUE;
  int rightExtremeBegin;  
  int currentTopEnd;
  
  
  public IntervalQueueAnd(int size) {
    super(size);
  }

  public void reset () {
    super.reset();
    rightExtreme = Integer.MIN_VALUE;
    rightExtremeOffset = Integer.MIN_VALUE;
  }

  public void updateRightExtreme(IntervalRef intervalRef) {
    final Interval interval = intervalRef.interval;
    if (rightExtreme <= interval.end) {
      rightExtreme = interval.end;
      rightExtremeOffset = interval.offsetEnd;
      rightExtremeBegin = interval.begin;
    }
  }
 
  public void updateCurrentCandidate() {
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
