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
 */ // nocommit - javadoc
final class IntervalQueueOr extends IntervalQueue {
  
  public IntervalQueueOr(int size) {
    super(size);
  }
  
  public void updateCurrentCandidate() {
    currentCandidate.copy(top().interval);
  }
  
  @Override
  protected boolean lessThan(IntervalRef left, IntervalRef right) {
    final Interval a = left.interval;
    final Interval b = right.interval;
    return a.end < b.end || (a.end == b.end && a.begin >= b.begin);
  }

}
