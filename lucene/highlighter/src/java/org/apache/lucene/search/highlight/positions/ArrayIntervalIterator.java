package org.apache.lucene.search.highlight.positions;

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

import org.apache.lucene.search.positions.IntervalCollector;
import org.apache.lucene.search.positions.IntervalIterator;
import org.apache.lucene.search.positions.Interval;

/**
 * Present an array of PositionIntervals as an Iterator.
 * @lucene.experimental
 */
public class ArrayIntervalIterator extends IntervalIterator {

  private int next = 0;
  private int count;
  private Interval[] positions;
  
  public ArrayIntervalIterator (Interval[] positions, int count) {
    super(null, false);
    this.positions = positions;
    this.count = count;
  }
  
  @Override
  public Interval next() {
    if (next >= count)
      return null;
    return positions[next++];
  }

  @Override
  public IntervalIterator[] subs(boolean inOrder) {
    return EMPTY;
  }

  @Override
  public void collect(IntervalCollector collector) {
    assert collectPositions;
  }

  @Override
  public int scorerAdvanced(int docId) throws IOException {
    return 0;
  }

  @Override
  public int matchDistance() {
    return 0;
  }
  
}