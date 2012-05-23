package org.apache.lucene.search.poshighlight;

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

import java.io.IOException;

import org.apache.lucene.search.positions.PositionIntervalIterator;

/**
 * Present an array of PositionIntervals as an Iterator.
 * @lucene.experimental
 */
public class PositionIntervalArrayIterator extends PositionIntervalIterator {

  private int next = 0;
  private int count;
  private PositionInterval[] positions;
  
  public PositionIntervalArrayIterator (PositionInterval[] positions, int count) {
    super(null);
    this.positions = positions;
    this.count = count;
  }
  
  @Override
  public PositionInterval next() {
    if (next >= count)
      return null;
    return positions[next++];
  }

  @Override
  public PositionIntervalIterator[] subs(boolean inOrder) {
    return EMPTY;
  }

  @Override
  public void collect() {
  }

  @Override
  public int advanceTo(int docId) throws IOException {
    return 0;
  }
  
}