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


import java.io.IOException;

import org.apache.lucene.search.positions.PositionIntervalIterator.PositionIntervalFilter;


public class RangePositionsIterator extends PositionIntervalIterator implements PositionIntervalFilter {

  private final PositionIntervalIterator iterator;
  private int start;
  private int end;
  
  public RangePositionsIterator(int start, int end) {
    this(start, end, null);
  }
  
  public RangePositionsIterator(int start, int end, PositionIntervalIterator iterator) {
    super(iterator != null ? iterator.scorer : null);
    this.iterator = iterator;
    this.start = start;
    this.end = end;
  }

  public PositionIntervalIterator filter(PositionIntervalIterator iter) {
    return new RangePositionsIterator(start, end, iter);
  }  
  
  @Override
  public PositionInterval next() throws IOException {
    PositionInterval interval = null;
    while ((interval = iterator.next()) != null) {
      if(interval.end > end) {
        return null;
      } else if (interval.begin >= start) {
        return interval;
      }
    }
    return null;
  }

  @Override
  public PositionIntervalIterator[] subs(boolean inOrder) {
    return new PositionIntervalIterator[] { iterator };
  }
  
  

}