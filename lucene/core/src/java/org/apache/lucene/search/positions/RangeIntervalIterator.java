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

import java.io.IOException;

import org.apache.lucene.search.positions.IntervalIterator.IntervalFilter;

/**
 * 
 * @lucene.experimental
 */ // nocommit - javadoc
public class RangeIntervalIterator extends IntervalIterator implements IntervalFilter {

  private final IntervalIterator iterator;
  private Interval interval;
  private int start;
  private int end;
  
  public RangeIntervalIterator(int start, int end) {
    this(false, start, end, null); // template
  }
  
  public RangeIntervalIterator(boolean collectPositions, int start, int end, IntervalIterator iterator) {
    super(iterator != null ? iterator.scorer : null,
        collectPositions);
    this.iterator = iterator;
    this.start = start;
    this.end = end;
  }

  public IntervalIterator filter(boolean collectPositions, IntervalIterator iter) {
    return new RangeIntervalIterator(collectPositions, start, end, iter);
  }  
  
  @Override
  public Interval next() throws IOException {
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
  public IntervalIterator[] subs(boolean inOrder) {
    return new IntervalIterator[] { iterator };
  }

  @Override
  public void collect(IntervalCollector collector) {
    assert collectPositions;
    collector.collectComposite(null, interval, iterator.docID());
    iterator.collect(collector);
  }

  @Override
  public int scorerAdvanced(int docId) throws IOException {
    return iterator.scorerAdvanced(docId);
  }

  @Override
  public int matchDistance() {
    return iterator.matchDistance();
  }
  
  

}