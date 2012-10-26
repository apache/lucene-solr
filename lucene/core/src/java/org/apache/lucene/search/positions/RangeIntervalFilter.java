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

/**
 * An IntervalFilter that restricts Intervals returned by an IntervalIterator
 * to those which occur between a given start and end position.
 *
 * @lucene.experimental
 */
public class RangeIntervalFilter implements IntervalFilter {

  private int start;
  private int end;

  /**
   * Constructs a new RangeIntervalFilter
   * @param start the start of the filtered range
   * @param end the end of the filtered range
   */
  public RangeIntervalFilter(int start, int end) {
    this.start = start;
    this.end = end;
  }

  @Override
  public IntervalIterator filter(boolean collectPositions, IntervalIterator iter) {
    return new RangeIntervalIterator(collectPositions, iter);
  }

  /**
   * Wraps an IntervalIterator ignoring Intervals that fall outside a
   * given range.
   */
  private class RangeIntervalIterator extends IntervalIterator {

    private final IntervalIterator iterator;
    private Interval interval;

    RangeIntervalIterator(boolean collectPositions, IntervalIterator iter) {
      super(iter == null ? null : iter.scorer, collectPositions);
      this.iterator = iter;
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

}