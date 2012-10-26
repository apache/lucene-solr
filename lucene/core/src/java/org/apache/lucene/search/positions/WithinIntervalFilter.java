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
 * to those which have a matchDistance less than a defined slop.
 *
 * @lucene.experimental
 */
public class WithinIntervalFilter implements IntervalFilter {

  private final int slop;

  /**
   * Construct a new WithinIntervalFilter
   * @param slop the maximum slop allowed for subintervals
   */
  public WithinIntervalFilter(int slop) {
    this.slop = slop;
  }

  /**
   * @return the slop
   */
  public int getSlop() {
    return slop;
  }

  @Override
  public IntervalIterator filter(boolean collectIntervals, IntervalIterator iter) {
    return new WithinIntervalIterator(collectIntervals, iter);
  }

  class WithinIntervalIterator extends IntervalIterator {

    private IntervalIterator iterator;
    private Interval interval;

    WithinIntervalIterator(boolean collectIntervals, IntervalIterator iter) {
      super(iter == null ? null : iter.scorer, collectIntervals);
      this.iterator = iter;
    }

    @Override
    public Interval next() throws IOException {
      while ((interval = iterator.next()) != null) {
        if((iterator.matchDistance()) <= slop){
          return interval;
        }
      }
      return null;
    }

    @Override
    public IntervalIterator[] subs(boolean inOrder) {
      return new IntervalIterator[] {iterator};
    }


    @Override
    public void collect(IntervalCollector collector) {
      assert collectIntervals;
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
