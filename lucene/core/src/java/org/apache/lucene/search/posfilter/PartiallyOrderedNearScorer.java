package org.apache.lucene.search.posfilter;

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

import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.search.PositionQueue;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.SorterTemplate;

import java.io.IOException;


public class PartiallyOrderedNearScorer extends PositionFilteredScorer {

  private final SloppySpanningPositionQueue posQueue;
  private final int allowedSlop;

  private int previousEnd;

  public static int MAX_SLOP = Integer.MAX_VALUE;

  public PartiallyOrderedNearScorer(Scorer filteredScorer, int allowedSlop) {
    super(filteredScorer);
    this.posQueue = new SloppySpanningPositionQueue(subScorers);
    this.allowedSlop = allowedSlop;
  }

  @Override
  protected int doNextPosition() throws IOException {
    int currentSlop = MAX_SLOP;
    while (posQueue.isFull() && posQueue.span.begin == current.begin) {
      posQueue.nextPosition();
    }
    if (!posQueue.isFull())
      return NO_MORE_POSITIONS;
    while (true) {
      do {
        posQueue.updateCurrent(current);
        currentSlop = posQueue.calculateSlop(allowedSlop);
        if (current.equals(posQueue.top().interval) && currentSlop <= allowedSlop) {
          previousEnd = current.end;
          return current.begin;
        }
        posQueue.nextPosition();
      } while (posQueue.isFull() && current.end == posQueue.span.end);
      if (current.begin <= previousEnd)
        continue;
      if (currentSlop <= allowedSlop) {
        previousEnd = current.end;
        return current.begin;
      }
      if (!posQueue.isFull())
        return NO_MORE_POSITIONS;
    }
  }

  @Override
  protected void reset(int doc) throws IOException {
    super.reset(doc);
    current.reset();
    posQueue.advanceTo(doc);
    previousEnd = -1;
  }

  private static class IntervalRef {

    public Interval interval = new Interval();
    public int ord;

    public IntervalRef() {}

    public void update(IntervalRef other) {
      this.ord = other.ord;
      this.interval.update(other.interval);
    }

    public void update(Interval interval, int ord) {
      this.ord = ord;
      this.interval.update(interval);
    }
  }

  private static class SloppySpanningPositionQueue extends PositionQueue {

    Interval span = new Interval();
    final Interval[] subIntervals;
    final IntervalRef[] sortedIntervals;
    int scorerCount;

    public SloppySpanningPositionQueue(Scorer[] subScorers) {
      super(subScorers);
      scorerCount = subScorers.length;
      subIntervals = new Interval[subScorers.length];
      sortedIntervals = new IntervalRef[subScorers.length];
      for (int i = 0; i < subScorers.length; i++) {
        subIntervals[i] = new Interval();
        sortedIntervals[i] = new IntervalRef();
      }
    }

    public boolean isFull() {
      return queuesize == scorerCount;
    }

    public void updateCurrent(Interval current) {
      final Interval top = this.top().interval;
      current.update(top, span);
    }

    private void updateRightExtreme(Interval newRight) {
      if (span.end <= newRight.end) {
        span.update(span, newRight);
      }
    }

    protected void updateInternalIntervals() {
      DocsEnumRef deRef = top();
      subIntervals[deRef.ord].update(deRef.interval);
      //subIntervals[deRef.ord].ord = deRef.ord;
      updateRightExtreme(deRef.interval);
    }

    @Override
    public int nextPosition() throws IOException {
      int position;
      if ((position = super.nextPosition()) == DocsEnum.NO_MORE_POSITIONS) {
        return DocsEnum.NO_MORE_POSITIONS;
      }
      span.update(top().interval, span);
      return position;
    }

    @Override
    protected void init() throws IOException {
      super.init();
      DocsEnumRef deRef;
      for (Object heapRef : getHeapArray()) {
        if (heapRef != null) {
          deRef = (DocsEnumRef) heapRef;
          subIntervals[deRef.ord].update(deRef.interval);
          //subIntervals[deRef.ord].ord = deRef.ord;
          updateRightExtreme(deRef.interval);
        }
      }
    }

    @Override
    public void advanceTo(int doc) {
      super.advanceTo(doc);
      span.reset();
    }

    @Override
    protected boolean lessThan(DocsEnumRef left, DocsEnumRef right) {
      final Interval a = left.interval;
      final Interval b = right.interval;
      return a.begin < b.begin || (a.begin == b.begin && a.end > b.end);
    }

    @Override
    public String toString() {
      return top().interval.toString();
    }

    // nocommit, is this algorithm ok or is it going to be horribly inefficient?
    // We sort the subintervals by their start positions.  If a subinterval is
    // out of position, we calculate it's slop contribution by counting the
    // number of subsequent subintervals with lower ords.  Gaps between subintervals
    // are also added.  If the running total exceeds a provided max allowed slop,
    // then we shortcut the calculation and return MAX_SLOP.
    public int calculateSlop(int maxAllowedSlop) {
      boolean swaps = false;
      int slop = 0;
      sortSubIntervals();
      for (int i = 0; i < sortedIntervals.length; i++) {
        if (swaps || sortedIntervals[i].ord != i) {
          swaps = true;
          for (int j = i + 1; j < sortedIntervals.length; j++) {
            if (sortedIntervals[j].ord < sortedIntervals[i].ord)
              slop++;
          }
        }
        if (i > 0)
          slop += (sortedIntervals[i].interval.begin - sortedIntervals[i - 1].interval.end) - 1;
        if (slop > maxAllowedSlop)
          return MAX_SLOP;
      }
      return slop;
    }

    private void sortSubIntervals() {

      for (int i = 0; i < subIntervals.length; i++) {
        sortedIntervals[i].update(subIntervals[i], i);
      }

      new SorterTemplate() {

        int pivot;

        @Override
        protected void swap(int i, int j) {
          IntervalRef temp = new IntervalRef();
          temp.update(sortedIntervals[i]);
          sortedIntervals[i].update(sortedIntervals[j]);
          sortedIntervals[j].update(temp);
        }

        @Override
        protected int compare(int i, int j) {
          return Long.signum(sortedIntervals[i].interval.begin - sortedIntervals[j].interval.begin);
        }

        @Override
        protected void setPivot(int i) {
          this.pivot = sortedIntervals[i].interval.begin;
        }

        @Override
        protected int comparePivot(int j) {
          return Long.signum(pivot - sortedIntervals[j].interval.begin);
        }
      }.mergeSort(0, sortedIntervals.length - 1);
    }
  }
}
