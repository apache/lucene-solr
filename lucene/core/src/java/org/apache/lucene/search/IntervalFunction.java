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

package org.apache.lucene.search;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.apache.lucene.util.PriorityQueue;

import static org.apache.lucene.search.Intervals.NO_MORE_INTERVALS;

public abstract class IntervalFunction {

  @Override
  public abstract int hashCode();

  @Override
  public abstract boolean equals(Object obj);

  @Override
  public abstract String toString();

  public abstract IntervalIterator apply(List<IntervalIterator> iterators);

  public static final IntervalFunction ORDERED = new SingletonFunction("ORDERED") {
    @Override
    public IntervalIterator apply(List<IntervalIterator> intervalIterators) {
      return orderedIntervalIterator(intervalIterators);
    }
  };

  public static class OrderedNearFunction extends IntervalFunction {

    public OrderedNearFunction(int minWidth, int maxWidth) {
      this.minWidth = minWidth;
      this.maxWidth = maxWidth;
    }

    final int minWidth;
    final int maxWidth;

    @Override
    public IntervalIterator apply(List<IntervalIterator> intervalIterators) {
      return IntervalFilter.innerWidthFilter(orderedIntervalIterator(intervalIterators), minWidth, maxWidth);
    }

    @Override
    public String toString() {
      return "ONEAR[" + minWidth + "/" + maxWidth + "]";
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      OrderedNearFunction that = (OrderedNearFunction) o;
      return minWidth == that.minWidth &&
          maxWidth == that.maxWidth;
    }

    @Override
    public int hashCode() {
      return Objects.hash(minWidth, maxWidth);
    }
  }

  public static IntervalIterator orderedIntervalIterator(List<IntervalIterator> subIterators) {
    for (IntervalIterator it : subIterators) {
      if (it == IntervalIterator.EMPTY)
        return IntervalIterator.EMPTY;
    }
    return new OrderedIntervalIterator(subIterators);
  }

  private static class OrderedIntervalIterator implements IntervalIterator {

    final List<IntervalIterator> subIntervals;

    int start;
    int end;
    int innerWidth;
    int i;

    private OrderedIntervalIterator(List<IntervalIterator> subIntervals) {
      this.subIntervals = subIntervals;
    }

    @Override
    public int start() {
      return start;
    }

    @Override
    public int end() {
      return end;
    }

    @Override
    public int innerWidth() {
      return innerWidth;
    }

    @Override
    public boolean reset(int doc) throws IOException {
      boolean positioned = true;
      for (IntervalIterator it : subIntervals) {
        positioned &= it.reset(doc);
      }
      subIntervals.get(0).nextInterval();
      i = 1;
      start = end = innerWidth = Integer.MIN_VALUE;
      return positioned;
    }

    @Override
    public int nextInterval() throws IOException {
      start = end = NO_MORE_INTERVALS;
      int b = Integer.MAX_VALUE;
      while (true) {
        while (true) {
          if (subIntervals.get(i - 1).end() >= b)
            return start;
          if (i == subIntervals.size() || subIntervals.get(i).start() > subIntervals.get(i - 1).end())
            break;
          do {
            if (subIntervals.get(i).end() >= b || subIntervals.get(i).nextInterval() == NO_MORE_INTERVALS)
              return start;
          }
          while (subIntervals.get(i).start() <= subIntervals.get(i - 1).end());
          i++;
        }
        start = subIntervals.get(0).start();
        end = subIntervals.get(subIntervals.size() - 1).end();
        b = subIntervals.get(subIntervals.size() - 1).start();
        innerWidth = b - subIntervals.get(0).end() - 1;
        i = 1;
        if (subIntervals.get(0).nextInterval() == NO_MORE_INTERVALS)
          return start;
      }
    }
  }


  public static final IntervalFunction UNORDERED = new SingletonFunction("UNORDERED") {
    @Override
    public IntervalIterator apply(List<IntervalIterator> intervalIterators) {
      return unorderedIntervalIterator(intervalIterators);
    }
  };

  public static class UnorderedNearFunction extends IntervalFunction {

    final int minWidth;
    final int maxWidth;

    public UnorderedNearFunction(int minWidth, int maxWidth) {
      this.minWidth = minWidth;
      this.maxWidth = maxWidth;
    }

    @Override
    public IntervalIterator apply(List<IntervalIterator> intervalIterators) {
      return IntervalFilter.innerWidthFilter(unorderedIntervalIterator(intervalIterators), minWidth, maxWidth);
    }

    @Override
    public String toString() {
      return "ONEAR[" + minWidth + "/" + maxWidth + "]";
    }


    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      UnorderedNearFunction that = (UnorderedNearFunction) o;
      return minWidth == that.minWidth &&
          maxWidth == that.maxWidth;
    }

    @Override
    public int hashCode() {
      return Objects.hash(minWidth, maxWidth);
    }
  }

  public static IntervalIterator unorderedIntervalIterator(List<IntervalIterator> subIntervals) {
    for (IntervalIterator it : subIntervals) {
      if (it == IntervalIterator.EMPTY)
        return IntervalIterator.EMPTY;
    }
    return new UnorderedIntervalIterator(subIntervals);
  }

  private static class UnorderedIntervalIterator implements IntervalIterator {

    private final PriorityQueue<IntervalIterator> queue;
    private final IntervalIterator[] subIterators;

    int start, end, innerStart, innerEnd, queueEnd;

    UnorderedIntervalIterator(List<IntervalIterator> subIterators) {
      this.queue = new PriorityQueue<IntervalIterator>(subIterators.size()) {
        @Override
        protected boolean lessThan(IntervalIterator a, IntervalIterator b) {
          return a.start() < b.start() || (a.start() == b.start() && a.end() >= b.end());
        }
      };
      this.subIterators = new IntervalIterator[subIterators.size()];

      for (int i = 0; i < subIterators.size(); i++) {
        this.subIterators[i] = subIterators.get(i);
      }
    }

    @Override
    public int start() {
      return start;
    }

    @Override
    public int end() {
      return end;
    }

    @Override
    public int innerWidth() {
      return innerEnd - innerStart + 1;
    }

    @Override
    public boolean reset(int doc) throws IOException {
      this.queue.clear();
      this.queueEnd = start = end = innerEnd = innerStart = -1;
      boolean positioned = true;
      for (IntervalIterator subIterator : subIterators) {
        positioned &= subIterator.reset(doc);
        subIterator.nextInterval();
        queue.add(subIterator);
        if (subIterator.end() > queueEnd) {
          queueEnd = subIterator.end();
          innerEnd = subIterator.start();
        }
      }
      return positioned;
    }

    void updateRightExtreme(IntervalIterator it) {
      int itEnd = it.end();
      if (itEnd > queueEnd) {
        queueEnd = itEnd;
        innerEnd = it.start();
      }
    }

    @Override
    public int nextInterval() throws IOException {
      while (this.queue.size() == subIterators.length && queue.top().start() == start) {
        IntervalIterator it = queue.pop();
        if (it != null && it.nextInterval() != NO_MORE_INTERVALS) {
          queue.add(it);
          updateRightExtreme(it);
        }
      }
      if (this.queue.size() < subIterators.length)
        return NO_MORE_INTERVALS;
      do {
        start = queue.top().start();
        innerStart = queue.top().end();
        end = queueEnd;
        if (queue.top().end() == end)
          return start;
        IntervalIterator it = queue.pop();
        if (it != null && it.nextInterval() != NO_MORE_INTERVALS) {
          queue.add(it);
          updateRightExtreme(it);
        }
      } while (this.queue.size() == subIterators.length && end == queueEnd);
      return start;
    }

  }

  private static abstract class SingletonFunction extends IntervalFunction {

    private final String name;

    protected SingletonFunction(String name) {
      this.name = name;
    }

    @Override
    public int hashCode() {
      return System.identityHashCode(this);
    }

    @Override
    public boolean equals(Object obj) {
      return obj == this;
    }

    @Override
    public String toString() {
      return name;
    }

  }

}
