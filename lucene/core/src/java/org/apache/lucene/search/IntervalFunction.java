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
import java.util.List;
import java.util.Objects;

import org.apache.lucene.util.PriorityQueue;

/**
 * Combine a list of {@link IntervalIterator}s into another
 */
public abstract class IntervalFunction {

  @Override
  public abstract int hashCode();

  @Override
  public abstract boolean equals(Object obj);

  @Override
  public abstract String toString();

  /**
   * Combine the iterators into another iterator
   */
  public abstract IntervalIterator apply(List<IntervalIterator> iterators);

  public static final IntervalFunction BLOCK = new SingletonFunction("BLOCK") {
    @Override
    public IntervalIterator apply(List<IntervalIterator> iterators) {
      return new BlockIntervalIterator(iterators);
    }
  };

  private static class BlockIntervalIterator extends ConjunctionIntervalIterator {

    int start, end;

    BlockIntervalIterator(List<IntervalIterator> subIterators) {
      super(subIterators);
    }

    @Override
    public void reset() throws IOException {
      for (IntervalIterator it : subIterators) {
        it.reset();
      }
      start = end = -1;
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
    public float score() {
      return 1;
    }

    @Override
    public int nextInterval() throws IOException {
      if (subIterators.get(0).nextInterval() == NO_MORE_INTERVALS)
        return NO_MORE_INTERVALS;
      int i = 1;
      while (i < subIterators.size()) {
        while (subIterators.get(i).start() <= subIterators.get(i - 1).end()) {
          if (subIterators.get(i).nextInterval() == NO_MORE_INTERVALS)
            return NO_MORE_INTERVALS;
        }
        if (subIterators.get(i).start() == subIterators.get(i - 1).end() + 1) {
          i = i + 1;
        }
        else {
          if (subIterators.get(0).nextInterval() == NO_MORE_INTERVALS)
            return NO_MORE_INTERVALS;
          i = 1;
        }
      }
      start = subIterators.get(0).start();
      end = subIterators.get(subIterators.size() - 1).end();
      return start;
    }
  }

  /**
   * Return an iterator over intervals where the subiterators appear in a given order
   */
  public static final IntervalFunction ORDERED = new SingletonFunction("ORDERED") {
    @Override
    public IntervalIterator apply(List<IntervalIterator> intervalIterators) {
      return new OrderedIntervalIterator(intervalIterators);
    }
  };

  private static class OrderedIntervalIterator extends ConjunctionIntervalIterator {

    int start;
    int end;
    int i;

    private OrderedIntervalIterator(List<IntervalIterator> subIntervals) {
      super(subIntervals);
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
    public void reset() throws IOException {
      for (IntervalIterator it : subIterators) {
        it.reset();
      }
      subIterators.get(0).nextInterval();
      i = 1;
      start = end = -1;
    }

    @Override
    public int nextInterval() throws IOException {
      start = end = NO_MORE_INTERVALS;
      int b = Integer.MAX_VALUE;
      while (true) {
        while (true) {
          if (subIterators.get(i - 1).end() >= b)
            return start;
          if (i == subIterators.size() || subIterators.get(i).start() > subIterators.get(i - 1).end())
            break;
          do {
            if (subIterators.get(i).end() >= b || subIterators.get(i).nextInterval() == NO_MORE_INTERVALS)
              return start;
          }
          while (subIterators.get(i).start() <= subIterators.get(i - 1).end());
          i++;
        }
        start = subIterators.get(0).start();
        end = subIterators.get(subIterators.size() - 1).end();
        b = subIterators.get(subIterators.size() - 1).start();
        i = 1;
        if (subIterators.get(0).nextInterval() == NO_MORE_INTERVALS)
          return start;
      }
    }
  }

  /**
   * Return an iterator over intervals where the subiterators appear in any order
   */
  public static final IntervalFunction UNORDERED = new SingletonFunction("UNORDERED") {
    @Override
    public IntervalIterator apply(List<IntervalIterator> intervalIterators) {
      return new UnorderedIntervalIterator(intervalIterators);
    }
  };

  private static class UnorderedIntervalIterator extends ConjunctionIntervalIterator {

    private final PriorityQueue<IntervalIterator> queue;
    private final IntervalIterator[] subIterators;

    int start, end, queueEnd;

    UnorderedIntervalIterator(List<IntervalIterator> subIterators) {
      super(subIterators);
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
    public void reset() throws IOException {
      this.queue.clear();
      this.queueEnd = start = end = -1;
      for (IntervalIterator subIterator : subIterators) {
        subIterator.reset();
        subIterator.nextInterval();
        queue.add(subIterator);
        if (subIterator.end() > queueEnd) {
          queueEnd = subIterator.end();
        }
      }
    }

    void updateRightExtreme(IntervalIterator it) {
      int itEnd = it.end();
      if (itEnd > queueEnd) {
        queueEnd = itEnd;
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

  /**
   * Returns an interval over iterators where the first iterator contains intervals from the second
   */
  public static final IntervalFunction CONTAINING = new SingletonFunction("CONTAINING") {
    @Override
    public IntervalIterator apply(List<IntervalIterator> iterators) {
      if (iterators.size() != 2)
        throw new IllegalStateException("CONTAINING function requires two iterators");
      IntervalIterator a = iterators.get(0);
      IntervalIterator b = iterators.get(1);
      return new ConjunctionIntervalIterator(iterators) {

        boolean bpos;

        @Override
        public int start() {
          return a.start();
        }

        @Override
        public int end() {
          return a.end();
        }

        @Override
        public void reset() throws IOException {
          a.reset();
          b.reset();
          bpos = true;
        }

        @Override
        public int nextInterval() throws IOException {
          if (bpos == false)
            return NO_MORE_INTERVALS;
          while (a.nextInterval() != NO_MORE_INTERVALS) {
            while (b.start() < a.start() && b.end() < a.end()) {
              if (b.nextInterval() == NO_MORE_INTERVALS)
                return NO_MORE_INTERVALS;
            }
            if (a.start() <= b.start() && a.end() >= b.end())
              return a.start();
          }
          return NO_MORE_INTERVALS;
        }
      };
    }
  };

  /**
   * Return an iterator over intervals where the first iterator is contained by intervals from the second
   */
  public static final IntervalFunction CONTAINED_BY = new SingletonFunction("CONTAINED_BY") {
    @Override
    public IntervalIterator apply(List<IntervalIterator> iterators) {
      if (iterators.size() != 2)
        throw new IllegalStateException("CONTAINED_BY function requires two iterators");
      IntervalIterator a = iterators.get(0);
      IntervalIterator b = iterators.get(1);
      return new ConjunctionIntervalIterator(iterators) {

        boolean bpos;

        @Override
        public int start() {
          return a.start();
        }

        @Override
        public int end() {
          return a.end();
        }

        @Override
        public void reset() throws IOException {
          a.reset();
          b.reset();
          bpos = true;
        }

        @Override
        public int nextInterval() throws IOException {
          if (bpos == false)
            return NO_MORE_INTERVALS;
          while (a.nextInterval() != NO_MORE_INTERVALS) {
            while (b.end() < a.end()) {
              if (b.nextInterval() == NO_MORE_INTERVALS)
                return NO_MORE_INTERVALS;
            }
            if (b.start() <= a.start())
              return a.start();
          }
          return NO_MORE_INTERVALS;
        }
      };
    }
  };

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
