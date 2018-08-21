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

package org.apache.lucene.search.intervals;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.util.PriorityQueue;

/**
 * Combine a list of {@link IntervalIterator}s into another
 */
abstract class IntervalFunction {

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

  static final IntervalFunction BLOCK = new SingletonFunction("BLOCK") {
    @Override
    public IntervalIterator apply(List<IntervalIterator> iterators) {
      return new BlockIntervalIterator(iterators);
    }
  };

  private static class BlockIntervalIterator extends ConjunctionIntervalIterator {

    int start = -1, end = -1;

    BlockIntervalIterator(List<IntervalIterator> subIterators) {
      super(subIterators);
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
    public int nextInterval() throws IOException {
      if (subIterators.get(0).nextInterval() == IntervalIterator.NO_MORE_INTERVALS)
        return IntervalIterator.NO_MORE_INTERVALS;
      int i = 1;
      while (i < subIterators.size()) {
        while (subIterators.get(i).start() <= subIterators.get(i - 1).end()) {
          if (subIterators.get(i).nextInterval() == IntervalIterator.NO_MORE_INTERVALS)
            return IntervalIterator.NO_MORE_INTERVALS;
        }
        if (subIterators.get(i).start() == subIterators.get(i - 1).end() + 1) {
          i = i + 1;
        }
        else {
          if (subIterators.get(0).nextInterval() == IntervalIterator.NO_MORE_INTERVALS)
            return IntervalIterator.NO_MORE_INTERVALS;
          i = 1;
        }
      }
      start = subIterators.get(0).start();
      end = subIterators.get(subIterators.size() - 1).end();
      return start;
    }

    @Override
    protected void reset() {
      start = end = -1;
    }
  }

  /**
   * Return an iterator over intervals where the subiterators appear in a given order
   */
  static final IntervalFunction ORDERED = new SingletonFunction("ORDERED") {
    @Override
    public IntervalIterator apply(List<IntervalIterator> intervalIterators) {
      return new OrderedIntervalIterator(intervalIterators);
    }
  };

  private static class OrderedIntervalIterator extends ConjunctionIntervalIterator {

    int start = -1, end = -1, i;

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
    public int nextInterval() throws IOException {
      start = end = IntervalIterator.NO_MORE_INTERVALS;
      int b = Integer.MAX_VALUE;
      i = 1;
      while (true) {
        while (true) {
          if (subIterators.get(i - 1).end() >= b)
            return start;
          if (i == subIterators.size() || subIterators.get(i).start() > subIterators.get(i - 1).end())
            break;
          do {
            if (subIterators.get(i).end() >= b || subIterators.get(i).nextInterval() == IntervalIterator.NO_MORE_INTERVALS)
              return start;
          }
          while (subIterators.get(i).start() <= subIterators.get(i - 1).end());
          i++;
        }
        start = subIterators.get(0).start();
        end = subIterators.get(subIterators.size() - 1).end();
        b = subIterators.get(subIterators.size() - 1).start();
        i = 1;
        if (subIterators.get(0).nextInterval() == IntervalIterator.NO_MORE_INTERVALS)
          return start;
      }
    }

    @Override
    protected void reset() throws IOException {
      subIterators.get(0).nextInterval();
      i = 1;
      start = end = -1;
    }
  }

  /**
   * Return an iterator over intervals where the subiterators appear in any order
   */
  static final IntervalFunction UNORDERED = new SingletonFunction("UNORDERED") {
    @Override
    public IntervalIterator apply(List<IntervalIterator> intervalIterators) {
      return new UnorderedIntervalIterator(intervalIterators, true);
    }
  };

  /**
   * Return an iterator over intervals where the subiterators appear in any order, and do not overlap
   */
  static final IntervalFunction UNORDERED_NO_OVERLAP = new SingletonFunction("UNORDERED_NO_OVERLAP") {
    @Override
    public IntervalIterator apply(List<IntervalIterator> iterators) {
      return new UnorderedIntervalIterator(iterators, false);
    }
  };

  private static class UnorderedIntervalIterator extends ConjunctionIntervalIterator {

    private final PriorityQueue<IntervalIterator> queue;
    private final IntervalIterator[] subIterators;
    private final boolean allowOverlaps;

    int start = -1, end = -1, queueEnd;

    UnorderedIntervalIterator(List<IntervalIterator> subIterators, boolean allowOverlaps) {
      super(subIterators);
      this.queue = new PriorityQueue<IntervalIterator>(subIterators.size()) {
        @Override
        protected boolean lessThan(IntervalIterator a, IntervalIterator b) {
          return a.start() < b.start() || (a.start() == b.start() && a.end() >= b.end());
        }
      };
      this.subIterators = new IntervalIterator[subIterators.size()];
      this.allowOverlaps = allowOverlaps;

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

    void updateRightExtreme(IntervalIterator it) {
      int itEnd = it.end();
      if (itEnd > queueEnd) {
        queueEnd = itEnd;
      }
    }

    @Override
    public int nextInterval() throws IOException {
      // first, find a matching interval
      while (this.queue.size() == subIterators.length && queue.top().start() == start) {
        IntervalIterator it = queue.pop();
        if (it != null && it.nextInterval() != IntervalIterator.NO_MORE_INTERVALS) {
          if (allowOverlaps == false) {
            while (hasOverlaps(it)) {
              if (it.nextInterval() == IntervalIterator.NO_MORE_INTERVALS)
                return IntervalIterator.NO_MORE_INTERVALS;
            }
          }
          queue.add(it);
          updateRightExtreme(it);
        }
      }
      if (this.queue.size() < subIterators.length)
        return IntervalIterator.NO_MORE_INTERVALS;
      // then, minimize it
      do {
        start = queue.top().start();
        end = queueEnd;
        if (queue.top().end() == end)
          return start;
        IntervalIterator it = queue.pop();
        if (it != null && it.nextInterval() != IntervalIterator.NO_MORE_INTERVALS) {
          if (allowOverlaps == false) {
            while (hasOverlaps(it)) {
              if (it.nextInterval() == IntervalIterator.NO_MORE_INTERVALS) {
                return start;
              }
            }
          }
          queue.add(it);
          updateRightExtreme(it);
        }
      } while (this.queue.size() == subIterators.length && end == queueEnd);
      return start;
    }

    @Override
    protected void reset() throws IOException {
      queueEnd = start = end = -1;
      this.queue.clear();
      loop: for (IntervalIterator it : subIterators) {
        if (it.nextInterval() == NO_MORE_INTERVALS) {
          break;
        }
        if (allowOverlaps == false) {
          while (hasOverlaps(it)) {
            if (it.nextInterval() == NO_MORE_INTERVALS) {
              break loop;
            }
          }
        }
        queue.add(it);
        updateRightExtreme(it);
      }
    }

    private boolean hasOverlaps(IntervalIterator candidate) {
      for (IntervalIterator it : queue) {
        if (it.start() < candidate.start()) {
          if (it.end() >= candidate.start()) {
            return true;
          }
          continue;
        }
        if (it.start() == candidate.start()) {
          return true;
        }
        if (it.start() <= candidate.end()) {
          return true;
        }
      }
      return false;
    }

  }

  /**
   * Returns an interval over iterators where the first iterator contains intervals from the second
   */
  static final IntervalFunction CONTAINING = new SingletonFunction("CONTAINING") {
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
        public int nextInterval() throws IOException {
          if (bpos == false)
            return IntervalIterator.NO_MORE_INTERVALS;
          while (a.nextInterval() != IntervalIterator.NO_MORE_INTERVALS) {
            while (b.start() < a.start() && b.end() < a.end()) {
              if (b.nextInterval() == IntervalIterator.NO_MORE_INTERVALS)
                return IntervalIterator.NO_MORE_INTERVALS;
            }
            if (a.start() <= b.start() && a.end() >= b.end())
              return a.start();
          }
          return IntervalIterator.NO_MORE_INTERVALS;
        }

        @Override
        protected void reset() throws IOException {
          bpos = true;
        }
      };
    }
  };

  /**
   * Return an iterator over intervals where the first iterator is contained by intervals from the second
   */
  static final IntervalFunction CONTAINED_BY = new SingletonFunction("CONTAINED_BY") {
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
        public int nextInterval() throws IOException {
          if (bpos == false)
            return IntervalIterator.NO_MORE_INTERVALS;
          while (a.nextInterval() != IntervalIterator.NO_MORE_INTERVALS) {
            while (b.end() < a.end()) {
              if (b.nextInterval() == IntervalIterator.NO_MORE_INTERVALS)
                return IntervalIterator.NO_MORE_INTERVALS;
            }
            if (b.start() <= a.start())
              return a.start();
          }
          return IntervalIterator.NO_MORE_INTERVALS;
        }

        @Override
        protected void reset() throws IOException {
          bpos = true;
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
