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
import java.util.Objects;

/**
 * A function that takes two interval iterators and combines them to produce a third,
 * generally by computing a difference interval between them
 */
abstract class DifferenceIntervalFunction {

  @Override
  public abstract int hashCode();

  @Override
  public abstract boolean equals(Object obj);

  @Override
  public abstract String toString();

  /**
   * Combine two interval iterators into a third
   */
  public abstract IntervalIterator apply(IntervalIterator minuend, IntervalIterator subtrahend);

  /**
   * Filters the minuend iterator so that only intervals that do not overlap intervals from the
   * subtrahend iterator are returned
   */
  static final DifferenceIntervalFunction NON_OVERLAPPING = new SingletonFunction("NON_OVERLAPPING") {
    @Override
    public IntervalIterator apply(IntervalIterator minuend, IntervalIterator subtrahend) {
      return new NonOverlappingIterator(minuend, subtrahend);
    }
  };

  /**
   * Filters the minuend iterator so that only intervals that do not contain intervals from the
   * subtrahend iterator are returned
   */
  static final DifferenceIntervalFunction NOT_CONTAINING = new SingletonFunction("NOT_CONTAINING") {
    @Override
    public IntervalIterator apply(IntervalIterator minuend, IntervalIterator subtrahend) {
      return new NotContainingIterator(minuend, subtrahend);
    }
  };

  /**
   * Filters the minuend iterator so that only intervals that are not contained by intervals from
   * the subtrahend iterator are returned
   */
  static final DifferenceIntervalFunction NOT_CONTAINED_BY = new SingletonFunction("NOT_CONTAINED_BY") {
    @Override
    public IntervalIterator apply(IntervalIterator minuend, IntervalIterator subtrahend) {
      return new NotContainedByIterator(minuend, subtrahend);
    }
  };

  private static abstract class RelativeIterator extends IntervalIterator {

    final IntervalIterator a;
    final IntervalIterator b;

    boolean bpos;

    RelativeIterator(IntervalIterator a, IntervalIterator b) {
      this.a = a;
      this.b = b;
    }

    @Override
    public int docID() {
      return a.docID();
    }

    @Override
    public int nextDoc() throws IOException {
      int doc = a.nextDoc();
      reset();
      return doc;
    }

    @Override
    public int advance(int target) throws IOException {
      int doc = a.advance(target);
      reset();
      return doc;
    }

    @Override
    public long cost() {
      return a.cost();
    }

    protected void reset() throws IOException {
      int doc = a.docID();
      bpos = b.docID() == doc ||
          (b.docID() < doc && b.advance(doc) == doc);
    }

    @Override
    public int start() {
      return a.start();
    }

    @Override
    public int end() {
      return a.end();
    }

    @Override
    public float matchCost() {
      return a.matchCost() + b.matchCost();
    }
  }

  private static class NonOverlappingIterator extends RelativeIterator {

    private NonOverlappingIterator(IntervalIterator minuend, IntervalIterator subtrahend) {
      super(minuend, subtrahend);
    }

    @Override
    public int nextInterval() throws IOException {
      if (bpos == false)
        return a.nextInterval();
      while (a.nextInterval() != NO_MORE_INTERVALS) {
        while (b.end() < a.start()) {
          if (b.nextInterval() == NO_MORE_INTERVALS) {
            bpos = false;
            return a.start();
          }
        }
        if (b.start() > a.end())
          return a.start();
      }
      return NO_MORE_INTERVALS;
    }
  }

  /**
   * Filters the minuend iterator so that only intervals that do not occur within a set number
   * of positions of intervals from the subtrahend iterator are returned
   */
  static class NotWithinFunction extends DifferenceIntervalFunction {

    private final int positions;

    NotWithinFunction(int positions) {
      this.positions = positions;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      NotWithinFunction that = (NotWithinFunction) o;
      return positions == that.positions;
    }

    @Override
    public String toString() {
      return "NOTWITHIN/" + positions;
    }

    @Override
    public int hashCode() {
      return Objects.hash(positions);
    }

    @Override
    public IntervalIterator apply(IntervalIterator minuend, IntervalIterator subtrahend) {
      IntervalIterator notWithin = new IntervalIterator() {

        @Override
        public int docID() {
          return subtrahend.docID();
        }

        @Override
        public int nextDoc() throws IOException {
          positioned = false;
          return subtrahend.nextDoc();
        }

        @Override
        public int advance(int target) throws IOException {
          positioned = false;
          return subtrahend.advance(target);
        }

        @Override
        public long cost() {
          return subtrahend.cost();
        }

        boolean positioned = false;

        @Override
        public int start() {
          if (positioned == false)
            return -1;
          int start = subtrahend.start();
          return Math.max(0, start - positions);
        }

        @Override
        public int end() {
          if (positioned == false)
            return -1;
          int end = subtrahend.end();
          int newEnd = end + positions;
          if (newEnd < 0) // check for overflow
            return Integer.MAX_VALUE;
          return newEnd;
        }

        @Override
        public int nextInterval() throws IOException {
          if (positioned == false) {
            positioned = true;
          }
          return subtrahend.nextInterval();
        }

        @Override
        public float matchCost() {
          return subtrahend.matchCost();
        }

      };
      return NON_OVERLAPPING.apply(minuend, notWithin);
    }
  }

  private static class NotContainingIterator extends RelativeIterator {

    private NotContainingIterator(IntervalIterator minuend, IntervalIterator subtrahend) {
      super(minuend, subtrahend);
    }

    @Override
    public int nextInterval() throws IOException {
      if (bpos == false)
        return a.nextInterval();
      while (a.nextInterval() != NO_MORE_INTERVALS) {
        while (b.start() < a.start() && b.end() < a.end()) {
          if (b.nextInterval() == NO_MORE_INTERVALS) {
            bpos = false;
            return a.start();
          }
        }
        if (b.start() > a.end())
          return a.start();
      }
      return NO_MORE_INTERVALS;
    }

  }

  private static class NotContainedByIterator extends RelativeIterator {

    NotContainedByIterator(IntervalIterator a, IntervalIterator b) {
      super(a, b);
    }

    @Override
    public int nextInterval() throws IOException {
      if (bpos == false)
        return a.nextInterval();
      while (a.nextInterval() != NO_MORE_INTERVALS) {
        while (b.end() < a.end()) {
          if (b.nextInterval() == NO_MORE_INTERVALS)
            return a.start();
        }
        if (a.start() < b.start())
          return a.start();
      }
      return NO_MORE_INTERVALS;
    }
  }

  private static abstract class SingletonFunction extends DifferenceIntervalFunction {

    private final String name;

    SingletonFunction(String name) {
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
