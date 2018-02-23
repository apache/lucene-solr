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
import java.util.Objects;

import static org.apache.lucene.search.Intervals.NO_MORE_INTERVALS;

public abstract class IntervalDifferenceFunction {

  @Override
  public abstract int hashCode();

  @Override
  public abstract boolean equals(Object obj);

  @Override
  public abstract String toString();

  public abstract IntervalIterator apply(IntervalIterator minuend, IntervalIterator subtrahend);

  public static final IntervalDifferenceFunction NON_OVERLAPPING = new SingletonFunction("NON_OVERLAPPING") {
    @Override
    public IntervalIterator apply(IntervalIterator minuend, IntervalIterator subtrahend) {
      return nonOverlapping(minuend, subtrahend);
    }
  };

  public static IntervalIterator nonOverlapping(IntervalIterator minuend, IntervalIterator subtrahend) {
    return new NonOverlappingIterator(minuend, subtrahend);
  }

  private static class NonOverlappingIterator implements IntervalIterator {

    final IntervalIterator minuend;
    final IntervalIterator subtrahend;
    boolean subPositioned;

    private NonOverlappingIterator(IntervalIterator minuend, IntervalIterator subtrahend) {
      this.minuend = minuend;
      this.subtrahend = subtrahend;
    }

    @Override
    public int start() {
      return minuend.start();
    }

    @Override
    public int end() {
      return minuend.end();
    }

    @Override
    public int innerWidth() {
      return minuend.innerWidth();
    }

    @Override
    public boolean reset(int doc) throws IOException {
      subPositioned = subtrahend.reset(doc);
      if (subPositioned)
        subPositioned = subtrahend.nextInterval() != NO_MORE_INTERVALS;
      return minuend.reset(doc);
    }

    @Override
    public int nextInterval() throws IOException {
      if (subPositioned == false)
        return minuend.nextInterval();
      while (minuend.nextInterval() != NO_MORE_INTERVALS) {
        while (subtrahend.end() < minuend.start()) {
          if (subtrahend.nextInterval() == NO_MORE_INTERVALS) {
            subPositioned = false;
            return minuend.start();
          }
        }
        if (subtrahend.start() > minuend.end())
          return minuend.start();
      }
      return NO_MORE_INTERVALS;
    }
  }

  public static class NotWithinFunction extends IntervalDifferenceFunction {

    private final int positions;

    public NotWithinFunction(int positions) {
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
        public int start() {
          int start = subtrahend.start();
          return Math.max(0, start - positions);
        }

        @Override
        public int end() {
          int end = subtrahend.end();
          int newEnd = end + positions;
          if (newEnd < 0) // check for overflow
            return Integer.MAX_VALUE;
          return newEnd;
        }

        @Override
        public int innerWidth() {
          throw new UnsupportedOperationException();
        }

        @Override
        public boolean reset(int doc) throws IOException {
          return subtrahend.reset(doc);
        }

        @Override
        public int nextInterval() throws IOException {
          return subtrahend.nextInterval();
        }
      };
      return NON_OVERLAPPING.apply(minuend, notWithin);
    }
  }

  private static abstract class SingletonFunction extends IntervalDifferenceFunction {

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
