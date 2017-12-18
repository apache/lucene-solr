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

import org.apache.lucene.index.PostingsEnum;

public final class Intervals {

  public static final int NO_MORE_INTERVALS = Integer.MAX_VALUE;

  public static IntervalIterator widthFilter(IntervalIterator in, int minWidth, int maxWidth) {
    return new IntervalFilter(in) {
      @Override
      protected boolean accept() {
        int width = end() - start();
        return width >= minWidth && width <= maxWidth;
      }
    };
  }

  public static IntervalIterator innerWidthFilter(IntervalIterator in, int minWidth, int maxWidth) {
    return new IntervalFilter(in) {
      @Override
      protected boolean accept() {
        int width = innerWidth();
        return width >= minWidth && width <= maxWidth;
      }
    };
  }

  public static IntervalIterator termIterator(PostingsEnum pe) {
    return new TermIntervalIterator(pe);
  }

  private static class TermIntervalIterator implements IntervalIterator {

    public TermIntervalIterator(PostingsEnum pe) {
      this.pe = pe;
    }

    private final PostingsEnum pe;

    int upTo = -1;
    int pos = -1;

    @Override
    public DocIdSetIterator approximation() {
      return pe;
    }

    @Override
    public int start() {
      return pos;
    }

    @Override
    public int end() {
      return pos;
    }

    @Override
    public int innerWidth() {
      return 0;
    }

    @Override
    public void advanceTo(int doc) throws IOException {
      pos = -1;
      if (pe.docID() == doc || (pe.docID() < doc && pe.advance(doc) == doc)) {
        upTo = pe.freq();
      }
      else {
        upTo = -1;
      }
    }

    @Override
    public int nextInterval() throws IOException {
      if (upTo <= 0) {
        return pos = NO_MORE_INTERVALS;
      }
      upTo--;
      return pos = pe.nextPosition();
    }

    @Override
    public String toString() {
      return pe.docID() + "[" + pos + "]";
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
    final DocIdSetIterator approximation;

    int start;
    int end;
    int innerWidth;
    int i;

    private OrderedIntervalIterator(List<IntervalIterator> subIntervals) {
      this.subIntervals = subIntervals;
      this.approximation = ConjunctionDISI.intersectIntervals(subIntervals);
    }

    @Override
    public DocIdSetIterator approximation() {
      return approximation;
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
    public void advanceTo(int doc) throws IOException {
      for (IntervalIterator it : subIntervals) {
        it.advanceTo(doc);
      }
      subIntervals.get(0).nextInterval();
      i = 1;
      start = end = innerWidth = Integer.MIN_VALUE;
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

}
