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
import org.apache.lucene.search.Scorer;

import java.io.IOException;
import java.util.Arrays;

/**
 * An IntervalIterator implementing minimum interval semantics for the
 * BLOCK operator
 *
 * See <a href=
 * "http://vigna.dsi.unimi.it/ftp/papers/EfficientAlgorithmsMinimalIntervalSemantics"
 * >"Efficient Optimally Lazy Algorithms for Minimal-Interval Semantic</a>
 *
 * @lucene.experimental
 */
public final class BlockIntervalIterator extends IntervalIterator {
  private final IntervalIterator[] iterators;

  private static final Interval INFINITE_INTERVAL = new Interval(
      Integer.MIN_VALUE, Integer.MIN_VALUE, -1, -1);
  private final Interval[] intervals;
  private final Interval interval = new Interval(
      Integer.MIN_VALUE, Integer.MIN_VALUE, -1, -1);
  private final int[] gaps;

  private final int lastIter;

  /**
   * Construct a BlockIntervalIterator over a compound IntervalIterator.  The
   * subiterators must be in order and sequential for a match.
   * @param collectIntervals true if intervals will be collected
   * @param other the compound IntervalIterator
   */
  public BlockIntervalIterator(boolean collectIntervals, IntervalIterator other) {
    this(collectIntervals, defaultIncrements(other.subs(true).length), other);
  }

  /**
   * Construct a BlockIntervalIterator over a compound IntervalIterator using
   * a supplied increments array.
   * @param collectIntervals
   * @param increments
   * @param other
   */
  public BlockIntervalIterator(boolean collectIntervals, int[] increments, IntervalIterator other) {
    super(other.getScorer(), collectIntervals);
    assert other.subs(true) != null;
    iterators = other.subs(true);
    assert iterators.length > 1;
    intervals = new Interval[iterators.length];
    lastIter = iterators.length - 1;
    this.gaps = increments;
  }

  /**
   * Construct a BlockIntervalIterator over a set of subiterators using a supplied
   * increments array
   * @param scorer the parent Scorer
   * @param increments an array of position increments between the iterators
   * @param collectIntervals true if intervals will be collected
   * @param iterators the subiterators
   */
  public BlockIntervalIterator(Scorer scorer, int[] increments, boolean collectIntervals,
                               IntervalIterator... iterators) {
    super(scorer, collectIntervals);
    assert iterators.length > 1;
    this.iterators = iterators;
    intervals = new Interval[iterators.length];
    lastIter = iterators.length - 1;
    this.gaps = increments;
  }

  /**
   * Construct a BlockIntervalIterator over a set of subiterators
   * @param scorer the parent Scorer
   * @param collectIntervals true if intervals will be collected
   * @param iterators the subiterators
   */
  public BlockIntervalIterator(Scorer scorer, boolean collectIntervals, IntervalIterator... iterators) {
    this(scorer, defaultIncrements(iterators.length), collectIntervals, iterators);
  }

  private static int[] defaultIncrements(int num) {
    int[] gaps = new int[num];
    Arrays.fill(gaps, 1);
    return gaps;
  }

  @Override
  public Interval next() throws IOException {
    if ((intervals[0] = iterators[0].next()) == null) {
      return null;
    }
    int offset = 0;
    for (int i = 1; i < iterators.length;) {
      final int gap = gaps[i];
      while (intervals[i].begin + gap <= intervals[i - 1].end) {
        if ((intervals[i] = iterators[i].next()) == null) {
          return null;
        }
      }
      offset += gap;
      if (intervals[i].begin == intervals[i - 1].end + gaps[i]) {
        i++;
        if (i < iterators.length && intervals[i] == INFINITE_INTERVAL) {
          // advance only if really necessary
          iterators[i].scorerAdvanced(docID());
          assert iterators[i].docID() == docID();
        }
      } else {
        do {
          if ((intervals[0] = iterators[0].next()) == null) {
            return null;
          }
        } while (intervals[0].begin < intervals[i].end - offset);

        i = 1;
      }
    }
    interval.begin = intervals[0].begin;
    interval.end = intervals[lastIter].end;
    interval.offsetBegin = intervals[0].offsetBegin;
    interval.offsetEnd = intervals[lastIter].offsetEnd;
    return interval;
  }

  @Override
  public IntervalIterator[] subs(boolean inOrder) {
    return iterators;
  }

  @Override
  public void collect(IntervalCollector collector) {
    assert collectIntervals;
    collector.collectComposite(scorer, interval, docID());
    for (IntervalIterator iter : iterators) {
      iter.collect(collector);
    }
  }

  @Override
  public int scorerAdvanced(int docId) throws IOException {
    iterators[0].scorerAdvanced(docId);
    assert iterators[0].docID() == docId;
    iterators[1].scorerAdvanced(docId);
    assert iterators[1].docID() == docId;
    Arrays.fill(intervals, INFINITE_INTERVAL);
    return docId;
  }

  @Override
  public int matchDistance() {
    return intervals[lastIter].begin - intervals[0].end;
  }
}
