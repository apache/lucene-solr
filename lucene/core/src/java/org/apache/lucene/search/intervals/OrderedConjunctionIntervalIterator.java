package org.apache.lucene.search.intervals;

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

/**
 * An IntervalIterator based on minimum interval semantics for the
 * AND< operator
 *
 * See <a href=
 * "http://vigna.dsi.unimi.it/ftp/papers/EfficientAlgorithmsMinimalIntervalSemantics"
 * >"Efficient Optimally Lazy Algorithms for Minimal-Interval Semantics"</a>
 *
 * @lucene.experimental
 */
public final class OrderedConjunctionIntervalIterator extends
    IntervalIterator {

  private final IntervalIterator[] iterators;
  private final Interval[] intervals;
  private final int lastIter;
  private final Interval interval = new Interval();

  private int index = 1;
  private int matchDistance = 0;

  private SnapshotPositionCollector snapshot = null;
  private boolean collectLeaves = true;

  /**
   * Create an OrderedConjunctionIntervalIterator over a composite IntervalIterator
   * @param collectIntervals true if intervals will be collected
   * @param other a composite IntervalIterator to wrap
   */
  public OrderedConjunctionIntervalIterator(boolean collectIntervals, boolean collectLeaves, IntervalIterator other) {
    this(other.scorer, collectIntervals, other.subs(true));
    this.collectLeaves = collectLeaves;
  }

  public OrderedConjunctionIntervalIterator(boolean collectIntervals, IntervalIterator other) {
    this(collectIntervals, true, other);
  }

  /**
   * Create an OrderedConjunctionIntervalIterator over a set of subiterators
   * @param scorer the parent Scorer
   * @param collectIntervals true if intervals will be collected
   * @param iterators the subintervals to wrap
   */
  public OrderedConjunctionIntervalIterator(Scorer scorer, boolean collectIntervals, IntervalIterator... iterators) {
    super(scorer, collectIntervals);
    this.iterators = iterators;
    assert iterators.length > 1;
    intervals = new Interval[iterators.length];
    lastIter = iterators.length - 1;
  }

  @Override
  public Interval next() throws IOException {
    if(intervals[0] == null) {
      return null;
    }
    interval.setMaximum();
    int b = Integer.MAX_VALUE;
    while (true) {
      while (true) {
        final Interval previous = intervals[index - 1];
        if (previous.end >= b) {
          return interval.begin == Integer.MAX_VALUE ? null : interval;
        }
        if (index == intervals.length || intervals[index].begin > previous.end) {
          break;
        }
        Interval current = intervals[index];
        do {
          final Interval next;
          if (current.end >= b || (next = iterators[index].next()) == null) {
            return interval.begin == Integer.MAX_VALUE ? null : interval;
          }
          current = intervals[index] = next;
        } while (current.begin <= previous.end);
        index++;
      }
      interval.update(intervals[0], intervals[lastIter]);
      matchDistance = (intervals[lastIter].begin - lastIter) - intervals[0].end;
      b = intervals[lastIter].begin;
      index = 1;
      if (collectIntervals)
        snapshotSubPositions();
      intervals[0] = iterators[0].next();
      if (intervals[0] == null) {
        return interval.begin == Integer.MAX_VALUE ? null : interval;
      }
    }
  }

  @Override
  public IntervalIterator[] subs(boolean inOrder) {
    return iterators;
  }

  @Override
  public void collect(IntervalCollector collector) {
    assert collectIntervals;
    if (snapshot == null) {
      // we might not be initialized if the first interval matches
      collectInternal(collector);
    } else {
      snapshot.replay(collector);
    }
  }

  private void snapshotSubPositions() {
    if (snapshot == null) {
      snapshot = new SnapshotPositionCollector(iterators.length);
    }
    snapshot.reset();
    collectInternal(snapshot);
  }

  private void collectInternal(IntervalCollector collector) {
    assert collectIntervals;
    collector.collectComposite(scorer, interval, docID());
    if (collectLeaves) {
      for (IntervalIterator iter : iterators) {
        iter.collect(collector);
      }
    }
  }

  @Override
  public int scorerAdvanced(int docId) throws IOException {
    assert scorer.docID() == docId;
    for (int i = 0; i < iterators.length; i++) {
      int advanceTo = iterators[i].scorerAdvanced(docId);
      assert advanceTo == docId;
      intervals[i] = Interval.INFINITE_INTERVAL;
    }
    intervals[0] = iterators[0].next();
    index = 1;
    return scorer.docID();
  }

  @Override
  public int matchDistance() {
    return matchDistance;
  }

}
