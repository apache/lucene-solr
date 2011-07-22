package org.apache.lucene.search.positions;

/**
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
import java.util.Arrays;

import org.apache.lucene.search.Scorer;

/**
 * 
 * @lucene.experimental
 */
// nocommit - javadoc
public final class BlockPositionIterator extends PositionIntervalIterator {
  private final PositionIntervalIterator[] iterators;

  private static final PositionInterval INFINITE_INTERVAL = new PositionInterval(
      Integer.MIN_VALUE, Integer.MIN_VALUE);
  private final PositionInterval[] intervals;
  private final PositionInterval interval = new PositionInterval(
      Integer.MIN_VALUE, Integer.MIN_VALUE);
  private final int[] gaps;

  private final int lastIter;

  public BlockPositionIterator(PositionIntervalIterator other) {
    this(other, defaultGaps(other.subs(true).length));
  }

  public BlockPositionIterator(PositionIntervalIterator other, int[] gaps) {
    super(other.getScorer());
    assert other.subs(true) != null;
    iterators = other.subs(true);
    assert iterators.length > 1;
    intervals = new PositionInterval[iterators.length];
    lastIter = iterators.length - 1;
    this.gaps = gaps;
  }

  public BlockPositionIterator(Scorer scorer, Scorer... subScorers)
      throws IOException {
    this(scorer, defaultGaps(subScorers.length), subScorers);
  }

  private static int[] defaultGaps(int num) {
    int[] gaps = new int[num];
    Arrays.fill(gaps, 1);
    return gaps;
  }

  public BlockPositionIterator(Scorer scorer, int[] gaps, Scorer... subScorers)
      throws IOException {
    super(scorer);
    assert subScorers.length > 1;
    iterators = new PositionIntervalIterator[subScorers.length];
    intervals = new PositionInterval[subScorers.length];
    for (int i = 0; i < subScorers.length; i++) {
      iterators[i] = subScorers[i].positions();
      assert iterators[i] != null;
    }
    lastIter = iterators.length - 1;
    this.gaps = gaps;
  }

  @Override
  public PositionInterval next() throws IOException {
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
          iterators[i].advanceTo(currentDoc);
          assert iterators[i].docID() == currentDoc;
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
    return interval;
  }

  @Override
  public PositionIntervalIterator[] subs(boolean inOrder) {
    return iterators;
  }

  @Override
  public void collect() {
    collector.collectComposite(scorer, interval, currentDoc);
    for (PositionIntervalIterator iter : iterators) {
      iter.collect();
    }
  }

  @Override
  public int advanceTo(int docId) throws IOException {
    iterators[0].advanceTo(docId);
    assert iterators[0].docID() == docId;
    iterators[1].advanceTo(docId);
    assert iterators[1].docID() == docId;
    Arrays.fill(intervals, INFINITE_INTERVAL);
    return currentDoc = docId;
  }
}
