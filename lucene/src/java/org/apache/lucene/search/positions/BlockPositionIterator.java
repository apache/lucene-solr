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

public class BlockPositionIterator extends PositionIntervalIterator {
  private final PositionIntervalIterator[] iterators;
  private static final PositionInterval INFINITE_INTERVAL = new PositionInterval(
      Integer.MIN_VALUE, Integer.MIN_VALUE);
  private final PositionInterval[] intervals;
  private int docID = -1;
  private final PositionInterval interval =  new PositionInterval(
      Integer.MIN_VALUE, Integer.MIN_VALUE);
  
  private final int lastIter;
  
  public BlockPositionIterator(PositionIntervalIterator other) {
    super(other.getScorer());
    assert other.subs(true) != null;
    iterators = other.subs(true);
    assert iterators.length > 1;
    intervals = new PositionInterval[iterators.length];
    lastIter = iterators.length-1;
  }

  public BlockPositionIterator(Scorer scorer, Scorer... subScorers)
      throws IOException {
    super(scorer);
    assert subScorers.length > 1;
    iterators = new PositionIntervalIterator[subScorers.length];
    intervals = new PositionInterval[subScorers.length];
    for (int i = 0; i < subScorers.length; i++) {
      iterators[i] = subScorers[i].positions();
      assert iterators[i] != null;
    }
    lastIter = iterators.length-1;
  }

  @Override
  public PositionInterval next() throws IOException {
    int currentDocID = scorer.docID();
    if (currentDocID != docID) {
      Arrays.fill(intervals, INFINITE_INTERVAL);
      docID = currentDocID;
    }
    if (advance()) {
      interval.begin = intervals[0].begin;
      interval.end = intervals[lastIter].end;
      return interval;
    }
    return null;
  }

  private boolean advance() throws IOException {
    intervals[0] = iterators[0].next();
    if (intervals[0] == null) {
      return false;
    }
    int i = 1;
    final int len = iterators.length;
    while (i < len ) {
      while (intervals[i].begin <= intervals[i-1].end) {
        intervals[i] = iterators[i].next();
        if (intervals[i] == null) {
          return false;
        }
      }
      if (intervals[i].begin == intervals[i-1].end+1) {
        i++;
      } else {
        intervals[0] = iterators[0].next();
        if (intervals[0] == null) {
          return false;
        }
        i = 1;
      }
    }
    return true;
  }

  @Override
  public PositionIntervalIterator[] subs(boolean inOrder) {
    return iterators;
  }
}
