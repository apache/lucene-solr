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
package org.apache.lucene.facet.range;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * This implementation assumes the requested ranges <i>do not</i> overlap. With this assumption,
 * we're able to take a simpler approach to accumulating range counts by just binary searching for
 * the appropriate range and counting directly as each value comes in.
 */
class ExclusiveLongRangeCounter extends LongRangeCounter {

  /** elementary interval boundaries used for efficient counting (bsearch to find interval) */
  private final long[] boundaries;
  /** original range number each elementary interval corresponds to (index into countBuffer) */
  private final int[] rangeNums;
  /** number of counted documents that haven't matched any requested ranges */
  private int missingCount;
  /** whether-or-not the multi-valued doc currently being counted has matched any ranges */
  private boolean multiValuedDocMatchedRange;

  ExclusiveLongRangeCounter(LongRange[] ranges, int[] countBuffer) {
    super(countBuffer);

    // Create a copy of the requested ranges, sorted by min, and keeping track of the original
    // position:
    LongRangeAndPos[] sortedRanges = new LongRangeAndPos[ranges.length];
    for (int i = 0; i < ranges.length; i++) {
      sortedRanges[i] = new LongRangeAndPos(ranges[i], i);
    }
    Arrays.sort(sortedRanges, Comparator.comparingLong(r -> r.range.min));

    // Create elementary intervals, which include requested ranges and "gaps" in-between:
    List<InclusiveRange> elementaryIntervals = buildElementaryIntervals(sortedRanges);

    // Keep track of elementary interval boundary ends (for bsearching) along with the requested
    // range they map back to (and -1 when they map to a "gap" range):
    boundaries = new long[elementaryIntervals.size()];
    rangeNums = new int[elementaryIntervals.size()];
    Arrays.fill(rangeNums, -1);
    int currRange = 0;
    for (int i = 0; i < boundaries.length; i++) {
      boundaries[i] = elementaryIntervals.get(i).end;
      if (currRange < sortedRanges.length) {
        LongRangeAndPos curr = sortedRanges[currRange];
        if (boundaries[i] == curr.range.max) {
          rangeNums[i] = curr.pos;
          currRange++;
        }
      }
    }
  }

  @Override
  void startMultiValuedDoc() {
    super.startMultiValuedDoc();
    multiValuedDocMatchedRange = false;
  }

  @Override
  boolean endMultiValuedDoc() {
    return multiValuedDocMatchedRange;
  }

  @Override
  void addSingleValued(long v) {
    if (rangeCount() == 0) {
      missingCount++;
      return;
    }

    super.addSingleValued(v);
  }

  @Override
  int finish() {
    // Nothing much to do in this case since we're able to count directly into the requested
    // ranges as we go in this implementation. Just report any missing count:
    return missingCount;
  }

  @Override
  protected long[] boundaries() {
    return boundaries;
  }

  @Override
  protected void processSingleValuedHit(int elementaryIntervalNum) {
    int rangeNum = rangeNums[elementaryIntervalNum];
    if (rangeNum != -1) {
      // The elementary interval we matched against corresponds to a requested
      // range, so increment it:
      increment(rangeNum);
    } else {
      // The matched elementary interval is a "gap" range, so the doc isn't
      // present in any requested ranges:
      missingCount++;
    }
  }

  @Override
  protected void processMultiValuedHit(int elementaryIntervalNum) {
    int rangeNum = rangeNums[elementaryIntervalNum];
    if (rangeNum != -1) {
      // The elementary interval we matched against corresponds to a requested
      // range, so increment it. We can do this without fear of double-counting
      // since we know the requested ranges don't overlap:
      increment(rangeNum);
      multiValuedDocMatchedRange = true;
    }
  }

  /**
   * Create elementary intervals, which include requested ranges and "gaps" in-between. This logic
   * assumes no requested ranges overlap, and that the incoming ranges have already been sorted.
   */
  private static List<InclusiveRange> buildElementaryIntervals(LongRangeAndPos[] sortedRanges) {
    List<InclusiveRange> elementaryIntervals = new ArrayList<>();
    long prev = Long.MIN_VALUE;
    for (LongRangeAndPos range : sortedRanges) {
      if (range.range.min > prev) {
        // add a "gap" range preceding requested range if necessary:
        elementaryIntervals.add(new InclusiveRange(prev, range.range.min - 1));
      }
      // add the requested range:
      elementaryIntervals.add(new InclusiveRange(range.range.min, range.range.max));
      prev = range.range.max + 1;
    }
    if (elementaryIntervals.isEmpty() == false) {
      long lastEnd = elementaryIntervals.get(elementaryIntervals.size() - 1).end;
      if (lastEnd < Long.MAX_VALUE) {
        elementaryIntervals.add(new InclusiveRange(lastEnd + 1, Long.MAX_VALUE));
      }
    } else {
      // If no ranges were requested, create a single entry from MIN_VALUE to MAX_VALUE:
      elementaryIntervals.add(new InclusiveRange(Long.MIN_VALUE, Long.MAX_VALUE));
    }

    return elementaryIntervals;
  }

  /** Simple container for a requested range and its original position */
  private static final class LongRangeAndPos {
    final LongRange range;
    final int pos;

    LongRangeAndPos(LongRange range, int pos) {
      this.range = range;
      this.pos = pos;
    }
  }
}
