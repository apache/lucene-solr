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

import java.util.Arrays;
import java.util.Comparator;

/**
 * Counter for numeric ranges. Works for both single- and multi-valued cases (assuming you use it
 * correctly).
 *
 * <p>Usage notes: When counting a document field that only has a single value, callers should call
 * addSingleValued() with the value. Whenever a document field has multiple values, callers should
 * call startMultiValuedDoc() at the beginning of processing the document, followed by
 * addMultiValued() with each value before finally calling endMultiValuedDoc() at the end of
 * processing the document. The call to endMultiValuedDoc() will respond with a boolean indicating
 * whether-or-not the specific document matched against at least one of the ranges being counted.
 * Finally, after processing all documents, the caller should call finish(). This final call will
 * ensure the contents of the user-provided {@code countBuffer} contains accurate counts (each index
 * corresponding to the provided {@code LongRange} in {@code ranges}). The final call to finish()
 * will also report how many additional documents did not match against any ranges. The combination
 * of the endMultiValuedDoc() boolean responses and the number reported by finish() communicates the
 * total number of missing documents. Note that the call to finish() will not report any documents
 * already reported missing by endMultiValuedDoc().
 */
abstract class LongRangeCounter {

  /** accumulated counts for all of the ranges */
  private final int[] countBuffer;

  /**
   * for multi-value docs, we keep track of the last elementary interval we've counted so we can use
   * that as a lower-bound when counting subsequent values. this takes advantage of the fact that
   * values within a given doc are sorted.
   */
  protected int multiValuedDocLastSeenElementaryInterval;

  static LongRangeCounter create(LongRange[] ranges, int[] countBuffer) {
    if (hasOverlappingRanges(ranges)) {
      return new OverlappingLongRangeCounter(ranges, countBuffer);
    } else {
      return new ExclusiveLongRangeCounter(ranges, countBuffer);
    }
  }

  protected LongRangeCounter(int[] countBuffer) {
    // We'll populate the user-provided count buffer with range counts:
    this.countBuffer = countBuffer;
  }

  /** Start processing a new doc. It's unnecessary to call this for single-value cases. */
  void startMultiValuedDoc() {
    multiValuedDocLastSeenElementaryInterval = -1;
  }

  /**
   * Finish processing a new doc. Returns whether-or-not the document contributed a count to at
   * least one range. It's unnecessary to call this for single-value cases.
   */
  abstract boolean endMultiValuedDoc();

  /** Count a single valued doc */
  void addSingleValued(long v) {

    // NOTE: this works too, but it's ~6% slower on a simple
    // test with a high-freq TermQuery w/ range faceting on
    // wikimediumall:
    /*
    int index = Arrays.binarySearch(boundaries, v);
    if (index < 0) {
      index = -index-1;
    }
    leafCounts[index]++;
    */

    // Binary search to find matched elementary range; we
    // are guaranteed to find a match because the last
    // boundary is Long.MAX_VALUE:

    long[] boundaries = boundaries();

    int lo = 0;
    int hi = boundaries.length - 1;
    while (true) {
      int mid = (lo + hi) >>> 1;
      if (v <= boundaries[mid]) {
        if (mid == 0) {
          processSingleValuedHit(mid);
          return;
        } else {
          hi = mid - 1;
        }
      } else if (v > boundaries[mid + 1]) {
        lo = mid + 1;
      } else {
        processSingleValuedHit(mid + 1);
        return;
      }
    }
  }

  /** Count a multi-valued doc value */
  void addMultiValued(long v) {

    if (rangeCount() == 0) {
      return; // don't bother if there aren't any requested ranges
    }

    long[] boundaries = boundaries();

    // First check if we've "advanced" beyond the last elementary interval we counted for this doc.
    // If we haven't, there's no sense doing anything else:
    if (multiValuedDocLastSeenElementaryInterval != -1
        && v <= boundaries[multiValuedDocLastSeenElementaryInterval]) {
      return;
    }

    // Also check if we've already counted the last elementary interval. If so, there's nothing
    // else to count for this doc:
    final int nextCandidateElementaryInterval = multiValuedDocLastSeenElementaryInterval + 1;
    if (nextCandidateElementaryInterval == boundaries.length) {
      return;
    }

    // Binary search in the range of the next candidate interval up to the last interval:
    int lo = nextCandidateElementaryInterval;
    int hi = boundaries.length - 1;
    while (true) {
      int mid = (lo + hi) >>> 1;
      if (v <= boundaries[mid]) {
        if (mid == nextCandidateElementaryInterval) {
          processMultiValuedHit(mid);
          multiValuedDocLastSeenElementaryInterval = mid;
          return;
        } else {
          hi = mid - 1;
        }
      } else if (v > boundaries[mid + 1]) {
        lo = mid + 1;
      } else {
        int idx = mid + 1;
        processMultiValuedHit(idx);
        multiValuedDocLastSeenElementaryInterval = idx;
        return;
      }
    }
  }

  /**
   * Finish processing all documents. This will return the number of docs that didn't contribute to
   * any ranges (that weren't already reported when calling endMultiValuedDoc()).
   */
  abstract int finish();

  /** Provide boundary information for elementary intervals (max inclusive value per interval) */
  protected abstract long[] boundaries();

  /** Process a single-value "hit" against an elementary interval. */
  protected abstract void processSingleValuedHit(int elementaryIntervalNum);

  /** Process a multi-value "hit" against an elementary interval. */
  protected abstract void processMultiValuedHit(int elementaryIntervalNum);

  /** Increment the specified range by one. */
  protected final void increment(int rangeNum) {
    countBuffer[rangeNum]++;
  }

  /** Increment the specified range by the specified count. */
  protected final void increment(int rangeNum, int count) {
    countBuffer[rangeNum] += count;
  }

  /** Number of ranges requested by the caller. */
  protected final int rangeCount() {
    return countBuffer.length;
  }

  /** Determine whether-or-not any requested ranges overlap */
  private static boolean hasOverlappingRanges(LongRange[] ranges) {
    if (ranges.length == 0) {
      return false;
    }

    // Copy before sorting so we don't mess with the caller's original ranges:
    LongRange[] sortedRanges = new LongRange[ranges.length];
    System.arraycopy(ranges, 0, sortedRanges, 0, ranges.length);
    Arrays.sort(sortedRanges, Comparator.comparingLong(r -> r.min));

    long previousMax = sortedRanges[0].max;
    for (int i = 1; i < sortedRanges.length; i++) {
      // Ranges overlap if the next min is <= the previous max (note that LongRange models
      // closed ranges, so equal limit points are considered overlapping):
      if (sortedRanges[i].min <= previousMax) {
        return true;
      }
      previousMax = sortedRanges[i].max;
    }

    return false;
  }

  protected static final class InclusiveRange {
    final long start;
    final long end;

    InclusiveRange(long start, long end) {
      assert end >= start;
      this.start = start;
      this.end = end;
    }

    @Override
    public String toString() {
      return start + " to " + end;
    }
  }
}
