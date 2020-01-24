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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Counts how many times each range was seen;
 *  per-hit it's just a binary search ({@link #add})
 *  against the elementary intervals, and in the end we
 *  rollup back to the original ranges. */

final class LongRangeCounter {

  final LongRangeNode root;
  final long[] boundaries;
  final int[] leafCounts;

  // Used during rollup
  private int leafUpto;
  private int missingCount;

  public LongRangeCounter(LongRange[] ranges) {
    // Maps all range inclusive endpoints to int flags; 1
    // = start of interval, 2 = end of interval.  We need to
    // track the start vs end case separately because if a
    // given point is both, then it must be its own
    // elementary interval:
    Map<Long,Integer> endsMap = new HashMap<>();

    endsMap.put(Long.MIN_VALUE, 1);
    endsMap.put(Long.MAX_VALUE, 2);

    for(LongRange range : ranges) {
      Integer cur = endsMap.get(range.min);
      if (cur == null) {
        endsMap.put(range.min, 1);
      } else {
        endsMap.put(range.min, cur.intValue() | 1);
      }
      cur = endsMap.get(range.max);
      if (cur == null) {
        endsMap.put(range.max, 2);
      } else {
        endsMap.put(range.max, cur.intValue() | 2);
      }
    }

    List<Long> endsList = new ArrayList<>(endsMap.keySet());
    Collections.sort(endsList);

    // Build elementaryIntervals (a 1D Venn diagram):
    List<InclusiveRange> elementaryIntervals = new ArrayList<>();
    int upto0 = 1;
    long v = endsList.get(0);
    long prev;
    if (endsMap.get(v) == 3) {
      elementaryIntervals.add(new InclusiveRange(v, v));
      prev = v+1;
    } else {
      prev = v;
    }

    while (upto0 < endsList.size()) {
      v = endsList.get(upto0);
      int flags = endsMap.get(v);
      //System.out.println("  v=" + v + " flags=" + flags);
      if (flags == 3) {
        // This point is both an end and a start; we need to
        // separate it:
        if (v > prev) {
          elementaryIntervals.add(new InclusiveRange(prev, v-1));
        }
        elementaryIntervals.add(new InclusiveRange(v, v));
        prev = v+1;
      } else if (flags == 1) {
        // This point is only the start of an interval;
        // attach it to next interval:
        if (v > prev) {
          elementaryIntervals.add(new InclusiveRange(prev, v-1));
        }
        prev = v;
      } else {
        assert flags == 2;
        // This point is only the end of an interval; attach
        // it to last interval:
        elementaryIntervals.add(new InclusiveRange(prev, v));
        prev = v+1;
      }
      //System.out.println("    ints=" + elementaryIntervals);
      upto0++;
    }

    // Build binary tree on top of intervals:
    root = split(0, elementaryIntervals.size(), elementaryIntervals);

    // Set outputs, so we know which range to output for
    // each node in the tree:
    for(int i=0;i<ranges.length;i++) {
      root.addOutputs(i, ranges[i]);
    }

    // Set boundaries (ends of each elementary interval):
    boundaries = new long[elementaryIntervals.size()];
    for(int i=0;i<boundaries.length;i++) {
      boundaries[i] = elementaryIntervals.get(i).end;
    }

    leafCounts = new int[boundaries.length];

    //System.out.println("ranges: " + Arrays.toString(ranges));
    //System.out.println("intervals: " + elementaryIntervals);
    //System.out.println("boundaries: " + Arrays.toString(boundaries));
    //System.out.println("root:\n" + root);
  }

  public void add(long v) {
    //System.out.println("add v=" + v);

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

    int lo = 0;
    int hi = boundaries.length - 1;
    while (true) {
      int mid = (lo + hi) >>> 1;
      //System.out.println("  cycle lo=" + lo + " hi=" + hi + " mid=" + mid + " boundary=" + boundaries[mid] + " to " + boundaries[mid+1]);
      if (v <= boundaries[mid]) {
        if (mid == 0) {
          leafCounts[0]++;
          return;
        } else {
          hi = mid - 1;
        }
      } else if (v > boundaries[mid+1]) {
        lo = mid + 1;
      } else {
        leafCounts[mid+1]++;
        //System.out.println("  incr @ " + (mid+1) + "; now " + leafCounts[mid+1]);
        return;
      }
    }
  }

  /** Fills counts corresponding to the original input
   *  ranges, returning the missing count (how many hits
   *  didn't match any ranges). */
  public int fillCounts(int[] counts) {
    //System.out.println("  rollup");
    missingCount = 0;
    leafUpto = 0;
    rollup(root, counts, false);
    return missingCount;
  }

  private int rollup(LongRangeNode node, int[] counts, boolean sawOutputs) {
    int count;
    sawOutputs |= node.outputs != null;
    if (node.left != null) {
      count = rollup(node.left, counts, sawOutputs);
      count += rollup(node.right, counts, sawOutputs);
    } else {
      // Leaf:
      count = leafCounts[leafUpto];
      leafUpto++;
      if (!sawOutputs) {
        // This is a missing count (no output ranges were
        // seen "above" us):
        missingCount += count;
      }
    }
    if (node.outputs != null) {
      for(int rangeIndex : node.outputs) {
        counts[rangeIndex] += count;
      }
    }
    //System.out.println("  rollup node=" + node.start + " to " + node.end + ": count=" + count);
    return count;
  }

  private static LongRangeNode split(int start, int end, List<InclusiveRange> elementaryIntervals) {
    if (start == end-1) {
      // leaf
      InclusiveRange range = elementaryIntervals.get(start);
      return new LongRangeNode(range.start, range.end, null, null, start);
    } else {
      int mid = (start + end) >>> 1;
      LongRangeNode left = split(start, mid, elementaryIntervals);
      LongRangeNode right = split(mid, end, elementaryIntervals);
      return new LongRangeNode(left.start, right.end, left, right, -1);
    }
  }

  private static final class InclusiveRange {
    public final long start;
    public final long end;

    public InclusiveRange(long start, long end) {
      assert end >= start;
      this.start = start;
      this.end = end;
    }

    @Override
    public String toString() {
      return start + " to " + end;
    }
  }

  /** Holds one node of the segment tree. */
  public static final class LongRangeNode {
    final LongRangeNode left;
    final LongRangeNode right;

    // Our range, inclusive:
    final long start;
    final long end;

    // If we are a leaf, the index into elementary ranges that
    // we point to:
    final int leafIndex;

    // Which range indices to output when a query goes
    // through this node:
    List<Integer> outputs;

    public LongRangeNode(long start, long end, LongRangeNode left, LongRangeNode right, int leafIndex) {
      this.start = start;
      this.end = end;
      this.left = left;
      this.right = right;
      this.leafIndex = leafIndex;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      toString(sb, 0);
      return sb.toString();
    }

    static void indent(StringBuilder sb, int depth) {
      for(int i=0;i<depth;i++) {
        sb.append("  ");
      }
    }

    /** Recursively assigns range outputs to each node. */
    void addOutputs(int index, LongRange range) {
      if (start >= range.min && end <= range.max) {
        // Our range is fully included in the incoming
        // range; add to our output list:
        if (outputs == null) {
          outputs = new ArrayList<>();
        }
        outputs.add(index);
      } else if (left != null) {
        assert right != null;
        // Recurse:
        left.addOutputs(index, range);
        right.addOutputs(index, range);
      }
    }

    void toString(StringBuilder sb, int depth) {
      indent(sb, depth);
      if (left == null) {
        assert right == null;
        sb.append("leaf: ").append(start).append(" to ").append(end);
      } else {
        sb.append("node: ").append(start).append(" to ").append(end);
      }
      if (outputs != null) {
        sb.append(" outputs=");
        sb.append(outputs);
      }
      sb.append('\n');

      if (left != null) {
        assert right != null;
        left.toString(sb, depth+1);
        right.toString(sb, depth+1);
      }
    }
  }
}
