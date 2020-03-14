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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;

/**
 * <p>MaxScoreTerminator is used by TopFieldCollector when the query sort is a prefix of the index sort (in which case
 * we can apply early termination), multiple threads are used for collection, and <code>numHits</code> is relatively
 * high. It is notified periodically by leaf collectors calling {@link #updateLeafState} with their worst (ie maximum)
 * score and how many hits they have collected.  When enough hits are collected, MaxScoreTerminator notifies
 * noncompetitive leaf collectors when they can stop (early terminate) by returning true from {@link
 * #updateLeafState}.</p>
 *
 * <p>Used by TopFieldCollector to orchestrate early termination (when query sort matches index sort) based on the worst
 * competitive score across all leaf collectors. Once we have globally collected the number of hits required to satisfy
 * the query (<code>collectionThreshold</code>, below, typically <code>max(numHits, 1000)</code>) then the worst
 * collected score across threads is a global lower bound on the score that must be met by any hit (we already have
 * sufficient hits to satisfy the query with score better than that, so any later hit with a worse score will be
 * discarded, and any Collector retrieving such a hit can be terminated). Further, once a collector terminates, the same
 * logic applies to the remaining collectors, which can result in raising the bound further. The termination bound used
 * is the minimum minimum score among the top collectors (ranked by their minimum scores) that together have at least
 * <code>collectionThreshold</code> hits.</p>
 *
 * <p>MaxScoreTerminator implements this termination strategy by tracking the worst score, and number of hits collected,
 * of each LeafCollector in {@link LeafState} objects that are shared with the leaf collectors, and updated by the
 * collectors asynchronously. The leaf collectors check in periodically (every {@link #interval}th hit collected), at
 * which time the leaf states are sorted, the termination bound is updated, and the collector is notified whether it can
 * terminate. The interval must be a power of 2 (since a mask is used to check the period), and should be set as low as
 * possible while avoiding thread contention on this synchronized method. A good heuristic seems to be the least power
 * of 2 greater than than the number of threads in use.</p>
 */
class MaxScoreTerminator {

  // This strategy performs best for higher N. The threshold below was determined empirically. The best tradeoff is
  // probably related to number of threads used by the searcher and the overall counting threshold.
  private static final int TERMINATION_STRATEGY_HIT_THRESHOLD = 50;

  // By default we use 2^5-1 to check the remainder with a bitwise operation, but for best performance
  // the actual value should always be set by calling setIntervalBits()
  private static final int DEFAULT_INTERVAL_BITS = 5;
  private int interval;
  int intervalMask;

  /** The worst score for each leaf */
  private final List<LeafState> leafStates;
  private final List<LeafState> scratch;

  /** How many hits were requested: Collector's numHits. */
  private final int numHits;

  /** the worst hit over all */
  private final LeafState thresholdState;

  /** The total number of docs to collect: the max of the Collector's numHits and its early termination threshold. */
  final int totalToCollect;

  /** An upper bound on the number of docs "excluded" from max-score accounting due to early termination. */
  private int numExcludedBound;

  /** A lower bound on the total hits collected by all leaves */
  private volatile int totalCollected;

  /**
   * @param numHits the number of hits to be returned
   * @param collectionThreshold collect at least this many, even if numHits is less, for the sake of counting
   */
  MaxScoreTerminator(int numHits, int collectionThreshold) {
    leafStates = new ArrayList<>();
    scratch = new ArrayList<>();
    this.numHits = numHits;
    this.totalToCollect = Math.max(numHits, collectionThreshold);
    setIntervalBits(DEFAULT_INTERVAL_BITS);
    thresholdState = new LeafState();
    thresholdState.score = Double.MAX_VALUE;
    thresholdState.docid = Integer.MAX_VALUE;
  }

  /**
   * @param sort the query sort
   * @param numHits the number of hits requested
   * @param collectionThreshold the number of hits to count
   * @return whether the Sort is compatible with early termination using {@link MaxScoreTerminator}, which requires
   * extracting a numeric score for each hit. Currently we only handle numeric fields, but in principle this could be
   * extended to handle some other field types, so long as their comparator-status can ultimately be encoded as a
   * numeric value.
   */
  static MaxScoreTerminator createIfApplicable(Sort sort, int numHits, int collectionThreshold) {
    if (numHits > TERMINATION_STRATEGY_HIT_THRESHOLD && sortIsNumeric(sort)) {
      return new MaxScoreTerminator(numHits, collectionThreshold);
    } else {
      return null;
    }
  }

  private static boolean sortIsNumeric(Sort sort) {
    switch (sort.getSort()[0].getType()) {
      case DOC:
      case INT:
      case FLOAT:
      case LONG:
      case DOUBLE:
        return true;
      default:
        return false;
    }
  }

  // for testing
  int getTotalCollected() {
    return totalCollected;
  }

  synchronized LeafState addLeafState() {
    LeafState newLeafState = new LeafState();
    newLeafState.index = leafStates.size();
    leafStates.add(newLeafState);
    scratch.add(new LeafState());
    return newLeafState;
  }

  /**
   * Set the update interval to 2^bitCount and the intervalMask to 2^bitCount-1. This controls the
   * rate at which multiple threads report their worst scores. For best performance this should be
   * set to the nearest power of 2 &gt; the number of expected calling threads.
   * @param bitCount the number of bits in the interval/intervalMask
   */
  void setIntervalBits(int bitCount) {
    interval = 1 << bitCount;
    intervalMask = interval - 1;
  }

  /**
   * Must be called by leaf collectors on every interval hits to update their progress; they should
   * call when ((leafCollector.numCollected &amp; this.intervalMask) == 0).
   * @param newLeafState the leaf collector's current lowest score
   * @return whether the collector should terminate
   */
  synchronized boolean updateLeafState(LeafState newLeafState) {
    if (newLeafState.isActive()) {
      // Only count hits from this leaf if it is still active: otherwise its hits are not competitive and do not count
      // towards meeting the threshold for terminating the remaining leaves
      totalCollected += interval;
    }
    // (1) Until we collect totalToCollect we can't terminate anything.
    // (2) after that, any single leaf that has collected numHits can be terminated,
    // (3) and we may be able to remove the worst leaves even if they have not yet collected numHits, as long as the
    // remaining leaves have collected totalToCollect.
    if (totalCollected >= totalToCollect) {
      if (newLeafState.resultCount >= numHits) {
        // This leaf has collected enough hits all on its own
        return true;
      }
      // We may be ready to terminate some leaves (although they may not include the one that just called
      // updateLeafState(), such leaves will eventually be terminated here).
      excludeSuperfluousLeaves();
      if (newLeafState.compareTo(thresholdState) <= 0) {
        // Tell the current leaf collector to terminate since it can no longer contribute any top hits
        //System.out.println("  terminate " + newLeafState + " b/c it is > " + thresholdState);
        return true;
      }
    }
    return false;
  }

  private void excludeSuperfluousLeaves() {
    if (leafStates.size() > 1 && totalCollected >= numHits + numExcludedBound) {
      // Copy the current states since they are being updated concurrently and we need stable values
      PriorityQueue<LeafState> queue = new PriorityQueue<>();
      for (LeafState leafState : leafStates) {
        if (leafState.isActive()) {
          LeafState copy = scratch.get(leafState.index);
          copy.updateFrom(leafState);
          queue.add(copy);
        }
      }
      if (queue.size() < 2) {
        return;
      }
      LeafState worstLeafState;
      do {
        worstLeafState = queue.remove();
        //System.out.println(" exclude " + worstLeafState + " because " + totalCollected + " >= " + numHits + "+" + numExcludedBound);
        // We don't need the worst leaf in order to get enough results, so remember how many results
        // (upper bound) it accounted for, setting a new threshold for hits collected
        numExcludedBound += worstLeafState.resultCount;
        leafStates.get(worstLeafState.index).deactivate();
      } while (queue.size() > 1 && totalCollected >= numHits + numExcludedBound);
      // And update the score threshold if the worst leaf remaining after exclusion has a lower max score
      if (worstLeafState.compareTo(thresholdState) > 0) {
        //System.out.println("  new threshold: " + worstLeafState);
        thresholdState.updateFrom(worstLeafState);
      }
    }
  }

  /**
   * For tracking the worst scoring hit and total number of hits collected by each leaf
   */
  class LeafState implements Comparable<LeafState> {

    int docid = -1;             // the global docid
    double score = -Double.MAX_VALUE;
    int resultCount;
    int index;
    boolean active = true;

    void update(double score, int docid) {
      // scores are nondecreasing
      assert score > this.score || (score == this.score && docid >= this.docid) :
        "descending (score,docid): (" + score + "," + docid + ") < (" + this.score + "," + this.docid + ")";
      this.score = score;
      this.docid = docid;
      resultCount += 1;
    }

    void updateFrom(LeafState other) {
      this.score = other.score;
      this.docid = other.docid;
      this.resultCount = other.resultCount;
      this.index = other.index;
    }

    boolean isActive() {
      return active;
    }

    void deactivate() {
      active = false;
    }

    @Override
    public int compareTo(LeafState o) {
      int cmp = Double.compare(score, o.score);
      if (cmp == 0) {
        // maybe we do not need this since we only terminate when scores become strictly worse (not >=)
        cmp = Integer.compare(docid, o.docid);
      }
      return -cmp;
    }

    @Override
    public String toString() {
      return "LeafState<" + index + " docid=" + docid + " score=" + score + ", count=" + resultCount + ">";
    }

  }

}
