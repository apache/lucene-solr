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

/**
 * MaxScoreTerminator is used by TopFieldCollector when the query sort is a prefix of the index sort
 * (in which case we can apply early termination), and multiple threads are used for collection. It
 * is notified periodically by leaf collectors calling {@link #updateLeafState} with their worst (ie
 * maximum) score and how many hits they have collected.  When enough hits are collected,
 * MaxScoreTerminator notifies noncompetitive leaf collectors when they can stop (early terminate)
 * by returning true from {@link #updateLeafState}.
 * 
 * <p>At any moment, N leaves have reported their counts of documents collected; documents are
 * collected in score order, so these counts represent the best for each leaf. And we also track
 * the scores of the lowest-scoring (most recently collected) document in each leaf.</p>
 *
 * <p>Once the total number of documents collected reaches the requested total (totalToCollect), the
 * worst-scoring leaf can no longer contribute any documents to the results, so it can be
 * terminated, and any leaves whose scores rise above that worst score are also no longer
 * competitive and can be terminated too. If we kept a global priority queue we could update the
 * global maximum competitive score, and use that as a termination threshold, but assuming this to
 * be too costly due to thread contention, we seek to more cheaply update an upper bound on the
 * worst score.  Specifically, when a leaf is terminated, if the remaining leaves together have
 * collected >= totalToCollect, we know that there are enough hits with scores at least as good as
 * their worst score, so we can update the current upper bound to the max of *their* max scores.</p>
 *
 * <p> In practice this leads to a good bound on the number of documents collected, which tend to
 * exceed totalToCollect by a small factor.  When the documents are evenly distributed among N
 * segments, we expect to collect approximately (N+1/N) * totalToCollect documents. In a worst case,
 * where *all* the best documents are in a single segment, we expect to collect something like O(log
 * N) ie (1/N + 1/N-1 + ... + 1) * totalToCollect documents, which is still much better than the N *
 * totalToCollect we would collect with a naive strategy, but does leave room for improvement.</p>
 */
class MaxScoreTerminator {

  // By default we use 2^5-1 to check the remainder with a bitwise operation, but for best performance
  // the actual value should always be set by calling setIntervalBits()
  private static final int DEFAULT_INTERVAL_BITS = 5;
  private int interval;
  int intervalMask;

  /** The worst score for each leaf */
  private final List<LeafState> leafStates;

  /** The total number of docs to collect: the max of the Collector's numHits and its early
   * termination threshold. */
  final int totalToCollect;

  /** An upper bound on the number of docs "excluded" from max-score accounting due to early termination. */
  private int numExcludedBound;

  /** A lower bound on the total hits collected by all leaves */
  private int totalCollected;

  /** the worst hit over all */
  private LeafState worstLeafState;

  MaxScoreTerminator(int totalToCollect) {
    leafStates = new ArrayList<>();
    this.totalToCollect = totalToCollect;
    setIntervalBits(DEFAULT_INTERVAL_BITS);
  }

  // for testing
  int getTotalCollected() {
    return totalCollected;
  }

  synchronized LeafState addLeafState() {
    LeafState newLeafState = new LeafState();
    leafStates.add(newLeafState);
    if (worstLeafState == null) {
      worstLeafState = newLeafState;
    }
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
    totalCollected += interval;
//    System.out.println(" scoreboard totalCollected = " + totalCollected + "/" + totalToCollect + " "
//        + newLeafState + " ? " + worstLeafState + ":" + newLeafState.compareTo(worstLeafState));
    if (newLeafState.compareTo(worstLeafState) >= 0 || newLeafState.resultCount >= totalToCollect) {
      worstLeafState = newLeafState;
      if (totalCollected >= totalToCollect) {
        // in this case, we have enough scores: remove the worst leaves
        updateWorstHit();
        // and tell the current leaf collector to terminate since it can
        // no longer contribute any top hits
        return true;
      }
    }
    return false;
  }

  private void updateWorstHit() {
    //System.out.println("scoreboard updateWorstHit");
    if (leafStates.size() > 1) {
      Collections.sort(leafStates);
      while (leafStates.size() > 1) {
        LeafState worst = leafStates.get(leafStates.size() - 1);
        if (totalCollected - numExcludedBound - worst.resultCount >= totalToCollect) {
          //System.out.println(" and remove");
          // We don't need the worst leaf in order to get enough results, so
          // remember how many results (upper bound) it accounted for
          numExcludedBound += worst.resultCount;
          // and remove it from the list of worstHits
          leafStates.remove(leafStates.size() - 1);
        } else {
          break;
        }
      }
      // and update the worstHit with the worst one that remains after exclusion
      worstLeafState = leafStates.get(leafStates.size() - 1);
    }
  }

  /**
   * For tracking the worst scoring hit and total number of hits collected by each leaf
   */
  class LeafState implements Comparable<LeafState> {

    private double worstScore = -Double.MAX_VALUE;;
    int resultCount;

    void update(double score) {
      assert score >= this.worstScore : "descending score: " + score + " < " + this.worstScore;
      this.worstScore = score;
      ++this.resultCount;
    }

    @Override
    public int compareTo(LeafState o) {
      return Double.compare(worstScore, o.worstScore);
    }

    @Override
    public String toString() {
      return "LeafState<score=" + worstScore + ", count=" + resultCount + ">";
    }

  }

}
