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
 * MaxScoreTerminator is notified periodically by leaf collectors calling {@link #update}
 * with their worst (ie maximum) score
 * and how many hits they have collected.  When enough hits are collected, Scoreboard notifies
 * noncompetitive leaf collectors when they can stop (early terminate) by returning true from
 * its {@link #update} method.
 * 
 * <p>At any moment, N leaves have reported their counts of documents collected; documents are
 * collected in score order, so these counts represent the best for each leaf. And we also track
 * the scores of the lowest-scoring (most recently collected) document in each leaf.</p>
 *
 * <p>Once the total number of documents collected reaches the requested total (numHits),
 * the worst-scoring leaf can no longer contribute any documents to the results, so it can be
 * terminated, and any leaves whose scores rise above that worst score is no longer competitive and
 * can also be terminated. If we kept a global priority queue we could update the global maximum
 * competitive score, and use that as a termination threshold, but assuming this to be too costly
 * due to thread contention, we seek to more cheaply update an upper bound on the worst score.
 * Specifically, when a leaf is terminated, if the remaining leaves together have collected >= numHits,
 * then we can update the maximum to the max of *their* max scores, excluding the terminated leaf's max
 * from consideration.</p>
 *
 * <p> In practice this leads to a good bound on the number of documents collected, which tend to
 * exceed numHits by a small factor.  When the documents are evenly distributed among N segments,
 * we expect to collect approximately (N+1/N) * numHits documents. In a worst case, where *all*
 * the best documents are in a single segment, we expect to collect something O(log N) ie
 * (1/N + 1/N-1 + ... + 1) * numHits documents, which is still much better than
 * the N * numHits we would collect with a naive strategy.
 * </p>
 */
class MaxScoreTerminator {

  // we use 2^5-1 to check the remainder with a bitwise operation
  // private static final int DEFAULT_INTERVAL_BITS = 10;
  private static final int DEFAULT_INTERVAL_BITS = 2;
  private int interval;
  int intervalMask;

  /** The worst score for each leaf */
  private final List<LeafState> leafStates;

  /** The total number of docs to collect: from the Collector's numHits */
  final int totalToCollect;

  /** An upper bound on the number of docs "excluded" from max-score accounting due to early termination. */
  private int numExcludedBound;

  /** A lower bound on the total hits collected by all leaves */
  int totalCollected;

  /** the worst hit over all */
  LeafState leafState;

  MaxScoreTerminator(int totalToCollect) {
    leafStates = new ArrayList<>();
    this.totalToCollect = totalToCollect;
    setIntervalBits(DEFAULT_INTERVAL_BITS);
  }

  synchronized LeafState add() {
    LeafState newLeafState = new LeafState();
    leafStates.add(newLeafState);
    if (leafState == null) {
      leafState = newLeafState;
    }
    return newLeafState;
  }

  // for testing
  void setIntervalBits(int bitCount) {
    interval = 1 << bitCount;
    intervalMask = interval - 1;
  }

  /**
   * Called by leaf collectors periodically to update their progress.
   * @param newLeafState the leaf collector's current lowest score
   * @return whether the collector should terminate
   */
  synchronized boolean update(LeafState newLeafState) {
    totalCollected += interval;
    //System.out.println(" scoreboard totalCollected = " + totalCollected + "/" + totalToCollect + " "
    //  + newLeafState + " ? " + leafState + ":" + newLeafState.compareTo(leafState));
    if (newLeafState.compareTo(leafState) >= 0 || newLeafState.resultCount >= totalToCollect) {
      leafState = newLeafState;
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
      leafState = leafStates.get(leafStates.size() - 1);
    }
  }

  /**
   * For tracking the worst scoring hit and total number of hits collected by each leaf
   */
  class LeafState implements Comparable<LeafState> {

    private float worstScore;
    int resultCount;

    LeafState() {
      this.worstScore = -1;
    }

    void update(float score) {
      assert score >= this.worstScore;
      this.worstScore = score;
      ++this.resultCount;
    }

    @Override
    public int compareTo(LeafState o) {
      return Float.compare(worstScore, o.worstScore);
    }

    @Override
    public String toString() {
      return "LeafState<score=" + worstScore + ", count=" + resultCount + ">";
    }

  }

}
