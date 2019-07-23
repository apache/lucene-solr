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

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Early terminating collector manager based on a global scoreboard
 * model where all Collectors report their number of hits in a shared
 * state which allows an early termination across all threads
 */
public class GlobalStateCollectorManager implements CollectorManager<TopFieldCollector.GlobalStateFieldCollector, TopFieldDocs> {

  /**
   * Interface for defining parent callback for global state operations
   */
  public interface GlobalStateCallback {
    float getGlobalMinCompetitiveScore();
    void checkAndUpdateMinCompetitiveScore(float score);
  }

  private static class GlobalMinCompetitiveScoreCallback implements GlobalStateCallback {
    private ReentrantLock lock = new ReentrantLock();
    private volatile float globalMinCompetitiveScore;

    // Returns the current global minimum competitive hit score
    // We do not acquire the lock for reads since it is guaranteed
    // that the score can only increase with time, not decrease
    // So the worst that can happen with a missed update for a collector read
    // is that a doc which would have been filtered out due to the new min competitive
    // score is accepted, which should be an ok penalty to take.
    public float getGlobalMinCompetitiveScore() {
      return globalMinCompetitiveScore;
    }

    // Update the min competitive score if it increases the score
    public void checkAndUpdateMinCompetitiveScore(float score) {
      lock.lock();
      try {
        if (score > globalMinCompetitiveScore) {
          globalMinCompetitiveScore = score;
        }
      } finally {
        lock.unlock();
      }
    }
  }

  private final Sort sort;
  private final int numHits;
  private final int totalHitsThreshold;
  private final AtomicInteger globalTotalHits;
  private final GlobalMinCompetitiveScoreCallback globalMinCompetitiveScoreCallback = new GlobalMinCompetitiveScoreCallback();

  public GlobalStateCollectorManager(Sort sort, int numHits, int totalHitsThreshold) {
    this.sort = sort;
    this.numHits = numHits;
    this.totalHitsThreshold = totalHitsThreshold;
    this.globalTotalHits = new AtomicInteger();
  }

  @Override
  public TopFieldCollector.GlobalStateFieldCollector newCollector() {
    return new TopFieldCollector.GlobalStateFieldCollector(sort, FieldValueHitQueue.create(sort.fields, numHits), numHits, totalHitsThreshold, globalTotalHits,
        globalMinCompetitiveScoreCallback);
  }

  @Override
  public TopFieldDocs reduce(Collection<TopFieldCollector.GlobalStateFieldCollector> collectors) throws IOException {
    final TopFieldDocs[] topDocs = new TopFieldDocs[collectors.size()];
    int i = 0;
    for (TopFieldCollector collector : collectors) {
      topDocs[i++] = collector.topDocs();
    }
    return TopDocs.merge(sort, 0, numHits, topDocs);
  }
}
