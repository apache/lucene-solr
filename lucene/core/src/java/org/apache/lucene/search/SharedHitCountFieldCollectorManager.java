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

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Early terminating collector manager based on a global scoreboard
 * model where all Collectors report their number of hits in a shared
 * state which allows an early termination across all threads
 */
public class SharedHitCountFieldCollectorManager implements CollectorManager<TopFieldCollector, TopFieldDocs> {

  /**
   * Implementation of HitsThresholdChecker which allows global hit counting
   */
  private static class GlobalHitsThresholdChecker extends TopFieldCollector.HitsThresholdChecker {
    private final int totalHitsThreshold;
    private final AtomicInteger globalHitCount;

    public GlobalHitsThresholdChecker(int totalHitsThreshold) {
      this.totalHitsThreshold = totalHitsThreshold;
      this.globalHitCount = new AtomicInteger();
    }

    @Override
    public void incrementHitCount() {
      globalHitCount.incrementAndGet();
    }

    @Override
    public boolean getAsBoolean() {
      return globalHitCount.getAcquire() > totalHitsThreshold;
    }
  }

  private final Sort sort;
  private final int numHits;
  private final int totalHitsThreshold;
  private final TopFieldCollector.HitsThresholdChecker hitsThresholdChecker;

  public SharedHitCountFieldCollectorManager(Sort sort, int numHits, int totalHitsThreshold) {
    this.sort = sort;
    this.numHits = numHits;
    this.totalHitsThreshold = totalHitsThreshold;
    this.hitsThresholdChecker = new GlobalHitsThresholdChecker(totalHitsThreshold);
  }

  @Override
  public TopFieldCollector newCollector() {
    return TopFieldCollector.create(sort, numHits, null, totalHitsThreshold, hitsThresholdChecker);
  }

  @Override
  public TopFieldDocs reduce(Collection<TopFieldCollector> collectors) {
    final TopFieldDocs[] topDocs = new TopFieldDocs[collectors.size()];
    int i = 0;
    for (TopFieldCollector collector : collectors) {
      topDocs[i++] = collector.topDocs();
    }
    return TopDocs.merge(sort, 0, numHits, topDocs);
  }
}