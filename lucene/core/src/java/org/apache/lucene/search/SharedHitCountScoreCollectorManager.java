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

/**
 * Early terminating collector manager based on a global scoreboard
 * model for indices sorted by relevance
 */
public class SharedHitCountScoreCollectorManager implements CollectorManager<TopScoreDocCollector, TopDocs> {
  private final Sort sort;
  private final int numHits;
  private final int totalHitsThreshold;
  private final HitsThresholdChecker hitsThresholdChecker;

  public SharedHitCountScoreCollectorManager(Sort sort, int numHits, int totalHitsThreshold) {
    this.sort = sort;
    this.numHits = numHits;
    this.totalHitsThreshold = totalHitsThreshold;
    this.hitsThresholdChecker = new GlobalHitsThresholdChecker(totalHitsThreshold);
  }

  @Override
  public TopScoreDocCollector newCollector() {
    return TopScoreDocCollector.create(numHits, null, totalHitsThreshold, new GlobalHitsThresholdChecker(totalHitsThreshold));
  }

  @Override
  public TopDocs reduce(Collection<TopScoreDocCollector> collectors) {
    final TopDocs[] topDocs = new TopDocs[collectors.size()];
    int i = 0;
    for (TopScoreDocCollector collector : collectors) {
      topDocs[i++] = collector.topDocs();
    }
    return TopDocs.merge(0, numHits, topDocs);
  }
}