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

/**
 * Early terminating collector manager based on a global scoreboard
 * model where all Collectors report their number of hits in a shared
 * state which allows an early termination across all threads
 */
public class SharedHitCountFieldCollectorManager implements CollectorManager<TopFieldCollector.SharedHitCountFieldCollector, TopFieldDocs> {

  private final Sort sort;
  private final int numHits;
  private final int totalHitsThreshold;
  private final AtomicInteger globalTotalHits;

  public SharedHitCountFieldCollectorManager(Sort sort, int numHits, int totalHitsThreshold) {
    this.sort = sort;
    this.numHits = numHits;
    this.totalHitsThreshold = totalHitsThreshold;
    this.globalTotalHits = new AtomicInteger();
  }

  @Override
  public TopFieldCollector.SharedHitCountFieldCollector newCollector() {
    return new TopFieldCollector.SharedHitCountFieldCollector(sort, FieldValueHitQueue.create(sort.fields, numHits),
        numHits, totalHitsThreshold, globalTotalHits);
  }

  @Override
  public TopFieldDocs reduce(Collection<TopFieldCollector.SharedHitCountFieldCollector> collectors) throws IOException {
    final TopFieldDocs[] topDocs = new TopFieldDocs[collectors.size()];
    int i = 0;
    for (TopFieldCollector collector : collectors) {
      topDocs[i++] = collector.topDocs();
    }
    return TopDocs.merge(sort, 0, numHits, topDocs);
  }
}