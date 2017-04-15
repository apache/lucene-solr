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
package org.apache.solr.analytics.plugin;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.codahale.metrics.Timer;
import org.apache.solr.util.stats.MetricUtils;

public class AnalyticsStatisticsCollector {
  private final AtomicLong numRequests;
  private final AtomicLong numAnalyticsRequests;
  private final AtomicLong numStatsRequests;
  private final AtomicLong numCollectedStats;
  private final AtomicLong numFieldFacets;
  private final AtomicLong numRangeFacets;
  private final AtomicLong numQueryFacets;
  private final AtomicLong numQueries;
  private final Timer requestTimes;
  
  public Timer.Context currentTimer;
  
  public AnalyticsStatisticsCollector() {
    numRequests = new AtomicLong();
    numAnalyticsRequests = new AtomicLong();
    numStatsRequests = new AtomicLong();
    numCollectedStats = new AtomicLong();
    numFieldFacets = new AtomicLong();
    numRangeFacets = new AtomicLong();
    numQueryFacets = new AtomicLong();
    numQueries = new AtomicLong();
    requestTimes = new Timer();
  }
  
  public void startRequest() {
    numRequests.incrementAndGet();
    currentTimer = requestTimes.time();
  }
  
  public void addRequests(long num) {
    numAnalyticsRequests.addAndGet(num);
  }
  
  public void addStatsRequests(long num) {
    numStatsRequests.addAndGet(num);
  }
  
  public void addStatsCollected(long num) {
    numCollectedStats.addAndGet(num);
  }
  
  public void addFieldFacets(long num) {
    numFieldFacets.addAndGet(num);
  }
  
  public void addRangeFacets(long num) {
    numRangeFacets.addAndGet(num);
  }
  
  public void addQueryFacets(long num) {
    numQueryFacets.addAndGet(num);
  }
  
  public void addQueries(long num) {
    numQueries.addAndGet(num);
  }
  
  public void endRequest() {
    currentTimer.stop();
  }

  public Map<String, Object> getStatistics() {

    Map<String, Object> map = new HashMap<>();
    MetricUtils.convertTimer("", requestTimes, false, false, (k, v) -> {
      map.putAll((Map<String, Object>)v);
    });
    map.put("requests", numRequests.longValue());
    map.put("analyticsRequests", numAnalyticsRequests.longValue());
    map.put("statsRequests", numStatsRequests.longValue());
    map.put("statsCollected", numCollectedStats.longValue());
    map.put("fieldFacets", numFieldFacets.longValue());
    map.put("rangeFacets", numRangeFacets.longValue());
    map.put("queryFacets", numQueryFacets.longValue());
    map.put("queriesInQueryFacets", numQueries.longValue());
    return map;
  }
}
