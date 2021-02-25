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
package org.apache.solr.search.stats;

import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.request.SolrQueryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements exact caching of statistics. It requires an additional
 * round-trip to parse query at shard servers, and return term statistics for
 * query terms (and collection statistics for term fields).
 * <p>Global statistics are accumulated in the instance of this component (with the same life-cycle as
 * SolrSearcher), in unbounded maps. NOTE: This may lead to excessive memory usage, in which case
 * a {@link LRUStatsCache} should be considered.</p>
 */
public class ExactSharedStatsCache extends ExactStatsCache {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  // local stats obtained from shard servers
  private final Map<String,Map<String,TermStats>> perShardTermStats = new ConcurrentHashMap<>();
  private final Map<String,Map<String,CollectionStats>> perShardColStats = new ConcurrentHashMap<>();
  // global stats synchronized from the leader
  private final Map<String,TermStats> currentGlobalTermStats = new ConcurrentHashMap<>();
  private final Map<String,CollectionStats> currentGlobalColStats = new ConcurrentHashMap<>();

  @Override
  protected StatsSource doGet(SolrQueryRequest req) {
    if (log.isDebugEnabled()) {
      log.debug("total={}, cache {}", currentGlobalColStats, currentGlobalTermStats.size());
    }
    return new ExactStatsSource(statsCacheMetrics, currentGlobalTermStats, currentGlobalColStats);
  }

  @Override
  public void clear() {
    super.clear();
    perShardTermStats.clear();
    perShardColStats.clear();
    currentGlobalTermStats.clear();
    currentGlobalColStats.clear();
  }

  @Override
  protected void addToPerShardColStats(SolrQueryRequest req, String shard,
      Map<String,CollectionStats> colStats) {
    perShardColStats.put(shard, colStats);
  }

  @Override
  protected void printStats(SolrQueryRequest req) {
    log.debug("perShardColStats={}, perShardTermStats={}", perShardColStats, perShardTermStats);
  }

  @Override
  protected void addToPerShardTermStats(SolrQueryRequest req, String shard, String termStatsString) {
    Map<String,TermStats> termStats = StatsUtil
        .termStatsMapFromString(termStatsString);
    if (termStats != null) {
      perShardTermStats.put(shard, termStats);
    }
  }
  
  protected Map<String,CollectionStats> getPerShardColStats(ResponseBuilder rb, String shard) {
    return perShardColStats.get(shard);
  }

  protected TermStats getPerShardTermStats(SolrQueryRequest req, String t, String shard) {
    Map<String,TermStats> cache = perShardTermStats.get(shard);
    return (cache != null) ? cache.get(t) : null; //Term doesn't exist in shard;
  }

  protected void addToGlobalColStats(SolrQueryRequest req,
      Entry<String,CollectionStats> e) {
    currentGlobalColStats.put(e.getKey(), e.getValue());
  }

  protected void addToGlobalTermStats(SolrQueryRequest req, Entry<String,TermStats> e) {
    currentGlobalTermStats.put(e.getKey(), e.getValue());
  }
}
