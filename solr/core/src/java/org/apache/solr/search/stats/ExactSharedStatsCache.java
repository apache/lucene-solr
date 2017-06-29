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

import org.apache.solr.core.PluginInfo;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.request.SolrQueryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ExactSharedStatsCache extends ExactStatsCache {
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  // local stats obtained from shard servers
  private final Map<String,Map<String,TermStats>> perShardTermStats = new ConcurrentHashMap<>();
  private final Map<String,Map<String,CollectionStats>> perShardColStats = new ConcurrentHashMap<>();
  // global stats synchronized from the master
  private final Map<String,TermStats> currentGlobalTermStats = new ConcurrentHashMap<>();
  private final Map<String,CollectionStats> currentGlobalColStats = new ConcurrentHashMap<>();

  @Override
  public StatsSource get(SolrQueryRequest req) {
    LOG.debug("total={}, cache {}", currentGlobalColStats, currentGlobalTermStats.size());
    return new ExactStatsSource(currentGlobalTermStats, currentGlobalColStats);
  }
  
  @Override
  public void init(PluginInfo info) {}

  @Override
  protected void addToPerShardColStats(SolrQueryRequest req, String shard,
      Map<String,CollectionStats> colStats) {
    perShardColStats.put(shard, colStats);
  }

  @Override
  protected void printStats(SolrQueryRequest req) {
    LOG.debug("perShardColStats={}, perShardTermStats={}", perShardColStats, perShardTermStats);
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
