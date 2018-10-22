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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermStates;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.TermStatistics;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.FastLRUCache;
import org.apache.solr.search.SolrCache;
import org.apache.solr.search.SolrIndexSearcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unlike {@link ExactStatsCache} this implementation preserves term stats
 * across queries in a set of LRU caches, and based on surface features of a
 * query it determines the need to send additional RPC-s. As a result the
 * additional RPC-s are needed much less frequently.
 * 
 * <p>
 * Query terms and their stats are maintained in a set of maps. At the query
 * front-end there will be as many maps as there are shards, each maintaining
 * the respective shard statistics. At each shard server there is a single map
 * that is updated with the global statistics on every request.
 */
public class LRUStatsCache extends ExactStatsCache {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  // local stats obtained from shard servers
  private final Map<String,SolrCache<String,TermStats>> perShardTermStats = new ConcurrentHashMap<>();
  private final Map<String,Map<String,CollectionStats>> perShardColStats = new ConcurrentHashMap<>();
  
  // global stats synchronized from the master
  private final FastLRUCache<String,TermStats> currentGlobalTermStats = new FastLRUCache<>();
  private final Map<String,CollectionStats> currentGlobalColStats = new ConcurrentHashMap<>();
  
  // local term context (caching term lookups)

  private final Map lruCacheInitArgs = new HashMap();
  
  @Override
  public StatsSource get(SolrQueryRequest req) {
    log.debug("## GET total={}, cache {}", currentGlobalColStats , currentGlobalTermStats.size());
    return new LRUStatsSource(currentGlobalTermStats, currentGlobalColStats);
  }
  
  @Override
  public void init(PluginInfo info) {
    // TODO: make this configurable via PluginInfo
    lruCacheInitArgs.put("size", "100");
    currentGlobalTermStats.init(lruCacheInitArgs, null, null);
  }
  
  @Override
  protected void addToGlobalTermStats(SolrQueryRequest req, Entry<String,TermStats> e) {
    currentGlobalTermStats.put(e.getKey(), e.getValue());
  }
  
  @Override
  protected void addToPerShardColStats(SolrQueryRequest req, String shard, Map<String,CollectionStats> colStats) {
    perShardColStats.put(shard, colStats);
  }
  
  @Override
  protected Map<String,CollectionStats> getPerShardColStats(ResponseBuilder rb, String shard) {
    return perShardColStats.get(shard);
  }
  
  @Override
  protected void addToPerShardTermStats(SolrQueryRequest req, String shard, String termStatsString) {
    Map<String,TermStats> termStats = StatsUtil.termStatsMapFromString(termStatsString);
    if (termStats != null) {
      SolrCache<String,TermStats> cache = perShardTermStats.get(shard);
      if (cache == null) { // initialize
        cache = new FastLRUCache<>();
        cache.init(lruCacheInitArgs, null, null);
        perShardTermStats.put(shard, cache);
      }
      for (Entry<String,TermStats> e : termStats.entrySet()) {
        cache.put(e.getKey(), e.getValue());
      }
    }
  }
  
  @Override
  protected TermStats getPerShardTermStats(SolrQueryRequest req, String t, String shard) {
    SolrCache<String,TermStats> cache = perShardTermStats.get(shard);
    return (cache != null) ? cache.get(t) : null; //Term doesn't exist in shard
  }
  
  @Override
  protected void addToGlobalColStats(SolrQueryRequest req, Entry<String,CollectionStats> e) {
    currentGlobalColStats.put(e.getKey(), e.getValue());
  }

  @Override
  protected void printStats(SolrQueryRequest req) {
    log.debug("## MERGED: perShardColStats={}, perShardTermStats={}", perShardColStats, perShardTermStats);
  }
  
  static class LRUStatsSource extends StatsSource {
    private final SolrCache<String,TermStats> termStatsCache;
    private final Map<String,CollectionStats> colStatsCache;
    
    public LRUStatsSource(SolrCache<String,TermStats> termStatsCache, Map<String,CollectionStats> colStatsCache) {
      this.termStatsCache = termStatsCache;
      this.colStatsCache = colStatsCache;
    }
    @Override
    public TermStatistics termStatistics(SolrIndexSearcher localSearcher, Term term, TermStates context)
        throws IOException {
      TermStats termStats = termStatsCache.get(term.toString());
      if (termStats == null) {
        log.debug("## Missing global termStats info: {}, using local", term);
        return localSearcher.localTermStatistics(term, context);
      } else {
        return termStats.toTermStatistics();
      }
    }
    
    @Override
    public CollectionStatistics collectionStatistics(SolrIndexSearcher localSearcher, String field)
        throws IOException {
      CollectionStats colStats = colStatsCache.get(field);
      if (colStats == null) {
        log.debug("## Missing global colStats info: {}, using local", field);
        return localSearcher.localCollectionStatistics(field);
      } else {
        return colStats.toCollectionStatistics();
      }
    }
  }
}
