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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.TermStatistics;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.CaffeineCache;
import org.apache.solr.search.SolrCache;
import org.apache.solr.search.SolrIndexSearcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unlike {@link ExactStatsCache} this implementation preserves term stats
 * across queries in a set of LRU caches (with the same life-cycle as SolrIndexSearcher),
 * and based on surface features of a
 * query it determines the need to send additional requests to retrieve local term
 * and collection statistics from shards. As a result the
 * additional requests may be needed much less frequently.
 * <p>
 * Query terms, their stats and field stats are maintained in LRU caches, with the size by default
 * {@link #DEFAULT_MAX_SIZE}, one cache per shard. These caches
 * are updated as needed (when term or field statistics are missing). Each instance of the component
 * keeps also a global stats cache, which is aggregated from per-shard caches.
 * <p>Cache entries expire after a max idle time, by default {@link #DEFAULT_MAX_IDLE_TIME}.
 */
public class LRUStatsCache extends ExactStatsCache {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final int DEFAULT_MAX_SIZE = 200;
  public static final int DEFAULT_MAX_IDLE_TIME = 60;

  // local stats obtained from shard servers
  // map of <shardName, <term, termStats>>
  private final Map<String,SolrCache<String,TermStats>> perShardTermStats = new ConcurrentHashMap<>();
  // map of <shardName, <field, collStats>>
  private final Map<String,Map<String,CollectionStats>> perShardColStats = new ConcurrentHashMap<>();
  
  // global stats synchronized from the master

  // cache of <term, termStats>
  private final CaffeineCache<String,TermStats> currentGlobalTermStats = new CaffeineCache<>();
  // cache of <field, colStats>
  private final CaffeineCache<String,CollectionStats> currentGlobalColStats = new CaffeineCache<>();

  // missing stats to be fetched with the next request
  private Set<String> missingColStats = ConcurrentHashMap.newKeySet();
  private Set<Term> missingTermStats = ConcurrentHashMap.newKeySet();
  
  private final Map<String, String> lruCacheInitArgs = new HashMap<>();

  private final StatsCacheMetrics ignorableMetrics = new StatsCacheMetrics();

  @Override
  protected StatsSource doGet(SolrQueryRequest req) {
    log.debug("## GET total={}, cache {}", currentGlobalColStats , currentGlobalTermStats.size());
    return new LRUStatsSource(statsCacheMetrics);
  }

  @Override
  public void clear() {
    super.clear();
    perShardTermStats.clear();
    perShardColStats.clear();
    currentGlobalTermStats.clear();
    currentGlobalColStats.clear();
    ignorableMetrics.clear();
  }

  @Override
  public void init(PluginInfo info) {
    super.init(info);
    if (info != null && info.attributes != null) {
      lruCacheInitArgs.putAll(info.attributes);
    }
    lruCacheInitArgs.computeIfAbsent(SolrCache.SIZE_PARAM, s -> String.valueOf(DEFAULT_MAX_SIZE));
    lruCacheInitArgs.computeIfAbsent(SolrCache.MAX_IDLE_TIME_PARAM, t -> String.valueOf(DEFAULT_MAX_IDLE_TIME));
    Map<String, Object> map = new HashMap<>(lruCacheInitArgs);
    map.put(CommonParams.NAME, "globalTermStats");
    currentGlobalTermStats.init(lruCacheInitArgs, null, null);
    currentGlobalTermStats.setState(SolrCache.State.LIVE);
    map = new HashMap<>(lruCacheInitArgs);
    map.put(CommonParams.NAME, "globalColStats");
    currentGlobalColStats.init(lruCacheInitArgs, null, null);
    currentGlobalColStats.setState(SolrCache.State.LIVE);  }

  @Override
  protected ShardRequest doRetrieveStatsRequest(ResponseBuilder rb) {
    // check approximately what terms are needed.

    // NOTE: query rewrite only expands to terms that are present in the local index
    // so it's possible that the result will contain less terms than present in all shards.

    // HOWEVER: the absence of these terms is recorded by LRUStatsSource, and they will be
    // force-fetched on next request and cached.

    // check for missing stats from previous requests
    if (!missingColStats.isEmpty() || !missingColStats.isEmpty()) {
      // needs to fetch anyway, so get the full query stats + the missing stats for caching
      ShardRequest sreq = super.doRetrieveStatsRequest(rb);
      if (!missingColStats.isEmpty()) {
        Set<String> requestColStats = missingColStats;
        // there's a small window when new items may be added before
        // creating the request and clearing, so don't clear - instead replace the instance
        missingColStats = ConcurrentHashMap.newKeySet();
        sreq.params.add(FIELDS_KEY, StatsUtil.fieldsToString(requestColStats));
      }
      if (!missingTermStats.isEmpty()) {
        Set<Term> requestTermStats = missingTermStats;
        missingTermStats = ConcurrentHashMap.newKeySet();
        sreq.params.add(TERMS_KEY, StatsUtil.termsToEncodedString(requestTermStats));
      }
      return sreq;
    }

    // rewrite locally to see if there are any missing terms. See the note above for caveats.
    LongAdder missing = new LongAdder();
    try {
      // use ignorableMetrics to avoid counting this checking as real misses
      approxCheckMissingStats(rb, new LRUStatsSource(ignorableMetrics), t -> missing.increment(), f -> missing.increment());
      if (missing.sum() == 0) {
        // it should be (approximately) ok to skip the fetching

        // since we already incremented the stats decrement it here
        statsCacheMetrics.retrieveStats.decrement();
        statsCacheMetrics.useCachedGlobalStats.increment();
        return null;
      } else {
        return super.doRetrieveStatsRequest(rb);
      }
    } catch (IOException e) {
      log.warn("Exception checking missing stats for query " + rb.getQuery() + ", forcing retrieving stats", e);
      // retrieve anyway
      return super.doRetrieveStatsRequest(rb);
    }
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
      SolrCache<String,TermStats> cache = perShardTermStats.computeIfAbsent(shard, s -> {
        CaffeineCache c = new CaffeineCache<>();
        Map<String, String> map = new HashMap<>(lruCacheInitArgs);
        map.put(CommonParams.NAME, s);
        c.init(map, null, null);
        c.setState(SolrCache.State.LIVE);
        return c;
      });
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
  
  class LRUStatsSource extends StatsSource {
    private final StatsCacheMetrics metrics;

    LRUStatsSource(StatsCacheMetrics metrics) {
      this.metrics = metrics;
    }

    @Override
    public TermStatistics termStatistics(SolrIndexSearcher localSearcher, Term term, int docFreq, long totalTermFreq)
        throws IOException {
      TermStats termStats = currentGlobalTermStats.get(term.toString());
      if (termStats == null) {
        log.debug("## Missing global termStats info: {}, using local", term);
        missingTermStats.add(term);
        metrics.missingGlobalTermStats.increment();
        return localSearcher != null ? localSearcher.localTermStatistics(term, docFreq, totalTermFreq) : null;
      } else {
        return termStats.toTermStatistics();
      }
    }
    
    @Override
    public CollectionStatistics collectionStatistics(SolrIndexSearcher localSearcher, String field)
        throws IOException {
      CollectionStats colStats = currentGlobalColStats.get(field);
      if (colStats == null) {
        log.debug("## Missing global colStats info: {}, using local", field);
        missingColStats.add(field);
        metrics.missingGlobalFieldStats.increment();
        return localSearcher != null ? localSearcher.localCollectionStatistics(field) : null;
      } else {
        return colStats.toCollectionStatistics();
      }
    }
  }
}
