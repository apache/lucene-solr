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
package org.apache.solr.store.blockcache;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.search.SolrCacheBase;

/**
 * A {@link SolrInfoBean} that provides metrics on block cache operations.
 *
 * @lucene.experimental
 */
public class Metrics extends SolrCacheBase implements SolrInfoBean {


  public AtomicLong blockCacheSize = new AtomicLong(0);
  public AtomicLong blockCacheHit = new AtomicLong(0);
  public AtomicLong blockCacheMiss = new AtomicLong(0);
  public AtomicLong blockCacheEviction = new AtomicLong(0);
  public AtomicLong blockCacheStoreFail = new AtomicLong(0);

  // since the last call
  private AtomicLong blockCacheHit_last = new AtomicLong(0);
  private AtomicLong blockCacheMiss_last = new AtomicLong(0);
  private AtomicLong blockCacheEviction_last = new AtomicLong(0);
  public AtomicLong blockCacheStoreFail_last = new AtomicLong(0);


  // These are used by the BufferStore (just a generic cache of byte[]).
  // TODO: If this (the Store) is a good idea, we should make it more general and use it across more places in Solr.
  public AtomicLong shardBuffercacheAllocate = new AtomicLong(0);
  public AtomicLong shardBuffercacheLost = new AtomicLong(0);

  private MetricsMap metricsMap;
  private Set<String> metricNames = ConcurrentHashMap.newKeySet();
  private SolrMetricsContext solrMetricsContext;
  private long previous = System.nanoTime();

  @Override
  public void initializeMetrics(SolrMetricsContext parentContext, String scope) {
    solrMetricsContext = parentContext.getChildContext(this);
    metricsMap = new MetricsMap((detailed, map) -> {
      long now = System.nanoTime();
      long delta = Math.max(now - previous, 1);
      double seconds = delta / 1000000000.0;

      long hits_total = blockCacheHit.get();
      long hits_delta = hits_total - blockCacheHit_last.get();
      blockCacheHit_last.set(hits_total);

      long miss_total = blockCacheMiss.get();
      long miss_delta = miss_total - blockCacheMiss_last.get();
      blockCacheMiss_last.set(miss_total);

      long evict_total = blockCacheEviction.get();
      long evict_delta = evict_total - blockCacheEviction_last.get();
      blockCacheEviction_last.set(evict_total);

      long storeFail_total = blockCacheStoreFail.get();
      long storeFail_delta = storeFail_total - blockCacheStoreFail_last.get();
      blockCacheStoreFail_last.set(storeFail_total);

      long lookups_delta = hits_delta + miss_delta;
      long lookups_total = hits_total + miss_total;

      map.put("size", blockCacheSize.get());
      map.put("lookups", lookups_total);
      map.put("hits", hits_total);
      map.put("evictions", evict_total);
      map.put("storeFails", storeFail_total);
      map.put("hitratio_current", calcHitRatio(lookups_delta, hits_delta));  // hit ratio since the last call
      map.put("lookups_persec", getPerSecond(lookups_delta, seconds)); // lookups per second since the last call
      map.put("hits_persec", getPerSecond(hits_delta, seconds));       // hits per second since the last call
      map.put("evictions_persec", getPerSecond(evict_delta, seconds));  // evictions per second since the last call
      map.put("storeFails_persec", getPerSecond(storeFail_delta, seconds));  // evictions per second since the last call
      map.put("time_delta", seconds);  // seconds since last call

      // TODO: these aren't really related to the BlockCache
      map.put("buffercache.allocations", getPerSecond(shardBuffercacheAllocate.getAndSet(0), seconds));
      map.put("buffercache.lost", getPerSecond(shardBuffercacheLost.getAndSet(0), seconds));

      previous = now;

    });
    solrMetricsContext.gauge(metricsMap, true, getName(), getCategory().toString(), scope);
  }

  private float getPerSecond(long value, double seconds) {
    return (float) (value / seconds);
  }

  // SolrInfoBean methods

  @Override
  public String getName() {
    return "hdfsBlockCache";
  }

  @Override
  public String getDescription() {
    return "Provides metrics for the HdfsDirectoryFactory BlockCache.";
  }

  @Override
  public SolrMetricsContext getSolrMetricsContext() {
    return solrMetricsContext;
  }
}
