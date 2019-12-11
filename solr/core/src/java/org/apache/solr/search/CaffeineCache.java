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
package org.apache.solr.search;

import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.solr.common.SolrException;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricsContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Policy.Eviction;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.google.common.annotations.VisibleForTesting;

/**
 * A SolrCache backed by the Caffeine caching library [1]. By default it uses the Window TinyLFU (W-TinyLFU)
 * eviction policy.
 * <p>This cache supports either maximum size limit (the number of items) or maximum ram bytes limit, but
 * not both. If both values are set then only maxRamMB limit is used and maximum size limit is ignored.</p>
 * <p>
 * W-TinyLFU [2] is a near optimal policy that uses recency and frequency to determine which entry
 * to evict in O(1) time. The estimated frequency is retained in a Count-Min Sketch and entries
 * reside on LRU priority queues [3]. By capturing the historic frequency of an entry, the cache is
 * able to outperform classic policies like LRU and LFU, as well as modern policies like ARC and
 * LIRS. This policy performed particularly well in search workloads.
 * <p>
 * [1] https://github.com/ben-manes/caffeine
 * [2] http://arxiv.org/pdf/1512.00727.pdf
 * [3] http://highscalability.com/blog/2016/1/25/design-of-a-modern-cache.html
 */
public class CaffeineCache<K, V> extends SolrCacheBase implements SolrCache<K, V>, Accountable, RemovalListener<K, V> {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(CaffeineCache.class)
      + RamUsageEstimator.shallowSizeOfInstance(CacheStats.class)
      + 2 * RamUsageEstimator.shallowSizeOfInstance(LongAdder.class);

  private Executor executor;

  private CacheStats priorStats;
  private long priorInserts;

  private String description = "Caffeine Cache";
  private LongAdder inserts;
  private Cache<K,V> cache;
  private long warmupTime;
  private int maxSize;
  private long maxRamBytes;
  private int initialSize;
  private int maxIdleTimeSec;
  private boolean cleanupThread;

  private Set<String> metricNames = ConcurrentHashMap.newKeySet();
  private MetricsMap cacheMap;
  private SolrMetricsContext solrMetricsContext;

  private long initialRamBytes = 0;
  private final LongAdder ramBytes = new LongAdder();

  public CaffeineCache() {
    this.priorStats = CacheStats.empty();
  }

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public Object init(Map args, Object persistence, CacheRegenerator regenerator) {
    super.init(args, regenerator);
    String str = (String) args.get(SIZE_PARAM);
    maxSize = (str == null) ? 1024 : Integer.parseInt(str);
    str = (String) args.get("initialSize");
    initialSize = Math.min((str == null) ? 1024 : Integer.parseInt(str), maxSize);
    str = (String) args.get(MAX_IDLE_TIME_PARAM);
    if (str == null) {
      maxIdleTimeSec = -1;
    } else {
      maxIdleTimeSec = Integer.parseInt(str);
    }
    str = (String) args.get(MAX_RAM_MB_PARAM);
    int maxRamMB = str == null ? -1 : Double.valueOf(str).intValue();
    maxRamBytes = maxRamMB < 0 ? Long.MAX_VALUE : maxRamMB * 1024L * 1024L;
    str = (String) args.get(CLEANUP_THREAD_PARAM);
    cleanupThread = str != null && Boolean.parseBoolean(str);
    if (cleanupThread) {
      executor = ForkJoinPool.commonPool();
    } else {
      executor = Runnable::run;
    }

    description = generateDescription(maxSize, initialSize);

    cache = buildCache(null);
    inserts = new LongAdder();

    initialRamBytes =
        RamUsageEstimator.shallowSizeOfInstance(cache.getClass()) +
        RamUsageEstimator.shallowSizeOfInstance(executor.getClass()) +
        RamUsageEstimator.sizeOfObject(description);

    return persistence;
  }

  private Cache<K, V> buildCache(Cache<K, V> prev) {
    Caffeine builder = Caffeine.newBuilder()
        .initialCapacity(initialSize)
        .executor(executor)
        .removalListener(this)
        .recordStats();
    if (maxIdleTimeSec > 0) {
      builder.expireAfterAccess(Duration.ofSeconds(maxIdleTimeSec));
    }
    if (maxRamBytes != Long.MAX_VALUE) {
      builder.maximumWeight(maxRamBytes);
      builder.weigher((k, v) -> (int) (RamUsageEstimator.sizeOfObject(k) + RamUsageEstimator.sizeOfObject(v)));
    } else {
      builder.maximumSize(maxSize);
    }
    Cache<K, V> newCache = builder.build();
    if (prev != null) {
      newCache.putAll(prev.asMap());
    }
    return newCache;
  }

  @Override
  public void onRemoval(K key, V value, RemovalCause cause) {
    ramBytes.add(
        - (RamUsageEstimator.sizeOfObject(key, RamUsageEstimator.QUERY_DEFAULT_RAM_BYTES_USED) +
        RamUsageEstimator.sizeOfObject(value, RamUsageEstimator.QUERY_DEFAULT_RAM_BYTES_USED) +
        RamUsageEstimator.LINKED_HASHTABLE_RAM_BYTES_PER_ENTRY)
    );
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES_USED + initialRamBytes + ramBytes.sum();
  }

  @Override
  public V get(K key) {
    return cache.getIfPresent(key);
  }

  @Override
  public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
    return cache.get(key, k -> {
      inserts.increment();
      V value = mappingFunction.apply(k);
      if (value == null) {
        return null;
      }
      ramBytes.add(RamUsageEstimator.sizeOfObject(key, RamUsageEstimator.QUERY_DEFAULT_RAM_BYTES_USED) +
          RamUsageEstimator.sizeOfObject(value, RamUsageEstimator.QUERY_DEFAULT_RAM_BYTES_USED));
      ramBytes.add(RamUsageEstimator.LINKED_HASHTABLE_RAM_BYTES_PER_ENTRY);
      return value;
    });
  }

  @Override
  public V put(K key, V val) {
    inserts.increment();
    V old = cache.asMap().put(key, val);
    ramBytes.add(RamUsageEstimator.sizeOfObject(key, RamUsageEstimator.QUERY_DEFAULT_RAM_BYTES_USED) +
        RamUsageEstimator.sizeOfObject(val, RamUsageEstimator.QUERY_DEFAULT_RAM_BYTES_USED));
    if (old != null) {
      ramBytes.add(- RamUsageEstimator.sizeOfObject(old, RamUsageEstimator.QUERY_DEFAULT_RAM_BYTES_USED));
    } else {
      ramBytes.add(RamUsageEstimator.LINKED_HASHTABLE_RAM_BYTES_PER_ENTRY);
    }
    return old;
  }

  @Override
  public V remove(K key) {
    V existing = cache.asMap().remove(key);
    if (existing != null) {
      ramBytes.add(- RamUsageEstimator.sizeOfObject(key, RamUsageEstimator.QUERY_DEFAULT_RAM_BYTES_USED));
      ramBytes.add(- RamUsageEstimator.sizeOfObject(existing, RamUsageEstimator.QUERY_DEFAULT_RAM_BYTES_USED));
      ramBytes.add(- RamUsageEstimator.LINKED_HASHTABLE_RAM_BYTES_PER_ENTRY);
    }
    return existing;
  }

  @Override
  public void clear() {
    cache.invalidateAll();
    ramBytes.reset();
  }

  @Override
  public int size() {
    return cache.asMap().size();
  }

  @Override
  public void close() throws Exception {
    SolrCache.super.close();
    cache.invalidateAll();
    cache.cleanUp();
    if (executor instanceof ExecutorService) {
      ((ExecutorService)executor).shutdownNow();
    }
    ramBytes.reset();
  }

  @Override
  public int getMaxSize() {
    return maxSize;
  }

  @Override
  public void setMaxSize(int maxSize) {
    if (this.maxSize == maxSize) {
      return;
    }
    Optional<Eviction<K, V>> evictionOpt = cache.policy().eviction();
    if (evictionOpt.isPresent()) {
      Eviction<K, V> eviction = evictionOpt.get();
      eviction.setMaximum(maxSize);
      this.maxSize = maxSize;
      initialSize = Math.min(1024, this.maxSize);
      description = generateDescription(this.maxSize, initialSize);
      cache.cleanUp();
    }
  }

  @Override
  public int getMaxRamMB() {
    return maxRamBytes != Long.MAX_VALUE ? (int) (maxRamBytes / 1024L / 1024L) : -1;
  }

  @Override
  public void setMaxRamMB(int maxRamMB) {
    long newMaxRamBytes = maxRamMB < 0 ? Long.MAX_VALUE : maxRamMB * 1024L * 1024L;
    if (newMaxRamBytes != maxRamBytes) {
      maxRamBytes = newMaxRamBytes;
      Optional<Eviction<K, V>> evictionOpt = cache.policy().eviction();
      if (evictionOpt.isPresent()) {
        Eviction<K, V> eviction = evictionOpt.get();
        if (!eviction.isWeighted()) {
          // rebuild cache using weigher
          cache = buildCache(cache);
          return;
        } else if (maxRamBytes == Long.MAX_VALUE) {
          // rebuild cache using maxSize
          cache = buildCache(cache);
          return;
        }
        eviction.setMaximum(newMaxRamBytes);
        description = generateDescription(this.maxSize, initialSize);
        cache.cleanUp();
      }
    }
  }

  @Override
  public void warm(SolrIndexSearcher searcher, SolrCache<K,V> old) {
    if (regenerator == null) {
      return;
    }
    
    long warmingStartTime = System.nanoTime();
    Map<K, V> hottest = Collections.emptyMap();
    CaffeineCache<K,V> other = (CaffeineCache<K,V>)old;

    // warm entries
    if (isAutowarmingOn()) {
      Eviction<K, V> policy = other.cache.policy().eviction().get();
      int size = autowarm.getWarmCount(other.cache.asMap().size());
      hottest = policy.hottest(size);
    }

    for (Entry<K, V> entry : hottest.entrySet()) {
      try {
        boolean continueRegen = regenerator.regenerateItem(
            searcher, this, old, entry.getKey(), entry.getValue());
        if (!continueRegen) {
          break;
        }
      }
      catch (Exception e) {
        SolrException.log(log, "Error during auto-warming of key:" + entry.getKey(), e);
      }
    }

    inserts.reset();
    priorStats = other.cache.stats().plus(other.priorStats);
    priorInserts = other.inserts.sum() + other.priorInserts;
    warmupTime = TimeUnit.MILLISECONDS.convert(System.nanoTime() - warmingStartTime, TimeUnit.NANOSECONDS);
  }

  /** Returns the description of this cache. */
  private String generateDescription(int limit, int initialSize) {
    return String.format(Locale.ROOT, "TinyLfu Cache(maxSize=%d, initialSize=%d%s)",
        limit, initialSize, isAutowarmingOn() ? (", " + getAutowarmDescription()) : "");
  }

  //////////////////////// SolrInfoBean methods //////////////////////

  @Override
  public String getName() {
    return CaffeineCache.class.getName();
  }

  @Override
  public String getDescription() {
     return description;
  }

  // for unit tests only
  @VisibleForTesting
  MetricsMap getMetricsMap() {
    return cacheMap;
  }

  @Override
  public SolrMetricsContext getSolrMetricsContext() {
    return solrMetricsContext;
  }

  @Override
  public String toString() {
    return name() + (cacheMap != null ? cacheMap.getValue().toString() : "");
  }

  @Override
  public void initializeMetrics(SolrMetricsContext parentContext, String scope) {
    solrMetricsContext = parentContext.getChildContext(this);
    cacheMap = new MetricsMap((detailed, map) -> {
      if (cache != null) {
        CacheStats stats = cache.stats();
        long insertCount = inserts.sum();

        map.put(LOOKUPS_PARAM, stats.requestCount());
        map.put(HITS_PARAM, stats.hitCount());
        map.put(HIT_RATIO_PARAM, stats.hitRate());
        map.put(INSERTS_PARAM, insertCount);
        map.put(EVICTIONS_PARAM, stats.evictionCount());
        map.put(SIZE_PARAM, cache.asMap().size());
        map.put("warmupTime", warmupTime);
        map.put(RAM_BYTES_USED_PARAM, ramBytesUsed());
        map.put(MAX_RAM_MB_PARAM, getMaxRamMB());

        CacheStats cumulativeStats = priorStats.plus(stats);
        map.put("cumulative_lookups", cumulativeStats.requestCount());
        map.put("cumulative_hits", cumulativeStats.hitCount());
        map.put("cumulative_hitratio", cumulativeStats.hitRate());
        map.put("cumulative_inserts", priorInserts + insertCount);
        map.put("cumulative_evictions", cumulativeStats.evictionCount());
      }
    });
    solrMetricsContext.gauge(cacheMap, true, scope, getCategory().toString());
  }
}
