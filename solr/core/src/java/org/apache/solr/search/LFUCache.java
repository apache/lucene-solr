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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.solr.common.SolrException;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.util.ConcurrentLFUCache;
import org.apache.solr.util.IOFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CommonParams.NAME;

/**
 * SolrCache based on ConcurrentLFUCache implementation.
 * <p>
 * This implementation does not use a separate cleanup thread. Instead it uses the calling thread
 * itself to do the cleanup when the size of the cache exceeds certain limits.
 * <p>
 * Also see <a href="http://wiki.apache.org/solr/SolrCaching">SolrCaching</a>
 * <p>
 * <b>This API is experimental and subject to change</b>
 *
 * @see org.apache.solr.util.ConcurrentLFUCache
 * @see org.apache.solr.search.SolrCache
 * @since solr 3.6
 * @deprecated This cache implementation is deprecated and will be removed in Solr 9.0.
 * Use {@link CaffeineCache} instead.
 */
public class LFUCache<K, V> implements SolrCache<K, V>, Accountable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(LFUCache.class);

  public static final String TIME_DECAY_PARAM = "timeDecay";
  public static final String CLEANUP_THREAD_PARAM = "cleanupThread";
  public static final String INITIAL_SIZE_PARAM = "initialSize";
  public static final String MIN_SIZE_PARAM = "minSize";
  public static final String ACCEPTABLE_SIZE_PARAM = "acceptableSize";
  public static final String AUTOWARM_COUNT_PARAM = "autowarmCount";
  public static final String SHOW_ITEMS_PARAM = "showItems";

  // contains the statistics objects for all open caches of the same type
  private List<ConcurrentLFUCache.Stats> statsList;

  private long warmupTime = 0;

  private String name;
  private int autowarmCount;
  private State state;
  private CacheRegenerator regenerator;
  private String description = "Concurrent LFU Cache";
  private ConcurrentLFUCache<K, V> cache;
  private int showItems = 0;
  private Boolean timeDecay = true;
  private int maxIdleTimeSec;
  private MetricsMap cacheMap;
  private Set<String> metricNames = ConcurrentHashMap.newKeySet();
  private SolrMetricsContext solrMetricsContext;


  private int maxSize;
  private int minSizeLimit;
  private int initialSize;
  private int acceptableSize;
  private boolean cleanupThread;

  @Override
  public Object init(Map args, Object persistence, CacheRegenerator regenerator) {
    state = State.CREATED;
    this.regenerator = regenerator;
    name = (String) args.get(NAME);
    String str = (String) args.get(SIZE_PARAM);
    maxSize = str == null ? 1024 : Integer.parseInt(str);
    str = (String) args.get(MIN_SIZE_PARAM);
    if (str == null) {
      minSizeLimit = (int) (maxSize * 0.9);
    } else {
      minSizeLimit = Integer.parseInt(str);
    }
    checkAndAdjustLimits();

    str = (String) args.get(ACCEPTABLE_SIZE_PARAM);
    if (str == null) {
      acceptableSize = (int) (maxSize * 0.95);
    } else {
      acceptableSize = Integer.parseInt(str);
    }
    // acceptable limit should be somewhere between minLimit and limit
    acceptableSize = Math.max(minSizeLimit, acceptableSize);

    str = (String) args.get(INITIAL_SIZE_PARAM);
    initialSize = str == null ? maxSize : Integer.parseInt(str);
    str = (String) args.get(AUTOWARM_COUNT_PARAM);
    autowarmCount = str == null ? 0 : Integer.parseInt(str);
    str = (String) args.get(CLEANUP_THREAD_PARAM);
    cleanupThread = str == null ? false : Boolean.parseBoolean(str);

    str = (String) args.get(SHOW_ITEMS_PARAM);
    showItems = str == null ? 0 : Integer.parseInt(str);

    // Don't make this "efficient" by removing the test, default is true and omitting the param will make it false.
    str = (String) args.get(TIME_DECAY_PARAM);
    timeDecay = (str == null) ? true : Boolean.parseBoolean(str);

    str = (String) args.get(MAX_IDLE_TIME_PARAM);
    if (str == null) {
      maxIdleTimeSec = -1;
    } else {
      maxIdleTimeSec = Integer.parseInt(str);
    }
    description = generateDescription();

    cache = new ConcurrentLFUCache<>(maxSize, minSizeLimit, acceptableSize, initialSize,
        cleanupThread, false, null, timeDecay, maxIdleTimeSec);
    cache.setAlive(false);

    statsList = (List<ConcurrentLFUCache.Stats>) persistence;
    if (statsList == null) {
      // must be the first time a cache of this type is being created
      // Use a CopyOnWriteArrayList since puts are very rare and iteration may be a frequent operation
      // because it is used in getStatistics()
      statsList = new CopyOnWriteArrayList<>();

      // the first entry will be for cumulative stats of caches that have been closed.
      statsList.add(new ConcurrentLFUCache.Stats());
    }
    statsList.add(cache.getStats());
    return statsList;
  }

  private String generateDescription() {
    String descr = "Concurrent LFU Cache(maxSize=" + maxSize + ", initialSize=" + initialSize +
        ", minSize=" + minSizeLimit + ", acceptableSize=" + acceptableSize + ", cleanupThread=" + cleanupThread +
        ", timeDecay=" + timeDecay +
        ", maxIdleTime=" + maxIdleTimeSec;
    if (autowarmCount > 0) {
      descr += ", autowarmCount=" + autowarmCount + ", regenerator=" + regenerator;
    }
    descr += ')';
    return descr;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public int size() {
    return cache.size();

  }

  @Override
  public V put(K key, V value) {
    return cache.put(key, value);
  }

  @Override
  public V remove(K key) {
    return cache.remove(key);
  }

  @Override
  public V computeIfAbsent(K key, IOFunction<? super K, ? extends V> mappingFunction) {
    return cache.computeIfAbsent(key, k -> {
      try {
        return mappingFunction.apply(k);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    });
  }

  @Override
  public V get(K key) {
    return cache.get(key);
  }

  @Override
  public void clear() {
    cache.clear();
  }

  @Override
  public void setState(State state) {
    this.state = state;
    cache.setAlive(state == State.LIVE);
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public void warm(SolrIndexSearcher searcher, SolrCache old) {
    if (regenerator == null) return;
    long warmingStartTime = System.nanoTime();
    LFUCache other = (LFUCache) old;
    // warm entries
    if (autowarmCount != 0) {
      int sz = other.size();
      if (autowarmCount != -1) sz = Math.min(sz, autowarmCount);
      Map items = other.cache.getMostUsedItems(sz);
      Map.Entry[] itemsArr = new Map.Entry[items.size()];
      int counter = 0;
      for (Object mapEntry : items.entrySet()) {
        itemsArr[counter++] = (Map.Entry) mapEntry;
      }
      for (int i = itemsArr.length - 1; i >= 0; i--) {
        try {
          boolean continueRegen = regenerator.regenerateItem(searcher,
              this, old, itemsArr[i].getKey(), itemsArr[i].getValue());
          if (!continueRegen) break;
        } catch (Exception e) {
          SolrException.log(log, "Error during auto-warming of key:" + itemsArr[i].getKey(), e);
        }
      }
    }
    warmupTime = TimeUnit.MILLISECONDS.convert(System.nanoTime() - warmingStartTime, TimeUnit.NANOSECONDS);
  }


  @Override
  public void close() throws IOException {
    SolrCache.super.close();
    // add the stats to the cumulative stats object (the first in the statsList)
    statsList.get(0).add(cache.getStats());
    statsList.remove(cache.getStats());
    cache.destroy();
  }

  //////////////////////// SolrInfoMBeans methods //////////////////////
  @Override
  public String getName() {
    return LFUCache.class.getName();
  }

  @Override
  public String getDescription() {
    return description;
  }

  @Override
  public Category getCategory() {
    return Category.CACHE;
  }

  // returns a ratio, not a percent.
  private static String calcHitRatio(long lookups, long hits) {
    if (lookups == 0) return "0.00";
    if (lookups == hits) return "1.00";
    int hundredths = (int) (hits * 100 / lookups);   // rounded down
    if (hundredths < 10) return "0.0" + hundredths;
    return "0." + hundredths;
  }

  @Override
  public SolrMetricsContext getSolrMetricsContext() {
    return solrMetricsContext;
  }

  @Override
  public void initializeMetrics(SolrMetricsContext parentContext, String scope) {
    solrMetricsContext = parentContext.getChildContext(this);
    cacheMap = new MetricsMap((detailed, map) -> {
      if (cache != null) {
        ConcurrentLFUCache.Stats stats = cache.getStats();
        long lookups = stats.getCumulativeLookups();
        long hits = stats.getCumulativeHits();
        long inserts = stats.getCumulativePuts();
        long evictions = stats.getCumulativeEvictions();
        long idleEvictions = stats.getCumulativeIdleEvictions();
        long size = stats.getCurrentSize();

        map.put(LOOKUPS_PARAM, lookups);
        map.put(HITS_PARAM, hits);
        map.put(HIT_RATIO_PARAM, calcHitRatio(lookups, hits));
        map.put(INSERTS_PARAM, inserts);
        map.put(EVICTIONS_PARAM, evictions);
        map.put(SIZE_PARAM, size);
        map.put(MAX_SIZE_PARAM, maxSize);
        map.put(MIN_SIZE_PARAM, minSizeLimit);
        map.put(ACCEPTABLE_SIZE_PARAM, acceptableSize);
        map.put(AUTOWARM_COUNT_PARAM, autowarmCount);
        map.put(CLEANUP_THREAD_PARAM, cleanupThread);
        map.put(SHOW_ITEMS_PARAM, showItems);
        map.put(TIME_DECAY_PARAM, timeDecay);
        map.put(RAM_BYTES_USED_PARAM, ramBytesUsed());
        map.put(MAX_IDLE_TIME_PARAM, maxIdleTimeSec);
        map.put("idleEvictions", idleEvictions);

        map.put("warmupTime", warmupTime);

        long clookups = 0;
        long chits = 0;
        long cinserts = 0;
        long cevictions = 0;
        long cidleEvictions = 0;

        // NOTE: It is safe to iterate on a CopyOnWriteArrayList
        for (ConcurrentLFUCache.Stats statistics : statsList) {
          clookups += statistics.getCumulativeLookups();
          chits += statistics.getCumulativeHits();
          cinserts += statistics.getCumulativePuts();
          cevictions += statistics.getCumulativeEvictions();
          cidleEvictions += statistics.getCumulativeIdleEvictions();
        }
        map.put("cumulative_lookups", clookups);
        map.put("cumulative_hits", chits);
        map.put("cumulative_hitratio", calcHitRatio(clookups, chits));
        map.put("cumulative_inserts", cinserts);
        map.put("cumulative_evictions", cevictions);
        map.put("cumulative_idleEvictions", cidleEvictions);

        if (detailed && showItems != 0) {
          Map items = cache.getMostUsedItems(showItems == -1 ? Integer.MAX_VALUE : showItems);
          for (Map.Entry e : (Set<Map.Entry>) items.entrySet()) {
            Object k = e.getKey();
            Object v = e.getValue();

            String ks = "item_" + k;
            String vs = v.toString();
            map.put(ks, vs);
          }

        }

      }
    });
    solrMetricsContext.gauge(this, cacheMap, true, scope, getCategory().toString());
  }

  // for unit tests only
  MetricsMap getMetricsMap() {
    return cacheMap;
  }

  @Override
  public Set<String> getMetricNames() {
    return metricNames;
  }

  @Override
  public String toString() {
    return name + (cacheMap != null ? cacheMap.getValue().toString() : "");
  }

  @Override
  public long ramBytesUsed() {
    synchronized (statsList) {
      return BASE_RAM_BYTES_USED +
          RamUsageEstimator.sizeOfObject(name) +
          RamUsageEstimator.sizeOfObject(metricNames) +
          RamUsageEstimator.sizeOfObject(statsList) +
          RamUsageEstimator.sizeOfObject(cache);
    }
  }

  @Override
  public int getMaxSize() {
    return maxSize != Integer.MAX_VALUE ? maxSize : -1;
  }

  @Override
  public void setMaxSize(int maxSize) {
    if (maxSize > 0) {
      this.maxSize = maxSize;
    } else {
      this.maxSize = Integer.MAX_VALUE;
    }
    checkAndAdjustLimits();
    cache.setUpperWaterMark(maxSize);
    cache.setLowerWaterMark(minSizeLimit);
    description = generateDescription();
  }

  @Override
  public int getMaxRamMB() {
    return -1;
  }

  @Override
  public void setMaxRamMB(int maxRamMB) {
    // no-op
  }

  private void checkAndAdjustLimits() {
    if (minSizeLimit <= 0) minSizeLimit = 1;
    if (maxSize <= minSizeLimit) {
      if (maxSize > 1) {
        minSizeLimit = maxSize - 1;
      } else {
        maxSize = minSizeLimit + 1;
      }
    }
  }
}
