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

import com.codahale.metrics.MetricRegistry;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.solr.common.SolrException;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.util.ConcurrentLRUCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

/**
 * SolrCache based on ConcurrentLRUCache implementation.
 * <p>
 * This implementation does not use a separate cleanup thread. Instead it uses the calling thread
 * itself to do the cleanup when the size of the cache exceeds certain limits.
 * <p>
 * Also see <a href="http://wiki.apache.org/solr/SolrCaching">SolrCaching</a>
 *
 *
 * @see org.apache.solr.util.ConcurrentLRUCache
 * @see org.apache.solr.search.SolrCache
 * @since solr 1.4
 */
public class FastLRUCache<K, V> extends SolrCacheBase implements SolrCache<K,V>, Accountable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(FastLRUCache.class);

  public static final String MIN_SIZE_PARAM = "minSize";
  public static final String ACCEPTABLE_SIZE_PARAM = "acceptableSize";
  public static final String INITIAL_SIZE_PARAM = "initialSize";
  public static final String CLEANUP_THREAD_PARAM = "cleanupThread";
  public static final String SHOW_ITEMS_PARAM = "showItems";

  // contains the statistics objects for all open caches of the same type
  private List<ConcurrentLRUCache.Stats> statsList;

  private long warmupTime = 0;

  private String description = "Concurrent LRU Cache";
  private ConcurrentLRUCache<K,V> cache;
  private int showItems = 0;

  private long maxRamBytes;
  private int sizeLimit;
  private int minSizeLimit;
  private int initialSize;
  private int acceptableSize;
  private boolean cleanupThread;
  private long ramLowerWatermark;

  private MetricsMap cacheMap;
  private Set<String> metricNames = ConcurrentHashMap.newKeySet();
  private MetricRegistry registry;

  @Override
  public Object init(Map args, Object persistence, CacheRegenerator regenerator) {
    super.init(args, regenerator);
    String str = (String) args.get(SIZE_PARAM);
    sizeLimit = str == null ? 1024 : Integer.parseInt(str);
    str = (String) args.get(MIN_SIZE_PARAM);
    if (str == null) {
      minSizeLimit = (int) (sizeLimit * 0.9);
    } else {
      minSizeLimit = Integer.parseInt(str);
    }
    checkAndAdjustLimits();

    str = (String) args.get(ACCEPTABLE_SIZE_PARAM);
    if (str == null) {
      acceptableSize = (int) (sizeLimit * 0.95);
    } else {
      acceptableSize = Integer.parseInt(str);
    }
    // acceptable limit should be somewhere between minLimit and limit
    acceptableSize = Math.max(minSizeLimit, acceptableSize);

    str = (String) args.get(INITIAL_SIZE_PARAM);
    initialSize = str == null ? sizeLimit : Integer.parseInt(str);
    str = (String) args.get(CLEANUP_THREAD_PARAM);
    cleanupThread = str == null ? false : Boolean.parseBoolean(str);

    str = (String) args.get(SHOW_ITEMS_PARAM);
    showItems = str == null ? 0 : Integer.parseInt(str);

    str = (String) args.get(MAX_RAM_MB_PARAM);
    long maxRamMB = str == null ? -1 : (long) Double.parseDouble(str);
    this.maxRamBytes = maxRamMB < 0 ? Long.MAX_VALUE : maxRamMB * 1024L * 1024L;
    if (maxRamBytes != Long.MAX_VALUE)  {
      ramLowerWatermark = Math.round(maxRamBytes * 0.8);
      description = generateDescription(maxRamBytes, ramLowerWatermark, cleanupThread);
      cache = new ConcurrentLRUCache<>(ramLowerWatermark, maxRamBytes, cleanupThread, null);
    } else  {
      ramLowerWatermark = -1L;
      description = generateDescription(sizeLimit, initialSize, minSizeLimit, acceptableSize, cleanupThread);
      cache = new ConcurrentLRUCache<>(sizeLimit, minSizeLimit, acceptableSize, initialSize, cleanupThread, false, null);
    }

    cache.setAlive(false);

    statsList = (List<ConcurrentLRUCache.Stats>) persistence;
    if (statsList == null) {
      // must be the first time a cache of this type is being created
      // Use a CopyOnWriteArrayList since puts are very rare and iteration may be a frequent operation
      // because it is used in getStatistics()
      statsList = new CopyOnWriteArrayList<>();

      // the first entry will be for cumulative stats of caches that have been closed.
      statsList.add(new ConcurrentLRUCache.Stats());
    }
    statsList.add(cache.getStats());
    return statsList;
  }

  protected String generateDescription() {
    if (maxRamBytes != Long.MAX_VALUE) {
      return generateDescription(maxRamBytes, ramLowerWatermark, cleanupThread);
    } else {
      return generateDescription(sizeLimit, initialSize, minSizeLimit, acceptableSize, cleanupThread);
    }
  }

  /**
   * @return Returns the description of this Cache.
   */
  protected String generateDescription(int limit, int initialSize, int minLimit, int acceptableLimit, boolean newThread) {
    String description = "Concurrent LRU Cache(maxSize=" + limit + ", initialSize=" + initialSize +
        ", minSize="+minLimit + ", acceptableSize="+acceptableLimit+", cleanupThread="+newThread;
    if (isAutowarmingOn()) {
      description += ", " + getAutowarmDescription();
    }
    description += ')';
    return description;
  }

  protected String generateDescription(long maxRamBytes, long ramLowerWatermark, boolean newThread) {
    String description = "Concurrent LRU Cache(ramMinSize=" + ramLowerWatermark + ", ramMaxSize=" + maxRamBytes
        + ", cleanupThread=" + newThread;
    if (isAutowarmingOn()) {
      description += ", " + getAutowarmDescription();
    }
    description += ')';
    return description;
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
  public V get(K key) {
    return cache.get(key);
  }

  @Override
  public void clear() {
    cache.clear();
  }

  @Override
  public void setState(State state) {
    super.setState(state);
    cache.setAlive(state == State.LIVE);
  }

  @Override
  public void warm(SolrIndexSearcher searcher, SolrCache old) {
    if (regenerator == null) return;
    long warmingStartTime = System.nanoTime();
    FastLRUCache other = (FastLRUCache) old;
    // warm entries
    if (isAutowarmingOn()) {
      int sz = autowarm.getWarmCount(other.size());
      Map items = other.cache.getLatestAccessedItems(sz);
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
        }
        catch (Exception e) {
          SolrException.log(log, "Error during auto-warming of key:" + itemsArr[i].getKey(), e);
        }
      }
    }
    warmupTime = TimeUnit.MILLISECONDS.convert(System.nanoTime() - warmingStartTime, TimeUnit.NANOSECONDS);
  }


  @Override
  public void close() {
    // add the stats to the cumulative stats object (the first in the statsList)
    statsList.get(0).add(cache.getStats());
    statsList.remove(cache.getStats());
    cache.destroy();
  }

  //////////////////////// SolrInfoMBeans methods //////////////////////
  @Override
  public String getName() {
    return FastLRUCache.class.getName();
  }

  @Override
  public String getDescription() {
    return description;
  }

  @Override
  public Set<String> getMetricNames() {
    return metricNames;
  }

  @Override
  public void initializeMetrics(SolrMetricManager manager, String registryName, String tag, String scope) {
    registry = manager.registry(registryName);
    cacheMap = new MetricsMap((detailed, map) -> {
      if (cache != null) {
        ConcurrentLRUCache.Stats stats = cache.getStats();
        long lookups = stats.getCumulativeLookups();
        long hits = stats.getCumulativeHits();
        long inserts = stats.getCumulativePuts();
        long evictions = stats.getCumulativeEvictions();
        long size = stats.getCurrentSize();
        long clookups = 0;
        long chits = 0;
        long cinserts = 0;
        long cevictions = 0;

        // NOTE: It is safe to iterate on a CopyOnWriteArrayList
        for (ConcurrentLRUCache.Stats statistiscs : statsList) {
          clookups += statistiscs.getCumulativeLookups();
          chits += statistiscs.getCumulativeHits();
          cinserts += statistiscs.getCumulativePuts();
          cevictions += statistiscs.getCumulativeEvictions();
        }

        map.put("lookups", lookups);
        map.put("hits", hits);
        map.put("hitratio", calcHitRatio(lookups, hits));
        map.put("inserts", inserts);
        map.put("evictions", evictions);
        map.put("size", size);
        map.put("cleanupThread", cleanupThread);
        map.put("ramBytesUsed", ramBytesUsed());
        map.put("maxRamMB", maxRamBytes != Long.MAX_VALUE ? maxRamBytes / 1024L / 1024L : -1L);

        map.put("warmupTime", warmupTime);
        map.put("cumulative_lookups", clookups);
        map.put("cumulative_hits", chits);
        map.put("cumulative_hitratio", calcHitRatio(clookups, chits));
        map.put("cumulative_inserts", cinserts);
        map.put("cumulative_evictions", cevictions);

        if (detailed && showItems != 0) {
          Map items = cache.getLatestAccessedItems( showItems == -1 ? Integer.MAX_VALUE : showItems );
          for (Map.Entry e : (Set <Map.Entry>)items.entrySet()) {
            Object k = e.getKey();
            Object v = e.getValue();

            String ks = "item_" + k;
            String vs = v.toString();
            map.put(ks,vs);
          }

        }
      }
    });
    manager.registerGauge(this, registryName, cacheMap, tag, true, scope, getCategory().toString());
  }


  // for unit tests only
  MetricsMap getMetricsMap() {
    return cacheMap;
  }

  @Override
  public MetricRegistry getMetricRegistry() {
    return registry;
  }

  @Override
  public String toString() {
    return name() + (cacheMap != null ? cacheMap.getValue().toString() : "");
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES_USED +
        RamUsageEstimator.sizeOfObject(cache) +
        RamUsageEstimator.sizeOfObject(statsList);
  }

  @Override
  public Map<String, Object> getResourceLimits() {
    Map<String, Object> limits = new HashMap<>();
    limits.put(SIZE_PARAM, cache.getStats().getCurrentSize());
    limits.put(MIN_SIZE_PARAM, minSizeLimit);
    limits.put(ACCEPTABLE_SIZE_PARAM, acceptableSize);
    limits.put(CLEANUP_THREAD_PARAM, cleanupThread);
    limits.put(SHOW_ITEMS_PARAM, showItems);
    limits.put(MAX_RAM_MB_PARAM, maxRamBytes != Long.MAX_VALUE ? maxRamBytes / 1024L / 1024L : -1L);
    return limits;
  }

  @Override
  public void setResourceLimit(String limitName, Object val) {
    if (CLEANUP_THREAD_PARAM.equals(limitName)) {
      Boolean value;
      try {
        value = Boolean.parseBoolean(val.toString());
        cleanupThread = value;
        cache.setRunCleanupThread(cleanupThread);
      } catch (Exception e) {
        throw new IllegalArgumentException("Invalid new value for boolean limit '" + limitName + "': " + val);
      }
    }
    Number value;
    try {
      value = Long.parseLong(String.valueOf(val));
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid new value for numeric limit '" + limitName +"': " + val);
    }
    if (!limitName.equals(MAX_RAM_MB_PARAM)) {
      if (value.intValue() <= 1) {
        throw new IllegalArgumentException("Invalid new value for numeric limit '" + limitName +"': " + value);
      }
    }
    if (value.longValue() > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("Invalid new value for numeric limit '" + limitName +"': " + value);
    }
    switch (limitName) {
      case SIZE_PARAM:
        sizeLimit = value.intValue();
        checkAndAdjustLimits();
        cache.setUpperWaterMark(sizeLimit);
        cache.setLowerWaterMark(minSizeLimit);
        break;
      case MIN_SIZE_PARAM:
        minSizeLimit = value.intValue();
        checkAndAdjustLimits();
        cache.setUpperWaterMark(sizeLimit);
        cache.setLowerWaterMark(minSizeLimit);
        break;
      case ACCEPTABLE_SIZE_PARAM:
        acceptableSize = value.intValue();
        acceptableSize = Math.max(minSizeLimit, acceptableSize);
        cache.setAcceptableWaterMark(acceptableSize);
        break;
      case MAX_RAM_MB_PARAM:
        long maxRamMB = value.intValue();
        maxRamBytes = maxRamMB < 0 ? Long.MAX_VALUE : maxRamMB * 1024L * 1024L;
        if (maxRamMB < 0) {
          ramLowerWatermark = Long.MIN_VALUE;
        } else {
          ramLowerWatermark = Math.round(maxRamBytes * 0.8);
        }
        cache.setRamUpperWatermark(maxRamBytes);
        cache.setRamLowerWatermark(ramLowerWatermark);
        break;
      case SHOW_ITEMS_PARAM:
        showItems = value.intValue();
        break;
      default:
        throw new IllegalArgumentException("Unsupported limit '" + limitName + "'");
    }
    description = generateDescription();
  }

  private void checkAndAdjustLimits() {
    if (minSizeLimit <= 0) minSizeLimit = 1;
    if (sizeLimit <= minSizeLimit) {
      if (sizeLimit > 1) {
        minSizeLimit = sizeLimit - 1;
      } else {
        sizeLimit = minSizeLimit + 1;
      }
    }
  }
}



