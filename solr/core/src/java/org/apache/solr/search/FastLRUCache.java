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

  // contains the statistics objects for all open caches of the same type
  private List<ConcurrentLRUCache.Stats> statsList;

  private long warmupTime = 0;

  private String description = "Concurrent LRU Cache";
  private ConcurrentLRUCache<K,V> cache;
  private int showItems = 0;

  private long maxRamBytes;

  private MetricsMap cacheMap;
  private Set<String> metricNames = ConcurrentHashMap.newKeySet();
  private MetricRegistry registry;

  @Override
  public Object init(Map args, Object persistence, CacheRegenerator regenerator) {
    super.init(args, regenerator);
    String str = (String) args.get("size");
    int limit = str == null ? 1024 : Integer.parseInt(str);
    int minLimit;
    str = (String) args.get("minSize");
    if (str == null) {
      minLimit = (int) (limit * 0.9);
    } else {
      minLimit = Integer.parseInt(str);
    }
    if (minLimit <= 0) minLimit = 1;
    if (limit <= minLimit) limit=minLimit+1;

    int acceptableLimit;
    str = (String) args.get("acceptableSize");
    if (str == null) {
      acceptableLimit = (int) (limit * 0.95);
    } else {
      acceptableLimit = Integer.parseInt(str);
    }
    // acceptable limit should be somewhere between minLimit and limit
    acceptableLimit = Math.max(minLimit, acceptableLimit);

    str = (String) args.get("initialSize");
    final int initialSize = str == null ? limit : Integer.parseInt(str);
    str = (String) args.get("cleanupThread");
    boolean newThread = str == null ? false : Boolean.parseBoolean(str);

    str = (String) args.get("showItems");
    showItems = str == null ? 0 : Integer.parseInt(str);

    str = (String) args.get("maxRamMB");
    this.maxRamBytes = str == null ? Long.MAX_VALUE : (long) (Double.parseDouble(str) * 1024L * 1024L);
    if (maxRamBytes != Long.MAX_VALUE)  {
      long ramLowerWatermark = Math.round(maxRamBytes * 0.8);
      description = generateDescription(maxRamBytes, ramLowerWatermark, newThread);
      cache = new ConcurrentLRUCache<K, V>(ramLowerWatermark, maxRamBytes, newThread, null);
    } else  {
      description = generateDescription(limit, initialSize, minLimit, acceptableLimit, newThread);
      cache = new ConcurrentLRUCache<>(limit, minLimit, acceptableLimit, initialSize, newThread, false, null);
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
}



