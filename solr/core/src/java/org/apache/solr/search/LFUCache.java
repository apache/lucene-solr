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
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.MetricRegistry;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.solr.common.SolrException;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.util.ConcurrentLFUCache;
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
  private MetricsMap cacheMap;
  private Set<String> metricNames = ConcurrentHashMap.newKeySet();
  private MetricRegistry registry;

  private int sizeLimit;
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
    str = (String) args.get(AUTOWARM_COUNT_PARAM);
    autowarmCount = str == null ? 0 : Integer.parseInt(str);
    str = (String) args.get(CLEANUP_THREAD_PARAM);
    cleanupThread = str == null ? false : Boolean.parseBoolean(str);

    str = (String) args.get(SHOW_ITEMS_PARAM);
    showItems = str == null ? 0 : Integer.parseInt(str);

    // Don't make this "efficient" by removing the test, default is true and omitting the param will make it false.
    str = (String) args.get(TIME_DECAY_PARAM);
    timeDecay = (str == null) ? true : Boolean.parseBoolean(str);

    description = generateDescription();

    cache = new ConcurrentLFUCache<>(sizeLimit, minSizeLimit, acceptableSize, initialSize, cleanupThread, false, null, timeDecay);
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
    String descr = "Concurrent LFU Cache(maxSize=" + sizeLimit + ", initialSize=" + initialSize +
        ", minSize=" + minSizeLimit + ", acceptableSize=" + acceptableSize + ", cleanupThread=" + cleanupThread +
        ", timeDecay=" + Boolean.toString(timeDecay);
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
  public void close() {
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
  public void initializeMetrics(SolrMetricManager manager, String registryName, String tag, String scope) {
    registry = manager.registry(registryName);
    cacheMap = new MetricsMap((detailed, map) -> {
      if (cache != null) {
        ConcurrentLFUCache.Stats stats = cache.getStats();
        long lookups = stats.getCumulativeLookups();
        long hits = stats.getCumulativeHits();
        long inserts = stats.getCumulativePuts();
        long evictions = stats.getCumulativeEvictions();
        long size = stats.getCurrentSize();

        map.put("lookups", lookups);
        map.put("hits", hits);
        map.put("hitratio", calcHitRatio(lookups, hits));
        map.put("inserts", inserts);
        map.put("evictions", evictions);
        map.put("size", size);

        map.put("warmupTime", warmupTime);
        map.put("timeDecay", timeDecay);
        map.put("cleanupThread", cleanupThread);

        long clookups = 0;
        long chits = 0;
        long cinserts = 0;
        long cevictions = 0;

        // NOTE: It is safe to iterate on a CopyOnWriteArrayList
        for (ConcurrentLFUCache.Stats statistics : statsList) {
          clookups += statistics.getCumulativeLookups();
          chits += statistics.getCumulativeHits();
          cinserts += statistics.getCumulativePuts();
          cevictions += statistics.getCumulativeEvictions();
        }
        map.put("cumulative_lookups", clookups);
        map.put("cumulative_hits", chits);
        map.put("cumulative_hitratio", calcHitRatio(clookups, chits));
        map.put("cumulative_inserts", cinserts);
        map.put("cumulative_evictions", cevictions);
        map.put("ramBytesUsed", ramBytesUsed());

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
    manager.registerGauge(this, registryName, cacheMap, tag, true, scope, getCategory().toString());
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
  public MetricRegistry getMetricRegistry() {
    return registry;
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
  public Map<String, Object> getResourceLimits() {
    Map<String, Object> limits = new HashMap<>();
    limits.put(SIZE_PARAM, cache.getStats().getCurrentSize());
    limits.put(MIN_SIZE_PARAM, minSizeLimit);
    limits.put(ACCEPTABLE_SIZE_PARAM, acceptableSize);
    limits.put(AUTOWARM_COUNT_PARAM, autowarmCount);
    limits.put(CLEANUP_THREAD_PARAM, cleanupThread);
    limits.put(SHOW_ITEMS_PARAM, showItems);
    limits.put(TIME_DECAY_PARAM, timeDecay);
    return limits;
  }

  @Override
  public synchronized void setResourceLimit(String limitName, Object val) {
    if (TIME_DECAY_PARAM.equals(limitName) || CLEANUP_THREAD_PARAM.equals(limitName)) {
      Boolean value;
      try {
        value = Boolean.parseBoolean(String.valueOf(val));
      } catch (Exception e) {
        throw new IllegalArgumentException("Invalid value of boolean limit '" + limitName + "': " + val);
      }
      switch (limitName) {
        case TIME_DECAY_PARAM:
          timeDecay = value;
          cache.setTimeDecay(timeDecay);
          break;
        case CLEANUP_THREAD_PARAM:
          cleanupThread = value;
          cache.setRunCleanupThread(cleanupThread);
          break;
      }
    } else {
      Number value;
      try {
        value = Long.parseLong(String.valueOf(val));
      } catch (Exception e) {
        throw new IllegalArgumentException("Invalid new value for numeric limit '" + limitName +"': " + val);
      }
      if (value.intValue() <= 1 || value.longValue() > Integer.MAX_VALUE) {
        throw new IllegalArgumentException("Out of range new value for numeric limit '" + limitName +"': " + value);
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
        case AUTOWARM_COUNT_PARAM:
          autowarmCount = value.intValue();
          break;
        case SHOW_ITEMS_PARAM:
          showItems = value.intValue();
          break;
        default:
          throw new IllegalArgumentException("Unsupported numeric limit '" + limitName + "'");
      }
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
