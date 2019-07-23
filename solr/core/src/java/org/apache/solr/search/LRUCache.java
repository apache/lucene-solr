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
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import com.codahale.metrics.MetricRegistry;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.solr.common.SolrException;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.lucene.util.RamUsageEstimator.LINKED_HASHTABLE_RAM_BYTES_PER_ENTRY;
import static org.apache.lucene.util.RamUsageEstimator.QUERY_DEFAULT_RAM_BYTES_USED;

/**
 *
 */
public class LRUCache<K,V> extends SolrCacheBase implements SolrCache<K,V>, Accountable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(LRUCache.class);

  /* An instance of this class will be shared across multiple instances
   * of an LRUCache at the same time.  Make sure everything is thread safe.
   */
  private static class CumulativeStats {
    LongAdder lookups = new LongAdder();
    LongAdder hits = new LongAdder();
    LongAdder inserts = new LongAdder();
    LongAdder evictions = new LongAdder();
    LongAdder evictionsRamUsage = new LongAdder();
  }

  private CumulativeStats stats;

  // per instance stats.  The synchronization used for the map will also be
  // used for updating these statistics (and hence they are not AtomicLongs
  private long lookups;
  private long hits;
  private long inserts;
  private long evictions;
  private long evictionsRamUsage;

  private long warmupTime = 0;

  private Map<K,V> map;
  private String description="LRU Cache";
  private MetricsMap cacheMap;
  private Set<String> metricNames = ConcurrentHashMap.newKeySet();
  private MetricRegistry registry;
  private int sizeLimit;
  private int initialSize;

  private long maxRamBytes = Long.MAX_VALUE;
  // The synchronization used for the map will be used to update this,
  // hence not an AtomicLong
  private long ramBytesUsed = 0;

  @Override
  public Object init(Map args, Object persistence, CacheRegenerator regenerator) {
    super.init(args, regenerator);
    String str = (String)args.get(SIZE_PARAM);
    this.sizeLimit = str==null ? 1024 : Integer.parseInt(str);
    str = (String)args.get("initialSize");
    initialSize = Math.min(str==null ? 1024 : Integer.parseInt(str), sizeLimit);
    str = (String) args.get(MAX_RAM_MB_PARAM);
    this.maxRamBytes = str == null ? Long.MAX_VALUE : (long) (Double.parseDouble(str) * 1024L * 1024L);
    description = generateDescription();

    map = new LinkedHashMap<K,V>(initialSize, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry eldest) {
          if (ramBytesUsed > getMaxRamBytes()) {
            Iterator<Map.Entry<K, V>> iterator = entrySet().iterator();
            do {
              Map.Entry<K, V> entry = iterator.next();
              long bytesToDecrement = RamUsageEstimator.sizeOfObject(entry.getKey(), QUERY_DEFAULT_RAM_BYTES_USED);
              bytesToDecrement += RamUsageEstimator.sizeOfObject(entry.getValue(), QUERY_DEFAULT_RAM_BYTES_USED);
              bytesToDecrement += LINKED_HASHTABLE_RAM_BYTES_PER_ENTRY;
              ramBytesUsed -= bytesToDecrement;
              iterator.remove();
              evictions++;
              evictionsRamUsage++;
              stats.evictions.increment();
              stats.evictionsRamUsage.increment();
            } while (iterator.hasNext() && ramBytesUsed > getMaxRamBytes());
            // must return false according to javadocs of removeEldestEntry if we're modifying
            // the map ourselves
            return false;
          } else if (size() > getSizeLimit()) {
            Iterator<Map.Entry<K, V>> iterator = entrySet().iterator();
            do {
              Map.Entry<K, V> entry = iterator.next();
              long bytesToDecrement = RamUsageEstimator.sizeOfObject(entry.getKey(), QUERY_DEFAULT_RAM_BYTES_USED);
              bytesToDecrement += RamUsageEstimator.sizeOfObject(entry.getValue(), QUERY_DEFAULT_RAM_BYTES_USED);
              bytesToDecrement += LINKED_HASHTABLE_RAM_BYTES_PER_ENTRY;
              ramBytesUsed -= bytesToDecrement;
              // increment evictions regardless of state.
              // this doesn't need to be synchronized because it will
              // only be called in the context of a higher level synchronized block.
              iterator.remove();
              evictions++;
              stats.evictions.increment();
            } while (iterator.hasNext() && size() > getSizeLimit());
            // must return false according to javadocs of removeEldestEntry if we're modifying
            // the map ourselves
            return false;
          }
          // neither size nor RAM exceeded - ok to keep the entry
          return false;
        }
      };

    if (persistence==null) {
      // must be the first time a cache of this type is being created
      persistence = new CumulativeStats();
    }

    stats = (CumulativeStats)persistence;

    return persistence;
  }

  public int getSizeLimit() {
    return sizeLimit;
  }

  public long getMaxRamBytes() {
    return maxRamBytes;
  }

  /**
   * 
   * @return Returns the description of this cache. 
   */
  private String generateDescription() {
    String description = "LRU Cache(maxSize=" + getSizeLimit() + ", initialSize=" + initialSize;
    if (isAutowarmingOn()) {
      description += ", " + getAutowarmDescription();
    }
    if (getMaxRamBytes() != Long.MAX_VALUE)  {
      description += ", maxRamMB=" + (getMaxRamBytes() / 1024L / 1024L);
    }
    description += ')';
    return description;
  }

  @Override
  public int size() {
    synchronized(map) {
      return map.size();
    }
  }

  @Override
  public V put(K key, V value) {
    if (sizeLimit == Integer.MAX_VALUE && maxRamBytes == Long.MAX_VALUE) {
      throw new IllegalStateException("Cache: " + getName() + " has neither size nor RAM limit!");
    }
    synchronized (map) {
      if (getState() == State.LIVE) {
        stats.inserts.increment();
      }

      // increment local inserts regardless of state???
      // it does make it more consistent with the current size...
      inserts++;

      // important to calc and add new ram bytes first so that removeEldestEntry can compare correctly
      long keySize = RamUsageEstimator.sizeOfObject(key, QUERY_DEFAULT_RAM_BYTES_USED);
      long valueSize = RamUsageEstimator.sizeOfObject(value, QUERY_DEFAULT_RAM_BYTES_USED);
      ramBytesUsed += keySize + valueSize + LINKED_HASHTABLE_RAM_BYTES_PER_ENTRY;
      V old = map.put(key, value);
      if (old != null) {
        long bytesToDecrement = RamUsageEstimator.sizeOfObject(old, QUERY_DEFAULT_RAM_BYTES_USED);
        // the key existed in the map but we added its size before the put, so let's back out
        bytesToDecrement += LINKED_HASHTABLE_RAM_BYTES_PER_ENTRY;
        bytesToDecrement += RamUsageEstimator.sizeOfObject(key, QUERY_DEFAULT_RAM_BYTES_USED);
        ramBytesUsed -= bytesToDecrement;
      }
      return old;
    }
  }

  @Override
  public V get(K key) {
    synchronized (map) {
      V val = map.get(key);
      if (getState() == State.LIVE) {
        // only increment lookups and hits if we are live.
        lookups++;
        stats.lookups.increment();
        if (val!=null) {
          hits++;
          stats.hits.increment();
        }
      }
      return val;
    }
  }

  @Override
  public void clear() {
    synchronized(map) {
      map.clear();
      ramBytesUsed = 0;
    }
  }

  @Override
  public void warm(SolrIndexSearcher searcher, SolrCache<K,V> old) {
    if (regenerator==null) return;
    long warmingStartTime = System.nanoTime();
    LRUCache<K,V> other = (LRUCache<K,V>)old;

    // warm entries
    if (isAutowarmingOn()) {
      Object[] keys,vals = null;

      // Don't do the autowarming in the synchronized block, just pull out the keys and values.
      synchronized (other.map) {
        
        int sz = autowarm.getWarmCount(other.map.size());
        
        keys = new Object[sz];
        vals = new Object[sz];

        Iterator<Map.Entry<K, V>> iter = other.map.entrySet().iterator();

        // iteration goes from oldest (least recently used) to most recently used,
        // so we need to skip over the oldest entries.
        int skip = other.map.size() - sz;
        for (int i=0; i<skip; i++) iter.next();


        for (int i=0; i<sz; i++) {
          Map.Entry<K,V> entry = iter.next();
          keys[i]=entry.getKey();
          vals[i]=entry.getValue();
        }
      }

      // autowarm from the oldest to the newest entries so that the ordering will be
      // correct in the new cache.
      for (int i=0; i<keys.length; i++) {
        try {
          boolean continueRegen = regenerator.regenerateItem(searcher, this, old, keys[i], vals[i]);
          if (!continueRegen) break;
        }
        catch (Exception e) {
          SolrException.log(log,"Error during auto-warming of key:" + keys[i], e);
        }
      }
    }

    warmupTime = TimeUnit.MILLISECONDS.convert(System.nanoTime() - warmingStartTime, TimeUnit.NANOSECONDS);
  }

  @Override
  public void close() {

  }


  //////////////////////// SolrInfoMBeans methods //////////////////////


  @Override
  public String getName() {
    return LRUCache.class.getName();
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
    cacheMap = new MetricsMap((detailed, res) -> {
      synchronized (map) {
        res.put("lookups", lookups);
        res.put("hits", hits);
        res.put("hitratio", calcHitRatio(lookups,hits));
        res.put("inserts", inserts);
        res.put("evictions", evictions);
        res.put("size", map.size());
        res.put("ramBytesUsed", ramBytesUsed());
        res.put("maxRamMB", maxRamBytes != Long.MAX_VALUE ? maxRamBytes / 1024L / 1024L : -1L);
        res.put("evictionsRamUsage", evictionsRamUsage);
      }
      res.put("warmupTime", warmupTime);

      long clookups = stats.lookups.longValue();
      long chits = stats.hits.longValue();
      res.put("cumulative_lookups", clookups);
      res.put("cumulative_hits", chits);
      res.put("cumulative_hitratio", calcHitRatio(clookups, chits));
      res.put("cumulative_inserts", stats.inserts.longValue());
      res.put("cumulative_evictions", stats.evictions.longValue());
      res.put("cumulative_evictionsRamUsage", stats.evictionsRamUsage.longValue());
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
    synchronized (map)  {
      return BASE_RAM_BYTES_USED + ramBytesUsed;
    }
  }

  @Override
  public Collection<Accountable> getChildResources() {
    synchronized (map)  {
      return Accountables.namedAccountables(getName(), (Map<?, ? extends Accountable>) map);
    }
  }

  @Override
  public Map<String, Object> getResourceLimits() {
    Map<String, Object> limits = new HashMap<>();
    limits.put(SIZE_PARAM, sizeLimit);
    limits.put(MAX_RAM_MB_PARAM, maxRamBytes != Long.MAX_VALUE ? maxRamBytes / 1024L / 1024L : -1L);
    return limits;
  }

  @Override
  public void setResourceLimit(String limitName, Object val) {
    if (!(val instanceof Number)) {
      try {
        val = Long.parseLong(String.valueOf(val));
      } catch (Exception e) {
        throw new IllegalArgumentException("Unsupported value type (not a number) for limit '" + limitName + "': " + val + " (" + val.getClass().getName() + ")");
      }
    }
    Number value = (Number)val;
    if (value.longValue() > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("Invalid new value for limit '" + limitName +"': " + value);
    }
    switch (limitName) {
      case SIZE_PARAM:
        if (value.intValue() > 0) {
          sizeLimit = value.intValue();
        } else {
          sizeLimit = Integer.MAX_VALUE;
        }
        break;
      case MAX_RAM_MB_PARAM:
        if (value.intValue() > 0) {
          maxRamBytes = value.intValue() * 1024L * 1024L;
        } else {
          maxRamBytes = Long.MAX_VALUE;
        }
        break;
      default:
        throw new IllegalArgumentException("Unsupported limit name '" + limitName + "'");
    }
    description = generateDescription();
  }
}
