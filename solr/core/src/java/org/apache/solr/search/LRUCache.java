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
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.util.IOFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.lucene.util.RamUsageEstimator.LINKED_HASHTABLE_RAM_BYTES_PER_ENTRY;
import static org.apache.lucene.util.RamUsageEstimator.QUERY_DEFAULT_RAM_BYTES_USED;

/**
 *
 * @deprecated This cache implementation is deprecated and will be removed in Solr 9.0.
 * Use {@link CaffeineCache} instead.
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
    LongAdder evictionsIdleTime = new LongAdder();
  }

  private CumulativeStats stats;

  // per instance stats.  The synchronization used for the map will also be
  // used for updating these statistics (and hence they are not AtomicLongs
  private long lookups;
  private long hits;
  private long inserts;
  private long evictions;
  private long evictionsRamUsage;
  private long evictionsIdleTime;

  private long warmupTime = 0;

  private Map<K, CacheValue<V>> map;
  private String description="LRU Cache";
  private MetricsMap cacheMap;
  private Set<String> metricNames = ConcurrentHashMap.newKeySet();
  private SolrMetricsContext solrMetricsContext;
  private int maxSize;
  private int initialSize;

  private long maxRamBytes = Long.MAX_VALUE;
  private long maxIdleTimeNs;
  private final TimeSource timeSource = TimeSource.NANO_TIME;
  private long oldestEntry = 0L;
  // for unit testing
  private boolean syntheticEntries = false;

  // The synchronization used for the map will be used to update this,
  // hence not an AtomicLong
  private long ramBytesUsed = 0L;

  public static final class CacheValue<V> implements Accountable {
    public static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(CacheValue.class);
    final long ramBytesUsed;
    public final long createTime;
    public final V value;

    public CacheValue(V value, long createTime) {
      this.value = value;
      this.createTime = createTime;
      ramBytesUsed = BASE_RAM_BYTES_USED +
          RamUsageEstimator.sizeOfObject(value, QUERY_DEFAULT_RAM_BYTES_USED);
    }

    @Override
    public long ramBytesUsed() {
      return ramBytesUsed;
    }
  }

  @Override
  public Object init(Map args, Object persistence, CacheRegenerator regenerator) {
    super.init(args, regenerator);
    String str = (String)args.get(SIZE_PARAM);
    this.maxSize = str==null ? 1024 : Integer.parseInt(str);
    str = (String)args.get("initialSize");
    initialSize = Math.min(str==null ? 1024 : Integer.parseInt(str), maxSize);
    str = (String) args.get(MAX_RAM_MB_PARAM);
    this.maxRamBytes = str == null ? Long.MAX_VALUE : (long) (Double.parseDouble(str) * 1024L * 1024L);
    str = (String) args.get(MAX_IDLE_TIME_PARAM);
    if (str == null) {
      maxIdleTimeNs = Long.MAX_VALUE;
    } else {
      int maxIdleTime = Integer.parseInt(str);
      if (maxIdleTime > 0) {
        maxIdleTimeNs = TimeUnit.NANOSECONDS.convert(Integer.parseInt(str), TimeUnit.SECONDS);
      } else {
        maxIdleTimeNs = Long.MAX_VALUE;
      }
    }
    description = generateDescription();

    map = new LinkedHashMap<K, CacheValue<V>>(initialSize, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry eldest) {
          // remove items older than maxIdleTimeNs
          if (maxIdleTimeNs != Long.MAX_VALUE) {
            long idleCutoff = timeSource.getEpochTimeNs() - maxIdleTimeNs;
            if (oldestEntry < idleCutoff) {
              long currentOldestEntry = Long.MAX_VALUE;
              Iterator<Map.Entry<K, CacheValue<V>>> iterator = entrySet().iterator();
              while (iterator.hasNext()) {
                Map.Entry<K, CacheValue<V>> entry = iterator.next();
                if (entry.getValue().createTime < idleCutoff) {
                  long bytesToDecrement = RamUsageEstimator.sizeOfObject(entry.getKey(), QUERY_DEFAULT_RAM_BYTES_USED);
                  bytesToDecrement += RamUsageEstimator.sizeOfObject(entry.getValue(), QUERY_DEFAULT_RAM_BYTES_USED);
                  bytesToDecrement += LINKED_HASHTABLE_RAM_BYTES_PER_ENTRY;
                  ramBytesUsed -= bytesToDecrement;
                  iterator.remove();
                  evictions++;
                  evictionsIdleTime++;
                  stats.evictionsIdleTime.increment();
                  stats.evictions.increment();
                } else {
                  if (syntheticEntries) {
                    // no guarantee on the actual create time - make a full sweep
                    if (currentOldestEntry > entry.getValue().createTime) {
                      currentOldestEntry = entry.getValue().createTime;
                    }
                  } else {
                    // iterator is sorted by insertion order (and time)
                    // so we can quickly terminate the sweep
                    currentOldestEntry = entry.getValue().createTime;
                    break;
                  }
                }
              }
              if (currentOldestEntry != Long.MAX_VALUE) {
                oldestEntry = currentOldestEntry;
              }
            }
          }
          if (ramBytesUsed > getMaxRamBytes()) {
            Iterator<Map.Entry<K, CacheValue<V>>> iterator = entrySet().iterator();
            do {
              Map.Entry<K, CacheValue<V>> entry = iterator.next();
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
          } else if (size() > getMaxSize()) {
            Iterator<Map.Entry<K, CacheValue<V>>> iterator = entrySet().iterator();
            do {
              Map.Entry<K, CacheValue<V>> entry = iterator.next();
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
            } while (iterator.hasNext() && size() > getMaxSize());
          }
          // must return false according to javadocs of removeEldestEntry if we're modifying
          // the map ourselves
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

  /**
   * Visible for testing. This flag tells the eviction code that (unlike with real entries)
   * there's no guarantee on the order of entries being inserted with monotonically ascending creation
   * time. Setting this to true causes a full sweep when looking for entries to evict.
   * @lucene.internal
   */
  public void setSyntheticEntries(boolean syntheticEntries) {
    this.syntheticEntries = syntheticEntries;
  }

  public long getMaxRamBytes() {
    return maxRamBytes;
  }

  /**
   *
   * @return Returns the description of this cache.
   */
  private String generateDescription() {
    String description = "LRU Cache(maxSize=" + getMaxSize() + ", initialSize=" + initialSize;
    if (isAutowarmingOn()) {
      description += ", " + getAutowarmDescription();
    }
    if (getMaxRamBytes() != Long.MAX_VALUE)  {
      description += ", maxRamMB=" + (getMaxRamBytes() / 1024L / 1024L);
    }
    if (maxIdleTimeNs != Long.MAX_VALUE) {
      description += ", " + MAX_IDLE_TIME_PARAM + "=" + TimeUnit.SECONDS.convert(maxIdleTimeNs, TimeUnit.NANOSECONDS);
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
  public V computeIfAbsent(K key, IOFunction<? super K, ? extends V> mappingFunction) {
    synchronized (map) {
      if (getState() == State.LIVE) {
        lookups++;
        stats.lookups.increment();
      }
      AtomicBoolean newEntry = new AtomicBoolean();
      CacheValue<V> entry = map.computeIfAbsent(key, k -> {
        V value;
        try {
          value = mappingFunction.apply(k);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
        // preserve the semantics of computeIfAbsent
        if (value == null) {
          return null;
        }
        CacheValue<V> cacheValue = new CacheValue<>(value, timeSource.getEpochTimeNs());
        if (getState() == State.LIVE) {
          stats.inserts.increment();
        }
        if (syntheticEntries) {
          if (cacheValue.createTime < oldestEntry) {
            oldestEntry = cacheValue.createTime;
          }
        }
        // increment local inserts regardless of state???
        // it does make it more consistent with the current size...
        inserts++;

        // important to calc and add new ram bytes first so that removeEldestEntry can compare correctly
        long keySize = RamUsageEstimator.sizeOfObject(key, QUERY_DEFAULT_RAM_BYTES_USED);
        long valueSize = RamUsageEstimator.sizeOfObject(cacheValue, QUERY_DEFAULT_RAM_BYTES_USED);
        ramBytesUsed += keySize + valueSize + LINKED_HASHTABLE_RAM_BYTES_PER_ENTRY;
        newEntry.set(true);
        return cacheValue;
      });
      if (!newEntry.get()) {
        if (getState() == State.LIVE) {
          hits++;
          stats.hits.increment();
        }
      }
      return entry != null ? entry.value : null;
    }
  }

  @Override
  public V remove(K key) {
    synchronized (map) {
      CacheValue<V> entry = map.remove(key);
      if (entry != null) {
        long delta = RamUsageEstimator.sizeOfObject(key, QUERY_DEFAULT_RAM_BYTES_USED)
            + RamUsageEstimator.sizeOfObject(entry, QUERY_DEFAULT_RAM_BYTES_USED)
            + LINKED_HASHTABLE_RAM_BYTES_PER_ENTRY;
        ramBytesUsed -= delta;
        return entry.value;
      } else {
        return null;
      }
    }
  }

  @Override
  public V put(K key, V value) {
    if (maxSize == Integer.MAX_VALUE && maxRamBytes == Long.MAX_VALUE) {
      throw new IllegalStateException("Cache: " + getName() + " has neither size nor RAM limit!");
    }
    CacheValue<V> cacheValue = new CacheValue<>(value, timeSource.getEpochTimeNs());
    return putCacheValue(key, cacheValue);
  }

  /**
   * Visible for testing to create synthetic cache entries.
   * @lucene.internal
   */
  public V putCacheValue(K key, CacheValue<V> cacheValue) {
    synchronized (map) {
      if (getState() == State.LIVE) {
        stats.inserts.increment();
      }

      if (syntheticEntries) {
        if (cacheValue.createTime < oldestEntry) {
          oldestEntry = cacheValue.createTime;
        }
      }

      // increment local inserts regardless of state???
      // it does make it more consistent with the current size...
      inserts++;

      // important to calc and add new ram bytes first so that removeEldestEntry can compare correctly
      long keySize = RamUsageEstimator.sizeOfObject(key, QUERY_DEFAULT_RAM_BYTES_USED);
      long valueSize = RamUsageEstimator.sizeOfObject(cacheValue, QUERY_DEFAULT_RAM_BYTES_USED);
      ramBytesUsed += keySize + valueSize + LINKED_HASHTABLE_RAM_BYTES_PER_ENTRY;
      CacheValue<V> old = map.put(key, cacheValue);
      if (old != null) {
        long bytesToDecrement = RamUsageEstimator.sizeOfObject(old, QUERY_DEFAULT_RAM_BYTES_USED);
        // the key existed in the map but we added its size before the put, so let's back out
        bytesToDecrement += LINKED_HASHTABLE_RAM_BYTES_PER_ENTRY;
        bytesToDecrement += RamUsageEstimator.sizeOfObject(key, QUERY_DEFAULT_RAM_BYTES_USED);
        ramBytesUsed -= bytesToDecrement;
      }
      return old == null ? null : old.value;
    }
  }

  @Override
  public V get(K key) {
    synchronized (map) {
      CacheValue<V> val = map.get(key);
      if (getState() == State.LIVE) {
        // only increment lookups and hits if we are live.
        lookups++;
        stats.lookups.increment();
        if (val!=null) {
          hits++;
          stats.hits.increment();
        }
      }
      return val == null ? null : val.value;
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

        Iterator<Map.Entry<K, CacheValue<V>>> iter = other.map.entrySet().iterator();

        // iteration goes from oldest (least recently used) to most recently used,
        // so we need to skip over the oldest entries.
        int skip = other.map.size() - sz;
        for (int i=0; i<skip; i++) iter.next();


        for (int i=0; i<sz; i++) {
          Map.Entry<K, CacheValue<V>> entry = iter.next();
          keys[i]=entry.getKey();
          vals[i]=entry.getValue().value;
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
  public SolrMetricsContext getSolrMetricsContext() {
    return solrMetricsContext;
  }

  @Override
  public void initializeMetrics(SolrMetricsContext parentContext, String scope) {
    solrMetricsContext = parentContext.getChildContext(this);
    cacheMap = new MetricsMap((detailed, res) -> {
      synchronized (map) {
        res.put(LOOKUPS_PARAM, lookups);
        res.put(HITS_PARAM, hits);
        res.put(HIT_RATIO_PARAM, calcHitRatio(lookups,hits));
        res.put(INSERTS_PARAM, inserts);
        res.put(EVICTIONS_PARAM, evictions);
        res.put(SIZE_PARAM, map.size());
        res.put(RAM_BYTES_USED_PARAM, ramBytesUsed());
        res.put(MAX_RAM_MB_PARAM, getMaxRamMB());
        res.put(MAX_SIZE_PARAM, maxSize);
        res.put(MAX_IDLE_TIME_PARAM, maxIdleTimeNs != Long.MAX_VALUE ?
            TimeUnit.SECONDS.convert(maxIdleTimeNs, TimeUnit.NANOSECONDS) : -1);
        res.put("evictionsRamUsage", evictionsRamUsage);
        res.put("evictionsIdleTime", evictionsIdleTime);
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
      res.put("cumulative_evictionsIdleTime", stats.evictionsIdleTime.longValue());
    });
    solrMetricsContext.gauge(this, cacheMap, true, scope, getCategory().toString());
  }

  // for unit tests only
  MetricsMap getMetricsMap() {
    return cacheMap;
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
    description = generateDescription();
  }

  @Override
  public int getMaxRamMB() {
    return maxRamBytes != Long.MAX_VALUE ? (int) (maxRamBytes / 1024L / 1024L) : -1;
  }

  @Override
  public void setMaxRamMB(int maxRamMB) {
    if (maxRamMB > 0) {
      maxRamBytes = maxRamMB * 1024L * 1024L;
    } else {
      maxRamBytes = Long.MAX_VALUE;
    }
    description = generateDescription();
  }
}
