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
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 */
public class LRUCache<K,V> extends SolrCacheBase implements SolrCache<K,V>, Accountable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(LRUCache.class);

  ///  Copied from Lucene's LRUQueryCache

  // memory usage of a simple term query
  public static final long DEFAULT_RAM_BYTES_USED = 192;

  public static final long HASHTABLE_RAM_BYTES_PER_ENTRY =
      2 * RamUsageEstimator.NUM_BYTES_OBJECT_REF // key + value
          * 2; // hash tables need to be oversized to avoid collisions, assume 2x capacity

  static final long LINKED_HASHTABLE_RAM_BYTES_PER_ENTRY =
      HASHTABLE_RAM_BYTES_PER_ENTRY
          + 2 * RamUsageEstimator.NUM_BYTES_OBJECT_REF; // previous & next references
  /// End copied code

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

  private long maxRamBytes = Long.MAX_VALUE;
  // The synchronization used for the map will be used to update this,
  // hence not an AtomicLong
  private long ramBytesUsed = 0;

  @Override
  public Object init(Map args, Object persistence, CacheRegenerator regenerator) {
    super.init(args, regenerator);
    String str = (String)args.get("size");
    final int limit = str==null ? 1024 : Integer.parseInt(str);
    str = (String)args.get("initialSize");
    final int initialSize = Math.min(str==null ? 1024 : Integer.parseInt(str), limit);
    str = (String) args.get("maxRamMB");
    final long maxRamBytes = this.maxRamBytes = str == null ? Long.MAX_VALUE : (long) (Double.parseDouble(str) * 1024L * 1024L);
    description = generateDescription(limit, initialSize);

    map = new LinkedHashMap<K,V>(initialSize, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry eldest) {
          if (size() > limit || ramBytesUsed > maxRamBytes) {
            if (maxRamBytes != Long.MAX_VALUE && ramBytesUsed > maxRamBytes) {
              long bytesToDecrement = 0;

              Iterator<Map.Entry<K, V>> iterator = entrySet().iterator();
              do {
                Map.Entry<K, V> entry = iterator.next();
                if (entry.getKey() != null) {
                  if (entry.getKey() instanceof Accountable) {
                    bytesToDecrement += ((Accountable) entry.getKey()).ramBytesUsed();
                  } else  {
                    bytesToDecrement += DEFAULT_RAM_BYTES_USED;
                  }
                }
                if (entry.getValue() != null) {
                  bytesToDecrement += ((Accountable) entry.getValue()).ramBytesUsed();
                }
                bytesToDecrement += LINKED_HASHTABLE_RAM_BYTES_PER_ENTRY;
                ramBytesUsed -= bytesToDecrement;
                iterator.remove();
                evictions++;
                evictionsRamUsage++;
                stats.evictions.increment();
                stats.evictionsRamUsage.increment();
              } while (iterator.hasNext() && ramBytesUsed > maxRamBytes);
              // must return false according to javadocs of removeEldestEntry if we're modifying
              // the map ourselves
              return false;
            } else  {
              // increment evictions regardless of state.
              // this doesn't need to be synchronized because it will
              // only be called in the context of a higher level synchronized block.
              evictions++;
              stats.evictions.increment();
              return true;
            }
          }
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
   * 
   * @return Returns the description of this cache. 
   */
  private String generateDescription(int limit, int initialSize) {
    String description = "LRU Cache(maxSize=" + limit + ", initialSize=" + initialSize;
    if (isAutowarmingOn()) {
      description += ", " + getAutowarmDescription();
    }
    if (maxRamBytes != Long.MAX_VALUE)  {
      description += ", maxRamMB=" + (maxRamBytes / 1024L / 1024L);
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
    synchronized (map) {
      if (getState() == State.LIVE) {
        stats.inserts.increment();
      }

      // increment local inserts regardless of state???
      // it does make it more consistent with the current size...
      inserts++;

      // important to calc and add new ram bytes first so that removeEldestEntry can compare correctly
      long keySize = DEFAULT_RAM_BYTES_USED;
      if (maxRamBytes != Long.MAX_VALUE) {
        if (key != null && key instanceof Accountable) {
          keySize = ((Accountable) key).ramBytesUsed();
        }
        long valueSize = 0;
        if (value != null) {
          if (value instanceof Accountable) {
            Accountable accountable = (Accountable) value;
            valueSize = accountable.ramBytesUsed();
          } else {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Cache: "
                + getName() + " is configured with maxRamBytes=" + RamUsageEstimator.humanReadableUnits(maxRamBytes)
                + " but its values do not implement org.apache.lucene.util.Accountable");
          }
        }
        ramBytesUsed += keySize + valueSize + LINKED_HASHTABLE_RAM_BYTES_PER_ENTRY;
      }
      V old = map.put(key, value);
      if (maxRamBytes != Long.MAX_VALUE && old != null) {
        long bytesToDecrement = ((Accountable) old).ramBytesUsed();
        // the key existed in the map but we added its size before the put, so let's back out
        bytesToDecrement += LINKED_HASHTABLE_RAM_BYTES_PER_ENTRY;
        if (key != null) {
          if (key instanceof Accountable) {
            Accountable aKey = (Accountable) key;
            bytesToDecrement += aKey.ramBytesUsed();
          } else {
            bytesToDecrement += DEFAULT_RAM_BYTES_USED;
          }
        }
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
  public String getSource() {
    return null;
  }

  @Override
  public NamedList getStatistics() {
    NamedList lst = new SimpleOrderedMap();
    synchronized (map) {
      lst.add("lookups", lookups);
      lst.add("hits", hits);
      lst.add("hitratio", calcHitRatio(lookups,hits));
      lst.add("inserts", inserts);
      lst.add("evictions", evictions);
      lst.add("size", map.size());
      if (maxRamBytes != Long.MAX_VALUE)  {
        lst.add("maxRamMB", maxRamBytes / 1024L / 1024L);
        lst.add("ramBytesUsed", ramBytesUsed());
        lst.add("evictionsRamUsage", evictionsRamUsage);
      }
    }
    lst.add("warmupTime", warmupTime);
    
    long clookups = stats.lookups.longValue();
    long chits = stats.hits.longValue();
    lst.add("cumulative_lookups", clookups);
    lst.add("cumulative_hits", chits);
    lst.add("cumulative_hitratio", calcHitRatio(clookups, chits));
    lst.add("cumulative_inserts", stats.inserts.longValue());
    lst.add("cumulative_evictions", stats.evictions.longValue());
    if (maxRamBytes != Long.MAX_VALUE)  {
      lst.add("cumulative_evictionsRamUsage", stats.evictionsRamUsage.longValue());
    }
    
    return lst;
  }

  @Override
  public String toString() {
    return name() + getStatistics().toString();
  }

  @Override
  public long ramBytesUsed() {
    synchronized (map)  {
      return BASE_RAM_BYTES_USED + ramBytesUsed;
    }
  }

  @Override
  public Collection<Accountable> getChildResources() {
    if (maxRamBytes != Long.MAX_VALUE)  {
      synchronized (map)  {
        return Accountables.namedAccountables(getName(), (Map<?, ? extends Accountable>) map);
      }
    } else  {
      return Collections.emptyList();
    }
  }
}
