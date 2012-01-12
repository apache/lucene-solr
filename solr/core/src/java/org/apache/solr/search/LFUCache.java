package org.apache.solr.search;
/**
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

import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrCore;
import org.apache.solr.util.ConcurrentLFUCache;

import java.io.IOException;
import java.io.Serializable;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * SolrCache based on ConcurrentLFUCache implementation.
 * <p/>
 * This implementation does not use a separate cleanup thread. Instead it uses the calling thread
 * itself to do the cleanup when the size of the cache exceeds certain limits.
 * <p/>
 * Also see <a href="http://wiki.apache.org/solr/SolrCaching">SolrCaching</a>
 * <p/>
 * <b>This API is experimental and subject to change</b>
 *
 * @version $Id: LFUCache.java 1170772 2011-09-14 19:09:56Z sarowe $
 * @see org.apache.solr.util.ConcurrentLFUCache
 * @see org.apache.solr.search.SolrCache
 * @since solr 3.6
 */
public class LFUCache<K, V> implements SolrCache<K, V> {

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

  public Object init(Map args, Object persistence, CacheRegenerator regenerator) {
    state = State.CREATED;
    this.regenerator = regenerator;
    name = (String) args.get("name");
    String str = (String) args.get("size");
    int limit = str == null ? 1024 : Integer.parseInt(str);
    int minLimit;
    str = (String) args.get("minSize");
    if (str == null) {
      minLimit = (int) (limit * 0.9);
    } else {
      minLimit = Integer.parseInt(str);
    }
    if (minLimit == 0) minLimit = 1;
    if (limit <= minLimit) limit = minLimit + 1;

    int acceptableSize;
    str = (String) args.get("acceptableSize");
    if (str == null) {
      acceptableSize = (int) (limit * 0.95);
    } else {
      acceptableSize = Integer.parseInt(str);
    }
    // acceptable limit should be somewhere between minLimit and limit
    acceptableSize = Math.max(minLimit, acceptableSize);

    str = (String) args.get("initialSize");
    final int initialSize = str == null ? limit : Integer.parseInt(str);
    str = (String) args.get("autowarmCount");
    autowarmCount = str == null ? 0 : Integer.parseInt(str);
    str = (String) args.get("cleanupThread");
    boolean newThread = str == null ? false : Boolean.parseBoolean(str);

    str = (String) args.get("showItems");
    showItems = str == null ? 0 : Integer.parseInt(str);

    // Don't make this "efficient" by removing the test, default is true and omitting the param will make it false.
    str = (String) args.get("timeDecay");
    timeDecay = (str == null) ? true : Boolean.parseBoolean(str);

    description = "Concurrent LFU Cache(maxSize=" + limit + ", initialSize=" + initialSize +
        ", minSize=" + minLimit + ", acceptableSize=" + acceptableSize + ", cleanupThread=" + newThread +
        ", timeDecay=" + Boolean.toString(timeDecay);
    if (autowarmCount > 0) {
      description += ", autowarmCount=" + autowarmCount + ", regenerator=" + regenerator;
    }
    description += ')';

    cache = new ConcurrentLFUCache<K, V>(limit, minLimit, acceptableSize, initialSize, newThread, false, null, timeDecay);
    cache.setAlive(false);

    statsList = (List<ConcurrentLFUCache.Stats>) persistence;
    if (statsList == null) {
      // must be the first time a cache of this type is being created
      // Use a CopyOnWriteArrayList since puts are very rare and iteration may be a frequent operation
      // because it is used in getStatistics()
      statsList = new CopyOnWriteArrayList<ConcurrentLFUCache.Stats>();

      // the first entry will be for cumulative stats of caches that have been closed.
      statsList.add(new ConcurrentLFUCache.Stats());
    }
    statsList.add(cache.getStats());
    return statsList;
  }

  public String name() {
    return name;
  }

  public int size() {
    return cache.size();

  }

  public V put(K key, V value) {
    return cache.put(key, value);
  }

  public V get(K key) {
    return cache.get(key);
  }

  public void clear() {
    cache.clear();
  }

  public void setState(State state) {
    this.state = state;
    cache.setAlive(state == State.LIVE);
  }

  public State getState() {
    return state;
  }

  public void warm(SolrIndexSearcher searcher, SolrCache old) throws IOException {
    if (regenerator == null) return;
    long warmingStartTime = System.currentTimeMillis();
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
        } catch (Throwable e) {
          SolrException.log(log, "Error during auto-warming of key:" + itemsArr[i].getKey(), e);
        }
      }
    }
    warmupTime = System.currentTimeMillis() - warmingStartTime;
  }


  public void close() {
    // add the stats to the cumulative stats object (the first in the statsList)
    statsList.get(0).add(cache.getStats());
    statsList.remove(cache.getStats());
    cache.destroy();
  }

  //////////////////////// SolrInfoMBeans methods //////////////////////
  public String getName() {
    return LFUCache.class.getName();
  }

  public String getVersion() {
    return SolrCore.version;
  }

  public String getDescription() {
    return description;
  }

  public Category getCategory() {
    return Category.CACHE;
  }

  public String getSourceId() {
    return "$Id: LFUCache.java 1170772 2011-09-14 19:09:56Z sarowe $";
  }

  public String getSource() {
    return "$URL: http://svn.apache.org/repos/asf/lucene/dev/branches/branch_3x/solr/core/src/java/org/apache/solr/search/LFUCache.java $";
  }

  public URL[] getDocs() {
    return null;
  }

  // returns a ratio, not a percent.
  private static String calcHitRatio(long lookups, long hits) {
    if (lookups == 0) return "0.00";
    if (lookups == hits) return "1.00";
    int hundredths = (int) (hits * 100 / lookups);   // rounded down
    if (hundredths < 10) return "0.0" + hundredths;
    return "0." + hundredths;
  }

  public NamedList getStatistics() {
    NamedList<Serializable> lst = new SimpleOrderedMap<Serializable>();
    if (cache == null) return lst;
    ConcurrentLFUCache.Stats stats = cache.getStats();
    long lookups = stats.getCumulativeLookups();
    long hits = stats.getCumulativeHits();
    long inserts = stats.getCumulativePuts();
    long evictions = stats.getCumulativeEvictions();
    long size = stats.getCurrentSize();

    lst.add("lookups", lookups);
    lst.add("hits", hits);
    lst.add("hitratio", calcHitRatio(lookups, hits));
    lst.add("inserts", inserts);
    lst.add("evictions", evictions);
    lst.add("size", size);

    lst.add("warmupTime", warmupTime);
    lst.add("timeDecay", timeDecay);

    long clookups = 0;
    long chits = 0;
    long cinserts = 0;
    long cevictions = 0;

    // NOTE: It is safe to iterate on a CopyOnWriteArrayList
    for (ConcurrentLFUCache.Stats statistiscs : statsList) {
      clookups += statistiscs.getCumulativeLookups();
      chits += statistiscs.getCumulativeHits();
      cinserts += statistiscs.getCumulativePuts();
      cevictions += statistiscs.getCumulativeEvictions();
    }
    lst.add("cumulative_lookups", clookups);
    lst.add("cumulative_hits", chits);
    lst.add("cumulative_hitratio", calcHitRatio(clookups, chits));
    lst.add("cumulative_inserts", cinserts);
    lst.add("cumulative_evictions", cevictions);

    if (showItems != 0) {
      Map items = cache.getMostUsedItems(showItems == -1 ? Integer.MAX_VALUE : showItems);
      for (Map.Entry e : (Set<Map.Entry>) items.entrySet()) {
        Object k = e.getKey();
        Object v = e.getValue();

        String ks = "item_" + k;
        String vs = v.toString();
        lst.add(ks, vs);
      }

    }

    return lst;
  }

  @Override
  public String toString() {
    return name + getStatistics().toString();
  }
}
