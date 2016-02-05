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
package org.apache.lucene.facet.taxonomy.writercache;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import org.apache.lucene.facet.taxonomy.FacetLabel;

/**
 * An an LRU cache of mapping from name to int.
 * Used to cache Ordinals of category paths.
 * 
 * @lucene.experimental
 */
// Note: Nothing in this class is synchronized. The caller is assumed to be
// synchronized so that no two methods of this class are called concurrently.
class NameIntCacheLRU {

  private HashMap<Object, Integer> cache;
  long nMisses = 0; // for debug
  long nHits = 0;  // for debug
  private int maxCacheSize;

  NameIntCacheLRU(int maxCacheSize) {
    this.maxCacheSize = maxCacheSize;
    createCache(maxCacheSize);
  }

  /** Maximum number of cache entries before eviction. */
  public int getMaxSize() {
    return maxCacheSize;
  }
  
  /** Number of entries currently in the cache. */
  public int getSize() {
    return cache.size();
  }

  private void createCache (int maxSize) {
    if (maxSize<Integer.MAX_VALUE) {
      cache = new LinkedHashMap<>(1000,(float)0.7,true); //for LRU
    } else {
      cache = new HashMap<>(1000,(float)0.7); //no need for LRU
    }
  }

  Integer get (FacetLabel name) {
    Integer res = cache.get(key(name));
    if (res==null) {
      nMisses ++;
    } else {
      nHits ++;
    }
    return res;
  }

  /** Subclasses can override this to provide caching by e.g. hash of the string. */
  Object key(FacetLabel name) {
    return name;
  }

  Object key(FacetLabel name, int prefixLen) {
    return name.subpath(prefixLen);
  }

  /**
   * Add a new value to cache.
   * Return true if cache became full and some room need to be made. 
   */
  boolean put (FacetLabel name, Integer val) {
    cache.put(key(name), val);
    return isCacheFull();
  }

  boolean put (FacetLabel name, int prefixLen, Integer val) {
    cache.put(key(name, prefixLen), val);
    return isCacheFull();
  }

  private boolean isCacheFull() {
    return cache.size() > maxCacheSize;
  }

  void clear() {
    cache.clear();
  }

  String stats() {
    return "#miss="+nMisses+" #hit="+nHits;
  }
  
  /**
   * If cache is full remove least recently used entries from cache. Return true
   * if anything was removed, false otherwise.
   * 
   * See comment in DirectoryTaxonomyWriter.addToCache(CategoryPath, int) for an
   * explanation why we clean 2/3rds of the cache, and not just one entry.
   */ 
  boolean makeRoomLRU() {
    if (!isCacheFull()) {
      return false;
    }
    int n = cache.size() - (2*maxCacheSize)/3;
    if (n<=0) {
      return false;
    }
    Iterator<Object> it = cache.keySet().iterator();
    int i = 0;
    while (i<n && it.hasNext()) {
      it.next();
      it.remove();
      i++;
    }
    return true;
  }

}
