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

import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.facet.taxonomy.writercache.TaxonomyWriterCache;

/**
 * LRU {@link TaxonomyWriterCache} - good choice for huge taxonomies.
 * 
 * @lucene.experimental
 */
public class LruTaxonomyWriterCache implements TaxonomyWriterCache {

  /**
   * Determines cache type.
   * For guaranteed correctness - not relying on no-collisions in the hash
   * function, LRU_STRING should be used.
   */
  public enum LRUType {
    /** Use only the label's 64 bit longHashCode as the hash key. Do not
     *  check equals, unlike most hash maps.
     *  Note that while these hashes are very likely to be unique, the chance
     *  of a collision is still greater than zero. If such an unlikely event
     *  occurs, your document will get an incorrect facet.
     */
    LRU_HASHED,

    /** Use the label as the hash key; this is always
     *  correct but will usually use more RAM. */
    LRU_STRING
  }

  private NameIntCacheLRU cache;

  /** Creates this with {@link LRUType#LRU_STRING} method. */
  public LruTaxonomyWriterCache(int cacheSize) {
    // TODO (Facet): choose between NameHashIntCacheLRU and NameIntCacheLRU.
    // For guaranteed correctness - not relying on no-collisions in the hash
    // function, NameIntCacheLRU should be used:
    // On the other hand, NameHashIntCacheLRU takes less RAM but if there
    // are collisions two different paths would be mapped to the same
    // ordinal...
    this(cacheSize, LRUType.LRU_STRING);
  }

  /** Creates this with the specified method. */
  public LruTaxonomyWriterCache(int cacheSize, LRUType lruType) {
    // TODO (Facet): choose between NameHashIntCacheLRU and NameIntCacheLRU.
    // For guaranteed correctness - not relying on no-collisions in the hash
    // function, NameIntCacheLRU should be used:
    // On the other hand, NameHashIntCacheLRU takes less RAM but if there
    // are collisions two different paths would be mapped to the same
    // ordinal...
    if (lruType == LRUType.LRU_HASHED) {
      this.cache = new NameHashIntCacheLRU(cacheSize);
    } else {
      this.cache = new NameIntCacheLRU(cacheSize);
    }
  }

  @Override
  public synchronized boolean isFull() {
    return cache.getSize() == cache.getMaxSize();
  }

  @Override
  public synchronized void clear() {
    cache.clear();
  }
  
  @Override
  public synchronized void close() {
    cache.clear();
    cache = null;
  }

  public int size() {
    return cache.getSize();
  }
  
  @Override
  public synchronized int get(FacetLabel categoryPath) {
    Integer res = cache.get(categoryPath);
    if (res == null) {
      return -1;
    }

    return res.intValue();
  }

  @Override
  public synchronized boolean put(FacetLabel categoryPath, int ordinal) {
    boolean ret = cache.put(categoryPath, new Integer(ordinal));
    // If the cache is full, we need to clear one or more old entries
    // from the cache. However, if we delete from the cache a recent
    // addition that isn't yet in our reader, for this entry to be
    // visible to us we need to make sure that the changes have been
    // committed and we reopen the reader. Because this is a slow
    // operation, we don't delete entries one-by-one but rather in bulk
    // (put() removes the 2/3rd oldest entries).
    if (ret) {
      cache.makeRoomLRU();
    }
    return ret;
  }

}
