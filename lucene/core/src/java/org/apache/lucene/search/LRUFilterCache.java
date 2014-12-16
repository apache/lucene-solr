package org.apache.lucene.search;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReader.CoreClosedListener;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.RoaringDocIdSet;

/**
 * A {@link FilterCache} that evicts filters using a LRU (least-recently-used)
 * eviction policy in order to remain under a given maximum size and number of
 * bytes used.
 *
 * This class is thread-safe.
 *
 * Note that filter eviction runs in linear time with the total number of
 * segments that have cache entries so this cache works best with
 * {@link FilterCachingPolicy caching policies} that only cache on "large"
 * segments, and it is advised to not share this cache across too many indices.
 *
 * @lucene.experimental
 */
public class LRUFilterCache implements FilterCache, Accountable {

  // memory usage of a simple query-wrapper filter around a term query
  static final long FILTER_DEFAULT_RAM_BYTES_USED = 216;

  static final long HASHTABLE_RAM_BYTES_PER_ENTRY =
      2 * RamUsageEstimator.NUM_BYTES_OBJECT_REF // key + value
      * 2; // hash tables need to be oversized to avoid collisions, assume 2x capacity

  static final long LINKED_HASHTABLE_RAM_BYTES_PER_ENTRY =
      HASHTABLE_RAM_BYTES_PER_ENTRY
      + 2 * RamUsageEstimator.NUM_BYTES_OBJECT_REF; // previous & next references

  private final int maxSize;
  private final long maxRamBytesUsed;
  // maps filters that are contained in the cache to a singleton so that this
  // cache does not store several copies of the same filter
  private final Map<Filter, Filter> uniqueFilters;
  // The contract between this set and the per-leaf caches is that per-leaf caches
  // are only allowed to store sub-sets of the filters that are contained in
  // mostRecentlyUsedFilters. This is why write operations are performed under a lock
  private final Set<Filter> mostRecentlyUsedFilters;
  private final Map<Object, LeafCache> cache;

  // these variables are volatile so that we do not need to sync reads
  // but increments need to be performed under the lock
  private volatile long ramBytesUsed;
  private volatile long hitCount;
  private volatile long missCount;
  private volatile long cacheCount;
  private volatile long cacheSize;

  /**
   * Create a new instance that will cache at most <code>maxSize</code> filters
   * with at most <code>maxRamBytesUsed</code> bytes of memory.
   */
  public LRUFilterCache(int maxSize, long maxRamBytesUsed) {
    this.maxSize = maxSize;
    this.maxRamBytesUsed = maxRamBytesUsed;
    uniqueFilters = new LinkedHashMap<Filter, Filter>(16, 0.75f, true);
    mostRecentlyUsedFilters = uniqueFilters.keySet();
    cache = new IdentityHashMap<>();
    ramBytesUsed = 0;
  }

  /** Whether evictions are required. */
  boolean requiresEviction() {
    final int size = mostRecentlyUsedFilters.size();
    if (size == 0) {
      return false;
    } else {
      return size > maxSize || ramBytesUsed() > maxRamBytesUsed;
    }
  }

  synchronized DocIdSet get(Filter filter, LeafReaderContext context) {
    final LeafCache leafCache = cache.get(context.reader().getCoreCacheKey());
    if (leafCache == null) {
      missCount += 1;
      return null;
    }
    // this get call moves the filter to the most-recently-used position
    final Filter singleton = uniqueFilters.get(filter);
    if (singleton == null) {
      missCount += 1;
      return null;
    }
    final DocIdSet cached = leafCache.get(singleton);
    if (cached == null) {
      missCount += 1;
    } else {
      hitCount += 1;
    }
    return cached;
  }

  synchronized void putIfAbsent(Filter filter, LeafReaderContext context, DocIdSet set) {
    // under a lock to make sure that mostRecentlyUsedFilters and cache remain sync'ed
    assert set.isCacheable();
    Filter singleton = uniqueFilters.putIfAbsent(filter, filter);
    if (singleton == null) {
      ramBytesUsed += LINKED_HASHTABLE_RAM_BYTES_PER_ENTRY + ramBytesUsed(filter);
    } else {
      filter = singleton;
    }
    LeafCache leafCache = cache.get(context.reader().getCoreCacheKey());
    if (leafCache == null) {
      leafCache = new LeafCache();
      final LeafCache previous = cache.put(context.reader().getCoreCacheKey(), leafCache);
      ramBytesUsed += HASHTABLE_RAM_BYTES_PER_ENTRY;
      assert previous == null;
      // we just created a new leaf cache, need to register a close listener
      context.reader().addCoreClosedListener(new CoreClosedListener() {
        @Override
        public void onClose(Object ownerCoreCacheKey) {
          clearCoreCacheKey(ownerCoreCacheKey);
        }
      });
    }
    leafCache.putIfAbsent(filter, set);
    evictIfNecessary();
  }

  synchronized void evictIfNecessary() {
    // under a lock to make sure that mostRecentlyUsedFilters and cache keep sync'ed
    if (requiresEviction()) {
      Iterator<Filter> iterator = mostRecentlyUsedFilters.iterator();
      do {
        final Filter filter = iterator.next();
        iterator.remove();
        onEviction(filter);
      } while (iterator.hasNext() && requiresEviction());
    }
  }

  /**
   * Remove all cache entries for the given core cache key.
   */
  public synchronized void clearCoreCacheKey(Object coreKey) {
    final LeafCache leafCache = cache.remove(coreKey);
    if (leafCache != null) {
      ramBytesUsed -= leafCache.ramBytesUsed + HASHTABLE_RAM_BYTES_PER_ENTRY;
      cacheSize -= leafCache.cache.size();
    }
  }

  /**
   * Remove all cache entries for the given filter.
   */
  public synchronized void clearFilter(Filter filter) {
    final Filter singleton = uniqueFilters.remove(filter);
    if (singleton != null) {
      onEviction(singleton);
    }
  }

  private void onEviction(Filter singleton) {
    ramBytesUsed -= LINKED_HASHTABLE_RAM_BYTES_PER_ENTRY + ramBytesUsed(singleton);
    for (LeafCache leafCache : cache.values()) {
      leafCache.remove(singleton);
    }
  }

  /**
   * Clear the content of this cache.
   */
  public synchronized void clear() {
    cache.clear();
    mostRecentlyUsedFilters.clear();
    ramBytesUsed = 0;
    cacheSize = 0;
  }

  // pkg-private for testing
  synchronized void assertConsistent() {
    if (requiresEviction()) {
      throw new AssertionError("requires evictions: size=" + mostRecentlyUsedFilters.size()
          + ", maxSize=" + maxSize + ", ramBytesUsed=" + ramBytesUsed() + ", maxRamBytesUsed=" + maxRamBytesUsed);
    }
    for (LeafCache leafCache : cache.values()) {
      Set<Filter> keys = Collections.newSetFromMap(new IdentityHashMap<>());
      keys.addAll(leafCache.cache.keySet());
      keys.removeAll(mostRecentlyUsedFilters);
      if (!keys.isEmpty()) {
        throw new AssertionError("One leaf cache contains more keys than the top-level cache: " + keys);
      }
    }
    long recomputedRamBytesUsed =
          HASHTABLE_RAM_BYTES_PER_ENTRY * cache.size()
        + LINKED_HASHTABLE_RAM_BYTES_PER_ENTRY * uniqueFilters.size();
    for (Filter filter : mostRecentlyUsedFilters) {
      recomputedRamBytesUsed += ramBytesUsed(filter);
    }
    for (LeafCache leafCache : cache.values()) {
      recomputedRamBytesUsed += HASHTABLE_RAM_BYTES_PER_ENTRY * leafCache.cache.size();
      for (DocIdSet set : leafCache.cache.values()) {
        recomputedRamBytesUsed += set.ramBytesUsed();
      }
    }
    if (recomputedRamBytesUsed != ramBytesUsed) {
      throw new AssertionError("ramBytesUsed mismatch : " + ramBytesUsed + " != " + recomputedRamBytesUsed);
    }

    long recomputedCacheSize = 0;
    for (LeafCache leafCache : cache.values()) {
      recomputedCacheSize += leafCache.cache.size();
    }
    if (recomputedCacheSize != getCacheSize()) {
      throw new AssertionError("cacheSize mismatch : " + getCacheSize() + " != " + recomputedCacheSize);
    }
  }

  // pkg-private for testing
  // return the list of cached filters in LRU order
  synchronized List<Filter> cachedFilters() {
    return new ArrayList<>(mostRecentlyUsedFilters);
  }

  @Override
  public Filter doCache(Filter filter, FilterCachingPolicy policy) {
    while (filter instanceof CachingWrapperFilter) {
      // should we throw an exception instead?
      filter = ((CachingWrapperFilter) filter).in;
    }

    return new CachingWrapperFilter(filter, policy);
  }

  /**
   *  Provide the DocIdSet to be cached, using the DocIdSet provided
   *  by the wrapped Filter. <p>This implementation returns the given {@link DocIdSet},
   *  if {@link DocIdSet#isCacheable} returns <code>true</code>, else it calls
   *  {@link #cacheImpl(DocIdSetIterator, org.apache.lucene.index.LeafReader)}
   *  <p>Note: This method returns {@linkplain DocIdSet#EMPTY} if the given docIdSet
   *  is <code>null</code> or if {@link DocIdSet#iterator()} return <code>null</code>. The empty
   *  instance is use as a placeholder in the cache instead of the <code>null</code> value.
   */
  protected DocIdSet docIdSetToCache(DocIdSet docIdSet, LeafReader reader) throws IOException {
    if (docIdSet == null || docIdSet.isCacheable()) {
      return docIdSet;
    } else {
      final DocIdSetIterator it = docIdSet.iterator();
      if (it == null) {
        return null;
      } else {
        return cacheImpl(it, reader);
      }
    }
  }

  @Override
  public long ramBytesUsed() {
    return ramBytesUsed;
  }

  @Override
  public Iterable<Accountable> getChildResources() {
    synchronized (this) {
      return Accountables.namedAccountables("segment", cache);
    }
  }

  /**
   * Return the number of bytes used by the given filter. The default
   * implementation returns {@link Accountable#ramBytesUsed()} if the filter
   * implements {@link Accountable} and <code>1024</code> otherwise.
   */
  protected long ramBytesUsed(Filter filter) {
    if (filter instanceof Accountable) {
      return ((Accountable) filter).ramBytesUsed();
    }
    return FILTER_DEFAULT_RAM_BYTES_USED;
  }

  /**
   * Default cache implementation: uses {@link RoaringDocIdSet}.
   */
  protected DocIdSet cacheImpl(DocIdSetIterator iterator, LeafReader reader) throws IOException {
    return new RoaringDocIdSet.Builder(reader.maxDoc()).add(iterator).build();
  }

  /**
   * Return the total number of times that a {@link Filter} has been looked up
   * in this {@link FilterCache}. Note that this number is incremented once per
   * segment so running a cached filter only once will increment this counter
   * by the number of segments that are wrapped by the searcher.
   * Note that by definition, {@link #getTotalCount()} is the sum of
   * {@link #getHitCount()} and {@link #getMissCount()}.
   * @see #getHitCount()
   * @see #getMissCount()
   */
  public final long getTotalCount() {
    return getHitCount() + getMissCount();
  }

  /**
   * Over the {@link #getTotalCount() total} number of times that a filter has
   * been looked up, return how many times a cached {@link DocIdSet} has been
   * found and returned.
   * @see #getTotalCount()
   * @see #getMissCount()
   */
  public final long getHitCount() {
    return hitCount;
  }

  /**
   * Over the {@link #getTotalCount() total} number of times that a filter has
   * been looked up, return how many times this filter was not contained in the
   * cache.
   * @see #getTotalCount()
   * @see #getHitCount()
   */
  public final long getMissCount() {
    return missCount;
  }

  /**
   * Return the total number of {@link DocIdSet}s which are currently stored
   * in the cache.
   * @see #getCacheCount()
   * @see #getEvictionCount()
   */
  public final long getCacheSize() {
    return cacheSize;
  }

  /**
   * Return the total number of cache entries that have been generated and put
   * in the cache. It is highly desirable to have a {@link #getHitCount() hit
   * count} that is much higher than the {@link #getCacheCount() cache count}
   * as the opposite would indicate that the filter cache makes efforts in order
   * to cache filters but then they do not get reused.
   * @see #getCacheSize()
   * @see #getEvictionCount()
   */
  public final long getCacheCount() {
    return cacheCount;
  }

  /**
   * Return the number of cache entries that have been removed from the cache
   * either in order to stay under the maximum configured size/ram usage, or
   * because a segment has been closed. High numbers of evictions might mean
   * that filters are not reused or that the {@link FilterCachingPolicy
   * caching policy} caches too aggressively on NRT segments which get merged
   * early.
   * @see #getCacheCount()
   * @see #getCacheSize()
   */
  public final long getEvictionCount() {
    return getCacheCount() - getCacheSize();
  }

  // this class is not thread-safe, everything but ramBytesUsed needs to be called under a lock
  private class LeafCache implements Accountable {

    private final Map<Filter, DocIdSet> cache;
    private volatile long ramBytesUsed;

    LeafCache() {
      cache = new IdentityHashMap<>();
      ramBytesUsed = 0;
    }

    private void incrementRamBytesUsed(long inc) {
      ramBytesUsed += inc;
      LRUFilterCache.this.ramBytesUsed += inc;
    }

    DocIdSet get(Filter filter) {
      return cache.get(filter);
    }

    void putIfAbsent(Filter filter, DocIdSet set) {
      if (cache.putIfAbsent(filter, set) == null) {
        // the set was actually put
        cacheCount += 1;
        cacheSize += 1;
        incrementRamBytesUsed(HASHTABLE_RAM_BYTES_PER_ENTRY + set.ramBytesUsed());
      }
    }

    void remove(Filter filter) {
      DocIdSet removed = cache.remove(filter);
      if (removed != null) {
        cacheSize -= 1;
        incrementRamBytesUsed(-(HASHTABLE_RAM_BYTES_PER_ENTRY + removed.ramBytesUsed()));
      }
    }

    @Override
    public long ramBytesUsed() {
      return ramBytesUsed;
    }

  }

  private class CachingWrapperFilter extends Filter {

    private final Filter in;
    private final FilterCachingPolicy policy;

    CachingWrapperFilter(Filter in, FilterCachingPolicy policy) {
      this.in = in;
      this.policy = policy;
    }

    @Override
    public DocIdSet getDocIdSet(LeafReaderContext context, Bits acceptDocs) throws IOException {
      if (context.ord == 0) {
        policy.onUse(in);
      }

      DocIdSet set = get(in, context);
      if (set == null) {
        // do not apply acceptDocs yet, we want the cached filter to not take them into account
        set = in.getDocIdSet(context, null);
        if (policy.shouldCache(in, context, set)) {
          set = docIdSetToCache(set, context.reader());
          if (set == null) {
            // null values are not supported
            set = DocIdSet.EMPTY;
          }
          // it might happen that another thread computed the same set in parallel
          // although this might incur some CPU overhead, it is probably better
          // this way than trying to lock and preventing other filters to be
          // computed at the same time?
          putIfAbsent(in, context, set);
        }
      }
      return set == DocIdSet.EMPTY ? null : BitsFilteredDocIdSet.wrap(set, acceptDocs);
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof CachingWrapperFilter
          && in.equals(((CachingWrapperFilter) obj).in);
    }

    @Override
    public int hashCode() {
      return in.hashCode() ^ getClass().hashCode();
    }
  }

}
