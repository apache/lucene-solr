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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReader.CoreClosedListener;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.Bits;
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

  private final int maxSize;
  private final long maxRamBytesUsed;
  // The contract between this set and the per-leaf caches is that per-leaf caches
  // are only allowed to store sub-sets of the filters that are contained in
  // mostRecentlyUsedFilters. This is why write operations are performed under a lock
  private final Set<Filter> mostRecentlyUsedFilters;
  private final Map<Object, LeafCache> cache;
  private volatile long ramBytesUsed; // all updates of this number must be performed under a lock

  /**
   * Create a new instance that will cache at most <code>maxSize</code> filters
   * with at most <code>maxRamBytesUsed</code> bytes of memory.
   */
  public LRUFilterCache(int maxSize, long maxRamBytesUsed) {
    this.maxSize = maxSize;
    this.maxRamBytesUsed = maxRamBytesUsed;
    mostRecentlyUsedFilters = Collections.newSetFromMap(new LinkedHashMap<Filter, Boolean>(16, 0.75f, true));
    cache = new IdentityHashMap<>();
    ramBytesUsed = 0;
  }

  /** Whether evictions are required. */
  boolean requiresEviction() {
    return mostRecentlyUsedFilters.size() > maxSize || ramBytesUsed() > maxRamBytesUsed;
  }

  synchronized DocIdSet get(Filter filter, LeafReaderContext context) {
    final LeafCache leafCache = cache.get(context.reader().getCoreCacheKey());
    if (leafCache == null) {
      return null;
    }
    final DocIdSet set = leafCache.get(filter);
    if (set != null) {
      // this filter becomes the most-recently used filter
      final boolean added = mostRecentlyUsedFilters.add(filter);
      // added is necessarily false since the leaf caches contain a subset of mostRecentlyUsedFilters
      assert added == false;
    }
    return set;
  }

  synchronized void putIfAbsent(Filter filter, LeafReaderContext context, DocIdSet set) {
    // under a lock to make sure that mostRecentlyUsedFilters and cache remain sync'ed
    assert set.isCacheable();
    final boolean added = mostRecentlyUsedFilters.add(filter);
    if (added) {
      ramBytesUsed += ramBytesUsed(filter);
    }
    LeafCache leafCache = cache.get(context.reader().getCoreCacheKey());
    if (leafCache == null) {
      leafCache = new LeafCache();
      final LeafCache previous = cache.put(context.reader().getCoreCacheKey(), leafCache);
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
    if (requiresEviction() && mostRecentlyUsedFilters.isEmpty() == false) {
      Iterator<Filter> iterator = mostRecentlyUsedFilters.iterator();
      do {
        final Filter filter = iterator.next();
        iterator.remove();
        ramBytesUsed -= ramBytesUsed(filter);
        clearFilter(filter);
      } while (iterator.hasNext() && requiresEviction());
    }
  }

  /**
   * Remove all cache entries for the given core cache key.
   */
  public synchronized void clearCoreCacheKey(Object coreKey) {
    final LeafCache leafCache = cache.remove(coreKey);
    if (leafCache != null) {
      ramBytesUsed -= leafCache.ramBytesUsed;
    }
  }

  /**
   * Remove all cache entries for the given filter.
   */
  public synchronized void clearFilter(Filter filter) {
    for (LeafCache leafCache : cache.values()) {
      leafCache.remove(filter);
    }
  }

  /**
   * Clear the content of this cache.
   */
  public synchronized void clear() {
    cache.clear();
    mostRecentlyUsedFilters.clear();
    ramBytesUsed = 0;
  }

  // pkg-private for testing
  synchronized void assertConsistent() {
    if (requiresEviction()) {
      throw new AssertionError("requires evictions: size=" + mostRecentlyUsedFilters.size()
          + ", maxSize=" + maxSize + ", ramBytesUsed=" + ramBytesUsed() + ", maxRamBytesUsed=" + maxRamBytesUsed);
    }
    for (LeafCache leafCache : cache.values()) {
      Set<Filter> keys = new HashSet<Filter>(leafCache.cache.keySet());
      keys.removeAll(mostRecentlyUsedFilters);
      if (!keys.isEmpty()) {
        throw new AssertionError("One leaf cache contains more keys than the top-level cache: " + keys);
      }
    }
    long recomputedRamBytesUsed = 0;
    for (Filter filter : mostRecentlyUsedFilters) {
      recomputedRamBytesUsed += ramBytesUsed(filter);
    }
    for (LeafCache leafCache : cache.values()) {
      recomputedRamBytesUsed += leafCache.ramBytesUsed();
    }
    if (recomputedRamBytesUsed != ramBytesUsed) {
      throw new AssertionError("ramBytesUsed mismatch : " + ramBytesUsed + " != " + recomputedRamBytesUsed);
    }
  }

  // pkg-private for testing
  synchronized Set<Filter> cachedFilters() {
    return new HashSet<>(mostRecentlyUsedFilters);
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
  public Iterable<? extends Accountable> getChildResources() {
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
    return 1024;
  }

  /**
   * Default cache implementation: uses {@link RoaringDocIdSet}.
   */
  protected DocIdSet cacheImpl(DocIdSetIterator iterator, LeafReader reader) throws IOException {
    return new RoaringDocIdSet.Builder(reader.maxDoc()).add(iterator).build();
  }

  // this class is not thread-safe, everything but ramBytesUsed needs to be called under a lock
  private class LeafCache implements Accountable {

    private final Map<Filter, DocIdSet> cache;
    private volatile long ramBytesUsed;

    LeafCache() {
      cache = new HashMap<>();
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
        incrementRamBytesUsed(set.ramBytesUsed());
      }
    }

    void remove(Filter filter) {
      DocIdSet removed = cache.remove(filter);
      if (removed != null) {
        incrementRamBytesUsed(-removed.ramBytesUsed());
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
