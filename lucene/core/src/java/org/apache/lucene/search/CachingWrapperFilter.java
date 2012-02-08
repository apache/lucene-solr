package org.apache.lucene.search;

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

import java.io.Serializable;
import java.io.IOException;
import java.util.Map;
import java.util.WeakHashMap;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.FixedBitSet;

/**
 * Wraps another filter's result and caches it.  The purpose is to allow
 * filters to simply filter, and then wrap with this class
 * to add caching.
 *
 * <p><b>NOTE</b>: if you wrap this filter as a query (eg,
 * using ConstantScoreQuery), you'll likely want to enforce
 * deletions (using either {@link DeletesMode#RECACHE} or
 * {@link DeletesMode#DYNAMIC}).
 */
public class CachingWrapperFilter extends Filter {
  Filter filter;

  /**
   * Expert: Specifies how new deletions against a reopened
   * reader should be handled.
   *
   * <p>The default is IGNORE, which means the cache entry
   * will be re-used for a given segment, even when that
   * segment has been reopened due to changes in deletions.
   * This is a big performance gain, especially with
   * near-real-timer readers, since you don't hit a cache
   * miss on every reopened reader for prior segments.</p>
   *
   * <p>However, in some cases this can cause invalid query
   * results, allowing deleted documents to be returned.
   * This only happens if the main query does not rule out
   * deleted documents on its own, such as a toplevel
   * ConstantScoreQuery.  To fix this, use RECACHE to
   * re-create the cached filter (at a higher per-reopen
   * cost, but at faster subsequent search performance), or
   * use DYNAMIC to dynamically intersect deleted docs (fast
   * reopen time but some hit to search performance).</p>
   */
  public static enum DeletesMode {IGNORE, RECACHE, DYNAMIC};

  protected final FilterCache<DocIdSet> cache;

  static abstract class FilterCache<T> implements Serializable {

    /**
     * A transient Filter cache (package private because of test)
     */
    // NOTE: not final so that we can dynamically re-init
    // after de-serialize
    transient Map<Object,T> cache;

    private final DeletesMode deletesMode;

    public FilterCache(DeletesMode deletesMode) {
      this.deletesMode = deletesMode;
    }

    public synchronized T get(IndexReader reader, Object coreKey, Object delCoreKey) throws IOException {
      T value;

      if (cache == null) {
        cache = new WeakHashMap<Object,T>();
      }

      if (deletesMode == DeletesMode.IGNORE) {
        // key on core
        value = cache.get(coreKey);
      } else if (deletesMode == DeletesMode.RECACHE) {
        // key on deletes, if any, else core
        value = cache.get(delCoreKey);
      } else {
        assert deletesMode == DeletesMode.DYNAMIC;

        // first try for exact match
        value = cache.get(delCoreKey);

        if (value == null) {
          // now for core match, but dynamically AND NOT
          // deletions
          value = cache.get(coreKey);
          if (value != null && reader.hasDeletions()) {
            value = mergeDeletes(reader, value);
          }
        }
      }

      return value;
    }

    protected abstract T mergeDeletes(IndexReader reader, T value);

    public synchronized void put(Object coreKey, Object delCoreKey, T value) {
      if (deletesMode == DeletesMode.IGNORE) {
        cache.put(coreKey, value);
      } else if (deletesMode == DeletesMode.RECACHE) {
        cache.put(delCoreKey, value);
      } else {
        cache.put(coreKey, value);
        cache.put(delCoreKey, value);
      }
    }
  }

  /**
   * New deletes are ignored by default, which gives higher
   * cache hit rate on reopened readers.  Most of the time
   * this is safe, because the filter will be AND'd with a
   * Query that fully enforces deletions.  If instead you
   * need this filter to always enforce deletions, pass
   * either {@link DeletesMode#RECACHE} or {@link
   * DeletesMode#DYNAMIC}.
   * @param filter Filter to cache results of
   */
  public CachingWrapperFilter(Filter filter) {
    this(filter, DeletesMode.IGNORE);
  }

  /**
   * Expert: by default, the cached filter will be shared
   * across reopened segments that only had changes to their
   * deletions.  
   *
   * @param filter Filter to cache results of
   * @param deletesMode See {@link DeletesMode}
   */
  public CachingWrapperFilter(Filter filter, DeletesMode deletesMode) {
    this.filter = filter;
    cache = new FilterCache<DocIdSet>(deletesMode) {
      @Override
      public DocIdSet mergeDeletes(final IndexReader r, final DocIdSet docIdSet) {
        return new FilteredDocIdSet(docIdSet) {
          @Override
          protected boolean match(int docID) {
            return !r.isDeleted(docID);
          }
        };
      }
    };
  }

  /** Provide the DocIdSet to be cached, using the DocIdSet provided
   *  by the wrapped Filter.
   *  <p>This implementation returns the given {@link DocIdSet}, if {@link DocIdSet#isCacheable}
   *  returns <code>true</code>, else it copies the {@link DocIdSetIterator} into
   *  an {@link FixedBitSet}.
   */
  protected DocIdSet docIdSetToCache(DocIdSet docIdSet, IndexReader reader) throws IOException {
    if (docIdSet == null) {
      // this is better than returning null, as the nonnull result can be cached
      return DocIdSet.EMPTY_DOCIDSET;
    } else if (docIdSet.isCacheable()) {
      return docIdSet;
    } else {
      final DocIdSetIterator it = docIdSet.iterator();
      // null is allowed to be returned by iterator(),
      // in this case we wrap with the empty set,
      // which is cacheable.
      if (it == null) {
        return DocIdSet.EMPTY_DOCIDSET;
      } else {
        final FixedBitSet bits = new FixedBitSet(reader.maxDoc());
        bits.or(it);
        return bits;
      }
    }
  }

  // for testing
  int hitCount, missCount;

  @Override
  public DocIdSet getDocIdSet(IndexReader reader) throws IOException {

    final Object coreKey = reader.getCoreCacheKey();
    final Object delCoreKey = reader.hasDeletions() ? reader.getDeletesCacheKey() : coreKey;

    DocIdSet docIdSet = cache.get(reader, coreKey, delCoreKey);
    if (docIdSet != null) {
      hitCount++;
      return docIdSet;
    }

    missCount++;

    // cache miss
    docIdSet = docIdSetToCache(filter.getDocIdSet(reader), reader);

    if (docIdSet != null) {
      cache.put(coreKey, delCoreKey, docIdSet);
    }
    
    return docIdSet;
  }

  @Override
  public String toString() {
    return "CachingWrapperFilter("+filter+")";
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof CachingWrapperFilter)) return false;
    return this.filter.equals(((CachingWrapperFilter)o).filter);
  }

  @Override
  public int hashCode() {
    return filter.hashCode() ^ 0x1117BF25;  
  }
}
