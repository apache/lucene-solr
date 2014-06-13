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

import static org.apache.lucene.search.DocIdSet.EMPTY;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.WAH8DocIdSet;

/**
 * Wraps another {@link Filter}'s result and caches it.  The purpose is to allow
 * filters to simply filter, and then wrap with this class
 * to add caching.
 */
public class CachingWrapperFilter extends Filter implements Accountable {
  private final Filter filter;
  private final Map<Object,DocIdSet> cache = Collections.synchronizedMap(new WeakHashMap<Object,DocIdSet>());

  /** Wraps another filter's result and caches it.
   * @param filter Filter to cache results of
   */
  public CachingWrapperFilter(Filter filter) {
    this.filter = filter;
  }

  /**
   * Gets the contained filter.
   * @return the contained filter.
   */
  public Filter getFilter() {
    return filter;
  }

  /** 
   *  Provide the DocIdSet to be cached, using the DocIdSet provided
   *  by the wrapped Filter. <p>This implementation returns the given {@link DocIdSet},
   *  if {@link DocIdSet#isCacheable} returns <code>true</code>, else it calls
   *  {@link #cacheImpl(DocIdSetIterator,AtomicReader)}
   *  <p>Note: This method returns {@linkplain DocIdSet#EMPTY} if the given docIdSet
   *  is <code>null</code> or if {@link DocIdSet#iterator()} return <code>null</code>. The empty
   *  instance is use as a placeholder in the cache instead of the <code>null</code> value.
   */
  protected DocIdSet docIdSetToCache(DocIdSet docIdSet, AtomicReader reader) throws IOException {
    if (docIdSet == null) {
      // this is better than returning null, as the nonnull result can be cached
      return EMPTY;
    } else if (docIdSet.isCacheable()) {
      return docIdSet;
    } else {
      final DocIdSetIterator it = docIdSet.iterator();
      // null is allowed to be returned by iterator(),
      // in this case we wrap with the sentinel set,
      // which is cacheable.
      if (it == null) {
        return EMPTY;
      } else {
        return cacheImpl(it, reader);
      }
    }
  }
  
  /**
   * Default cache implementation: uses {@link WAH8DocIdSet}.
   */
  protected DocIdSet cacheImpl(DocIdSetIterator iterator, AtomicReader reader) throws IOException {
    WAH8DocIdSet.Builder builder = new WAH8DocIdSet.Builder();
    builder.add(iterator);
    return builder.build();
  }

  // for testing
  int hitCount, missCount;

  @Override
  public DocIdSet getDocIdSet(AtomicReaderContext context, final Bits acceptDocs) throws IOException {
    final AtomicReader reader = context.reader();
    final Object key = reader.getCoreCacheKey();

    DocIdSet docIdSet = cache.get(key);
    if (docIdSet != null) {
      hitCount++;
    } else {
      missCount++;
      docIdSet = docIdSetToCache(filter.getDocIdSet(context, null), reader);
      assert docIdSet.isCacheable();
      cache.put(key, docIdSet);
    }

    return docIdSet == EMPTY ? null : BitsFilteredDocIdSet.wrap(docIdSet, acceptDocs);
  }
  
  @Override
  public String toString() {
    return getClass().getSimpleName() + "("+filter+")";
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || !getClass().equals(o.getClass())) return false;
    final CachingWrapperFilter other = (CachingWrapperFilter) o;
    return this.filter.equals(other.filter);
  }

  @Override
  public int hashCode() {
    return (filter.hashCode() ^ getClass().hashCode());
  }

  @Override
  public long ramBytesUsed() {

    // Sync only to pull the current set of values:
    List<DocIdSet> docIdSets;
    synchronized(cache) {
      docIdSets = new ArrayList<>(cache.values());
    }

    long total = 0;
    for(DocIdSet dis : docIdSets) {
      total += dis.ramBytesUsed();
    }

    return total;
  }
}
