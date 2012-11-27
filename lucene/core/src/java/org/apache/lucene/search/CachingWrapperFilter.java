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
import java.util.Map;
import java.util.WeakHashMap;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DirectoryReader; // javadocs
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.Bits;

/**
 * Wraps another {@link Filter}'s result and caches it.  The purpose is to allow
 * filters to simply filter, and then wrap with this class
 * to add caching.
 */
public class CachingWrapperFilter extends Filter {
  // TODO: make this filter aware of ReaderContext. a cached filter could 
  // specify the actual readers key or something similar to indicate on which
  // level of the readers hierarchy it should be cached.
  private final Filter filter;
  private final Map<Object,DocIdSet> cache = Collections.synchronizedMap(new WeakHashMap<Object,DocIdSet>());

  /** Wraps another filter's result and caches it.
   * @param filter Filter to cache results of
   */
  public CachingWrapperFilter(Filter filter) {
    this.filter = filter;
  }

  /** Provide the DocIdSet to be cached, using the DocIdSet provided
   *  by the wrapped Filter.
   *  <p>This implementation returns the given {@link DocIdSet}, if {@link DocIdSet#isCacheable}
   *  returns <code>true</code>, else it copies the {@link DocIdSetIterator} into
   *  a {@link FixedBitSet}.
   */
  protected DocIdSet docIdSetToCache(DocIdSet docIdSet, AtomicReader reader) throws IOException {
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
  public DocIdSet getDocIdSet(AtomicReaderContext context, final Bits acceptDocs) throws IOException {
    final AtomicReader reader = context.reader();
    final Object key = reader.getCoreCacheKey();

    DocIdSet docIdSet = cache.get(key);
    if (docIdSet != null) {
      hitCount++;
    } else {
      missCount++;
      docIdSet = docIdSetToCache(filter.getDocIdSet(context, null), reader);
      cache.put(key, docIdSet);
    }

    return BitsFilteredDocIdSet.wrap(docIdSet, acceptDocs);
  }

  @Override
  public String toString() {
    return "CachingWrapperFilter("+filter+")";
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof CachingWrapperFilter)) return false;
    final CachingWrapperFilter other = (CachingWrapperFilter) o;
    return this.filter.equals(other.filter);
  }

  @Override
  public int hashCode() {
    return (filter.hashCode() ^ 0x1117BF25);
  }
}
