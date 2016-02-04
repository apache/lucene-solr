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
package org.apache.lucene.search.join;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.BitDocIdSet;

/**
 * {@link Filter} wrapper that implements {@link BitDocIdSetFilter}.
 * @deprecated Use {@link QueryBitSetProducer} instead
 */
@Deprecated
public class BitDocIdSetCachingWrapperFilter extends BitDocIdSetFilter {
  private final Filter filter;
  private final Map<Object,DocIdSet> cache = Collections.synchronizedMap(new WeakHashMap<Object,DocIdSet>());

  /** Wraps another filter's result and caches it into bitsets.
   * @param filter Filter to cache results of
   */
  public BitDocIdSetCachingWrapperFilter(Filter filter) {
    this.filter = filter;
  }

  /**
   * Gets the contained filter.
   * @return the contained filter.
   */
  public Filter getFilter() {
    return filter;
  }

  private BitDocIdSet docIdSetToCache(DocIdSet docIdSet, LeafReader reader) throws IOException {
    final DocIdSetIterator it = docIdSet.iterator();
    if (it == null) {
      return null;
    } else {
      BitDocIdSet.Builder builder = new BitDocIdSet.Builder(reader.maxDoc());
      builder.or(it);
      return builder.build();
    }
  }
  
  @Override
  public BitDocIdSet getDocIdSet(LeafReaderContext context) throws IOException {
    final LeafReader reader = context.reader();
    final Object key = reader.getCoreCacheKey();

    DocIdSet docIdSet = cache.get(key);
    if (docIdSet == null) {
      docIdSet = filter.getDocIdSet(context, null);
      docIdSet = docIdSetToCache(docIdSet, reader);
      if (docIdSet == null) {
        // We use EMPTY as a sentinel for the empty set, which is cacheable
        docIdSet = DocIdSet.EMPTY;
      }
      cache.put(key, docIdSet);
    }
    return docIdSet == DocIdSet.EMPTY ? null : (BitDocIdSet) docIdSet;
  }
  
  @Override
  public String toString(String field) {
    return getClass().getSimpleName() + "("+filter.toString(field)+")";
  }

  @Override
  public boolean equals(Object o) {
    if (super.equals(o) == false) {
      return false;
    }
    final BitDocIdSetCachingWrapperFilter other = (BitDocIdSetCachingWrapperFilter) o;
    return this.filter.equals(other.filter);
  }

  @Override
  public int hashCode() {
    return 31 * super.hashCode() + filter.hashCode();
  }
}
