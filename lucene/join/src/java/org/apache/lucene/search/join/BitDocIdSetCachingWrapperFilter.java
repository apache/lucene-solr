package org.apache.lucene.search.join;

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

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CachingWrapperFilter;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilterCachingPolicy;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BitDocIdSet;

/**
 * A filter wrapper that transforms the produces doc id sets into
 * {@link BitDocIdSet}s if necessary and caches them.
 */
public class BitDocIdSetCachingWrapperFilter extends BitDocIdSetFilter implements Accountable {

  private final CachingWrapperFilter filter;

  /** Sole constructor. */
  public BitDocIdSetCachingWrapperFilter(Filter filter) {
    super();
    this.filter = new CachingWrapperFilter(filter, FilterCachingPolicy.ALWAYS_CACHE) {
      @Override
      protected BitDocIdSet docIdSetToCache(DocIdSet docIdSet, LeafReader reader) throws IOException {
        if (docIdSet == null || docIdSet instanceof BitDocIdSet) {
          // this is different from CachingWrapperFilter: even when the DocIdSet is
          // cacheable, we convert it to a BitSet since we require all the
          // cached filters to be BitSets
          return (BitDocIdSet) docIdSet;
        }

        final DocIdSetIterator it = docIdSet.iterator();
        if (it == null) {
          return null;
        }
        BitDocIdSet.Builder builder = new BitDocIdSet.Builder(reader.maxDoc());
        builder.or(it);
        return builder.build();
      }
    };
  }

  @Override
  public BitDocIdSet getDocIdSet(LeafReaderContext context) throws IOException {
    return (BitDocIdSet) filter.getDocIdSet(context, null);
  }

  @Override
  public int hashCode() {
    return getClass().hashCode() ^ filter.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof BitDocIdSetCachingWrapperFilter == false) {
      return false;
    }
    return filter.equals(((BitDocIdSetCachingWrapperFilter) obj).filter);
  }

  @Override
  public String toString() {
    return filter.toString();
  }

  @Override
  public long ramBytesUsed() {
    return filter.ramBytesUsed();
  }

  @Override
  public Iterable<Accountable> getChildResources() {
    return filter.getChildResources();
  }

}
