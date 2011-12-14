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

import java.io.IOException;
import java.lang.ref.SoftReference;
import java.util.WeakHashMap;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.WeakIdentityHashMap;

/**
 * Wraps another filter's result and caches it.  The purpose is to allow
 * filters to simply filter, and then wrap with this class
 * to add caching.
 */
public class CachingWrapperFilter extends Filter {
  // TODO: make this filter aware of ReaderContext. a cached filter could 
  // specify the actual readers key or something similar to indicate on which
  // level of the readers hierarchy it should be cached.
  private final Filter filter;
  private final FilterCache cache = new FilterCache();
  private final boolean recacheDeletes;

  private static class FilterCache implements SegmentReader.CoreClosedListener, IndexReader.ReaderClosedListener {
    private final WeakHashMap<Object,WeakIdentityHashMap<Bits,SoftReference<DocIdSet>>> cache =
      new WeakHashMap<Object,WeakIdentityHashMap<Bits,SoftReference<DocIdSet>>>();

    public synchronized DocIdSet get(IndexReader reader, Bits acceptDocs) throws IOException {
      final Object coreKey = reader.getCoreCacheKey();
      WeakIdentityHashMap<Bits,SoftReference<DocIdSet>> innerCache = cache.get(coreKey);
      if (innerCache == null) {
        if (reader instanceof SegmentReader) {
          ((SegmentReader) reader).addCoreClosedListener(this);
        } else {
          assert reader.getSequentialSubReaders() == null : 
            "we only operate on AtomicContext, so all cached readers must be atomic";
          reader.addReaderClosedListener(this);
        }
        innerCache = new WeakIdentityHashMap<Bits,SoftReference<DocIdSet>>();
        cache.put(coreKey, innerCache);
      }

      final SoftReference<DocIdSet> innerRef = innerCache.get(acceptDocs);
      return innerRef == null ? null : innerRef.get();
    }

    public synchronized void put(IndexReader reader, Bits acceptDocs, DocIdSet value) {
      cache.get(reader.getCoreCacheKey()).put(acceptDocs, new SoftReference<DocIdSet>(value));
    }
    
    @Override
    public synchronized void onClose(IndexReader reader) {
      cache.remove(reader.getCoreCacheKey());
    }
    
    @Override
    public synchronized void onClose(SegmentReader reader) {
      cache.remove(reader.getCoreCacheKey());
    }
  }

  /** Wraps another filter's result and caches it.
   * @param filter Filter to cache results of
   */
  public CachingWrapperFilter(Filter filter) {
    this(filter, false);
  }

  /** Wraps another filter's result and caches it.  If
   *  recacheDeletes is true, then new deletes (for example
   *  after {@link IndexReader#openIfChanged}) will be AND'd
   *  and cached again.
   *
   *  @param filter Filter to cache results of
   */
  public CachingWrapperFilter(Filter filter, boolean recacheDeletes) {
    this.filter = filter;
    this.recacheDeletes = recacheDeletes;
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
  public DocIdSet getDocIdSet(AtomicReaderContext context, final Bits acceptDocs) throws IOException {
    final IndexReader reader = context.reader;

    // Only cache if incoming acceptDocs is == live docs;
    // if Lucene passes in more interesting acceptDocs in
    // the future we don't want to over-cache:
    final boolean doCacheSubAcceptDocs = recacheDeletes && acceptDocs == reader.getLiveDocs();

    final Bits subAcceptDocs;
    if (doCacheSubAcceptDocs) {
      subAcceptDocs = acceptDocs;
    } else {
      subAcceptDocs = null;
    }

    DocIdSet docIdSet = cache.get(reader, subAcceptDocs);
    if (docIdSet != null) {
      hitCount++;
    } else {
      missCount++;
      docIdSet = docIdSetToCache(filter.getDocIdSet(context, subAcceptDocs), reader);
      cache.put(reader, subAcceptDocs, docIdSet);
    }

    if (doCacheSubAcceptDocs) {
      return docIdSet;
    } else {
      return BitsFilteredDocIdSet.wrap(docIdSet, acceptDocs);
    }
  }

  @Override
  public String toString() {
    return "CachingWrapperFilter("+filter+",recacheDeletes=" + recacheDeletes + ")";
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof CachingWrapperFilter)) return false;
    final CachingWrapperFilter other = (CachingWrapperFilter) o;
    return this.filter.equals(other.filter) && this.recacheDeletes == other.recacheDeletes;
  }

  @Override
  public int hashCode() {
    return (filter.hashCode() ^ 0x1117BF25) + (recacheDeletes ? 0 : 1);
  }
}
