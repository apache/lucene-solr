package org.apache.lucene.search;
/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Wraps another SpanFilter's result and caches it.  The purpose is to allow
 * filters to simply filter, and then wrap with this class to add caching.
 */
public class CachingSpanFilter extends SpanFilter {
  private SpanFilter filter;

  /**
   * A transient Filter cache (package private because of test)
   */
  private final CachingWrapperFilter.FilterCache<SpanFilterResult> cache;

  /**
   * New deletions always result in a cache miss, by default
   * ({@link CachingWrapperFilter.DeletesMode#RECACHE}.
   * @param filter Filter to cache results of
   */
  public CachingSpanFilter(SpanFilter filter) {
    this.filter = filter;
    this.cache = new CachingWrapperFilter.FilterCache<SpanFilterResult>();
  }

  @Override
  public DocIdSet getDocIdSet(AtomicReaderContext context, final Bits acceptDocs) throws IOException {
    final SpanFilterResult result = getCachedResult(context);
    return BitsFilteredDocIdSet.wrap(result.getDocIdSet(), acceptDocs);
  }

  @Override
  public SpanFilterResult bitSpans(AtomicReaderContext context, final Bits acceptDocs) throws IOException {
    final SpanFilterResult result = getCachedResult(context);
    if (acceptDocs == null) {
      return result;
    } else {
      // TODO: filter positions more efficient
      List<SpanFilterResult.PositionInfo> allPositions = result.getPositions();
      List<SpanFilterResult.PositionInfo> positions = new ArrayList<SpanFilterResult.PositionInfo>(allPositions.size() / 2 + 1);
      for (SpanFilterResult.PositionInfo p : allPositions) {
        if (acceptDocs.get(p.getDoc())) {
          positions.add(p);
        }        
      }
      return new SpanFilterResult(BitsFilteredDocIdSet.wrap(result.getDocIdSet(), acceptDocs), positions);
    }
  }

  /** Provide the DocIdSet to be cached, using the DocIdSet provided
   *  by the wrapped Filter.
   *  <p>This implementation returns the given {@link DocIdSet}, if {@link DocIdSet#isCacheable}
   *  returns <code>true</code>, else it copies the {@link DocIdSetIterator} into
   *  an {@link FixedBitSet}.
   */
  protected SpanFilterResult spanFilterResultToCache(SpanFilterResult result, IndexReader reader) throws IOException {
    if (result == null || result.getDocIdSet() == null) {
      // this is better than returning null, as the nonnull result can be cached
      return SpanFilterResult.EMPTY_SPAN_FILTER_RESULT;
    } else if (result.getDocIdSet().isCacheable()) {
      return result;
    } else {
      final DocIdSetIterator it = result.getDocIdSet().iterator();
      // null is allowed to be returned by iterator(),
      // in this case we wrap with the empty set,
      // which is cacheable.
      if (it == null) {
        return SpanFilterResult.EMPTY_SPAN_FILTER_RESULT;
      } else {
        final FixedBitSet bits = new FixedBitSet(reader.maxDoc());
        bits.or(it);
        return new SpanFilterResult(bits, result.getPositions());
      }
    }
  }
  
  // for testing
  int hitCount, missCount;

  private SpanFilterResult getCachedResult(AtomicReaderContext context) throws IOException {
    final IndexReader reader = context.reader;
    final Object coreKey = reader.getCoreCacheKey();

    SpanFilterResult result = cache.get(reader, coreKey);
    if (result != null) {
      hitCount++;
      return result;
    } else {
      missCount++;
      // cache miss: we use no acceptDocs here
      // (this saves time on building SpanFilterResult, the acceptDocs will be applied on the cached set)
      result = spanFilterResultToCache(filter.bitSpans(context, null/**!!!*/), reader);
      cache.put(coreKey, result);
    }
    
    return result;
  }

  @Override
  public String toString() {
    return "CachingSpanFilter("+filter+")";
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof CachingSpanFilter)) return false;
    return this.filter.equals(((CachingSpanFilter)o).filter);
  }

  @Override
  public int hashCode() {
    return filter.hashCode() ^ 0x1117BF25;
  }
}
