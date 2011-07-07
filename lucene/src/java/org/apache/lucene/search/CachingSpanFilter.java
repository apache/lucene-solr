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

import java.io.IOException;

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
    this(filter, CachingWrapperFilter.DeletesMode.RECACHE);
  }

  /**
   * @param filter Filter to cache results of
   * @param deletesMode See {@link CachingWrapperFilter.DeletesMode}
   */
  public CachingSpanFilter(SpanFilter filter, CachingWrapperFilter.DeletesMode deletesMode) {
    this.filter = filter;
    if (deletesMode == CachingWrapperFilter.DeletesMode.DYNAMIC) {
      throw new IllegalArgumentException("DeletesMode.DYNAMIC is not supported");
    }
    this.cache = new CachingWrapperFilter.FilterCache<SpanFilterResult>(deletesMode) {
      @Override
      protected SpanFilterResult mergeLiveDocs(final Bits liveDocs, final SpanFilterResult value) {
        throw new IllegalStateException("DeletesMode.DYNAMIC is not supported");
      }
    };
  }

  @Override
  public DocIdSet getDocIdSet(AtomicReaderContext context) throws IOException {
    SpanFilterResult result = getCachedResult(context);
    return result != null ? result.getDocIdSet() : null;
  }
  
  // for testing
  int hitCount, missCount;

  private SpanFilterResult getCachedResult(AtomicReaderContext context) throws IOException {
    final IndexReader reader = context.reader;

    final Object coreKey = reader.getCoreCacheKey();
    final Object delCoreKey = reader.hasDeletions() ? reader.getLiveDocs() : coreKey;

    SpanFilterResult result = cache.get(reader, coreKey, delCoreKey);
    if (result != null) {
      hitCount++;
      return result;
    }

    missCount++;
    result = filter.bitSpans(context);

    cache.put(coreKey, delCoreKey, result);
    return result;
  }


  @Override
  public SpanFilterResult bitSpans(AtomicReaderContext context) throws IOException {
    return getCachedResult(context);
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
