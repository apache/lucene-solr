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

import java.io.IOException;
import java.util.BitSet;
import java.util.Map;
import java.util.WeakHashMap;

/**
 * Wraps another SpanFilter's result and caches it.  The purpose is to allow
 * filters to simply filter, and then wrap with this class to add caching.
 */
public class CachingSpanFilter extends SpanFilter {
  protected SpanFilter filter;

  /**
   * A transient Filter cache.  To cache Filters even when using {@link org.apache.lucene.search.RemoteSearchable} use
   * {@link org.apache.lucene.search.RemoteCachingWrapperFilter} instead.
   */
  protected transient Map cache;

  /**
   * @param filter Filter to cache results of
   */
  public CachingSpanFilter(SpanFilter filter) {
    this.filter = filter;
  }

  /**
   * @deprecated Use {@link #getDocIdSet(IndexReader)} instead.
   */
  public BitSet bits(IndexReader reader) throws IOException {
    SpanFilterResult result = getCachedResult(reader);
    return result != null ? result.getBits() : null;
  }
  
  public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
    SpanFilterResult result = getCachedResult(reader);
    return result != null ? result.getDocIdSet() : null;
  }
  
  private SpanFilterResult getCachedResult(IndexReader reader) throws IOException {
    SpanFilterResult result = null;
    if (cache == null) {
      cache = new WeakHashMap();
    }

    synchronized (cache) {  // check cache
      result = (SpanFilterResult) cache.get(reader);
      if (result == null) {
        result = filter.bitSpans(reader);
        cache.put(reader, result);
      }
    }
    return result;
  }


  public SpanFilterResult bitSpans(IndexReader reader) throws IOException {
    return getCachedResult(reader);
  }

  public String toString() {
    return "CachingSpanFilter("+filter+")";
  }

  public boolean equals(Object o) {
    if (!(o instanceof CachingSpanFilter)) return false;
    return this.filter.equals(((CachingSpanFilter)o).filter);
  }

  public int hashCode() {
    return filter.hashCode() ^ 0x1117BF25;
  }
}
