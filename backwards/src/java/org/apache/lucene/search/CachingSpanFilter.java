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
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Wraps another SpanFilter's result and caches it.  The purpose is to allow
 * filters to simply filter, and then wrap with this class to add caching.
 */
public class CachingSpanFilter extends SpanFilter {
  private SpanFilter filter;

  /**
   * A transient Filter cache (package private because of test)
   */
  private transient Map<IndexReader,SpanFilterResult> cache;

  private final ReentrantLock lock = new ReentrantLock();

  /**
   * @param filter Filter to cache results of
   */
  public CachingSpanFilter(SpanFilter filter) {
    this.filter = filter;
  }

  @Override
  public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
    SpanFilterResult result = getCachedResult(reader);
    return result != null ? result.getDocIdSet() : null;
  }
  
  private SpanFilterResult getCachedResult(IndexReader reader) throws IOException {
    lock.lock();
    try {
      if (cache == null) {
        cache = new WeakHashMap<IndexReader,SpanFilterResult>();
      }
      final SpanFilterResult cached = cache.get(reader);
      if (cached != null) return cached;
    } finally {
      lock.unlock();
    }
    
    final SpanFilterResult result = filter.bitSpans(reader);
    lock.lock();
    try {
      cache.put(reader, result);
    } finally {
      lock.unlock();
    }
    
    return result;
  }


  @Override
  public SpanFilterResult bitSpans(IndexReader reader) throws IOException {
    return getCachedResult(reader);
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
