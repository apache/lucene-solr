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

import org.apache.lucene.index.IndexReader;
import java.util.BitSet;
import java.util.WeakHashMap;
import java.util.Map;
import java.io.IOException;

/**
 * Wraps another filter's result and caches it.  The purpose is to allow
 * filters to simply filter, and then wrap with this class to add caching.
 */
public class CachingWrapperFilter extends Filter {
  protected Filter filter;

  /**
   * A transient Filter cache.  To cache Filters even when using {@link RemoteSearchable} use
   * {@link RemoteCachingWrapperFilter} instead.
   */
  protected transient Map cache;

  /**
   * @param filter Filter to cache results of
   */
  public CachingWrapperFilter(Filter filter) {
    this.filter = filter;
  }

  /**
   * @deprecated Use {@link #getDocIdSet(IndexReader)} instead.
   */
  public BitSet bits(IndexReader reader) throws IOException {
    if (cache == null) {
      cache = new WeakHashMap();
    }

    synchronized (cache) {  // check cache
      BitSet cached = (BitSet) cache.get(reader);
      if (cached != null) {
        return cached;
      }
    }

    final BitSet bits = filter.bits(reader);

    synchronized (cache) {  // update cache
      cache.put(reader, bits);
    }

    return bits;
  }
  
  public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
    if (cache == null) {
      cache = new WeakHashMap();
    }

    synchronized (cache) {  // check cache
      DocIdSet cached = (DocIdSet) cache.get(reader);
      if (cached != null) {
        return cached;
      }
    }

    final DocIdSet docIdSet = filter.getDocIdSet(reader);

    synchronized (cache) {  // update cache
      cache.put(reader, docIdSet);
    }

    return docIdSet;
    
  }

  public String toString() {
    return "CachingWrapperFilter("+filter+")";
  }

  public boolean equals(Object o) {
    if (!(o instanceof CachingWrapperFilter)) return false;
    return this.filter.equals(((CachingWrapperFilter)o).filter);
  }

  public int hashCode() {
    return filter.hashCode() ^ 0x1117BF25;  
  }
}
