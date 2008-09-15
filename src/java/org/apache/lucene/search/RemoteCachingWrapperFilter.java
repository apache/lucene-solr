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
import java.util.BitSet;

import org.apache.lucene.index.IndexReader;

/**
 * Provides caching of {@link Filter}s themselves on the remote end of an RMI connection.
 * The cache is keyed on Filter's hashCode(), so if it sees the same filter twice
 * it will reuse the original version.
 * <p/>
 * NOTE: This does NOT cache the Filter bits, but rather the Filter itself.
 * Thus, this works hand-in-hand with {@link CachingWrapperFilter} to keep both
 * file Filter cache and the Filter bits on the remote end, close to the searcher.
 * <p/>
 * Usage:
 * <p/>
 * To cache a result you must do something like 
 * RemoteCachingWrapperFilter f = new RemoteCachingWrapperFilter(new CachingWrapperFilter(myFilter));
 * <p/>
 */
public class RemoteCachingWrapperFilter extends Filter {
  protected Filter filter;

  public RemoteCachingWrapperFilter(Filter filter) {
    this.filter = filter;
  }

  /**
   * Uses the {@link FilterManager} to keep the cache for a filter on the 
   * searcher side of a remote connection.
   * @param reader the index reader for the Filter
   * @return the bitset
   * @deprecated Use {@link #getDocIdSet(IndexReader)} instead.
   */
  public BitSet bits(IndexReader reader) throws IOException {
    Filter cachedFilter = FilterManager.getInstance().getFilter(filter);
    return cachedFilter.bits(reader);
  }
  
  /**
   * Uses the {@link FilterManager} to keep the cache for a filter on the 
   * searcher side of a remote connection.
   * @param reader the index reader for the Filter
   * @return the DocIdSet
   */
  public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
    Filter cachedFilter = FilterManager.getInstance().getFilter(filter);
    return cachedFilter.getDocIdSet(reader);
  }
}
