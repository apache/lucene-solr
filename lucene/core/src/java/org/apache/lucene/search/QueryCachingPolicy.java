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
package org.apache.lucene.search;


import java.io.IOException;

/**
 * A policy defining which filters should be cached.
 *
 * Implementations of this class must be thread-safe.
 *
 * @see UsageTrackingQueryCachingPolicy
 * @see LRUQueryCache
 * @lucene.experimental
 */
// TODO: add APIs for integration with IndexWriter.IndexReaderWarmer
public interface QueryCachingPolicy {

  /** A simple policy that caches all the provided filters on all segments.
   *  @deprecated This policy is inefficient as caching too aggressively
   *  disables the ability to skip non-interesting documents. See
   *  {@link UsageTrackingQueryCachingPolicy}. */
  @Deprecated
  public static final QueryCachingPolicy ALWAYS_CACHE = new QueryCachingPolicy() {

    @Override
    public void onUse(Query query) {}

    @Override
    public boolean shouldCache(Query query) throws IOException {
      return true;
    }

  };

  /** Callback that is called every time that a cached filter is used.
   *  This is typically useful if the policy wants to track usage statistics
   *  in order to make decisions. */
  void onUse(Query query);

  /** Whether the given {@link Query} is worth caching.
   *  This method will be called by the {@link QueryCache} to know whether to
   *  cache. It will first attempt to load a {@link DocIdSet} from the cache.
   *  If it is not cached yet and this method returns <tt>true</tt> then a
   *  cache entry will be generated. Otherwise an uncached scorer will be
   *  returned. */
  boolean shouldCache(Query query) throws IOException;

}
