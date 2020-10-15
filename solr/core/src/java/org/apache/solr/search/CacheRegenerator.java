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
package org.apache.solr.search;

import java.io.IOException;

/**
 * Implementations of <code>CacheRegenerator</code> are used in autowarming to populate a new cache
 * based on an old cache.  <code>regenerateItem</code> is called for each item that should be inserted into the new cache.
 * <p>
 * Implementations should have a noarg constructor and be thread safe (a single instance will be
 * used for all cache autowarmings).
 *
 *
 */
public interface CacheRegenerator {
  /**
   * Regenerate an old cache item and insert it into <code>newCache</code>
   *
   * @param newSearcher the new searcher who's caches are being autowarmed
   * @param newCache    where regenerated cache items should be stored. the target of the autowarming
   * @param oldCache    the old cache being used as a source for autowarming
   * @param oldKey      the key of the old cache item to regenerate in the new cache
   * @param oldVal      the old value of the cache item
   * @return true to continue with autowarming, false to stop
   */
  public boolean regenerateItem(SolrIndexSearcher newSearcher,
                                @SuppressWarnings({"rawtypes"})SolrCache newCache,
                                @SuppressWarnings({"rawtypes"})SolrCache oldCache, Object oldKey, Object oldVal) throws IOException;
}
