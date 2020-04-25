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

package org.apache.lucene.monitor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.lucene.search.Query;

class QueryCacheEntry {

  /**
   * The (possibly partial due to decomposition) query
   */
  final Query matchQuery;

  /**
   * The id of this query
   */
  final String cacheId;

  /**
   * The id of the MonitorQuery that produced this entry
   *
   * Note that this may be different to {@link #cacheId} due to decomposition
   */
  final String queryId;

  /**
   * The metadata from the entry's parent {@link MonitorQuery}
   */
  final Map<String, String> metadata;

  private QueryCacheEntry(String cacheId, String queryId, Query matchQuery, Map<String, String> metadata) {
    this.cacheId = cacheId;
    this.queryId = queryId;
    this.matchQuery = matchQuery;
    this.metadata = metadata;
  }

  static List<QueryCacheEntry> decompose(MonitorQuery mq, QueryDecomposer decomposer) {
    int upto = 0;
    List<QueryCacheEntry> cacheEntries = new ArrayList<>();
    for (Query subquery : decomposer.decompose(mq.getQuery())) {
      cacheEntries.add(new QueryCacheEntry(mq.getId() + "_" + upto, mq.getId(), subquery, mq.getMetadata()));
      upto++;
    }
    return cacheEntries;
  }

  @Override
  public String toString() {
    return queryId + "/" + cacheId + "/" + matchQuery;
  }
}
