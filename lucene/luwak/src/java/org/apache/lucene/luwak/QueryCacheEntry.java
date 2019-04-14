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

package org.apache.lucene.luwak;

import java.util.Map;

import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;

/**
 * An entry in the query cache
 */
public class QueryCacheEntry {

  /**
   * The (possibly partial due to decomposition) query
   */
  public final Query matchQuery;

  /**
   * A hash value for lookups
   */
  public final BytesRef hash;

  /**
   * The metadata from the entry's parent {@link MonitorQuery}
   */
  public final Map<String, String> metadata;

  public QueryCacheEntry(BytesRef hash, Query matchQuery, Map<String, String> metadata) {
    this.hash = hash;
    this.matchQuery = matchQuery;
    this.metadata = metadata;
  }
}
