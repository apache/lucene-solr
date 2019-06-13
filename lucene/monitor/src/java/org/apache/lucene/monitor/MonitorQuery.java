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

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import org.apache.lucene.search.Query;

/**
 * Defines a query to be stored in a Monitor
 */
public class MonitorQuery {

  private final String id;
  private final Query query;
  private final String queryString;
  private final Map<String, String> metadata;

  /**
   * Creates a new MonitorQuery
   *
   * @param id          the query ID
   * @param query       the query to store
   * @param queryString an optional string representation of the query, for persistent Monitors
   * @param metadata    metadata passed to {@link Presearcher#indexQuery(Query, Map)}.  Must not
   *                    have any null values
   */
  public MonitorQuery(String id, Query query, String queryString, Map<String, String> metadata) {
    this.id = id;
    this.query = query;
    this.queryString = queryString;
    this.metadata = Collections.unmodifiableMap(new TreeMap<>(metadata));
    checkNullEntries(this.metadata);
  }

  /**
   * Creates a new MonitorQuery with empty metadata and no string representation
   *
   * @param id    the ID
   * @param query the query
   */
  public MonitorQuery(String id, Query query) {
    this(id, query, null, Collections.emptyMap());
  }

  private static void checkNullEntries(Map<String, String> metadata) {
    for (Map.Entry<String, String> entry : metadata.entrySet()) {
      if (entry.getValue() == null)
        throw new IllegalArgumentException("Null value for key " + entry.getKey() + " in metadata map");
    }
  }

  /**
   * @return this MonitorQuery's ID
   */
  public String getId() {
    return id;
  }

  /**
   * @return this MonitorQuery's query
   */
  public Query getQuery() {
    return query;
  }

  /**
   * @return this MonitorQuery's string representation
   */
  public String getQueryString() {
    return queryString;
  }

  /**
   * @return this MonitorQuery's metadata
   */
  public Map<String, String> getMetadata() {
    return metadata;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    MonitorQuery that = (MonitorQuery) o;
    return Objects.equals(id, that.id) && Objects.equals(query, that.query) && Objects.equals(metadata, that.metadata);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, query, metadata);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(id);
    sb.append(": ");
    if (queryString == null) {
      sb.append(query.toString());
    }
    else {
      sb.append(queryString);
    }
    if (metadata.size() != 0) {
      sb.append(" { ");
      int n = metadata.size();
      for (Map.Entry<String, String> entry : metadata.entrySet()) {
        n--;
        sb.append(entry.getKey()).append(": ").append(entry.getValue());
        if (n > 0)
          sb.append(", ");
      }
      sb.append(" }");
    }
    return sb.toString();
  }
}
