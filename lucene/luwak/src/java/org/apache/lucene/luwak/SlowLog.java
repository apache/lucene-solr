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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Reports on slow queries in a given match run
 */
public class SlowLog implements Iterable<SlowLog.Entry> {

  private long limit;
  private final List<Entry> slowQueries = new ArrayList<>();

  /**
   * Record all queries that take longer than this limit (in ns)
   *
   * @param limit the limit
   */
  public void setLimit(long limit) {
    this.limit = limit;
  }

  /**
   * @return the current limit above which slow queries are recorded
   */
  public long getLimit() {
    return limit;
  }

  /**
   * Add a query and time taken to the slow log.
   * <p>
   * The query will only be recorded if the time is above the configured limit
   *
   * @param query the query id
   * @param time  the time taken by the query in ns
   */
  public void addQuery(String query, long time) {
    if (time < limit)
      return;
    slowQueries.add(new Entry(query, time));
  }

  /**
   * Add all entries to this slow log
   *
   * @param queries the entries to add
   */
  public void addAll(Iterable<SlowLog.Entry> queries) {
    for (SlowLog.Entry query : queries) {
      if (query.time > limit)
        slowQueries.add(query);
    }
  }

  @Override
  public Iterator<Entry> iterator() {
    return slowQueries.iterator();
  }

  /**
   * An individual entry in the slow log
   */
  public static class Entry {

    /**
     * The query id
     */
    final String queryId;

    /**
     * The time taken to execute the query in ms
     */
    final long time;

    Entry(String queryId, long time) {
      this.queryId = queryId;
      this.time = time;
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("Limit: ").append(limit).append("\n");
    for (Entry entry : slowQueries) {
      sb.append("\t").append(entry.queryId).append(" [").append(entry.time).append("ns]\n");
    }
    return sb.toString();
  }
}
