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

import org.apache.lucene.util.FrequencyTrackingRingBuffer;

/**
 * A {@link QueryCachingPolicy} that tracks usage statistics of recently-used
 * filters in order to decide on which filters are worth caching.
 *
 * @lucene.experimental
 */
public final class UsageTrackingQueryCachingPolicy implements QueryCachingPolicy {

  // the hash code that we use as a sentinel in the ring buffer.
  private static final int SENTINEL = Integer.MIN_VALUE;

  private static boolean isPointQuery(Query query) {
    // we need to check for super classes because we occasionally use anonymous
    // sub classes of eg. PointRangeQuery
    for (Class<?> clazz = query.getClass(); clazz != Query.class; clazz = clazz.getSuperclass()) {
      final String simpleName = clazz.getSimpleName();
      if (simpleName.startsWith("Point") && simpleName.endsWith("Query")) {
        return true;
      }
    }
    return false;
  }

  static boolean isCostly(Query query) {
    // This does not measure the cost of iterating over the filter (for this we
    // already have the DocIdSetIterator#cost API) but the cost to build the
    // DocIdSet in the first place
    return query instanceof MultiTermQuery ||
        query instanceof MultiTermQueryConstantScoreWrapper ||
        isPointQuery(query) ||
        // can't refer to TermsQuery directly as it is in another module
        "TermsQuery".equals(query.getClass().getSimpleName());
  }

  static boolean isCheap(Query query) {
    // same for cheap queries
    // these queries are so cheap that they usually do not need caching
    return query instanceof TermQuery;
  }

  private final FrequencyTrackingRingBuffer recentlyUsedFilters;

  /**
   * Create a new instance.
   *
   * @param historySize               the number of recently used filters to track
   */
  public UsageTrackingQueryCachingPolicy(int historySize) {
    this.recentlyUsedFilters = new FrequencyTrackingRingBuffer(historySize, SENTINEL);
  }

  /** Create a new instance with an history size of 256. */
  public UsageTrackingQueryCachingPolicy() {
    this(256);
  }

  /**
   * For a given query, return how many times it should appear in the history
   * before being cached.
   */
  protected int minFrequencyToCache(Query query) {
    if (isCostly(query)) {
      return 2;
    } else if (isCheap(query)) {
      return 20;
    } else {
      // default: cache after the filter has been seen 5 times
      return 5;
    }
  }

  @Override
  public void onUse(Query query) {
    assert query instanceof BoostQuery == false;
    assert query instanceof ConstantScoreQuery == false;

    // call hashCode outside of sync block
    // in case it's somewhat expensive:
    int hashCode = query.hashCode();

    // we only track hash codes to avoid holding references to possible
    // large queries; this may cause rare false positives, but at worse
    // this just means we cache a query that was not in fact used enough:
    synchronized (this) {
      recentlyUsedFilters.add(hashCode);
    }
  }

  int frequency(Query query) {
    assert query instanceof BoostQuery == false;
    assert query instanceof ConstantScoreQuery == false;

    // call hashCode outside of sync block
    // in case it's somewhat expensive:
    int hashCode = query.hashCode();

    synchronized (this) {
      return recentlyUsedFilters.frequency(hashCode);
    }
  }

  @Override
  public boolean shouldCache(Query query) throws IOException {
    if (query instanceof MatchAllDocsQuery
        // MatchNoDocsQuery currently rewrites to a BooleanQuery,
        // but who knows, it might get its own Weight one day
        || query instanceof MatchNoDocsQuery) {
      return false;
    }
    if (query instanceof BooleanQuery) {
      BooleanQuery bq = (BooleanQuery) query;
      if (bq.clauses().isEmpty()) {
        return false;
      }
    }
    if (query instanceof DisjunctionMaxQuery) {
      DisjunctionMaxQuery dmq = (DisjunctionMaxQuery) query;
      if (dmq.getDisjuncts().isEmpty()) {
        return false;
      }
    }
    final int frequency = frequency(query);
    final int minFrequency = minFrequencyToCache(query);
    return frequency >= minFrequency;
  }

}
