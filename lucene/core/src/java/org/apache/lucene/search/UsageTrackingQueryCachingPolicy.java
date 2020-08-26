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
public class UsageTrackingQueryCachingPolicy implements QueryCachingPolicy {

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
        query instanceof TermInSetQuery ||
        isPointQuery(query);
  }

  private static boolean shouldNeverCache(Query query) {
    if (query instanceof TermQuery) {
      // We do not bother caching term queries since they are already plenty fast.
      return true;
    }

    if (query instanceof DocValuesFieldExistsQuery) {
      // We do not bother caching DocValuesFieldExistsQuery queries since they are already plenty fast.
      return true;
    }

    if (query instanceof MatchAllDocsQuery) {
      // MatchAllDocsQuery has an iterator that is faster than what a bit set could do.
      return true;
    }

    // For the below queries, it's cheap to notice they cannot match any docs so
    // we do not bother caching them.
    if (query instanceof MatchNoDocsQuery) {
      return true;
    }

    if (query instanceof BooleanQuery) {
      BooleanQuery bq = (BooleanQuery) query;
      if (bq.clauses().isEmpty()) {
        return true;
      }
    }

    if (query instanceof DisjunctionMaxQuery) {
      DisjunctionMaxQuery dmq = (DisjunctionMaxQuery) query;
      if (dmq.getDisjuncts().isEmpty()) {
        return true;
      }
    }

    return false;
  }

  private final FrequencyTrackingRingBuffer recentlyUsedFilters;

  /**
   * Expert: Create a new instance with a configurable history size. Beware of
   * passing too large values for the size of the history, either
   * {@link #minFrequencyToCache} returns low values and this means some filters
   * that are rarely used will be cached, which would hurt performance. Or
   * {@link #minFrequencyToCache} returns high values that are function of the
   * size of the history but then filters will be slow to make it to the cache.
   *
   * @param historySize               the number of recently used filters to track
   */
  public UsageTrackingQueryCachingPolicy(int historySize) {
    this.recentlyUsedFilters = new FrequencyTrackingRingBuffer(historySize, SENTINEL);
  }

  /** Create a new instance with an history size of 256. This should be a good
   *  default for most cases. */
  public UsageTrackingQueryCachingPolicy() {
    this(256);
  }

  /**
   * For a given filter, return how many times it should appear in the history
   * before being cached. The default implementation returns 2 for filters that
   * need to evaluate against the entire index to build a {@link DocIdSetIterator},
   * like {@link MultiTermQuery}, point-based queries or {@link TermInSetQuery},
   * and 5 for other filters.
   */
  protected int minFrequencyToCache(Query query) {
    if (isCostly(query)) {
      return 2;
    } else {
      // default: cache after the filter has been seen 5 times
      int minFrequency = 5;
      if (query instanceof BooleanQuery
          || query instanceof DisjunctionMaxQuery) {
        // Say you keep reusing a boolean query that looks like "A OR B" and
        // never use the A and B queries out of that context. 5 times after it
        // has been used, we would cache both A, B and A OR B, which is
        // wasteful. So instead we cache compound queries a bit earlier so that
        // we would only cache "A OR B" in that case.
        minFrequency--;
      }
      return minFrequency;
    }
  }

  @Override
  public void onUse(Query query) {
    assert query instanceof BoostQuery == false;
    assert query instanceof ConstantScoreQuery == false;

    if (shouldNeverCache(query)) {
      return;
    }

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
    if (shouldNeverCache(query)) {
      return false;
    }
    final int frequency = frequency(query);
    final int minFrequency = minFrequencyToCache(query);
    return frequency >= minFrequency;
  }

}
