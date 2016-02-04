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
package org.apache.lucene.queryparser.xml.builders;

import org.apache.lucene.queryparser.xml.*;
import org.apache.lucene.search.CachingWrapperQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCache;
import org.apache.lucene.search.QueryWrapperFilter;
import org.w3c.dom.Element;

import java.util.Map;

/**
 * Filters are cached in an LRU Cache keyed on the contained query or filter object. Using this will
 * speed up overall performance for repeated uses of the same expensive query/filter. The sorts of
 * queries/filters likely to benefit from caching need not necessarily be complex - e.g. simple
 * TermQuerys with a large DF (document frequency) can be expensive  on large indexes.
 * A good example of this might be a term query on a field with only 2 possible  values -
 * "true" or "false". In a large index, querying or filtering on this field requires reading
 * millions  of document ids from disk which can more usefully be cached as a filter bitset.
 * <p>
 * For Queries/Filters to be cached and reused the object must implement hashcode and
 * equals methods correctly so that duplicate queries/filters can be detected in the cache.
 * <p>
 * The CoreParser.maxNumCachedFilters property can be used to control the size of the LRU
 * Cache established during the construction of CoreParser instances.
 * @deprecated You should plug a {@link QueryCache} into {@link IndexSearcher} instead
 * and let it make decisions.
 */
@Deprecated
public class CachedFilterBuilder implements FilterBuilder {

  private final QueryBuilderFactory queryFactory;
  private final FilterBuilderFactory filterFactory;

  private LRUCache<Object, Query> filterCache;

  private final int cacheSize;

  public CachedFilterBuilder(QueryBuilderFactory queryFactory,
                             FilterBuilderFactory filterFactory,
                             int cacheSize) {
    this.queryFactory = queryFactory;
    this.filterFactory = filterFactory;
    this.cacheSize = cacheSize;
  }

  @Override
  public synchronized Filter getFilter(Element e) throws ParserException {
    Element childElement = DOMUtils.getFirstChildOrFail(e);

    if (filterCache == null) {
      filterCache = new LRUCache<>(cacheSize);
    }

    // Test to see if child Element is a query or filter that needs to be
    // cached
    QueryBuilder qb = queryFactory.getQueryBuilder(childElement.getNodeName());
    Object cacheKey = null;
    Query q = null;
    Filter f = null;
    if (qb != null) {
      q = qb.getQuery(childElement);
      cacheKey = q;
    } else {
      f = filterFactory.getFilter(childElement);
      cacheKey = f;
    }
    Query cachedFilter = filterCache.get(cacheKey);
    if (cachedFilter != null) {
      return new QueryWrapperFilter(cachedFilter); // cache hit
    }

    //cache miss
    if (qb != null) {
      cachedFilter = new QueryWrapperFilter(q);
    } else {
      cachedFilter = new CachingWrapperQuery(f);
    }

    filterCache.put(cacheKey, cachedFilter);
    return new QueryWrapperFilter(cachedFilter);
  }

  static class LRUCache<K, V> extends java.util.LinkedHashMap<K, V> {

    public LRUCache(int maxsize) {
      super(maxsize * 4 / 3 + 1, 0.75f, true);
      this.maxsize = maxsize;
    }

    protected int maxsize;

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
      return size() > maxsize;
    }

  }

}
