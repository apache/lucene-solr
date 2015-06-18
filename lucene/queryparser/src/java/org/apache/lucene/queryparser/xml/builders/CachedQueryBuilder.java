/*
 * Created on 25-Jan-2006
 */
package org.apache.lucene.queryparser.xml.builders;

import java.util.Map;

import org.apache.lucene.queryparser.xml.DOMUtils;
import org.apache.lucene.queryparser.xml.ParserException;
import org.apache.lucene.queryparser.xml.QueryBuilder;
import org.apache.lucene.queryparser.xml.QueryBuilderFactory;
import org.apache.lucene.search.CachingWrapperQuery;
import org.apache.lucene.search.Query;
import org.w3c.dom.Element;
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
 */
public class CachedQueryBuilder implements QueryBuilder {

  private final QueryBuilderFactory queryFactory;

  private LRUCache<Object, Query> queryCache;

  private final int cacheSize;

  public CachedQueryBuilder(QueryBuilderFactory queryFactory,
                             int cacheSize) {
    this.queryFactory = queryFactory;
    this.cacheSize = cacheSize;
  }

  @Override
  public synchronized Query getQuery(Element e) throws ParserException {
    Element childElement = DOMUtils.getFirstChildOrFail(e);

    if (queryCache == null) {
      queryCache = new LRUCache<>(cacheSize);
    }

    // Test to see if child Element is a query or filter that needs to be
    // cached
    QueryBuilder qb = queryFactory.getQueryBuilder(childElement.getNodeName());
    Object cacheKey = null;
    Query q = qb.getQuery(childElement);
    cacheKey = q;
    Query cachedQuery = queryCache.get(cacheKey);
    if (cachedQuery != null) {
      return cachedQuery; // cache hit
    }

    //cache miss
    cachedQuery = new CachingWrapperQuery(q);

    queryCache.put(cacheKey, cachedQuery);
    return cachedQuery;
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
