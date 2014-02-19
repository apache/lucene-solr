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

package org.apache.solr.search.join;

import org.apache.lucene.search.CachingWrapperFilter;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.join.FixedBitSetCachingWrapperFilter;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.search.join.ToParentBlockJoinQuery;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.SolrCache;
import org.apache.solr.search.SolrConstantScoreQuery;
import org.apache.solr.search.SyntaxError;

class BlockJoinParentQParser extends QParser {
  /** implementation detail subject to change */
  public String CACHE_NAME="perSegFilter";

  protected String getParentFilterLocalParamName() {
    return "which";
  }

  BlockJoinParentQParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    super(qstr, localParams, params, req);
  }

  @Override
  public Query parse() throws SyntaxError {
    String filter = localParams.get(getParentFilterLocalParamName());
    QParser parentParser = subQuery(filter, null);
    Query parentQ = parentParser.getQuery();

    String queryText = localParams.get(QueryParsing.V);
    // there is no child query, return parent filter from cache
    if (queryText == null || queryText.length()==0) {
                  SolrConstantScoreQuery wrapped = new SolrConstantScoreQuery(getFilter(parentQ));
                  wrapped.setCache(false);
                  return wrapped;
    }
    QParser childrenParser = subQuery(queryText, null);
    Query childrenQuery = childrenParser.getQuery();
    return createQuery(parentQ, childrenQuery);
  }

  protected Query createQuery(Query parentList, Query query) {
    return new ToParentBlockJoinQuery(query, getFilter(parentList), ScoreMode.None);
  }

  protected Filter getFilter(Query parentList) {
    SolrCache parentCache = req.getSearcher().getCache(CACHE_NAME);
    // lazily retrieve from solr cache
    Filter filter = null;
    if (parentCache != null) {
      filter = (Filter) parentCache.get(parentList);
    }
    Filter result;
    if (filter == null) {
      result = createParentFilter(parentList);
      if (parentCache != null) {
        parentCache.put(parentList, result);
      }
    } else {
      result = filter;
    }
    return result;
  }

  protected Filter createParentFilter(Query parentQ) {
    return new FixedBitSetCachingWrapperFilter(new QueryWrapperFilter(parentQ));
  }
}






