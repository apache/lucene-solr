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
package org.apache.solr.analytics.facet;

import java.util.Map;
import java.util.function.Consumer;

import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.solr.analytics.function.ReductionCollectionManager.ReductionDataCollection;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.Filter;
import org.apache.solr.search.QParser;

/**
 * A facet that breaks down the data by additional Solr Queries.
 */
public class QueryFacet extends AbstractSolrQueryFacet {
  private final Map<String,String> queries;

  public QueryFacet(String name, Map<String, String> queries) {
    super(name);
    this.queries = queries;
  }

  @Override
  public void createFacetValueExecuters(final Filter filter, SolrQueryRequest queryRequest, Consumer<FacetValueQueryExecuter> consumer) {
    queries.forEach( (queryName, query) -> {
      final Query q;
      try {
        q = QParser.getParser(query, queryRequest).getQuery();
      } catch( Exception e ){
        throw new SolrException(ErrorCode.BAD_REQUEST,"Invalid query '"+query+"' in query facet '" + getName() + "'",e);
      }
      // The searcher sends docIds to the QueryFacetAccumulator which forwards
      // them to <code>collectQuery()</code> in this class for collection.
      Query queryQuery = new BooleanQuery.Builder()
          .add(q, Occur.MUST)
          .add(filter, Occur.FILTER)
          .build();

      ReductionDataCollection dataCol = collectionManager.newDataCollection();
      reductionData.put(queryName, dataCol);
      consumer.accept(new FacetValueQueryExecuter(dataCol, queryQuery));
    });
  }
}