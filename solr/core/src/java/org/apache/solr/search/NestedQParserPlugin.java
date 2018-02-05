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
package org.apache.solr.search;

import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;

/**
 * Create a nested query, with the ability of that query to redefine its type via
 * local parameters.  This is useful in specifying defaults in configuration and
 * letting clients indirectly reference them.
 * <br>Example: <code>{!query defType=func v=$q1}</code>
 * <br> if the q1 parameter is <code>price</code> then the query would be a function query on the price field.
 * <br> if the q1 parameter is <code>{!lucene}inStock:true</code> then a term query is
 *     created from the lucene syntax string that matches documents with inStock=true.
 */
public class NestedQParserPlugin extends QParserPlugin {
  public static final String NAME = "query";

  @Override
  public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    if (localParams == null) { // avoid an NPE later
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "the 'query' QParser must be invoked with local-params, e.g. {!query defType=...}");
    }
    return new QParser(qstr, localParams, params, req) {
      QParser baseParser;
      ValueSource vs;
      String b;

      @Override
      public Query parse() throws SyntaxError {
        baseParser = subQuery(localParams.get(QueryParsing.V), null);
        return baseParser.getQuery();
      }

      @Override
      public String[] getDefaultHighlightFields() {
        return baseParser.getDefaultHighlightFields();
      }

      @Override
      public Query getHighlightQuery() throws SyntaxError {
        return baseParser.getHighlightQuery();
      }

      @Override
      public void addDebugInfo(NamedList<Object> debugInfo) {
        // encapsulate base debug info in a sub-list?
        baseParser.addDebugInfo(debugInfo);
      }
    };
  }

}
