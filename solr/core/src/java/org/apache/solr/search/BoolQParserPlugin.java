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

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Query;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.query.FilterQuery;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.join.FiltersQParser;

import java.util.IdentityHashMap;
import java.util.Map;

/**
 * Create a boolean query from sub queries.
 * Sub queries can be marked as must, must_not, filter or should
 *
 * <p>Example: <code>{!bool should=title:lucene should=title:solr must_not=id:1}</code>
 */
public class BoolQParserPlugin extends QParserPlugin {
  public static final String NAME = "bool";

  @Override
  public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new FiltersQParser(qstr, localParams, params, req) {
      @Override
      public Query parse() throws SyntaxError {
        return parseImpl();
      }

      @Override
      protected Query unwrapQuery(Query query, BooleanClause.Occur occur) {
        if (occur== BooleanClause.Occur.FILTER) {
          if (!(query instanceof ExtendedQuery) || (
                  ((ExtendedQuery) query).getCache())) {
            return new FilterQuery(query);
          }
        } else {
          if (query instanceof WrappedQuery) {
            return ((WrappedQuery)query).getWrappedQuery();
          }
        }
        return query;
      }

      @Override
      protected Map<QParser, BooleanClause.Occur> clauses() throws SyntaxError {
        Map<QParser, BooleanClause.Occur> clauses = new IdentityHashMap<>();
        SolrParams solrParams = SolrParams.wrapDefaults(localParams, params);
        addQueries(clauses, solrParams.getParams("must"), BooleanClause.Occur.MUST);
        addQueries(clauses, solrParams.getParams("must_not"), BooleanClause.Occur.MUST_NOT);
        addQueries(clauses, solrParams.getParams("filter"), BooleanClause.Occur.FILTER);
        addQueries(clauses, solrParams.getParams("should"), BooleanClause.Occur.SHOULD);
        return clauses;
      }

      private void addQueries(Map<QParser, BooleanClause.Occur> clausesDest, String[] subQueries, BooleanClause.Occur occur) throws SyntaxError {
        if (subQueries != null) {
          for (String subQuery : subQueries) {
            final QParser subParser = subQuery(subQuery, null);
            clausesDest.put(subParser, occur);
          }
        }
      }
    };
  }
}
