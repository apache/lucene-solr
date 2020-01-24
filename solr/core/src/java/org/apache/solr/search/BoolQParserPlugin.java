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
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.query.FilterQuery;
import org.apache.solr.request.SolrQueryRequest;

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
    return new QParser(qstr, localParams, params, req) {
      @Override
      public Query parse() throws SyntaxError {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        SolrParams solrParams = SolrParams.wrapDefaults(localParams, params);
        addQueries(builder, solrParams.getParams("must"), BooleanClause.Occur.MUST);
        addQueries(builder, solrParams.getParams("must_not"), BooleanClause.Occur.MUST_NOT);
        addQueries(builder, solrParams.getParams("filter"), BooleanClause.Occur.FILTER);
        addQueries(builder, solrParams.getParams("should"), BooleanClause.Occur.SHOULD);
        return builder.build();
      }

      private void addQueries(BooleanQuery.Builder builder, String[] subQueries, BooleanClause.Occur occur) throws SyntaxError {
        if (subQueries != null) {
          for (String subQuery : subQueries) {
            final QParser subParser = subQuery(subQuery, null);
            Query extQuery;
            if (BooleanClause.Occur.FILTER.equals(occur)) {
              extQuery = subParser.getQuery();
              if (!(extQuery instanceof ExtendedQuery) || (
                  ((ExtendedQuery) extQuery).getCache())) {
                  extQuery = new FilterQuery(extQuery);
              }
            } else {
              extQuery = subParser.parse();
            }
            builder.add(extQuery, occur);
          }
        }
      }
    };
  }
}
