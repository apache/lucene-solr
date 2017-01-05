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

import org.apache.lucene.queries.function.FunctionQuery;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.QueryValueSource;
import org.apache.lucene.search.*;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.function.*;

/**
 * Create a range query over a function.
 * <br>Other parameters:
 * <br><code>l</code>, the lower bound, optional)
 * <br><code>u</code>, the upper bound, optional)
 * <br><code>incl</code>, include the lower bound: true/false, optional, default=true
 * <br><code>incu</code>, include the upper bound: true/false, optional, default=true
 * <br>Example: <code>{!frange l=1000 u=50000}myfield</code>
 * <br>Filter query example: <code>fq={!frange l=0 u=2.2}sum(user_ranking,editor_ranking)</code> 
 */
public class FunctionRangeQParserPlugin extends QParserPlugin {
  public static final String NAME = "frange";

  @Override
  public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new QParser(qstr, localParams, params, req) {
      ValueSource vs;
      String funcStr;

      @Override
      public Query parse() throws SyntaxError {
        funcStr = localParams.get(QueryParsing.V, null);
        QParser subParser = subQuery(funcStr, FunctionQParserPlugin.NAME);
        subParser.setIsFilter(false);  // the range can be based on the relevancy score of embedded queries.
        Query funcQ = subParser.getQuery();
        if (funcQ instanceof FunctionQuery) {
          vs = ((FunctionQuery)funcQ).getValueSource();
        } else {
          vs = new QueryValueSource(funcQ, 0.0f);
        }

        String l = localParams.get("l");
        String u = localParams.get("u");
        boolean includeLower = localParams.getBool("incl",true);
        boolean includeUpper = localParams.getBool("incu",true);

        // TODO: add a score=val option to allow score to be the value
        ValueSourceRangeFilter rf = new ValueSourceRangeFilter(vs, l, u, includeLower, includeUpper);
        FunctionRangeQuery frq = new FunctionRangeQuery(rf);
        return frq;
      }
    };
  }

}

