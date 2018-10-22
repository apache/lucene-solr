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
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.Query;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;

import java.util.ArrayList;
import java.util.Collection;

/**
 * @see MaxScoreQParserPlugin
 */
public class MaxScoreQParser extends LuceneQParser {
  float tie = 0.0f;

  public MaxScoreQParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    super(qstr, localParams, params, req);
    if (getParam("tie") != null) {
      tie = Float.parseFloat(getParam("tie"));
    }
  }

  /**
   * Parses the query exactly like the Lucene parser does, but
   * delegates all SHOULD clauses to DisjunctionMaxQuery with
   * meaning only the clause with the max score will contribute
   * to the overall score, unless the tie parameter is specified.
   * <br>
   * The max() is only calculated from the SHOULD clauses.
   * Any MUST clauses will be passed through as separate
   * BooleanClauses and thus always contribute to the score.
   * @return the resulting Query
   * @throws org.apache.solr.search.SyntaxError if parsing fails
   */
  @Override
  public Query parse() throws SyntaxError {
    Query q = super.parse();
    float boost = 1f;
    if (q instanceof BoostQuery) {
      BoostQuery bq = (BoostQuery) q;
      boost = bq.getBoost();
      q = bq.getQuery();
    }
    if (q instanceof BooleanQuery == false) {
      if (boost != 1f) {
        q = new BoostQuery(q, boost);
      }
      return q;
    }
    BooleanQuery obq = (BooleanQuery)q;
    Collection<Query> should = new ArrayList<>();
    Collection<BooleanClause> prohibOrReq = new ArrayList<>();
    BooleanQuery.Builder newqb = new BooleanQuery.Builder();

    for (BooleanClause clause : obq) {
      if(clause.isProhibited() || clause.isRequired()) {
        prohibOrReq.add(clause);
      } else {
        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        bq.add(clause);
        should.add(bq.build());
      }
    }
    if (should.size() > 0) {
      DisjunctionMaxQuery dmq = new DisjunctionMaxQuery(should, tie);
      newqb.add(dmq, BooleanClause.Occur.SHOULD);
    }
    for(BooleanClause c : prohibOrReq) {
      newqb.add(c);
    }
    Query newq = newqb.build();
    if (boost != 1f) {
      newq = new BoostQuery(newq, boost);
    }
    return newq;
  }
}