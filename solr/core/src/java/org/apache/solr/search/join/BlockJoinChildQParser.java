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

import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.ToChildBlockJoinQuery;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.SolrConstantScoreQuery;
import org.apache.solr.search.SyntaxError;

public class BlockJoinChildQParser extends BlockJoinParentQParser {

  public BlockJoinChildQParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    super(qstr, localParams, params, req);
  }

  @Override
  protected Query createQuery(Query parentListQuery, Query query, String scoreMode) {
    return new ToChildBlockJoinQuery(query, getFilter(parentListQuery).getFilter());
  }

  @Override
  protected String getParentFilterLocalParamName() {
    return "of";
  }
  
  @Override
  protected Query noClausesQuery() throws SyntaxError {
    final Query parents = parseParentFilter();
    final BooleanQuery notParents = new BooleanQuery.Builder()
        .add(new MatchAllDocsQuery(), Occur.MUST)
        .add(parents, Occur.MUST_NOT)
      .build();
    SolrConstantScoreQuery wrapped = new SolrConstantScoreQuery(getFilter(notParents));
    wrapped.setCache(false);
    return wrapped;
  }
}
