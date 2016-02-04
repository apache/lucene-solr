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
package org.apache.lucene.queryparser.surround.query;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.index.Term;

class SimpleTermRewriteQuery extends RewriteQuery<SimpleTerm> {

  SimpleTermRewriteQuery(
      SimpleTerm srndQuery,
      String fieldName,
      BasicQueryFactory qf) {
    super(srndQuery, fieldName, qf);
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    if (getBoost() != 1f) {
      return super.rewrite(reader);
    }
    final List<Query> luceneSubQueries = new ArrayList<>();
    srndQuery.visitMatchingTerms(reader, fieldName,
    new SimpleTerm.MatchingTermVisitor() {
      @Override
      public void visitMatchingTerm(Term term) throws IOException {
        luceneSubQueries.add(qf.newTermQuery(term));
      }
    });
    return  (luceneSubQueries.size() == 0) ? new MatchNoDocsQuery()
    : (luceneSubQueries.size() == 1) ? luceneSubQueries.get(0)
    : SrndBooleanQuery.makeBooleanQuery(
      /* luceneSubQueries all have default weight */
      luceneSubQueries, BooleanClause.Occur.SHOULD); /* OR the subquery terms */
  }
}

