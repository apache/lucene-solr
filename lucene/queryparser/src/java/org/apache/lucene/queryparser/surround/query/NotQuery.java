package org.apache.lucene.queryparser.surround.query;
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

import java.util.List;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BooleanClause;

/**
 * Factory for prohibited clauses
 */
public class NotQuery extends ComposedQuery { 
  public NotQuery(List<SrndQuery> queries, String opName) { super(queries, true /* infix */, opName); }
  
  @Override
  public Query makeLuceneQueryFieldNoBoost(String fieldName, BasicQueryFactory qf) {
    List<Query> luceneSubQueries = makeLuceneSubQueriesField(fieldName, qf);
    BooleanQuery bq = new BooleanQuery();
    bq.add( luceneSubQueries.get(0), BooleanClause.Occur.MUST);
    SrndBooleanQuery.addQueriesToBoolean(bq,
            // FIXME: do not allow weights on prohibited subqueries.
            luceneSubQueries.subList(1, luceneSubQueries.size()),
            // later subqueries: not required, prohibited
            BooleanClause.Occur.MUST_NOT);
    return bq;
  }
}
