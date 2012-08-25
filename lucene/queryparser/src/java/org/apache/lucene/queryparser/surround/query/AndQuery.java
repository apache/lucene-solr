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
import org.apache.lucene.search.BooleanClause;

/**
 * Factory for conjunctions
 */
public class AndQuery extends ComposedQuery { 
  public AndQuery(List<SrndQuery> queries, boolean inf, String opName) { 
    super(queries, inf, opName);
  }
  
  @Override
  public Query makeLuceneQueryFieldNoBoost(String fieldName, BasicQueryFactory qf) {
    return SrndBooleanQuery.makeBooleanQuery( /* subqueries can be individually boosted */
      makeLuceneSubQueriesField(fieldName, qf), BooleanClause.Occur.MUST);
  }
}
