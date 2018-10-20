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
import java.util.List;
import java.util.Iterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.BooleanClause;

import java.io.IOException;

/**
 * Factory for disjunctions
 */
public class OrQuery extends ComposedQuery implements DistanceSubQuery { 
  public OrQuery(List<SrndQuery> queries, boolean infix, String opName) {
    super(queries, infix, opName);
  }
  
  @Override
  public Query makeLuceneQueryFieldNoBoost(String fieldName, BasicQueryFactory qf) {
    return SrndBooleanQuery.makeBooleanQuery(
      /* subqueries can be individually boosted */
      makeLuceneSubQueriesField(fieldName, qf), BooleanClause.Occur.SHOULD);
  }
  
  @Override
  public String distanceSubQueryNotAllowed() {
    Iterator<SrndQuery> sqi = getSubQueriesIterator();
    while (sqi.hasNext()) {
      SrndQuery leq = sqi.next();
      if (leq instanceof DistanceSubQuery) {
        String m = ((DistanceSubQuery)leq).distanceSubQueryNotAllowed();
        if (m != null) {
          return m;
        }
      } else {
        return "subquery not allowed: " + leq.toString();
      }
    }
    return null;
  }
    
  @Override
  public void addSpanQueries(SpanNearClauseFactory sncf) throws IOException {
    Iterator<SrndQuery> sqi = getSubQueriesIterator();
    while (sqi.hasNext()) {
      SrndQuery s = sqi.next();
      ((DistanceSubQuery) s).addSpanQueries(sncf);
    }
  }
}

