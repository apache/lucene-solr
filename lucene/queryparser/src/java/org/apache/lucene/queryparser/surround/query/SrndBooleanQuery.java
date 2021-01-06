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
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;

class SrndBooleanQuery {
  public static void addQueriesToBoolean(
      BooleanQuery.Builder bq, List<Query> queries, BooleanClause.Occur occur) {
    for (int i = 0; i < queries.size(); i++) {
      bq.add(queries.get(i), occur);
    }
  }

  public static Query makeBooleanQuery(List<Query> queries, BooleanClause.Occur occur) {
    if (queries.size() <= 1) {
      throw new AssertionError("Too few subqueries: " + queries.size());
    }
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    addQueriesToBoolean(bq, queries.subList(0, queries.size()), occur);
    return bq.build();
  }
}
