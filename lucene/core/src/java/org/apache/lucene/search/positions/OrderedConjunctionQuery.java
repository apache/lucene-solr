package org.apache.lucene.search.positions;

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

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;

public class OrderedConjunctionQuery extends PositionFilterQuery {

  public OrderedConjunctionQuery(int slop, Query... queries) {
    super(buildBooleanQuery(queries), new WithinOrderedFilter(slop + queries.length - 1));
  }

  private static BooleanQuery buildBooleanQuery(Query... queries) {
    BooleanQuery bq = new BooleanQuery();
    for (Query q : queries) {
      bq.add(q, BooleanClause.Occur.MUST);
    }
    return bq;
  }

}
