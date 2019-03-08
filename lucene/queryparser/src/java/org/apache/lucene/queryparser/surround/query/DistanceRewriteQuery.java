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
import java.util.function.Predicate;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;

class DistanceRewriteQuery extends RewriteQuery<DistanceQuery> {

  DistanceRewriteQuery(
      DistanceQuery srndQuery,
      String fieldName,
      BasicQueryFactory qf) {
    super(srndQuery, fieldName, qf);
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    return srndQuery.getSpanNearQuery(reader, fieldName, qf);
  }

  @Override
  public void visit(QueryVisitor visitor, Predicate<String> fieldSelector) {
    // TODO implement this
    visitor.visitLeaf(this);
  }
}

