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
import java.util.Objects;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;

abstract class RewriteQuery<SQ extends SrndQuery> extends Query {
  protected final SQ srndQuery;
  protected final String fieldName;
  protected final BasicQueryFactory qf;

  RewriteQuery(SQ srndQuery, String fieldName, BasicQueryFactory qf) {
    this.srndQuery = Objects.requireNonNull(srndQuery);
    this.fieldName = Objects.requireNonNull(fieldName);
    this.qf = Objects.requireNonNull(qf);
  }

  @Override
  public abstract Query rewrite(IndexReader reader) throws IOException;

  @Override
  public String toString(String field) {
    return getClass().getName()
        + (field.isEmpty() ? "" : "(unused: " + field + ")")
        + "("
        + fieldName
        + ", "
        + srndQuery.toString()
        + ", "
        + qf.toString()
        + ")";
  }

  @Override
  public int hashCode() {
    return classHash() ^ fieldName.hashCode() ^ qf.hashCode() ^ srndQuery.hashCode();
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) && equalsTo(getClass().cast(other));
  }

  private boolean equalsTo(RewriteQuery<?> other) {
    return fieldName.equals(other.fieldName)
        && qf.equals(other.qf)
        && srndQuery.equals(other.srndQuery);
  }
}
