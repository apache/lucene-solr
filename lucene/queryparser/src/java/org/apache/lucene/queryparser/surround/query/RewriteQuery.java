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

import org.apache.lucene.search.Query;

abstract class RewriteQuery<SQ extends SrndQuery> extends Query {
  protected final SQ srndQuery;
  protected final String fieldName;
  protected final BasicQueryFactory qf;

  RewriteQuery(
      SQ srndQuery,
      String fieldName,
      BasicQueryFactory qf) {
    this.srndQuery = srndQuery;
    this.fieldName = fieldName;
    this.qf = qf;
  }

  @Override
  public String toString(String field) {
    return getClass().getName()
    + (field.isEmpty() ? "" : "(unused: " + field + ")")
    + "(" + fieldName
    + ", " + srndQuery.toString()
    + ", " + qf.toString()
    + ")";
  }

  @Override
  public int hashCode() {
    return super.hashCode()
    ^ fieldName.hashCode()
    ^ qf.hashCode()
    ^ srndQuery.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null)
      return false;
    if (! getClass().equals(obj.getClass()))
      return false;
    @SuppressWarnings("unchecked") RewriteQuery<SQ> other = (RewriteQuery<SQ>)obj;
    return super.equals(obj)
      && fieldName.equals(other.fieldName)
      && qf.equals(other.qf)
      && srndQuery.equals(other.srndQuery);
  }

}

