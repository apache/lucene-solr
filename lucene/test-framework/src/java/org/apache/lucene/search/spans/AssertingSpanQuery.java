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
package org.apache.lucene.search.spans;

import java.io.IOException;
import java.util.Objects;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;

/** Wraps a span query with asserts */
public class AssertingSpanQuery extends SpanQuery {
  private final SpanQuery in;
  
  public AssertingSpanQuery(SpanQuery in) {
    this.in = in;
  }

  @Override
  public String getField() {
    return in.getField();
  }

  @Override
  public String toString(String field) {
    return "AssertingSpanQuery(" + in.toString(field) + ")";
  }

  @Override
  public SpanWeight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
    SpanWeight weight = in.createWeight(searcher, scoreMode, boost);
    return new AssertingSpanWeight(searcher, weight);
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    Query q = in.rewrite(reader);
    if (q == in) {
      return super.rewrite(reader);
    } else if (q instanceof SpanQuery) {
      return new AssertingSpanQuery((SpanQuery) q);
    } else {
      return q;
    }
  }

  @Override
  public void visit(QueryVisitor visitor) {
    in.visit(visitor);
  }

  @Override
  public Query clone() {
    return new AssertingSpanQuery(in);
  }

  @Override
  public boolean equals(Object o) {
    return sameClassAs(o) &&
           equalsTo(getClass().cast(o));
  }

  private boolean equalsTo(AssertingSpanQuery other) {
    return Objects.equals(in, other.in);
  }

  @Override
  public int hashCode() {
    return (in == null) ? 0 : in.hashCode();
  }
}
