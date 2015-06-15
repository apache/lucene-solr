package org.apache.lucene.search.spans;

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

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;

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
  public SpanWeight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
    SpanWeight weight = in.createWeight(searcher, needsScores);
    return new AssertingSpanWeight(searcher, weight);
  }

  @Override
  public void setBoost(float b) {
    in.setBoost(b);
  }

  @Override
  public float getBoost() {
    return in.getBoost();
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    Query q = in.rewrite(reader);
    if (q == in) {
      return this;
    } else if (q instanceof SpanQuery) {
      return new AssertingSpanQuery((SpanQuery) q);
    } else {
      return q;
    }
  }

  @Override
  public Query clone() {
    return new AssertingSpanQuery((SpanQuery) in.clone());
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((in == null) ? 0 : in.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!super.equals(obj)) return false;
    if (getClass() != obj.getClass()) return false;
    AssertingSpanQuery other = (AssertingSpanQuery) obj;
    if (in == null) {
      if (other.in != null) return false;
    } else if (!in.equals(other.in)) return false;
    return true;
  }
}
