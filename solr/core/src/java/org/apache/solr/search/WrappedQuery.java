/**
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

package org.apache.solr.search;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.Set;

/** A simple query that wraps another query and implements ExtendedQuery. */
public class WrappedQuery extends ExtendedQueryBase {
  private Query q;

  public WrappedQuery(Query q) {
    this.q = q;
  }

  public Query getWrappedQuery() {
    return q;
  }

  public void setWrappedQuery(Query q) {
    this.q = q;
  }

  @Override
  public void setBoost(float b) {
    q.setBoost(b);
  }

  @Override
  public float getBoost() {
    return q.getBoost();
  }

  @Override
  public Weight createWeight(IndexSearcher searcher) throws IOException {
    return q.createWeight(searcher);
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    // currently no need to continue wrapping at this point.
    return q.rewrite(reader);
  }

  @Override
  public void extractTerms(Set<Term> terms) {
    q.extractTerms(terms);
  }

  @Override
  public Object clone() {
    WrappedQuery newQ = (WrappedQuery)super.clone();
    newQ.q = (Query) q.clone();
    return newQ;
  }

  @Override
  public int hashCode() {
    return q.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof WrappedQuery) {
      return this.q.equals(((WrappedQuery)obj).q);
    }
    return q.equals(obj);
  }

  @Override
  public String toString(String field) {
    return getOptions() + q.toString();
  }
}

