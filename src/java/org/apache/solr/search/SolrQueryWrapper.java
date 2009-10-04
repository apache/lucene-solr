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

import org.apache.lucene.search.Query;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.index.IndexReader;

import java.io.IOException;
import java.util.Set;

public class SolrQueryWrapper extends Query {
  private final Query q;
  public SolrQueryWrapper(Query q) {
    this.q = q;
  }

  public Query getWrappedQuery() {
    return q;
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
  public String toString() {
    return q.toString();
  }

  @Override
  public Weight createWeight(Searcher searcher) throws IOException {
    return q.createWeight(searcher);
  }

  @Override
  public Weight weight(Searcher searcher) throws IOException {
    return q.weight(searcher);
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    return q.rewrite(reader);
  }

  @Override
  public Query combine(Query[] queries) {
    return q.combine(queries);
  }

  @Override
  public void extractTerms(Set terms) {
    q.extractTerms(terms);
  }

  @Override
  public Similarity getSimilarity(Searcher searcher) {
    return q.getSimilarity(searcher);
  }

  @Override
  public Object clone() {
    return new SolrQueryWrapper((Query)q.clone());
  }

  @Override
  public int hashCode() {
    return q.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    Query other;
    if (obj instanceof SolrQueryWrapper) {
      other = ((SolrQueryWrapper)obj).q;
    } else if (obj instanceof Query) {
      other = (Query)obj;
    } else {
      return false;
    }

    return q.equals(other);
  }

  @Override
  public String toString(String field) {
    return q.toString();
  }
}
