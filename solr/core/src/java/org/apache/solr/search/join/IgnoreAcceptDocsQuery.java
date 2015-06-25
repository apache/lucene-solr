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

package org.apache.solr.search.join;

import java.io.IOException;
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;

public class IgnoreAcceptDocsQuery extends Query {
  private final Query q;

  public IgnoreAcceptDocsQuery(Query q) {
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
  public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
    Weight inner = q.createWeight(searcher, needsScores);
    return new IADWeight(inner);
  }

  private class IADWeight extends Weight {
    Weight w;

    IADWeight(Weight delegate) {
      super(q);
      this.w = delegate;
    }

    @Override
    public void extractTerms(Set<Term> terms) {
      w.extractTerms(terms);
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      return w.explain(context, doc);
    }

    @Override
    public float getValueForNormalization() throws IOException {
      return w.getValueForNormalization();
    }

    @Override
    public void normalize(float norm, float topLevelBoost) {
      w.normalize(norm, topLevelBoost);
    }

    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
      return w.scorer(context);
    }

    @Override
    public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {
      final BulkScorer in = w.bulkScorer(context);
      return new BulkScorer() {

        @Override
        public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
          return in.score(collector, null, min, max);
        }

        @Override
        public long cost() {
          return in.cost();
        }
        
      };
    }
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    Query n = q.rewrite(reader);
    if (q == n) return this;
    return new IgnoreAcceptDocsQuery(n);
  }

  @Override
  public int hashCode() {
    return q.hashCode()*31;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof IgnoreAcceptDocsQuery)) return false;
    IgnoreAcceptDocsQuery other = (IgnoreAcceptDocsQuery)o;
    return q.equals(other.q);
  }

  @Override
  public String toString(String field) {
    return "IgnoreAcceptDocs(" + q + ")";
  }
}
