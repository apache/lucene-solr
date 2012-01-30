package org.apache.lucene.search;

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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.ToStringUtils;
import org.apache.lucene.util.Bits;

import java.util.Set;
import java.io.IOException;

/**
 * A query that matches all documents.
 *
 */
public class MatchAllDocsQuery extends Query {

  private class MatchAllScorer extends Scorer {
    final float score;
    private int doc = -1;
    private final int maxDoc;
    private final Bits liveDocs;

    MatchAllScorer(IndexReader reader, Bits liveDocs, Weight w, float score) throws IOException {
      super(w);
      this.liveDocs = liveDocs;
      this.score = score;
      maxDoc = reader.maxDoc();
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() throws IOException {
      doc++;
      while(liveDocs != null && doc < maxDoc && !liveDocs.get(doc)) {
        doc++;
      }
      if (doc == maxDoc) {
        doc = NO_MORE_DOCS;
      }
      return doc;
    }
    
    @Override
    public float score() {
      return score;
    }

    @Override
    public int advance(int target) throws IOException {
      doc = target-1;
      return nextDoc();
    }
  }

  private class MatchAllDocsWeight extends Weight {
    private float queryWeight;
    private float queryNorm;

    public MatchAllDocsWeight(IndexSearcher searcher) {
    }

    @Override
    public String toString() {
      return "weight(" + MatchAllDocsQuery.this + ")";
    }

    @Override
    public Query getQuery() {
      return MatchAllDocsQuery.this;
    }

    @Override
    public float getValueForNormalization() {
      queryWeight = getBoost();
      return queryWeight * queryWeight;
    }

    @Override
    public void normalize(float queryNorm, float topLevelBoost) {
      this.queryNorm = queryNorm * topLevelBoost;
      queryWeight *= this.queryNorm;
    }

    @Override
    public Scorer scorer(AtomicReaderContext context, boolean scoreDocsInOrder,
        boolean topScorer, Bits acceptDocs) throws IOException {
      return new MatchAllScorer(context.reader(), acceptDocs, this, queryWeight);
    }

    @Override
    public Explanation explain(AtomicReaderContext context, int doc) {
      // explain query weight
      Explanation queryExpl = new ComplexExplanation
        (true, queryWeight, "MatchAllDocsQuery, product of:");
      if (getBoost() != 1.0f) {
        queryExpl.addDetail(new Explanation(getBoost(),"boost"));
      }
      queryExpl.addDetail(new Explanation(queryNorm,"queryNorm"));

      return queryExpl;
    }
  }

  @Override
  public Weight createWeight(IndexSearcher searcher) {
    return new MatchAllDocsWeight(searcher);
  }

  @Override
  public void extractTerms(Set<Term> terms) {
  }

  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    buffer.append("*:*");
    buffer.append(ToStringUtils.boost(getBoost()));
    return buffer.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof MatchAllDocsQuery))
      return false;
    MatchAllDocsQuery other = (MatchAllDocsQuery) o;
    return this.getBoost() == other.getBoost();
  }

  @Override
  public int hashCode() {
    return Float.floatToIntBits(getBoost()) ^ 0x1AA71190;
  }
}
