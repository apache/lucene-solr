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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReader.AtomicReaderContext;
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

  public MatchAllDocsQuery() {
    this(null);
  }

  private final String normsField;

  /**
   * @param normsField Field used for normalization factor (document boost). Null if nothing.
   */
  public MatchAllDocsQuery(String normsField) {
    this.normsField = normsField;
  }

  private class MatchAllScorer extends Scorer {
    final float score;
    final byte[] norms;
    private int doc = -1;
    private final int maxDoc;
    private final Bits liveDocs;
    private final Similarity similarity;
    
    MatchAllScorer(IndexReader reader, Similarity similarity, Weight w,
        byte[] norms) throws IOException {
      super(w);
      this.similarity = similarity;
      liveDocs = reader.getLiveDocs();
      score = w.getValue();
      maxDoc = reader.maxDoc();
      this.norms = norms;
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
      return norms == null ? score : score * similarity.decodeNormValue(norms[docID()]);
    }

    @Override
    public int advance(int target) throws IOException {
      doc = target-1;
      return nextDoc();
    }
  }

  private class MatchAllDocsWeight extends Weight {
    private Similarity similarity;
    private float queryWeight;
    private float queryNorm;

    public MatchAllDocsWeight(IndexSearcher searcher) {
      this.similarity = normsField == null ? null : searcher.getSimilarityProvider().get(normsField);
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
    public float getValue() {
      return queryWeight;
    }

    @Override
    public float sumOfSquaredWeights() {
      queryWeight = getBoost();
      return queryWeight * queryWeight;
    }

    @Override
    public void normalize(float queryNorm) {
      this.queryNorm = queryNorm;
      queryWeight *= this.queryNorm;
    }

    @Override
    public Scorer scorer(AtomicReaderContext context, ScorerContext scorerContext) throws IOException {
      return new MatchAllScorer(context.reader, similarity, this,
          normsField != null ? context.reader.norms(normsField) : null);
    }

    @Override
    public Explanation explain(AtomicReaderContext context, int doc) {
      // explain query weight
      Explanation queryExpl = new ComplexExplanation
        (true, getValue(), "MatchAllDocsQuery, product of:");
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
