package org.apache.lucene.search;

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
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.ToStringUtils;

/**
 * A query that wraps another query and simply returns a constant score equal to the
 * query boost for every document that matches the query.
 * It therefore simply strips of all scores and returns a constant one.
 */
public class ConstantScoreQuery extends Query {
  protected final Query query;

  /** Strips off scores from the passed in Query. The hits will get a constant score
   * dependent on the boost factor of this query. */
  public ConstantScoreQuery(Query query) {
    this.query = Objects.requireNonNull(query, "Query must not be null");
  }

  /** Returns the encapsulated query. */
  public Query getQuery() {
    return query;
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    Query rewritten = query.rewrite(reader);

    if (rewritten.getClass() == getClass()) {
      if (getBoost() != rewritten.getBoost()) {
        rewritten = rewritten.clone();
        rewritten.setBoost(getBoost());
      }
      return rewritten;
    }

    if (rewritten != query) {
      rewritten = new ConstantScoreQuery(rewritten);
      rewritten.setBoost(this.getBoost());
      return rewritten;
    }

    return this;
  }

  protected class ConstantWeight extends Weight {
    private final Weight innerWeight;
    private float queryNorm;
    private float queryWeight;
    
    public ConstantWeight(Weight innerWeight) throws IOException {
      super(ConstantScoreQuery.this);
      this.innerWeight = innerWeight;
    }

    @Override
    public void extractTerms(Set<Term> terms) {
      // no-op
    }

    @Override
    public float getValueForNormalization() throws IOException {
      // we calculate sumOfSquaredWeights of the inner weight, but ignore it (just to initialize everything)
      innerWeight.getValueForNormalization();
      queryWeight = getBoost();
      return queryWeight * queryWeight;
    }

    @Override
    public void normalize(float norm, float topLevelBoost) {
      this.queryNorm = norm * topLevelBoost;
      queryWeight *= this.queryNorm;
      // we normalize the inner weight, but ignore it (just to initialize everything)
      innerWeight.normalize(norm, topLevelBoost);
    }

    @Override
    public BulkScorer bulkScorer(LeafReaderContext context, Bits acceptDocs) throws IOException {
      BulkScorer bulkScorer = innerWeight.bulkScorer(context, acceptDocs);
      if (bulkScorer == null) {
        return null;
      }
      return new ConstantBulkScorer(bulkScorer, this, queryWeight);
    }

    @Override
    public Scorer scorer(LeafReaderContext context, Bits acceptDocs) throws IOException {
      Scorer scorer = innerWeight.scorer(context, acceptDocs);
      if (scorer == null) {
        return null;
      }
      return new ConstantScoreScorer(scorer, queryWeight);
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      final Scorer cs = scorer(context, context.reader().getLiveDocs());
      final boolean exists = (cs != null && cs.advance(doc) == doc);

      if (exists) {
        return Explanation.match(
            queryWeight, ConstantScoreQuery.this.toString() + ", product of:",
            Explanation.match(getBoost(), "boost"), Explanation.match(queryNorm, "queryNorm"));
      } else {
        return Explanation.noMatch(ConstantScoreQuery.this.toString() + " doesn't match id " + doc);
      }
    }
  }

  /** We return this as our {@link BulkScorer} so that if the CSQ
   *  wraps a query with its own optimized top-level
   *  scorer (e.g. BooleanScorer) we can use that
   *  top-level scorer. */
  protected class ConstantBulkScorer extends BulkScorer {
    final BulkScorer bulkScorer;
    final Weight weight;
    final float theScore;

    public ConstantBulkScorer(BulkScorer bulkScorer, Weight weight, float theScore) {
      this.bulkScorer = bulkScorer;
      this.weight = weight;
      this.theScore = theScore;
    }

    @Override
    public int score(LeafCollector collector, int min, int max) throws IOException {
      return bulkScorer.score(wrapCollector(collector), min, max);
    }

    private LeafCollector wrapCollector(LeafCollector collector) {
      return new FilterLeafCollector(collector) {
        @Override
        public void setScorer(Scorer scorer) throws IOException {
          // we must wrap again here, but using the scorer passed in as parameter:
          in.setScorer(new ConstantScoreScorer(scorer, theScore));
        }
      };
    }

    @Override
    public long cost() {
      return bulkScorer.cost();
    }
  }

  protected class ConstantScoreScorer extends FilterScorer {

    private final float score;

    public ConstantScoreScorer(Scorer wrapped, float score) {
      super(wrapped);
      this.score = score;
    }

    @Override
    public int freq() throws IOException {
      return 1;
    }

    @Override
    public float score() throws IOException {
      return score;
    }

    @Override
    public Collection<ChildScorer> getChildren() {
      return Collections.singletonList(new ChildScorer(in, "constant"));
    }
  }

  protected class ConstantDocIdSetIteratorScorer extends Scorer {
    final DocIdSetIterator docIdSetIterator;
    final float theScore;

    public ConstantDocIdSetIteratorScorer(DocIdSetIterator docIdSetIterator, Weight w, float theScore) {
      super(w);
      this.theScore = theScore;
      this.docIdSetIterator = docIdSetIterator;
    }

    @Override
    public int nextDoc() throws IOException {
      return docIdSetIterator.nextDoc();
    }
    
    @Override
    public int docID() {
      return docIdSetIterator.docID();
    }

    @Override
    public float score() throws IOException {
      assert docIdSetIterator.docID() != NO_MORE_DOCS;
      return theScore;
    }

    @Override
    public int freq() throws IOException {
      return 1;
    }

    @Override
    public int advance(int target) throws IOException {
      return docIdSetIterator.advance(target);
    }

    @Override
    public long cost() {
      return docIdSetIterator.cost();
    }

    @Override
    public Collection<ChildScorer> getChildren() {
      if (query != null) {
        return Collections.singletonList(new ChildScorer((Scorer) docIdSetIterator, "constant"));
      } else {
        return Collections.emptyList();
      }
    }
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
    final Weight innerWeight = searcher.createWeight(query, false);
    if (needsScores) {
      return new ConstantScoreQuery.ConstantWeight(innerWeight);
    } else {
      return innerWeight;
    }
  }

  @Override
  public String toString(String field) {
    return new StringBuilder("ConstantScore(")
      .append(query.toString(field))
      .append(')')
      .append(ToStringUtils.boost(getBoost()))
      .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!super.equals(o))
      return false;
    if (o instanceof ConstantScoreQuery) {
      final ConstantScoreQuery other = (ConstantScoreQuery) o;
      return this.query.equals(other.query);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return 31 * super.hashCode() + query.hashCode();
  }

}
