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
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.ToStringUtils;

import java.io.IOException;
import java.util.Set;

/**
 * A query that wraps another query or a filter and simply returns a constant score equal to the
 * query boost for every document that matches the filter or query.
 * For queries it therefore simply strips of all scores and returns a constant one.
 */
public class ConstantScoreQuery extends Query {
  protected final Filter filter;
  protected final Query query;

  /** Strips off scores from the passed in Query. The hits will get a constant score
   * dependent on the boost factor of this query. */
  public ConstantScoreQuery(Query query) {
    if (query == null)
      throw new NullPointerException("Query may not be null");
    this.filter = null;
    this.query = query;
  }

  /** Wraps a Filter as a Query. The hits will get a constant score
   * dependent on the boost factor of this query.
   * If you simply want to strip off scores from a Query, no longer use
   * {@code new ConstantScoreQuery(new QueryWrapperFilter(query))}, instead
   * use {@link #ConstantScoreQuery(Query)}!
   */
  public ConstantScoreQuery(Filter filter) {
    if (filter == null)
      throw new NullPointerException("Filter may not be null");
    this.filter = filter;
    this.query = null;
  }

  /** Returns the encapsulated filter, returns {@code null} if a query is wrapped. */
  public Filter getFilter() {
    return filter;
  }

  /** Returns the encapsulated query, returns {@code null} if a filter is wrapped. */
  public Query getQuery() {
    return query;
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    if (query != null) {
      Query rewritten = query.rewrite(reader);
      if (rewritten != query) {
        rewritten = new ConstantScoreQuery(rewritten);
        rewritten.setBoost(this.getBoost());
        return rewritten;
      }
    }
    return this;
  }

  @Override
  public void extractTerms(Set<Term> terms) {
    // TODO: OK to not add any terms when wrapped a filter
    // and used with MultiSearcher, but may not be OK for
    // highlighting.
    // If a query was wrapped, we delegate to query.
    if (query != null)
      query.extractTerms(terms);
  }

  protected class ConstantWeight extends Weight {
    private final Weight innerWeight;
    private float queryNorm;
    private float queryWeight;
    
    public ConstantWeight(IndexSearcher searcher) throws IOException {
      this.innerWeight = (query == null) ? null : query.createWeight(searcher);
    }

    @Override
    public Query getQuery() {
      return ConstantScoreQuery.this;
    }

    @Override
    public float getValueForNormalization() throws IOException {
      // we calculate sumOfSquaredWeights of the inner weight, but ignore it (just to initialize everything)
      if (innerWeight != null) innerWeight.getValueForNormalization();
      queryWeight = getBoost();
      return queryWeight * queryWeight;
    }

    @Override
    public void normalize(float norm, float topLevelBoost) {
      this.queryNorm = norm * topLevelBoost;
      queryWeight *= this.queryNorm;
      // we normalize the inner weight, but ignore it (just to initialize everything)
      if (innerWeight != null) innerWeight.normalize(norm, topLevelBoost);
    }

    @Override
    public Scorer scorer(AtomicReaderContext context, boolean scoreDocsInOrder,
        boolean topScorer, final Bits acceptDocs) throws IOException {
      final DocIdSetIterator disi;
      if (filter != null) {
        assert query == null;
        final DocIdSet dis = filter.getDocIdSet(context, acceptDocs);
        if (dis == null) {
          return null;
        }
        disi = dis.iterator();
      } else {
        assert query != null && innerWeight != null;
        disi = innerWeight.scorer(context, scoreDocsInOrder, topScorer, acceptDocs);
      }

      if (disi == null) {
        return null;
      }
      return new ConstantScorer(disi, this, queryWeight);
    }
    
    @Override
    public boolean scoresDocsOutOfOrder() {
      return (innerWeight != null) ? innerWeight.scoresDocsOutOfOrder() : false;
    }

    @Override
    public Explanation explain(AtomicReaderContext context, int doc) throws IOException {
      final Scorer cs = scorer(context, true, false, context.reader().getLiveDocs());
      final boolean exists = (cs != null && cs.advance(doc) == doc);

      final ComplexExplanation result = new ComplexExplanation();
      if (exists) {
        result.setDescription(ConstantScoreQuery.this.toString() + ", product of:");
        result.setValue(queryWeight);
        result.setMatch(Boolean.TRUE);
        result.addDetail(new Explanation(getBoost(), "boost"));
        result.addDetail(new Explanation(queryNorm, "queryNorm"));
      } else {
        result.setDescription(ConstantScoreQuery.this.toString() + " doesn't match id " + doc);
        result.setValue(0);
        result.setMatch(Boolean.FALSE);
      }
      return result;
    }
  }

  protected class ConstantScorer extends Scorer {
    final DocIdSetIterator docIdSetIterator;
    final float theScore;

    public ConstantScorer(DocIdSetIterator docIdSetIterator, Weight w, float theScore) throws IOException {
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
      return theScore;
    }

    @Override
    public int advance(int target) throws IOException {
      return docIdSetIterator.advance(target);
    }
    
    private Collector wrapCollector(final Collector collector) {
      return new Collector() {
        @Override
        public void setScorer(Scorer scorer) throws IOException {
          // we must wrap again here, but using the scorer passed in as parameter:
          collector.setScorer(new ConstantScorer(scorer, ConstantScorer.this.weight, ConstantScorer.this.theScore));
        }
        
        @Override
        public void collect(int doc) throws IOException {
          collector.collect(doc);
        }
        
        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
          collector.setNextReader(context);
        }
        
        @Override
        public boolean acceptsDocsOutOfOrder() {
          return collector.acceptsDocsOutOfOrder();
        }
      };
    }

    // this optimization allows out of order scoring as top scorer!
    @Override
    public void score(Collector collector) throws IOException {
      if (docIdSetIterator instanceof Scorer) {
        ((Scorer) docIdSetIterator).score(wrapCollector(collector));
      } else {
        super.score(collector);
      }
    }

    // this optimization allows out of order scoring as top scorer,
    @Override
    public boolean score(Collector collector, int max, int firstDocID) throws IOException {
      if (docIdSetIterator instanceof Scorer) {
        return ((Scorer) docIdSetIterator).score(wrapCollector(collector), max, firstDocID);
      } else {
        return super.score(collector, max, firstDocID);
      }
    }
  }

  @Override
  public Weight createWeight(IndexSearcher searcher) throws IOException {
    return new ConstantScoreQuery.ConstantWeight(searcher);
  }

  @Override
  public String toString(String field) {
    return new StringBuilder("ConstantScore(")
      .append((query == null) ? filter.toString() : query.toString(field))
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
      return 
        ((this.filter == null) ? other.filter == null : this.filter.equals(other.filter)) &&
        ((this.query == null) ? other.query == null : this.query.equals(other.query));
    }
    return false;
  }

  @Override
  public int hashCode() {
    return 31 * super.hashCode() +
      ((query == null) ? filter : query).hashCode();
  }

}
