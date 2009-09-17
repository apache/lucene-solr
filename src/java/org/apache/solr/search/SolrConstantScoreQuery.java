package org.apache.solr.search;

import org.apache.lucene.search.*;
import org.apache.lucene.index.IndexReader;
import org.apache.solr.search.function.ValueSource;
import org.apache.solr.common.SolrException;

import java.io.IOException;
import java.util.Set;
import java.util.Map;

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

/**
 * A query that wraps a filter and simply returns a constant score equal to the
 * query boost for every document in the filter.   This Solr extension also supports
 * weighting of a SolrFilter.
 *
 * Experimental and subject to change.
 */
public class SolrConstantScoreQuery extends ConstantScoreQuery {

  public SolrConstantScoreQuery(Filter filter) {
    super(filter);
  }

  /** Returns the encapsulated filter */
  public Filter getFilter() {
    return filter;
  }

  public Query rewrite(IndexReader reader) throws IOException {
    return this;
  }

  public void extractTerms(Set terms) {
    // OK to not add any terms when used for MultiSearcher,
    // but may not be OK for highlighting
  }

  protected class ConstantWeight extends Weight {
    private Similarity similarity;
    private float queryNorm;
    private float queryWeight;
    private Map context;

    public ConstantWeight(Searcher searcher) throws IOException {
      this.similarity = getSimilarity(searcher);
      this.context = ValueSource.newContext();
      if (filter instanceof SolrFilter)
        ((SolrFilter)filter).createWeight(context, searcher);
    }

    public Query getQuery() {
      return SolrConstantScoreQuery.this;
    }

    public float getValue() {
      return queryWeight;
    }

    public float sumOfSquaredWeights() throws IOException {
      queryWeight = getBoost();
      return queryWeight * queryWeight;
    }

    public void normalize(float norm) {
      this.queryNorm = norm;
      queryWeight *= this.queryNorm;
    }

    public Scorer scorer(IndexReader reader, boolean scoreDocsInOrder, boolean topScorer) throws IOException {
      return new ConstantScorer(similarity, reader, this);
    }

    public Explanation explain(IndexReader reader, int doc) throws IOException {

      ConstantScorer cs = new ConstantScorer(similarity, reader, this);
      boolean exists = cs.docIdSetIterator.advance(doc) == doc;

      ComplexExplanation result = new ComplexExplanation();

      if (exists) {
        result.setDescription("ConstantScoreQuery(" + filter
        + "), product of:");
        result.setValue(queryWeight);
        result.setMatch(Boolean.TRUE);
        result.addDetail(new Explanation(getBoost(), "boost"));
        result.addDetail(new Explanation(queryNorm,"queryNorm"));
      } else {
        result.setDescription("ConstantScoreQuery(" + filter
        + ") doesn't match id " + doc);
        result.setValue(0);
        result.setMatch(Boolean.FALSE);
      }
      return result;
    }
  }

  protected class ConstantScorer extends Scorer {
    final DocIdSetIterator docIdSetIterator;
    final float theScore;
    int doc = -1;

    public ConstantScorer(Similarity similarity, IndexReader reader, ConstantWeight w) throws IOException {
      super(similarity);
      theScore = w.getValue();
      DocIdSet docIdSet = filter instanceof SolrFilter ? ((SolrFilter)filter).getDocIdSet(w.context, reader) : filter.getDocIdSet(reader);
      if (docIdSet == null) {
        docIdSetIterator = DocIdSet.EMPTY_DOCIDSET.iterator();
      } else {
        DocIdSetIterator iter = docIdSet.iterator();
        if (iter == null) {
          docIdSetIterator = DocIdSet.EMPTY_DOCIDSET.iterator();
        } else {
          docIdSetIterator = iter;
        }
      }
    }

    /** @deprecated use {@link #nextDoc()} instead. */
    public boolean next() throws IOException {
      return docIdSetIterator.nextDoc() != NO_MORE_DOCS;
    }

    public int nextDoc() throws IOException {
      return docIdSetIterator.nextDoc();
    }

    /** @deprecated use {@link #docID()} instead. */
    public int doc() {
      return docIdSetIterator.doc();
    }

    public int docID() {
      return docIdSetIterator.docID();
    }

    public float score() throws IOException {
      return theScore;
    }

    /** @deprecated use {@link #advance(int)} instead. */
    public boolean skipTo(int target) throws IOException {
      return docIdSetIterator.advance(target) != NO_MORE_DOCS;
    }

    public int advance(int target) throws IOException {
      return docIdSetIterator.advance(target);
    }

    public Explanation explain(int doc) throws IOException {
      throw new UnsupportedOperationException();
    }
  }

  public Weight createWeight(Searcher searcher) {
    try {
      return new SolrConstantScoreQuery.ConstantWeight(searcher);
    } catch (IOException e) {
      // TODO: remove this if ConstantScoreQuery.createWeight adds IOException
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  /** Prints a user-readable version of this query. */
  public String toString(String field) {
    return "ConstantScore(" + filter.toString()
      + (getBoost()==1.0 ? ")" : "^" + getBoost());
  }

  /** Returns true if <code>o</code> is equal to this. */
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof SolrConstantScoreQuery)) return false;
    SolrConstantScoreQuery other = (SolrConstantScoreQuery)o;
    return this.getBoost()==other.getBoost() && filter.equals(other.filter);
  }

  /** Returns a hash code value for this object. */
  public int hashCode() {
    // Simple add is OK since no existing filter hashcode has a float component.
    return filter.hashCode() + Float.floatToIntBits(getBoost());
  }

}
