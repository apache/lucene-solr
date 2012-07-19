package org.apache.lucene.search.positions;

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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.search.positions.IntervalIterator.IntervalFilter;
import org.apache.lucene.util.Bits;

import java.io.IOException;
import java.util.Set;

/**
 *
 * @lucene.experimental
 */ // nocommit - javadoc
public class IntervalFilterQuery extends Query implements Cloneable {

  
  private Query inner;
  private IntervalFilter filter;

  public IntervalFilterQuery(Query inner, IntervalFilter filter) {
    this.inner = inner;
    this.filter = filter;
  }

  @Override
  public void extractTerms(Set<Term> terms) {
    inner.extractTerms(terms);
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    IntervalFilterQuery clone = null;

    Query rewritten =  inner.rewrite(reader);
    if (rewritten != inner) {
      clone = (IntervalFilterQuery) this.clone();
      clone.inner = rewritten;
    }

    if (clone != null) {
      return clone; // some clauses rewrote
    } else {
      return this; // no clauses rewrote
    }
  }

  @Override
  public Weight createWeight(IndexSearcher searcher) throws IOException {
    return new PositionFilterWeight(inner.createWeight(searcher));
  }

  class PositionFilterWeight extends Weight {

    private final Weight other;

    public PositionFilterWeight(Weight other) {
      this.other = other;
    }

    @Override
    public Explanation explain(AtomicReaderContext context, int doc)
        throws IOException {
      Scorer scorer = scorer(context, true, false, context.reader()
          .getLiveDocs());
      if (scorer != null) {
        int newDoc = scorer.advance(doc);
        if (newDoc == doc) {
          return other.explain(context, doc);
        }
      }
      return new ComplexExplanation(false, 0.0f,
          "No matching term within position filter");
    }

    @Override
    public Scorer scorer(AtomicReaderContext context, boolean scoreDocsInOrder,
        boolean topScorer, Bits acceptDocs) throws IOException {
      final Scorer scorer = other.scorer(context, scoreDocsInOrder, topScorer, acceptDocs);
      return scorer == null ? null : new PositionFilterScorer(this, scorer);
    }

    @Override
    public Query getQuery() {
      return IntervalFilterQuery.this;
    }
    
    @Override
    public float getValueForNormalization() throws IOException {
      return other.getValueForNormalization();
    }

    @Override
    public void normalize(float norm, float topLevelBoost) {
      other.normalize(norm, topLevelBoost);
    }
  }

  class PositionFilterScorer extends Scorer {

    private final Scorer other;
    private IntervalIterator filter;

    public PositionFilterScorer(Weight weight, Scorer other) throws IOException {
      super(weight);
      this.other = other;
      // nocommit - offsets and payloads?
      this.filter = IntervalFilterQuery.this.filter != null
          ? IntervalFilterQuery.this.filter.filter(other.positions(false, false, false))
          : other.positions(false, false, false);
    }

    @Override
    public float score() throws IOException {
      return other.score();
    }

    @Override
    public IntervalIterator positions(boolean needsPayloads, boolean needsOffsets, boolean collectPositions) throws IOException {
      return IntervalFilterQuery.this.filter != null ? IntervalFilterQuery.this.filter
          .filter(other.positions(needsPayloads, needsOffsets, false)) : other.positions(needsPayloads, needsOffsets, false);
    }

    @Override
    public int docID() {
      return other.docID();
    }

    @Override
    public int nextDoc() throws IOException {
      int docId = -1;
      while ((docId = other.nextDoc()) != Scorer.NO_MORE_DOCS) {
        filter.advanceTo(docId);
        if (filter.next() != null) { // just check if there is a position that matches!
          return other.docID();
        }
      }
      return Scorer.NO_MORE_DOCS;
    }

    @Override
    public int advance(int target) throws IOException {
      int docId = other.advance(target);
      if (docId == Scorer.NO_MORE_DOCS) {
        return NO_MORE_DOCS;
      }
      do {
        filter.advanceTo(docId);
        if (filter.next() != null) {
          return other.docID();
        }
      } while ((docId = other.nextDoc()) != Scorer.NO_MORE_DOCS);
      return NO_MORE_DOCS;
    }

  }

  @Override
  public String toString(String field) {
    return filter.toString() + "(" + inner.toString() + ")";
  }
  
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((filter == null) ? 0 : filter.hashCode());
    result = prime * result + ((inner == null) ? 0 : inner.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!super.equals(obj)) return false;
    if (getClass() != obj.getClass()) return false;
    IntervalFilterQuery other = (IntervalFilterQuery) obj;
    if (filter == null) {
      if (other.filter != null) return false;
    } else if (!filter.equals(other.filter)) return false;
    if (inner == null) {
      if (other.inner != null) return false;
    } else if (!inner.equals(other.inner)) return false;
    return true;
  }

}