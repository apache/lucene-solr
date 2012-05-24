package org.apache.lucene.search.positions;

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
import org.apache.lucene.search.*;
import org.apache.lucene.search.positions.PositionIntervalIterator.PositionIntervalFilter;
import org.apache.lucene.util.Bits;

import java.io.IOException;
import java.util.Set;

/**
 *
 * @lucene.experimental
 */ // nocommit - javadoc
public class PositionFilterQuery extends Query implements Cloneable {

  private Query inner;
  private PositionIntervalFilter filter;

  public PositionFilterQuery(Query inner, PositionIntervalFilter filter) {
    this.inner = inner;
    this.filter = filter;
  }

  @Override
  public void extractTerms(Set<Term> terms) {
    inner.extractTerms(terms);
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    PositionFilterQuery clone = null;

    Query rewritten = (Query) inner.rewrite(reader);
    if (rewritten != inner) {
      clone = (PositionFilterQuery) this.clone();
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
      return other.explain(context, doc);
    }

    @Override
    public Scorer scorer(AtomicReaderContext context, boolean scoreDocsInOrder,
        boolean topScorer, Bits acceptDocs) throws IOException {
      final Scorer scorer = other.scorer(context, scoreDocsInOrder, topScorer, acceptDocs);
      return scorer == null ? null : new PositionFilterScorer(this, scorer);
    }

    @Override
    public Query getQuery() {
      return PositionFilterQuery.this;
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
    private PositionIntervalIterator filter;
    private final FilteredPositions filtered;

    public PositionFilterScorer(Weight weight, Scorer other) throws IOException {
      super(weight);
      this.other = other;
      // nocommit - offsets and payloads?
      this.filter = PositionFilterQuery.this.filter != null
          ? PositionFilterQuery.this.filter.filter(other.positions(false, false))
          : other.positions(false, false);
      filtered = new FilteredPositions(this, filter);
    }

    @Override
    public float score() throws IOException {
      return other.score();
    }

    @Override
    public PositionIntervalIterator positions(boolean needsPayloads, boolean needsOffsets) throws IOException {
      return PositionFilterQuery.this.filter != null ? PositionFilterQuery.this.filter
          .filter(other.positions(needsPayloads, needsOffsets)) : other.positions(needsPayloads, needsOffsets);
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
        if ((filtered.interval = filter.next()) != null) { // just check if there is a position that matches!
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
        if ((filtered.interval = filter.next()) != null) {
          return other.docID();
        }
      } while ((docId = other.nextDoc()) != Scorer.NO_MORE_DOCS);
      return NO_MORE_DOCS;
    }

  }

  @Override
  public String toString(String field) {
    return inner.toString();
  }
  
  private final class FilteredPositions extends PositionIntervalIterator {
    public FilteredPositions(Scorer scorer, PositionIntervalIterator other) {
      super(scorer);
      this.other = other;
    }

    private final PositionIntervalIterator other;
    PositionInterval interval;
    @Override
    public int advanceTo(int docId) throws IOException {
      return currentDoc = other.docID();
    }

    @Override
    public PositionInterval next() throws IOException {
      PositionInterval current = this.interval;
      this.interval = null;
      return current;
    }

    @Override
    public void collect() {
      other.collect();
      
    }
    @Override
    public PositionIntervalIterator[] subs(boolean inOrder) {
      return EMPTY;
    }

    public void setPositionCollector(PositionCollector collector) {
      other.setPositionCollector(collector);
    }

  }

}