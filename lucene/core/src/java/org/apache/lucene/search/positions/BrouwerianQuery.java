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

import java.io.IOException;
import java.util.Set;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;

/**
 *
 * @lucene.experimental
 */ // nocommit - javadoc
public final class BrouwerianQuery extends Query implements Cloneable {
  
  private Query minuted;
  private Query subtracted;


  public BrouwerianQuery(Query minuted, Query subtracted) {
    this.minuted = minuted;
    this.subtracted = subtracted;
  }

  @Override
  public void extractTerms(Set<Term> terms) {
    minuted.extractTerms(terms);
    subtracted.extractTerms(terms);
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    BrouwerianQuery clone = null;

    Query rewritten =  minuted.rewrite(reader);
    Query subRewritten =  subtracted.rewrite(reader);
    if (rewritten != minuted || subRewritten != subtracted) {
      clone = (BrouwerianQuery) this.clone();
      clone.minuted = rewritten;
      clone.subtracted = subRewritten;
    }

    if (clone != null) {
      return clone; // some clauses rewrote
    } else {
      return this; // no clauses rewrote
    }
  }

  @Override
  public Weight createWeight(IndexSearcher searcher) throws IOException {
    return new BrouwerianQueryWeight(minuted.createWeight(searcher), subtracted.createWeight(searcher));
  }

  class BrouwerianQueryWeight extends Weight {

    private final Weight minuted;
    private final Weight subtracted;

    public BrouwerianQueryWeight(Weight minuted, Weight subtracted) {
      this.minuted = minuted;
      this.subtracted = subtracted;
    }

    @Override
    public Explanation explain(AtomicReaderContext context, int doc)
        throws IOException {
      return minuted.explain(context, doc);
    }

    @Override
    public Scorer scorer(AtomicReaderContext context, boolean scoreDocsInOrder,
        boolean topScorer, boolean needsPositions, boolean needsOffsets, boolean collectPositions, Bits acceptDocs) throws IOException {
      final Scorer scorer = minuted.scorer(context, true, topScorer, true, needsOffsets, collectPositions, acceptDocs);
      final Scorer subScorer = subtracted.scorer(context, true, topScorer, true, needsOffsets, collectPositions, acceptDocs);
      if (subScorer == null) {
        return scorer;
      }
      return scorer == null ? null : new PositionFilterScorer(this, collectPositions, scorer, subScorer);
    }

    @Override
    public Query getQuery() {
      return BrouwerianQuery.this;
    }
    
    @Override
    public float getValueForNormalization() throws IOException {
      return minuted.getValueForNormalization();
    }

    @Override
    public void normalize(float norm, float topLevelBoost) {
      minuted.normalize(norm, topLevelBoost);
    }
  }

  class PositionFilterScorer extends Scorer {

    private final Scorer other;
    private IntervalIterator filter;
    private final Scorer subtracted;
    Interval current;
    private final boolean collectPositions;

    public PositionFilterScorer(Weight weight, boolean collectPositions, Scorer other, Scorer substacted) throws IOException {
      super(weight);
      this.other = other;
      this.subtracted = substacted;
      this.collectPositions = collectPositions;
      this.filter = new BrouwerianIntervalIterator(other, collectPositions, other.positions(), substacted.positions());
    }

    @Override
    public float score() throws IOException {
      return other.score();
    }

    @Override
    public IntervalIterator positions() throws IOException {
      return new IntervalIterator(this, collectPositions) {
        private boolean buffered = true;
        @Override
        public int scorerAdvanced(int docId) throws IOException {
          buffered = true;
          assert docId == filter.docID();
          return docId;
        }

        @Override
        public Interval next() throws IOException {
          if (buffered) {
            buffered = false;
            return current;
          }
          else if (current != null) {
            return current = filter.next();
          }
          return null;
        }

        @Override
        public void collect(IntervalCollector collector) {
          filter.collect(collector);
        }

        @Override
        public IntervalIterator[] subs(boolean inOrder) {
          return filter.subs(inOrder);
        }

        @Override
        public int matchDistance() {
          return filter.matchDistance();
        }
        
      };
    }

    @Override
    public int docID() {
      return other.docID();
    }

    @Override
    public int nextDoc() throws IOException {
      int docId = -1;
      while ((docId = other.nextDoc()) != Scorer.NO_MORE_DOCS) {
        subtracted.advance(docId);
        filter.scorerAdvanced(docId);
        if ((current = filter.next()) != null) { // just check if there is a position that matches!
          return other.docID();
        }
      }
      return Scorer.NO_MORE_DOCS;
    }

    @Override
    public int advance(int target) throws IOException {
      int docId = other.advance(target);
      subtracted.advance(docId);
      if (docId == Scorer.NO_MORE_DOCS) {
        return NO_MORE_DOCS;
      }
      do {
        filter.scorerAdvanced(docId);
        if ((current = filter.next()) != null) {
          return other.docID();
        }
      } while ((docId = other.nextDoc()) != Scorer.NO_MORE_DOCS);
      return NO_MORE_DOCS;
    }

    @Override
    public float freq() throws IOException {
      return other.freq();
    }

  }

  @Override
  public String toString(String field) {
    //nocommit TODO
    return minuted.toString();
  }

  /* 
   *
   */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((minuted == null) ? 0 : minuted.hashCode());
    result = prime * result
        + ((subtracted == null) ? 0 : subtracted.hashCode());
    return result;
  }

  /* 
   *
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!super.equals(obj)) return false;
    if (getClass() != obj.getClass()) return false;
    BrouwerianQuery other = (BrouwerianQuery) obj;
    if (minuted == null) {
      if (other.minuted != null) return false;
    } else if (!minuted.equals(other.minuted)) return false;
    if (subtracted == null) {
      if (other.subtracted != null) return false;
    } else if (!subtracted.equals(other.subtracted)) return false;
    return true;
  }

}