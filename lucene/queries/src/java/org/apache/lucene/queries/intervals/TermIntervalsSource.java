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

package org.apache.lucene.queries.intervals;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

import org.apache.lucene.codecs.lucene84.Lucene84PostingsFormat;
import org.apache.lucene.codecs.lucene84.Lucene84PostingsReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.MatchesIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.util.BytesRef;

class TermIntervalsSource extends IntervalsSource {

  final BytesRef term;

  TermIntervalsSource(BytesRef term) {
    this.term = term;
  }

  @Override
  public IntervalIterator intervals(String field, LeafReaderContext ctx) throws IOException {
    Terms terms = ctx.reader().terms(field);
    if (terms == null)
      return null;
    if (terms.hasPositions() == false) {
      throw new IllegalArgumentException("Cannot create an IntervalIterator over field " + field + " because it has no indexed positions");
    }
    TermsEnum te = terms.iterator();
    if (te.seekExact(term) == false) {
      return null;
    }
    return intervals(term, te);
  }

  static IntervalIterator intervals(BytesRef term, TermsEnum te) throws IOException {
    PostingsEnum pe = te.postings(null, PostingsEnum.POSITIONS);
    float cost = termPositionsCost(te);
    return new IntervalIterator() {

      @Override
      public int docID() {
        return pe.docID();
      }

      @Override
      public int nextDoc() throws IOException {
        int doc = pe.nextDoc();
        reset();
        return doc;
      }

      @Override
      public int advance(int target) throws IOException {
        int doc = pe.advance(target);
        reset();
        return doc;
      }

      @Override
      public long cost() {
        return pe.cost();
      }

      int pos = -1, upto;

      @Override
      public int start() {
        return pos;
      }

      @Override
      public int end() {
        return pos;
      }

      @Override
      public int gaps() {
        return 0;
      }

      @Override
      public int nextInterval() throws IOException {
        if (upto <= 0)
          return pos = NO_MORE_INTERVALS;
        upto--;
        return pos = pe.nextPosition();
      }

      @Override
      public float matchCost() {
        return cost;
      }

      private void reset() throws IOException {
        if (pe.docID() == NO_MORE_DOCS) {
          upto = -1;
          pos = NO_MORE_INTERVALS;
        }
        else {
          upto = pe.freq();
          pos = -1;
        }
      }

      @Override
      public String toString() {
        return term.utf8ToString() + ":" + super.toString();
      }
    };
  }

  @Override
  public MatchesIterator matches(String field, LeafReaderContext ctx, int doc) throws IOException {
    Terms terms = ctx.reader().terms(field);
    if (terms == null)
      return null;
    if (terms.hasPositions() == false) {
      throw new IllegalArgumentException("Cannot create an IntervalIterator over field " + field + " because it has no indexed positions");
    }
    TermsEnum te = terms.iterator();
    if (te.seekExact(term) == false) {
      return null;
    }
    return matches(te, doc, field);
  }

  static MatchesIterator matches(TermsEnum te, int doc, String field) throws IOException {
    TermQuery query = new TermQuery(new Term(field, te.term()));
    PostingsEnum pe = te.postings(null, PostingsEnum.OFFSETS);
    if (pe.advance(doc) != doc) {
      return null;
    }
    return new MatchesIterator() {

      int upto = pe.freq();
      int pos = -1;

      @Override
      public boolean next() throws IOException {
        if (upto <= 0) {
          pos = IntervalIterator.NO_MORE_INTERVALS;
          return false;
        }
        upto--;
        pos = pe.nextPosition();
        return true;
      }

      @Override
      public int startPosition() {
        return pos;
      }

      @Override
      public int endPosition() {
        return pos;
      }

      @Override
      public int startOffset() throws IOException {
        return pe.startOffset();
      }

      @Override
      public int endOffset() throws IOException {
        return pe.endOffset();
      }

      @Override
      public MatchesIterator getSubMatches() {
        return null;
      }

      @Override
      public Query getQuery() {
        return query;
      }
    };
  }

  @Override
  public int minExtent() {
    return 1;
  }

  @Override
  public Collection<IntervalsSource> pullUpDisjunctions() {
    return Collections.singleton(this);
  }

  @Override
  public int hashCode() {
    return Objects.hash(term);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TermIntervalsSource that = (TermIntervalsSource) o;
    return Objects.equals(term, that.term);
  }

  @Override
  public String toString() {
    return term.utf8ToString();
  }

  @Override
  public void visit(String field, QueryVisitor visitor) {
    visitor.consumeTerms(new IntervalQuery(field, this), new Term(field, term));
  }

  /** A guess of
   * the average number of simple operations for the initial seek and buffer refill
   * per document for the positions of a term.
   * See also {@link Lucene84PostingsReader.EverythingEnum#nextPosition()}.
   * <p>
   * Aside: Instead of being constant this could depend among others on
   * {@link Lucene84PostingsFormat#BLOCK_SIZE},
   * {@link TermsEnum#docFreq()},
   * {@link TermsEnum#totalTermFreq()},
   * {@link DocIdSetIterator#cost()} (expected number of matching docs),
   * {@link LeafReader#maxDoc()} (total number of docs in the segment),
   * and the seek time and block size of the device storing the index.
   */
  private static final int TERM_POSNS_SEEK_OPS_PER_DOC = 128;

  /** Number of simple operations in {@link Lucene84PostingsReader.EverythingEnum#nextPosition()}
   *  when no seek or buffer refill is done.
   */
  private static final int TERM_OPS_PER_POS = 7;

  /** Returns an expected cost in simple operations
   *  of processing the occurrences of a term
   *  in a document that contains the term.
   *  This is for use by {@link TwoPhaseIterator#matchCost} implementations.
   *  @param termsEnum The term is the term at which this TermsEnum is positioned.
   */
  static float termPositionsCost(TermsEnum termsEnum) throws IOException {
    // TODO: When intervals move to core, refactor to use the copy of this in PhraseQuery
    int docFreq = termsEnum.docFreq();
    assert docFreq > 0;
    long totalTermFreq = termsEnum.totalTermFreq();
    float expOccurrencesInMatchingDoc = totalTermFreq / (float) docFreq;
    return TERM_POSNS_SEEK_OPS_PER_DOC + expOccurrencesInMatchingDoc * TERM_OPS_PER_POS;
  }
}
