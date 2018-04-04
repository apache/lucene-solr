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

package org.apache.lucene.search.intervals;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;

import org.apache.lucene.codecs.lucene50.Lucene50PostingsFormat;
import org.apache.lucene.codecs.lucene50.Lucene50PostingsReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
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
  public void extractTerms(String field, Set<Term> terms) {
    terms.add(new Term(field, term));
  }

  /** A guess of
   * the average number of simple operations for the initial seek and buffer refill
   * per document for the positions of a term.
   * See also {@link Lucene50PostingsReader.BlockPostingsEnum#nextPosition()}.
   * <p>
   * Aside: Instead of being constant this could depend among others on
   * {@link Lucene50PostingsFormat#BLOCK_SIZE},
   * {@link TermsEnum#docFreq()},
   * {@link TermsEnum#totalTermFreq()},
   * {@link DocIdSetIterator#cost()} (expected number of matching docs),
   * {@link LeafReader#maxDoc()} (total number of docs in the segment),
   * and the seek time and block size of the device storing the index.
   */
  private static final int TERM_POSNS_SEEK_OPS_PER_DOC = 128;

  /** Number of simple operations in {@link Lucene50PostingsReader.BlockPostingsEnum#nextPosition()}
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
