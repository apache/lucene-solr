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
import java.util.function.Predicate;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.MatchesIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.util.BytesRef;

class PayloadFilteredTermIntervalsSource extends IntervalsSource {

  final BytesRef term;
  final Predicate<BytesRef> filter;

  PayloadFilteredTermIntervalsSource(BytesRef term, Predicate<BytesRef> filter) {
    this.term = term;
    this.filter = filter;
  }

  @Override
  public IntervalIterator intervals(String field, LeafReaderContext ctx) throws IOException {
    Terms terms = ctx.reader().terms(field);
    if (terms == null)
      return null;
    if (terms.hasPositions() == false) {
      throw new IllegalArgumentException("Cannot create an IntervalIterator over field " + field + " because it has no indexed positions");
    }
    if (terms.hasPayloads() == false) {
      throw new IllegalArgumentException("Cannot create a payload-filtered iterator over field " + field + " because it has no indexed payloads");
    }
    TermsEnum te = terms.iterator();
    if (te.seekExact(term) == false) {
      return null;
    }
    return intervals(te);
  }

  private IntervalIterator intervals(TermsEnum te) throws IOException {
    PostingsEnum pe = te.postings(null, PostingsEnum.PAYLOADS);
    float cost = TermIntervalsSource.termPositionsCost(te);
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
        do {
          if (upto <= 0)
            return pos = NO_MORE_INTERVALS;
          upto--;
          pos = pe.nextPosition();
        }
        while (filter.test(pe.getPayload()) == false);
        return pos;
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
  public IntervalMatchesIterator matches(String field, LeafReaderContext ctx, int doc) throws IOException {
    Terms terms = ctx.reader().terms(field);
    if (terms == null)
      return null;
    if (terms.hasPositions() == false) {
      throw new IllegalArgumentException("Cannot create an IntervalIterator over field " + field + " because it has no indexed positions");
    }
    if (terms.hasPayloads() == false) {
      throw new IllegalArgumentException("Cannot create a payload-filtered iterator over field " + field + " because it has no indexed payloads");
    }
    TermsEnum te = terms.iterator();
    if (te.seekExact(term) == false) {
      return null;
    }
    return matches(te, doc);
  }

  @Override
  public void visit(String field, QueryVisitor visitor) {
    visitor.consumeTerms(new IntervalQuery(field, this), new Term(field, term));
  }

  private IntervalMatchesIterator matches(TermsEnum te, int doc) throws IOException {
    PostingsEnum pe = te.postings(null, PostingsEnum.ALL);
    if (pe.advance(doc) != doc) {
      return null;
    }
    return new IntervalMatchesIterator() {

      @Override
      public int gaps() {
        return 0;
      }

      @Override
      public int width() {
        return 1;
      }

      int upto = pe.freq();
      int pos = -1;

      @Override
      public boolean next() throws IOException {
        do {
          if (upto <= 0) {
            pos = IntervalIterator.NO_MORE_INTERVALS;
            return false;
          }
          upto--;
          pos = pe.nextPosition();
        }
        while (filter.test(pe.getPayload()) == false);
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
        throw new UnsupportedOperationException();
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
    return "PAYLOAD_FILTERED(" + term.utf8ToString() + ")";
  }

}
