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

package org.apache.lucene.search;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
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
    TermsEnum te = terms.iterator();
    te.seekExact(term);
    PostingsEnum pe = te.postings(null, PostingsEnum.POSITIONS);
    float cost = PhraseQuery.termPositionsCost(te);
    return new IntervalIterator() {

      int pos, upto;

      @Override
      public DocIdSetIterator approximation() {
        return pe;
      }

      @Override
      public void reset() throws IOException {
        pos = -1;
        upto = pe.freq();
      }

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
      public float score() {
        return 1;
      }

      @Override
      public float cost() {
        return cost;
      }

      @Override
      public String toString() {
        return pe.docID() + ":" + pos;
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
}
