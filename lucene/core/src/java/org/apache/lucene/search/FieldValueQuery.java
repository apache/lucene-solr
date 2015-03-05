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
import java.util.Objects;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.Bits.MatchNoBits;
import org.apache.lucene.util.ToStringUtils;

/**
 * A {@link Query} that matches documents that have a value for a given field
 * as reported by {@link LeafReader#getDocsWithField(String)}.
 */
public final class FieldValueQuery extends Query {

  private final String field;

  /** Create a query that will match that have a value for the given
   *  {@code field}. */
  public FieldValueQuery(String field) {
    this.field = Objects.requireNonNull(field);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof FieldValueQuery == false) {
      return false;
    }
    final FieldValueQuery that = (FieldValueQuery) obj;
    return super.equals(obj) && field.equals(that.field);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), field, getBoost());
  }

  @Override
  public String toString(String field) {
    return "FieldValueQuery [field=" + this.field + "]" + ToStringUtils.boost(getBoost());
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
    return new ConstantScoreWeight(this) {

      @Override
      public Scorer scorer(LeafReaderContext context, final Bits acceptDocs, final float score) throws IOException {
        final Bits docsWithField = context.reader().getDocsWithField(field);
        if (docsWithField == null || docsWithField instanceof MatchNoBits) {
          return null;
        }

        final DocIdSetIterator approximation = DocIdSetIterator.all(context.reader().maxDoc());
        final TwoPhaseIterator twoPhaseIterator = new TwoPhaseIterator() {

          @Override
          public boolean matches() throws IOException {
            final int doc = approximation.docID();
            if (acceptDocs != null && acceptDocs.get(doc) == false) {
              return false;
            }
            if (docsWithField.get(doc) == false) {
              return false;
            }
            return true;
          }

          @Override
          public DocIdSetIterator approximation() {
            return approximation;
          }
        };
        final DocIdSetIterator disi = TwoPhaseIterator.asDocIdSetIterator(twoPhaseIterator);

        return new Scorer(this) {

          @Override
          public TwoPhaseIterator asTwoPhaseIterator() {
            return twoPhaseIterator;
          }

          @Override
          public int nextDoc() throws IOException {
            return disi.nextDoc();
          }

          @Override
          public int docID() {
            return disi.docID();
          }

          @Override
          public long cost() {
            return disi.cost();
          }

          @Override
          public int advance(int target) throws IOException {
            return disi.advance(target);
          }

          @Override
          public int freq() throws IOException {
            return 1;
          }

          @Override
          public float score() throws IOException {
            return score;
          }
        };
      }
    };
  }

}
