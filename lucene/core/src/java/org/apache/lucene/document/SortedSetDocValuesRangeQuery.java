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
package org.apache.lucene.document;

import java.io.IOException;
import java.util.Objects;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldValueQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;

abstract class SortedSetDocValuesRangeQuery extends Query {

  private final String field;
  private final BytesRef lowerValue;
  private final BytesRef upperValue;
  private final boolean lowerInclusive;
  private final boolean upperInclusive;

  SortedSetDocValuesRangeQuery(String field,
      BytesRef lowerValue, BytesRef upperValue,
      boolean lowerInclusive, boolean upperInclusive) {
    this.field = Objects.requireNonNull(field);
    this.lowerValue = lowerValue;
    this.upperValue = upperValue;
    this.lowerInclusive = lowerInclusive && lowerValue != null;
    this.upperInclusive = upperInclusive && upperValue != null;
  }

  @Override
  public boolean equals(Object obj) {
    if (sameClassAs(obj) == false) {
      return false;
    }
    SortedSetDocValuesRangeQuery that = (SortedSetDocValuesRangeQuery) obj;
    return Objects.equals(field, that.field)
        && Objects.equals(lowerValue, that.lowerValue)
        && Objects.equals(upperValue, that.upperValue)
        && lowerInclusive == that.lowerInclusive
        && upperInclusive == that.upperInclusive;
  }

  @Override
  public int hashCode() {
    int h = classHash();
    h = 31 * h + field.hashCode();
    h = 31 * h + Objects.hashCode(lowerValue);
    h = 31 * h + Objects.hashCode(upperValue);
    h = 31 * h + Boolean.hashCode(lowerInclusive);
    h = 31 * h + Boolean.hashCode(upperInclusive);
    return h;
  }

  @Override
  public String toString(String field) {
    StringBuilder b = new StringBuilder();
    if (this.field.equals(field) == false) {
      b.append(this.field).append(":");
    }
    return b
        .append(lowerInclusive ? "[" : "{")
        .append(lowerValue == null ? "*" : lowerValue)
        .append(" TO ")
        .append(upperValue == null ? "*" : upperValue)
        .append(upperInclusive ? "]" : "}")
        .toString();
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    if (lowerValue == null && upperValue == null) {
      return new FieldValueQuery(field);
    }
    return super.rewrite(reader);
  }

  abstract SortedSetDocValues getValues(LeafReader reader, String field) throws IOException;

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
    return new ConstantScoreWeight(this) {
      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        SortedSetDocValues values = getValues(context.reader(), field);
        if (values == null) {
          return null;
        }

        final long minOrd;
        if (lowerValue == null) {
          minOrd = 0;
        } else {
          final long ord = values.lookupTerm(lowerValue);
          if (ord < 0) {
            minOrd = -1 - ord;
          } else if (lowerInclusive) {
            minOrd = ord;
          } else {
            minOrd = ord + 1;
          }
        }

        final long maxOrd;
        if (upperValue == null) {
          maxOrd = values.getValueCount() - 1;
        } else {
          final long ord = values.lookupTerm(upperValue);
          if (ord < 0) {
            maxOrd = -2 - ord;
          } else if (upperInclusive) {
            maxOrd = ord;
          } else {
            maxOrd = ord - 1;
          }
        }

        if (minOrd > maxOrd) {
          return null;
        }

        final SortedDocValues singleton = DocValues.unwrapSingleton(values);
        final DocIdSetIterator approximation = DocIdSetIterator.all(context.reader().maxDoc());
        final TwoPhaseIterator iterator;
        if (singleton != null) {
          iterator = new TwoPhaseIterator(approximation) {
            @Override
            public boolean matches() throws IOException {
              final long ord = singleton.getOrd(approximation.docID());
              return ord >= minOrd && ord <= maxOrd;
            }

            @Override
            public float matchCost() {
              return 2; // 2 comparisons
            }
          };
        } else {
          iterator = new TwoPhaseIterator(approximation) {
            @Override
            public boolean matches() throws IOException {
              values.setDocument(approximation.docID());
              for (long ord = values.nextOrd(); ord != SortedSetDocValues.NO_MORE_ORDS; ord = values.nextOrd()) {
                if (ord < minOrd) {
                  continue;
                }
                // Values are sorted, so the first ord that is >= minOrd is our best candidate
                return ord <= maxOrd;
              }
              return false; // all ords were < minOrd
            }

            @Override
            public float matchCost() {
              return 2; // 2 comparisons
            }
          };
        }
        return new ConstantScoreScorer(this, score(), iterator);
      }
    };
  }

}
