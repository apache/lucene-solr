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
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldValueQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;

abstract class SortedNumericDocValuesRangeQuery extends Query {

  private final String field;
  private final long lowerValue;
  private final long upperValue;

  SortedNumericDocValuesRangeQuery(String field, long lowerValue, long upperValue) {
    this.field = Objects.requireNonNull(field);
    this.lowerValue = lowerValue;
    this.upperValue = upperValue;
  }

  @Override
  public boolean equals(Object obj) {
    if (sameClassAs(obj) == false) {
      return false;
    }
    SortedNumericDocValuesRangeQuery that = (SortedNumericDocValuesRangeQuery) obj;
    return Objects.equals(field, that.field)
        && lowerValue == that.lowerValue
        && upperValue == that.upperValue;
  }

  @Override
  public int hashCode() {
    int h = classHash();
    h = 31 * h + field.hashCode();
    h = 31 * h + Long.hashCode(lowerValue);
    h = 31 * h + Long.hashCode(upperValue);
    return h;
  }

  @Override
  public String toString(String field) {
    StringBuilder b = new StringBuilder();
    if (this.field.equals(field) == false) {
      b.append(this.field).append(":");
    }
    return b
        .append("[")
        .append(lowerValue)
        .append(" TO ")
        .append(upperValue)
        .append("]")
        .toString();
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    if (lowerValue == Long.MIN_VALUE && upperValue == Long.MAX_VALUE) {
      return new FieldValueQuery(field);
    }
    return super.rewrite(reader);
  }

  abstract SortedNumericDocValues getValues(LeafReader reader, String field) throws IOException;

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
    return new ConstantScoreWeight(this) {
      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        SortedNumericDocValues values = getValues(context.reader(), field);
        if (values == null) {
          return null;
        }
        final NumericDocValues singleton = DocValues.unwrapSingleton(values);
        final TwoPhaseIterator iterator;
        final DocIdSetIterator approximation = DocIdSetIterator.all(context.reader().maxDoc());
        if (singleton != null) {
          final Bits docsWithField = DocValues.unwrapSingletonBits(values);
          iterator = new TwoPhaseIterator(approximation) {
            @Override
            public boolean matches() throws IOException {
              final long value = singleton.get(approximation.docID());
              return (value != 0 || docsWithField == null || docsWithField.get(approximation.docID()))
                  && value >= lowerValue && value <= upperValue;
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
              for (int i = 0, count = values.count(); i < count; ++i) {
                final long value = values.valueAt(i);
                if (value < lowerValue) {
                  continue;
                }
                // Values are sorted, so the first value that is >= lowerValue is our best candidate
                return value <= upperValue;
              }
              return false; // all values were < lowerValue
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
