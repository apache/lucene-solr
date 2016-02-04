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
package org.apache.lucene.rangetree;

import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.ToStringUtils;

import java.io.IOException;

/** Finds all previously indexed long values that fall within the specified range.
 *
 * <p>The field must be indexed with {@link RangeTreeDocValuesFormat}, and {@link SortedNumericDocValuesField} added per document.
 *
 * @lucene.experimental */

public class NumericRangeTreeQuery extends Query {
  final String field;
  final Long minValue;
  final Long maxValue;
  final boolean minInclusive;
  final boolean maxInclusive;

  // TODO: sugar for all numeric conversions?

  /** Matches all values in the specified long range. */ 
  public NumericRangeTreeQuery(String field, Long minValue, boolean minInclusive, Long maxValue, boolean maxInclusive) {
    this.field = field;
    this.minInclusive = minInclusive;
    this.minValue = minValue;
    this.maxInclusive = maxInclusive;
    this.maxValue = maxValue;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {

    // I don't use RandomAccessWeight here: it's no good to approximate with "match all docs"; this is an inverted structure and should be
    // used in the first pass:

    return new ConstantScoreWeight(this) {

      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        LeafReader reader = context.reader();
        SortedNumericDocValues sdv = reader.getSortedNumericDocValues(field);
        if (sdv == null) {
          // No docs in this segment had this field
          return null;
        }

        if (sdv instanceof RangeTreeSortedNumericDocValues == false) {
          throw new IllegalStateException("field \"" + field + "\" was not indexed with RangeTreeDocValuesFormat: got: " + sdv);
        }
        RangeTreeSortedNumericDocValues treeDV = (RangeTreeSortedNumericDocValues) sdv;
        RangeTreeReader tree = treeDV.getRangeTreeReader();

        // lower
        long minBoundIncl = (minValue == null) ? Long.MIN_VALUE : minValue.longValue();

        if (minInclusive == false && minValue != null) {
          if (minBoundIncl == Long.MAX_VALUE) {
            return null;
          }
          minBoundIncl++;
        }
          
        // upper
        long maxBoundIncl = (maxValue == null) ? Long.MAX_VALUE : maxValue.longValue();
        if (maxInclusive == false && maxValue != null) {
          if (maxBoundIncl == Long.MIN_VALUE) {
            return null;
          }
          maxBoundIncl--;
        }

        if (maxBoundIncl < minBoundIncl) {
          return null;
        }

        DocIdSet result = tree.intersect(minBoundIncl, maxBoundIncl, treeDV.delegate, context.reader().maxDoc());

        final DocIdSetIterator disi = result.iterator();

        return new ConstantScoreScorer(this, score(), disi);
      }
    };
  }

  @Override
  public int hashCode() {
    int hash = super.hashCode();
    if (minValue != null) hash += minValue.hashCode()^0x14fa55fb;
    if (maxValue != null) hash += maxValue.hashCode()^0x733fa5fe;
    return hash +
      (Boolean.valueOf(minInclusive).hashCode()^0x14fa55fb)+
      (Boolean.valueOf(maxInclusive).hashCode()^0x733fa5fe);
  }

  @Override
  public boolean equals(Object other) {
    if (super.equals(other)) {
      final NumericRangeTreeQuery q = (NumericRangeTreeQuery) other;
      return (
        (q.minValue == null ? minValue == null : q.minValue.equals(minValue)) &&
        (q.maxValue == null ? maxValue == null : q.maxValue.equals(maxValue)) &&
        minInclusive == q.minInclusive &&
        maxInclusive == q.maxInclusive
      );
    }

    return false;
  }

  @Override
  public String toString(String field) {
    final StringBuilder sb = new StringBuilder();
    sb.append(getClass().getSimpleName());
    sb.append(':');
    if (this.field.equals(field) == false) {
      sb.append("field=");
      sb.append(this.field);
      sb.append(':');
    }

    return sb.append(minInclusive ? '[' : '{')
      .append((minValue == null) ? "*" : minValue.toString())
      .append(" TO ")
      .append((maxValue == null) ? "*" : maxValue.toString())
      .append(maxInclusive ? ']' : '}')
      .append(ToStringUtils.boost(getBoost()))
      .toString();
  }
}
