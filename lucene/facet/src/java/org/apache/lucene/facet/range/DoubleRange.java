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
package org.apache.lucene.facet.range;

import java.io.IOException;
import java.util.Objects;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.NumericUtils;

/** Represents a range over double values.
 *
 * @lucene.experimental */
public final class DoubleRange extends Range {
  /** Minimum (inclusive). */
  public final double min;

  /** Maximum (inclusive. */
  public final double max;

  /** Create a DoubleRange. */
  public DoubleRange(String label, double minIn, boolean minInclusive, double maxIn, boolean maxInclusive) {
    super(label);

    // TODO: if DoubleDocValuesField used
    // NumericUtils.doubleToSortableLong format (instead of
    // Double.doubleToRawLongBits) we could do comparisons
    // in long space 

    if (Double.isNaN(minIn)) {
      throw new IllegalArgumentException("min cannot be NaN");
    }
    if (!minInclusive) {
      minIn = Math.nextUp(minIn);
    }

    if (Double.isNaN(maxIn)) {
      throw new IllegalArgumentException("max cannot be NaN");
    }
    if (!maxInclusive) {
      // Why no Math.nextDown?
      maxIn = Math.nextAfter(maxIn, Double.NEGATIVE_INFINITY);
    }

    if (minIn > maxIn) {
      failNoMatch();
    }

    this.min = minIn;
    this.max = maxIn;
  }

  /** True if this range accepts the provided value. */
  public boolean accept(double value) {
    return value >= min && value <= max;
  }

  LongRange toLongRange() {
    return new LongRange(label,
                         NumericUtils.doubleToSortableLong(min), true,
                         NumericUtils.doubleToSortableLong(max), true);
  }

  @Override
  public String toString() {
    return "DoubleRange(" + min + " to " + max + ")";
  }

  private static class ValueSourceQuery extends Query {
    private final DoubleRange range;
    private final Query fastMatchQuery;
    private final DoubleValuesSource valueSource;

    ValueSourceQuery(DoubleRange range, Query fastMatchQuery, DoubleValuesSource valueSource) {
      this.range = range;
      this.fastMatchQuery = fastMatchQuery;
      this.valueSource = valueSource;
    }

    @Override
    public boolean equals(Object other) {
      return sameClassAs(other) &&
             equalsTo(getClass().cast(other));
    }

    private boolean equalsTo(ValueSourceQuery other) {
      return range.equals(other.range) && 
             Objects.equals(fastMatchQuery, other.fastMatchQuery) && 
             valueSource.equals(other.valueSource);
    }

    @Override
    public int hashCode() {
      return classHash() + 31 * Objects.hash(range, fastMatchQuery, valueSource);
    }

    @Override
    public String toString(String field) {
      return "Filter(" + range.toString() + ")";
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
      if (fastMatchQuery != null) {
        final Query fastMatchRewritten = fastMatchQuery.rewrite(reader);
        if (fastMatchRewritten != fastMatchQuery) {
          return new ValueSourceQuery(range, fastMatchRewritten, valueSource);
        }
      }
      return super.rewrite(reader);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
      final Weight fastMatchWeight = fastMatchQuery == null
          ? null
          : searcher.createWeight(fastMatchQuery, false);

      return new ConstantScoreWeight(this) {
        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
          final int maxDoc = context.reader().maxDoc();

          final DocIdSetIterator approximation;
          if (fastMatchWeight == null) {
            approximation = DocIdSetIterator.all(maxDoc);
          } else {
            Scorer s = fastMatchWeight.scorer(context);
            if (s == null) {
              return null;
            }
            approximation = s.iterator();
          }

          final DoubleValues values = valueSource.getValues(context, null);
          final TwoPhaseIterator twoPhase = new TwoPhaseIterator(approximation) {
            @Override
            public boolean matches() throws IOException {
              return values.advanceExact(approximation.docID()) && range.accept(values.doubleValue());
            }

            @Override
            public float matchCost() {
              return 100; // TODO: use cost of range.accept()
            }
          };
          return new ConstantScoreScorer(this, score(), twoPhase);
        }
      };
    }

  }

  /**
   * @deprecated Use {@link #getQuery(Query, DoubleValuesSource)}
   */
  @Deprecated
  public Query getQuery(final Query fastMatchQuery, final ValueSource valueSource) {
    return new ValueSourceQuery(this, fastMatchQuery, valueSource.asDoubleValuesSource());
  }

  /**
   * Create a Query that matches documents in this range
   *
   * The query will check all documents that match the provided match query,
   * or every document in the index if the match query is null.
   *
   * If the value source is static, eg an indexed numeric field, it may be
   * faster to use {@link org.apache.lucene.search.PointRangeQuery}
   *
   * @param fastMatchQuery a query to use as a filter
   * @param valueSource    the source of values for the range check
   */
  public Query getQuery(Query fastMatchQuery, DoubleValuesSource valueSource) {
    return new ValueSourceQuery(this, fastMatchQuery, valueSource);
  }
}

