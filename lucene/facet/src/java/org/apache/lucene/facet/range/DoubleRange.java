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
import java.util.Collections;
import java.util.Objects;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
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
  final double minIncl;
  final double maxIncl;

  /** Minimum. */
  public final double min;

  /** Maximum. */
  public final double max;

  /** True if the minimum value is inclusive. */
  public final boolean minInclusive;

  /** True if the maximum value is inclusive. */
  public final boolean maxInclusive;

  /** Create a DoubleRange. */
  public DoubleRange(String label, double minIn, boolean minInclusive, double maxIn, boolean maxInclusive) {
    super(label);
    this.min = minIn;
    this.max = maxIn;
    this.minInclusive = minInclusive;
    this.maxInclusive = maxInclusive;

    // TODO: if DoubleDocValuesField used
    // NumericUtils.doubleToSortableLong format (instead of
    // Double.doubleToRawLongBits) we could do comparisons
    // in long space 

    if (Double.isNaN(min)) {
      throw new IllegalArgumentException("min cannot be NaN");
    }
    if (!minInclusive) {
      minIn = Math.nextUp(minIn);
    }

    if (Double.isNaN(max)) {
      throw new IllegalArgumentException("max cannot be NaN");
    }
    if (!maxInclusive) {
      // Why no Math.nextDown?
      maxIn = Math.nextAfter(maxIn, Double.NEGATIVE_INFINITY);
    }

    if (minIn > maxIn) {
      failNoMatch();
    }

    this.minIncl = minIn;
    this.maxIncl = maxIn;
  }

  /** True if this range accepts the provided value. */
  public boolean accept(double value) {
    return value >= minIncl && value <= maxIncl;
  }

  LongRange toLongRange() {
    return new LongRange(label,
                         NumericUtils.doubleToSortableLong(minIncl), true,
                         NumericUtils.doubleToSortableLong(maxIncl), true);
  }

  @Override
  public String toString() {
    return "DoubleRange(" + minIncl + " to " + maxIncl + ")";
  }

  private static class ValueSourceQuery extends Query {
    private final DoubleRange range;
    private final Query fastMatchQuery;
    private final ValueSource valueSource;

    ValueSourceQuery(DoubleRange range, Query fastMatchQuery, ValueSource valueSource) {
      this.range = range;
      this.fastMatchQuery = fastMatchQuery;
      this.valueSource = valueSource;
    }

    @Override
    public boolean equals(Object obj) {
      if (super.equals(obj) == false) {
        return false;
      }
      ValueSourceQuery other = (ValueSourceQuery) obj;
      return range.equals(other.range)
          && Objects.equals(fastMatchQuery, other.fastMatchQuery)
          && valueSource.equals(other.valueSource);
    }

    @Override
    public int hashCode() {
      return 31 * Objects.hash(range, fastMatchQuery, valueSource) + super.hashCode();
    }

    @Override
    public String toString(String field) {
      return "Filter(" + range.toString() + ")";
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
      if (getBoost() != 1f) {
        return super.rewrite(reader);
      }
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

          final FunctionValues values = valueSource.getValues(Collections.emptyMap(), context);
          final TwoPhaseIterator twoPhase = new TwoPhaseIterator(approximation) {
            @Override
            public boolean matches() throws IOException {
              return range.accept(values.doubleVal(approximation.docID()));
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

  @Override
  public Query getQuery(final Query fastMatchQuery, final ValueSource valueSource) {
    return new ValueSourceQuery(this, fastMatchQuery, valueSource);
  }
}

