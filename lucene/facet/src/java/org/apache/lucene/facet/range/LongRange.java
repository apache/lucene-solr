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

/** Represents a range over long values.
 *
 * @lucene.experimental */
public final class LongRange extends Range {
  /** Minimum (inclusive). */
  public final long min;

  /** Maximum (inclusive). */
  public final long max;

  // TODO: can we require fewer args? (same for
  // Double/FloatRange too)

  /** Create a LongRange. */
  public LongRange(String label, long minIn, boolean minInclusive, long maxIn, boolean maxInclusive) {
    super(label);

    if (!minInclusive) {
      if (minIn != Long.MAX_VALUE) {
        minIn++;
      } else {
        failNoMatch();
      }
    }

    if (!maxInclusive) {
      if (maxIn != Long.MIN_VALUE) {
        maxIn--;
      } else {
        failNoMatch();
      }
    }

    if (minIn > maxIn) {
      failNoMatch();
    }

    this.min = minIn;
    this.max = maxIn;
  }

  /** True if this range accepts the provided value. */
  public boolean accept(long value) {
    return value >= min && value <= max;
  }

  @Override
  public String toString() {
    return "LongRange(" + min + " to " + max + ")";
  }

  private static class ValueSourceQuery extends Query {
    private final LongRange range;
    private final Query fastMatchQuery;
    private final ValueSource valueSource;

    ValueSourceQuery(LongRange range, Query fastMatchQuery, ValueSource valueSource) {
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
    public Weight createWeight(IndexSearcher searcher, boolean needsScores, float boost) throws IOException {
      final Weight fastMatchWeight = fastMatchQuery == null
          ? null
          : searcher.createWeight(fastMatchQuery, false, 1f);

      return new ConstantScoreWeight(this, boost) {
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
              return range.accept(values.longVal(approximation.docID()));
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
