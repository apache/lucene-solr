package org.apache.lucene.facet.range;

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
import java.util.Collections;
import java.util.Objects;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredDocIdSet;
import org.apache.lucene.util.Bits;
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

  private static class ValueSourceFilter extends Filter {
    private final DoubleRange range;
    private final Filter fastMatchFilter;
    private final ValueSource valueSource;

    ValueSourceFilter(DoubleRange range, Filter fastMatchFilter, ValueSource valueSource) {
      super(true);
      this.range = range;
      this.fastMatchFilter = fastMatchFilter;
      this.valueSource = valueSource;
    }

    @Override
    public boolean equals(Object obj) {
      if (super.equals(obj) == false) {
        return false;
      }
      ValueSourceFilter other = (ValueSourceFilter) obj;
      return range.equals(other.range)
          && Objects.equals(fastMatchFilter, other.fastMatchFilter)
          && valueSource.equals(other.valueSource);
    }

    @Override
    public int hashCode() {
      return 31 * Objects.hash(range, fastMatchFilter, valueSource) + super.hashCode();
    }

    @Override
    public String toString(String field) {
      return "Filter(" + range.toString() + ")";
    }

    @Override
    public DocIdSet getDocIdSet(LeafReaderContext context, final Bits acceptDocs) throws IOException {

      // TODO: this is just like ValueSourceScorer,
      // ValueSourceFilter (spatial),
      // ValueSourceRangeFilter (solr); also,
      // https://issues.apache.org/jira/browse/LUCENE-4251

      final FunctionValues values = valueSource.getValues(Collections.emptyMap(), context);

      final int maxDoc = context.reader().maxDoc();

      final DocIdSet fastMatchDocs;
      if (fastMatchFilter != null) {
        fastMatchDocs = fastMatchFilter.getDocIdSet(context, null);
        if (fastMatchDocs == null) {
          // No documents match
          return null;
        }
      } else {
        fastMatchDocs = new DocIdSet() {
          @Override
          public long ramBytesUsed() {
            return 0;
          }
          @Override
          public DocIdSetIterator iterator() throws IOException {
            return DocIdSetIterator.all(maxDoc);
          }
        };
      }

      return new FilteredDocIdSet(fastMatchDocs) {
        @Override
        protected boolean match(int docID) {
          if (acceptDocs != null && acceptDocs.get(docID) == false) {
            return false;
          }
          return range.accept(values.doubleVal(docID));
        }
      };
    }
  }

  @Override
  public Filter getFilter(final Filter fastMatchFilter, final ValueSource valueSource) {
    return new ValueSourceFilter(this, fastMatchFilter, valueSource);
  }
}

