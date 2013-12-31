package org.apache.lucene.facet;

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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.NumericRangeFilter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.NumericUtils;

/** Represents a range over double values. */
public final class DoubleRange extends Range {
  final double minIncl;
  final double maxIncl;

  public final double min;
  public final double max;
  public final boolean minInclusive;
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

  /** Returns a new {@link Filter} accepting only documents
   *  in this range.  Note that this filter is not
   *  efficient: it's a linear scan of all docs, testing
   *  each value.  If the {@link ValueSource} is static,
   *  e.g. an indexed numeric field, then it's more
   *  efficient to use {@link NumericRangeFilter}. */
  public Filter getFilter(final ValueSource valueSource) {
    return new Filter() {
      @Override
      public DocIdSet getDocIdSet(AtomicReaderContext context, final Bits acceptDocs) throws IOException {

        // TODO: this is just like ValueSourceScorer,
        // ValueSourceFilter (spatial),
        // ValueSourceRangeFilter (solr); also,
        // https://issues.apache.org/jira/browse/LUCENE-4251

        final FunctionValues values = valueSource.getValues(Collections.emptyMap(), context);

        final int maxDoc = context.reader().maxDoc();

        return new DocIdSet() {

          @Override
          public DocIdSetIterator iterator() {
            return new DocIdSetIterator() {
              int doc = -1;

              @Override
              public int nextDoc() throws IOException {
                while (true) {
                  doc++;
                  if (doc == maxDoc) {
                    return doc = NO_MORE_DOCS;
                  }
                  if (acceptDocs != null && acceptDocs.get(doc) == false) {
                    continue;
                  }
                  double v = values.doubleVal(doc);
                  if (accept(v)) {
                    return doc;
                  }
                }
              }

              @Override
              public int advance(int target) throws IOException {
                doc = target-1;
                return nextDoc();
              }

              @Override
              public int docID() {
                return doc;
              }

              @Override
              public long cost() {
                // Since we do a linear scan over all
                // documents, our cost is O(maxDoc):
                return maxDoc;
              }
            };
          }
        };
      }
    };
  }
}

