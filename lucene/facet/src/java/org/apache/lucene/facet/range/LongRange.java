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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;

/** Represents a range over long values.
 *
 * @lucene.experimental */
public final class LongRange extends Range {
  final long minIncl;
  final long maxIncl;

  /** Minimum. */
  public final long min;

  /** Maximum. */
  public final long max;

  /** True if the minimum value is inclusive. */
  public final boolean minInclusive;

  /** True if the maximum value is inclusive. */
  public final boolean maxInclusive;

  // TODO: can we require fewer args? (same for
  // Double/FloatRange too)

  /** Create a LongRange. */
  public LongRange(String label, long minIn, boolean minInclusive, long maxIn, boolean maxInclusive) {
    super(label);
    this.min = minIn;
    this.max = maxIn;
    this.minInclusive = minInclusive;
    this.maxInclusive = maxInclusive;

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

    this.minIncl = minIn;
    this.maxIncl = maxIn;
  }

  /** True if this range accepts the provided value. */
  public boolean accept(long value) {
    return value >= minIncl && value <= maxIncl;
  }

  @Override
  public String toString() {
    return "LongRange(" + minIncl + " to " + maxIncl + ")";
  }

  @Override
  public Filter getFilter(final Filter fastMatchFilter, final ValueSource valueSource) {
    return new Filter() {

      @Override
      public String toString() {
        return "Filter(" + LongRange.this.toString() + ")";
      }

      @Override
      public DocIdSet getDocIdSet(AtomicReaderContext context, final Bits acceptDocs) throws IOException {

        // TODO: this is just like ValueSourceScorer,
        // ValueSourceFilter (spatial),
        // ValueSourceRangeFilter (solr); also,
        // https://issues.apache.org/jira/browse/LUCENE-4251

        final FunctionValues values = valueSource.getValues(Collections.emptyMap(), context);

        final int maxDoc = context.reader().maxDoc();

        final Bits fastMatchBits;
        if (fastMatchFilter != null) {
          DocIdSet dis = fastMatchFilter.getDocIdSet(context, null);
          if (dis == null) {
            // No documents match
            return null;
          }
          fastMatchBits = dis.bits();
          if (fastMatchBits == null) {
            throw new IllegalArgumentException("fastMatchFilter does not implement DocIdSet.bits");
          }
        } else {
          fastMatchBits = null;
        }

        return new DocIdSet() {

          @Override
          public Bits bits() {
            return new Bits() {
              @Override
              public boolean get(int docID) {
                if (acceptDocs != null && acceptDocs.get(docID) == false) {
                  return false;
                }
                if (fastMatchBits != null && fastMatchBits.get(docID) == false) {
                  return false;
                }
                return accept(values.longVal(docID));
              }

              @Override
              public int length() {
                return maxDoc;
              }
            };
          }

          @Override
          public DocIdSetIterator iterator() {
            throw new UnsupportedOperationException("this filter can only be accessed via bits()");
          }
        };
      }
    };
  }
}
