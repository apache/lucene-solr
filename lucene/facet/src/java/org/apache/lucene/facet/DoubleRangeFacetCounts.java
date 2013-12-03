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
import java.util.List;

import org.apache.lucene.document.DoubleDocValuesField; // javadocs
import org.apache.lucene.document.FloatDocValuesField; // javadocs
import org.apache.lucene.facet.FacetsCollector.MatchingDocs;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.DoubleFieldSource;
import org.apache.lucene.queries.function.valuesource.FloatFieldSource; // javadocs

/** {@link Facets} implementation that computes counts for
 *  dynamic double ranges from a provided {@link
 *  ValueSource}, using {@link FunctionValues#doubleVal}.  Use
 *  this for dimensions that change in real-time (e.g. a
 *  relative time based dimension like "Past day", "Past 2
 *  days", etc.) or that change for each user (e.g. a
 *  distance dimension like "< 1 km", "< 2 km", etc.).
 *
 *  <p> If you had indexed your field using {@link
 *  FloatDocValuesField} then pass {@link FloatFieldSource}
 *  as the {@link ValueSource}; if you used {@link
 *  DoubleDocValuesField} then pass {@link
 *  DoubleFieldSource} (this is the default used when you
 *  pass just a the field name).
 *
 *  @lucene.experimental */
public class DoubleRangeFacetCounts extends RangeFacetCounts {

  /** Create {@code RangeFacetCounts}, using {@link
   *  DoubleFieldSource} from the specified field. */
  public DoubleRangeFacetCounts(String field, FacetsCollector hits, DoubleRange... ranges) throws IOException {
    this(field, new DoubleFieldSource(field), hits, ranges);
  }

  /** Create {@code RangeFacetCounts}, using the provided
   *  {@link ValueSource}. */
  public DoubleRangeFacetCounts(String field, ValueSource valueSource, FacetsCollector hits, DoubleRange... ranges) throws IOException {
    super(field, ranges);
    count(valueSource, hits.getMatchingDocs());
  }

  private void count(ValueSource valueSource, List<MatchingDocs> matchingDocs) throws IOException {

    DoubleRange[] ranges = (DoubleRange[]) this.ranges;

    // Compute min & max over all ranges:
    double minIncl = Double.POSITIVE_INFINITY;
    double maxIncl = Double.NEGATIVE_INFINITY;
    for(DoubleRange range : ranges) {
      minIncl = Math.min(minIncl, range.minIncl);
      maxIncl = Math.max(maxIncl, range.maxIncl);
    }

    // TODO: test if this is faster (in the past it was
    // faster to do MatchingDocs on the inside) ... see
    // patches on LUCENE-4965):
    for (MatchingDocs hits : matchingDocs) {
      FunctionValues fv = valueSource.getValues(Collections.emptyMap(), hits.context);
      final int length = hits.bits.length();
      int doc = 0;
      totCount += hits.totalHits;
      while (doc < length && (doc = hits.bits.nextSetBit(doc)) != -1) {
        // Skip missing docs:
        if (fv.exists(doc)) {
          
          double v = fv.doubleVal(doc);
          if (v < minIncl || v > maxIncl) {
            doc++;
            continue;
          }

          // TODO: if all ranges are non-overlapping, we
          // should instead do a bin-search up front
          // (really, a specialized case of the interval
          // tree)
          // TODO: use interval tree instead of linear search:
          for (int j = 0; j < ranges.length; j++) {
            if (ranges[j].accept(v)) {
              counts[j]++;
            }
          }
        }

        doc++;
      }
    }
  }
}
