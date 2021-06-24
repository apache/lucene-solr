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
import java.util.List;

import org.apache.lucene.document.FloatDocValuesField;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsCollector.MatchingDocs;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.NumericUtils;

/**
 * {@link Facets} implementation that computes counts for dynamic double ranges. Use this for
 * dimensions that change in real-time (e.g. a relative time based dimension like "Past day", "Past
 * 2 days", etc.) or that change for each request (e.g. distance from the user's location, "&lt; 1
 * km", "&lt; 2 km", etc.).
 *
 * <p>If you have indexed your field using {@link FloatDocValuesField}, then you should use a
 * DoubleValuesSource generated from {@link DoubleValuesSource#fromFloatField(String)}.
 *
 *  @lucene.experimental */
public class DoubleRangeFacetCounts extends RangeFacetCounts {

  /**
   * Create {@code RangeFacetCounts}, using double value from the specified field. The field may be
   * single-valued ({@link NumericDocValues}) or multi-valued ({@link SortedNumericDocValues}), and
   * will be interpreted as containing double values.
   *
   * <p>N.B This assumes that the field was indexed with {@link
   * org.apache.lucene.document.DoubleDocValuesField}. For float-valued fields, use {@link
   * #DoubleRangeFacetCounts(String, DoubleValuesSource, FacetsCollector, DoubleRange...)}
   *
   * <p>TODO: Extend multi-valued support to fields that have been indexed as float values
   */
  public DoubleRangeFacetCounts(String field, FacetsCollector hits, DoubleRange... ranges) throws IOException {
    this(field, null, hits, ranges);
  }

  /**
   * Create {@code RangeFacetCounts}, using the provided {@link DoubleValuesSource} if non-null. If
   * {@code valueSource} is null, doc values from the provided {@code field} will be used.
   *
   * <p>N.B If relying on the provided {@code field}, see javadoc notes associated with {@link
   * #DoubleRangeFacetCounts(String, FacetsCollector, DoubleRange...)} for assumptions on how the
   * field is indexed.
   */
  public DoubleRangeFacetCounts(String field, DoubleValuesSource valueSource, FacetsCollector hits, DoubleRange... ranges) throws IOException {
    this(field, valueSource, hits, null, ranges);
  }

  /**
   * Create {@code RangeFacetCounts}, using the provided {@link DoubleValuesSource} if non-null. If
   * {@code valueSource} is null, doc values from the provided {@code field} will be used. Use the
   * provided {@code Query} as a fastmatch: only documents matching the query are checked for the
   * matching ranges.
   *
   * <p>N.B If relying on the provided {@code field}, see javadoc notes associated with {@link
   * #DoubleRangeFacetCounts(String, FacetsCollector, DoubleRange...)} for assumptions on how the
   * field is indexed.
   */
 public DoubleRangeFacetCounts(String field, DoubleValuesSource valueSource, FacetsCollector hits, Query fastMatchQuery, DoubleRange... ranges) throws IOException {
    super(field, ranges, fastMatchQuery);
    // use the provided valueSource if non-null, otherwise use the doc values associated with the
    // field
    if (valueSource != null) {
      count(valueSource, hits.getMatchingDocs());
    } else {
      count(field, hits.getMatchingDocs());
    }
  }

  /** Counts from the provided valueSource. */
  private void count(DoubleValuesSource valueSource, List<MatchingDocs> matchingDocs) throws IOException {

    LongRange[] longRanges = getLongRanges();

    LongRangeCounter counter = LongRangeCounter.create(longRanges, counts);

    int missingCount = 0;
    for (MatchingDocs hits : matchingDocs) {
      DoubleValues fv = valueSource.getValues(hits.context, null);
      
      totCount += hits.totalHits;

      final DocIdSetIterator it = createIterator(hits);
      if (it == null) {
        continue;
      }

      for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; ) {
        // Skip missing docs:
        if (fv.advanceExact(doc)) {
          counter.addSingleValued(NumericUtils.doubleToSortableLong(fv.doubleValue()));
        } else {
          missingCount++;
        }

        doc = it.nextDoc();
      }
    }

    missingCount += counter.finish();
    totCount -= missingCount;
  }

  /** Create long ranges from the double ranges. */
  @Override
  protected LongRange[] getLongRanges() {
    DoubleRange[] ranges = (DoubleRange[]) this.ranges;
    LongRange[] longRanges = new LongRange[ranges.length];
    for (int i = 0; i < ranges.length; i++) {
      DoubleRange range = ranges[i];
      longRanges[i] =
          new LongRange(
              range.label,
              NumericUtils.doubleToSortableLong(range.min),
              true,
              NumericUtils.doubleToSortableLong(range.max),
              true);
    }

    return longRanges;
  }

  /** Map the stored bits to the
   * {@link org.apache.lucene.util.NumericUtils#sortableDoubleBits(long)} representation for counting. */
  @Override
  protected long mapDocValue(long l) {
    return NumericUtils.sortableDoubleBits(l);
  }
}
