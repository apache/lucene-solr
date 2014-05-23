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
import java.util.List;

import org.apache.lucene.document.DoubleDocValuesField; // javadocs
import org.apache.lucene.document.FloatDocValuesField; // javadocs
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector.MatchingDocs;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.DoubleFieldSource;
import org.apache.lucene.queries.function.valuesource.FloatFieldSource; // javadocs
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.NumericUtils;

/** {@link Facets} implementation that computes counts for
 *  dynamic double ranges from a provided {@link
 *  ValueSource}, using {@link FunctionValues#doubleVal}.  Use
 *  this for dimensions that change in real-time (e.g. a
 *  relative time based dimension like "Past day", "Past 2
 *  days", etc.) or that change for each request (e.g.
 *  distance from the user's location, "< 1 km", "< 2 km",
 *  etc.).
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
    this(field, valueSource, hits, null, ranges);
  }

  /** Create {@code RangeFacetCounts}, using the provided
   *  {@link ValueSource}, and using the provided Filter as
   *  a fastmatch: only documents passing the filter are
   *  checked for the matching ranges.  The filter must be
   *  random access (implement {@link DocIdSet#bits}). */
  public DoubleRangeFacetCounts(String field, ValueSource valueSource, FacetsCollector hits, Filter fastMatchFilter, DoubleRange... ranges) throws IOException {
    super(field, ranges, fastMatchFilter);
    count(valueSource, hits.getMatchingDocs());
  }

  private void count(ValueSource valueSource, List<MatchingDocs> matchingDocs) throws IOException {

    DoubleRange[] ranges = (DoubleRange[]) this.ranges;

    LongRange[] longRanges = new LongRange[ranges.length];
    for(int i=0;i<ranges.length;i++) {
      DoubleRange range = ranges[i];
      longRanges[i] =  new LongRange(range.label,
                                     NumericUtils.doubleToSortableLong(range.minIncl), true,
                                     NumericUtils.doubleToSortableLong(range.maxIncl), true);
    }

    LongRangeCounter counter = new LongRangeCounter(longRanges);

    int missingCount = 0;
    for (MatchingDocs hits : matchingDocs) {
      FunctionValues fv = valueSource.getValues(Collections.emptyMap(), hits.context);
      
      totCount += hits.totalHits;
      Bits bits;
      if (fastMatchFilter != null) {
        DocIdSet dis = fastMatchFilter.getDocIdSet(hits.context, null);
        if (dis == null) {
          // No documents match
          continue;
        }
        bits = dis.bits();
        if (bits == null) {
          throw new IllegalArgumentException("fastMatchFilter does not implement DocIdSet.bits");
        }
      } else {
        bits = null;
      }

      DocIdSetIterator docs = hits.bits.iterator();
      
      int doc;
      while ((doc = docs.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        if (bits != null && bits.get(doc) == false) {
          doc++;
          continue;
        }
        // Skip missing docs:
        if (fv.exists(doc)) {
          counter.add(NumericUtils.doubleToSortableLong(fv.doubleVal(doc)));
        } else {
          missingCount++;
        }
      }
    }

    missingCount += counter.fillCounts(counts);
    totCount -= missingCount;
  }
}
