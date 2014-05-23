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

import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector.MatchingDocs;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.LongFieldSource;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.search.DocIdSetIterator;

/** {@link Facets} implementation that computes counts for
 *  dynamic long ranges from a provided {@link ValueSource},
 *  using {@link FunctionValues#longVal}.  Use
 *  this for dimensions that change in real-time (e.g. a
 *  relative time based dimension like "Past day", "Past 2
 *  days", etc.) or that change for each request (e.g. 
 *  distance from the user's location, "< 1 km", "< 2 km",
 *  etc.).
 *
 *  @lucene.experimental */
public class LongRangeFacetCounts extends RangeFacetCounts {

  /** Create {@code LongRangeFacetCounts}, using {@link
   *  LongFieldSource} from the specified field. */
  public LongRangeFacetCounts(String field, FacetsCollector hits, LongRange... ranges) throws IOException {
    this(field, new LongFieldSource(field), hits, ranges);
  }

  /** Create {@code RangeFacetCounts}, using the provided
   *  {@link ValueSource}. */
  public LongRangeFacetCounts(String field, ValueSource valueSource, FacetsCollector hits, LongRange... ranges) throws IOException {
    this(field, valueSource, hits, null, ranges);
  }

  /** Create {@code RangeFacetCounts}, using the provided
   *  {@link ValueSource}, and using the provided Filter as
   *  a fastmatch: only documents passing the filter are
   *  checked for the matching ranges.  The filter must be
   *  random access (implement {@link DocIdSet#bits}). */
  public LongRangeFacetCounts(String field, ValueSource valueSource, FacetsCollector hits, Filter fastMatchFilter, LongRange... ranges) throws IOException {
    super(field, ranges, fastMatchFilter);
    count(valueSource, hits.getMatchingDocs());
  }

  private void count(ValueSource valueSource, List<MatchingDocs> matchingDocs) throws IOException {

    LongRange[] ranges = (LongRange[]) this.ranges;

    LongRangeCounter counter = new LongRangeCounter(ranges);

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
          counter.add(fv.longVal(doc));
        } else {
          missingCount++;
        }
      }
    }
    
    int x = counter.fillCounts(counts);

    missingCount += x;

    //System.out.println("totCount " + totCount + " missingCount " + counter.missingCount);
    totCount -= missingCount;
  }
}
