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

import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsCollector.MatchingDocs;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LongValues;
import org.apache.lucene.search.LongValuesSource;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;

/** {@link Facets} implementation that computes counts for
 *  dynamic long ranges from a provided {@link LongValuesSource}.  Use
 *  this for dimensions that change in real-time (e.g. a
 *  relative time based dimension like "Past day", "Past 2
 *  days", etc.) or that change for each request (e.g. 
 *  distance from the user's location, "&lt; 1 km", "&lt; 2 km",
 *  etc.).
 *
 *  @lucene.experimental */
public class LongRangeFacetCounts extends RangeFacetCounts {

  /** Create {@code LongRangeFacetCounts}, using {@link
   *  LongValuesSource} from the specified field. */
  public LongRangeFacetCounts(String field, FacetsCollector hits, LongRange... ranges) throws IOException {
    this(field, LongValuesSource.fromLongField(field), hits, ranges);
  }

  /** Create {@code LongRangeFacetCounts}, using the provided
   *  {@link LongValuesSource}. */
  public LongRangeFacetCounts(String field, LongValuesSource valueSource, FacetsCollector hits, LongRange... ranges) throws IOException {
    this(field, valueSource, hits, null, ranges);
  }

  /** Create {@code LongRangeFacetCounts}, using the provided
   *  {@link LongValuesSource}, and using the provided Filter as
   *  a fastmatch: only documents passing the filter are
   *  checked for the matching ranges, which is helpful when
   *  the provided {@link LongValuesSource} is costly per-document,
   *  such as a geo distance.  The filter must be
   *  random access (implement {@link DocIdSet#bits}). */
  public LongRangeFacetCounts(String field, LongValuesSource valueSource, FacetsCollector hits, Query fastMatchQuery, LongRange... ranges) throws IOException {
    super(field, ranges, fastMatchQuery);
    count(valueSource, hits.getMatchingDocs());
  }

  private void count(LongValuesSource valueSource, List<MatchingDocs> matchingDocs) throws IOException {

    LongRange[] ranges = (LongRange[]) this.ranges;

    LongRangeCounter counter = new LongRangeCounter(ranges);

    int missingCount = 0;
    for (MatchingDocs hits : matchingDocs) {
      LongValues fv = valueSource.getValues(hits.context, null);
      
      totCount += hits.totalHits;
      final DocIdSetIterator fastMatchDocs;
      if (fastMatchQuery != null) {
        final IndexReaderContext topLevelContext = ReaderUtil.getTopLevelContext(hits.context);
        final IndexSearcher searcher = new IndexSearcher(topLevelContext);
        searcher.setQueryCache(null);
        final Weight fastMatchWeight = searcher.createWeight(searcher.rewrite(fastMatchQuery), false, 1);
        Scorer s = fastMatchWeight.scorer(hits.context);
        if (s == null) {
          continue;
        }
        fastMatchDocs = s.iterator();
      } else {
        fastMatchDocs = null;
      }

      DocIdSetIterator docs = hits.bits.iterator();      
      for (int doc = docs.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; ) {
        if (fastMatchDocs != null) {
          int fastMatchDoc = fastMatchDocs.docID();
          if (fastMatchDoc < doc) {
            fastMatchDoc = fastMatchDocs.advance(doc);
          }

          if (doc != fastMatchDoc) {
            doc = docs.advance(fastMatchDoc);
            continue;
          }
        }
        // Skip missing docs:
        if (fv.advanceExact(doc)) {
          counter.add(fv.longValue());
        } else {
          missingCount++;
        }

        doc = docs.nextDoc();
      }
    }
    
    int x = counter.fillCounts(counts);

    missingCount += x;

    //System.out.println("totCount " + totCount + " x " + x + " missingCount " + missingCount);
    totCount -= missingCount;
  }
}
