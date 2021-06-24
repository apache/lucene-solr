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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ConjunctionDISI;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;

/** Base class for range faceting.
 *
 *  @lucene.experimental */
abstract class RangeFacetCounts extends Facets {
  /** Ranges passed to constructor. */
  protected final Range[] ranges;

  /** Counts, initialized in by subclass. */
  protected final int[] counts;

  /** Optional: if specified, we first test this Query to
   *  see whether the document should be checked for
   *  matching ranges.  If this is null, all documents are
   *  checked. */
  protected final Query fastMatchQuery;

  /** Our field name. */
  protected final String field;

  /** Total number of hits. */
  protected int totCount;

  /** Create {@code RangeFacetCounts} */
  protected RangeFacetCounts(String field, Range[] ranges, Query fastMatchQuery) throws IOException {
    this.field = field;
    this.ranges = ranges;
    this.fastMatchQuery = fastMatchQuery;
    counts = new int[ranges.length];
  }

  /**
   * Create a {@link org.apache.lucene.search.DocIdSetIterator} from the provided {@code hits} that
   * relies on {@code fastMatchQuery} if available for first-pass filtering. A null response
   * indicates no documents will match.
   */
  protected DocIdSetIterator createIterator(FacetsCollector.MatchingDocs hits) throws IOException {

    if (fastMatchQuery != null) {

      final IndexReaderContext topLevelContext = ReaderUtil.getTopLevelContext(hits.context);
      final IndexSearcher searcher = new IndexSearcher(topLevelContext);
      searcher.setQueryCache(null);
      final Weight fastMatchWeight =
          searcher.createWeight(searcher.rewrite(fastMatchQuery), ScoreMode.COMPLETE_NO_SCORES, 1);
      final Scorer s = fastMatchWeight.scorer(hits.context);
      if (s == null) {
        return null; // no hits from the fastMatchQuery; return null
      } else {
        DocIdSetIterator fastMatchDocs = s.iterator();
        return ConjunctionDISI.intersectIterators(Arrays.asList(hits.bits.iterator(), fastMatchDocs));
      }

    } else {
      return hits.bits.iterator();
    }
  }

  protected abstract LongRange[] getLongRanges();

  /** Allow sub-classes to (optionally) map from the stored long bits to a long that should be
   * used for the actual counting. Default behavior is a no-op.
   */
  protected long mapDocValue(long l) {
    return l;
  }

  /** Counts from the provided field. */
  protected void count(String field, List<FacetsCollector.MatchingDocs> matchingDocs)
      throws IOException {

    // load doc values for all segments up front and keep track of whether-or-not we found any that
    // were actually multi-valued. this allows us to optimize the case where all segments contain
    // single-values.
    SortedNumericDocValues[] multiValuedDocVals = new SortedNumericDocValues[matchingDocs.size()];
    NumericDocValues[] singleValuedDocVals = null;
    boolean foundMultiValued = false;

    for (int i = 0; i < matchingDocs.size(); i++) {

      FacetsCollector.MatchingDocs hits = matchingDocs.get(i);

      SortedNumericDocValues multiValues = DocValues.getSortedNumeric(hits.context.reader(), field);
      multiValuedDocVals[i] = multiValues;

      // only bother trying to unwrap a singleton if we haven't yet seen any true multi-valued cases
      if (foundMultiValued == false) {
        NumericDocValues singleValues = DocValues.unwrapSingleton(multiValues);
        if (singleValues != null) {
          if (singleValuedDocVals == null) {
            singleValuedDocVals = new NumericDocValues[matchingDocs.size()];
          }
          singleValuedDocVals[i] = singleValues;
        } else {
          foundMultiValued = true;
        }
      }
    }

    // we only need to keep around one or the other at this point
    if (foundMultiValued) {
      singleValuedDocVals = null;
    } else {
      multiValuedDocVals = null;
    }

    LongRangeCounter counter = LongRangeCounter.create(getLongRanges(), counts);

    int missingCount = 0;

    // if we didn't find any multi-valued cases, we can run a more optimal counting algorithm
    if (foundMultiValued == false) {

      for (int i = 0; i < matchingDocs.size(); i++) {

        FacetsCollector.MatchingDocs hits = matchingDocs.get(i);

        final DocIdSetIterator it = createIterator(hits);
        if (it == null) {
          continue;
        }

        assert singleValuedDocVals != null;
        NumericDocValues singleValues = singleValuedDocVals[i];

        totCount += hits.totalHits;
        for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; ) {
          if (singleValues.advanceExact(doc)) {
            counter.addSingleValued(mapDocValue(singleValues.longValue()));
          } else {
            missingCount++;
          }

          doc = it.nextDoc();
        }
      }
    } else {

      for (int i = 0; i < matchingDocs.size(); i++) {

        final DocIdSetIterator it = createIterator(matchingDocs.get(i));
        if (it == null) {
          continue;
        }

        SortedNumericDocValues multiValues = multiValuedDocVals[i];

        for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; ) {
          if (multiValues.advanceExact(doc)) {
            int limit = multiValues.docValueCount();
            // optimize single-value case
            if (limit == 1) {
              counter.addSingleValued(mapDocValue(multiValues.nextValue()));
              totCount++;
            } else {
              counter.startMultiValuedDoc();
              for (int j = 0; j < limit; j++) {
                counter.addMultiValued(mapDocValue(multiValues.nextValue()));
              }
              if (counter.endMultiValuedDoc()) {
                totCount++;
              }
            }
          }

          doc = it.nextDoc();
        }
      }
    }

    missingCount += counter.finish();
    totCount -= missingCount;
  }

  @Override
  public FacetResult getTopChildren(int topN, String dim, String... path) {
    if (dim.equals(field) == false) {
      throw new IllegalArgumentException("invalid dim \"" + dim + "\"; should be \"" + field + "\"");
    }
    if (path.length != 0) {
      throw new IllegalArgumentException("path.length should be 0");
    }
    LabelAndValue[] labelValues = new LabelAndValue[counts.length];
    for(int i=0;i<counts.length;i++) {
      labelValues[i] = new LabelAndValue(ranges[i].label, counts[i]);
    }
    return new FacetResult(dim, path, totCount, labelValues, labelValues.length);
  }

  @Override
  public Number getSpecificValue(String dim, String... path) throws IOException {
    // TODO: should we impl this?
    throw new UnsupportedOperationException();
  }

  @Override
  public List<FacetResult> getAllDims(int topN) throws IOException {
    return Collections.singletonList(getTopChildren(topN, field));
  }

  @Override
  public String toString() {
    StringBuilder b = new StringBuilder();
    b.append("RangeFacetCounts totCount=");
    b.append(totCount);
    b.append(":\n");
    for(int i=0;i<ranges.length;i++) {
      b.append("  ");
      b.append(ranges[i].label);
      b.append(" -> count=");
      b.append(counts[i]);
      b.append('\n');
    }
    return b.toString();
  }
}
