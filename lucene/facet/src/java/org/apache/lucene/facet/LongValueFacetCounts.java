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

package org.apache.lucene.facet;

import com.carrotsearch.hppc.LongIntScatterMap;
import com.carrotsearch.hppc.cursors.LongIntCursor;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.facet.FacetsCollector.MatchingDocs;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ConjunctionDISI;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LongValues;
import org.apache.lucene.search.LongValuesSource;
import org.apache.lucene.util.InPlaceMergeSorter;
import org.apache.lucene.util.PriorityQueue;

/**
 * {@link Facets} implementation that computes counts for all unique long values, more efficiently
 * counting small values (0-1023) using an int array, and switching to a <code>HashMap</code> for
 * values above 1023. Retrieve all facet counts, in value order, with {@link
 * #getAllChildrenSortByValue}, or get the topN values sorted by count with {@link
 * #getTopChildrenSortByCount}.
 *
 * @lucene.experimental
 */
public class LongValueFacetCounts extends Facets {

  /** Used for all values that are < 1K. */
  private final int[] counts = new int[1024];

  /** Used for all values that are >= 1K. */
  private final LongIntScatterMap hashCounts = new LongIntScatterMap();

  /** Field being counted. */
  private final String field;

  /**
   * Total value count. For single-value cases, this is the subset of hits that had a value for this
   * field.
   */
  private int totCount;

  /**
   * Create {@code LongValueFacetCounts}, using either single-valued {@link NumericDocValues} or
   * multi-valued {@link SortedNumericDocValues} from the specified field (depending on what has
   * been indexed).
   */
  public LongValueFacetCounts(String field, FacetsCollector hits) throws IOException {
    this(field, null, hits);
  }

  /**
   * Create {@code LongValueFacetCounts}, using the provided {@link LongValuesSource} if non-null.
   * If {@code valueSource} is null, doc values from the provided {@code field} will be used.
   */
  public LongValueFacetCounts(String field, LongValuesSource valueSource, FacetsCollector hits)
      throws IOException {
    this.field = field;
    if (valueSource != null) {
      count(valueSource, hits.getMatchingDocs());
    } else {
      count(field, hits.getMatchingDocs());
    }
  }

  /**
   * Counts all facet values for this reader. This produces the same result as computing facets on a
   * {@link org.apache.lucene.search.MatchAllDocsQuery}, but is more efficient.
   */
  public LongValueFacetCounts(String field, IndexReader reader) throws IOException {
    this(field, null, reader);
  }

  /**
   * Counts all facet values for the provided {@link LongValuesSource} if non-null. If {@code
   * valueSource} is null, doc values from the provided {@code field} will be used. This produces
   * the same result as computing facets on a {@link org.apache.lucene.search.MatchAllDocsQuery},
   * but is more efficient.
   */
  public LongValueFacetCounts(String field, LongValuesSource valueSource, IndexReader reader)
      throws IOException {
    this.field = field;
    if (valueSource != null) {
      countAll(reader, valueSource);
    } else {
      countAll(reader, field);
    }
  }

  /** Counts from the provided valueSource. */
  private void count(LongValuesSource valueSource, List<MatchingDocs> matchingDocs)
      throws IOException {

    for (MatchingDocs hits : matchingDocs) {

      LongValues fv = valueSource.getValues(hits.context, null);

      // NOTE: this is not as efficient as working directly with the doc values APIs in the sparse
      // case
      // because we are doing a linear scan across all hits, but this API is more flexible since a
      // LongValuesSource can compute interesting values at query time

      DocIdSetIterator docs = hits.bits.iterator();
      for (int doc = docs.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; ) {
        // Skip missing docs:
        if (fv.advanceExact(doc)) {
          increment(fv.longValue());
          totCount++;
        }

        doc = docs.nextDoc();
      }
    }
  }

  /** Counts from the field's indexed doc values. */
  private void count(String field, List<MatchingDocs> matchingDocs) throws IOException {

    for (MatchingDocs hits : matchingDocs) {

      SortedNumericDocValues multiValues = DocValues.getSortedNumeric(hits.context.reader(), field);
      NumericDocValues singleValues = DocValues.unwrapSingleton(multiValues);

      if (singleValues != null) {

        DocIdSetIterator it =
            ConjunctionDISI.intersectIterators(Arrays.asList(hits.bits.iterator(), singleValues));

        for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
          increment(singleValues.longValue());
          totCount++;
        }
      } else {

        DocIdSetIterator it =
            ConjunctionDISI.intersectIterators(Arrays.asList(hits.bits.iterator(), multiValues));

        for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
          int limit = multiValues.docValueCount();
          totCount += limit;
          for (int i = 0; i < limit; i++) {
            increment(multiValues.nextValue());
          }
        }
      }
    }
  }

  /** Count everything in the provided valueSource. */
  private void countAll(IndexReader reader, LongValuesSource valueSource) throws IOException {

    for (LeafReaderContext context : reader.leaves()) {
      LongValues fv = valueSource.getValues(context, null);
      int maxDoc = context.reader().maxDoc();

      for (int doc = 0; doc < maxDoc; doc++) {
        // Skip missing docs:
        if (fv.advanceExact(doc)) {
          increment(fv.longValue());
          totCount++;
        }
      }
    }
  }

  /** Count everything in the specified field. */
  private void countAll(IndexReader reader, String field) throws IOException {

    for (LeafReaderContext context : reader.leaves()) {

      SortedNumericDocValues multiValues = DocValues.getSortedNumeric(context.reader(), field);
      NumericDocValues singleValues = DocValues.unwrapSingleton(multiValues);

      if (singleValues != null) {

        while (singleValues.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
          totCount++;
          increment(singleValues.longValue());
        }
      } else {

        while (multiValues.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
          int limit = multiValues.docValueCount();
          totCount += limit;
          for (int i = 0; i < limit; i++) {
            increment(multiValues.nextValue());
          }
        }
      }
    }
  }

  private void increment(long value) {
    if (value >= 0 && value < counts.length) {
      counts[(int) value]++;
    } else {
      hashCounts.addTo(value, 1);
    }
  }

  @Override
  public FacetResult getTopChildren(int topN, String dim, String... path) {
    if (dim.equals(field) == false) {
      throw new IllegalArgumentException(
          "invalid dim \"" + dim + "\"; should be \"" + field + "\"");
    }
    if (path.length != 0) {
      throw new IllegalArgumentException("path.length should be 0");
    }
    return getTopChildrenSortByCount(topN);
  }

  /** Reusable hash entry to hold long facet value and int count. */
  private static class Entry {
    int count;
    long value;
  }

  /** Returns the specified top number of facets, sorted by count. */
  public FacetResult getTopChildrenSortByCount(int topN) {
    PriorityQueue<Entry> pq =
        new PriorityQueue<>(Math.min(topN, counts.length + hashCounts.size())) {
          @Override
          protected boolean lessThan(Entry a, Entry b) {
            // sort by count descending, breaking ties by value ascending:
            return a.count < b.count || (a.count == b.count && a.value > b.value);
          }
        };

    int childCount = 0;
    Entry e = null;
    for (int i = 0; i < counts.length; i++) {
      if (counts[i] != 0) {
        childCount++;
        if (e == null) {
          e = new Entry();
        }
        e.value = i;
        e.count = counts[i];
        e = pq.insertWithOverflow(e);
      }
    }

    if (hashCounts.size() != 0) {
      childCount += hashCounts.size();
      for (LongIntCursor c : hashCounts) {
        int count = c.value;
        if (count != 0) {
          if (e == null) {
            e = new Entry();
          }
          e.value = c.key;
          e.count = count;
          e = pq.insertWithOverflow(e);
        }
      }
    }

    LabelAndValue[] results = new LabelAndValue[pq.size()];
    while (pq.size() != 0) {
      Entry entry = pq.pop();
      results[pq.size()] = new LabelAndValue(Long.toString(entry.value), entry.count);
    }

    return new FacetResult(field, new String[0], totCount, results, childCount);
  }

  /** Returns all unique values seen, sorted by value. */
  public FacetResult getAllChildrenSortByValue() {
    List<LabelAndValue> labelValues = new ArrayList<>();

    // compact & sort hash table's arrays by value
    int[] hashCounts = new int[this.hashCounts.size()];
    long[] hashValues = new long[this.hashCounts.size()];

    int upto = 0;
    for (LongIntCursor c : this.hashCounts) {
      if (c.value != 0) {
        hashCounts[upto] = c.value;
        hashValues[upto] = c.key;
        upto++;
      }
    }

    assert upto == this.hashCounts.size()
        : "upto=" + upto + " hashCounts.size=" + this.hashCounts.size();

    new InPlaceMergeSorter() {
      @Override
      public int compare(int i, int j) {
        return Long.compare(hashValues[i], hashValues[j]);
      }

      @Override
      public void swap(int i, int j) {
        int x = hashCounts[i];
        hashCounts[i] = hashCounts[j];
        hashCounts[j] = x;

        long y = hashValues[j];
        hashValues[j] = hashValues[i];
        hashValues[i] = y;
      }
    }.sort(0, upto);

    boolean countsAdded = false;
    for (int i = 0; i < upto; i++) {
      if (countsAdded == false && hashValues[i] >= counts.length) {
        countsAdded = true;
        appendCounts(labelValues);
      }

      labelValues.add(new LabelAndValue(Long.toString(hashValues[i]), hashCounts[i]));
    }

    if (countsAdded == false) {
      appendCounts(labelValues);
    }

    return new FacetResult(
        field,
        new String[0],
        totCount,
        labelValues.toArray(new LabelAndValue[0]),
        labelValues.size());
  }

  private void appendCounts(List<LabelAndValue> labelValues) {
    for (int i = 0; i < counts.length; i++) {
      if (counts[i] != 0) {
        labelValues.add(new LabelAndValue(Long.toString(i), counts[i]));
      }
    }
  }

  @Override
  public Number getSpecificValue(String dim, String... path) {
    // TODO: should we impl this?
    throw new UnsupportedOperationException();
  }

  @Override
  public List<FacetResult> getAllDims(int topN) {
    return Collections.singletonList(getTopChildren(topN, field));
  }

  @Override
  public String toString() {
    StringBuilder b = new StringBuilder();
    b.append("LongValueFacetCounts totCount=");
    b.append(totCount);
    b.append(":\n");
    for (int i = 0; i < counts.length; i++) {
      if (counts[i] != 0) {
        b.append("  ");
        b.append(i);
        b.append(" -> count=");
        b.append(counts[i]);
        b.append('\n');
      }
    }

    if (hashCounts.size() != 0) {
      for (LongIntCursor c : hashCounts) {
        if (c.value != 0) {
          b.append("  ");
          b.append(c.key);
          b.append(" -> count=");
          b.append(c.value);
          b.append('\n');
        }
      }
    }

    return b.toString();
  }
}
