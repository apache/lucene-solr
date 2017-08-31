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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector.MatchingDocs;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.LabelAndValue;
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

/** {@link Facets} implementation that computes counts for
 *  all uniqute long values, more efficiently counting small values (0-1023) using an int array,
 *  and switching to a <code>HashMap</code> for values above 1023.
 *  Retrieve all facet counts, in value order, with {@link #getAllChildrenSortByValue},
 *  or get the topN values sorted by count with {@link #getTopChildrenSortByCount}.
 *
 *  @lucene.experimental */
public class LongValueFacetCounts extends Facets {

  /** Used for all values that are < 1K. */
  private final int[] counts = new int[1024];

  /** Used for all values that are >= 1K. */
  private final HashTable hashCounts = new HashTable();

  private final String field;

  /** Total number of values counted, which is the subset of hits that had a value for this field. */
  private int totCount;

  /** Create {@code LongValueFacetCounts}, using either single-valued {@link
   *  NumericDocValues} or multi-valued {@link SortedNumericDocValues} from the
   *  specified field. */
  public LongValueFacetCounts(String field, FacetsCollector hits, boolean multiValued) throws IOException {
    this(field, null, hits, multiValued);
  }

  /** Create {@code LongValueFacetCounts}, using the provided
   *  {@link org.apache.lucene.queries.function.ValueSource}.  If hits is
   *  null then all facets are counted. */
  public LongValueFacetCounts(String field, LongValuesSource valueSource, FacetsCollector hits) throws IOException {
    this(field, valueSource, hits, false);
  }

  /** Create {@code LongValueFacetCounts}, using the provided
   *  {@link org.apache.lucene.queries.function.ValueSource}.
   *  random access (implement {@link org.apache.lucene.search.DocIdSet#bits}). */
  public LongValueFacetCounts(String field, LongValuesSource valueSource, FacetsCollector hits,
                              boolean multiValued) throws IOException {
    this.field = field;
    if (valueSource == null) {
      if (multiValued) {
        countMultiValued(field, hits.getMatchingDocs());
      } else {
        count(field, hits.getMatchingDocs());
      }
    } else {
      // value source is always single valued
      if (multiValued) {
        throw new IllegalArgumentException("can only compute multi-valued facets directly from doc values (when valueSource is null)");
      }
      count(valueSource, hits.getMatchingDocs());
    }
  }

  /** Counts all facet values for this reader.  This produces the same result as computing
   *  facets on a {@link org.apache.lucene.search.MatchAllDocsQuery}, but is more efficient. */
  public LongValueFacetCounts(String field, IndexReader reader, boolean multiValued) throws IOException {
    this.field = field;
    if (multiValued) {
      countAllMultiValued(reader, field);
    } else {
      countAll(reader, field);
    }
  }

  /** Counts all facet values for the provided {@link LongValuesSource}.  This produces the same result as computing
   *  facets on a {@link org.apache.lucene.search.MatchAllDocsQuery}, but is more efficient. */
  public LongValueFacetCounts(String field, LongValuesSource valueSource, IndexReader reader) throws IOException {
    this.field = field;
    countAll(valueSource, field, reader);
  }

  private void count(LongValuesSource valueSource, List<MatchingDocs> matchingDocs) throws IOException {

    for (MatchingDocs hits : matchingDocs) {
      LongValues fv = valueSource.getValues(hits.context, null);

      // NOTE: this is not as efficient as working directly with the doc values APIs in the sparse case
      // because we are doing a linear scan across all hits, but this API is more flexible since a
      // LongValuesSource can compute interesting values at query time

      DocIdSetIterator docs = hits.bits.iterator();
      for (int doc = docs.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS;) {
        // Skip missing docs:
        if (fv.advanceExact(doc)) {
          increment(fv.longValue());
          totCount++;
        }

        doc = docs.nextDoc();
      }
    }
  }

  private void count(String field, List<MatchingDocs> matchingDocs) throws IOException {
    for (MatchingDocs hits : matchingDocs) {
      NumericDocValues fv = hits.context.reader().getNumericDocValues(field);
      if (fv == null) {
        continue;
      }
      countOneSegment(fv, hits);
    }
  }

  private void countOneSegment(NumericDocValues values, MatchingDocs hits) throws IOException {
    DocIdSetIterator it = ConjunctionDISI.intersectIterators(
                             Arrays.asList(hits.bits.iterator(), values));

    for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
      increment(values.longValue());
      totCount++;
    }
  }

  /** Counts directly from SortedNumericDocValues. */
  private void countMultiValued(String field, List<MatchingDocs> matchingDocs) throws IOException {

    for (MatchingDocs hits : matchingDocs) {
      SortedNumericDocValues values = hits.context.reader().getSortedNumericDocValues(field);
      if (values == null) {
        // this field has no doc values for this segment
        continue;
      }

      NumericDocValues singleValues = DocValues.unwrapSingleton(values);

      if (singleValues != null) {
        countOneSegment(singleValues, hits);
      } else {

        DocIdSetIterator it = ConjunctionDISI.intersectIterators(
                                 Arrays.asList(hits.bits.iterator(), values));
      
        for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
          int limit = values.docValueCount();
          totCount += limit;
          for (int i = 0; i < limit; i++) {
            increment(values.nextValue());
          }
        }
      }
    }
  }

  /** Optimized version that directly counts all doc values. */
  private void countAll(IndexReader reader, String field) throws IOException {

    for (LeafReaderContext context : reader.leaves()) {

      NumericDocValues values = context.reader().getNumericDocValues(field);
      if (values == null) {
        // this field has no doc values for this segment
        continue;
      }

      countAllOneSegment(values);
    }
  }

  private void countAllOneSegment(NumericDocValues values) throws IOException {
    int doc;
    while ((doc = values.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      totCount++;
      increment(values.longValue());
    }
  }

  private void countAll(LongValuesSource valueSource, String field, IndexReader reader) throws IOException {

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
  
  private void countAllMultiValued(IndexReader reader, String field) throws IOException {

    for (LeafReaderContext context : reader.leaves()) {

      SortedNumericDocValues values = context.reader().getSortedNumericDocValues(field);
      if (values == null) {
        // this field has no doc values for this segment
        continue;
      }
      NumericDocValues singleValues = DocValues.unwrapSingleton(values);
      if (singleValues != null) {
        countAllOneSegment(singleValues);
      } else {
        int doc;
        while ((doc = values.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
          int limit = values.docValueCount();
          totCount += limit;
          for (int i = 0; i < limit; i++) {
            increment(values.nextValue());
          }
        }
      }
    }
  }

  private void increment(long value) {
    if (value >= 0 && value < counts.length) {
      counts[(int) value]++;
    } else {
      hashCounts.add(value, 1);
    }
  }

  @Override
  public FacetResult getTopChildren(int topN, String dim, String... path) {
    if (dim.equals(field) == false) {
      throw new IllegalArgumentException("invalid dim \"" + dim + "\"; should be \"" + field + "\"");
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
    PriorityQueue<Entry> pq = new PriorityQueue<Entry>(Math.min(topN, counts.length + hashCounts.size)) {
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

    if (hashCounts.size != 0) {
      childCount += hashCounts.size;
      for (int i = 0; i < hashCounts.values.length; i++) {
        int count = hashCounts.counts[i];
        if (count != 0) {
          if (e == null) {
            e = new Entry();
          }
          e.value = hashCounts.values[i];
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


  /** Returns all unique values seen, sorted by value.  */
  public FacetResult getAllChildrenSortByValue() {
    List<LabelAndValue> labelValues = new ArrayList<>();

    // compact & sort hash table's arrays by value
    int upto = 0;
    for (int i = 0; i < hashCounts.values.length; i++) {
      if (hashCounts.counts[i] != 0) {
        hashCounts.counts[upto] = hashCounts.counts[i];
        hashCounts.values[upto] = hashCounts.values[i];
        upto++;
      }
    }

    // zero fill all remaining counts so if we are called again we don't mistake these as real values
    Arrays.fill(hashCounts.counts, upto, hashCounts.counts.length, 0);

    assert upto == hashCounts.size : "upto=" + upto + " hashCounts.size=" + hashCounts.size;

    new InPlaceMergeSorter() {
      @Override
      public int compare(int i, int j) {
        return Long.compare(hashCounts.values[i], hashCounts.values[j]);
      }

      @Override
      public void swap(int i, int j) {
        int x = hashCounts.counts[i];
        hashCounts.counts[i] = hashCounts.counts[j];
        hashCounts.counts[j] = x;

        long y = hashCounts.values[j];
        hashCounts.values[j] = hashCounts.values[i];
        hashCounts.values[i] = y;
      }
    }.sort(0, upto);

    boolean countsAdded = false;
    for (int i = 0; i < upto; i++) {
      if (countsAdded == false && hashCounts.values[i] >= counts.length) {
        countsAdded = true;
        appendCounts(labelValues);
      }

      labelValues.add(new LabelAndValue(Long.toString(hashCounts.values[i]),
                                        hashCounts.counts[i]));
    }

    if (countsAdded == false) {
      appendCounts(labelValues);
    }

    return new FacetResult(field, new String[0], totCount, labelValues.toArray(new LabelAndValue[0]), labelValues.size());
  }

  private void appendCounts(List<LabelAndValue> labelValues) {
    for (int i = 0; i < counts.length; i++) {
      if (counts[i] != 0) {
        labelValues.add(new LabelAndValue(Long.toString(i), counts[i]));
      }
    }
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

    if (hashCounts.size != 0) {
      for (int i = 0; i < hashCounts.values.length; i++) {
        if (hashCounts.counts[i] != 0) {
          b.append("  ");
          b.append(hashCounts.values[i]);
          b.append(" -> count=");
          b.append(hashCounts.counts[i]);
          b.append('\n');
        }
      }
    }

    return b.toString();
  }

  /** Native typed hash table. */
  static class HashTable {

    static final float LOAD_FACTOR = 0.7f;

    long[] values; // values identifying a value
    int[] counts;
    int mask;
    int size;
    int threshold;

    HashTable() {
      int capacity = 64; // must be a power of 2
      values = new long[capacity];
      counts = new int[capacity];
      mask = capacity - 1;
      size = 0;
      threshold = (int) (capacity * LOAD_FACTOR);
    }

    private int hash(long v) {
      int h = (int) (v ^ (v >>> 32));
      h = (31 * h) & mask; // * 31 to try to use the whole table, even if values are dense
      return h;
    }

    void add(long value, int inc) {
      if (size >= threshold) {
        rehash();
      }
      final int h = hash(value);
      for (int slot = h;; slot = (slot + 1) & mask) {
        if (counts[slot] == 0) {
          values[slot] = value;
          ++size;
        } else if (values[slot] != value) {
          continue;
        }
        counts[slot] += inc;
        break;
      }
    }

    private void rehash() {
      final long[] oldValues = values;
      final int[] oldCounts = counts;

      final int newCapacity = values.length * 2;
      values = new long[newCapacity];
      counts = new int[newCapacity];
      mask = newCapacity - 1;
      threshold = (int) (LOAD_FACTOR * newCapacity);
      size = 0;

      for (int i = 0; i < oldValues.length; ++i) {
        if (oldCounts[i] > 0) {
          add(oldValues[i], oldCounts[i]);
        }
      }
    }
  }
}
