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
package org.apache.lucene.search;

import java.io.IOException;
import java.util.Objects;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;

/**
 * A range query that can take advantage of the fact that the index is sorted to speed up
 * execution. If the index is sorted on the same field as the query, it performs binary
 * search on the field's numeric doc values to find the documents at the lower and upper
 * ends of the range.
 *
 * This optimized execution strategy is only used if the following conditions hold:
 * <ul>
 *   <li> The index is sorted, and its primary sort is on the same field as the query.
 *   <li> The query field has either {@link SortedNumericDocValues} or {@link NumericDocValues}.
 *   <li> The segments must have at most one field value per document (otherwise we cannot easily
 * determine the matching document IDs through a binary search).
 * </ul>
 *
 * If any of these conditions isn't met, the search is delegated to {@code fallbackQuery}.
 *
 * This fallback must be an equivalent range query -- it should produce the same documents and give
 * constant scores. As an example, an {@link IndexSortSortedNumericDocValuesRangeQuery} might be
 * constructed as follows:
 * <pre class="prettyprint">
 *   String field = "field";
 *   long lowerValue = 0, long upperValue = 10;
 *   Query fallbackQuery = LongPoint.newRangeQuery(field, lowerValue, upperValue);
 *   Query rangeQuery = new IndexSortSortedNumericDocValuesRangeQuery(
 *       field, lowerValue, upperValue, fallbackQuery);
 * </pre>
 *
 * @lucene.experimental
 */
public class IndexSortSortedNumericDocValuesRangeQuery extends Query {

  private final String field;
  private final long lowerValue;
  private final long upperValue;
  private final Query fallbackQuery;

  /**
   * Creates a new {@link IndexSortSortedNumericDocValuesRangeQuery}.
   *
   * @param field The field name.
   * @param lowerValue The lower end of the range (inclusive).
   * @param upperValue The upper end of the range (exclusive).
   * @param fallbackQuery A query to fall back to if the optimization cannot be applied.
      */
  public IndexSortSortedNumericDocValuesRangeQuery(String field,
                                                   long lowerValue,
                                                   long upperValue,
                                                   Query fallbackQuery) {
    this.field = Objects.requireNonNull(field);
    this.lowerValue = lowerValue;
    this.upperValue = upperValue;
    this.fallbackQuery = fallbackQuery;
  }

  public Query getFallbackQuery() {
    return fallbackQuery;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    IndexSortSortedNumericDocValuesRangeQuery that = (IndexSortSortedNumericDocValuesRangeQuery) o;
    return lowerValue == that.lowerValue &&
        upperValue == that.upperValue &&
        Objects.equals(field, that.field) &&
        Objects.equals(fallbackQuery, that.fallbackQuery);
  }

  @Override
  public int hashCode() {
    return Objects.hash(field, lowerValue, upperValue, fallbackQuery);
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(field)) {
      visitor.visitLeaf(this);
      fallbackQuery.visit(visitor);
    }
  }

  @Override
  public String toString(String field) {
    StringBuilder b = new StringBuilder();
    if (this.field.equals(field) == false) {
      b.append(this.field).append(":");
    }
    return b
        .append("[")
        .append(lowerValue)
        .append(" TO ")
        .append(upperValue)
        .append("]")
        .toString();
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    if (lowerValue == Long.MIN_VALUE && upperValue == Long.MAX_VALUE) {
      return new DocValuesFieldExistsQuery(field);
    }

    Query rewrittenFallback = fallbackQuery.rewrite(reader);
    if (rewrittenFallback == fallbackQuery) {
      return this;
    } else {
      return new IndexSortSortedNumericDocValuesRangeQuery(
          field, lowerValue, upperValue, rewrittenFallback);
    }
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
    Weight fallbackWeight = fallbackQuery.createWeight(searcher, scoreMode, boost);

    return new ConstantScoreWeight(this, boost) {
      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        SortedNumericDocValues sortedNumericValues = DocValues.getSortedNumeric(context.reader(), field);
        NumericDocValues numericValues = DocValues.unwrapSingleton(sortedNumericValues);

        if (numericValues != null) {
          Sort indexSort = context.reader().getMetaData().getSort();
          if (indexSort != null
              && indexSort.getSort().length > 0
              && indexSort.getSort()[0].getField().equals(field)) {

            SortField sortField = indexSort.getSort()[0];
            DocIdSetIterator disi = getDocIdSetIterator(sortField, context, numericValues);
            return new ConstantScoreScorer(this, score(), scoreMode, disi);
          }
        }
        return fallbackWeight.scorer(context);
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        // Both queries should always return the same values, so we can just check
        // if the fallback query is cacheable.
        return fallbackWeight.isCacheable(ctx);
      }
    };
  }

  /**
   * Computes the document IDs that lie within the range [lowerValue, upperValue] by
   * performing binary search on the field's doc values.
   *
   * Because doc values only allow forward iteration, we need to reload the field comparator
   * every time the binary search accesses an earlier element.
   *
   * We must also account for missing values when performing the binary search. For this
   * reason, we load the {@link FieldComparator} instead of checking the docvalues directly.
   * The returned {@link DocIdSetIterator} makes sure to wrap the original docvalues to skip
   * over documents with no value.
   */
  private DocIdSetIterator getDocIdSetIterator(SortField sortField,
                                               LeafReaderContext context,
                                               DocIdSetIterator delegate) throws IOException {
    long lower = sortField.getReverse() ? upperValue : lowerValue;
    long upper = sortField.getReverse() ? lowerValue : upperValue;
    int maxDoc = context.reader().maxDoc();

    // Perform a binary search to find the first document with value >= lower.
    ValueComparator comparator = loadComparator(sortField, lower, context);
    int low = 0;
    int high = maxDoc - 1;

    while (low <= high) {
      int mid = (low + high) >>> 1;
      if (comparator.compare(mid) <= 0) {
        high = mid - 1;
        comparator = loadComparator(sortField, lower, context);
      } else {
        low = mid + 1;
      }
    }
    int firstDocIdInclusive = high + 1;

    // Perform a binary search to find the first document with value > upper.
    // Since we know that upper >= lower, we can initialize the lower bound
    // of the binary search to the result of the previous search.
    comparator = loadComparator(sortField, upper, context);
    low = firstDocIdInclusive;
    high = maxDoc - 1;

    while (low <= high) {
      int mid = (low + high) >>> 1;
      if (comparator.compare(mid) < 0) {
        high = mid - 1;
        comparator = loadComparator(sortField, upper, context);
      } else {
        low = mid + 1;
      }
    }

    int lastDocIdExclusive = high + 1;
    return new BoundedDocSetIdIterator(firstDocIdInclusive, lastDocIdExclusive, delegate);
  }

  /**
   * Compares the given document's value with a stored reference value.
   */
  private interface ValueComparator {
    int compare(int docID) throws IOException;
  }

  private static ValueComparator loadComparator(SortField sortField,
                                                long topValue,
                                                LeafReaderContext context) throws IOException {
    @SuppressWarnings("unchecked")
    FieldComparator<Long> fieldComparator = (FieldComparator<Long>) sortField.getComparator(1, 0);
    fieldComparator.setTopValue(topValue);

    LeafFieldComparator leafFieldComparator = fieldComparator.getLeafComparator(context);
    int direction = sortField.getReverse() ? -1 : 1;

    return doc -> {
      int value = leafFieldComparator.compareTop(doc);
      return direction * value;
    };
  }

  /**
   * A doc ID set iterator that wraps a delegate iterator and only returns doc IDs in
   * the range [firstDocInclusive, lastDoc).
   */
  private static class BoundedDocSetIdIterator extends DocIdSetIterator {
    private final int firstDoc;
    private final int lastDoc;
    private final DocIdSetIterator delegate;

    private int docID = -1;

    BoundedDocSetIdIterator(int firstDoc,
                            int lastDoc,
                            DocIdSetIterator delegate) {
      this.firstDoc = firstDoc;
      this.lastDoc = lastDoc;
      this.delegate = delegate;
    }

    @Override
    public int docID() {
      return docID;
    }

    @Override
    public int nextDoc() throws IOException {
      return advance(docID + 1);
    }

    @Override
    public int advance(int target) throws IOException {
      if (target < firstDoc) {
        target = firstDoc;
      }

      int result = delegate.advance(target);
      if (result < lastDoc) {
        docID = result;
      } else {
        docID = NO_MORE_DOCS;
      }
      return docID;
    }

    @Override
    public long cost() {
      return lastDoc - firstDoc;
    }
  }
}
