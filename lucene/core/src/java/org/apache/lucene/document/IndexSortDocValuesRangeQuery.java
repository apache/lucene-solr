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
package org.apache.lucene.document;

import java.io.IOException;
import java.util.Objects;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.Weight;

/**
 * A range query that takes advantage of the fact that the index is sorted to speed up execution.
 * It assumes that the index is sorted on the same field as the query, and performs binary search
 * on the field's doc values to find the documents at the lower and upper ends of the range.
 *
 * This optimized execution strategy can only be used if all of these conditions hold:
 *   - The index is sorted, and its primary sort is on the same field as the query.
 *   - The provided field has {@link SortedNumericDocValues}.
 *   - The segments must have at most one field value per document (otherwise we cannot easily
 *     determine the matching document IDs through a binary search).
 */
public final class IndexSortDocValuesRangeQuery extends Query {

  private final String field;
  private final long lowerValue;
  private final long upperValue;

  /**
   * Creates a new {@link IndexSortDocValuesRangeQuery}.
   *
   * @param field The field name.
   * @param lowerValue The lower end of the range (inclusive).
   * @param upperValue The upper end of the range (inclusive).
   */
  public IndexSortDocValuesRangeQuery(String field,
                                      long lowerValue,
                                      long upperValue) {
    this.field = field;
    this.lowerValue = lowerValue;
    this.upperValue = upperValue;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
    return new ConstantScoreWeight(this, boost) {
      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        LeafReader reader = context.reader();

        Sort indexSort = reader.getMetaData().getSort();
        if (indexSort == null || indexSort.getSort().length == 0) {
          throw new IllegalArgumentException("To use IndexSortDocValuesRangeQuery, the index must be sorted.");
        }

        SortField sortField = indexSort.getSort()[0];
        if (sortField.getField().equals(field) == false) {
          throw new IllegalArgumentException("To use IndexSortDocValuesRangeQuery, the index must be sorted " +
              "on the same field as the query field.");
        }

        SortedNumericDocValues sortedDocValues = reader.getSortedNumericDocValues(field);
        NumericDocValues docValues = DocValues.unwrapSingleton(sortedDocValues);
        if (docValues == null) {
          throw new IllegalArgumentException("To use IndexSortDocValuesRangeQuery, the query field must have " +
              "SortedNumericDocValues and have at most one value per document.");
        }

        DocIdSetIterator disi = getDocIdSetIterator(lowerValue, upperValue,
            sortField, context, docValues);
        return new ConstantScoreScorer(this, score(), scoreMode, disi);
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return DocValues.isCacheable(ctx, field);
      }
    };
  }

  /**
   * Computes the document IDs that lie within the range [lowerValue, upperValue] by
   * performing binary search on the field's doc values.
   *
   * Because doc values only allow forward iteration, we need to reload the field comparator
   * every time the binary search accesses an earlier element.
   */
  private static DocIdSetIterator getDocIdSetIterator(long lowerValue,
                                                      long upperValue,
                                                      SortField sortField,
                                                      LeafReaderContext context,
                                                      DocIdSetIterator delegate) throws IOException {
    int maxDoc = context.reader().maxDoc();
    if (maxDoc <= 0) {
      return DocIdSetIterator.empty();
    }

    if (sortField.getReverse()) {
      long temp = lowerValue;
      lowerValue = upperValue;
      upperValue = temp;
    }

    // Perform a binary search to find the first document with value >= lowerValue.
    ValueComparator comparator = loadComparator(sortField, lowerValue, context);
    int low = 0;
    int high = maxDoc - 1;

    while (low <= high) {
      int mid = (low + high) >>> 1;
      if (comparator.compare(mid) <= 0) {
        high = mid - 1;
        comparator = loadComparator(sortField, lowerValue, context);
      } else {
        low = mid + 1;
      }
    }
    int firstDocIdInclusive = high + 1;

    // Perform a binary search to find the first document with value > upperValue.
    // Since we know that upperValue >= lowerValue, we can initialize the lower bound
    // of the binary search to the result of the previous search.
    comparator = loadComparator(sortField, upperValue, context);
    low = firstDocIdInclusive;
    high = maxDoc - 1;

    while (low <= high) {
      int mid = (low + high) >>> 1;
      if (comparator.compare(mid) < 0) {
        high = mid - 1;
        comparator = loadComparator(sortField, upperValue, context);
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
        return docID;
      } else {
        return NO_MORE_DOCS;
      }
    }

    @Override
    public long cost() {
      return lastDoc - firstDoc;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    IndexSortDocValuesRangeQuery that = (IndexSortDocValuesRangeQuery) o;
    return lowerValue == that.lowerValue &&
        upperValue == that.upperValue &&
        Objects.equals(field, that.field);
  }

  @Override
  public int hashCode() {
    return Objects.hash(field, lowerValue, upperValue);
  }


  @Override
  public void visit(QueryVisitor visitor) {
    QueryVisitor v = visitor.getSubVisitor(BooleanClause.Occur.MUST, this);
    v.visitLeaf(this);
  }

  @Override
  public String toString(String field) {
    StringBuilder builder = new StringBuilder();
    if (this.field.equals(field) == false) {
      builder.append(this.field).append(":");
    }
    return builder.append("[")
        .append(lowerValue)
        .append(" TO ")
        .append(upperValue)
        .append("]")
        .toString();
  }
}
