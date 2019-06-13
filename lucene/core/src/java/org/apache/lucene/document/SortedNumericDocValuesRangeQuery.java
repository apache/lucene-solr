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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;

/**
 * A range query that executes using {@link SortedNumericDocValues}.
 *
 * By default, the query uses a {@link TwoPhaseIterator} where the leading iterator scans
 * over all doc values, and each document's values are checked against the range bounds.
 *
 * When the index is sorted on the same field as the query, we can instead perform binary search
 * on the field's doc values to find the documents at the lower and upper ends of the range. This
 * optimized execution strategy is only used if the following conditions hold:
 *   - The segment is sorted, and its primary sort is on the same field as the query.
 *   - The segments must have at most one field value per document (otherwise we cannot easily
 *     determine the matching document IDs through a binary search).
 */
abstract class SortedNumericDocValuesRangeQuery extends Query {

  private final String field;
  private final long lowerValue;
  private final long upperValue;

  SortedNumericDocValuesRangeQuery(String field, long lowerValue, long upperValue) {
    this.field = Objects.requireNonNull(field);
    this.lowerValue = lowerValue;
    this.upperValue = upperValue;
  }

  @Override
  public boolean equals(Object obj) {
    if (sameClassAs(obj) == false) {
      return false;
    }
    SortedNumericDocValuesRangeQuery that = (SortedNumericDocValuesRangeQuery) obj;
    return Objects.equals(field, that.field)
        && lowerValue == that.lowerValue
        && upperValue == that.upperValue;
  }

  @Override
  public int hashCode() {
    int h = classHash();
    h = 31 * h + field.hashCode();
    h = 31 * h + Long.hashCode(lowerValue);
    h = 31 * h + Long.hashCode(upperValue);
    return h;
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(field)) {
      visitor.visitLeaf(this);
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
    return super.rewrite(reader);
  }

  abstract SortedNumericDocValues getValues(LeafReader reader, String field) throws IOException;

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
    return new ConstantScoreWeight(this, boost) {

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return DocValues.isCacheable(ctx, field);
      }

      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        SortedNumericDocValues values = getValues(context.reader(), field);
        if (values == null) {
          return null;
        }

        final NumericDocValues singleton = DocValues.unwrapSingleton(values);
        if (singleton != null) {
          Sort indexSort = context.reader().getMetaData().getSort();
          if (indexSort != null
              && indexSort.getSort().length > 0
              && indexSort.getSort()[0].getField().equals(field)) {

            SortField sortField = indexSort.getSort()[0];
            DocIdSetIterator disi = getDocIdSetIterator(sortField, context, singleton);
            return new ConstantScoreScorer(this, score(), scoreMode, disi);
          }
        }

        TwoPhaseIterator twoPhaseIterator = getTwoPhaseIterator(values);
        return new ConstantScoreScorer(this, score(), scoreMode, twoPhaseIterator);
      }
    };
  }

  private TwoPhaseIterator getTwoPhaseIterator(SortedNumericDocValues values) {
    final NumericDocValues singleton = DocValues.unwrapSingleton(values);
    if (singleton != null) {
      return new TwoPhaseIterator(singleton) {
        @Override
        public boolean matches() throws IOException {
          final long value = singleton.longValue();
          return value >= lowerValue && value <= upperValue;
        }

        @Override
        public float matchCost() {
          return 2; // 2 comparisons
        }
      };
    } else {
      return new TwoPhaseIterator(values) {
        @Override
        public boolean matches() throws IOException {
          for (int i = 0, count = values.docValueCount(); i < count; ++i) {
            final long value = values.nextValue();
            if (value < lowerValue) {
              continue;
            }
            // Values are sorted, so the first value that is >= lowerValue is our best candidate
            return value <= upperValue;
          }
          return false; // all values were < lowerValue
        }

        @Override
        public float matchCost() {
          return 2; // 2 comparisons
        }
      };
    }
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
    int maxDoc = context.reader().maxDoc();
    if (maxDoc <= 0) {
      return DocIdSetIterator.empty();
    }

    long lower = sortField.getReverse() ? upperValue : lowerValue;
    long upper = sortField.getReverse() ? lowerValue : upperValue;

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

}
