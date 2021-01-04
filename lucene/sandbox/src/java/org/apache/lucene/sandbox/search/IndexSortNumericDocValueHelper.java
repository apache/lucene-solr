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
package org.apache.lucene.sandbox.search;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.SortField;

import java.io.IOException;

/**
 * a helper to do a binary search with sorted NumericDocValues
 */
class IndexSortNumericDocValueHelper {
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
  static BoundedNumericDocValues boundNumericDocValueOptimizedByIndexSort(long lowerValue,
                                                                          long upperValue,
                                                                          SortField sortField,
                                                                          NumericDocValues delegate,
                                                                          LeafReaderContext context) throws IOException {
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
    return new BoundedNumericDocValues(firstDocIdInclusive, lastDocIdExclusive, delegate);
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
  public static class BoundedNumericDocValues extends DocIdSetIterator {
    private final NumericDocValues delegate;
    private final int firstDoc;
    private final int lastDoc;

    private int docID = -1;

    BoundedNumericDocValues(int firstDoc,
                            int lastDoc,
                            NumericDocValues delegate) {
      this.firstDoc = firstDoc;
      this.lastDoc = lastDoc;
      this.delegate = delegate;
    }

    public long longValue() throws IOException {
      return delegate.longValue();
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
