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

package org.apache.lucene.search.comparators;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.FutureArrays;

import java.io.IOException;

/**
 * Abstract numeric comparator for comparing numeric values.
 * This comparator provides a skipping functionality – an iterator that can skip over non-competitive documents.
 */
public abstract class NumericComparator<T extends Number> extends FieldComparator<T> {
  protected final T missingValue;
  protected final String field;
  protected final boolean reverse;
  private final int bytesCount; // how many bytes are used to encode this number

  protected boolean topValueSet;
  protected boolean singleSort; // singleSort is true, if sort is based on a single sort field.
  protected boolean hitsThresholdReached;
  protected boolean queueFull;
  private boolean canSkipDocuments;

  protected NumericComparator(String field, T missingValue, boolean reverse, int sortPos, int bytesCount) {
    this.field = field;
    this.missingValue = missingValue;
    this.reverse = reverse;
    this.canSkipDocuments = (sortPos == 0); // skipping functionality is only relevant for primary sort
    this.bytesCount = bytesCount;
  }

  @Override
  public void disableSkipping() {
    canSkipDocuments = false;
  }

  @Override
  public void setTopValue(T value) {
    topValueSet = true;
  }

  @Override
  public void setSingleSort() {
    singleSort = true;
  }

  /**
   * Leaf comparator for {@link NumericComparator} that provides skipping functionality
   */
  public abstract class NumericLeafComparator implements LeafFieldComparator {
    protected final NumericDocValues docValues;
    private final PointValues pointValues;
    private final boolean enableSkipping; // if skipping functionality should be enabled on this segment
    private final int maxDoc;
    private final byte[] minValueAsBytes;
    private final byte[] maxValueAsBytes;

    private DocIdSetIterator competitiveIterator;
    private long iteratorCost;
    private int maxDocVisited = 0;
    private int updateCounter = 0;

    public NumericLeafComparator(LeafReaderContext context) throws IOException {
      this.docValues = getNumericDocValues(context, field);
      this.pointValues = canSkipDocuments ? context.reader().getPointValues(field) : null;
      if (pointValues != null) {
        this.enableSkipping = true; // skipping is enabled when points are available
        this.maxDoc = context.reader().maxDoc();
        this.maxValueAsBytes = reverse == false ? new byte[bytesCount] : topValueSet ? new byte[bytesCount] : null;
        this.minValueAsBytes = reverse ? new byte[bytesCount] : topValueSet ? new byte[bytesCount] : null;
        this.competitiveIterator = DocIdSetIterator.all(maxDoc);
        this.iteratorCost = maxDoc;
      } else {
        this.enableSkipping = false;
        this.maxDoc = 0;
        this.maxValueAsBytes = null;
        this.minValueAsBytes = null;
      }
    }

    /**
     * Retrieves the NumericDocValues for the field in this segment
     */
    protected NumericDocValues getNumericDocValues(LeafReaderContext context, String field) throws IOException {
      return DocValues.getNumeric(context.reader(), field);
    }

    @Override
    public void setBottom(int slot) throws IOException {
      queueFull = true; // if we are setting bottom, it means that we have collected enough hits
      updateCompetitiveIterator(); // update an iterator if we set a new bottom
    }

    @Override
    public void copy(int slot, int doc) throws IOException {
      maxDocVisited = doc;
    }

    @Override
    public void setScorer(Scorable scorer) throws IOException {
      if (scorer instanceof Scorer) {
        iteratorCost = ((Scorer) scorer).iterator().cost(); // starting iterator cost is the scorer's cost
        updateCompetitiveIterator(); // update an iterator when we have a new segment
      }
    }

    @Override
    public void setHitsThresholdReached() throws IOException {
      hitsThresholdReached = true;
      updateCompetitiveIterator();
    }

    // update its iterator to include possibly only docs that are "stronger" than the current bottom entry
    private void updateCompetitiveIterator() throws IOException {
      if (enableSkipping == false || hitsThresholdReached == false || queueFull == false) return;
      // if some documents have missing points, check that missing values prohibits optimization
      if ((pointValues.getDocCount() < maxDoc) && isMissingValueCompetitive()) {
        return; // we can't filter out documents, as documents with missing values are competitive
      }

      updateCounter++;
      if (updateCounter > 256 && (updateCounter & 0x1f) != 0x1f) { // Start sampling if we get called too much
        return;
      }
      if (reverse == false) {
        encodeBottom(maxValueAsBytes);
        if (topValueSet) {
          encodeTop(minValueAsBytes);
        }
      } else {
        encodeBottom(minValueAsBytes);
        if (topValueSet) {
          encodeTop(maxValueAsBytes);
        }
      }

      DocIdSetBuilder result = new DocIdSetBuilder(maxDoc);
      PointValues.IntersectVisitor visitor = new PointValues.IntersectVisitor() {
        DocIdSetBuilder.BulkAdder adder;

        @Override
        public void grow(int count) {
          adder = result.grow(count);
        }

        @Override
        public void visit(int docID) {
          if (docID <= maxDocVisited) {
            return; // Already visited or skipped
          }
          adder.add(docID);
        }

        @Override
        public void visit(int docID, byte[] packedValue) {
          if (docID <= maxDocVisited) {
            return;  // already visited or skipped
          }
          if (maxValueAsBytes != null) {
            int cmp = FutureArrays.compareUnsigned(packedValue, 0, bytesCount, maxValueAsBytes, 0, bytesCount);
            // if doc's value is too high or for single sort even equal, it is not competitive and the doc can be skipped
            if (cmp > 0 || (singleSort && cmp == 0)) return;
          }
          if (minValueAsBytes != null) {
            int cmp = FutureArrays.compareUnsigned(packedValue, 0, bytesCount, minValueAsBytes, 0, bytesCount);
            // if doc's value is too low or for single sort even equal, it is not competitive and the doc can be skipped
            if (cmp < 0 || (singleSort && cmp == 0)) return;
          }
          adder.add(docID); // doc is competitive
        }

        @Override
        public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
          if (maxValueAsBytes != null) {
            int cmp = FutureArrays.compareUnsigned(minPackedValue, 0, bytesCount, maxValueAsBytes, 0, bytesCount);
            if (cmp > 0 || (singleSort && cmp == 0)) return PointValues.Relation.CELL_OUTSIDE_QUERY;
          }
          if (minValueAsBytes != null) {
            int cmp = FutureArrays.compareUnsigned(maxPackedValue, 0, bytesCount, minValueAsBytes, 0, bytesCount);
            if (cmp < 0 || (singleSort && cmp == 0)) return PointValues.Relation.CELL_OUTSIDE_QUERY;
          }
          if ((maxValueAsBytes != null &&
              FutureArrays.compareUnsigned(maxPackedValue, 0, bytesCount, maxValueAsBytes, 0, bytesCount) > 0) ||
              (minValueAsBytes != null &&
                  FutureArrays.compareUnsigned(minPackedValue, 0, bytesCount, minValueAsBytes, 0, bytesCount) < 0)) {
            return PointValues.Relation.CELL_CROSSES_QUERY;
          }
          return PointValues.Relation.CELL_INSIDE_QUERY;
        }
      };
      final long threshold = iteratorCost >>> 3;
      long estimatedNumberOfMatches = pointValues.estimatePointCount(visitor); // runs in O(log(numPoints))
      if (estimatedNumberOfMatches >= threshold) {
        // the new range is not selective enough to be worth materializing, it doesn't reduce number of docs at least 8x
        return;
      }
      pointValues.intersect(visitor);
      competitiveIterator = result.build().iterator();
      iteratorCost = competitiveIterator.cost();
    }

    @Override
    public DocIdSetIterator competitiveIterator() {
      if (enableSkipping == false) return null;
      return new DocIdSetIterator() {
        private int docID = -1;

        @Override
        public int nextDoc() throws IOException {
          return advance(docID + 1);
        }

        @Override
        public int docID() {
          return docID;
        }

        @Override
        public long cost() {
          return competitiveIterator.cost();
        }

        @Override
        public int advance(int target) throws IOException {
          return docID = competitiveIterator.advance(target);
        }
      };
    }

    protected abstract boolean isMissingValueCompetitive();

    protected abstract void encodeBottom(byte[] packedValue);

    protected abstract void encodeTop(byte[] packedValue);
  }
}
