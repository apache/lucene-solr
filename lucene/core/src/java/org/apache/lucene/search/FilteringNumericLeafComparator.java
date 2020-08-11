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

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.util.DocIdSetBuilder;

import java.io.IOException;
import java.util.Arrays;

/**
 * A {@code FilteringLeafFieldComparator} that provides a functionality to skip over non-competitive documents
 * for numeric fields indexed with points.
 */
abstract class FilteringNumericLeafComparator implements FilteringLeafFieldComparator {
  protected final LeafFieldComparator in;
  protected final boolean reverse;
  protected final boolean singleSort; //if sort is based on a single sort field as opposed to multiple sort fields
  private final boolean hasTopValue;
  private final PointValues pointValues;
  private final int bytesCount;
  private final int maxDoc;
  private final byte[] minValueAsBytes;
  private final byte[] maxValueAsBytes;

  private long iteratorCost;
  private int maxDocVisited = 0;
  private int updateCounter = 0;
  private boolean canUpdateIterator = false; // set to true when queue becomes full and hitsThreshold is reached
  private DocIdSetIterator competitiveIterator;

  public FilteringNumericLeafComparator(LeafFieldComparator in, LeafReaderContext context, String field,
        boolean reverse, boolean singleSort, boolean hasTopValue, int bytesCount) throws IOException {
    this.in = in;
    this.pointValues = context.reader().getPointValues(field);
    this.reverse = reverse;
    this.singleSort = singleSort;
    this.hasTopValue = hasTopValue;
    this.maxDoc = context.reader().maxDoc();
    this.bytesCount = bytesCount;
    this.maxValueAsBytes = reverse == false ? new byte[bytesCount] : hasTopValue ? new byte[bytesCount] : null;
    this.minValueAsBytes = reverse ? new byte[bytesCount] : hasTopValue ? new byte[bytesCount] : null;

    // TODO: optimize a case when pointValues are missing only on this segment
    this.competitiveIterator = pointValues == null ? null : DocIdSetIterator.all(maxDoc);
    this.iteratorCost = maxDoc;
  }

  @Override
  public void setBottom(int slot) throws IOException {
    in.setBottom(slot);
    updateCompetitiveIterator(); // update an iterator if we set a new bottom
  }

  @Override
  public int compareBottom(int doc) throws IOException {
    return in.compareBottom(doc);
  }

  @Override
  public int compareTop(int doc) throws IOException {
    return in.compareTop(doc);
  }

  @Override
  public void copy(int slot, int doc) throws IOException {
    in.copy(slot, doc);
    maxDocVisited = doc;
  }

  @Override
  public void setScorer(Scorable scorer) throws IOException {
    in.setScorer(scorer);
    if (scorer instanceof Scorer) {
      iteratorCost = ((Scorer) scorer).iterator().cost(); // starting iterator cost is the scorer's cost
      updateCompetitiveIterator(); // update an iterator when we have a new segment
    }
  }

  @Override
  public void setCanUpdateIterator() throws IOException {
    this.canUpdateIterator = true;
    updateCompetitiveIterator();
  }

  @Override
  public DocIdSetIterator competitiveIterator() {
    if (competitiveIterator == null) return null;
    return new DocIdSetIterator() {
      private int doc;

      @Override
      public int nextDoc() throws IOException {
        return doc = competitiveIterator.nextDoc();
      }

      @Override
      public int docID() {
        return doc;
      }

      @Override
      public long cost() {
        return competitiveIterator.cost();
      }

      @Override
      public int advance(int target) throws IOException {
        return doc = competitiveIterator.advance(target);
      }
    };
  }

  // update its iterator to include possibly only docs that are "stronger" than the current bottom entry
  private void updateCompetitiveIterator() throws IOException {
    if (canUpdateIterator == false) return;
    if (pointValues == null) return;
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
      if (hasTopValue) {
        encodeTop(minValueAsBytes);
      }
    } else {
      encodeBottom(minValueAsBytes);
      if (hasTopValue) {
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
          int cmp = Arrays.compareUnsigned(packedValue, 0, bytesCount, maxValueAsBytes, 0, bytesCount);
          // if doc's value is too high or for single sort even equal, it is not competitive and the doc can be skipped
          if (cmp > 0 || (singleSort && cmp == 0)) return;
        }
        if (minValueAsBytes != null) {
          int cmp = Arrays.compareUnsigned(packedValue, 0, bytesCount, minValueAsBytes, 0, bytesCount);
          // if doc's value is too low or for single sort even equal, it is not competitive and the doc can be skipped
          if (cmp < 0 || (singleSort && cmp == 0)) return;
        }
        adder.add(docID); // doc is competitive
      }

      @Override
      public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
        if (maxValueAsBytes != null) {
          int cmp = Arrays.compareUnsigned(minPackedValue, 0, bytesCount, maxValueAsBytes, 0, bytesCount);
          if (cmp > 0 || (singleSort && cmp == 0)) return PointValues.Relation.CELL_OUTSIDE_QUERY;
        }
        if (minValueAsBytes != null) {
          int cmp =  Arrays.compareUnsigned(maxPackedValue, 0, bytesCount, minValueAsBytes, 0, bytesCount);
          if (cmp < 0 || (singleSort && cmp == 0)) return PointValues.Relation.CELL_OUTSIDE_QUERY;
        }
        if ((maxValueAsBytes != null && Arrays.compareUnsigned(maxPackedValue, 0, bytesCount, maxValueAsBytes, 0, bytesCount) > 0) ||
            (minValueAsBytes != null && Arrays.compareUnsigned(minPackedValue, 0, bytesCount, minValueAsBytes, 0, bytesCount) < 0)) {
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

  protected abstract boolean isMissingValueCompetitive();

  protected abstract void encodeBottom(byte[] packedValue);

  protected abstract void encodeTop(byte[] packedValue);


  /**
   * A wrapper over double long comparator that adds a functionality to filter non-competitive docs.
   */
  static class FilteringLongLeafComparator extends FilteringNumericLeafComparator {
    public FilteringLongLeafComparator(FieldComparator.LongComparator in, LeafReaderContext context,
        String field, boolean reverse, boolean singleSort, boolean hasTopValue) throws IOException {
      super(in, context, field, reverse, singleSort, hasTopValue, Long.BYTES);
    }

    @Override
    protected boolean isMissingValueCompetitive() {
      int result = Long.compare(((FieldComparator.LongComparator) in).missingValue, ((FieldComparator.LongComparator) in).bottom);
      // in reverse (desc) sort missingValue is competitive when it's greater or equal to bottom,
      // in asc sort missingValue is competitive when it's smaller or equal to bottom
      return reverse ? (result >= 0) : (result <= 0);
    }

    @Override
    protected void encodeBottom(byte[] packedValue) {
      LongPoint.encodeDimension(((FieldComparator.LongComparator) in).bottom, packedValue, 0);
    }

    @Override
    protected void encodeTop(byte[] packedValue) {
      LongPoint.encodeDimension(((FieldComparator.LongComparator) in).topValue, packedValue, 0);
    }
  }

  /**
   * A wrapper over integer leaf comparator that adds a functionality to filter non-competitive docs.
   */
  static class FilteringIntLeafComparator extends FilteringNumericLeafComparator {
    public FilteringIntLeafComparator(FieldComparator.IntComparator in, LeafReaderContext context,
        String field, boolean reverse, boolean singleSort, boolean hasTopValue) throws IOException {
      super(in, context, field, reverse, singleSort, hasTopValue, Integer.BYTES);
    }

    @Override
    protected boolean isMissingValueCompetitive() {
      int result = Integer.compare(((FieldComparator.IntComparator) in).missingValue, ((FieldComparator.IntComparator) in).bottom);
      // in reverse (desc) sort missingValue is competitive when it's greater or equal to bottom,
      // in asc sort missingValue is competitive when it's smaller or equal to bottom
      return reverse ? (result >= 0) : (result <= 0);
    }

    @Override
    protected void encodeBottom(byte[] packedValue) {
      IntPoint.encodeDimension(((FieldComparator.IntComparator) in).bottom, packedValue, 0);
    }

    @Override
    protected void encodeTop(byte[] packedValue) {
      IntPoint.encodeDimension(((FieldComparator.IntComparator) in).topValue, packedValue, 0);
    }
  }

  /**
   * A wrapper over double leaf comparator that adds a functionality to filter non-competitive docs.
   */
  static class FilteringDoubleLeafComparator extends FilteringNumericLeafComparator {
    public FilteringDoubleLeafComparator(FieldComparator.DoubleComparator in, LeafReaderContext context,
        String field, boolean reverse, boolean singleSort, boolean hasTopValue) throws IOException {
      super(in, context, field, reverse, singleSort, hasTopValue, Double.BYTES);
    }

    @Override
    protected boolean isMissingValueCompetitive() {
      int result = Double.compare(((FieldComparator.DoubleComparator) in).missingValue, ((FieldComparator.DoubleComparator) in).bottom);
      return reverse ? (result >= 0) : (result <= 0);
    }

    @Override
    protected void encodeBottom(byte[] packedValue) {
      DoublePoint.encodeDimension(((FieldComparator.DoubleComparator) in).bottom, packedValue, 0);
    }

    @Override
    protected void encodeTop(byte[] packedValue) {
      DoublePoint.encodeDimension(((FieldComparator.DoubleComparator) in).topValue, packedValue, 0);
    }
  }

  /**
   * A wrapper over float leaf comparator that adds a functionality to filter non-competitive docs.
   */
  static class FilteringFloatLeafComparator extends FilteringNumericLeafComparator {
    public FilteringFloatLeafComparator(FieldComparator.FloatComparator in, LeafReaderContext context,
        String field, boolean reverse, boolean singleSort, boolean hasTopValue) throws IOException {
      super(in, context, field, reverse, singleSort, hasTopValue, Float.BYTES);
    }

    @Override
    protected boolean isMissingValueCompetitive() {
      int result = Float.compare(((FieldComparator.FloatComparator) in).missingValue, ((FieldComparator.FloatComparator) in).bottom);
      return reverse ? (result >= 0) : (result <= 0);
    }

    @Override
    protected void encodeBottom(byte[] packedValue) {
      FloatPoint.encodeDimension(((FieldComparator.FloatComparator) in).bottom, packedValue, 0);
    }

    @Override
    protected void encodeTop(byte[] packedValue) {
      FloatPoint.encodeDimension(((FieldComparator.FloatComparator) in).topValue, packedValue, 0);
    }
  }

}
