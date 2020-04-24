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
 * A wrapper over {@code NumericComparator} that adds a functionality to filter non-competitive docs.
 */
public abstract class FilteringNumericComparator<T extends Number> extends FilteringFieldComparator<T> implements LeafFieldComparator {
  protected final boolean reverse;
  private boolean hasTopValue = false;
  private PointValues pointValues;
  private final int bytesCount;
  private final byte[] minValueAsBytes;
  private final byte[] maxValueAsBytes;
  private boolean minValueExist = false;
  private boolean maxValueExist = false;
  private int maxDoc;
  private int maxDocVisited;
  private int updateCounter = 0;
  private final String field;
  protected boolean canUpdateIterator = false; // set to true when queue becomes full and hitsThreshold is reached
  protected DocIdSetIterator competitiveIterator = null;
  private long iteratorCost = 0;

  public FilteringNumericComparator(NumericComparator<T> in, boolean reverse, int bytesCount) {
    super(in);
    this.field = in.field;
    this.bytesCount = bytesCount;
    this.reverse = reverse;
    minValueAsBytes = new byte[bytesCount];
    maxValueAsBytes = new byte[bytesCount];
    if (reverse) {
      minValueExist = true;
    } else {
      maxValueExist = true;
    }
  }

  /**
   * Returns an iterator over competitive documents
   */
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

  @Override
  public void setCanUpdateIterator() throws IOException {
    this.canUpdateIterator = true;
    // for the 1st time queue becomes full and hitsThreshold is reached
    // we can start updating competitive iterator
    updateCompetitiveIterator();
  }

  @Override
  public void setTopValue(T value) {
    hasTopValue = true;
    if (reverse) {
      maxValueExist = true;
    } else {
      minValueExist = true;
    }
    in.setTopValue(value);
  }

  @Override
  public void setBottom(int slot) throws IOException {
    ((NumericComparator) in).setBottom(slot);
    updateCompetitiveIterator(); // update an iterator if we set a new bottom
  }

  @Override
  public int compareBottom(int doc) throws IOException {
    return ((NumericComparator) in).compareBottom(doc);
  }

  @Override
  public int compareTop(int doc) throws IOException {
    return ((NumericComparator) in).compareTop(doc);
  }

  @Override
  public void copy(int slot, int doc) throws IOException {
    ((NumericComparator) in).copy(slot, doc);
  }

  @Override
  public void setScorer(Scorable scorer) throws IOException {
    ((NumericComparator) in).setScorer(scorer);
    if (scorer instanceof Scorer) {
      iteratorCost = ((Scorer) scorer).iterator().cost(); // starting iterator cost is the scorer's cost
      updateCompetitiveIterator(); // update an iterator when we have a new segment
    }
  }

  @Override
  public final LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
    ((NumericComparator) in).doSetNextReader(context);
    maxDoc = context.reader().maxDoc();
    maxDocVisited = 0;
    pointValues = context.reader().getPointValues(field);
    // TODO: optimize a case when pointValues are missing only on this segment
    competitiveIterator = pointValues == null ? null : DocIdSetIterator.all(maxDoc);
    iteratorCost = 0;
    return this;
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
          return;  // Already visited or skipped
        }
        if (maxValueExist) {
          // doc's value is too high
          if (Arrays.compareUnsigned(packedValue, 0, bytesCount, maxValueAsBytes, 0, bytesCount) > 0) return;
        }
        if (minValueExist) {
          // doc's value is too low
          if (Arrays.compareUnsigned(packedValue, 0, bytesCount, minValueAsBytes, 0, bytesCount) < 0) return;
        }
        adder.add(docID); // doc is competitive
      }

      @Override
      public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
        if ((maxValueExist && Arrays.compareUnsigned(minPackedValue, 0, bytesCount, maxValueAsBytes, 0, bytesCount) > 0) ||
            (minValueExist && Arrays.compareUnsigned(maxPackedValue, 0, bytesCount, minValueAsBytes, 0, bytesCount) < 0)) {
          return PointValues.Relation.CELL_OUTSIDE_QUERY;
        }
        if ((maxValueExist && Arrays.compareUnsigned(maxPackedValue, 0, bytesCount, maxValueAsBytes, 0, bytesCount) > 0) ||
            (minValueExist && Arrays.compareUnsigned(minPackedValue, 0, bytesCount, minValueAsBytes, 0, bytesCount) < 0)) {
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
   * A wrapper over {@code LongComparator} that adds a functionality to filter non-competitive docs.
   */
  public static class FilteringLongComparator extends FilteringNumericComparator<Long> {
    public FilteringLongComparator(LongComparator in, boolean reverse) {
      super(in, reverse, Long.BYTES);
    }

    @Override
    protected boolean isMissingValueCompetitive() {
      int result = Long.compare(((LongComparator) in).missingValue, ((LongComparator) in).bottom);
      // in reverse (desc) sort missingValue is competitive when it's greater or equal to bottom,
      // in asc sort missingValue is competitive when it's smaller or equal to bottom
      return reverse ? (result >= 0) : (result <= 0);
    }

    @Override
    protected void encodeBottom(byte[] packedValue) {
      LongPoint.encodeDimension(((LongComparator) in).bottom, packedValue, 0);
    }

    @Override
    protected void encodeTop(byte[] packedValue) {
      LongPoint.encodeDimension(((LongComparator) in).topValue, packedValue, 0);
    }
  }

  /**
   * A wrapper over {@code IntComparator} that adds a functionality to filter non-competitive docs.
   */
  public static class FilteringIntComparator extends FilteringNumericComparator<Integer> {
    public FilteringIntComparator(IntComparator in, boolean reverse) {
      super(in, reverse, Integer.BYTES);
    }

    @Override
    protected boolean isMissingValueCompetitive() {
      int result = Integer.compare(((IntComparator) in).missingValue, ((IntComparator) in).bottom);
      return reverse ? (result >= 0) : (result <= 0);
    }

    @Override
    protected void encodeBottom(byte[] packedValue) {
      IntPoint.encodeDimension(((IntComparator) in).bottom, packedValue, 0);
    }

    @Override
    protected void encodeTop(byte[] packedValue) {
      IntPoint.encodeDimension(((IntComparator) in).topValue, packedValue, 0);
    }
  }

  /**
   * A wrapper over {@code DoubleComparator} that adds a functionality to filter non-competitive docs.
   */
  public static class FilteringDoubleComparator extends FilteringNumericComparator<Double> {
    public FilteringDoubleComparator(DoubleComparator in, boolean reverse) {
      super(in, reverse, Double.BYTES);
    }

    @Override
    protected boolean isMissingValueCompetitive() {
      int result = Double.compare(((DoubleComparator) in).missingValue, ((DoubleComparator) in).bottom);
      return reverse ? (result >= 0) : (result <= 0);
    }

    @Override
    protected void encodeBottom(byte[] packedValue) {
      DoublePoint.encodeDimension(((DoubleComparator) in).bottom, packedValue, 0);
    }

    @Override
    protected void encodeTop(byte[] packedValue) {
      DoublePoint.encodeDimension(((DoubleComparator) in).topValue, packedValue, 0);
    }
  }

  /**
   * A wrapper over {@code FloatComparator} that adds a functionality to filter non-competitive docs.
   */
  public static class FilteringFloatComparator extends FilteringNumericComparator<Float> {
    public FilteringFloatComparator(FloatComparator in, boolean reverse) {
      super(in, reverse, Float.BYTES);
    }

    @Override
    protected boolean isMissingValueCompetitive() {
      int result = Float.compare(((FloatComparator) in).missingValue, ((FloatComparator) in).bottom);
      return reverse ? (result >= 0) : (result <= 0);
    }

    @Override
    protected void encodeBottom(byte[] packedValue) {
      FloatPoint.encodeDimension(((FloatComparator) in).bottom, packedValue, 0);
    }

    @Override
    protected void encodeTop(byte[] packedValue) {
      FloatPoint.encodeDimension(((FloatComparator) in).topValue, packedValue, 0);
    }
  }

}