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
import java.util.AbstractCollection;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.lucene.document.IntPoint;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.index.PrefixCodedTerms;
import org.apache.lucene.index.PrefixCodedTerms.TermIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.StringHelper;

/**
 * Abstract query class to find all documents whose single or multi-dimensional point values, previously indexed with e.g. {@link IntPoint},
 * is contained in the specified set.
 *
 * <p>
 * This is for subclasses and works on the underlying binary encoding: to
 * create range queries for lucene's standard {@code Point} types, refer to factory
 * methods on those classes, e.g. {@link IntPoint#newSetQuery IntPoint.newSetQuery()} for 
 * fields indexed with {@link IntPoint}.
 * @see PointValues
 * @lucene.experimental */

public abstract class PointInSetQuery extends Query {
  // A little bit overkill for us, since all of our "terms" are always in the same field:
  final PrefixCodedTerms sortedPackedPoints;
  final int sortedPackedPointsHashCode;
  final String field;
  final int numDims;
  final int bytesPerDim;
  
  /** 
   * Iterator of encoded point values.
   */
  // TODO: if we want to stream, maybe we should use jdk stream class?
  public static abstract class Stream implements BytesRefIterator {
    @Override
    public abstract BytesRef next();
  };

  /** The {@code packedPoints} iterator must be in sorted order. */
  protected PointInSetQuery(String field, int numDims, int bytesPerDim, Stream packedPoints) {
    this.field = field;
    if (bytesPerDim < 1 || bytesPerDim > PointValues.MAX_NUM_BYTES) {
      throw new IllegalArgumentException("bytesPerDim must be > 0 and <= " + PointValues.MAX_NUM_BYTES + "; got " + bytesPerDim);
    }
    this.bytesPerDim = bytesPerDim;
    if (numDims < 1 || numDims > PointValues.MAX_DIMENSIONS) {
      throw new IllegalArgumentException("numDims must be > 0 and <= " + PointValues.MAX_DIMENSIONS + "; got " + numDims);
    }

    this.numDims = numDims;

    // In the 1D case this works well (the more points, the more common prefixes they share, typically), but in
    // the > 1 D case, where we are only looking at the first dimension's prefix bytes, it can at worst not hurt:
    PrefixCodedTerms.Builder builder = new PrefixCodedTerms.Builder();
    BytesRefBuilder previous = null;
    BytesRef current;
    while ((current = packedPoints.next()) != null) {
      if (current.length != numDims * bytesPerDim) {
        throw new IllegalArgumentException("packed point length should be " + (numDims * bytesPerDim) + " but got " + current.length + "; field=\"" + field + "\" numDims=" + numDims + " bytesPerDim=" + bytesPerDim);
      }
      if (previous == null) {
        previous = new BytesRefBuilder();
      } else {
        int cmp = previous.get().compareTo(current);
        if (cmp == 0) {
          continue; // deduplicate
        } else if (cmp > 0) {
          throw new IllegalArgumentException("values are out of order: saw " + previous + " before " + current);
        }
      }
      builder.add(field, current);
      previous.copyBytes(current);
    }
    sortedPackedPoints = builder.finish();
    sortedPackedPointsHashCode = sortedPackedPoints.hashCode();
  }

  @Override
  public final Weight createWeight(IndexSearcher searcher, boolean needsScores, float boost) throws IOException {

    // We don't use RandomAccessWeight here: it's no good to approximate with "match all docs".
    // This is an inverted structure and should be used in the first pass:

    return new ConstantScoreWeight(this, boost) {

      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        LeafReader reader = context.reader();

        PointValues values = reader.getPointValues(field);
        if (values == null) {
          // No docs in this segment/field indexed any points
          return null;
        }

        if (values.getNumDimensions() != numDims) {
          throw new IllegalArgumentException("field=\"" + field + "\" was indexed with numDims=" + values.getNumDimensions() + " but this query has numDims=" + numDims);
        }
        if (values.getBytesPerDimension() != bytesPerDim) {
          throw new IllegalArgumentException("field=\"" + field + "\" was indexed with bytesPerDim=" + values.getBytesPerDimension() + " but this query has bytesPerDim=" + bytesPerDim);
        }

        DocIdSetBuilder result = new DocIdSetBuilder(reader.maxDoc(), values, field);

        if (numDims == 1) {

          // We optimize this common case, effectively doing a merge sort of the indexed values vs the queried set:
          values.intersect(new MergePointVisitor(sortedPackedPoints, result));

        } else {
          // NOTE: this is naive implementation, where for each point we re-walk the KD tree to intersect.  We could instead do a similar
          // optimization as the 1D case, but I think it'd mean building a query-time KD tree so we could efficiently intersect against the
          // index, which is probably tricky!
          SinglePointVisitor visitor = new SinglePointVisitor(result);
          TermIterator iterator = sortedPackedPoints.iterator();
          for (BytesRef point = iterator.next(); point != null; point = iterator.next()) {
            visitor.setPoint(point);
            values.intersect(visitor);
          }
        }

        return new ConstantScoreScorer(this, score(), result.build().iterator());
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return true;
      }

    };
  }

  /** Essentially does a merge sort, only collecting hits when the indexed point and query point are the same.  This is an optimization,
   *  used in the 1D case. */
  private class MergePointVisitor implements IntersectVisitor {

    private final DocIdSetBuilder result;
    private TermIterator iterator;
    private BytesRef nextQueryPoint;
    private final BytesRef scratch = new BytesRef();
    private final PrefixCodedTerms sortedPackedPoints;
    private DocIdSetBuilder.BulkAdder adder;

    public MergePointVisitor(PrefixCodedTerms sortedPackedPoints, DocIdSetBuilder result) throws IOException {
      this.result = result;
      this.sortedPackedPoints = sortedPackedPoints;
      scratch.length = bytesPerDim;
      this.iterator = this.sortedPackedPoints.iterator();
      nextQueryPoint = iterator.next();
    }

    @Override
    public void grow(int count) {
      adder = result.grow(count);
    }

    @Override
    public void visit(int docID) {
      adder.add(docID);
    }

    @Override
    public void visit(int docID, byte[] packedValue) {
      scratch.bytes = packedValue;
      while (nextQueryPoint != null) {
        int cmp = nextQueryPoint.compareTo(scratch);
        if (cmp == 0) {
          // Query point equals index point, so collect and return
          adder.add(docID);
          break;
        } else if (cmp < 0) {
          // Query point is before index point, so we move to next query point
          nextQueryPoint = iterator.next();
        } else {
          // Query point is after index point, so we don't collect and we return:
          break;
        }
      }
    }

    @Override
    public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
      while (nextQueryPoint != null) {
        scratch.bytes = minPackedValue;
        int cmpMin = nextQueryPoint.compareTo(scratch);
        if (cmpMin < 0) {
          // query point is before the start of this cell
          nextQueryPoint = iterator.next();
          continue;
        }
        scratch.bytes = maxPackedValue;
        int cmpMax = nextQueryPoint.compareTo(scratch);
        if (cmpMax > 0) {
          // query point is after the end of this cell
          return Relation.CELL_OUTSIDE_QUERY;
        }

        if (cmpMin == 0 && cmpMax == 0) {
          // NOTE: we only hit this if we are on a cell whose min and max values are exactly equal to our point,
          // which can easily happen if many (> 1024) docs share this one value
          return Relation.CELL_INSIDE_QUERY;
        } else {
          return Relation.CELL_CROSSES_QUERY;
        }
      }

      // We exhausted all points in the query:
      return Relation.CELL_OUTSIDE_QUERY;
    }
  }

  /** IntersectVisitor that queries against a highly degenerate shape: a single point.  This is used in the > 1D case. */
  private class SinglePointVisitor implements IntersectVisitor {

    private final DocIdSetBuilder result;
    private final byte[] pointBytes;
    private DocIdSetBuilder.BulkAdder adder;

    public SinglePointVisitor(DocIdSetBuilder result) {
      this.result = result;
      this.pointBytes = new byte[bytesPerDim * numDims];
    }

    public void setPoint(BytesRef point) {
      // we verified this up front in query's ctor:
      assert point.length == pointBytes.length;
      System.arraycopy(point.bytes, point.offset, pointBytes, 0, pointBytes.length);
    }

    @Override
    public void grow(int count) {
      adder = result.grow(count);
    }

    @Override
    public void visit(int docID) {
      adder.add(docID);
    }

    @Override
    public void visit(int docID, byte[] packedValue) {
      assert packedValue.length == pointBytes.length;
      if (Arrays.equals(packedValue, pointBytes)) {
        // The point for this doc matches the point we are querying on
        adder.add(docID);
      }
    }

    @Override
    public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {

      boolean crosses = false;

      for(int dim=0;dim<numDims;dim++) {
        int offset = dim*bytesPerDim;

        int cmpMin = StringHelper.compare(bytesPerDim, minPackedValue, offset, pointBytes, offset);
        if (cmpMin > 0) {
          return Relation.CELL_OUTSIDE_QUERY;
        }

        int cmpMax = StringHelper.compare(bytesPerDim, maxPackedValue, offset, pointBytes, offset);
        if (cmpMax < 0) {
          return Relation.CELL_OUTSIDE_QUERY;
        }

        if (cmpMin != 0 || cmpMax != 0) {
          crosses = true;
        }
      }

      if (crosses) {
        return Relation.CELL_CROSSES_QUERY;
      } else {
        // NOTE: we only hit this if we are on a cell whose min and max values are exactly equal to our point,
        // which can easily happen if many docs share this one value
        return Relation.CELL_INSIDE_QUERY;
      }
    }
  }

  public Collection<byte[]> getPackedPoints() {
    return new AbstractCollection<byte[]>() {

      @Override
      public Iterator<byte[]> iterator() {
        int size = (int) sortedPackedPoints.size();
        PrefixCodedTerms.TermIterator iterator = sortedPackedPoints.iterator();
        return new Iterator<byte[]>() {

          int upto = 0;

          @Override
          public boolean hasNext() {
            return upto < size;
          }

          @Override
          public byte[] next() {
            if (upto == size) {
              throw new NoSuchElementException();
            }

            upto++;
            BytesRef next = iterator.next();
            return Arrays.copyOfRange(next.bytes, next.offset, next.length);
          }
        };
      }

      @Override
      public int size() {
        return (int) sortedPackedPoints.size();
      }
    };
  }

  public String getField() {
    return field;
  }

  public int getNumDims() {
    return numDims;
  }

  public int getBytesPerDim() {
    return bytesPerDim;
  }

  @Override
  public final int hashCode() {
    int hash = classHash();
    hash = 31 * hash + field.hashCode();
    hash = 31 * hash + sortedPackedPointsHashCode;
    hash = 31 * hash + numDims;
    hash = 31 * hash + bytesPerDim;
    return hash;
  }

  @Override
  public final boolean equals(Object other) {
    return sameClassAs(other) &&
           equalsTo(getClass().cast(other));
  }
  
  private boolean equalsTo(PointInSetQuery other) {
    return other.field.equals(field) &&
           other.numDims == numDims &&
           other.bytesPerDim == bytesPerDim &&
           other.sortedPackedPointsHashCode == sortedPackedPointsHashCode &&
           other.sortedPackedPoints.equals(sortedPackedPoints);
  }

  @Override
  public final String toString(String field) {
    final StringBuilder sb = new StringBuilder();
    if (this.field.equals(field) == false) {
      sb.append(this.field);
      sb.append(':');
    }

    sb.append("{");

    TermIterator iterator = sortedPackedPoints.iterator();
    byte[] pointBytes = new byte[numDims * bytesPerDim];
    boolean first = true;
    for (BytesRef point = iterator.next(); point != null; point = iterator.next()) {
      if (first == false) {
        sb.append(" ");
      }
      first = false;
      System.arraycopy(point.bytes, point.offset, pointBytes, 0, pointBytes.length);
      sb.append(toString(pointBytes));
    }
    sb.append("}");
    return sb.toString();
  }

  /**
   * Returns a string of a single value in a human-readable format for debugging.
   * This is used by {@link #toString()}.
   *
   * The default implementation encodes the individual byte values.
   *
   * @param value single value, never null
   * @return human readable value for debugging
   */
  protected abstract String toString(byte[] value);
}
