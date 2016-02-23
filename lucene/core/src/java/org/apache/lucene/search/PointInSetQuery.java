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
import java.util.Arrays;
import java.util.Objects;

import org.apache.lucene.document.IntPoint;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PrefixCodedTerms.TermIterator;
import org.apache.lucene.index.PrefixCodedTerms;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.StringHelper;

/** Finds all documents whose point value, previously indexed with e.g. {@link org.apache.lucene.document.LongPoint}, is contained in the
 *  specified set */

// nocommit make abstract
public class PointInSetQuery extends Query {
  // A little bit overkill for us, since all of our "terms" are always in the same field:
  final PrefixCodedTerms sortedPackedPoints;
  final int sortedPackedPointsHashCode;
  final String field;
  final int numDims;
  final int bytesPerDim;

  /** {@code packedPoints} must already be sorted! */
  protected PointInSetQuery(String field, int numDims, int bytesPerDim, BytesRefIterator packedPoints) throws IOException {
    this.field = field;
    // nocommit validate these:
    this.bytesPerDim = bytesPerDim;
    this.numDims = numDims;

    // In the 1D case this works well (the more points, the more common prefixes they share, typically), but in
    // the > 1 D case, where we are only looking at the first dimension's prefix bytes, it can at worst not hurt:
    PrefixCodedTerms.Builder builder = new PrefixCodedTerms.Builder();
    BytesRefBuilder previous = null;
    BytesRef current;
    while ((current = packedPoints.next()) != null) {
      // nocommit make sure a test tests this:
      if (current.length != numDims * bytesPerDim) {
        throw new IllegalArgumentException("packed point length should be " + (numDims * bytesPerDim) + " but got " + current.length + "; field=\"" + field + "\", numDims=" + numDims + " bytesPerDim=" + bytesPerDim);
      }
      if (previous == null) {
        previous = new BytesRefBuilder();
      } else if (previous.get().equals(current)) {
        continue; // deduplicate
      }
      builder.add(field, current);
      previous.copyBytes(current);
    }
    sortedPackedPoints = builder.finish();
    sortedPackedPointsHashCode = sortedPackedPoints.hashCode();
  }

  /** Use in the 1D case when you indexed 1D int values using {@link org.apache.lucene.document.IntPoint} */
  public static PointInSetQuery newIntSet(String field, int... valuesIn) {

    // Don't unexpectedly change the user's incoming array:
    int[] values = valuesIn.clone();

    Arrays.sort(values);

    final BytesRef value = new BytesRef(new byte[Integer.BYTES]);
    value.length = Integer.BYTES;

    try {
      return new PointInSetQuery(field, 1, Integer.BYTES,
                                 new BytesRefIterator() {

                                   int upto;

                                   @Override
                                   public BytesRef next() {
                                     if (upto == values.length) {
                                       return null;
                                     } else {
                                       IntPoint.encodeDimension(values[upto], value.bytes, 0);
                                       upto++;
                                       return value;
                                     }
                                   }
                                 });
    } catch (IOException bogus) {
      // Should never happen ;)
      throw new RuntimeException(bogus);
    }
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {

    // We don't use RandomAccessWeight here: it's no good to approximate with "match all docs".
    // This is an inverted structure and should be used in the first pass:

    return new ConstantScoreWeight(this) {

      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        LeafReader reader = context.reader();
        PointValues values = reader.getPointValues();
        if (values == null) {
          // No docs in this segment indexed any points
          return null;
        }
        FieldInfo fieldInfo = reader.getFieldInfos().fieldInfo(field);
        if (fieldInfo == null) {
          // No docs in this segment indexed this field at all
          return null;
        }
        if (fieldInfo.getPointDimensionCount() != numDims) {
          throw new IllegalArgumentException("field=\"" + field + "\" was indexed with numDims=" + fieldInfo.getPointDimensionCount() + " but this query has numDims=" + numDims);
        }
        if (bytesPerDim != fieldInfo.getPointNumBytes()) {
          throw new IllegalArgumentException("field=\"" + field + "\" was indexed with bytesPerDim=" + fieldInfo.getPointNumBytes() + " but this query has bytesPerDim=" + bytesPerDim);
        }
        int bytesPerDim = fieldInfo.getPointNumBytes();

        DocIdSetBuilder result = new DocIdSetBuilder(reader.maxDoc());

        int[] hitCount = new int[1];

        if (numDims == 1) {

          // We optimize this common case, effectively doing a merge sort of the indexed values vs the queried set:
          values.intersect(field, new MergePointVisitor(sortedPackedPoints.iterator(), hitCount, result));

        } else {
          // NOTE: this is naive implementation, where for each point we re-walk the KD tree to intersect.  We could instead do a similar
          // optimization as the 1D case, but I think it'd mean building a query-time KD tree so we could efficiently intersect against the
          // index, which is probably tricky!
          SinglePointVisitor visitor = new SinglePointVisitor(hitCount, result);
          TermIterator iterator = sortedPackedPoints.iterator();
          for (BytesRef point = iterator.next(); point != null; point = iterator.next()) {
            visitor.setPoint(point);
            values.intersect(field, visitor);
          }
        }

        // NOTE: hitCount[0] will be over-estimate in multi-valued case
        return new ConstantScoreScorer(this, score(), result.build(hitCount[0]).iterator());
      }
    };
  }

  /** Essentially does a merge sort, only collecting hits when the indexed point and query point are the same.  This is an optimization,
   *  used in the 1D case. */
  private class MergePointVisitor implements IntersectVisitor {

    private final DocIdSetBuilder result;
    private final int[] hitCount;
    private final TermIterator iterator;
    private BytesRef nextQueryPoint;
    private final BytesRef scratch = new BytesRef();

    public MergePointVisitor(TermIterator iterator, int[] hitCount, DocIdSetBuilder result) throws IOException {
      this.hitCount = hitCount;
      this.result = result;
      this.iterator = iterator;
      nextQueryPoint = iterator.next();
      scratch.length = bytesPerDim;
    }

    @Override
    public void grow(int count) {
      result.grow(count);
    }

    @Override
    public void visit(int docID) {
      hitCount[0]++;
      result.add(docID);
    }

    @Override
    public void visit(int docID, byte[] packedValue) {
      scratch.bytes = packedValue;
      while (nextQueryPoint != null) {
        int cmp = nextQueryPoint.compareTo(scratch);
        if (cmp == 0) {
          // Query point equals index point, so collect and return
          hitCount[0]++;
          result.add(docID);
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
    private final int[] hitCount;
    public BytesRef point;
    private final byte[] pointBytes;

    public SinglePointVisitor(int[] hitCount, DocIdSetBuilder result) {
      this.hitCount = hitCount;
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
      result.grow(count);
    }

    @Override
    public void visit(int docID) {
      hitCount[0]++;
      result.add(docID);
    }

    @Override
    public void visit(int docID, byte[] packedValue) {
      assert packedValue.length == point.length;
      if (Arrays.equals(packedValue, pointBytes)) {
        // The point for this doc matches the point we are querying on
        hitCount[0]++;
        result.add(docID);
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
        // nocommit make sure tests hit this case:
        // NOTE: we only hit this if we are on a cell whose min and max values are exactly equal to our point,
        // which can easily happen if many docs share this one value
        return Relation.CELL_INSIDE_QUERY;
      }
    }
  }

  @Override
  public int hashCode() {
    int hash = super.hashCode();
    hash += sortedPackedPointsHashCode^0x14fa55fb;
    hash += numDims^0x14fa55fb;
    hash += bytesPerDim^0x14fa55fb;
    return hash;
  }

  @Override
  public boolean equals(Object other) {
    if (super.equals(other)) {
      final PointInSetQuery q = (PointInSetQuery) other;
      return q.numDims == numDims &&
        q.bytesPerDim == bytesPerDim &&
        q.sortedPackedPointsHashCode == sortedPackedPointsHashCode &&
        q.sortedPackedPoints.equals(sortedPackedPoints);
    }

    return false;
  }

  @Override
  public String toString(String field) {
    final StringBuilder sb = new StringBuilder();
    sb.append(getClass().getSimpleName());
    sb.append(':');
    if (this.field.equals(field) == false) {
      sb.append("field=");
      sb.append(this.field);
      sb.append(':');
    }

    TermIterator iterator = sortedPackedPoints.iterator();
    for (BytesRef point = iterator.next(); point != null; point = iterator.next()) {
      sb.append(' ');
      // nocommit fix me to convert back to the numbers/etc.:
      sb.append(point);
    }

    return sb.toString();
  }
}
