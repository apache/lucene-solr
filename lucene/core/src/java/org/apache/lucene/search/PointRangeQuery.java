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

import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.document.BinaryPoint; // javadocs
import org.apache.lucene.document.DoublePoint; // javadocs
import org.apache.lucene.document.FloatPoint;  // javadocs
import org.apache.lucene.document.IntPoint;    // javadocs
import org.apache.lucene.document.LongPoint;   // javadocs
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.StringHelper;

/** 
 * Abstract class for range queries against single or multidimensional points such as
 * {@link IntPoint}.
 * <p>
 * This is for subclasses and works on the underlying binary encoding: to
 * create range queries for lucene's standard {@code Point} types, refer to factory
 * methods on those classes, e.g. {@link IntPoint#newRangeQuery IntPoint.newRangeQuery()} for 
 * fields indexed with {@link IntPoint}.
 * <p>
 * For a single-dimensional field this query is a simple range query; in a multi-dimensional field it's a box shape.
 * @see IntPoint
 * @see LongPoint
 * @see FloatPoint
 * @see DoublePoint
 * @see BinaryPoint 
 *
 * @lucene.experimental
 */
public abstract class PointRangeQuery extends Query {
  final String field;
  final int numDims;
  final int bytesPerDim;
  final byte[][] lowerPoint;
  final byte[][] upperPoint;

  /** 
   * Expert: create a multidimensional range query for point values.
   *
   * @param field field name. must not be {@code null}.
   * @param lowerPoint lower portion of the range (inclusive).
   * @param upperPoint upper portion of the range (inclusive).
   * @throws IllegalArgumentException if {@code field} is null, or if {@code lowerValue.length != upperValue.length}
   */
  protected PointRangeQuery(String field, byte[][] lowerPoint, byte[][] upperPoint) {
    checkArgs(field, lowerPoint, upperPoint);
    this.field = field;
    if (lowerPoint.length == 0) {
      throw new IllegalArgumentException("lowerPoint has length of zero");
    }
    this.numDims = lowerPoint.length;

    if (upperPoint.length != numDims) {
      throw new IllegalArgumentException("lowerPoint has length=" + numDims + " but upperPoint has different length=" + upperPoint.length);
    }
    this.lowerPoint = lowerPoint;
    this.upperPoint = upperPoint;

    if (lowerPoint[0] == null) {
      throw new IllegalArgumentException("lowerPoint[0] is null");
    }
    this.bytesPerDim = lowerPoint[0].length;
    for (int i = 0; i < numDims; i++) {
      if (lowerPoint[i] == null) {
        throw new IllegalArgumentException("lowerPoint[" + i + "] is null");
      }
      if (upperPoint[i] == null) {
        throw new IllegalArgumentException("upperPoint[" + i + "] is null");
      }
      if (lowerPoint[i].length != bytesPerDim) {
        throw new IllegalArgumentException("all dimensions must have same bytes length, but saw " + bytesPerDim + " and " + lowerPoint[i].length);
      }
      if (upperPoint[i].length != bytesPerDim) {
        throw new IllegalArgumentException("all dimensions must have same bytes length, but saw " + bytesPerDim + " and " + upperPoint[i].length);
      }
    }
  }

  /** 
   * Check preconditions for all factory methods
   * @throws IllegalArgumentException if {@code field}, {@code lowerPoint} or {@code upperPoint} are null.
   */
  public static void checkArgs(String field, Object lowerPoint, Object upperPoint) {
    if (field == null) {
      throw new IllegalArgumentException("field must not be null");
    }
    if (lowerPoint == null) {
      throw new IllegalArgumentException("lowerPoint must not be null");
    }
    if (upperPoint == null) {
      throw new IllegalArgumentException("upperPoint must not be null");
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

        byte[] packedLower = new byte[numDims * bytesPerDim];
        byte[] packedUpper = new byte[numDims * bytesPerDim];

        // Carefully pack lower and upper bounds
        for(int dim=0;dim<numDims;dim++) {
          System.arraycopy(lowerPoint[dim], 0, packedLower, dim*bytesPerDim, bytesPerDim);
          System.arraycopy(upperPoint[dim], 0, packedUpper, dim*bytesPerDim, bytesPerDim);
        }

        // Now packedLowerIncl and packedUpperIncl are inclusive, and non-empty space:

        DocIdSetBuilder result = new DocIdSetBuilder(reader.maxDoc());

        values.intersect(field,
                         new IntersectVisitor() {

                           @Override
                           public void grow(int count) {
                             result.grow(count);
                           }

                           @Override
                           public void visit(int docID) {
                             result.add(docID);
                           }

                           @Override
                           public void visit(int docID, byte[] packedValue) {
                             for(int dim=0;dim<numDims;dim++) {
                               int offset = dim*bytesPerDim;
                               if (StringHelper.compare(bytesPerDim, packedValue, offset, packedLower, offset) < 0) {
                                 // Doc's value is too low, in this dimension
                                 return;
                               }
                               if (StringHelper.compare(bytesPerDim, packedValue, offset, packedUpper, offset) > 0) {
                                 // Doc's value is too high, in this dimension
                                 return;
                               }
                             }

                             // Doc is in-bounds
                             result.add(docID);
                           }

                           @Override
                           public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {

                             boolean crosses = false;

                             for(int dim=0;dim<numDims;dim++) {
                               int offset = dim*bytesPerDim;

                               if (StringHelper.compare(bytesPerDim, minPackedValue, offset, packedUpper, offset) > 0 ||
                                   StringHelper.compare(bytesPerDim, maxPackedValue, offset, packedLower, offset) < 0) {
                                 return Relation.CELL_OUTSIDE_QUERY;
                               }

                               crosses |= StringHelper.compare(bytesPerDim, minPackedValue, offset, packedLower, offset) < 0 ||
                                 StringHelper.compare(bytesPerDim, maxPackedValue, offset, packedUpper, offset) > 0;
                             }

                             if (crosses) {
                               return Relation.CELL_CROSSES_QUERY;
                             } else {
                               return Relation.CELL_INSIDE_QUERY;
                             }
                           }
                         });

        return new ConstantScoreScorer(this, score(), result.build().iterator());
      }
    };
  }

  @Override
  public int hashCode() {
    int hash = super.hashCode();
    hash = 31 * hash + Arrays.hashCode(lowerPoint);
    hash = 31 * hash + Arrays.hashCode(upperPoint);
    hash = 31 * hash + numDims;
    hash = 31 * hash + Objects.hashCode(bytesPerDim);
    return hash;
  }

  @Override
  public boolean equals(Object other) {
    if (super.equals(other)) {
      final PointRangeQuery q = (PointRangeQuery) other;
      return q.numDims == numDims &&
        q.bytesPerDim == bytesPerDim &&
        Arrays.equals(lowerPoint, q.lowerPoint) &&
        Arrays.equals(upperPoint, q.upperPoint);
    }

    return false;
  }

  @Override
  public String toString(String field) {
    final StringBuilder sb = new StringBuilder();
    if (this.field.equals(field) == false) {
      sb.append(this.field);
      sb.append(':');
    }

    // print ourselves as "range per dimension"
    for (int i = 0; i < numDims; i++) {
      if (i > 0) {
        sb.append(',');
      }

      sb.append('[');
      sb.append(toString(i, lowerPoint[i]));
      sb.append(" TO ");
      sb.append(toString(i, upperPoint[i]));
      sb.append(']');
    }

    return sb.toString();
  }

  /**
   * Returns a string of a single value in a human-readable format for debugging.
   * This is used by {@link #toString()}.
   *
   * @param dimension dimension of the particular value
   * @param value single value, never null
   * @return human readable value for debugging
   */
  protected abstract String toString(int dimension, byte[] value);
}
