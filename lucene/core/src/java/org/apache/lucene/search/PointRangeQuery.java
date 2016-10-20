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
import org.apache.lucene.document.IntPoint;    // javadocs
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
 * @see PointValues
 * @lucene.experimental
 */
public abstract class PointRangeQuery extends Query {
  final String field;
  final int numDims;
  final int bytesPerDim;
  final byte[] lowerPoint;
  final byte[] upperPoint;

  /** 
   * Expert: create a multidimensional range query for point values.
   *
   * @param field field name. must not be {@code null}.
   * @param lowerPoint lower portion of the range (inclusive).
   * @param upperPoint upper portion of the range (inclusive).
   * @param numDims number of dimensions.
   * @throws IllegalArgumentException if {@code field} is null, or if {@code lowerValue.length != upperValue.length}
   */
  protected PointRangeQuery(String field, byte[] lowerPoint, byte[] upperPoint, int numDims) {
    checkArgs(field, lowerPoint, upperPoint);
    this.field = field;
    if (numDims <= 0) {
      throw new IllegalArgumentException("numDims must be positive, got " + numDims);
    }
    if (lowerPoint.length == 0) {
      throw new IllegalArgumentException("lowerPoint has length of zero");
    }
    if (lowerPoint.length % numDims != 0) {
      throw new IllegalArgumentException("lowerPoint is not a fixed multiple of numDims");
    }
    if (lowerPoint.length != upperPoint.length) {
      throw new IllegalArgumentException("lowerPoint has length=" + lowerPoint.length + " but upperPoint has different length=" + upperPoint.length);
    }
    this.numDims = numDims;
    this.bytesPerDim = lowerPoint.length / numDims;

    this.lowerPoint = lowerPoint;
    this.upperPoint = upperPoint;
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
  public final Weight createWeight(IndexSearcher searcher, boolean needsScores, float boost) throws IOException {

    // We don't use RandomAccessWeight here: it's no good to approximate with "match all docs".
    // This is an inverted structure and should be used in the first pass:

    return new ConstantScoreWeight(this, boost) {

      private DocIdSet buildMatchingDocIdSet(LeafReader reader, PointValues values) throws IOException {
        DocIdSetBuilder result = new DocIdSetBuilder(reader.maxDoc(), values, field);

        values.intersect(
            new IntersectVisitor() {

              DocIdSetBuilder.BulkAdder adder;

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
                for(int dim=0;dim<numDims;dim++) {
                  int offset = dim*bytesPerDim;
                  if (StringHelper.compare(bytesPerDim, packedValue, offset, lowerPoint, offset) < 0) {
                    // Doc's value is too low, in this dimension
                    return;
                  }
                  if (StringHelper.compare(bytesPerDim, packedValue, offset, upperPoint, offset) > 0) {
                    // Doc's value is too high, in this dimension
                    return;
                  }
                }

                // Doc is in-bounds
                adder.add(docID);
              }

              @Override
              public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {

                boolean crosses = false;

                for(int dim=0;dim<numDims;dim++) {
                  int offset = dim*bytesPerDim;

                  if (StringHelper.compare(bytesPerDim, minPackedValue, offset, upperPoint, offset) > 0 ||
                      StringHelper.compare(bytesPerDim, maxPackedValue, offset, lowerPoint, offset) < 0) {
                    return Relation.CELL_OUTSIDE_QUERY;
                  }

                  crosses |= StringHelper.compare(bytesPerDim, minPackedValue, offset, lowerPoint, offset) < 0 ||
                    StringHelper.compare(bytesPerDim, maxPackedValue, offset, upperPoint, offset) > 0;
                }

                if (crosses) {
                  return Relation.CELL_CROSSES_QUERY;
                } else {
                  return Relation.CELL_INSIDE_QUERY;
                }
              }
            });
        return result.build();
      }

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
        if (bytesPerDim != values.getBytesPerDimension()) {
          throw new IllegalArgumentException("field=\"" + field + "\" was indexed with bytesPerDim=" + values.getBytesPerDimension() + " but this query has bytesPerDim=" + bytesPerDim);
        }

        boolean allDocsMatch;
        if (values.getDocCount() == reader.maxDoc()) {
          final byte[] fieldPackedLower = values.getMinPackedValue();
          final byte[] fieldPackedUpper = values.getMaxPackedValue();
          allDocsMatch = true;
          for (int i = 0; i < numDims; ++i) {
            int offset = i * bytesPerDim;
            if (StringHelper.compare(bytesPerDim, lowerPoint, offset, fieldPackedLower, offset) > 0
                || StringHelper.compare(bytesPerDim, upperPoint, offset, fieldPackedUpper, offset) < 0) {
              allDocsMatch = false;
              break;
            }
          }
        } else {
          allDocsMatch = false;
        }

        DocIdSetIterator iterator;
        if (allDocsMatch) {
          // all docs have a value and all points are within bounds, so everything matches
          iterator = DocIdSetIterator.all(reader.maxDoc());
        } else {
          iterator = buildMatchingDocIdSet(reader, values).iterator();
        }

        return new ConstantScoreScorer(this, score(), iterator);
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

  public byte[] getLowerPoint() {
    return lowerPoint.clone();
  }

  public byte[] getUpperPoint() {
    return upperPoint.clone();
  }

  @Override
  public final int hashCode() {
    int hash = classHash();
    hash = 31 * hash + field.hashCode();
    hash = 31 * hash + Arrays.hashCode(lowerPoint);
    hash = 31 * hash + Arrays.hashCode(upperPoint);
    hash = 31 * hash + numDims;
    hash = 31 * hash + Objects.hashCode(bytesPerDim);
    return hash;
  }

  @Override
  public final boolean equals(Object o) {
    return sameClassAs(o) &&
           equalsTo(getClass().cast(o));
  }

  private boolean equalsTo(PointRangeQuery other) {
    return Objects.equals(field, other.field) &&
           numDims == other.numDims &&
           bytesPerDim == other.bytesPerDim &&
           Arrays.equals(lowerPoint, other.lowerPoint) &&
           Arrays.equals(upperPoint, other.upperPoint);
  }

  @Override
  public final String toString(String field) {
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
      
      int startOffset = bytesPerDim * i;

      sb.append('[');
      sb.append(toString(i, Arrays.copyOfRange(lowerPoint, startOffset, startOffset + bytesPerDim)));
      sb.append(" TO ");
      sb.append(toString(i, Arrays.copyOfRange(upperPoint, startOffset, startOffset + bytesPerDim)));
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
