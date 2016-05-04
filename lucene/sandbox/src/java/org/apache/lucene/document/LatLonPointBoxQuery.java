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
import java.util.Arrays;
import java.util.Objects;

import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.StringHelper;

/** 
 * Fast version of {@link PointRangeQuery}. It is fast for actual range queries!
 * @lucene.experimental
 */
abstract class LatLonPointBoxQuery extends Query {
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
  protected LatLonPointBoxQuery(String field, byte[] lowerPoint, byte[] upperPoint, int numDims) {
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
  public final Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {

    // We don't use RandomAccessWeight here: it's no good to approximate with "match all docs".
    // This is an inverted structure and should be used in the first pass:

    return new ConstantScoreWeight(this) {

      private DocIdSetIterator buildMatchingIterator(LeafReader reader, PointValues values) throws IOException {
        MatchingPoints result = new MatchingPoints(reader, field);

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
                result.add(docID);
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
        return result.iterator();
      }

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

        boolean allDocsMatch;
        if (values.getDocCount(field) == reader.maxDoc()) {
          final byte[] fieldPackedLower = values.getMinPackedValue(field);
          final byte[] fieldPackedUpper = values.getMaxPackedValue(field);
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
          iterator = buildMatchingIterator(reader, values);
        }

        return new ConstantScoreScorer(this, score(), iterator);
      }
    };
  }

  @Override
  public final int hashCode() {
    int hash = super.hashCode();
    hash = 31 * hash + field.hashCode();
    hash = 31 * hash + Arrays.hashCode(lowerPoint);
    hash = 31 * hash + Arrays.hashCode(upperPoint);
    hash = 31 * hash + numDims;
    hash = 31 * hash + Objects.hashCode(bytesPerDim);
    return hash;
  }

  @Override
  public final boolean equals(Object other) {
    if (super.equals(other) == false) {
      return false;
    }

    final LatLonPointBoxQuery q = (LatLonPointBoxQuery) other;
    if (field.equals(q.field) == false) {
      return false;
    }

    if (q.numDims != numDims) {
      return false;
    }

    if (q.bytesPerDim != bytesPerDim) {
      return false;
    }

    if (Arrays.equals(lowerPoint, q.lowerPoint) == false) {
      return false;
    }
    
    if (Arrays.equals(upperPoint, q.upperPoint) == false) {
      return false;
    }

    return true;
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
