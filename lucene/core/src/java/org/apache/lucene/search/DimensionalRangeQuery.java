package org.apache.lucene.search;

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

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import org.apache.lucene.document.DimensionalField;
import org.apache.lucene.index.DimensionalValues;
import org.apache.lucene.index.DimensionalValues.IntersectVisitor;
import org.apache.lucene.index.DimensionalValues.Relation;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.bkd.BKDUtil;

/** Searches for ranges in fields previously indexed using {@link DimensionalField}.  In
 *  a 1D field this is a simple range query; in a multi-dimensional field it's a box shape. */

public class DimensionalRangeQuery extends Query {
  final String field;
  final int numDims;
  final byte[][] lowerPoint;
  final boolean[] lowerInclusive;
  final byte[][] upperPoint;
  final boolean[] upperInclusive;
  // This is null only in the "fully open range" case
  final Integer bytesPerDim;

  public DimensionalRangeQuery(String field,
                               byte[][] lowerPoint, boolean[] lowerInclusive,
                               byte[][] upperPoint, boolean[] upperInclusive) {
    this.field = field;
    if (lowerPoint == null) {
      throw new IllegalArgumentException("lowerPoint must not be null");
    }
    if (upperPoint == null) {
      throw new IllegalArgumentException("upperPoint must not be null");
    }
    numDims = lowerPoint.length;
    if (upperPoint.length != numDims) {
      throw new IllegalArgumentException("lowerPoint has length=" + numDims + " but upperPoint has different length=" + upperPoint.length);
    }
    this.lowerPoint = lowerPoint;
    this.lowerInclusive = lowerInclusive;
    this.upperPoint = upperPoint;
    this.upperInclusive = upperInclusive;

    int bytesPerDim = -1;
    for(byte[] value : lowerPoint) {
      if (value != null) {
        if (bytesPerDim == -1) {
          bytesPerDim = value.length;
        } else if (value.length != bytesPerDim) {
          throw new IllegalArgumentException("all dimensions must have same bytes length, but saw " + bytesPerDim + " and " + value.length);
        }
      }
    }
    for(byte[] value : upperPoint) {
      if (value != null) {
        if (bytesPerDim == -1) {
          bytesPerDim = value.length;
        } else if (value.length != bytesPerDim) {
          throw new IllegalArgumentException("all dimensions must have same bytes length, but saw " + bytesPerDim + " and " + value.length);
        }
      }
    }
    if (bytesPerDim == -1) {
      this.bytesPerDim = null;
    } else {
      this.bytesPerDim = bytesPerDim;
    }
  }

  /** Sugar constructor: use in the 1D case when you indexed 1D long values using {@link DimensionalField} */
  public DimensionalRangeQuery(String field, Long lowerValue, boolean lowerInclusive, Long upperValue, boolean upperInclusive) {
    this(field, pack(lowerValue), new boolean[] {lowerInclusive}, pack(upperValue), new boolean[] {upperInclusive});
  }

  /** Sugar constructor: use in the 1D case when you indexed binary values using {@link DimensionalField} */
  public DimensionalRangeQuery(String field, byte[] lowerValue, boolean lowerInclusive, byte[] upperValue, boolean upperInclusive) {
    this(field, new byte[][] {lowerValue}, new boolean[] {lowerInclusive}, new byte[][] {upperValue}, new boolean[] {upperInclusive});
  }

  private static byte[][] pack(Long value) {
    if (value == null) {
      // OK: open ended range
      return new byte[1][];
    }
    byte[][] result = new byte[][] {new byte[RamUsageEstimator.NUM_BYTES_LONG]};
    BKDUtil.longToBytes(value, result[0], 0);
    return result;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {

    // We don't use RandomAccessWeight here: it's no good to approximate with "match all docs".
    // This is an inverted structure and should be used in the first pass:

    return new ConstantScoreWeight(this) {

      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        LeafReader reader = context.reader();
        DimensionalValues values = reader.getDimensionalValues();
        if (values == null) {
          // No docs in this segment indexed any field dimensionally
          return null;
        }
        FieldInfo fieldInfo = reader.getFieldInfos().fieldInfo(field);
        if (fieldInfo == null) {
          // No docs in this segment indexed this field at all
          return null;
        }
        if (fieldInfo.getDimensionCount() != numDims) {
          throw new IllegalArgumentException("field=\"" + field + "\" was indexed with numDims=" + fieldInfo.getDimensionCount() + " but this query has numDims=" + numDims);
        }
        if (bytesPerDim != null && bytesPerDim.intValue() != fieldInfo.getDimensionNumBytes()) {
          throw new IllegalArgumentException("field=\"" + field + "\" was indexed with bytesPerDim=" + fieldInfo.getDimensionNumBytes() + " but this query has bytesPerDim=" + bytesPerDim);
        }
        int bytesPerDim = fieldInfo.getDimensionNumBytes();

        byte[] packedLowerIncl = new byte[numDims * bytesPerDim];
        byte[] packedUpperIncl = new byte[numDims * bytesPerDim];

        byte[] minValue = new byte[bytesPerDim];
        byte[] maxValue = new byte[bytesPerDim];
        Arrays.fill(maxValue, (byte) 0xff);

        byte[] one = new byte[bytesPerDim];
        one[bytesPerDim-1] = 1;

        // Carefully pack lower and upper bounds, taking care of per-dim inclusive:
        for(int dim=0;dim<numDims;dim++) {
          if (lowerPoint[dim] != null) {
            if (lowerInclusive[dim] == false) {
              if (Arrays.equals(lowerPoint[dim], maxValue)) {
                return null;
              } else {
                byte[] value = new byte[bytesPerDim];
                BKDUtil.add(bytesPerDim, 0, lowerPoint[dim], one, value);
                System.arraycopy(value, 0, packedLowerIncl, dim*bytesPerDim, bytesPerDim);
              }
            } else {
              System.arraycopy(lowerPoint[dim], 0, packedLowerIncl, dim*bytesPerDim, bytesPerDim);
            }
          } else {
            // Open-ended range: we just leave 0s in this packed dim for the lower value
          }

          if (upperPoint[dim] != null) {
            if (upperInclusive[dim] == false) {
              if (Arrays.equals(upperPoint[dim], minValue)) {
                return null;
              } else {
                byte[] value = new byte[bytesPerDim];
                BKDUtil.subtract(bytesPerDim, 0, upperPoint[dim], one, value);
                System.arraycopy(value, 0, packedUpperIncl, dim*bytesPerDim, bytesPerDim);
              }
            } else {
              System.arraycopy(upperPoint[dim], 0, packedUpperIncl, dim*bytesPerDim, bytesPerDim);
            }
          } else {
            // Open-ended range: fill with max point for this dim:
            System.arraycopy(maxValue, 0, packedUpperIncl, dim*bytesPerDim, bytesPerDim);
          }
        }

        // Now packedLowerIncl and packedUpperIncl are inclusive, and non-empty space:

        DocIdSetBuilder result = new DocIdSetBuilder(reader.maxDoc());

        int[] hitCount = new int[1];
        values.intersect(field,
                         new IntersectVisitor() {

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
                             for(int dim=0;dim<numDims;dim++) {
                               int offset = dim*bytesPerDim;
                               if (StringHelper.compare(bytesPerDim, packedValue, offset, packedLowerIncl, offset) < 0) {
                                 // Doc's value is too low, in this dimension
                                 return;
                               }
                               if (StringHelper.compare(bytesPerDim, packedValue, offset, packedUpperIncl, offset) > 0) {
                                 // Doc's value is too high, in this dimension
                                 return;
                               }
                             }

                             // Doc is in-bounds
                             hitCount[0]++;
                             result.add(docID);
                           }

                           @Override
                           public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {

                             boolean crosses = false;

                             for(int dim=0;dim<numDims;dim++) {
                               int offset = dim*bytesPerDim;

                               if (StringHelper.compare(bytesPerDim, minPackedValue, offset, packedUpperIncl, offset) > 0 ||
                                   StringHelper.compare(bytesPerDim, maxPackedValue, offset, packedLowerIncl, offset) < 0) {
                                 return Relation.CELL_OUTSIDE_QUERY;
                               }

                               crosses |= StringHelper.compare(bytesPerDim, minPackedValue, offset, packedLowerIncl, offset) < 0 ||
                                 StringHelper.compare(bytesPerDim, maxPackedValue, offset, packedUpperIncl, offset) > 0;
                             }

                             if (crosses) {
                               return Relation.CELL_CROSSES_QUERY;
                             } else {
                               return Relation.CELL_INSIDE_QUERY;
                             }
                           }
                         });

        // NOTE: hitCount[0] will be over-estimate in multi-valued case
        return new ConstantScoreScorer(this, score(), result.build(hitCount[0]).iterator());
      }
    };
  }

  @Override
  public int hashCode() {
    int hash = super.hashCode();
    hash += Arrays.hashCode(lowerPoint)^0x14fa55fb;
    hash += Arrays.hashCode(upperPoint)^0x733fa5fe;
    hash += Arrays.hashCode(lowerInclusive)^0x14fa55fb;
    hash += Arrays.hashCode(upperInclusive)^0x733fa5fe;
    hash += numDims^0x14fa55fb;
    hash += Objects.hashCode(bytesPerDim);
    return hash;
  }

  @Override
  public boolean equals(Object other) {
    if (super.equals(other)) {
      final DimensionalRangeQuery q = (DimensionalRangeQuery) other;
      return q.numDims == numDims &&
        q.bytesPerDim == bytesPerDim &&
        Arrays.equals(lowerPoint, q.lowerPoint) &&
        Arrays.equals(lowerInclusive, q.lowerInclusive) &&
        Arrays.equals(upperPoint, q.upperPoint) &&
        Arrays.equals(upperInclusive, q.upperInclusive);
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

    return sb.append('[')
      .append(Arrays.toString(lowerPoint))
      .append(" TO ")
      .append(Arrays.toString(upperPoint))
      .append(']')
      .toString();
  }
}
