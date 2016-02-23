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
import org.apache.lucene.document.BinaryPoint;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.StringHelper;

/** Searches for ranges in fields previously indexed using points e.g.
 *  {@link org.apache.lucene.document.LongPoint}.  In a 1D field this is
 *  a simple range query; in a multi-dimensional field it's a box shape. */
// TODO: enhance this and add simple example
public class PointRangeQuery extends Query {
  final String field;
  final int numDims;
  final byte[][] lowerPoint;
  final boolean[] lowerInclusive;
  final byte[][] upperPoint;
  final boolean[] upperInclusive;
  // This is null only in the "fully open range" case
  final Integer bytesPerDim;

  /** 
   * Expert: create a multidimensional range query for point values.
   * <p>
   * This is for subclasses and works on the underlying binary encoding: to
   * create range queries for lucene's standard {@code Point} types, refer to these factory methods:
   * <ul>
   *   <li>{@link #newIntRange newIntRange()}/{@link #newMultiIntRange newMultiIntRange()} for fields indexed with {@link IntPoint}
   *   <li>{@link #newIntRange newLongRange()}/{@link #newMultiIntRange newMultiLongRange()} for fields indexed with {@link LongPoint}
   *   <li>{@link #newIntRange newFloatRange()}/{@link #newMultiIntRange newMultiFloatRange()} for fields indexed with {@link FloatPoint}
   *   <li>{@link #newIntRange newDoubleRange()}/{@link #newMultiIntRange newMultiDoubleRange()} for fields indexed with {@link DoublePoint}
   *   <li>{@link #newIntRange newBinaryRange()}/{@link #newMultiIntRange newMultiBinaryRange()} for fields indexed with {@link BinaryPoint}
   * </ul>
   * <p>
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting a {@code lowerValue} element or {@code upperValue} element to {@code null}. 
   * <p>
   * By setting a dimension's inclusive ({@code lowerInclusive} or {@code upperInclusive}) to false, it will
   * match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
   *
   * @param field field name. must not be {@code null}.
   * @param lowerPoint lower portion of the range. {@code null} values mean "open" for that dimension.
   * @param lowerInclusive {@code true} if the lower portion of the range is inclusive, {@code false} if it should be excluded.
   * @param upperPoint upper portion of the range. {@code null} values mean "open" for that dimension.
   * @param upperInclusive {@code true} if the upper portion of the range is inclusive, {@code false} if it should be excluded.
   * @throws IllegalArgumentException if {@code field} is null, or if {@code lowerValue.length != upperValue.length}
   */
  protected PointRangeQuery(String field,
                         byte[][] lowerPoint, boolean[] lowerInclusive,
                         byte[][] upperPoint, boolean[] upperInclusive) {
    checkArgs(field, lowerPoint, upperPoint);
    this.field = field;
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

  /** Check preconditions for all factory methods */
  private static void checkArgs(String field, Object lowerPoint, Object upperPoint) {
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

  /** 
   * Create a range query for matching an exact integer value.
   * <p>
   * This is for simple one-dimension points, for multidimensional points use
   * {@link #newMultiIntRange newMultiIntRange()} instead.
   *
   * @param field field name. must not be {@code null}.
   * @param value exact value
   * @throws IllegalArgumentException if {@code field} is null.
   * @return a query matching documents with this exact value
   */
  public static PointRangeQuery newIntExact(String field, int value) {
    return newIntRange(field, value, true, value, true);
  }

  /** 
   * Create a range query for integer values indexed with {@link IntPoint}.
   * <p>
   * This is for simple one-dimension ranges, for multidimensional ranges use
   * {@link #newMultiIntRange newMultiIntRange()} instead.
   * <p>
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting the {@code lowerValue} or {@code upperValue} to {@code null}. 
   * <p>
   * By setting inclusive ({@code lowerInclusive} or {@code upperInclusive}) to false, it will
   * match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
   *
   * @param field field name. must not be {@code null}.
   * @param lowerValue lower portion of the range. {@code null} means "open".
   * @param lowerInclusive {@code true} if the lower portion of the range is inclusive, {@code false} if it should be excluded.
   * @param upperValue upper portion of the range. {@code null} means "open".
   * @param upperInclusive {@code true} if the upper portion of the range is inclusive, {@code false} if it should be excluded.
   * @throws IllegalArgumentException if {@code field} is null.
   * @return a query matching documents within this range.
   */
  public static PointRangeQuery newIntRange(String field, Integer lowerValue, boolean lowerInclusive, Integer upperValue, boolean upperInclusive) {
    return newMultiIntRange(field, 
                            new Integer[] { lowerValue },
                            new boolean[] { lowerInclusive }, 
                            new Integer[] { upperValue },
                            new boolean[] { upperInclusive });
  }

  /** 
   * Create a multidimensional range query for integer values indexed with {@link IntPoint}.
   * <p>
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting a {@code lowerValue} element or {@code upperValue} element to {@code null}. 
   * <p>
   * By setting a dimension's inclusive ({@code lowerInclusive} or {@code upperInclusive}) to false, it will
   * match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
   *
   * @param field field name. must not be {@code null}.
   * @param lowerValue lower portion of the range. {@code null} values mean "open" for that dimension.
   * @param lowerInclusive {@code true} if the lower portion of the range is inclusive, {@code false} if it should be excluded.
   * @param upperValue upper portion of the range. {@code null} values mean "open" for that dimension.
   * @param upperInclusive {@code true} if the upper portion of the range is inclusive, {@code false} if it should be excluded.
   * @throws IllegalArgumentException if {@code field} is null, or if {@code lowerValue.length != upperValue.length}
   * @return a query matching documents within this range.
   */
  public static PointRangeQuery newMultiIntRange(String field, Integer[] lowerValue, boolean lowerInclusive[], Integer[] upperValue, boolean upperInclusive[]) {
    checkArgs(field, lowerValue, upperValue);
    return new PointRangeQuery(field, IntPoint.encode(lowerValue), lowerInclusive, IntPoint.encode(upperValue), upperInclusive) {
      @Override
      protected String toString(byte[] value) {
        return IntPoint.decodeDimension(value, 0).toString();
      }
    };
  }

  /** 
   * Create a range query for matching an exact long value.
   * <p>
   * This is for simple one-dimension points, for multidimensional points use
   * {@link #newMultiLongRange newMultiLongRange()} instead.
   *
   * @param field field name. must not be {@code null}.
   * @param value exact value
   * @throws IllegalArgumentException if {@code field} is null.
   * @return a query matching documents with this exact value
   */
  public static PointRangeQuery newLongExact(String field, long value) {
    return newLongRange(field, value, true, value, true);
  }

  /** 
   * Create a range query for long values indexed with {@link LongPoint}.
   * <p>
   * This is for simple one-dimension ranges, for multidimensional ranges use
   * {@link #newMultiLongRange newMultiLongRange()} instead.
   * <p>
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting the {@code lowerValue} or {@code upperValue} to {@code null}. 
   * <p>
   * By setting inclusive ({@code lowerInclusive} or {@code upperInclusive}) to false, it will
   * match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
   *
   * @param field field name. must not be {@code null}.
   * @param lowerValue lower portion of the range. {@code null} means "open".
   * @param lowerInclusive {@code true} if the lower portion of the range is inclusive, {@code false} if it should be excluded.
   * @param upperValue upper portion of the range. {@code null} means "open".
   * @param upperInclusive {@code true} if the upper portion of the range is inclusive, {@code false} if it should be excluded.
   * @throws IllegalArgumentException if {@code field} is null.
   * @return a query matching documents within this range.
   */
  public static PointRangeQuery newLongRange(String field, Long lowerValue, boolean lowerInclusive, Long upperValue, boolean upperInclusive) {
    return newMultiLongRange(field, 
                             new Long[] { lowerValue },
                             new boolean[] { lowerInclusive }, 
                             new Long[] { upperValue },
                             new boolean[] { upperInclusive });
  }

  /** 
   * Create a multidimensional range query for long values indexed with {@link LongPoint}.
   * <p>
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting a {@code lowerValue} element or {@code upperValue} element to {@code null}. 
   * <p>
   * By setting a dimension's inclusive ({@code lowerInclusive} or {@code upperInclusive}) to false, it will
   * match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
   *
   * @param field field name. must not be {@code null}.
   * @param lowerValue lower portion of the range. {@code null} values mean "open" for that dimension.
   * @param lowerInclusive {@code true} if the lower portion of the range is inclusive, {@code false} if it should be excluded.
   * @param upperValue upper portion of the range. {@code null} values mean "open" for that dimension.
   * @param upperInclusive {@code true} if the upper portion of the range is inclusive, {@code false} if it should be excluded.
   * @throws IllegalArgumentException if {@code field} is null, or if {@code lowerValue.length != upperValue.length}
   * @return a query matching documents within this range.
   */
  public static PointRangeQuery newMultiLongRange(String field, Long[] lowerValue, boolean lowerInclusive[], Long[] upperValue, boolean upperInclusive[]) {
    checkArgs(field, lowerValue, upperValue);
    return new PointRangeQuery(field, LongPoint.encode(lowerValue), lowerInclusive, LongPoint.encode(upperValue), upperInclusive) {
      @Override
      protected String toString(byte[] value) {
        return LongPoint.decodeDimension(value, 0).toString();
      }
    };
  }

  /** 
   * Create a range query for matching an exact float value.
   * <p>
   * This is for simple one-dimension points, for multidimensional points use
   * {@link #newMultiFloatRange newMultiFloatRange()} instead.
   *
   * @param field field name. must not be {@code null}.
   * @param value float value
   * @throws IllegalArgumentException if {@code field} is null.
   * @return a query matching documents with this exact value
   */
  public static PointRangeQuery newFloatExact(String field, float value) {
    return newFloatRange(field, value, true, value, true);
  }
  
  /** 
   * Create a range query for float values indexed with {@link FloatPoint}.
   * <p>
   * This is for simple one-dimension ranges, for multidimensional ranges use
   * {@link #newMultiFloatRange newMultiFloatRange()} instead.
   * <p>
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting the {@code lowerValue} or {@code upperValue} to {@code null}. 
   * <p>
   * By setting inclusive ({@code lowerInclusive} or {@code upperInclusive}) to false, it will
   * match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
   *
   * @param field field name. must not be {@code null}.
   * @param lowerValue lower portion of the range. {@code null} means "open".
   * @param lowerInclusive {@code true} if the lower portion of the range is inclusive, {@code false} if it should be excluded.
   * @param upperValue upper portion of the range. {@code null} means "open".
   * @param upperInclusive {@code true} if the upper portion of the range is inclusive, {@code false} if it should be excluded.
   * @throws IllegalArgumentException if {@code field} is null.
   * @return a query matching documents within this range.
   */
  public static PointRangeQuery newFloatRange(String field, Float lowerValue, boolean lowerInclusive, Float upperValue, boolean upperInclusive) {
    return newMultiFloatRange(field, 
                            new Float[] { lowerValue },
                            new boolean[] { lowerInclusive }, 
                            new Float[] { upperValue },
                            new boolean[] { upperInclusive });
  }

  /** 
   * Create a multidimensional range query for float values indexed with {@link FloatPoint}.
   * <p>
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting a {@code lowerValue} element or {@code upperValue} element to {@code null}. 
   * <p>
   * By setting a dimension's inclusive ({@code lowerInclusive} or {@code upperInclusive}) to false, it will
   * match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
   *
   * @param field field name. must not be {@code null}.
   * @param lowerValue lower portion of the range. {@code null} values mean "open" for that dimension.
   * @param lowerInclusive {@code true} if the lower portion of the range is inclusive, {@code false} if it should be excluded.
   * @param upperValue upper portion of the range. {@code null} values mean "open" for that dimension.
   * @param upperInclusive {@code true} if the upper portion of the range is inclusive, {@code false} if it should be excluded.
   * @throws IllegalArgumentException if {@code field} is null, or if {@code lowerValue.length != upperValue.length}
   * @return a query matching documents within this range.
   */
  public static PointRangeQuery newMultiFloatRange(String field, Float[] lowerValue, boolean lowerInclusive[], Float[] upperValue, boolean upperInclusive[]) {
    checkArgs(field, lowerValue, upperValue);
    return new PointRangeQuery(field, FloatPoint.encode(lowerValue), lowerInclusive, FloatPoint.encode(upperValue), upperInclusive) {
      @Override
      protected String toString(byte[] value) {
        return FloatPoint.decodeDimension(value, 0).toString();
      }
    };
  }

  /** 
   * Create a range query for matching an exact double value.
   * <p>
   * This is for simple one-dimension points, for multidimensional points use
   * {@link #newMultiDoubleRange newMultiDoubleRange()} instead.
   *
   * @param field field name. must not be {@code null}.
   * @param value double value
   * @throws IllegalArgumentException if {@code field} is null.
   * @return a query matching documents with this exact value
   */
  public static PointRangeQuery newDoubleExact(String field, double value) {
    return newDoubleRange(field, value, true, value, true);
  }
  
  /** 
   * Create a range query for double values indexed with {@link DoublePoint}.
   * <p>
   * This is for simple one-dimension ranges, for multidimensional ranges use
   * {@link #newMultiDoubleRange newMultiDoubleRange()} instead.
   * <p>
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting the {@code lowerValue} or {@code upperValue} to {@code null}. 
   * <p>
   * By setting inclusive ({@code lowerInclusive} or {@code upperInclusive}) to false, it will
   * match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
   *
   * @param field field name. must not be {@code null}.
   * @param lowerValue lower portion of the range. {@code null} means "open".
   * @param lowerInclusive {@code true} if the lower portion of the range is inclusive, {@code false} if it should be excluded.
   * @param upperValue upper portion of the range. {@code null} means "open".
   * @param upperInclusive {@code true} if the upper portion of the range is inclusive, {@code false} if it should be excluded.
   * @throws IllegalArgumentException if {@code field} is null.
   * @return a query matching documents within this range.
   */
  public static PointRangeQuery newDoubleRange(String field, Double lowerValue, boolean lowerInclusive, Double upperValue, boolean upperInclusive) {
    return newMultiDoubleRange(field, 
                            new Double[] { lowerValue },
                            new boolean[] { lowerInclusive }, 
                            new Double[] { upperValue },
                            new boolean[] { upperInclusive });
  }

  /** 
   * Create a multidimensional range query for double values indexed with {@link DoublePoint}.
   * <p>
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting a {@code lowerValue} element or {@code upperValue} element to {@code null}. 
   * <p>
   * By setting a dimension's inclusive ({@code lowerInclusive} or {@code upperInclusive}) to false, it will
   * match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
   *
   * @param field field name. must not be {@code null}.
   * @param lowerValue lower portion of the range. {@code null} values mean "open" for that dimension.
   * @param lowerInclusive {@code true} if the lower portion of the range is inclusive, {@code false} if it should be excluded.
   * @param upperValue upper portion of the range. {@code null} values mean "open" for that dimension.
   * @param upperInclusive {@code true} if the upper portion of the range is inclusive, {@code false} if it should be excluded.
   * @throws IllegalArgumentException if {@code field} is null, or if {@code lowerValue.length != upperValue.length}
   * @return a query matching documents within this range.
   */
  public static PointRangeQuery newMultiDoubleRange(String field, Double[] lowerValue, boolean lowerInclusive[], Double[] upperValue, boolean upperInclusive[]) {
    checkArgs(field, lowerValue, upperValue);
    return new PointRangeQuery(field, DoublePoint.encode(lowerValue), lowerInclusive, DoublePoint.encode(upperValue), upperInclusive) {
      @Override
      protected String toString(byte[] value) {
        return DoublePoint.decodeDimension(value, 0).toString();
      }
    };
  }

  /** 
   * Create a range query for matching an exact binary value.
   * <p>
   * This is for simple one-dimension points, for multidimensional points use
   * {@link #newMultiBinaryRange newMultiBinaryRange()} instead.
   *
   * @param field field name. must not be {@code null}.
   * @param value binary value
   * @throws IllegalArgumentException if {@code field} is null or {@code value} is null
   * @return a query matching documents with this exact value
   */
  public static PointRangeQuery newBinaryExact(String field, byte[] value) {
    if (value == null) {
      throw new IllegalArgumentException("value cannot be null");
    }
    return newBinaryRange(field, value, true, value, true);
  }

  /** 
   * Create a range query for binary values indexed with {@link BinaryPoint}.
   * <p>
   * This is for simple one-dimension ranges, for multidimensional ranges use
   * {@link #newMultiBinaryRange newMultiBinaryRange()} instead.
   * <p>
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting the {@code lowerValue} or {@code upperValue} to {@code null}. 
   * <p>
   * By setting inclusive ({@code lowerInclusive} or {@code upperInclusive}) to false, it will
   * match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
   *
   * @param field field name. must not be {@code null}.
   * @param lowerValue lower portion of the range. {@code null} means "open".
   * @param lowerInclusive {@code true} if the lower portion of the range is inclusive, {@code false} if it should be excluded.
   * @param upperValue upper portion of the range. {@code null} means "open".
   * @param upperInclusive {@code true} if the upper portion of the range is inclusive, {@code false} if it should be excluded.
   * @throws IllegalArgumentException if {@code field} is null.
   * @return a query matching documents within this range.
   */
  public static PointRangeQuery newBinaryRange(String field, byte[] lowerValue, boolean lowerInclusive, byte[] upperValue, boolean upperInclusive) {
    return newMultiBinaryRange(field, new byte[][] {lowerValue}, new boolean[] {lowerInclusive}, new byte[][] {upperValue}, new boolean[] {upperInclusive});
  }
  
  /** 
   * Create a multidimensional range query for binary values indexed with {@link BinaryPoint}.
   * <p>
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting a {@code lowerValue} element or {@code upperValue} element to {@code null}. 
   * <p>
   * By setting a dimension's inclusive ({@code lowerInclusive} or {@code upperInclusive}) to false, it will
   * match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
   *
   * @param field field name. must not be {@code null}.
   * @param lowerValue lower portion of the range. {@code null} values mean "open" for that dimension.
   * @param lowerInclusive {@code true} if the lower portion of the range is inclusive, {@code false} if it should be excluded.
   * @param upperValue upper portion of the range. {@code null} values mean "open" for that dimension.
   * @param upperInclusive {@code true} if the upper portion of the range is inclusive, {@code false} if it should be excluded.
   * @throws IllegalArgumentException if {@code field} is null, or if {@code lowerValue.length != upperValue.length}
   * @return a query matching documents within this range.
   */
  public static PointRangeQuery newMultiBinaryRange(String field, byte[][] lowerValue, boolean[] lowerInclusive, byte[][] upperValue, boolean[] upperInclusive) {
    checkArgs(field, lowerValue, upperValue);
    return new PointRangeQuery(field, lowerValue, lowerInclusive, upperValue, upperInclusive);
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
        if (bytesPerDim != null && bytesPerDim.intValue() != fieldInfo.getPointNumBytes()) {
          throw new IllegalArgumentException("field=\"" + field + "\" was indexed with bytesPerDim=" + fieldInfo.getPointNumBytes() + " but this query has bytesPerDim=" + bytesPerDim);
        }
        int bytesPerDim = fieldInfo.getPointNumBytes();

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
                NumericUtils.add(bytesPerDim, 0, lowerPoint[dim], one, value);
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
                NumericUtils.subtract(bytesPerDim, 0, upperPoint[dim], one, value);
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
      final PointRangeQuery q = (PointRangeQuery) other;
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
    if (this.field.equals(field) == false) {
      sb.append(this.field);
      sb.append(':');
    }

    // print ourselves as "range per dimension"
    for (int i = 0; i < numDims; i++) {
      if (i > 0) {
        sb.append(',');
      }

      if (lowerInclusive[i]) {
        sb.append('[');
      } else {
        sb.append('{');
      }

      if (lowerPoint[i] == null) {
        sb.append('*');
      } else {
        sb.append(toString(lowerPoint[i]));
      }

      sb.append(" TO ");

      if (upperPoint[i] == null) {
        sb.append('*');
      } else {
        sb.append(toString(upperPoint[i]));
      }

      if (upperInclusive[i]) {
        sb.append(']');
      } else {
        sb.append('}');
      }
    }

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
  protected String toString(byte[] value) {
    assert value != null;
    StringBuilder sb = new StringBuilder();
    sb.append("binary(");
    for (int i = 0; i < value.length; i++) {
      if (i > 0) {
        sb.append(' ');
      }
      sb.append(Integer.toHexString(value[i] & 0xFF));
    }
    sb.append(')');
    return sb.toString();
  }
}
