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
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.FixedBitSet;

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
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(field)) {
      visitor.visitLeaf(this);
    }
  }

  @Override
  public final Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {

    // We don't use RandomAccessWeight here: it's no good to approximate with "match all docs".
    // This is an inverted structure and should be used in the first pass:

    return new ConstantScoreWeight(this, boost) {

      private boolean matches(byte[] packedValue) {
        for(int dim=0;dim<numDims;dim++) {
          int offset = dim*bytesPerDim;
          if (Arrays.compareUnsigned(packedValue, offset, offset + bytesPerDim, lowerPoint, offset, offset + bytesPerDim) < 0) {
            // Doc's value is too low, in this dimension
            return false;
          }
          if (Arrays.compareUnsigned(packedValue, offset, offset + bytesPerDim, upperPoint, offset, offset + bytesPerDim) > 0) {
            // Doc's value is too high, in this dimension
            return false;
          }
        }
        return true;
      }

      private Relation relate(byte[] minPackedValue, byte[] maxPackedValue) {

        boolean crosses = false;

        for(int dim=0;dim<numDims;dim++) {
          int offset = dim*bytesPerDim;

          if (Arrays.compareUnsigned(minPackedValue, offset, offset + bytesPerDim, upperPoint, offset, offset + bytesPerDim) > 0 ||
              Arrays.compareUnsigned(maxPackedValue, offset, offset + bytesPerDim, lowerPoint, offset, offset + bytesPerDim) < 0) {
            return Relation.CELL_OUTSIDE_QUERY;
          }

          crosses |= Arrays.compareUnsigned(minPackedValue, offset, offset + bytesPerDim, lowerPoint, offset, offset + bytesPerDim) < 0 ||
              Arrays.compareUnsigned(maxPackedValue, offset, offset + bytesPerDim, upperPoint, offset, offset + bytesPerDim) > 0;
        }

        if (crosses) {
          return Relation.CELL_CROSSES_QUERY;
        } else {
          return Relation.CELL_INSIDE_QUERY;
        }
      }

      private IntersectVisitor getIntersectVisitor(DocIdSetBuilder result) {
        return new IntersectVisitor() {

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
            if (matches(packedValue)) {
              visit(docID);
            }
          }

          @Override
          public void visit(DocIdSetIterator iterator, byte[] packedValue) throws IOException {
            if (matches(packedValue)) {
              int docID;
              while ((docID = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                visit(docID);
              }
            }
          }

          @Override
          public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
            return relate(minPackedValue, maxPackedValue);
          }
        };
      }

      /**
       * Create a visitor that clears documents that do NOT match the range.
       */
      private IntersectVisitor getInverseIntersectVisitor(FixedBitSet result, int[] cost) {
        return new IntersectVisitor() {

          @Override
          public void visit(int docID) {
            result.clear(docID);
            cost[0]--;
          }

          @Override
          public void visit(int docID, byte[] packedValue) {
            if (matches(packedValue) == false) {
              visit(docID);
            }
          }

          @Override
          public void visit(DocIdSetIterator iterator, byte[] packedValue) throws IOException {
            if (matches(packedValue) == false) {
              int docID;
              while ((docID = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                visit(docID);
              }
            }
          }

          @Override
          public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
             Relation relation = relate(minPackedValue, maxPackedValue);
            switch (relation) {
              case CELL_INSIDE_QUERY:
                // all points match, skip this subtree
                return Relation.CELL_OUTSIDE_QUERY;
              case CELL_OUTSIDE_QUERY:
                // none of the points match, clear all documents
                return Relation.CELL_INSIDE_QUERY;
              default:
                return relation;
            }
          }
        };
      }

      @Override
      public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        LeafReader reader = context.reader();

        PointValues values = reader.getPointValues(field);
        if (values == null) {
          // No docs in this segment/field indexed any points
          return null;
        }

        if (values.getNumIndexDimensions() != numDims) {
          throw new IllegalArgumentException("field=\"" + field + "\" was indexed with numIndexDimensions=" + values.getNumIndexDimensions() + " but this query has numDims=" + numDims);
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
            if (Arrays.compareUnsigned(lowerPoint, offset, offset + bytesPerDim, fieldPackedLower, offset, offset + bytesPerDim) > 0
                || Arrays.compareUnsigned(upperPoint, offset, offset + bytesPerDim, fieldPackedUpper, offset, offset + bytesPerDim) < 0) {
              allDocsMatch = false;
              break;
            }
          }
        } else {
          allDocsMatch = false;
        }

        final Weight weight = this;
        if (allDocsMatch) {
          // all docs have a value and all points are within bounds, so everything matches
          return new ScorerSupplier() {
            @Override
            public Scorer get(long leadCost) {
              return new ConstantScoreScorer(weight, score(), scoreMode, DocIdSetIterator.all(reader.maxDoc()));
            }
            
            @Override
            public long cost() {
              return reader.maxDoc();
            }
          };
        } else {
          return new ScorerSupplier() {

            final DocIdSetBuilder result = new DocIdSetBuilder(reader.maxDoc(), values, field);
            final IntersectVisitor visitor = getIntersectVisitor(result);
            long cost = -1;

            @Override
            public Scorer get(long leadCost) throws IOException {
              if (values.getDocCount() == reader.maxDoc()
                  && values.getDocCount() == values.size()
                  && cost() > reader.maxDoc() / 2) {
                // If all docs have exactly one value and the cost is greater
                // than half the leaf size then maybe we can make things faster
                // by computing the set of documents that do NOT match the range
                final FixedBitSet result = new FixedBitSet(reader.maxDoc());
                result.set(0, reader.maxDoc());
                int[] cost = new int[] { reader.maxDoc() };
                values.intersect(getInverseIntersectVisitor(result, cost));
                final DocIdSetIterator iterator = new BitSetIterator(result, cost[0]);
                return new ConstantScoreScorer(weight, score(), scoreMode, iterator);
              }

              values.intersect(visitor);
              DocIdSetIterator iterator = result.build().iterator();
              return new ConstantScoreScorer(weight, score(), scoreMode, iterator);
            }
            
            @Override
            public long cost() {
              if (cost == -1) {
                // Computing the cost may be expensive, so only do it if necessary
                cost = values.estimateDocCount(visitor);
                assert cost >= 0;
              }
              return cost;
            }
          };
        }
      }

      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        ScorerSupplier scorerSupplier = scorerSupplier(context);
        if (scorerSupplier == null) {
          return null;
        }
        return scorerSupplier.get(Long.MAX_VALUE);
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return true;
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
      sb.append(toString(i, ArrayUtil.copyOfSubArray(lowerPoint, startOffset, startOffset + bytesPerDim)));
      sb.append(" TO ");
      sb.append(toString(i, ArrayUtil.copyOfSubArray(upperPoint, startOffset, startOffset + bytesPerDim)));
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
