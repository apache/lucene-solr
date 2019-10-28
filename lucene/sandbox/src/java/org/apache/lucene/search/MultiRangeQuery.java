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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.FutureArrays;

/**
 * Abstract class for range queries involving multiple ranges against physical points such as {@code IntPoints}
 * All ranges are logically ORed together
 * TODO: Add capability for handling overlapping ranges at rewrite time
 * @lucene.experimental
 */
public abstract class MultiRangeQuery extends Query {
  /**
   * Representation of a single clause in a MultiRangeQuery
   */
  public static class RangeClause {
    byte[] lowerValue;
    byte[] upperValue;

    public RangeClause(byte[] lowerValue, byte[] upperValue) {
      this.lowerValue = lowerValue;
      this.upperValue = upperValue;
    }
  }

  /** A builder for multirange queries. */
  public static abstract class Builder {

    protected final String field;
    protected final int bytesPerDim;
    protected final int numDims;
    protected final List<RangeClause> clauses = new ArrayList<>();

    /** Sole constructor. */
    public Builder(String field, int bytesPerDim, int numDims) {
      if (field == null) {
        throw new IllegalArgumentException("field should not be null");
      }
      if (bytesPerDim <= 0) {
        throw new IllegalArgumentException("bytesPerDim should be a valid value");
      }
      if (numDims <= 0) {
        throw new IllegalArgumentException("numDims should be a valid value");
      }

      this.field = field;
      this.bytesPerDim = bytesPerDim;
      this.numDims = numDims;
    }

    /**
     * Add a new clause to this {@link Builder}.
     */
    public Builder add(RangeClause clause) {
      clauses.add(clause);
      return this;
    }

    /**
     * Add a new clause to this {@link Builder}.
     */
    public Builder add(byte[] lowerValue, byte[] upperValue) {
      checkArgs(lowerValue, upperValue);
      return add(new RangeClause(lowerValue, upperValue));
    }

    /** Create a new {@link MultiRangeQuery} based on the parameters that have
     *  been set on this builder. */
    public abstract MultiRangeQuery build();

    /**
     * Check preconditions for all factory methods
     * @throws IllegalArgumentException if {@code field}, {@code lowerPoint} or {@code upperPoint} are null.
     */
    private void checkArgs(Object lowerPoint, Object upperPoint) {
      if (lowerPoint == null) {
        throw new IllegalArgumentException("lowerPoint must not be null");
      }
      if (upperPoint == null) {
        throw new IllegalArgumentException("upperPoint must not be null");
      }
    }
  }

  final String field;
  final int numDims;
  final int bytesPerDim;
  final List<RangeClause> rangeClauses;
  /**
   * Expert: create a multidimensional range query with multiple connected ranges
   *
   * @param field field name. must not be {@code null}.
   * @param rangeClauses Range Clauses for this query
   * @param numDims number of dimensions.
   */
  protected MultiRangeQuery(String field, int numDims, int bytesPerDim, List<RangeClause> rangeClauses) {
    this.field = field;
    this.numDims = numDims;
    this.bytesPerDim = bytesPerDim;
    this.rangeClauses = rangeClauses;
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(field)) {
      visitor.visitLeaf(this);
    }
  }

  /*
   * TODO: Organize ranges similar to how EdgeTree does, to avoid linear scan of ranges
   */
  @Override
  public final Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {

    // We don't use RandomAccessWeight here: it's no good to approximate with "match all docs".
    // This is an inverted structure and should be used in the first pass:

    return new ConstantScoreWeight(this, boost) {

      private PointValues.IntersectVisitor getIntersectVisitor(DocIdSetBuilder result) {
        return new PointValues.IntersectVisitor() {

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
            // If a single OR clause has the value in range, the entire query accepts the value
            for (RangeClause rangeClause : rangeClauses) {
              for (int dim = 0; dim < numDims; dim++) {
                int offset = dim * bytesPerDim;
                if ((FutureArrays.compareUnsigned(packedValue, offset, offset + bytesPerDim, rangeClause.lowerValue, offset, offset + bytesPerDim) >= 0) &&
                  (FutureArrays.compareUnsigned(packedValue, offset, offset + bytesPerDim, rangeClause.upperValue, offset, offset + bytesPerDim) <= 0)) {
                  // Doc is in-bounds. Add and short circuit
                  adder.add(docID);
                  return;
                }
                // Iterate till we have any OR clauses remaining
              }
            }
          }

          @Override
          public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {

            boolean crosses = false;

            /**
             * CROSSES and INSIDE take priority over OUTSIDE. How we calculate the position is:
             * 1) If any range sees the point as inside, return INSIDE.
             * 2) If no range sees the point as inside and atleast one range sees the point as CROSSES, return CROSSES
             * 3) If none of the above, return OUTSIDE
             */
            for (RangeClause rangeClause : rangeClauses) {
              for (int dim = 0; dim < numDims; dim++) {
                int offset = dim * bytesPerDim;

                if ((FutureArrays.compareUnsigned(minPackedValue, offset, offset + bytesPerDim, rangeClause.lowerValue, offset, offset + bytesPerDim) >= 0) &&
                    (FutureArrays.compareUnsigned(maxPackedValue, offset, offset + bytesPerDim, rangeClause.upperValue, offset, offset + bytesPerDim) <= 0)) {
                  return PointValues.Relation.CELL_INSIDE_QUERY;
                }

                crosses |= FutureArrays.compareUnsigned(minPackedValue, offset, offset + bytesPerDim, rangeClause.lowerValue, offset, offset + bytesPerDim) < 0 ||
                    FutureArrays.compareUnsigned(maxPackedValue, offset, offset + bytesPerDim, rangeClause.upperValue, offset, offset + bytesPerDim) > 0;
              }
            }

            if (crosses) {
              return PointValues.Relation.CELL_CROSSES_QUERY;
            } else {
              return PointValues.Relation.CELL_OUTSIDE_QUERY;
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
          for (RangeClause rangeClause : rangeClauses) {
            for (int i = 0; i < numDims; ++i) {
              int offset = i * bytesPerDim;
              if (FutureArrays.compareUnsigned(rangeClause.lowerValue, offset, offset + bytesPerDim, fieldPackedLower, offset, offset + bytesPerDim) > 0
                  || FutureArrays.compareUnsigned(rangeClause.upperValue, offset, offset + bytesPerDim, fieldPackedUpper, offset, offset + bytesPerDim) < 0) {
                allDocsMatch = false;
                break;
              }
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
            final PointValues.IntersectVisitor visitor = getIntersectVisitor(result);
            long cost = -1;

            @Override
            public Scorer get(long leadCost) throws IOException {
              values.intersect(visitor);
              DocIdSetIterator iterator = result.build().iterator();
              return new ConstantScoreScorer(weight, score(), scoreMode, iterator);
            }

            @Override
            public long cost() {
              if (cost == -1) {
                // Computing the cost may be expensive, so only do it if necessary
                cost = values.estimateDocCount(visitor) * rangeClauses.size();
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

  @Override
  public final int hashCode() {
    int hash = classHash();
    hash = 31 * hash + field.hashCode();
    for (RangeClause rangeClause : rangeClauses) {
      hash = 31 * hash + Arrays.hashCode(rangeClause.lowerValue);
      hash = 31 * hash + Arrays.hashCode(rangeClause.lowerValue);
    }
    hash = 31 * hash + numDims;
    hash = 31 * hash + Objects.hashCode(bytesPerDim);
    return hash;
  }

  @Override
  public final boolean equals(Object o) {
    return sameClassAs(o) &&
        equalsTo(getClass().cast(o));
  }

  private boolean equalsTo(MultiRangeQuery other) {
    return Objects.equals(field, other.field) &&
        numDims == other.numDims &&
        bytesPerDim == other.bytesPerDim &&
        rangeClauses.equals(other.rangeClauses);
  }

  @Override
  public final String toString(String field) {
    final StringBuilder sb = new StringBuilder();
    if (this.field.equals(field) == false) {
      sb.append(this.field);
      sb.append(':');
    }

    int count = 0;
    // print ourselves as "range per dimension per value"
    for (RangeClause rangeClause : rangeClauses) {
      if (count > 0) {
        sb.append(',');
      }
      sb.append('{');
      for (int i = 0; i < numDims; i++) {
        if (i > 0) {
          sb.append(',');
        }

        int startOffset = bytesPerDim * i;

        sb.append('[');
        sb.append(toString(i, ArrayUtil.copyOfSubArray(rangeClause.lowerValue, startOffset, startOffset + bytesPerDim)));
        sb.append(" TO ");
        sb.append(toString(i, ArrayUtil.copyOfSubArray(rangeClause.upperValue, startOffset, startOffset + bytesPerDim)));
        sb.append(']');
      }
      sb.append('}');
      ++count;
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
