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
import java.util.Objects;

import org.apache.lucene.document.ShapeField.QueryRelation;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.FixedBitSet;

/**
 * Base query class for all spatial geometries: {@link LatLonShape} and {@link XYShape}.
 *
 * <p>The field must be indexed using either {@link LatLonShape#createIndexableFields} or
 * {@link XYShape#createIndexableFields} and the corresponding factory method must be used:
 * <ul>
 *   <li>{@link LatLonShape#newBoxQuery newBoxQuery()} for matching geo shapes that have some {@link QueryRelation} with a bounding box.
 *   <li>{@link LatLonShape#newLineQuery newLineQuery()} for matching geo shapes that have some {@link QueryRelation} with a linestring.
 *   <li>{@link LatLonShape#newPolygonQuery newPolygonQuery()} for matching geo shapes that have some {@link QueryRelation} with a polygon.
 *   <li>{@link XYShape#newBoxQuery newBoxQuery()} for matching cartesian shapes that have some {@link QueryRelation} with a bounding box.
 *   <li>{@link XYShape#newLineQuery newLineQuery()} for matching cartesian shapes that have some {@link QueryRelation} with a linestring.
 *   <li>{@link XYShape#newPolygonQuery newPolygonQuery()} for matching cartesian shapes that have some {@link QueryRelation} with a polygon.
 * </ul>
 * <p>
 *
 *  @lucene.experimental
 **/
abstract class ShapeQuery extends Query {
  /** field name */
  final String field;
  /** query relation
   * disjoint: {@code CELL_OUTSIDE_QUERY}
   * intersects: {@code CELL_CROSSES_QUERY},
   * within: {@code CELL_WITHIN_QUERY} */
  final QueryRelation queryRelation;

  protected ShapeQuery(String field, final QueryRelation queryType) {
    if (field == null) {
      throw new IllegalArgumentException("field must not be null");
    }
    this.field = field;
    this.queryRelation = queryType;
  }

  /**
   *   relates an internal node (bounding box of a range of triangles) to the target query
   *   Note: logic is specific to query type
   *   see {@link LatLonShapeBoundingBoxQuery#relateRangeToQuery} and {@link LatLonShapePolygonQuery#relateRangeToQuery}
   */
  protected abstract Relation relateRangeBBoxToQuery(int minXOffset, int minYOffset, byte[] minTriangle,
                                                     int maxXOffset, int maxYOffset, byte[] maxTriangle);

  /** returns true if the provided triangle matches the query */
  protected abstract boolean queryMatches(byte[] triangle, int[] scratchTriangle, ShapeField.QueryRelation queryRelation);

  /** relates a range of triangles (internal node) to the query */
  protected Relation relateRangeToQuery(byte[] minTriangle, byte[] maxTriangle, QueryRelation queryRelation) {
    // compute bounding box of internal node
    final Relation r = relateRangeBBoxToQuery(ShapeField.BYTES, 0, minTriangle, 3 * ShapeField.BYTES, 2 * ShapeField.BYTES, maxTriangle);
    if (queryRelation == QueryRelation.DISJOINT) {
      return transposeRelation(r);
    }
    return r;
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(field)) {
      visitor.visitLeaf(this);
    }
  }

  @Override
  public final Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {

    return new ConstantScoreWeight(this, boost) {

      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        final ScorerSupplier scorerSupplier = scorerSupplier(context);
        if (scorerSupplier == null) {
          return null;
        }
        return scorerSupplier.get(Long.MAX_VALUE);
      }

      @Override
      public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        final LeafReader reader = context.reader();
        final PointValues values = reader.getPointValues(field);
        if (values == null) {
          // No docs in this segment had any points fields
          return null;
        }
        final FieldInfo fieldInfo = reader.getFieldInfos().fieldInfo(field);
        if (fieldInfo == null) {
          // No docs in this segment indexed this field at all
          return null;
        }

        final Weight weight = this;
        final Relation rel = relateRangeToQuery(values.getMinPackedValue(), values.getMaxPackedValue(), queryRelation);
        if (rel == Relation.CELL_OUTSIDE_QUERY) {
          // no documents match the query
          return null;
        }
        else if (values.getDocCount() == reader.maxDoc() && rel == Relation.CELL_INSIDE_QUERY) {
          // all documents match the query
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
          // walk the tree to get matching documents
          return new RelationScorerSupplier(values, ShapeQuery.this) {
            @Override
            public Scorer get(long leadCost) throws IOException {
              return getScorer(reader, weight, score(), scoreMode);
            }
          };
        }
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return true;
      }
    };
  }

  /** returns the field name */
  public String getField() {
    return field;
  }

  /** returns the query relation */
  public QueryRelation getQueryRelation() {
    return queryRelation;
  }

  @Override
  public int hashCode() {
    int hash = classHash();
    hash = 31 * hash + field.hashCode();
    hash = 31 * hash + queryRelation.hashCode();
    return hash;
  }

  @Override
  public boolean equals(Object o) {
    return sameClassAs(o) && equalsTo(o);
  }

  protected boolean equalsTo(Object o) {
    return Objects.equals(field, ((ShapeQuery)o).field) && this.queryRelation == ((ShapeQuery)o).queryRelation;
  }

  /** transpose the relation; INSIDE becomes OUTSIDE, OUTSIDE becomes INSIDE, CROSSES remains unchanged */
  private static Relation transposeRelation(Relation r) {
    if (r == Relation.CELL_INSIDE_QUERY) {
      return Relation.CELL_OUTSIDE_QUERY;
    } else if (r == Relation.CELL_OUTSIDE_QUERY) {
      return Relation.CELL_INSIDE_QUERY;
    }
    return Relation.CELL_CROSSES_QUERY;
  }

  /** utility class for implementing constant score logic specific to INTERSECT, WITHIN, and DISJOINT */
  private static abstract class RelationScorerSupplier extends ScorerSupplier {
    final private PointValues values;
    final private ShapeQuery query;
    private long cost = -1;

    RelationScorerSupplier(final PointValues values, final ShapeQuery query) {
      this.values = values;
      this.query = query;
    }

    protected Scorer getScorer(final LeafReader reader, final Weight weight, final float boost, final ScoreMode scoreMode) throws IOException {
      switch (query.getQueryRelation()) {
        case INTERSECTS: return getIntersectsScorer(reader, weight, boost, scoreMode);
        case WITHIN:
        case DISJOINT: return getDenseScorer(reader, weight, boost, scoreMode);
        default: throw new IllegalArgumentException("Unsupported query type :[" + query.getQueryRelation() + "]");
      }
    }

    /** Scorer used for INTERSECTS **/
    private Scorer getIntersectsScorer(final LeafReader reader, final Weight weight, final float boost, final ScoreMode scoreMode) throws IOException {
      if (values.getDocCount() == reader.maxDoc()
          && values.getDocCount() == values.size()
          && cost() > reader.maxDoc() / 2) {
        // If all docs have exactly one value and the cost is greater
        // than half the leaf size then maybe we can make things faster
        // by computing the set of documents that do NOT match the query
        final FixedBitSet result = new FixedBitSet(reader.maxDoc());
        result.set(0, reader.maxDoc());
        final int[] cost = new int[]{reader.maxDoc()};
        values.intersect(getInverseIntersectVisitor(query, result, cost));
        final DocIdSetIterator iterator = new BitSetIterator(result, cost[0]);
        return new ConstantScoreScorer(weight, boost, scoreMode, iterator);
      }
      final DocIdSetBuilder docIdSetBuilder = new DocIdSetBuilder(reader.maxDoc(), values, query.getField());
      values.intersect(getIntersectVisitor(query, docIdSetBuilder));
      final DocIdSetIterator iterator = docIdSetBuilder.build().iterator();
      return new ConstantScoreScorer(weight, boost, scoreMode, iterator);
    }

    /** Scorer used for WITHIN and DISJOINT **/
    private Scorer getDenseScorer(LeafReader reader, Weight weight, final float boost, ScoreMode scoreMode) throws IOException {
      final FixedBitSet result = new FixedBitSet(reader.maxDoc());
      if (values.getDocCount() == reader.maxDoc()) {
        // In this case we can spare one visit to the tree
        result.set(0, reader.maxDoc());
        // Remove false positives
        values.intersect(getInverseDenseVisitor(query, result));
      } else {
        // Get potential disjoint documents
        values.intersect(getDenseVisitor(query, result));
        // Remove false positives
        values.intersect(getInverseDenseVisitor(query, result));
      }
      // TODO: we are calling cost() here which might be expensive and it does return an estimate of the number of points
      // but not an estimate of the number of docs. Maybe we can just give a fix cost of numDocs on the tree???
      final DocIdSetIterator iterator = new BitSetIterator(result, cost());
      return new ConstantScoreScorer(weight, boost, scoreMode, iterator);
    }

    @Override
    public long cost() {
      if (cost == -1) {
        // Computing the cost may be expensive, so only do it if necessary
        cost = values.estimatePointCount(getEstimateVisitor(query));
        assert cost >= 0;
      }
      return cost;
    }
  }

  /** create a visitor for calculating point count estimates for the provided relation */
  private static IntersectVisitor getEstimateVisitor(final ShapeQuery query) {
    return new IntersectVisitor() {
      @Override
      public void visit(int docID) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void visit(int docID, byte[] t) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Relation compare(byte[] minTriangle, byte[] maxTriangle) {
        return query.relateRangeToQuery(minTriangle, maxTriangle, query.getQueryRelation());
      }
    };
  }

  /** create a visitor that adds documents that match the query using a sparse bitset. (Used by INTERSECT) */
  private static IntersectVisitor getIntersectVisitor(final ShapeQuery query, final DocIdSetBuilder result) {
    return new IntersectVisitor() {
      final int[] scratchTriangle = new int[6];
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
      public void visit(int docID, byte[] t) {
        if (query.queryMatches(t, scratchTriangle, QueryRelation.INTERSECTS)) {
          visit(docID);
        }
      }

      @Override
      public void visit(DocIdSetIterator iterator, byte[] t) throws IOException {
        if (query.queryMatches(t, scratchTriangle, QueryRelation.INTERSECTS)) {
          int docID;
          while ((docID = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            visit(docID);
          }
        }
      }

      @Override
      public Relation compare(byte[] minTriangle, byte[] maxTriangle) {
        return query.relateRangeToQuery(minTriangle, maxTriangle, ShapeField.QueryRelation.INTERSECTS);
      }
    };
  }

  /** create a visitor that clears documents that do NOT match the polygon query; used with INTERSECTS */
  private static IntersectVisitor getInverseIntersectVisitor(final ShapeQuery query, final FixedBitSet result, final int[] cost) {
    return new IntersectVisitor() {
      final int[] scratchTriangle = new int[6];

      @Override
      public void visit(int docID) {
        result.clear(docID);
        cost[0]--;
      }

      @Override
      public void visit(int docID, byte[] packedTriangle) {
        if (query.queryMatches(packedTriangle, scratchTriangle, QueryRelation.INTERSECTS) == false) {
          visit(docID);
        }
      }

      @Override
      public void visit(DocIdSetIterator iterator, byte[] t) throws IOException {
        if (query.queryMatches(t, scratchTriangle, QueryRelation.INTERSECTS) == false) {
          int docID;
          while ((docID = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            visit(docID);
          }
        }
      }

      @Override
      public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
        return transposeRelation(query.relateRangeToQuery(minPackedValue, maxPackedValue, QueryRelation.INTERSECTS));
      }
    };
  }

  /** create a visitor that adds documents that match the query using a dense bitset; used with WITHIN & DISJOINT */
  private static IntersectVisitor getDenseVisitor(final ShapeQuery query, final FixedBitSet result) {
    return new IntersectVisitor() {
      final int[] scratchTriangle = new int[6];

      @Override
      public void visit(int docID) {
        result.set(docID);
      }

      @Override
      public void visit(int docID, byte[] t) {
        if (query.queryMatches(t, scratchTriangle, query.getQueryRelation())) {
          result.set(docID);
        }
      }

      @Override
      public void visit(DocIdSetIterator iterator, byte[] t) throws IOException {
        if (query.queryMatches(t, scratchTriangle, query.getQueryRelation())) {
          int docID;
          while ((docID = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            result.set(docID);
          }
        }
      }

      @Override
      public Relation compare(byte[] minTriangle, byte[] maxTriangle) {
        return query.relateRangeToQuery(minTriangle, maxTriangle, query.getQueryRelation());
      }
    };
  }

  /** create a visitor that clears documents that do not match the polygon query using a dense bitset; used with WITHIN & DISJOINT */
  private static IntersectVisitor getInverseDenseVisitor(final ShapeQuery query, final FixedBitSet result) {
    return new IntersectVisitor() {
      final int[] scratchTriangle = new int[6];

      @Override
      public void visit(int docID) {
        result.clear(docID);
      }

      @Override
      public void visit(int docID, byte[] packedTriangle) {
        if (query.queryMatches(packedTriangle, scratchTriangle, query.getQueryRelation()) == false) {
          visit(docID);
        }
      }

      @Override
      public void visit(DocIdSetIterator iterator, byte[] t) throws IOException {
        if (query.queryMatches(t, scratchTriangle, query.getQueryRelation()) == false) {
          int docID;
          while ((docID = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            visit(docID);
          }
        }
      }

      @Override
      public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
        return transposeRelation(query.relateRangeToQuery(minPackedValue, maxPackedValue, query.getQueryRelation()));
      }
    };
  }
}
