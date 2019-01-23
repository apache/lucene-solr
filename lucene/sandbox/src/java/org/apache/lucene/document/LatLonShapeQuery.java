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

import org.apache.lucene.document.LatLonShape.QueryRelation;
import org.apache.lucene.geo.EdgeTree;
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
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.FixedBitSet;

/**
 * Base LatLonShape Query class providing common query logic for
 * {@link LatLonShapeBoundingBoxQuery} and {@link LatLonShapePolygonQuery}
 *
 * Note: this class implements the majority of the INTERSECTS, WITHIN, DISJOINT and CONTAINS relation logic
 *
 * @lucene.experimental
 **/
abstract class LatLonShapeQuery extends Query {
  /** field name */
  final String field;
  /** query relation
   * disjoint: {@link QueryRelation#DISJOINT},
   * intersects: {@link QueryRelation#INTERSECTS},
   * within: {@link QueryRelation#DISJOINT},
   * contains: {@link QueryRelation#CONTAINS}
   * */
  final LatLonShape.QueryRelation queryRelation;

  protected LatLonShapeQuery(String field, final QueryRelation queryType) {
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
  protected abstract boolean queryMatches(byte[] triangle, LatLonShape.Triangle scratchTriangle, QueryRelation queryRelation);

  /**
   * Checks if the query shape is within the provided triangle.
   * @param triangle the encoded triangle to check
   * @param scratchTriangle triangle helper to be used for decoding the provided encoded triangle.
   * @return the {@link EdgeTree.WithinRelation}
   */
  protected abstract EdgeTree.WithinRelation queryWithin(byte[] triangle, LatLonShape.Triangle scratchTriangle);

  /** relates a range of triangles (internal node) to the query */
  protected Relation relateRangeToQuery(byte[] minTriangle, byte[] maxTriangle, QueryRelation queryRelation) {
    // compute bounding box of internal node
    Relation r = relateRangeBBoxToQuery(LatLonShape.BYTES, 0, minTriangle, 3 * LatLonShape.BYTES, 2 * LatLonShape.BYTES, maxTriangle);
    if (queryRelation == QueryRelation.DISJOINT) {
      return transposeRelation(r);
    } else if (queryRelation == QueryRelation.CONTAINS && r == Relation.CELL_INSIDE_QUERY) {
      return Relation.CELL_OUTSIDE_QUERY;
    }
    return r;
  }

  @Override
  public final Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {

    return new ConstantScoreWeight(this, boost) {

      /** create a visitor that adds documents that match the query using a sparse bitset. (Used by INTERSECT) */
      protected IntersectVisitor getSparseIntersectVisitor(DocIdSetBuilder result) {
        return new IntersectVisitor() {
          final LatLonShape.Triangle scratchTriangle = new LatLonShape.Triangle();
          DocIdSetBuilder.BulkAdder adder;

          @Override
          public void grow(int count) {
            adder = result.grow(count);
          }

          @Override
          public void visit(int docID) throws IOException {
            adder.add(docID);
          }

          @Override
          public void visit(int docID, byte[] t) throws IOException {
            if (queryMatches(t, scratchTriangle, QueryRelation.INTERSECTS)) {
              adder.add(docID);
            }
          }

          @Override
          public Relation compare(byte[] minTriangle, byte[] maxTriangle) {
            return relateRangeToQuery(minTriangle, maxTriangle, QueryRelation.INTERSECTS);
          }
        };
      }

      /** create a visitor that adds documents that match the query using a dense bitset. (Used by WITHIN, DISJOINT) */
      protected IntersectVisitor getDenseIntersectVisitor(FixedBitSet intersect, FixedBitSet disjoint, QueryRelation queryRelation) {
        return new IntersectVisitor() {
          final LatLonShape.Triangle scratchTriangle = new LatLonShape.Triangle();
          @Override
          public void visit(int docID) throws IOException {
            if (queryRelation == QueryRelation.DISJOINT || queryRelation == QueryRelation.CONTAINS) {
              // if DISJOINT or CONTAINS query then set the doc in the disjoint bitset
              // For contains we assume that at least one point on the triangle belongs to the polygon
              disjoint.set(docID);
            } else {
              // for INTERSECT, and WITHIN queries we set the intersect bitset
              intersect.set(docID);
            }
          }

          @Override
          public void visit(int docID, byte[] t) throws IOException {
            if (queryRelation == QueryRelation.CONTAINS) {
              //If disjoint, the relationship is undefined so it is not added to
              // any of the bitsets
              EdgeTree.WithinRelation within = queryWithin(t, scratchTriangle);
              if (within == EdgeTree.WithinRelation.CANDIDATE) {
                intersect.set(docID);
              } else if (within == EdgeTree.WithinRelation.NOTWITHIN) {
                disjoint.set(docID);
              }
            } else {
              if (queryMatches(t, scratchTriangle, queryRelation)) {
                intersect.set(docID);
              } else {
                disjoint.set(docID);
              }
            }
          }

          @Override
          public Relation compare(byte[] minTriangle, byte[] maxTriangle) {
            return relateRangeToQuery(minTriangle, maxTriangle, queryRelation);
          }
        };
      }

      /** get a scorer supplier for INTERSECT queries */
      protected ScorerSupplier getIntersectScorerSupplier(LeafReader reader, PointValues values, Weight weight, ScoreMode scoreMode) throws IOException {
        DocIdSetBuilder result = new DocIdSetBuilder(reader.maxDoc(), values, field);
        IntersectVisitor visitor = getSparseIntersectVisitor(result);
        return new RelationScorerSupplier(values, visitor, null, queryRelation) {
          @Override
          public Scorer get(long leadCost) throws IOException {
            return getIntersectsScorer(LatLonShapeQuery.this, reader, weight, result, score(), scoreMode);
          }
        };
      }

      /** get a scorer supplier for all other queries (DISJOINT, WITHIN, CONTAINS) */
      protected ScorerSupplier getScorerSupplier(LeafReader reader, PointValues values, Weight weight, ScoreMode scoreMode) throws IOException {
        if (queryRelation == QueryRelation.INTERSECTS) {
          return getIntersectScorerSupplier(reader, values, weight, scoreMode);
        }
        FixedBitSet candidates = new FixedBitSet(reader.maxDoc());
        FixedBitSet rejected = new FixedBitSet(reader.maxDoc());
        IntersectVisitor visitor;
        IntersectVisitor disjointVisitor;
        if (queryRelation == QueryRelation.CONTAINS) {
          visitor = getDenseIntersectVisitor(candidates, rejected, queryRelation);
          disjointVisitor = null;
        } else {
          //For within and disjoint we need two passes to remove false positives in case of multi-shapes.
          visitor = getDenseIntersectVisitor(candidates, rejected, QueryRelation.WITHIN);
          disjointVisitor = getDenseIntersectVisitor(candidates, rejected, QueryRelation.DISJOINT);
        }
        return new RelationScorerSupplier(values, visitor, disjointVisitor, queryRelation) {
          @Override
          public Scorer get(long leadCost) throws IOException {
            return getScorer(LatLonShapeQuery.this, weight, candidates, rejected, score(), scoreMode);
          }
        };
      }

      @Override
      public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        LeafReader reader = context.reader();
        PointValues values = reader.getPointValues(field);
        if (values == null) {
          // No docs in this segment had any points fields
          return null;
        }
        FieldInfo fieldInfo = reader.getFieldInfos().fieldInfo(field);
        if (fieldInfo == null) {
          // No docs in this segment indexed this field at all
          return null;
        }

        boolean allDocsMatch = true;
        if (values.getDocCount() != reader.maxDoc() ||
            relateRangeToQuery(values.getMinPackedValue(), values.getMaxPackedValue(), queryRelation) != Relation.CELL_INSIDE_QUERY) {
          allDocsMatch = false;
        }

        final Weight weight = this;
        if (allDocsMatch) {
          return new ScorerSupplier() {
            @Override
            public Scorer get(long leadCost) throws IOException {
              return new ConstantScoreScorer(weight, score(), scoreMode, DocIdSetIterator.all(reader.maxDoc()));
            }

            @Override
            public long cost() {
              return reader.maxDoc();
            }
          };
        } else {
          return getScorerSupplier(reader, values, weight, scoreMode);
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
    return Objects.equals(field, ((LatLonShapeQuery)o).field) && this.queryRelation == ((LatLonShapeQuery)o).queryRelation;
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
    PointValues values;
    IntersectVisitor visitor;
    IntersectVisitor disjointVisitor;//it can be null
    QueryRelation queryRelation;
    long cost = -1;

    RelationScorerSupplier(PointValues values, IntersectVisitor visitor, IntersectVisitor disjointVisitor, QueryRelation queryRelation) {
      this.values = values;
      this.visitor = visitor;
      this.disjointVisitor = disjointVisitor;
      this.queryRelation = queryRelation;
    }

    /** create a visitor that clears documents that do NOT match the polygon query; used with CROSSES */
    private IntersectVisitor getInverseIntersectVisitor(LatLonShapeQuery query, FixedBitSet result, int[] cost) {
      return new IntersectVisitor() {
        LatLonShape.Triangle scratchTriangle = new LatLonShape.Triangle();
        @Override
        public void visit(int docID) {
          result.clear(docID);
          cost[0]--;
        }

        @Override
        public void visit(int docID, byte[] packedTriangle) {
          if (query.queryMatches(packedTriangle, scratchTriangle, QueryRelation.INTERSECTS) == false) {
            result.clear(docID);
            cost[0]--;
          }
        }

        @Override
        public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
          return transposeRelation(query.relateRangeToQuery(minPackedValue, maxPackedValue, QueryRelation.INTERSECTS));
        }
      };
    }

    /** returns a Scorer for INTERSECT queries that uses a sparse bitset */
    protected Scorer getIntersectsScorer(LatLonShapeQuery query, LeafReader reader, Weight weight,
                                         DocIdSetBuilder docIdSetBuilder, final float boost, ScoreMode scoreMode) throws IOException {
      if (values.getDocCount() == reader.maxDoc()
          && values.getDocCount() == values.size()
          && cost() > reader.maxDoc() / 2) {
        // If all docs have exactly one value and the cost is greater
        // than half the leaf size then maybe we can make things faster
        // by computing the set of documents that do NOT match the query
        final FixedBitSet result = new FixedBitSet(reader.maxDoc());
        result.set(0, reader.maxDoc());
        int[] cost = new int[]{reader.maxDoc()};
        values.intersect(getInverseIntersectVisitor(query, result, cost));
        final DocIdSetIterator iterator = new BitSetIterator(result, cost[0]);
        return new ConstantScoreScorer(weight, boost, scoreMode, iterator);
      }

      values.intersect(visitor);
      DocIdSetIterator iterator = docIdSetBuilder.build().iterator();
      return new ConstantScoreScorer(weight, boost, scoreMode, iterator);
    }

    /** returns a Scorer for all other (non INTERSECT) queries */
    protected Scorer getScorer(LatLonShapeQuery query, Weight weight,
                               FixedBitSet candidates, FixedBitSet rejected, final float boost, ScoreMode scoreMode) throws IOException {
      values.intersect(visitor);
      if (disjointVisitor != null) {
        values.intersect(disjointVisitor);
      }
      DocIdSetIterator iterator;
      if (query.queryRelation == QueryRelation.DISJOINT) {
        rejected.andNot(candidates);
        iterator = new BitSetIterator(rejected, cost());
      } else if (query.queryRelation == QueryRelation.WITHIN || query.queryRelation == QueryRelation.CONTAINS) {
        candidates.andNot(rejected);
        iterator = new BitSetIterator(candidates, cost());
      } else {
        iterator = new BitSetIterator(candidates, cost());
      }
      return new ConstantScoreScorer(weight, boost, scoreMode, iterator);
    }

    @Override
    public long cost() {
      if (cost == -1) {
        // Computing the cost may be expensive, so only do it if necessary
        if (queryRelation == QueryRelation.DISJOINT) {
          cost = values.estimatePointCount(disjointVisitor);
        } else {
          cost = values.estimatePointCount(visitor);
        }
        assert cost >= 0;
      }
      return cost;
    }
  }
}
