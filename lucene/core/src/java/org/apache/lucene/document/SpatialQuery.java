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
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.lucene.document.ShapeField.QueryRelation;
import org.apache.lucene.geo.Component2D;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.search.CollectionTerminatedException;
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
 * Base query class for all spatial geometries: {@link LatLonShape}, {@link LatLonPoint} and {@link
 * XYShape}. In order to create a query, use the factory methods on those classes.
 */
abstract class SpatialQuery extends Query {
  /** field name */
  final String field;
  /**
   * query relation disjoint: {@link QueryRelation#DISJOINT}, intersects: {@link
   * QueryRelation#INTERSECTS}, within: {@link QueryRelation#DISJOINT}, contains: {@link
   * QueryRelation#CONTAINS}
   */
  final QueryRelation queryRelation;

  protected SpatialQuery(String field, final QueryRelation queryRelation) {
    if (field == null) {
      throw new IllegalArgumentException("field must not be null");
    }
    if (queryRelation == null) {
      throw new IllegalArgumentException("queryRelation must not be null");
    }
    this.field = field;
    this.queryRelation = queryRelation;
  }

  /**
   * returns the spatial visitor to be used for this query. Called before generating the query
   * {@link Weight}
   */
  protected abstract SpatialVisitor getSpatialVisitor();

  /** Visitor used for walking the BKD tree. */
  protected abstract static class SpatialVisitor {
    /** relates a range of points (internal node) to the query */
    protected abstract Relation relate(byte[] minPackedValue, byte[] maxPackedValue);

    /** Gets a intersects predicate. Called when constructing a {@link Scorer} */
    protected abstract Predicate<byte[]> intersects();

    /** Gets a within predicate. Called when constructing a {@link Scorer} */
    protected abstract Predicate<byte[]> within();

    /** Gets a contains function. Called when constructing a {@link Scorer} */
    protected abstract Function<byte[], Component2D.WithinRelation> contains();

    private Predicate<byte[]> containsPredicate() {
      final Function<byte[], Component2D.WithinRelation> contains = contains();
      return bytes -> contains.apply(bytes) == Component2D.WithinRelation.CANDIDATE;
    }

    private BiFunction<byte[], byte[], Relation> getInnerFunction(
            ShapeField.QueryRelation queryRelation) {
      if (queryRelation == QueryRelation.DISJOINT) {
        return (minPackedValue, maxPackedValue) ->
                transposeRelation(relate(minPackedValue, maxPackedValue));
      }
      return (minPackedValue, maxPackedValue) -> relate(minPackedValue, maxPackedValue);
    }

    private Predicate<byte[]> getLeafPredicate(ShapeField.QueryRelation queryRelation) {
      switch (queryRelation) {
        case INTERSECTS:
          return intersects();
        case WITHIN:
          return within();
        case DISJOINT:
          return intersects().negate();
        case CONTAINS:
          return containsPredicate();
        default:
          throw new IllegalArgumentException("Unsupported query type :[" + queryRelation + "]");
      }
    }
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(field)) {
      visitor.visitLeaf(this);
    }
  }

  @Override
  public final Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
    final SpatialQuery query = this;
    final SpatialVisitor spatialVisitor = getSpatialVisitor();
    return new ConstantScoreWeight(query, boost) {

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
        final Relation rel =
                spatialVisitor
                        .getInnerFunction(queryRelation)
                        .apply(values.getMinPackedValue(), values.getMaxPackedValue());
        if (rel == Relation.CELL_OUTSIDE_QUERY
                || (rel == Relation.CELL_INSIDE_QUERY && queryRelation == QueryRelation.CONTAINS)) {
          // no documents match the query
          return null;
        } else if (values.getDocCount() == reader.maxDoc() && rel == Relation.CELL_INSIDE_QUERY) {
          // all documents match the query
          return new ScorerSupplier() {
            @Override
            public Scorer get(long leadCost) {
              return new ConstantScoreScorer(
                      weight, score(), scoreMode, DocIdSetIterator.all(reader.maxDoc()));
            }

            @Override
            public long cost() {
              return reader.maxDoc();
            }
          };
        } else {
          if (queryRelation != QueryRelation.INTERSECTS
                  && queryRelation != QueryRelation.CONTAINS
                  && values.getDocCount() != values.size()
                  && hasAnyHits(spatialVisitor, queryRelation, values) == false) {
            // First we check if we have any hits so we are fast in the adversarial case where
            // the shape does not match any documents and we are in the dense case
            return null;
          }
          // walk the tree to get matching documents
          return new RelationScorerSupplier(values, spatialVisitor, queryRelation, field) {
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

  /** class specific equals check */
  protected boolean equalsTo(Object o) {
    return Objects.equals(field, ((SpatialQuery) o).field)
            && this.queryRelation == ((SpatialQuery) o).queryRelation;
  }

  /**
   * transpose the relation; INSIDE becomes OUTSIDE, OUTSIDE becomes INSIDE, CROSSES remains
   * unchanged
   */
  protected static Relation transposeRelation(Relation r) {
    if (r == Relation.CELL_INSIDE_QUERY) {
      return Relation.CELL_OUTSIDE_QUERY;
    } else if (r == Relation.CELL_OUTSIDE_QUERY) {
      return Relation.CELL_INSIDE_QUERY;
    }
    return Relation.CELL_CROSSES_QUERY;
  }

  /**
   * utility class for implementing constant score logic specific to INTERSECT, WITHIN, and DISJOINT
   */
  private abstract static class RelationScorerSupplier extends ScorerSupplier {
    private final PointValues values;
    private final SpatialVisitor spatialVisitor;
    private final QueryRelation queryRelation;
    private final String field;
    private long cost = -1;

    RelationScorerSupplier(
            final PointValues values,
            SpatialVisitor spatialVisitor,
            final QueryRelation queryRelation,
            final String field) {
      this.values = values;
      this.spatialVisitor = spatialVisitor;
      this.queryRelation = queryRelation;
      this.field = field;
    }

    protected Scorer getScorer(
            final LeafReader reader, final Weight weight, final float boost, final ScoreMode scoreMode)
            throws IOException {
      switch (queryRelation) {
        case INTERSECTS:
          return getSparseScorer(reader, weight, boost, scoreMode);
        case CONTAINS:
          return getContainsDenseScorer(reader, weight, boost, scoreMode);
        case WITHIN:
        case DISJOINT:
          return values.getDocCount() == values.size()
                  ? getSparseScorer(reader, weight, boost, scoreMode)
                  : getDenseScorer(reader, weight, boost, scoreMode);
        default:
          throw new IllegalArgumentException("Unsupported query type :[" + queryRelation + "]");
      }
    }

    /** Scorer used for INTERSECTS and single value points */
    private Scorer getSparseScorer(
            final LeafReader reader, final Weight weight, final float boost, final ScoreMode scoreMode)
            throws IOException {
      if (queryRelation == QueryRelation.DISJOINT
              && values.getDocCount() == reader.maxDoc()
              && values.getDocCount() == values.size()
              && cost() > reader.maxDoc() / 2) {
        // If all docs have exactly one value and the cost is greater
        // than half the leaf size then maybe we can make things faster
        // by computing the set of documents that do NOT match the query
        final FixedBitSet result = new FixedBitSet(reader.maxDoc());
        result.set(0, reader.maxDoc());
        final long[] cost = new long[] {reader.maxDoc()};
        values.intersect(getInverseDenseVisitor(spatialVisitor, queryRelation, result, cost));
        final DocIdSetIterator iterator = new BitSetIterator(result, cost[0]);
        return new ConstantScoreScorer(weight, boost, scoreMode, iterator);
      } else if (values.getDocCount() < (values.size() >>> 2)) {
        // we use a dense structure so we can skip already visited documents
        final FixedBitSet result = new FixedBitSet(reader.maxDoc());
        final long[] cost = new long[] {0};
        values.intersect(getIntersectsDenseVisitor(spatialVisitor, queryRelation, result, cost));
        assert cost[0] > 0 || result.cardinality() == 0;
        final DocIdSetIterator iterator =
                cost[0] == 0 ? DocIdSetIterator.empty() : new BitSetIterator(result, cost[0]);
        return new ConstantScoreScorer(weight, boost, scoreMode, iterator);
      } else {
        final DocIdSetBuilder docIdSetBuilder = new DocIdSetBuilder(reader.maxDoc(), values, field);
        values.intersect(getSparseVisitor(spatialVisitor, queryRelation, docIdSetBuilder));
        final DocIdSetIterator iterator = docIdSetBuilder.build().iterator();
        return new ConstantScoreScorer(weight, boost, scoreMode, iterator);
      }
    }

    /** Scorer used for WITHIN and DISJOINT */
    private Scorer getDenseScorer(
            LeafReader reader, Weight weight, final float boost, ScoreMode scoreMode)
            throws IOException {
      final FixedBitSet result = new FixedBitSet(reader.maxDoc());
      final long[] cost;
      if (values.getDocCount() == reader.maxDoc()) {
        cost = new long[] {values.size()};
        // In this case we can spare one visit to the tree, all documents
        // are potential matches
        result.set(0, reader.maxDoc());
        // Remove false positives
        values.intersect(getInverseDenseVisitor(spatialVisitor, queryRelation, result, cost));
      } else {
        cost = new long[] {0};
        // Get potential  documents.
        final FixedBitSet excluded = new FixedBitSet(reader.maxDoc());
        values.intersect(getDenseVisitor(spatialVisitor, queryRelation, result, excluded, cost));
        result.andNot(excluded);
        // Remove false positives, we only care about the inner nodes as intersecting
        // leaf nodes have been already taken into account. Unfortunately this
        // process still reads the leaf nodes.
        values.intersect(getShallowInverseDenseVisitor(spatialVisitor, queryRelation, result));
      }
      assert cost[0] > 0 || result.cardinality() == 0;
      final DocIdSetIterator iterator =
              cost[0] == 0 ? DocIdSetIterator.empty() : new BitSetIterator(result, cost[0]);
      return new ConstantScoreScorer(weight, boost, scoreMode, iterator);
    }

    private Scorer getContainsDenseScorer(
            LeafReader reader, Weight weight, final float boost, ScoreMode scoreMode)
            throws IOException {
      final FixedBitSet result = new FixedBitSet(reader.maxDoc());
      final long[] cost = new long[] {0};
      // Get potential  documents.
      final FixedBitSet excluded = new FixedBitSet(reader.maxDoc());
      values.intersect(
              getContainsDenseVisitor(spatialVisitor, queryRelation, result, excluded, cost));
      result.andNot(excluded);
      assert cost[0] > 0 || result.cardinality() == 0;
      final DocIdSetIterator iterator =
              cost[0] == 0 ? DocIdSetIterator.empty() : new BitSetIterator(result, cost[0]);
      return new ConstantScoreScorer(weight, boost, scoreMode, iterator);
    }

    @Override
    public long cost() {
      if (cost == -1) {
        // Computing the cost may be expensive, so only do it if necessary
        cost = values.estimateDocCount(getEstimateVisitor(spatialVisitor, queryRelation));
        assert cost >= 0;
      }
      return cost;
    }
  }

  /** create a visitor for calculating point count estimates for the provided relation */
  private static IntersectVisitor getEstimateVisitor(
          final SpatialVisitor spatialVisitor, QueryRelation queryRelation) {
    final BiFunction<byte[], byte[], Relation> innerFunction =
            spatialVisitor.getInnerFunction(queryRelation);
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
        return innerFunction.apply(minTriangle, maxTriangle);
      }
    };
  }

  /**
   * create a visitor that adds documents that match the query using a sparse bitset. (Used by
   * INTERSECT when the number of docs <= 4 * number of points )
   */
  private static IntersectVisitor getSparseVisitor(
          final SpatialVisitor spatialVisitor,
          QueryRelation queryRelation,
          final DocIdSetBuilder result) {
    final BiFunction<byte[], byte[], Relation> innerFunction =
            spatialVisitor.getInnerFunction(queryRelation);
    final Predicate<byte[]> leafPredicate = spatialVisitor.getLeafPredicate(queryRelation);
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
      public void visit(int docID, byte[] t) {
        if (leafPredicate.test(t)) {
          visit(docID);
        }
      }

      @Override
      public void visit(DocIdSetIterator iterator, byte[] t) throws IOException {
        if (leafPredicate.test(t)) {
          int docID;
          while ((docID = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            visit(docID);
          }
        }
      }

      @Override
      public Relation compare(byte[] minTriangle, byte[] maxTriangle) {
        return innerFunction.apply(minTriangle, maxTriangle);
      }
    };
  }

  /** Scorer used for INTERSECTS when the number of points > 4 * number of docs */
  private static IntersectVisitor getIntersectsDenseVisitor(
          final SpatialVisitor spatialVisitor,
          QueryRelation queryRelation,
          final FixedBitSet result,
          final long[] cost) {
    final BiFunction<byte[], byte[], Relation> innerFunction =
            spatialVisitor.getInnerFunction(queryRelation);
    final Predicate<byte[]> leafPredicate = spatialVisitor.getLeafPredicate(queryRelation);
    return new IntersectVisitor() {

      @Override
      public void visit(int docID) {
        result.set(docID);
        cost[0]++;
      }

      @Override
      public void visit(int docID, byte[] t) {
        if (result.get(docID) == false) {
          if (leafPredicate.test(t)) {
            visit(docID);
          }
        }
      }

      @Override
      public void visit(DocIdSetIterator iterator, byte[] t) throws IOException {
        if (leafPredicate.test(t)) {
          int docID;
          while ((docID = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            visit(docID);
          }
        }
      }

      @Override
      public Relation compare(byte[] minTriangle, byte[] maxTriangle) {
        return innerFunction.apply(minTriangle, maxTriangle);
      }
    };
  }

  /**
   * create a visitor that adds documents that match the query using a dense bitset; used with
   * WITHIN & DISJOINT
   */
  private static IntersectVisitor getDenseVisitor(
          final SpatialVisitor spatialVisitor,
          final QueryRelation queryRelation,
          final FixedBitSet result,
          final FixedBitSet excluded,
          final long[] cost) {
    final BiFunction<byte[], byte[], Relation> innerFunction =
            spatialVisitor.getInnerFunction(queryRelation);
    final Predicate<byte[]> leafPredicate = spatialVisitor.getLeafPredicate(queryRelation);
    return new IntersectVisitor() {
      @Override
      public void visit(int docID) {
        result.set(docID);
        cost[0]++;
      }

      @Override
      public void visit(int docID, byte[] t) {
        if (excluded.get(docID) == false) {
          if (leafPredicate.test(t)) {
            visit(docID);
          } else {
            excluded.set(docID);
          }
        }
      }

      @Override
      public void visit(DocIdSetIterator iterator, byte[] t) throws IOException {
        boolean matches = leafPredicate.test(t);
        int docID;
        while ((docID = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
          if (matches) {
            visit(docID);
          } else {
            excluded.set(docID);
          }
        }
      }

      @Override
      public Relation compare(byte[] minTriangle, byte[] maxTriangle) {
        return innerFunction.apply(minTriangle, maxTriangle);
      }
    };
  }

  /**
   * create a visitor that adds documents that match the query using a dense bitset; used with
   * CONTAINS
   */
  private static IntersectVisitor getContainsDenseVisitor(
          final SpatialVisitor spatialVisitor,
          final QueryRelation queryRelation,
          final FixedBitSet result,
          final FixedBitSet excluded,
          final long[] cost) {
    final BiFunction<byte[], byte[], Relation> innerFunction =
            spatialVisitor.getInnerFunction(queryRelation);
    final Function<byte[], Component2D.WithinRelation> leafFunction = spatialVisitor.contains();
    return new IntersectVisitor() {
      @Override
      public void visit(int docID) {
        excluded.set(docID);
      }

      @Override
      public void visit(int docID, byte[] t) {
        if (excluded.get(docID) == false) {
          Component2D.WithinRelation within = leafFunction.apply(t);
          if (within == Component2D.WithinRelation.CANDIDATE) {
            cost[0]++;
            result.set(docID);
          } else if (within == Component2D.WithinRelation.NOTWITHIN) {
            excluded.set(docID);
          }
        }
      }

      @Override
      public void visit(DocIdSetIterator iterator, byte[] t) throws IOException {
        Component2D.WithinRelation within = leafFunction.apply(t);
        int docID;
        while ((docID = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
          if (within == Component2D.WithinRelation.CANDIDATE) {
            cost[0]++;
            result.set(docID);
          } else if (within == Component2D.WithinRelation.NOTWITHIN) {
            excluded.set(docID);
          }
        }
      }

      @Override
      public Relation compare(byte[] minTriangle, byte[] maxTriangle) {
        return innerFunction.apply(minTriangle, maxTriangle);
      }
    };
  }

  /**
   * create a visitor that clears documents that do not match the polygon query using a dense
   * bitset; used with WITHIN & DISJOINT
   */
  private static IntersectVisitor getInverseDenseVisitor(
          final SpatialVisitor spatialVisitor,
          final QueryRelation queryRelation,
          final FixedBitSet result,
          final long[] cost) {
    final BiFunction<byte[], byte[], Relation> innerFunction =
            spatialVisitor.getInnerFunction(queryRelation);
    final Predicate<byte[]> leafPredicate = spatialVisitor.getLeafPredicate(queryRelation);
    return new IntersectVisitor() {

      @Override
      public void visit(int docID) {
        result.clear(docID);
        cost[0]--;
      }

      @Override
      public void visit(int docID, byte[] packedTriangle) {
        if (result.get(docID)) {
          if (leafPredicate.test(packedTriangle) == false) {
            visit(docID);
          }
        }
      }

      @Override
      public void visit(DocIdSetIterator iterator, byte[] t) throws IOException {
        if (leafPredicate.test(t) == false) {
          int docID;
          while ((docID = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            visit(docID);
          }
        }
      }

      @Override
      public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
        return transposeRelation(innerFunction.apply(minPackedValue, maxPackedValue));
      }
    };
  }

  /**
   * create a visitor that clears documents that do not match the polygon query using a dense
   * bitset; used with WITHIN & DISJOINT. This visitor only takes into account inner nodes
   */
  private static IntersectVisitor getShallowInverseDenseVisitor(
          final SpatialVisitor spatialVisitor, QueryRelation queryRelation, final FixedBitSet result) {
    final BiFunction<byte[], byte[], Relation> innerFunction =
            spatialVisitor.getInnerFunction(queryRelation);
    ;
    return new IntersectVisitor() {

      @Override
      public void visit(int docID) {
        result.clear(docID);
      }

      @Override
      public void visit(int docID, byte[] packedTriangle) {
        // NO-OP
      }

      @Override
      public void visit(DocIdSetIterator iterator, byte[] t) {
        // NO-OP
      }

      @Override
      public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
        return transposeRelation(innerFunction.apply(minPackedValue, maxPackedValue));
      }
    };
  }

  /**
   * Return true if the query matches at least one document. It creates a visitor that terminates as
   * soon as one or more docs are matched.
   */
  private static boolean hasAnyHits(
          final SpatialVisitor spatialVisitor, QueryRelation queryRelation, final PointValues values)
          throws IOException {
    try {
      final BiFunction<byte[], byte[], Relation> innerFunction =
              spatialVisitor.getInnerFunction(queryRelation);
      final Predicate<byte[]> leafPredicate = spatialVisitor.getLeafPredicate(queryRelation);
      values.intersect(
              new IntersectVisitor() {

                @Override
                public void visit(int docID) {
                  throw new CollectionTerminatedException();
                }

                @Override
                public void visit(int docID, byte[] t) {
                  if (leafPredicate.test(t)) {
                    throw new CollectionTerminatedException();
                  }
                }

                @Override
                public void visit(DocIdSetIterator iterator, byte[] t) {
                  if (leafPredicate.test(t)) {
                    throw new CollectionTerminatedException();
                  }
                }

                @Override
                public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                  Relation rel = innerFunction.apply(minPackedValue, maxPackedValue);
                  if (rel == Relation.CELL_INSIDE_QUERY) {
                    throw new CollectionTerminatedException();
                  }
                  return rel;
                }
              });
    } catch (CollectionTerminatedException e) {
      return true;
    }
    return false;
  }
}
