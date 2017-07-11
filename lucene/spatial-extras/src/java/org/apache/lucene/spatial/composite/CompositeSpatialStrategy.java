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
package org.apache.lucene.spatial.composite;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.document.Field;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.apache.lucene.spatial.query.UnsupportedSpatialOperation;
import org.apache.lucene.spatial.serialized.SerializedDVStrategy;
import org.apache.lucene.spatial.util.ShapeValuesPredicate;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Shape;

/**
 * A composite {@link SpatialStrategy} based on {@link RecursivePrefixTreeStrategy} (RPT) and
 * {@link SerializedDVStrategy} (SDV).
 * RPT acts as an index to the precision available in SDV, and in some circumstances can avoid geometry lookups based
 * on where a cell is in relation to the query shape.  Currently the only predicate optimized like this is Intersects.
 * All predicates are supported except for the BBox* ones, and Disjoint.
 *
 * @lucene.experimental
 */
public class CompositeSpatialStrategy extends SpatialStrategy {

  //TODO support others? (BBox)
  private final RecursivePrefixTreeStrategy indexStrategy;

  /** Has the geometry. */ // TODO support others?
  private final SerializedDVStrategy geometryStrategy;
  private boolean optimizePredicates = true;

  public CompositeSpatialStrategy(String fieldName,
                                  RecursivePrefixTreeStrategy indexStrategy, SerializedDVStrategy geometryStrategy) {
    super(indexStrategy.getSpatialContext(), fieldName);//field name; unused
    this.indexStrategy = indexStrategy;
    this.geometryStrategy = geometryStrategy;
  }

  public RecursivePrefixTreeStrategy getIndexStrategy() {
    return indexStrategy;
  }

  public SerializedDVStrategy getGeometryStrategy() {
    return geometryStrategy;
  }

  public boolean isOptimizePredicates() {
    return optimizePredicates;
  }

  /** Set to false to NOT use optimized search predicates that avoid checking the geometry sometimes. Only useful for
   * benchmarking. */
  public void setOptimizePredicates(boolean optimizePredicates) {
    this.optimizePredicates = optimizePredicates;
  }

  @Override
  public Field[] createIndexableFields(Shape shape) {
    List<Field> fields = new ArrayList<>();
    Collections.addAll(fields, indexStrategy.createIndexableFields(shape));
    Collections.addAll(fields, geometryStrategy.createIndexableFields(shape));
    return fields.toArray(new Field[fields.size()]);
  }

  @Override
  public DoubleValuesSource makeDistanceValueSource(Point queryPoint, double multiplier) {
    //TODO consider indexing center-point in DV?  Guarantee contained by the shape, which could then be used for
    // other purposes like faster WITHIN predicate?
    throw new UnsupportedOperationException();
  }

  @Override
  public Query makeQuery(SpatialArgs args) {
    final SpatialOperation pred = args.getOperation();

    if (pred == SpatialOperation.BBoxIntersects || pred == SpatialOperation.BBoxWithin) {
      throw new UnsupportedSpatialOperation(pred);
    }

    if (pred == SpatialOperation.IsDisjointTo) {
//      final Query intersectQuery = makeQuery(new SpatialArgs(SpatialOperation.Intersects, args.getShape()));
//      DocValues.getDocsWithField(reader, geometryStrategy.getFieldName());
      //TODO resurrect Disjoint spatial query utility accepting a field name known to have DocValues.
      // update class docs when it's added.
      throw new UnsupportedSpatialOperation(pred);
    }

    final ShapeValuesPredicate predicateValueSource =
        new ShapeValuesPredicate(geometryStrategy.makeShapeValueSource(), pred, args.getShape());
    //System.out.println("PredOpt: " + optimizePredicates);
    if (pred == SpatialOperation.Intersects && optimizePredicates) {
      // We have a smart Intersects impl

      final SpatialPrefixTree grid = indexStrategy.getGrid();
      final int detailLevel = grid.getLevelForDistance(args.resolveDistErr(ctx, 0.0));//default to max precision
      return new IntersectsRPTVerifyQuery(args.getShape(), indexStrategy.getFieldName(), grid,
          detailLevel, indexStrategy.getPrefixGridScanLevel(), predicateValueSource);
    } else {
      //The general path; all index matches get verified

      SpatialArgs indexArgs;
      if (pred == SpatialOperation.Contains) {
        // note: we could map IsWithin as well but it's pretty darned slow since it touches all world grids
        indexArgs = args;
      } else {
        //TODO add args.clone method with new predicate? Or simply make non-final?
        indexArgs = new SpatialArgs(SpatialOperation.Intersects, args.getShape());
        indexArgs.setDistErr(args.getDistErr());
        indexArgs.setDistErrPct(args.getDistErrPct());
      }

      if (indexArgs.getDistErr() == null && indexArgs.getDistErrPct() == null) {
        indexArgs.setDistErrPct(0.10);
      }

      final Query indexQuery = indexStrategy.makeQuery(indexArgs);
      return new CompositeVerifyQuery(indexQuery, predicateValueSource);
    }
  }

}
