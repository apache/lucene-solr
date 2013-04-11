package org.apache.lucene.spatial.prefix;

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

import com.spatial4j.core.shape.Shape;
import org.apache.lucene.search.Filter;
import org.apache.lucene.spatial.DisjointSpatialFilter;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.apache.lucene.spatial.query.UnsupportedSpatialOperation;

/**
 * A {@link PrefixTreeStrategy} which uses {@link AbstractVisitingPrefixTreeFilter}.
 * This strategy has support for searching non-point shapes (note: not tested).
 * Even a query shape with distErrPct=0 (fully precise to the grid) should have
 * good performance for typical data, unless there is a lot of indexed data
 * coincident with the shape's edge.
 *
 * @lucene.experimental
 */
public class RecursivePrefixTreeStrategy extends PrefixTreeStrategy {

  private int prefixGridScanLevel;

  public RecursivePrefixTreeStrategy(SpatialPrefixTree grid, String fieldName) {
    super(grid, fieldName,
        true);//simplify indexed cells
    prefixGridScanLevel = grid.getMaxLevels() - 4;//TODO this default constant is dependent on the prefix grid size
  }

  /**
   * Sets the grid level [1-maxLevels] at which indexed terms are scanned brute-force
   * instead of by grid decomposition.  By default this is maxLevels - 4.  The
   * final level, maxLevels, is always scanned.
   *
   * @param prefixGridScanLevel 1 to maxLevels
   */
  public void setPrefixGridScanLevel(int prefixGridScanLevel) {
    //TODO if negative then subtract from maxlevels
    this.prefixGridScanLevel = prefixGridScanLevel;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName()+"(prefixGridScanLevel:"+prefixGridScanLevel+",SPG:("+ grid +"))";
  }

  @Override
  public Filter makeFilter(SpatialArgs args) {
    final SpatialOperation op = args.getOperation();
    if (op == SpatialOperation.IsDisjointTo)
      return new DisjointSpatialFilter(this, args, getFieldName());

    Shape shape = args.getShape();
    int detailLevel = grid.getLevelForDistance(args.resolveDistErr(ctx, distErrPct));
    final boolean hasIndexedLeaves = true;

    if (op == SpatialOperation.Intersects) {
      return new IntersectsPrefixTreeFilter(
          shape, getFieldName(), grid, detailLevel, prefixGridScanLevel,
          hasIndexedLeaves);
    } else if (op == SpatialOperation.IsWithin) {
      return new WithinPrefixTreeFilter(
          shape, getFieldName(), grid, detailLevel, prefixGridScanLevel,
          -1);//-1 flag is slower but ensures correct results
    } else if (op == SpatialOperation.Contains) {
      return new ContainsPrefixTreeFilter(shape, getFieldName(), grid, detailLevel);
    }
    throw new UnsupportedSpatialOperation(op);
  }
}




