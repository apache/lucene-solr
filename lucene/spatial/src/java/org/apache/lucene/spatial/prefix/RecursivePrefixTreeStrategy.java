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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Shape;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.prefix.tree.Cell;
import org.apache.lucene.spatial.prefix.tree.CellIterator;
import org.apache.lucene.spatial.prefix.tree.LegacyCell;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.apache.lucene.spatial.query.UnsupportedSpatialOperation;

/**
 * A {@link PrefixTreeStrategy} which uses {@link AbstractVisitingPrefixTreeQuery}.
 * This strategy has support for searching non-point shapes (note: not tested).
 * Even a query shape with distErrPct=0 (fully precise to the grid) should have
 * good performance for typical data, unless there is a lot of indexed data
 * coincident with the shape's edge.
 *
 * @lucene.experimental
 */
public class RecursivePrefixTreeStrategy extends PrefixTreeStrategy {
  /* Future potential optimizations:

    Each shape.relate(otherShape) result could be cached since much of the same relations will be invoked when
    multiple segments are involved. Do this for "complex" shapes, not cheap ones, and don't cache when disjoint to
    bbox because it's a cheap calc. This is one advantage TermQueryPrefixTreeStrategy has over RPT.

   */

  protected int prefixGridScanLevel;

  //Formerly known as simplifyIndexedCells. Eventually will be removed. Only compatible with RPT
  // and a LegacyPrefixTree.
  protected boolean pruneLeafyBranches = true;

  protected boolean multiOverlappingIndexedShapes = true;

  public RecursivePrefixTreeStrategy(SpatialPrefixTree grid, String fieldName) {
    super(grid, fieldName);
    prefixGridScanLevel = grid.getMaxLevels() - 4;//TODO this default constant is dependent on the prefix grid size
  }

  public int getPrefixGridScanLevel() {
    return prefixGridScanLevel;
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

  public boolean isMultiOverlappingIndexedShapes() {
    return multiOverlappingIndexedShapes;
  }

  /** See {@link ContainsPrefixTreeQuery#multiOverlappingIndexedShapes}. */
  public void setMultiOverlappingIndexedShapes(boolean multiOverlappingIndexedShapes) {
    this.multiOverlappingIndexedShapes = multiOverlappingIndexedShapes;
  }

  public boolean isPruneLeafyBranches() {
    return pruneLeafyBranches;
  }

  /**
   * An optional hint affecting non-point shapes: it will
   * prune away a complete set sibling leaves to their parent (recursively), resulting in ~20-50%
   * fewer indexed cells, and consequently that much less disk and that much faster indexing.
   * So if it's a quad tree and all 4 sub-cells are there marked as a leaf, then they will be
   * removed (pruned) and the parent is marked as a leaf instead.  This occurs recursively on up.  Unfortunately, the
   * current implementation will buffer all cells to do this, so consider disabling for high precision (low distErrPct)
   * shapes. (default=true)
   */
  public void setPruneLeafyBranches(boolean pruneLeafyBranches) {
    this.pruneLeafyBranches = pruneLeafyBranches;
  }

  @Override
  public String toString() {
    StringBuilder str = new StringBuilder(getClass().getSimpleName()).append('(');
    str.append("SPG:(").append(grid.toString()).append(')');
    if (pointsOnly)
      str.append(",pointsOnly");
    if (pruneLeafyBranches)
      str.append(",pruneLeafyBranches");
    if (prefixGridScanLevel != grid.getMaxLevels() - 4)
      str.append(",prefixGridScanLevel:").append(""+prefixGridScanLevel);
    if (!multiOverlappingIndexedShapes)
      str.append(",!multiOverlappingIndexedShapes");
    return str.append(')').toString();
  }

  @Override
  protected Iterator<Cell> createCellIteratorToIndex(Shape shape, int detailLevel, Iterator<Cell> reuse) {
    if (shape instanceof Point || !pruneLeafyBranches)
      return super.createCellIteratorToIndex(shape, detailLevel, reuse);

    List<Cell> cells = new ArrayList<>(4096);
    recursiveTraverseAndPrune(grid.getWorldCell(), shape, detailLevel, cells);
    return cells.iterator();
  }

  /** Returns true if cell was added as a leaf. If it wasn't it recursively descends. */
  private boolean recursiveTraverseAndPrune(Cell cell, Shape shape, int detailLevel, List<Cell> result) {
    // Important: this logic assumes Cells don't share anything with other cells when
    // calling cell.getNextLevelCells(). This is only true for LegacyCell.
    if (!(cell instanceof LegacyCell))
      throw new IllegalStateException("pruneLeafyBranches must be disabled for use with grid "+grid);

    if (cell.getLevel() == detailLevel) {
      cell.setLeaf();//FYI might already be a leaf
    }
    if (cell.isLeaf()) {
      result.add(cell);
      return true;
    }
    if (cell.getLevel() != 0)
      result.add(cell);

    int leaves = 0;
    CellIterator subCells = cell.getNextLevelCells(shape);
    while (subCells.hasNext()) {
      Cell subCell = subCells.next();
      if (recursiveTraverseAndPrune(subCell, shape, detailLevel, result))
        leaves++;
    }
    //can we prune?
    if (leaves == ((LegacyCell)cell).getSubCellsSize() && cell.getLevel() != 0) {
      //Optimization: substitute the parent as a leaf instead of adding all
      // children as leaves

      //remove the leaves
      do {
        result.remove(result.size() - 1);//remove last
      } while (--leaves > 0);
      //add cell as the leaf
      cell.setLeaf();
      return true;
    }
    return false;
  }

  @Override
  public Query makeQuery(SpatialArgs args) {
    final SpatialOperation op = args.getOperation();

    Shape shape = args.getShape();
    int detailLevel = grid.getLevelForDistance(args.resolveDistErr(ctx, distErrPct));

    if (op == SpatialOperation.Intersects) {
      return new IntersectsPrefixTreeQuery(
          shape, getFieldName(), grid, detailLevel, prefixGridScanLevel);
    } else if (op == SpatialOperation.IsWithin) {
      return new WithinPrefixTreeQuery(
          shape, getFieldName(), grid, detailLevel, prefixGridScanLevel,
          -1);//-1 flag is slower but ensures correct results
    } else if (op == SpatialOperation.Contains) {
      return new ContainsPrefixTreeQuery(shape, getFieldName(), grid, detailLevel,
          multiOverlappingIndexedShapes);
    }
    throw new UnsupportedSpatialOperation(op);
  }
}




