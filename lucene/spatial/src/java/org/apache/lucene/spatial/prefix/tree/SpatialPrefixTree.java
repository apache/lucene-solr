package org.apache.lucene.spatial.prefix.tree;

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

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Rectangle;
import com.spatial4j.core.shape.Shape;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * A spatial Prefix Tree, or Trie, which decomposes shapes into prefixed strings
 * at variable lengths corresponding to variable precision.   Each string
 * corresponds to a rectangular spatial region.  This approach is
 * also referred to "Grids", "Tiles", and "Spatial Tiers".
 * <p/>
 * Implementations of this class should be thread-safe and immutable once
 * initialized.
 *
 * @lucene.experimental
 */
public abstract class SpatialPrefixTree {

  protected static final Charset UTF8 = Charset.forName("UTF-8");

  protected final int maxLevels;

  protected final SpatialContext ctx;

  public SpatialPrefixTree(SpatialContext ctx, int maxLevels) {
    assert maxLevels > 0;
    this.ctx = ctx;
    this.maxLevels = maxLevels;
  }

  public SpatialContext getSpatialContext() {
    return ctx;
  }

  public int getMaxLevels() {
    return maxLevels;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(maxLevels:" + maxLevels + ",ctx:" + ctx + ")";
  }

  /**
   * Returns the level of the largest grid in which its longest side is less
   * than or equal to the provided distance (in degrees). Consequently {@code
   * dist} acts as an error epsilon declaring the amount of detail needed in the
   * grid, such that you can get a grid with just the right amount of
   * precision.
   *
   * @param dist >= 0
   * @return level [1 to maxLevels]
   */
  public abstract int getLevelForDistance(double dist);

  /**
   * Given a cell having the specified level, returns the distance from opposite
   * corners. Since this might very depending on where the cell is, this method
   * may over-estimate.
   *
   * @param level [1 to maxLevels]
   * @return > 0
   */
  public double getDistanceForLevel(int level) {
    if (level < 1 || level > getMaxLevels())
      throw new IllegalArgumentException("Level must be in 1 to maxLevels range");
    //TODO cache for each level
    Cell cell = getCell(ctx.getWorldBounds().getCenter(), level);
    Rectangle bbox = cell.getShape().getBoundingBox();
    double width = bbox.getWidth();
    double height = bbox.getHeight();
    //Use standard cartesian hypotenuse. For geospatial, this answer is larger
    // than the correct one but it's okay to over-estimate.
    return Math.sqrt(width * width + height * height);
  }

  private transient Cell worldCell;//cached

  /**
   * Returns the level 0 cell which encompasses all spatial data. Equivalent to {@link #getCell(String)} with "".
   * This cell is threadsafe, just like a spatial prefix grid is, although cells aren't
   * generally threadsafe.
   * TODO rename to getTopCell or is this fine?
   */
  public Cell getWorldCell() {
    if (worldCell == null) {
      worldCell = getCell("");
    }
    return worldCell;
  }

  /**
   * The cell for the specified token. The empty string should be equal to {@link #getWorldCell()}.
   * Precondition: Never called when token length > maxLevel.
   */
  public abstract Cell getCell(String token);

  public abstract Cell getCell(byte[] bytes, int offset, int len);

  public final Cell getCell(byte[] bytes, int offset, int len, Cell target) {
    if (target == null) {
      return getCell(bytes, offset, len);
    }

    target.reset(bytes, offset, len);
    return target;
  }

  /**
   * Returns the cell containing point {@code p} at the specified {@code level}.
   */
  protected Cell getCell(Point p, int level) {
    return getCells(p, level, false).get(0);
  }

  /**
   * Gets the intersecting cells for the specified shape, without exceeding
   * detail level. If a cell is within the query shape then it's marked as a
   * leaf and none of its children are added.
   * <p/>
   * This implementation checks if shape is a Point and if so returns {@link
   * #getCells(com.spatial4j.core.shape.Point, int, boolean)}.
   *
   * @param shape       the shape; non-null
   * @param detailLevel the maximum detail level to get cells for
   * @param inclParents if true then all parent cells of leaves are returned
   *                    too. The top world cell is never returned.
   * @param simplify    for non-point shapes, this will simply/aggregate sets of
   *                    complete leaves in a cell to its parent, resulting in
   *                    ~20-25% fewer cells.
   * @return a set of cells (no dups), sorted, immutable, non-null
   */
  public List<Cell> getCells(Shape shape, int detailLevel, boolean inclParents,
                             boolean simplify) {
    //TODO consider an on-demand iterator -- it won't build up all cells in memory.
    if (detailLevel > maxLevels) {
      throw new IllegalArgumentException("detailLevel > maxLevels");
    }
    if (shape instanceof Point) {
      return getCells((Point) shape, detailLevel, inclParents);
    }
    List<Cell> cells = new ArrayList<Cell>(inclParents ? 4096 : 2048);
    recursiveGetCells(getWorldCell(), shape, detailLevel, inclParents, simplify, cells);
    return cells;
  }

  /**
   * Returns true if cell was added as a leaf. If it wasn't it recursively
   * descends.
   */
  private boolean recursiveGetCells(Cell cell, Shape shape, int detailLevel,
                                    boolean inclParents, boolean simplify,
                                    List<Cell> result) {
    if (cell.getLevel() == detailLevel) {
      cell.setLeaf();//FYI might already be a leaf
    }
    if (cell.isLeaf()) {
      result.add(cell);
      return true;
    }
    if (inclParents && cell.getLevel() != 0)
      result.add(cell);

    Collection<Cell> subCells = cell.getSubCells(shape);
    int leaves = 0;
    for (Cell subCell : subCells) {
      if (recursiveGetCells(subCell, shape, detailLevel, inclParents, simplify, result))
        leaves++;
    }
    //can we simplify?
    if (simplify && leaves == cell.getSubCellsSize() && cell.getLevel() != 0) {
      //Optimization: substitute the parent as a leaf instead of adding all
      // children as leaves

      //remove the leaves
      do {
        result.remove(result.size() - 1);//remove last
      } while (--leaves > 0);
      //add cell as the leaf
      cell.setLeaf();
      if (!inclParents) // otherwise it was already added up above
        result.add(cell);
      return true;
    }
    return false;
  }

  /**
   * A Point-optimized implementation of
   * {@link #getCells(com.spatial4j.core.shape.Shape, int, boolean, boolean)}. That
   * method in facts calls this for points.
   * <p/>
   * This implementation depends on {@link #getCell(String)} being fast, as its
   * called repeatedly when incPlarents is true.
   */
  public List<Cell> getCells(Point p, int detailLevel, boolean inclParents) {
    Cell cell = getCell(p, detailLevel);
    if (!inclParents) {
      return Collections.singletonList(cell);
    }

    String endToken = cell.getTokenString();
    assert endToken.length() == detailLevel;
    List<Cell> cells = new ArrayList<Cell>(detailLevel);
    for (int i = 1; i < detailLevel; i++) {
      cells.add(getCell(endToken.substring(0, i)));
    }
    cells.add(cell);
    return cells;
  }

  /**
   * Will add the trailing leaf byte for leaves. This isn't particularly efficient.
   */
  public static List<String> cellsToTokenStrings(Collection<Cell> cells) {
    List<String> tokens = new ArrayList<String>((cells.size()));
    for (Cell cell : cells) {
      final String token = cell.getTokenString();
      if (cell.isLeaf()) {
        tokens.add(token + (char) Cell.LEAF_BYTE);
      } else {
        tokens.add(token);
      }
    }
    return tokens;
  }
}
