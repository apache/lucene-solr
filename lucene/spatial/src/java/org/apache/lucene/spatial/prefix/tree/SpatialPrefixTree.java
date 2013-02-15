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

  //TODO double getDistanceForLevel(int level)

  private transient Node worldNode;//cached

  /**
   * Returns the level 0 cell which encompasses all spatial data. Equivalent to {@link #getNode(String)} with "".
   * This cell is threadsafe, just like a spatial prefix grid is, although cells aren't
   * generally threadsafe.
   * TODO rename to getTopCell or is this fine?
   */
  public Node getWorldNode() {
    if (worldNode == null) {
      worldNode = getNode("");
    }
    return worldNode;
  }

  /**
   * The cell for the specified token. The empty string should be equal to {@link #getWorldNode()}.
   * Precondition: Never called when token length > maxLevel.
   */
  public abstract Node getNode(String token);

  public abstract Node getNode(byte[] bytes, int offset, int len);

  public final Node getNode(byte[] bytes, int offset, int len, Node target) {
    if (target == null) {
      return getNode(bytes, offset, len);
    }

    target.reset(bytes, offset, len);
    return target;
  }

  protected Node getNode(Point p, int level) {
    return getNodes(p, level, false).get(0);
  }

  /**
   * Gets the intersecting cells for the specified shape, without exceeding
   * detail level. If a cell is within the query shape then it's marked as a
   * leaf and none of its children are added.
   * <p/>
   * This implementation checks if shape is a Point and if so returns {@link
   * #getNodes(com.spatial4j.core.shape.Point, int, boolean)}.
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
  public List<Node> getNodes(Shape shape, int detailLevel, boolean inclParents,
                             boolean simplify) {
    //TODO consider an on-demand iterator -- it won't build up all cells in memory.
    if (detailLevel > maxLevels) {
      throw new IllegalArgumentException("detailLevel > maxLevels");
    }
    if (shape instanceof Point) {
      return getNodes((Point) shape, detailLevel, inclParents);
    }
    List<Node> cells = new ArrayList<Node>(inclParents ? 4096 : 2048);
    recursiveGetNodes(getWorldNode(), shape, detailLevel, inclParents, simplify, cells);
    return cells;
  }

  /**
   * Returns true if node was added as a leaf. If it wasn't it recursively
   * descends.
   */
  private boolean recursiveGetNodes(Node node, Shape shape, int detailLevel,
                                    boolean inclParents, boolean simplify,
                                    List<Node> result) {
    if (node.getLevel() == detailLevel) {
      node.setLeaf();//FYI might already be a leaf
    }
    if (node.isLeaf()) {
      result.add(node);
      return true;
    }
    if (inclParents && node.getLevel() != 0)
      result.add(node);

    Collection<Node> subCells = node.getSubCells(shape);
    int leaves = 0;
    for (Node subCell : subCells) {
      if (recursiveGetNodes(subCell, shape, detailLevel, inclParents, simplify, result))
        leaves++;
    }
    //can we simplify?
    if (simplify && leaves == node.getSubCellsSize() && node.getLevel() != 0) {
      //Optimization: substitute the parent as a leaf instead of adding all
      // children as leaves

      //remove the leaves
      do {
        result.remove(result.size() - 1);//remove last
      } while (--leaves > 0);
      //add node as the leaf
      node.setLeaf();
      if (!inclParents) // otherwise it was already added up above
        result.add(node);
      return true;
    }
    return false;
  }

  /**
   * A Point-optimized implementation of
   * {@link #getNodes(com.spatial4j.core.shape.Shape, int, boolean, boolean)}. That
   * method in facts calls this for points.
   * <p/>
   * This implementation depends on {@link #getNode(String)} being fast, as its
   * called repeatedly when incPlarents is true.
   */
  public List<Node> getNodes(Point p, int detailLevel, boolean inclParents) {
    Node cell = getNode(p, detailLevel);
    if (!inclParents) {
      return Collections.singletonList(cell);
    }

    String endToken = cell.getTokenString();
    assert endToken.length() == detailLevel;
    List<Node> cells = new ArrayList<Node>(detailLevel);
    for (int i = 1; i < detailLevel; i++) {
      cells.add(getNode(endToken.substring(0, i)));
    }
    cells.add(cell);
    return cells;
  }

  /**
   * Will add the trailing leaf byte for leaves. This isn't particularly efficient.
   */
  public static List<String> nodesToTokenStrings(Collection<Node> nodes) {
    List<String> tokens = new ArrayList<String>((nodes.size()));
    for (Node node : nodes) {
      final String token = node.getTokenString();
      if (node.isLeaf()) {
        tokens.add(token + (char) Node.LEAF_BYTE);
      } else {
        tokens.add(token);
      }
    }
    return tokens;
  }
}
