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
package org.apache.lucene.spatial.prefix.tree;

import java.io.PrintStream;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Rectangle;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.SpatialRelation;

/**
 * A {@link SpatialPrefixTree} which uses a <a href="http://en.wikipedia.org/wiki/Quadtree">quad
 * tree</a> in which an indexed term will be generated for each cell, 'A', 'B', 'C', 'D'.
 *
 * @lucene.experimental
 */
public class QuadPrefixTree extends LegacyPrefixTree {

  /** Factory for creating {@link QuadPrefixTree} instances with useful defaults */
  public static class Factory extends SpatialPrefixTreeFactory {
    @Override
    protected int getLevelForDistance(double degrees) {
      return newSPT().getLevelForDistance(degrees);
    }

    @Override
    protected SpatialPrefixTree newSPT() {
      QuadPrefixTree tree =
          new QuadPrefixTree(ctx, maxLevels != null ? maxLevels : MAX_LEVELS_POSSIBLE);
      @SuppressWarnings("deprecation")
      Version LUCENE_8_3_0 = Version.LUCENE_8_3_0;
      tree.robust = getVersion().onOrAfter(LUCENE_8_3_0);
      return tree;
    }
  }

  public static final int MAX_LEVELS_POSSIBLE = 50; // not really sure how big this should be

  public static final int DEFAULT_MAX_LEVELS = 12;
  protected final double xmin;
  protected final double xmax;
  protected final double ymin;
  protected final double ymax;
  protected final double xmid;
  protected final double ymid;

  protected final double gridW;
  public final double gridH;

  final double[] levelW;
  final double[] levelH;

  protected boolean robust =
      true; // for backward compatibility, use the old method if user specified old version.

  public QuadPrefixTree(SpatialContext ctx, Rectangle bounds, int maxLevels) {
    super(ctx, maxLevels);
    this.xmin = bounds.getMinX();
    this.xmax = bounds.getMaxX();
    this.ymin = bounds.getMinY();
    this.ymax = bounds.getMaxY();

    levelW = new double[maxLevels + 1];
    levelH = new double[maxLevels + 1];

    gridW = xmax - xmin;
    gridH = ymax - ymin;
    this.xmid = xmin + gridW / 2.0;
    this.ymid = ymin + gridH / 2.0;
    levelW[0] = gridW / 2.0;
    levelH[0] = gridH / 2.0;

    for (int i = 1; i < levelW.length; i++) {
      levelW[i] = levelW[i - 1] / 2.0;
      levelH[i] = levelH[i - 1] / 2.0;
    }
  }

  public QuadPrefixTree(SpatialContext ctx) {
    this(ctx, DEFAULT_MAX_LEVELS);
  }

  public QuadPrefixTree(SpatialContext ctx, int maxLevels) {
    this(ctx, ctx.getWorldBounds(), maxLevels);
  }

  @Override
  public Cell getWorldCell() {
    return new QuadCell(BytesRef.EMPTY_BYTES, 0, 0);
  }

  public void printInfo(PrintStream out) {
    NumberFormat nf = NumberFormat.getNumberInstance(Locale.ROOT);
    nf.setMaximumFractionDigits(5);
    nf.setMinimumFractionDigits(5);
    nf.setMinimumIntegerDigits(3);

    for (int i = 0; i < maxLevels; i++) {
      out.println(i + "]\t" + nf.format(levelW[i]) + "\t" + nf.format(levelH[i]));
    }
  }

  @Override
  public int getLevelForDistance(double dist) {
    if (dist == 0) // short circuit
    return maxLevels;
    for (int i = 0; i < maxLevels - 1; i++) {
      // note: level[i] is actually a lookup for level i+1
      if (dist > levelW[i] && dist > levelH[i]) {
        return i + 1;
      }
    }
    return maxLevels;
  }

  @Override
  public Cell getCell(Point p, int level) {
    if (!robust) { // old method
      List<Cell> cells = new ArrayList<>(1);
      buildNotRobustly(
          xmid,
          ymid,
          0,
          cells,
          new BytesRef(maxLevels + 1),
          ctx.getShapeFactory().pointXY(p.getX(), p.getY()),
          level);
      if (!cells.isEmpty()) {
        return cells.get(0); // note cells could be longer if p on edge
      }
    }

    double currentXmid = xmid;
    double currentYmid = ymid;
    double xp = p.getX();
    double yp = p.getY();
    BytesRef str = new BytesRef(maxLevels + 1);
    int levelLimit = level > maxLevels ? maxLevels : level;
    SpatialRelation rel = SpatialRelation.CONTAINS;
    for (int lvl = 0; lvl < levelLimit; lvl++) {
      int c = battenberg(currentXmid, currentYmid, xp, yp);
      double halfWidth = levelW[lvl + 1];
      double halfHeight = levelH[lvl + 1];
      switch (c) {
        case 0:
          currentXmid -= halfWidth;
          currentYmid += halfHeight;
          break;
        case 1:
          currentXmid += halfWidth;
          currentYmid += halfHeight;
          break;
        case 2:
          currentXmid -= halfWidth;
          currentYmid -= halfHeight;
          break;
        case 3:
          currentXmid += halfWidth;
          currentYmid -= halfHeight;
          break;
        default:
      }
      str.bytes[str.length++] = (byte) ('A' + c);
    }
    return new QuadCell(str, rel);
  }

  private void buildNotRobustly(
      double x, double y, int level, List<Cell> matches, BytesRef str, Shape shape, int maxLevel) {
    assert str.length == level;
    double w = levelW[level] / 2;
    double h = levelH[level] / 2;

    // Z-Order
    // http://en.wikipedia.org/wiki/Z-order_%28curve%29
    checkBattenbergNotRobustly('A', x - w, y + h, level, matches, str, shape, maxLevel);
    checkBattenbergNotRobustly('B', x + w, y + h, level, matches, str, shape, maxLevel);
    checkBattenbergNotRobustly('C', x - w, y - h, level, matches, str, shape, maxLevel);
    checkBattenbergNotRobustly('D', x + w, y - h, level, matches, str, shape, maxLevel);

    // possibly consider hilbert curve
    // http://en.wikipedia.org/wiki/Hilbert_curve
    // http://blog.notdot.net/2009/11/Damn-Cool-Algorithms-Spatial-indexing-with-Quadtrees-and-Hilbert-Curves
    // if we actually use the range property in the query, this could be useful
  }

  protected void checkBattenbergNotRobustly(
      char c,
      double cx,
      double cy,
      int level,
      List<Cell> matches,
      BytesRef str,
      Shape shape,
      int maxLevel) {
    assert str.length == level;
    assert str.offset == 0;
    double w = levelW[level] / 2;
    double h = levelH[level] / 2;

    int strlen = str.length;
    Rectangle rectangle = ctx.getShapeFactory().rect(cx - w, cx + w, cy - h, cy + h);
    SpatialRelation v = shape.relate(rectangle);
    if (SpatialRelation.CONTAINS == v) {
      str.bytes[str.length++] = (byte) c; // append
      // str.append(SpatialPrefixGrid.COVER);
      matches.add(new QuadCell(BytesRef.deepCopyOf(str), v.transpose()));
    } else if (SpatialRelation.DISJOINT == v) {
      // nothing
    } else { // SpatialRelation.WITHIN, SpatialRelation.INTERSECTS
      str.bytes[str.length++] = (byte) c; // append

      int nextLevel = level + 1;
      if (nextLevel >= maxLevel) {
        // str.append(SpatialPrefixGrid.INTERSECTS);
        matches.add(new QuadCell(BytesRef.deepCopyOf(str), v.transpose()));
      } else {
        buildNotRobustly(cx, cy, nextLevel, matches, str, shape, maxLevel);
      }
    }
    str.length = strlen;
  }

  /** Returns a Z-Order quadrant [0-3]. */
  protected int battenberg(double xmid, double ymid, double xp, double yp) {
    // http://en.wikipedia.org/wiki/Z-order_%28curve%29
    if (ymid <= yp) {
      if (xmid >= xp) {
        return 0;
      }
      return 1;
    } else {
      if (xmid >= xp) {
        return 2;
      }
      return 3;
    }
    // possibly consider hilbert curve
    // http://en.wikipedia.org/wiki/Hilbert_curve
    // http://blog.notdot.net/2009/11/Damn-Cool-Algorithms-Spatial-indexing-with-Quadtrees-and-Hilbert-Curves
    // if we actually use the range property in the query, this could be useful
  }

  /** individual QuadPrefixTree grid cell */
  protected class QuadCell extends LegacyCell {

    QuadCell(byte[] bytes, int off, int len) {
      super(bytes, off, len);
    }

    QuadCell(BytesRef str, SpatialRelation shapeRel) {
      this(str.bytes, str.offset, str.length);
      this.shapeRel = shapeRel;
    }

    @Override
    protected QuadPrefixTree getGrid() {
      return QuadPrefixTree.this;
    }

    @Override
    protected int getMaxLevels() {
      return maxLevels;
    }

    @Override
    protected Collection<Cell> getSubCells() {
      BytesRef source = getTokenBytesNoLeaf(null);

      List<Cell> cells = new ArrayList<>(4);
      cells.add(new QuadCell(concat(source, (byte) 'A'), null));
      cells.add(new QuadCell(concat(source, (byte) 'B'), null));
      cells.add(new QuadCell(concat(source, (byte) 'C'), null));
      cells.add(new QuadCell(concat(source, (byte) 'D'), null));
      return cells;
    }

    protected BytesRef concat(BytesRef source, byte b) {
      // +2 for new char + potential leaf
      final byte[] buffer = new byte[source.length + 2];
      System.arraycopy(source.bytes, source.offset, buffer, 0, source.length);
      BytesRef target = new BytesRef(buffer);
      target.length = source.length;
      target.bytes[target.length++] = b;
      return target;
    }

    @Override
    public int getSubCellsSize() {
      return 4;
    }

    @Override
    protected QuadCell getSubCell(Point p) {
      return (QuadCell) QuadPrefixTree.this.getCell(p, getLevel() + 1); // not performant!
    }

    @Override
    public Shape getShape() {
      if (shape == null) shape = makeShape();
      return shape;
    }

    protected Rectangle makeShape() {
      BytesRef token = getTokenBytesNoLeaf(null);
      double xmin = QuadPrefixTree.this.xmin;
      double ymin = QuadPrefixTree.this.ymin;

      for (int i = 0; i < token.length; i++) {
        byte c = token.bytes[token.offset + i];
        switch (c) {
          case 'A':
            ymin += levelH[i];
            break;
          case 'B':
            xmin += levelW[i];
            ymin += levelH[i];
            break;
          case 'C':
            break; // nothing really
          case 'D':
            xmin += levelW[i];
            break;
          default:
            throw new RuntimeException("unexpected char: " + c);
        }
      }
      int len = token.length;
      double width, height;
      if (len > 0) {
        width = levelW[len - 1];
        height = levelH[len - 1];
      } else {
        width = gridW;
        height = gridH;
      }
      return ctx.getShapeFactory().rect(xmin, xmin + width, ymin, ymin + height);
    }
  } // QuadCell
}
