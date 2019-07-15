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
package org.apache.lucene.component2D;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.document.ShapeField;
import org.apache.lucene.index.PointValues.Relation;

/**
 * Represents a 2D polygon
 *
 * @lucene.internal
 */
final class PolygonComponent2D implements Component2D {
  /** X values, used for equality and hashcode */
  private final double[] Xs;
  /** Y values, used for equality and hashcode */
  private final double[] Ys;
  /** edge tree representing the polygon */
  private final EdgeTree tree;
  /** bounding box of the polygon */
  private final RectangleComponent2D box;
  /** Holes component2D or null */
  private final Component2D holes;
  /** keeps track if points lies on polygon boundary */
  private final AtomicBoolean containsBoundary = new AtomicBoolean(false);

  private final ShapeField.Decoder decoder;


  protected PolygonComponent2D(double[] Xs, double[] Ys, RectangleComponent2D box, Component2D holes, ShapeField.Decoder decoder) {
    this.Xs = Xs;
    this.Ys = Ys;
    this.holes = holes;
    this.tree = EdgeTree.createTree(Xs, Ys);
    this.box = box;
    this.decoder = decoder;
  }
  
  @Override
  public boolean contains(int x, int y) {
    if (box.contains(x, y)) {
      containsBoundary.set(false);
      if (tree.contains(decoder.decodeX(x), decoder.decodeY(y), containsBoundary)) {
        if (holes != null && holes.contains(x, y)) {
          return false;
        }
        return true;
      }
    }
    return false;
  }

  @Override
  public Relation relate(int minX, int maxX, int minY, int maxY) {
    if (box.disjoint(minX, maxX, minY, maxY)) {
      return Relation.CELL_OUTSIDE_QUERY;
    }
    if (box.within(minX, maxX, minY, maxY)) {
      return Relation.CELL_CROSSES_QUERY;
    }
    // check any holes
    if (holes != null) {
      Relation holeRelation = holes.relate(minX, maxX, minY, maxY);
      if (holeRelation == Relation.CELL_CROSSES_QUERY) {
        return Relation.CELL_CROSSES_QUERY;
      } else if (holeRelation == Relation.CELL_INSIDE_QUERY) {
        return Relation.CELL_OUTSIDE_QUERY;
      }
    }
    // check each corner: if < 4 && > 0 are present, its cheaper than crossesSlowly
    int numCorners = numberOfCorners(minX, maxX, minY, maxY);
    if (numCorners == 4) {
      if (tree.crossesBox(decoder.decodeX(minX), decoder.decodeX(maxX), decoder.decodeY(minY), decoder.decodeY(maxY), false)) {
        return Relation.CELL_CROSSES_QUERY;
      }
      return Relation.CELL_INSIDE_QUERY;
    }  else if (numCorners == 0) {
      if (crossesBox(decoder.decodeX(minX), decoder.decodeX(maxX), decoder.decodeY(minY), decoder.decodeY(maxY))) {
        return Relation.CELL_CROSSES_QUERY;
      }
      return Relation.CELL_OUTSIDE_QUERY;
    }
    return Relation.CELL_CROSSES_QUERY;
  }

  private boolean crossesBox(double minX, double maxX, double minY, double maxY) {
    return containsPoint(tree.x1, tree.y1, minX, maxX, minY, maxY) ||
        tree.crossesBox(minX, maxX, minY, maxY, false);
  }

  /** returns true if this {@link RectangleComponent2D} contains the encoded lat lon point */
  public static  boolean containsPoint(final double x, final double y, final double minX, final double maxX, final double minY, final double maxY)  {
    return x >= minX && x <= maxX && y >= minY && y <= maxY;
  }

  @Override
  public Relation relateTriangle(int minX, int maxX, int minY, int maxY, int aX, int aY, int bX, int bY, int cX, int cY) {
    if (box.disjoint(minX, maxX, minY, maxY)) {
      return Relation.CELL_OUTSIDE_QUERY;
    }
    if (holes != null) {
      Relation holeRelation = holes.relateTriangle(minX, maxX, minY, maxY, aX, aY, bX, bY, cX, cY);
      if (holeRelation == Relation.CELL_CROSSES_QUERY) {
        return Relation.CELL_CROSSES_QUERY;
      } else if (holeRelation == Relation.CELL_INSIDE_QUERY) {
        return Relation.CELL_OUTSIDE_QUERY;
      }
    }
    if (aX == bX && bX == cX && aY == bY && bY == cY)  {
      // indexed "triangle" is a point: shortcut by checking contains
      return contains(aX, aY) ? Relation.CELL_INSIDE_QUERY : Relation.CELL_OUTSIDE_QUERY;
    } else if ((aX == cX && aY == cY) || (bX == cX && bY == cY)) {
      // indexed "triangle" is a line segment: shortcut by calling appropriate method
      return relateIndexedLineSegment(aX, aY, bX, bY);
    }
    // indexed "triangle" is a triangle:
    return relateIndexedTriangle(minX, maxX, minY, maxY, aX, aY, bX, bY, cX, cY);
  }

  /** relates an indexed line segment (a "flat triangle") with the polygon */
  private Relation relateIndexedLineSegment(int a2x, int a2y, int b2x, int b2y) {
    // check endpoints of the line segment
    int numCorners = 0;
    if (contains(a2x, a2y)) {
      ++numCorners;
    }
    if (contains(b2x, b2y)) {
      ++numCorners;
    }

    if (numCorners == 2) {
      if (tree.crossesLine(decoder.decodeX(a2x), decoder.decodeY(a2y), decoder.decodeX(b2x), decoder.decodeY(b2y))) {
        return Relation.CELL_CROSSES_QUERY;
      }
      return Relation.CELL_INSIDE_QUERY;
    } else if (numCorners == 0) {
      if (tree.crossesLine(decoder.decodeX(a2x), decoder.decodeY(a2y), decoder.decodeX(b2x), decoder.decodeY(b2y))) {
        return Relation.CELL_CROSSES_QUERY;
      }
      return Relation.CELL_OUTSIDE_QUERY;
    }
    return Relation.CELL_CROSSES_QUERY;
  }

  /** relates an indexed triangle with the polygon */
  private Relation relateIndexedTriangle(int minX, int maxX, int minY, int maxY, int aX, int aY, int bX, int bY, int cX, int cY) {
    // check each corner: if < 3 && > 0 are present, its cheaper than crossesSlowly
    int numCorners = numberOfTriangleCorners(aX, aY, bX, bY, cX, cY);
    if (numCorners == 3) {
      if (tree.crossesTriangle(decoder.decodeX(minX), decoder.decodeX(maxX), decoder.decodeY(minY), decoder.decodeY(maxY),
          decoder.decodeX(aX), decoder.decodeY(aY), decoder.decodeX(bX), decoder.decodeY(bY), decoder.decodeX(cX), decoder.decodeY(cY))) {
        return Relation.CELL_CROSSES_QUERY;
      }
      return Relation.CELL_INSIDE_QUERY;
    } else if (numCorners == 0) {
      if  (crossesTriangle(decoder.decodeX(minX), decoder.decodeX(maxX), decoder.decodeY(minY), decoder.decodeY(maxY),
          decoder.decodeX(aX), decoder.decodeY(aY), decoder.decodeX(bX), decoder.decodeY(bY), decoder.decodeX(cX), decoder.decodeY(cY))) {
        return Relation.CELL_CROSSES_QUERY;
      }
      return Relation.CELL_OUTSIDE_QUERY;
    }
    return Relation.CELL_CROSSES_QUERY;
  }

  private boolean crossesTriangle(double minX, double maxX, double minY, double maxY, double aX, double aY, double bX, double bY, double cX, double cY) {
    return Component2D.pointInTriangle(minX, maxX, minY, maxY, tree.x1, tree.y1, aX, aY, bX, bY, cX, cY) == true ||
        tree.crossesTriangle(minX, maxX, minY, maxY, aX, aY, bX, bY, cX, cY);
  }

  private int numberOfTriangleCorners(int aX, int aY, int bX, int bY, int cX, int cY) {
    int containsCount = 0;
    if (contains(aX, aY)) {
      containsCount++;
    }
    if (contains(bX, bY)) {
      containsCount++;
    }
    if (containsCount == 1) {
      return containsCount;
    }
    if (contains(cX, cY)) {
      containsCount++;
    }
    return containsCount;
  }

  // returns 0, 4, or something in between
  private int numberOfCorners(int minX, int maxX, int minY, int maxY) {
    int containsCount = 0;
    if (contains(minX, minY)) {
      containsCount++;
    }
    if (contains(maxX, minY)) {
      containsCount++;
    }
    if (containsCount == 1) {
      return containsCount;
    }
    if (contains(maxX, maxY)) {
      containsCount++;
    }
    if (containsCount == 2) {
      return containsCount;
    }
    if (contains(minX, maxY)) {
      containsCount++;
    }
    return containsCount;
  }

  @Override
  public RectangleComponent2D getBoundingBox() {
    return box;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PolygonComponent2D polygonComponent = (PolygonComponent2D) o;
    return Arrays.equals(Xs, polygonComponent.Xs) && Arrays.equals(Ys, polygonComponent.Ys);
  }

  @Override
  public int hashCode() {
    int result;
    long temp;
    temp = Arrays.hashCode(Xs);
    result = (int) (temp ^ (temp >>> 32));
    temp =  Arrays.hashCode(Ys);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return "PolygonComponent2D{" +
        "Xs=" + Arrays.toString(Xs) + ", Ys=" + Arrays.toString(Ys) +
        '}';
  }
}