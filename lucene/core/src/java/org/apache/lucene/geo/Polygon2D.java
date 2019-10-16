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
package org.apache.lucene.geo;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.index.PointValues.Relation;

/**
 * 2D polygon implementation represented as a balanced interval tree of edges.
 * <p>
 * Loosely based on the algorithm described in <a href="http://www-ma2.upc.es/geoc/Schirra-pointPolygon.pdf">
 * http://www-ma2.upc.es/geoc/Schirra-pointPolygon.pdf</a>.
 * @lucene.internal
 */

public class Polygon2D implements Component2D {
  /** minimum latitude of this geometry's bounding box area */
  final private double minY;
  /** maximum latitude of this geometry's bounding box area */
  final private double maxY;
  /** minimum longitude of this geometry's bounding box area */
  final private double minX;
  /** maximum longitude of this geometry's bounding box area */
  final private double maxX;
  /** tree of holes, or null */
  final protected Component2D holes;
  /** Edges of the polygon represented as a 2-d interval tree.*/
  final EdgeTree tree;
  /** helper boolean for points on boundary */
  final private AtomicBoolean containsBoundary = new AtomicBoolean(false);

  protected Polygon2D(final double minX, final double maxX, final double minY, final double maxY, double[] x, double[] y, Component2D holes) {
    this.minY = minY;
    this.maxY = maxY;
    this.minX = minX;
    this.maxX = maxX;
    this.holes = holes;
    this.tree = EdgeTree.createTree(x, y);
  }

  protected Polygon2D(Polygon polygon, Component2D holes) {
    this(polygon.minLon, polygon.maxLon, polygon.minLat, polygon.maxLat, polygon.getPolyLons(), polygon.getPolyLats(), holes);
  }

  @Override
  public double getMinX() {
    return minX;
  }

  @Override
  public double getMaxX() {
    return maxX;
  }

  @Override
  public double getMinY() {
    return minY;
  }

  @Override
  public double getMaxY() {
    return maxY;
  }

  /**
   * Returns true if the point is contained within this polygon.
   * <p>
   * See <a href="https://www.ecse.rpi.edu/~wrf/Research/Short_Notes/pnpoly.html">
   * https://www.ecse.rpi.edu/~wrf/Research/Short_Notes/pnpoly.html</a> for more information.
   */
  @Override
  public boolean contains(double x, double y) {
    if (Component2D.containsPoint(x, y, minX, maxX, minY, maxY)) {
      return internalContains(x, y);
    }
    return false;
  }

  private boolean internalContains(double x, double y) {
    containsBoundary.set(false);
    if (tree.contains(x, y, containsBoundary)) {
      if (holes != null && holes.contains(x, y)) {
        return false;
      }
      return true;
    }
    return false;
  }

  @Override
  public Relation relate(double minX, double maxX, double minY, double maxY) {
    if (Component2D.disjoint(this.minX, this.maxX, this.minY, this.maxY, minX, maxX, minY, maxY)) {
      return Relation.CELL_OUTSIDE_QUERY;
    }
    if (Component2D.within(this.minX, this.maxX, this.minY, this.maxY, minX, maxX, minY, maxY)) {
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
      if (tree.crossesBox(minX, maxX, minY, maxY, false)) {
        return Relation.CELL_CROSSES_QUERY;
      }
      return Relation.CELL_INSIDE_QUERY;
    }  else if (numCorners == 0) {
      if (Component2D.containsPoint(tree.x1, tree.y1, minX, maxX, minY, maxY)) {
        return Relation.CELL_CROSSES_QUERY;
      }
      if (tree.crossesBox(minX, maxX, minY, maxY, false)) {
        return Relation.CELL_CROSSES_QUERY;
      }
      return Relation.CELL_OUTSIDE_QUERY;
    }
    return Relation.CELL_CROSSES_QUERY;
  }

  @Override
  public Relation relateTriangle(double minX, double maxX, double minY, double maxY,
                                 double ax, double ay, double bx, double by, double cx, double cy) {
    if (Component2D.disjoint(this.minX, this.maxX, this.minY, this.maxY, minX, maxX, minY, maxY)) {
      return Relation.CELL_OUTSIDE_QUERY;
    }
    // check any holes
    if (holes != null) {
      Relation holeRelation = holes.relateTriangle(minX, maxX, minY, maxY, ax, ay, bx, by, cx, cy);
      if (holeRelation == Relation.CELL_CROSSES_QUERY) {
        return Relation.CELL_CROSSES_QUERY;
      } else if (holeRelation == Relation.CELL_INSIDE_QUERY) {
        return Relation.CELL_OUTSIDE_QUERY;
      }
    }
    if (ax == bx && bx == cx && ay == by && by == cy) {
      // indexed "triangle" is a point: shortcut by checking contains
      return internalContains(ax, ay) ? Relation.CELL_INSIDE_QUERY : Relation.CELL_OUTSIDE_QUERY;
    } else if (ax == cx && ay == cy) {
      // indexed "triangle" is a line segment: shortcut by calling appropriate method
      return relateIndexedLineSegment(minX, maxX, minY, maxY, ax, ay, bx, by);
    } else if (ax == bx && ay == by) {
      // indexed "triangle" is a line segment: shortcut by calling appropriate method
      return relateIndexedLineSegment(minX, maxX, minY, maxY, bx, by, cx, cy);
    } else if (bx == cx && by == cy) {
      // indexed "triangle" is a line segment: shortcut by calling appropriate method
      return relateIndexedLineSegment(minX, maxX, minY, maxY, cx, cy, ax, ay);
    }
    // indexed "triangle" is a triangle:
    return relateIndexedTriangle(minX, maxX, minY, maxY, ax, ay, bx, by, cx, cy);
  }

  /** relates an indexed line segment (a "flat triangle") with the polygon */
  private Relation relateIndexedLineSegment(double minX, double maxX, double minY, double maxY,
                                            double a2x, double a2y, double b2x, double b2y) {
    // check endpoints of the line segment
    int numCorners = 0;
    if (contains(a2x, a2y)) {
      ++numCorners;
    }
    if (contains(b2x, b2y)) {
      ++numCorners;
    }

    if (numCorners == 2) {
      if (tree.crossesLine(minX, maxX, minY, maxY, a2x, a2y, b2x, b2y)) {
        return Relation.CELL_CROSSES_QUERY;
      }
      return Relation.CELL_INSIDE_QUERY;
    } else if (numCorners == 0) {
      if (tree.crossesLine(minX, maxX, minY, maxY, a2x, a2y, b2x, b2y)) {
        return Relation.CELL_CROSSES_QUERY;
      }
      return Relation.CELL_OUTSIDE_QUERY;
    }
    return Relation.CELL_CROSSES_QUERY;
  }

  /** relates an indexed triangle with the polygon */
  private Relation relateIndexedTriangle(double minX, double maxX, double minY, double maxY,
                                         double ax, double ay, double bx, double by, double cx, double cy) {
    // check each corner: if < 3 && > 0 are present, its cheaper than crossesSlowly
    int numCorners = numberOfTriangleCorners(ax, ay, bx, by, cx, cy);
    if (numCorners == 3) {
      if (tree.crossesTriangle(minX, maxX, minY, maxY, ax, ay, bx, by, cx, cy)) {
        return Relation.CELL_CROSSES_QUERY;
      }
      return Relation.CELL_INSIDE_QUERY;
    } else if (numCorners == 0) {
      if (Component2D.pointInTriangle(minX, maxX, minY, maxY, tree.x1, tree.y1, ax, ay, bx, by, cx, cy) == true) {
        return Relation.CELL_CROSSES_QUERY;
      }
      if (tree.crossesTriangle(minX, maxX, minY, maxY, ax, ay, bx, by, cx, cy)) {
        return Relation.CELL_CROSSES_QUERY;
      }
      return Relation.CELL_OUTSIDE_QUERY;
    }
    return Relation.CELL_CROSSES_QUERY;
  }

  private int numberOfTriangleCorners(double ax, double ay, double bx, double by, double cx, double cy) {
    int containsCount = 0;
    if (contains(ax, ay)) {
      containsCount++;
    }
    if (contains(bx, by)) {
      containsCount++;
    }
    if (containsCount == 1) {
      return containsCount;
    }
    if (contains(cx, cy)) {
      containsCount++;
    }
    return containsCount;
  }

  // returns 0, 4, or something in between
  private int numberOfCorners(double minX, double maxX, double minY, double maxY) {
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

  /** Builds a Polygon2D from multipolygon */
  public static Component2D create(Polygon... polygons) {
    Component2D components[] = new Component2D[polygons.length];
    for (int i = 0; i < components.length; i++) {
      Polygon gon = polygons[i];
      Polygon gonHoles[] = gon.getHoles();
      Component2D holes = null;
      if (gonHoles.length > 0) {
        holes = create(gonHoles);
      }
      components[i] = new Polygon2D(gon, holes);
    }
    return ComponentTree.create(components);
  }
}
