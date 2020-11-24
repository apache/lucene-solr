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

import org.apache.lucene.index.PointValues.Relation;

/**
 * 2D polygon implementation represented as a balanced interval tree of edges.
 * <p>
 * Loosely based on the algorithm described in <a href="http://www-ma2.upc.es/geoc/Schirra-pointPolygon.pdf">
 * http://www-ma2.upc.es/geoc/Schirra-pointPolygon.pdf</a>.
 */

final class Polygon2D implements Component2D {
  /** minimum Y of this geometry's bounding box area */
  final private double minY;
  /** maximum Y of this geometry's bounding box area */
  final private double maxY;
  /** minimum X of this geometry's bounding box area */
  final private double minX;
  /** maximum X of this geometry's bounding box area */
  final private double maxX;
  /** tree of holes, or null */
  final protected Component2D holes;
  /** Edges of the polygon represented as a 2-d interval tree.*/
  final EdgeTree tree;

  private Polygon2D(final double minX, final double maxX, final double minY, final double maxY, double[] x, double[] y, Component2D holes) {
    this.minY = minY;
    this.maxY = maxY;
    this.minX = minX;
    this.maxX = maxX;
    this.holes = holes;
    this.tree = EdgeTree.createTree(x, y);
  }

  private Polygon2D(XYPolygon polygon, Component2D holes) {
    this(polygon.minX, polygon.maxX, polygon.minY, polygon.maxY, XYEncodingUtils.floatArrayToDoubleArray(polygon.getPolyX()), XYEncodingUtils.floatArrayToDoubleArray(polygon.getPolyY()), holes);
  }

  private Polygon2D(Polygon polygon, Component2D holes) {
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
    if (Component2D.containsPoint(x, y, minX, maxX, minY, maxY) && tree.contains(x, y)) {
      return holes == null || holes.contains(x, y) == false;
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
      if (tree.crossesBox(minX, maxX, minY, maxY, true)) {
        return Relation.CELL_CROSSES_QUERY;
      }
      return Relation.CELL_INSIDE_QUERY;
    }  else if (numCorners == 0) {
      if (Component2D.containsPoint(tree.x1, tree.y1, minX, maxX, minY, maxY)) {
        return Relation.CELL_CROSSES_QUERY;
      }
      if (tree.crossesBox(minX, maxX, minY, maxY, true)) {
        return Relation.CELL_CROSSES_QUERY;
      }
      return Relation.CELL_OUTSIDE_QUERY;
    }
    return Relation.CELL_CROSSES_QUERY;
  }

  @Override
  public boolean intersectsLine(double minX, double maxX, double minY, double maxY,
                                double aX, double aY, double bX, double bY) {
    if (Component2D.disjoint(this.minX, this.maxX, this.minY, this.maxY, minX, maxX, minY, maxY)) {
      return false;
    }
    if (contains(aX, aY) || contains(bX, bY) ||
        tree.crossesLine(minX, maxX, minY, maxY, aX, aY, bX, bY, true)) {
      return holes == null || holes.containsLine(minX, maxX, minY, maxY, aX, aY, bX, bY) == false;
    }
    return false;
  }

  @Override
  public boolean intersectsTriangle(double minX, double maxX, double minY, double maxY,
                                    double aX, double aY, double bX, double bY, double cX, double cY) {
    if (Component2D.disjoint(this.minX, this.maxX, this.minY, this.maxY, minX, maxX, minY, maxY)) {
      return false;
    }
    if (contains(aX, aY) || contains(bX, bY) || contains(cX, cY) ||
        Component2D.pointInTriangle(minX, maxX, minY, maxY, tree.x1, tree.y1, aX, aY, bX, bY, cX, cY)||
        tree.crossesTriangle(minX, maxX, minY, maxY, aX, aY, bX, bY, cX, cY, true)) {
      return holes == null || holes.containsTriangle(minX, maxX, minY, maxY, aX, aY, bX, bY, cX, cY) == false;
    }
    return false;
  }

  @Override
  public boolean containsLine(double minX, double maxX, double minY, double maxY,
                              double aX, double aY, double bX, double bY) {
    if (Component2D.disjoint(this.minX, this.maxX, this.minY, this.maxY, minX, maxX, minY, maxY)) {
      return false;
    }
    if (contains(aX, aY) && contains(bX, bY) &&
        tree.crossesLine(minX, maxX, minY, maxY, aX, aY, bX, bY, false) == false) {
      return holes == null || holes.intersectsLine(minX, maxX, minY, maxY, aX, aY, bX, bY) == false;
    }
    return false;
  }

  @Override
  public boolean containsTriangle(double minX, double maxX, double minY, double maxY,
                                  double aX, double aY, double bX, double bY, double cX, double cY) {
    if (Component2D.disjoint(this.minX, this.maxX, this.minY, this.maxY, minX, maxX, minY, maxY)) {
      return false;
    }
    if (contains(aX, aY) && contains(bX, bY) && contains(cX, cY) &&
        tree.crossesTriangle(minX, maxX, minY, maxY, aX, aY, bX, bY, cX, cY, false) == false) {
      return holes == null || holes.intersectsTriangle(minX, maxX, minY, maxY, aX, aY, bX, bY, cX, cY) == false;
    }
    return false;
  }

  @Override
  public WithinRelation withinPoint(double x, double y) {
    return contains(x, y) ? WithinRelation.NOTWITHIN : WithinRelation.DISJOINT;
  }

  @Override
  public WithinRelation withinLine(double minX, double maxX, double minY, double maxY,
                                   double aX, double aY, boolean ab, double bX, double bY) {
    if (ab == true && Component2D.disjoint(this.minX, this.maxX, this.minY, this.maxY, minX, maxX, minY, maxY) == false &&
        tree.crossesLine(minX, maxX, minY, maxY, aX, aY, bX, bY, true)) {
      return WithinRelation.NOTWITHIN;
    }
    return WithinRelation.DISJOINT;
  }

  @Override
  public WithinRelation withinTriangle(double minX, double maxX, double minY, double maxY,
                                          double aX, double aY, boolean ab, double bX, double bY, boolean bc, double cX, double cY, boolean ca) {
    if (Component2D.disjoint(this.minX, this.maxX, this.minY, this.maxY, minX, maxX, minY, maxY)) {
      return WithinRelation.DISJOINT;
    }

    // if any of the points is inside the polygon, the polygon cannot be within this indexed
    // shape because points belong to the original indexed shape.
    if (contains(aX, aY) || contains(bX, bY) || contains(cX, cY)) {
      return WithinRelation.NOTWITHIN;
    }

    WithinRelation relation = WithinRelation.DISJOINT;
    // if any of the edges intersects an the edge belongs to the shape then it cannot be within.
    // if it only intersects edges that do not belong to the shape, then it is a candidate
    // we skip edges at the dateline to support shapes crossing it
    if (tree.crossesLine(minX, maxX, minY, maxY, aX, aY, bX, bY, true)) {
      if (ab == true) {
        return WithinRelation.NOTWITHIN;
      } else {
        relation = WithinRelation.CANDIDATE;
      }
    }

    if (tree.crossesLine(minX, maxX, minY, maxY, bX, bY, cX, cY, true)) {
      if (bc == true) {
        return WithinRelation.NOTWITHIN;
      } else {
        relation = WithinRelation.CANDIDATE;
      }
    }
    if (tree.crossesLine(minX, maxX, minY, maxY, cX, cY, aX, aY, true)) {
      if (ca == true) {
        return WithinRelation.NOTWITHIN;
      } else {
        relation = WithinRelation.CANDIDATE;
      }
    }

    // if any of the edges crosses and edge that does not belong to the shape
    // then it is a candidate for within
    if (relation == WithinRelation.CANDIDATE) {
      return WithinRelation.CANDIDATE;
    }

    // Check if shape is within the triangle
    if (Component2D.pointInTriangle(minX, maxX, minY, maxY, tree.x1, tree.y1, aX, aY, bX, bY, cX, cY) == true) {
      return WithinRelation.CANDIDATE;
    }
    return relation;
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

  /** Builds a Polygon2D from LatLon polygon */
  static Component2D create(Polygon polygon) {
    Polygon gonHoles[] = polygon.getHoles();
    Component2D holes = null;
    if (gonHoles.length > 0) {
      holes = LatLonGeometry.create(gonHoles);
    }
    return new Polygon2D(polygon, holes);
  }

  /** Builds a Polygon2D from XY polygon */
  static Component2D create(XYPolygon polygon) {
    XYPolygon gonHoles[] = polygon.getHoles();
    Component2D holes = null;
    if (gonHoles.length > 0) {
      holes = XYGeometry.create(gonHoles);
    }
    return new Polygon2D(polygon, holes);
  }

}
