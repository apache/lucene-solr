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
 * 2D geo line implementation represented as a balanced interval tree of edges.
 * <p>
 * Line {@code Line2D} Construction takes {@code O(n log n)} time for sorting and tree construction.
 * {@link #relate relate()} are {@code O(n)}, but for most practical lines are much faster than brute force.
 */
final class Line2D implements Component2D {

  /** minimum Y of this geometry's bounding box area */
  final private double minY;
  /** maximum Y of this geometry's bounding box area */
  final private double maxY;
  /** minimum X of this geometry's bounding box area */
  final private double minX;
  /** maximum X of this geometry's bounding box area */
  final private double maxX;
  /** lines represented as a 2-d interval tree.*/
  final private EdgeTree tree;

  private Line2D(Line line) {
    this.minY = line.minLat;
    this.maxY = line.maxLat;
    this.minX = line.minLon;
    this.maxX = line.maxLon;
    this.tree = EdgeTree.createTree(line.getLons(), line.getLats());
  }

  private Line2D(XYLine line) {
    this.minY = line.minY;
    this.maxY = line.maxY;
    this.minX = line.minX;
    this.maxX = line.maxX;
    this.tree = EdgeTree.createTree(XYEncodingUtils.floatArrayToDoubleArray(line.getX()), XYEncodingUtils.floatArrayToDoubleArray(line.getY()));
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

  @Override
  public boolean contains(double x, double y) {
    if (Component2D.containsPoint(x, y, this.minX, this.maxX, this.minY, this.maxY)) {
      return tree.isPointOnLine(x, y);
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
    if (tree.crossesBox(minX, maxX, minY, maxY, true)) {
      return Relation.CELL_CROSSES_QUERY;
    }
    return Relation.CELL_OUTSIDE_QUERY;
  }

  @Override
  public Relation relateTriangle(double minX, double maxX, double minY, double maxY,
                                 double ax, double ay, double bx, double by, double cx, double cy) {
    if (Component2D.disjoint(this.minX, this.maxX, this.minY, this.maxY, minX, maxX, minY, maxY)) {
      return Relation.CELL_OUTSIDE_QUERY;
    }
    if (ax == bx && bx == cx && ay == by && by == cy) {
      // indexed "triangle" is a point: check if point lies on any line segment
      if (tree.isPointOnLine(ax, ay)) {
        return Relation.CELL_INSIDE_QUERY;
      }
    } else if (ax == cx && ay == cy) {
      // indexed "triangle" is a line:
      if (tree.crossesLine(minX, maxX, minY, maxY, ax, ay, bx, by, true)) {
        return Relation.CELL_CROSSES_QUERY;
      }
      return Relation.CELL_OUTSIDE_QUERY;
    } else if (ax == bx && ay == by) {
      // indexed "triangle" is a line:
      if (tree.crossesLine(minX, maxX, minY, maxY, bx, by, cx, cy, true)) {
        return Relation.CELL_CROSSES_QUERY;
      }
      return Relation.CELL_OUTSIDE_QUERY;
    } else if (bx == cx && by == cy) {
      // indexed "triangle" is a line:
      if (tree.crossesLine(minX, maxX, minY, maxY, cx, cy, ax, ay, true)) {
        return Relation.CELL_CROSSES_QUERY;
      }
      return Relation.CELL_OUTSIDE_QUERY;
    } else if (Component2D.pointInTriangle(minX, maxX, minY, maxY, tree.x1, tree.y1, ax, ay, bx, by, cx, cy) == true ||
        tree.crossesTriangle(minX, maxX, minY, maxY, ax, ay, bx, by, cx, cy, true)) {
      // indexed "triangle" is a triangle:
      return Relation.CELL_CROSSES_QUERY;
    }
    return Relation.CELL_OUTSIDE_QUERY;
  }

  @Override
  public WithinRelation withinTriangle(double minX, double maxX, double minY, double maxY,
                                       double ax, double ay, boolean ab, double bx, double by, boolean bc, double cx, double cy, boolean ca) {
    // short cut, lines and points cannot contain this type of shape
    if ((ax == bx && ay == by) || (ax == cx && ay == cy) || (bx == cx && by == cy)) {
      return WithinRelation.DISJOINT;
    }

    if (Component2D.disjoint(this.minX, this.maxX, this.minY, this.maxY, minX, maxX, minY, maxY)) {
      return WithinRelation.DISJOINT;
    }

    WithinRelation relation = WithinRelation.DISJOINT;
    // if any of the edges intersects an the edge belongs to the shape then it cannot be within.
    // if it only intersects edges that do not belong to the shape, then it is a candidate
    // we skip edges at the dateline to support shapes crossing it
    if (tree.crossesLine(minX, maxX, minY, maxY, ax, ay, bx, by, true)) {
      if (ab == true) {
        return WithinRelation.NOTWITHIN;
      } else {
        relation = WithinRelation.CANDIDATE;
      }
    }

    if (tree.crossesLine(minX, maxX, minY, maxY, bx, by, cx, cy, true)) {
      if (bc == true) {
        return WithinRelation.NOTWITHIN;
      } else {
        relation = WithinRelation.CANDIDATE;
      }
    }
    if (tree.crossesLine(minX, maxX, minY, maxY, cx, cy, ax, ay, true)) {
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
    if (Component2D.pointInTriangle(minX, maxX, minY, maxY, tree.x1, tree.y1, ax, ay, bx, by, cx, cy) == true) {
      return WithinRelation.CANDIDATE;
    }
    return relation;
  }

  /** create a Line2D from the provided LatLon Linestring */
  static Component2D create(Line line) {
    return new Line2D(line);
  }

  /** create a Line2D from the provided XY Linestring */
  static Component2D create(XYLine line) {
    return new Line2D(line);
  }
}