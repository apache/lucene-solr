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

import java.util.Objects;

import org.apache.lucene.index.PointValues;

import static org.apache.lucene.geo.GeoUtils.orient;

/**
 * 2D rectangle implementation containing cartesian spatial logic.
 *
 * @lucene.internal
 */
public class XYRectangle2D implements Component2D {

  private final double minX;
  private final double maxX;
  private final double minY;
  private final double maxY;

  protected XYRectangle2D(double minX, double maxX, double minY, double maxY) {
    this.minX =  minX;
    this.maxX =  maxX;
    this.minY =  minY;
    this.maxY =  maxY;
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
    return Component2D.containsPoint(x, y, this.minX, this.maxX, this.minY, this.maxY);
  }

  @Override
  public PointValues.Relation relate(double minX, double maxX, double minY, double maxY) {
    if (Component2D.disjoint(this.minX, this.maxX, this.minY, this.maxY, minX, maxX, minY, maxY)) {
      return PointValues.Relation.CELL_OUTSIDE_QUERY;
    }
    if (Component2D.within(minX, maxX, minY, maxY, this.minX, this.maxX, this.minY, this.maxY)) {
      return PointValues.Relation.CELL_INSIDE_QUERY;
    }
    return PointValues.Relation.CELL_CROSSES_QUERY;
  }

  @Override
  public PointValues.Relation relateTriangle(double minX, double maxX, double minY, double maxY,
                                             double ax, double ay, double bx, double by, double cx, double cy) {


    if (Component2D.disjoint(this.minX, this.maxX, this.minY, this.maxY, minX, maxX, minY, maxY)) {
      return PointValues.Relation.CELL_OUTSIDE_QUERY;
    }
    int edgesContain = numberOfCorners(ax, ay, bx, by, cx, cy);
    if (edgesContain == 3) {
      return PointValues.Relation.CELL_INSIDE_QUERY;
    } else if (edgesContain != 0) {
      return PointValues.Relation.CELL_CROSSES_QUERY;
    } else if (Component2D.pointInTriangle(minX, maxX, minY, maxY, this.minX, this.minY,ax, ay, bx, by, cx, cy)
        || edgesIntersect(ax, ay, bx, by)
        || edgesIntersect(bx, by, cx, cy)
        || edgesIntersect(cx, cy, ax, ay)) {
      return PointValues.Relation.CELL_CROSSES_QUERY;
    }
    return PointValues.Relation.CELL_OUTSIDE_QUERY;
  }

  @Override
  public WithinRelation withinTriangle(double minX, double maxX, double minY, double maxY,
                                       double ax, double ay, boolean ab, double bx, double by, boolean bc, double cx, double cy, boolean ca) {
    // Short cut, lines and points cannot contain a bbox
    if ((ax == bx && ay == by) || (ax == cx && ay == cy) || (bx == cx && by == cy)) {
      return WithinRelation.DISJOINT;
    }

    // Bounding boxes disjoint?
    if (Component2D.disjoint(this.minX, this.maxX, this.minY, this.maxY, minX, maxX, minY, maxY)) {
      return WithinRelation.DISJOINT;
    }

    // Points belong to the shape so if points are inside the rectangle then it cannot be within.
    if (contains(ax, ay) || contains(bx, by) || contains(cx, cy)) {
      return WithinRelation.NOTWITHIN;
    }
    // If any of the edges intersects an edge belonging to the shape then it cannot be within.
    WithinRelation relation = WithinRelation.DISJOINT;
    if (edgesIntersect(ax, ay, bx, by) == true) {
      if (ab == true) {
        return WithinRelation.NOTWITHIN;
      } else {
        relation = WithinRelation.CANDIDATE;
      }
    }
    if (edgesIntersect(bx, by, cx, cy) == true) {
      if (bc == true) {
        return WithinRelation.NOTWITHIN;
      } else {
        relation = WithinRelation.CANDIDATE;
      }
    }

    if (edgesIntersect(cx, cy, ax, ay) == true) {
      if (ca == true) {
        return WithinRelation.NOTWITHIN;
      } else {
        relation = WithinRelation.CANDIDATE;
      }
    }
    // If any of the rectangle edges crosses a triangle edge that does not belong to the shape
    // then it is a candidate for within
    if (relation == WithinRelation.CANDIDATE) {
      return WithinRelation.CANDIDATE;
    }
    // Check if shape is within the triangle
    if (Component2D.pointInTriangle(minX, maxX, minY, maxY, this.minX, this.minY, ax, ay, bx, by, cx, cy)) {
      return WithinRelation.CANDIDATE;
    }
    return relation;
  }

  private  boolean edgesIntersect(double ax, double ay, double bx, double by) {
    // shortcut: if edge is a point (occurs w/ Line shapes); simply check bbox w/ point
    if (ax == bx && ay == by) {
      return false;
    }

    // shortcut: check bboxes of edges are disjoint
    if ( Math.max(ax, bx) < minX || Math.min(ax, bx) > maxX || Math.min(ay, by) > maxY || Math.max(ay, by) < minY) {
      return false;
    }

    // top
    if (orient(ax, ay, bx, by, minX, maxY) * orient(ax, ay, bx, by, maxX, maxY) <= 0 &&
        orient(minX, maxY, maxX, maxY, ax, ay) * orient(minX, maxY, maxX, maxY, bx, by) <= 0) {
      return true;
    }

    // right
    if (orient(ax, ay, bx, by, maxX, maxY) * orient(ax, ay, bx, by, maxX, minY) <= 0 &&
        orient(maxX, maxY, maxX, minY, ax, ay) * orient(maxX, maxY, maxX, minY, bx, by) <= 0) {
      return true;
    }

    // bottom
    if (orient(ax, ay, bx, by, maxX, minY) * orient(ax, ay, bx, by, minX, minY) <= 0 &&
        orient(maxX, minY, minX, minY, ax, ay) * orient(maxX, minY, minX, minY, bx, by) <= 0) {
      return true;
    }

    // left
    if (orient(ax, ay, bx, by, minX, minY) * orient(ax, ay, bx, by, minX, maxY) <= 0 &&
        orient(minX, minY, minX, maxY, ax, ay) * orient(minX, minY, minX, maxY, bx, by) <= 0) {
      return true;
    }
    return false;
  }

  private int numberOfCorners(double ax, double ay, double bx, double by, double cx, double cy) {
    int containsCount = 0;
    if (contains(ax, ay)) {
      containsCount++;
    }
    if (contains(bx, by)) {
      containsCount++;
    }
    if (contains(cx, cy)) {
      containsCount++;
    }
    return containsCount;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof XYRectangle2D)) return false;
    XYRectangle2D that = (XYRectangle2D) o;
    return minX == that.minX &&
        maxX == that.maxX &&
        minY == that.minY &&
        maxY == that.maxY;
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(minX, maxX, minY, maxY);
    return result;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("XYRectangle(x=");
    sb.append(minX);
    sb.append(" TO ");
    sb.append(maxX);
    sb.append(" y=");
    sb.append(minY);
    sb.append(" TO ");
    sb.append(maxY);
    sb.append(")");
    return sb.toString();
  }

  /** create a component2D from provided array of rectangles */
  public static Component2D create(XYRectangle... rectangles) {
    XYRectangle2D[] components = new XYRectangle2D[rectangles.length];
    for (int i = 0; i < components.length; ++i) {
      components[i] = new XYRectangle2D(XYEncodingUtils.decode(XYEncodingUtils.encode(rectangles[i].minX)),
          XYEncodingUtils.decode(XYEncodingUtils.encode(rectangles[i].maxX)),
          XYEncodingUtils.decode(XYEncodingUtils.encode(rectangles[i].minY)),
          XYEncodingUtils.decode(XYEncodingUtils.encode(rectangles[i].maxY)));
    }
    return ComponentTree.create(components);
  }
}