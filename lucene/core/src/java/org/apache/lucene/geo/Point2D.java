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

import org.apache.lucene.index.PointValues;

import static org.apache.lucene.geo.GeoUtils.orient;

/**
 * 2D point implementation containing geo spatial logic.
 *
 * @lucene.internal
 */
public class Point2D implements Component2D {

  final private double x;
  final private double y;

  Point2D(double x, double y) {
    this.x = x;
    this.y = y;
  }

  @Override
  public double getMinX() {
    return x;
  }

  @Override
  public double getMaxX() {
    return x;
  }

  @Override
  public double getMinY() {
    return y;
  }

  @Override
  public double getMaxY() {
    return y;
  }

  @Override
  public boolean contains(double x, double y) {
    return x == this.x && y == this.y;
  }

  @Override
  public PointValues.Relation relate(double minX, double maxX, double minY, double maxY) {
    if (Component2D.containsPoint(x, y, minX, maxX, minY, maxY)) {
      return PointValues.Relation.CELL_CROSSES_QUERY;
    }
    return PointValues.Relation.CELL_OUTSIDE_QUERY;
  }

  @Override
  public PointValues.Relation relateTriangle(double minX, double maxX, double minY, double maxY,
                                             double ax, double ay, double bx, double by, double cx, double cy) {
    if (Component2D.containsPoint(x, y, minX, maxX, minY, maxY) == false) {
      return PointValues.Relation.CELL_OUTSIDE_QUERY;
    }
    if (ax == bx && bx == cx && ay == by && by == cy) {
      return  PointValues.Relation.CELL_INSIDE_QUERY;
    } else if (ax == cx && ay == cy) {
      // indexed "triangle" is a line:
      if (orient(ax, ay, bx, by, x, y) == 0) {
        return PointValues.Relation.CELL_INSIDE_QUERY;
      }
      return PointValues.Relation.CELL_OUTSIDE_QUERY;
    } else if (ax == bx && ay == by) {
      // indexed "triangle" is a line:
      if (orient(bx, by, cx, cy, x, y) == 0) {
        return PointValues.Relation.CELL_INSIDE_QUERY;
      }
      return PointValues.Relation.CELL_OUTSIDE_QUERY;
    } else if (bx == cx && by == cy) {
      // indexed "triangle" is a line:
      if (orient(cx, cy, ax, ay, x, y) == 0) {
        return PointValues.Relation.CELL_INSIDE_QUERY;
      }
      return PointValues.Relation.CELL_OUTSIDE_QUERY;
    } else if (Component2D.pointInTriangle(minX, maxX, minY, maxY, x, y, ax, ay, bx, by, cx, cy) == true) {
      // indexed "triangle" is a triangle:
      return PointValues.Relation.CELL_INSIDE_QUERY;
    }
    return PointValues.Relation.CELL_OUTSIDE_QUERY;
  }

  @Override
  public WithinRelation withinTriangle(double minX, double maxX, double minY, double maxY,
                                       double aX, double aY, boolean ab, double bX, double bY, boolean bc, double cX, double cY, boolean ca) {
    if (aX == bX && aY == bY && aX == cX && aY == cY) {
      if (contains(aX, aY)) {
        return WithinRelation.CANDIDATE;
      }
    }
    return WithinRelation.DISJOINT;
  }

  /** create a Point2D component tree from provided array of LatLon points.  */
  public static Component2D create(double[]... points) {
    Point2D components[] = new Point2D[points.length];
    for (int i = 0; i < components.length; ++i) {
      components[i] = new Point2D(GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(points[i][1]))
          , GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(points[i][0])));
    }
    return ComponentTree.create(components);
  }

  /** create a Point2D component tree from provided array of XY points.  */
  public static Component2D create(float[]... xyPoints) {
    Point2D components[] = new Point2D[xyPoints.length];
    for (int i = 0; i < components.length; ++i) {
      components[i] = new Point2D(xyPoints[i][0], xyPoints[i][1]);
    }
    return ComponentTree.create(components);
  }
}
