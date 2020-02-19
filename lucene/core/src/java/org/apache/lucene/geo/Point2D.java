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

/**
 * 2D point implementation containing geo spatial logic.
 */
final class Point2D implements Component2D {

  final private double x;
  final private double y;

  private Point2D(double x, double y) {
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
    if (ax == bx && bx == cx && ay == by && by == cy) {
      return contains(ax, ay) ? PointValues.Relation.CELL_INSIDE_QUERY : PointValues.Relation.CELL_OUTSIDE_QUERY;
    }
    if (Component2D.pointInTriangle(minX, maxX, minY, maxY, x, y, ax, ay, bx, by, cx, cy)) {
      return PointValues.Relation.CELL_CROSSES_QUERY;
    }
    return PointValues.Relation.CELL_OUTSIDE_QUERY;
  }

  @Override
  public WithinRelation withinTriangle(double minX, double maxX, double minY, double maxY,
                                       double aX, double aY, boolean ab, double bX, double bY, boolean bc, double cX, double cY, boolean ca) {
    if (Component2D.pointInTriangle(minX, maxX, minY, maxY, x, y, aX, aY, bX, bY, cX, cY)) {
      return WithinRelation.CANDIDATE;
    }
    return WithinRelation.DISJOINT;
  }

  /** create a Point2D component tree from a LatLon point */
  static Component2D create(Point point) {
    return new Point2D(GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(point.getLon())),
        GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(point.getLat())));
  }

  /** create a Point2D component tree from a XY point */
  static Component2D create(XYPoint xyPoint) {
    return new Point2D(xyPoint.getX(), xyPoint.getY());
  }
}
