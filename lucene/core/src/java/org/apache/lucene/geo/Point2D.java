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

import static org.apache.lucene.geo.GeoUtils.orient;

import org.apache.lucene.index.PointValues;

/** 2D point implementation containing geo spatial logic. */
final class Point2D implements Component2D {

  private final double x;
  private final double y;

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
  public boolean intersectsLine(
          double minX,
          double maxX,
          double minY,
          double maxY,
          double aX,
          double aY,
          double bX,
          double bY) {
    return Component2D.containsPoint(x, y, minX, maxX, minY, maxY)
            && orient(aX, aY, bX, bY, x, y) == 0;
  }

  @Override
  public boolean intersectsTriangle(
          double minX,
          double maxX,
          double minY,
          double maxY,
          double aX,
          double aY,
          double bX,
          double bY,
          double cX,
          double cY) {
    return Component2D.pointInTriangle(minX, maxX, minY, maxY, x, y, aX, aY, bX, bY, cX, cY);
  }

  @Override
  public boolean containsLine(
          double minX,
          double maxX,
          double minY,
          double maxY,
          double aX,
          double aY,
          double bX,
          double bY) {
    return false;
  }

  @Override
  public boolean containsTriangle(
          double minX,
          double maxX,
          double minY,
          double maxY,
          double aX,
          double aY,
          double bX,
          double bY,
          double cX,
          double cY) {
    return false;
  }

  @Override
  public WithinRelation withinPoint(double x, double y) {
    return contains(x, y) ? WithinRelation.CANDIDATE : WithinRelation.DISJOINT;
  }

  @Override
  public WithinRelation withinLine(
          double minX,
          double maxX,
          double minY,
          double maxY,
          double aX,
          double aY,
          boolean ab,
          double bX,
          double bY) {
    // can be improved?
    return intersectsLine(minX, maxX, minY, maxY, aX, aY, bX, bY)
            ? WithinRelation.CANDIDATE
            : WithinRelation.DISJOINT;
  }

  @Override
  public WithinRelation withinTriangle(
          double minX,
          double maxX,
          double minY,
          double maxY,
          double aX,
          double aY,
          boolean ab,
          double bX,
          double bY,
          boolean bc,
          double cX,
          double cY,
          boolean ca) {
    if (Component2D.pointInTriangle(minX, maxX, minY, maxY, x, y, aX, aY, bX, bY, cX, cY)) {
      return WithinRelation.CANDIDATE;
    }
    return WithinRelation.DISJOINT;
  }

  /** create a Point2D component tree from a LatLon point */
  static Component2D create(Point point) {
    // Points behave as rectangles
    double qLat =
            point.getLat() == GeoUtils.MAX_LAT_INCL
                    ? point.getLat()
                    : GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitudeCeil(point.getLat()));
    double qLon =
            point.getLon() == GeoUtils.MAX_LON_INCL
                    ? point.getLon()
                    : GeoEncodingUtils.decodeLongitude(
                    GeoEncodingUtils.encodeLongitudeCeil(point.getLon()));
    return new Point2D(qLon, qLat);
  }

  /** create a Point2D component tree from a XY point */
  static Component2D create(XYPoint xyPoint) {
    return new Point2D(xyPoint.getX(), xyPoint.getY());
  }
}
