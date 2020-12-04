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

import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitudeCeil;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitudeCeil;
import static org.apache.lucene.geo.GeoEncodingUtils.decodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.decodeLongitude;
import static org.apache.lucene.geo.GeoEncodingUtils.MAX_LON_ENCODED;
import static org.apache.lucene.geo.GeoEncodingUtils.MIN_LON_ENCODED;

/**
 * 2D rectangle implementation containing cartesian spatial logic.
 */
final class Rectangle2D implements Component2D {

  private final double minX;
  private final double maxX;
  private final double minY;
  private final double maxY;

  private Rectangle2D(double minX, double maxX, double minY, double maxY) {
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
  public boolean intersectsLine(double minX, double maxX, double minY, double maxY,
                                double aX, double aY, double bX, double bY) {
    if (Component2D.disjoint(this.minX, this.maxX, this.minY, this.maxY, minX, maxX, minY, maxY)) {
      return false;
    }
    return contains(aX, aY) || contains(bX, bY) || edgesIntersect(aX, aY, bX, bY);
  }

  @Override
  public boolean intersectsTriangle(double minX, double maxX, double minY, double maxY,
                                    double aX, double aY, double bX, double bY, double cX, double cY) {
    if (Component2D.disjoint(this.minX, this.maxX, this.minY, this.maxY, minX, maxX, minY, maxY)) {
      return false;
    }
    return contains(aX, aY) || contains(bX, bY) || contains(cX, cY) ||
        Component2D.pointInTriangle(minX, maxX, minY, maxY, this.minX, this.minY,aX, aY, bX, bY, cX, cY) ||
        edgesIntersect(aX, aY, bX, bY) ||
        edgesIntersect(bX, bY, cX, cY) ||
        edgesIntersect(cX, cY, aX, aY);
  }

  @Override
  public boolean containsLine(double minX, double maxX, double minY, double maxY,
                              double aX, double aY, double bX, double bY) {
    return Component2D.within(minX, maxX, minY, maxY, this.minX, this.maxX, this.minY, this.maxY);
  }

  @Override
  public boolean containsTriangle(double minX, double maxX, double minY, double maxY,
                                  double aX, double aY, double bX, double bY, double cX, double cY) {
    return Component2D.within(minX, maxX, minY, maxY, this.minX, this.maxX, this.minY, this.maxY);
  }

  @Override
  public WithinRelation withinPoint(double x, double y) {
    return contains(x, y) ? WithinRelation.NOTWITHIN : WithinRelation.DISJOINT;
  }

  @Override
  public WithinRelation withinLine(double minX, double maxX, double minY, double maxY,
                                   double aX, double aY, boolean ab, double bX, double bY) {
    if (ab == true && Component2D.disjoint(this.minX, this.maxX, this.minY, this.maxY, minX, maxX, minY, maxY) == false &&
        edgesIntersect(aX, aY, bX, bY)) {
      return WithinRelation.NOTWITHIN;
    }
    return WithinRelation.DISJOINT;
  }

  @Override
  public WithinRelation withinTriangle(double minX, double maxX, double minY, double maxY,
                                       double aX, double aY, boolean ab, double bX, double bY, boolean bc, double cX, double cY, boolean ca) {
    // Bounding boxes disjoint?
    if (Component2D.disjoint(this.minX, this.maxX, this.minY, this.maxY, minX, maxX, minY, maxY)) {
      return WithinRelation.DISJOINT;
    }

    // Points belong to the shape so if points are inside the rectangle then it cannot be within.
    if (contains(aX, aY) || contains(bX, bY) || contains(cX, cY)) {
      return WithinRelation.NOTWITHIN;
    }
    // If any of the edges intersects an edge belonging to the shape then it cannot be within.
    WithinRelation relation = WithinRelation.DISJOINT;
    if (edgesIntersect(aX, aY, bX, bY) == true) {
      if (ab == true) {
        return WithinRelation.NOTWITHIN;
      } else {
        relation = WithinRelation.CANDIDATE;
      }
    }
    if (edgesIntersect(bX, bY, cX, cY) == true) {
      if (bc == true) {
        return WithinRelation.NOTWITHIN;
      } else {
        relation = WithinRelation.CANDIDATE;
      }
    }

    if (edgesIntersect(cX, cY, aX, aY) == true) {
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
    if (Component2D.pointInTriangle(minX, maxX, minY, maxY, this.minX, this.minY, aX, aY, bX, bY, cX, cY)) {
      return WithinRelation.CANDIDATE;
    }
    return relation;
  }

  private boolean edgesIntersect(double aX, double aY, double bX, double bY) {
    // shortcut: check bboxes of edges are disjoint
    if (Math.max(aX, bX) < minX || Math.min(aX, bX) > maxX || Math.min(aY, bY) > maxY || Math.max(aY, bY) < minY) {
      return false;
    }
    return GeoUtils.lineCrossesLineWithBoundary(aX, aY, bX, bY, minX, maxY,  maxX, maxY) || // top
           GeoUtils.lineCrossesLineWithBoundary(aX, aY, bX, bY, maxX, maxY,  maxX, minY) || // bottom
           GeoUtils.lineCrossesLineWithBoundary(aX, aY, bX, bY, maxX, minY,  minX, minY) || // left
           GeoUtils.lineCrossesLineWithBoundary(aX, aY, bX, bY, minX, minY,  minX, maxY);   // right
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Rectangle2D)) return false;
    Rectangle2D that = (Rectangle2D) o;
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

  /** create a component2D from the provided XY rectangle */
  static Component2D create(XYRectangle rectangle) {
    return new Rectangle2D(rectangle.minX, rectangle.maxX, rectangle.minY, rectangle.maxY);
  }

  private static double MIN_LON_INCL_QUANTIZE = decodeLongitude(MIN_LON_ENCODED);
  private static double MAX_LON_INCL_QUANTIZE = decodeLongitude(MAX_LON_ENCODED);

  /** create a component2D from the provided LatLon rectangle */
  static Component2D create(Rectangle rectangle) {
    // behavior of LatLonPoint.newBoxQuery()
    double minLongitude = rectangle.minLon;
    boolean crossesDateline = rectangle.minLon > rectangle.maxLon;
    if (minLongitude == 180.0 && crossesDateline) {
      minLongitude = -180;
      crossesDateline = false;
    }
    // need to quantize!
    double qMinLat = decodeLatitude(encodeLatitudeCeil(rectangle.minLat));
    double qMaxLat = decodeLatitude(encodeLatitude(rectangle.maxLat));
    double qMinLon = decodeLongitude(encodeLongitudeCeil(minLongitude));
    double qMaxLon = decodeLongitude(encodeLongitude(rectangle.maxLon));
    if (crossesDateline) {
      // for rectangles that cross the dateline we need to create two components
      Component2D[] components = new Component2D[2];
      components[0] = new Rectangle2D(MIN_LON_INCL_QUANTIZE, qMaxLon, qMinLat, qMaxLat);
      components[1] = new Rectangle2D(qMinLon, MAX_LON_INCL_QUANTIZE, qMinLat, qMaxLat);
      return ComponentTree.create(components);
    } else {
      return new Rectangle2D(qMinLon, qMaxLon, qMinLat, qMaxLat);
    }
  }
}