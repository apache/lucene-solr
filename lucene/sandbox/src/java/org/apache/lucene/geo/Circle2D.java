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
import org.apache.lucene.util.SloppyMath;


/**
 * 2D circle implementation containing geo spatial logic.
 *
 * @lucene.internal
 */
public class Circle2D implements Component2D {
  final GeoEncodingUtils.DistancePredicate distancePredicate;
  final Rectangle rectangle;
  final double lat;
  final double lon;
  final double distance;
  final double sortKey;
  final double axisLat;

  private Circle2D(double lon, double lat, double distance) {
    this.lat = lat;
    this.lon = lon;
    this.distance = distance;
    this.distancePredicate  = GeoEncodingUtils.createDistancePredicate(lat, lon, distance);
    this.rectangle = Rectangle.fromPointDistance(lat, lon, distance);
    this.sortKey = GeoUtils.distanceQuerySortKey(distance);
    this.axisLat = Rectangle.axisLat(lat, distance);
  }

  /** Checks if the circle contains the provided triangle **/
  public boolean containsTriangle(int ax, int ay, int bx, int by, int cx, int cy) {
    if (distancePredicate.test(ay, ax) && distancePredicate.test(by, bx) && distancePredicate.test(cy, cx)) {
      return true;
    }
    return false;
  }

  @Override
  public double getMinX() {
    return rectangle.minLon;
  }

  @Override
  public double getMaxX() {
    return rectangle.maxLon;
  }

  @Override
  public double getMinY() {
    return rectangle.minLat;
  }

  @Override
  public double getMaxY() {
    return rectangle.maxLat;
  }

  @Override
  public boolean contains(double x, double y) {
    return SloppyMath.haversinSortKey(lat, lon, this.lat, this.lon) <= distance;
  }

  @Override
  public Relation relate(double minX, double maxX, double minY, double maxY) {
    if (Component2D.disjoint(rectangle.minLon, rectangle.maxLon, rectangle.minLat, rectangle.maxLat, minX, maxX, minY, maxY)) {
      return Relation.CELL_OUTSIDE_QUERY;
    }
    if (Component2D.within(rectangle.minLon, rectangle.maxLon, rectangle.minLat, rectangle.maxLat, minX, maxX, minY, maxY)) {
      return Relation.CELL_CROSSES_QUERY;
    }
    return GeoUtils.relate(minY, maxY, minX, maxX, lat, lon, sortKey, axisLat);
  }

  @Override
  public Relation relateTriangle(double minX, double maxX, double minY, double maxY, double ax, double ay, double bx, double by, double cx, double cy) {
    if (Component2D.disjoint(rectangle.minLon, rectangle.maxLon, rectangle.minLat, rectangle.maxLat, minX, maxX, minY, maxY)) {
      return Relation.CELL_OUTSIDE_QUERY;
    }
    if (ax == bx && bx == cx && ay == by && by == cy) {
      // indexed "triangle" is a point: shortcut by checking contains
      return contains(ax, ay) ? Relation.CELL_INSIDE_QUERY : Relation.CELL_OUTSIDE_QUERY;
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

  @Override
  public WithinRelation withinTriangle(double minX, double maxX, double minY, double maxY, double ax, double ay, boolean ab, double bx, double by, boolean bc, double cx, double cy, boolean ca) {
    // short cut, lines and points cannot contain this type of shape
    if ((ax == bx && ay == by) || (ax == cx && ay == cy) || (bx == cx && by == cy)) {
      return WithinRelation.DISJOINT;
    }

    if (Component2D.disjoint(rectangle.minLon, rectangle.maxLon, rectangle.minLat, rectangle.maxLat, minX, maxX, minY, maxY)) {
      return WithinRelation.DISJOINT;
    }

    // if any of the points is inside the polygon, the polygon cannot be within this indexed
    // shape because points belong to the original indexed shape.
    if (contains(ax, ay) || contains(bx, by) || contains(cx, cy)) {
      return WithinRelation.NOTWITHIN;
    }

    WithinRelation relation = WithinRelation.DISJOINT;
    // if any of the edges intersects an the edge belongs to the shape then it cannot be within.
    // if it only intersects edges that do not belong to the shape, then it is a candidate
    // we skip edges at the dateline to support shapes crossing it
    if (intersectsLine(ax, ay, bx, by)) {
      if (ab == true) {
        return WithinRelation.NOTWITHIN;
      } else {
        relation = WithinRelation.CANDIDATE;
      }
    }

    if (intersectsLine(bx, by, cx, cy)) {
      if (bc == true) {
        return WithinRelation.NOTWITHIN;
      } else {
        relation = WithinRelation.CANDIDATE;
      }
    }
    if (intersectsLine(cx, cy, ax, ay)) {
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
    if (Component2D.pointInTriangle(minX, maxX, minY, maxY, lon, lat, ax, ay, bx, by, cx, cy) == true) {
      return WithinRelation.CANDIDATE;
    }
    return relation;
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

    //intersectsLine(aX, aY, bX, bY) || intersectsLine(bX, bY, cX, cY) || intersectsLine(cX, cY, aX, aY)
    if (numCorners == 2) {
      if (intersectsLine(a2x, a2y, b2x, b2y)) {
        return Relation.CELL_CROSSES_QUERY;
      }
      return Relation.CELL_INSIDE_QUERY;
    } else if (numCorners == 0) {
      if (intersectsLine(a2x, a2y, b2x, b2y)) {
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
      if (intersectsLine(ax, ay, bx, by) || intersectsLine(bx, by, cx, cy) || intersectsLine(cx, cy, ax, ay)) {
        return Relation.CELL_CROSSES_QUERY;
      }
      return Relation.CELL_INSIDE_QUERY;
    } else if (numCorners == 0) {
      if (Component2D.pointInTriangle(minX, maxX, minY, maxY, lon, lat, ax, ay, bx, by, cx, cy) == true) {
        return Relation.CELL_CROSSES_QUERY;
      }
      if (intersectsLine(ax, ay, bx, by) || intersectsLine(bx, by, cx, cy) || intersectsLine(cx, cy, ax, ay)) {
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

  /** Checks if the circle intersects the provided segment **/
  private boolean intersectsLine(double aX, double aY, double bX, double bY) {
    //Algorithm based on this thread : https://stackoverflow.com/questions/3120357/get-closest-point-to-a-line
    double[] vectorAP = new double[] {lon - aX, lat - aY};
    double[] vectorAB = new double[] {bX - aX, bY - aY};

    double magnitudeAB = vectorAB[0] * vectorAB[0] + vectorAB[1] * vectorAB[1];
    double dotProduct = vectorAP[0] * vectorAB[0] + vectorAP[1] * vectorAB[1];

    double distance = dotProduct / magnitudeAB;

    if (distance < 0 || distance > dotProduct)
    {
      return false;
    }

    double pX = aX + vectorAB[0] * distance;
    double pY = aY + vectorAB[1] * distance;

    double minLon = StrictMath.min(aX, bX);
    double minLat = StrictMath.min(aY, bY);
    double maxLon = StrictMath.max(aX, bX);
    double maxLat = StrictMath.max(aY, bY);

    if (pX >= minLon && pX <= maxLon && pY >= minLat && pY <= maxLat) {
      return contains(pX, pY);
    }
    return false;
  }

  /** Builds a circle from  a point and a distance in meters */
  public static Circle2D create(Circle circle) {
    return new Circle2D(circle.getLon(), circle.getLat(), circle.getRadius());
  }

}
