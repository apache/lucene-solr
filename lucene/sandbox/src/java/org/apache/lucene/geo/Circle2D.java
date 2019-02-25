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
import org.apache.lucene.util.NumericUtils;

import static org.apache.lucene.geo.GeoUtils.orient;

/**
 * 2D circle implementation containing spatial logic.
 *
 * @lucene.internal
 */
public class Circle2D {
  final GeoEncodingUtils.DistancePredicate distancePredicate;
  final Rectangle2D rectangle2D;
  final double lat;
  final double lon;
  final double distance;
  final double sortKey;
  final double axisLat;

  private Circle2D(double lat, double lon, double distance) {
    this.lat = lat;
    this.lon = lon;
    this.distance = distance;
    this.distancePredicate  = GeoEncodingUtils.createDistancePredicate(lat, lon, distance);
    Rectangle rectangle = Rectangle.fromPointDistance(lat, lon, distance);
    rectangle2D = Rectangle2D.create(rectangle);
    sortKey = GeoUtils.distanceQuerySortKey(distance);
    axisLat = Rectangle.axisLat(lat, distance);
  }

  /** Builds a circle from  a point and a distance in meters */
  public static Circle2D create(Circle circle) {
    return new Circle2D(circle.getLat(), circle.getLon(), circle.getRadius());
  }

  /** Checks if the circle contains the provided point **/
  public boolean queryContainsPoint(int x, int y) {
    return distancePredicate.test(y, x);
  }

  /** compare this to a provided range bounding box **/
  public Relation relateRangeBBox(int minXOffset, int minYOffset, byte[] minTriangle,
                                  int maxXOffset, int maxYOffset, byte[] maxTriangle) {
    //first we try the bounding box
    Relation relation = rectangle2D.relateRangeBBox(minXOffset, minYOffset, minTriangle, maxXOffset, maxYOffset, maxTriangle);
    if (relation != Relation.CELL_OUTSIDE_QUERY) {
      double minLat = GeoEncodingUtils.decodeLatitude(NumericUtils.sortableBytesToInt(minTriangle, minYOffset));
      double minLon = GeoEncodingUtils.decodeLongitude(NumericUtils.sortableBytesToInt(minTriangle, minXOffset));
      double maxLat = GeoEncodingUtils.decodeLatitude(NumericUtils.sortableBytesToInt(maxTriangle, maxYOffset));
      double maxLon = GeoEncodingUtils.decodeLongitude(NumericUtils.sortableBytesToInt(maxTriangle, maxXOffset));
      return GeoUtils.relate(minLat, maxLat, minLon, maxLon, lat, lon, sortKey, axisLat);
    }
    return relation;
  }

  /** Checks if the circle intersects the provided triangle **/
  public boolean intersectsTriangle(int aX, int aY, int bX, int bY, int cX, int cY) {
    // 1. query contains any triangle points
    if (distancePredicate.test(aY, aX) || distancePredicate.test(bY, bX) || distancePredicate.test(cY, cX)) {
      return true;
    }

    double aLon = GeoEncodingUtils.decodeLongitude(aX);
    double aLat = GeoEncodingUtils.decodeLatitude(aY);
    double bLon = GeoEncodingUtils.decodeLongitude(bX);
    double bLat = GeoEncodingUtils.decodeLatitude(bY);
    double cLon = GeoEncodingUtils.decodeLongitude(cX);
    double cLat = GeoEncodingUtils.decodeLatitude(cY);
    // 2.- Is center of the circle within the triangle
    if (pointInTriangle(lon, lat, aLon, aLat, bLon, bLat , cLon, cLat)) {
      return true;
    }
    // 3.- check intersections
    if (intersectsLine(aLon, aLat, bLon, bLat) || intersectsLine(bLon, bLat, cLon, cLat) || intersectsLine(cLon, cLat, aLon, aLat)) {
      return true;
    }
    return false;
  }

  //This should be moved when LatLonShape is moved from sandbox!
  /**
   * Compute whether the given x, y point is in a triangle; uses the winding order method */
  private static boolean pointInTriangle (double x, double y, double ax, double ay, double bx, double by, double cx, double cy) {
    double minX = StrictMath.min(ax, StrictMath.min(bx, cx));
    double minY = StrictMath.min(ay, StrictMath.min(by, cy));
    double maxX = StrictMath.max(ax, StrictMath.max(bx, cx));
    double maxY = StrictMath.max(ay, StrictMath.max(by, cy));
    //check the bounding box because if the triangle is degenerated, e.g points and lines, we need to filter out
    //coplanar points that are not part of the triangle.
    if (x >= minX && x <= maxX && y >= minY && y <= maxY ) {
      int a = orient(x, y, ax, ay, bx, by);
      int b = orient(x, y, bx, by, cx, cy);
      if (a == 0 || b == 0 || a < 0 == b < 0) {
        int c = orient(x, y, cx, cy, ax, ay);
        return c == 0 || (c < 0 == (b < 0 || a < 0));
      }
      return false;
    } else {
      return false;
    }
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
      return distancePredicate.test(GeoEncodingUtils.encodeLatitude(pY), GeoEncodingUtils.encodeLongitude(pX));
    }
    return false;
  }

  /** Checks if the circle contains the provided triangle **/
  public boolean containsTriangle(int ax, int ay, int bx, int by, int cx, int cy) {
    if (distancePredicate.test(ay, ax) && distancePredicate.test(by, bx) && distancePredicate.test(cy, cx)) {
      return true;
    }
    return false;
  }

  @Override
  public boolean equals(Object o) {
    Circle2D other = (Circle2D)o;
    return lon == other.lon && lat == other.lat && distance == other.distance;
  }

  @Override
  public int hashCode() {
    int hash = super.hashCode();
    hash = 31 * hash + Double.hashCode(lat);
    hash = 31 * hash + Double.hashCode(lon);
    hash = 31 * hash + Double.hashCode(distance);
    return hash;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("Circle(lat=" + lat);
    sb.append(", lon=" + lon);
    sb.append(", distance=" + distance);
    sb.append(")");
    return sb.toString();
  }
}
