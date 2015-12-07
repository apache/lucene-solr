package org.apache.lucene.util;

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

/**
 * Reusable geo-relation utility methods
 */
public class GeoRelationUtils {

  /**
   * Determine if a bbox (defined by minLon, minLat, maxLon, maxLat) contains the provided point (defined by lon, lat)
   * NOTE: this is a basic method that does not handle dateline or pole crossing. Unwrapping must be done before
   * calling this method.
   */
  public static boolean pointInRect(final double lon, final double lat, final double minLon,
                                    final double minLat, final double maxLon, final double maxLat) {
    return (GeoUtils.compare(lon, minLon) >= 0 && GeoUtils.compare(lon, maxLon) <= 0
        && GeoUtils.compare(lat, minLat) >= 0 && GeoUtils.compare(lat, maxLat) <= 0);
  }

  /**
   * simple even-odd point in polygon computation
   *    1.  Determine if point is contained in the longitudinal range
   *    2.  Determine whether point crosses the edge by computing the latitudinal delta
   *        between the end-point of a parallel vector (originating at the point) and the
   *        y-component of the edge sink
   *
   * NOTE: Requires polygon point (x,y) order either clockwise or counter-clockwise
   */
  public static boolean pointInPolygon(double[] x, double[] y, double lat, double lon) {
    assert x.length == y.length;
    boolean inPoly = false;
    /**
     * Note: This is using a euclidean coordinate system which could result in
     * upwards of 110KM error at the equator.
     * TODO convert coordinates to cylindrical projection (e.g. mercator)
     */
    for (int i = 1; i < x.length; i++) {
      if (x[i] < lon && x[i-1] >= lon || x[i-1] < lon && x[i] >= lon) {
        if (y[i] + (lon - x[i]) / (x[i-1] - x[i]) * (y[i-1] - y[i]) < lat) {
          inPoly = !inPoly;
        }
      }
    }
    return inPoly;
  }

  public static boolean rectDisjoint(final double aMinX, final double aMinY, final double aMaxX, final double aMaxY,
                                     final double bMinX, final double bMinY, final double bMaxX, final double bMaxY) {
    return (aMaxX < bMinX || aMinX > bMaxX || aMaxY < bMinY || aMinY > bMaxY);
  }

  /**
   * Computes whether the first (a) rectangle is wholly within another (b) rectangle (shared boundaries allowed)
   */
  public static boolean rectWithin(final double aMinX, final double aMinY, final double aMaxX, final double aMaxY,
                                   final double bMinX, final double bMinY, final double bMaxX, final double bMaxY) {
    return !(aMinX < bMinX || aMinY < bMinY || aMaxX > bMaxX || aMaxY > bMaxY);
  }

  public static boolean rectCrosses(final double aMinX, final double aMinY, final double aMaxX, final double aMaxY,
                                    final double bMinX, final double bMinY, final double bMaxX, final double bMaxY) {
    return !(rectDisjoint(aMinX, aMinY, aMaxX, aMaxY, bMinX, bMinY, bMaxX, bMaxY) ||
        rectWithin(aMinX, aMinY, aMaxX, aMaxY, bMinX, bMinY, bMaxX, bMaxY));
  }

  /**
   * Computes whether rectangle a contains rectangle b (touching allowed)
   */
  public static boolean rectContains(final double aMinX, final double aMinY, final double aMaxX, final double aMaxY,
                                     final double bMinX, final double bMinY, final double bMaxX, final double bMaxY) {
    return !(bMinX < aMinX || bMinY < aMinY || bMaxX > aMaxX || bMaxY > aMaxY);
  }

  /**
   * Computes whether a rectangle intersects another rectangle (crosses, within, touching, etc)
   */
  public static boolean rectIntersects(final double aMinX, final double aMinY, final double aMaxX, final double aMaxY,
                                       final double bMinX, final double bMinY, final double bMaxX, final double bMaxY) {
    return !((aMaxX < bMinX || aMinX > bMaxX || aMaxY < bMinY || aMinY > bMaxY) );
  }

  /**
   * Computes whether a rectangle crosses a shape. (touching not allowed)
   */
  public static boolean rectCrossesPoly(final double rMinX, final double rMinY, final double rMaxX,
                                        final double rMaxY, final double[] shapeX, final double[] shapeY,
                                        final double sMinX, final double sMinY, final double sMaxX,
                                        final double sMaxY) {
    // short-circuit: if the bounding boxes are disjoint then the shape does not cross
    if (rectDisjoint(rMinX, rMinY, rMaxX, rMaxY, sMinX, sMinY, sMaxX, sMaxY)) {
      return false;
    }

    final double[][] bbox = new double[][] { {rMinX, rMinY}, {rMaxX, rMinY}, {rMaxX, rMaxY}, {rMinX, rMaxY}, {rMinX, rMinY} };
    final int polyLength = shapeX.length-1;
    double d, s, t, a1, b1, c1, a2, b2, c2;
    double x00, y00, x01, y01, x10, y10, x11, y11;

    // computes the intersection point between each bbox edge and the polygon edge
    for (short b=0; b<4; ++b) {
      a1 = bbox[b+1][1]-bbox[b][1];
      b1 = bbox[b][0]-bbox[b+1][0];
      c1 = a1*bbox[b+1][0] + b1*bbox[b+1][1];
      for (int p=0; p<polyLength; ++p) {
        a2 = shapeY[p+1]-shapeY[p];
        b2 = shapeX[p]-shapeX[p+1];
        // compute determinant
        d = a1*b2 - a2*b1;
        if (d != 0) {
          // lines are not parallel, check intersecting points
          c2 = a2*shapeX[p+1] + b2*shapeY[p+1];
          s = (1/d)*(b2*c1 - b1*c2);
          t = (1/d)*(a1*c2 - a2*c1);
          x00 = StrictMath.min(bbox[b][0], bbox[b+1][0]) - GeoUtils.TOLERANCE;
          x01 = StrictMath.max(bbox[b][0], bbox[b+1][0]) + GeoUtils.TOLERANCE;
          y00 = StrictMath.min(bbox[b][1], bbox[b+1][1]) - GeoUtils.TOLERANCE;
          y01 = StrictMath.max(bbox[b][1], bbox[b+1][1]) + GeoUtils.TOLERANCE;
          x10 = StrictMath.min(shapeX[p], shapeX[p+1]) - GeoUtils.TOLERANCE;
          x11 = StrictMath.max(shapeX[p], shapeX[p+1]) + GeoUtils.TOLERANCE;
          y10 = StrictMath.min(shapeY[p], shapeY[p+1]) - GeoUtils.TOLERANCE;
          y11 = StrictMath.max(shapeY[p], shapeY[p+1]) + GeoUtils.TOLERANCE;
          // check whether the intersection point is touching one of the line segments
          boolean touching = ((x00 == s && y00 == t) || (x01 == s && y01 == t))
              || ((x10 == s && y10 == t) || (x11 == s && y11 == t));
          // if line segments are not touching and the intersection point is within the range of either segment
          if (!(touching || x00 > s || x01 < s || y00 > t || y01 < t || x10 > s || x11 < s || y10 > t || y11 < t)) {
            return true;
          }
        }
      } // for each poly edge
    } // for each bbox edge
    return false;
  }

  /**
   * Computes whether a rectangle is within a given polygon (shared boundaries allowed)
   */
  public static boolean rectWithinPoly(final double rMinX, final double rMinY, final double rMaxX, final double rMaxY,
                                       final double[] shapeX, final double[] shapeY, final double sMinX,
                                       final double sMinY, final double sMaxX, final double sMaxY) {
    // check if rectangle crosses poly (to handle concave/pacman polys), then check that all 4 corners
    // are contained
    return !(rectCrossesPoly(rMinX, rMinY, rMaxX, rMaxY, shapeX, shapeY, sMinX, sMinY, sMaxX, sMaxY) ||
        !pointInPolygon(shapeX, shapeY, rMinY, rMinX) || !pointInPolygon(shapeX, shapeY, rMinY, rMaxX) ||
        !pointInPolygon(shapeX, shapeY, rMaxY, rMaxX) || !pointInPolygon(shapeX, shapeY, rMaxY, rMinX));
  }

  private static boolean rectAnyCornersInCircle(final double rMinX, final double rMinY, final double rMaxX,
                                                final double rMaxY, final double centerLon, final double centerLat,
                                                final double radiusMeters, final boolean approx) {
    if (approx == true) {
      return rectAnyCornersInCircleSloppy(rMinX, rMinY, rMaxX, rMaxY, centerLon, centerLat, radiusMeters);
    }
    double w = Math.abs(rMaxX - rMinX);
    if (w <= 90.0) {
      return GeoDistanceUtils.haversin(centerLat, centerLon, rMinY, rMinX) <= radiusMeters
          || GeoDistanceUtils.haversin(centerLat, centerLon, rMaxY, rMinX) <= radiusMeters
          || GeoDistanceUtils.haversin(centerLat, centerLon, rMaxY, rMaxX) <= radiusMeters
          || GeoDistanceUtils.haversin(centerLat, centerLon, rMinY, rMaxX) <= radiusMeters;
    }
    // partition
    w /= 4;
    final double p1 = rMinX + w;
    final double p2 = p1 + w;
    final double p3 = p2 + w;

    return GeoDistanceUtils.haversin(centerLat, centerLon, rMinY, rMinX) <= radiusMeters
        || GeoDistanceUtils.haversin(centerLat, centerLon, rMaxY, rMinX) <= radiusMeters
        || GeoDistanceUtils.haversin(centerLat, centerLon, rMaxY, p1) <= radiusMeters
        || GeoDistanceUtils.haversin(centerLat, centerLon, rMinY, p1) <= radiusMeters
        || GeoDistanceUtils.haversin(centerLat, centerLon, rMinY, p2) <= radiusMeters
        || GeoDistanceUtils.haversin(centerLat, centerLon, rMaxY, p2) <= radiusMeters
        || GeoDistanceUtils.haversin(centerLat, centerLon, rMaxY, p3) <= radiusMeters
        || GeoDistanceUtils.haversin(centerLat, centerLon, rMinY, p3) <= radiusMeters
        || GeoDistanceUtils.haversin(centerLat, centerLon, rMaxY, rMaxX) <= radiusMeters
        || GeoDistanceUtils.haversin(centerLat, centerLon, rMinY, rMaxX) <= radiusMeters;
  }

  private static boolean rectAnyCornersInCircleSloppy(final double rMinX, final double rMinY, final double rMaxX, final double rMaxY,
                                                      final double centerLon, final double centerLat, final double radiusMeters) {
    return SloppyMath.haversin(centerLat, centerLon, rMinY, rMinX)*1000.0 <= radiusMeters
        || SloppyMath.haversin(centerLat, centerLon, rMaxY, rMinX)*1000.0 <= radiusMeters
        || SloppyMath.haversin(centerLat, centerLon, rMaxY, rMaxX)*1000.0 <= radiusMeters
        || SloppyMath.haversin(centerLat, centerLon, rMinY, rMaxX)*1000.0 <= radiusMeters;
  }

  private static boolean rectAnyCornersOutsideCircle(final double rMinX, final double rMinY, final double rMaxX,
                                                     final double rMaxY, final double centerLon, final double centerLat,
                                                     final double radiusMeters, final boolean approx) {
    if (approx == true) {
      return rectAnyCornersOutsideCircleSloppy(rMinX, rMinY, rMaxX, rMaxY, centerLon, centerLat, radiusMeters);
    }
    double w = Math.abs(rMaxX - rMinX);
    if (w <= 90.0) {
      return GeoDistanceUtils.haversin(centerLat, centerLon, rMinY, rMinX) > radiusMeters
          || GeoDistanceUtils.haversin(centerLat, centerLon, rMaxY, rMinX) > radiusMeters
          || GeoDistanceUtils.haversin(centerLat, centerLon, rMaxY, rMaxX) > radiusMeters
          || GeoDistanceUtils.haversin(centerLat, centerLon, rMinY, rMaxX) > radiusMeters;
    }
    // partition
    w /= 4;
    final double p1 = rMinX + w;
    final double p2 = p1 + w;
    final double p3 = p2 + w;

    return GeoDistanceUtils.haversin(centerLat, centerLon, rMinY, rMinX) > radiusMeters
        || GeoDistanceUtils.haversin(centerLat, centerLon, rMaxY, rMinX) > radiusMeters
        || GeoDistanceUtils.haversin(centerLat, centerLon, rMaxY, p1) > radiusMeters
        || GeoDistanceUtils.haversin(centerLat, centerLon, rMinY, p1) > radiusMeters
        || GeoDistanceUtils.haversin(centerLat, centerLon, rMinY, p2) > radiusMeters
        || GeoDistanceUtils.haversin(centerLat, centerLon, rMaxY, p2) > radiusMeters
        || GeoDistanceUtils.haversin(centerLat, centerLon, rMaxY, p3) > radiusMeters
        || GeoDistanceUtils.haversin(centerLat, centerLon, rMinY, p3) > radiusMeters
        || GeoDistanceUtils.haversin(centerLat, centerLon, rMaxY, rMaxX) > radiusMeters
        || GeoDistanceUtils.haversin(centerLat, centerLon, rMinY, rMaxX) > radiusMeters;
  }

  private static boolean rectAnyCornersOutsideCircleSloppy(final double rMinX, final double rMinY, final double rMaxX, final double rMaxY,
                                                           final double centerLon, final double centerLat, final double radiusMeters) {
    return SloppyMath.haversin(centerLat, centerLon, rMinY, rMinX)*1000.0 > radiusMeters
        || SloppyMath.haversin(centerLat, centerLon, rMaxY, rMinX)*1000.0 > radiusMeters
        || SloppyMath.haversin(centerLat, centerLon, rMaxY, rMaxX)*1000.0 > radiusMeters
        || SloppyMath.haversin(centerLat, centerLon, rMinY, rMaxX)*1000.0 > radiusMeters;
  }

  public static boolean rectWithinCircle(final double rMinX, final double rMinY, final double rMaxX, final double rMaxY,
                                         final double centerLon, final double centerLat, final double radiusMeters) {
    return rectWithinCircle(rMinX, rMinY, rMaxX, rMaxY, centerLon, centerLat, radiusMeters, false);
  }

  public static boolean rectWithinCircle(final double rMinX, final double rMinY, final double rMaxX, final double rMaxY,
                                         final double centerLon, final double centerLat, final double radiusMeters,
                                         final boolean approx) {
    return rectAnyCornersOutsideCircle(rMinX, rMinY, rMaxX, rMaxY, centerLon, centerLat, radiusMeters, approx) == false;
  }

  /**
   * Determine if a bbox (defined by minLon, minLat, maxLon, maxLat) contains the provided point (defined by lon, lat)
   * NOTE: this is basic method that does not handle dateline or pole crossing. Unwrapping must be done before
   * calling this method.
   */
  public static boolean rectCrossesCircle(final double rMinX, final double rMinY, final double rMaxX, final double rMaxY,
                                          final double centerLon, final double centerLat, final double radiusMeters) {
    return rectCrossesCircle(rMinX, rMinY, rMaxX, rMaxY, centerLon, centerLat, radiusMeters, false);
  }

  public static boolean rectCrossesCircle(final double rMinX, final double rMinY, final double rMaxX, final double rMaxY,
                                          final double centerLon, final double centerLat, final double radiusMeters,
                                          final boolean approx) {
    if (approx == true) {
      return rectAnyCornersInCircle(rMinX, rMinY, rMaxX, rMaxY, centerLon, centerLat, radiusMeters, approx)
          || isClosestPointOnRectWithinRange(rMinX, rMinY, rMaxX, rMaxY, centerLon, centerLat, radiusMeters, approx);
    }

    return (rectAnyCornersInCircle(rMinX, rMinY, rMaxX, rMaxY, centerLon, centerLat, radiusMeters, approx) &&
        rectAnyCornersOutsideCircle(rMinX, rMinY, rMaxX, rMaxY, centerLon, centerLat, radiusMeters, approx))
        || isClosestPointOnRectWithinRange(rMinX, rMinY, rMaxX, rMaxY, centerLon, centerLat, radiusMeters, approx);
  }

  private static boolean isClosestPointOnRectWithinRange(final double rMinX, final double rMinY, final double rMaxX, final double rMaxY,
                                                         final double centerLon, final double centerLat, final double radiusMeters,
                                                         final boolean approx) {
    double[] closestPt = {0, 0};
    GeoDistanceUtils.closestPointOnBBox(rMinX, rMinY, rMaxX, rMaxY, centerLon, centerLat, closestPt);
    boolean haverShortCut = GeoDistanceUtils.haversin(centerLat, centerLon, closestPt[1], closestPt[0]) <= radiusMeters;
    if (approx == true || haverShortCut == true) {
      return haverShortCut;
    }
    double lon1 = rMinX;
    double lon2 = rMaxX;
    double lat1 = rMinY;
    double lat2 = rMaxY;
    if (closestPt[0] == rMinX || closestPt[0] == rMaxX) {
      lon1 = closestPt[0];
      lon2 = lon1;
    } else if (closestPt[1] == rMinY || closestPt[1] == rMaxY) {
      lat1 = closestPt[1];
      lat2 = lat1;
    }

    return lineCrossesSphere(lon1, lat1, 0, lon2, lat2, 0, centerLon, centerLat, 0, radiusMeters);
  }

  /**
   * Computes whether or a 3dimensional line segment intersects or crosses a sphere
   *
   * @param lon1 longitudinal location of the line segment start point (in degrees)
   * @param lat1 latitudinal location of the line segment start point (in degrees)
   * @param alt1 altitude of the line segment start point (in degrees)
   * @param lon2 longitudinal location of the line segment end point (in degrees)
   * @param lat2 latitudinal location of the line segment end point (in degrees)
   * @param alt2 altitude of the line segment end point (in degrees)
   * @param centerLon longitudinal location of center search point (in degrees)
   * @param centerLat latitudinal location of center search point (in degrees)
   * @param centerAlt altitude of the center point (in meters)
   * @param radiusMeters search sphere radius (in meters)
   * @return whether the provided line segment is a secant of the
   */
  private static boolean lineCrossesSphere(double lon1, double lat1, double alt1, double lon2,
                                           double lat2, double alt2, double centerLon, double centerLat,
                                           double centerAlt, double radiusMeters) {
    // convert to cartesian 3d (in meters)
    double[] ecf1 = GeoProjectionUtils.llaToECF(lon1, lat1, alt1, null);
    double[] ecf2 = GeoProjectionUtils.llaToECF(lon2, lat2, alt2, null);
    double[] cntr = GeoProjectionUtils.llaToECF(centerLon, centerLat, centerAlt, null);

    // convert radius from arc radius to cartesian radius
    double[] oneEighty = GeoProjectionUtils.pointFromLonLatBearingGreatCircle(centerLon, centerLat, 180.0d, radiusMeters, new double[3]);
    GeoProjectionUtils.llaToECF(oneEighty[0], oneEighty[1], 0, oneEighty);

    radiusMeters = GeoDistanceUtils.linearDistance(oneEighty, cntr);//   Math.sqrt(oneEighty[0]*cntr[0] + oneEighty[1]*cntr[1] + oneEighty[2]*cntr[2]);

    final double dX = ecf2[0] - ecf1[0];
    final double dY = ecf2[1] - ecf1[1];
    final double dZ = ecf2[2] - ecf1[2];
    final double fX = ecf1[0] - cntr[0];
    final double fY = ecf1[1] - cntr[1];
    final double fZ = ecf1[2] - cntr[2];

    final double a = dX*dX + dY*dY + dZ*dZ;
    final double b = 2 * (fX*dX + fY*dY + fZ*dZ);
    final double c = (fX*fX + fY*fY + fZ*fZ) - (radiusMeters*radiusMeters);

    double discrim = (b*b)-(4*a*c);
    if (discrim < 0) {
      return false;
    }

    discrim = StrictMath.sqrt(discrim);
    final double a2 = 2*a;
    final double t1 = (-b - discrim)/a2;
    final double t2 = (-b + discrim)/a2;

    if ( (t1 < 0 || t1 > 1) ) {
      return !(t2 < 0 || t2 > 1);
    }

    return true;
  }
}
