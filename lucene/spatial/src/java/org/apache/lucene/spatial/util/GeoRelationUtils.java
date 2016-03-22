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
package org.apache.lucene.spatial.util;

/**
 * Reusable geo-relation utility methods
 */
public class GeoRelationUtils {

  // No instance:
  private GeoRelationUtils() {
  }

  /**
   * Determine if a bbox (defined by minLat, maxLat, minLon, maxLon) contains the provided point (defined by lat, lon)
   * NOTE: this is a basic method that does not handle dateline or pole crossing. Unwrapping must be done before
   * calling this method.
   */
  public static boolean pointInRectPrecise(final double lat, final double lon,
                                           final double minLat, final double maxLat,
                                           final double minLon, final double maxLon) {
    return lat >= minLat && lat <= maxLat && lon >= minLon && lon <= maxLon;
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
  public static boolean pointInPolygon(double[] polyLats, double[] polyLons, double lat, double lon) {
    assert polyLats.length == polyLons.length;
    boolean inPoly = false;
    /**
     * Note: This is using a euclidean coordinate system which could result in
     * upwards of 110KM error at the equator.
     * TODO convert coordinates to cylindrical projection (e.g. mercator)
     */
    for (int i = 1; i < polyLats.length; i++) {
      if (polyLons[i] <= lon && polyLons[i-1] >= lon || polyLons[i-1] <= lon && polyLons[i] >= lon) {
        if (polyLats[i] + (lon - polyLons[i]) / (polyLons[i-1] - polyLons[i]) * (polyLats[i-1] - polyLats[i]) <= lat) {
          inPoly = !inPoly;
        }
      }
    }
    return inPoly;
  }

  /////////////////////////
  // Rectangle relations
  /////////////////////////

  /**
   * Computes whether two rectangles are disjoint
   */
  private static boolean rectDisjoint(final double aMinLat, final double aMaxLat, final double aMinLon, final double aMaxLon,
                                      final double bMinLat, final double bMaxLat, final double bMinLon, final double bMaxLon) {
    return (aMaxLon < bMinLon || aMinLon > bMaxLon || aMaxLat < bMinLat || aMinLat > bMaxLat);
  }

  /**
   * Computes whether the first (a) rectangle is wholly within another (b) rectangle (shared boundaries allowed)
   */
  public static boolean rectWithin(final double aMinLat, final double aMaxLat, final double aMinLon, final double aMaxLon,
                                   final double bMinLat, final double bMaxLat, final double bMinLon, final double bMaxLon) {
    return !(aMinLon < bMinLon || aMinLat < bMinLat || aMaxLon > bMaxLon || aMaxLat > bMaxLat);
  }

  /**
   * Computes whether two rectangles cross
   */
  public static boolean rectCrosses(final double aMinLat, final double aMaxLat, final double aMinLon, final double aMaxLon,
                                    final double bMinLat, final double bMaxLat, final double bMinLon, final double bMaxLon) {
    return !(rectDisjoint(aMinLat, aMaxLat, aMinLon, aMaxLon, bMinLat, bMaxLat, bMinLon, bMaxLon) ||
             rectWithin(aMinLat, aMaxLat, aMinLon, aMaxLon, bMinLat, bMaxLat, bMinLon, bMaxLon));
  }

  /**
   * Computes whether a rectangle intersects another rectangle (crosses, within, touching, etc)
   */
  public static boolean rectIntersects(final double aMinLat, final double aMaxLat, final double aMinLon, final double aMaxLon,
                                       final double bMinLat, final double bMaxLat, final double bMinLon, final double bMaxLon) {
    return !((aMaxLon < bMinLon || aMinLon > bMaxLon || aMaxLat < bMinLat || aMinLat > bMaxLat));
  }

  /////////////////////////
  // Polygon relations
  /////////////////////////

  /**
   * Convenience method for accurately computing whether a rectangle crosses a poly
   */
  public static boolean rectCrossesPolyPrecise(final double rMinLat, final double rMaxLat,
                                               final double rMinLon, final double rMaxLon,
                                               final double[] shapeLat, final double[] shapeLon,
                                               final double sMinLat, final double sMaxLat,
                                               final double sMinLon, final double sMaxLon) {
    // short-circuit: if the bounding boxes are disjoint then the shape does not cross
    if (rectDisjoint(rMinLat, rMaxLat, rMinLon, rMaxLon, sMinLat, sMaxLat, sMinLon, sMaxLon)) {
      return false;
    }
    return rectCrossesPoly(rMinLat, rMaxLat, rMinLon, rMaxLon, shapeLat, shapeLon);
  }

  /**
   * Compute whether a rectangle crosses a shape. (touching not allowed) Includes a flag for approximating the
   * relation.
   */
  public static boolean rectCrossesPolyApprox(final double rMinLat, final double rMaxLat,
                                              final double rMinLon, final double rMaxLon,
                                              final double[] shapeLat, final double[] shapeLon,
                                              final double sMinLat, final double sMaxLat,
                                              final double sMinLon, final double sMaxLon) {
    // short-circuit: if the bounding boxes are disjoint then the shape does not cross
    if (rectDisjoint(rMinLat, rMaxLat, rMinLon, rMaxLon, sMinLat, sMaxLat, sMinLon, sMaxLon)) {
      return false;
    }

    final int polyLength = shapeLon.length-1;
    for (short p=0; p<polyLength; ++p) {
      if (lineCrossesRect(shapeLat[p], shapeLon[p], shapeLat[p+1], shapeLon[p+1], rMinLat, rMaxLat, rMinLon, rMaxLon) == true) {
        return true;
      }
    }
    return false;
  }

  /**
   * Accurately compute (within restrictions of cartesian decimal degrees) whether a rectangle crosses a polygon
   */
  private static boolean rectCrossesPoly(final double rMinLat, final double rMaxLat,
                                         final double rMinLon, final double rMaxLon,
                                         final double[] shapeLats, final double[] shapeLons) {
    final double[][] bbox = new double[][] { {rMinLon, rMinLat}, {rMaxLon, rMinLat}, {rMaxLon, rMaxLat}, {rMinLon, rMaxLat}, {rMinLon, rMinLat} };
    final int polyLength = shapeLons.length-1;
    double d, s, t, a1, b1, c1, a2, b2, c2;
    double x00, y00, x01, y01, x10, y10, x11, y11;

    // computes the intersection point between each bbox edge and the polygon edge
    for (short b=0; b<4; ++b) {
      a1 = bbox[b+1][1]-bbox[b][1];
      b1 = bbox[b][0]-bbox[b+1][0];
      c1 = a1*bbox[b+1][0] + b1*bbox[b+1][1];
      for (int p=0; p<polyLength; ++p) {
        a2 = shapeLats[p+1]-shapeLats[p];
        b2 = shapeLons[p]-shapeLons[p+1];
        // compute determinant
        d = a1*b2 - a2*b1;
        if (d != 0) {
          // lines are not parallel, check intersecting points
          c2 = a2*shapeLons[p+1] + b2*shapeLats[p+1];
          s = (1/d)*(b2*c1 - b1*c2);
          t = (1/d)*(a1*c2 - a2*c1);
          x00 = StrictMath.min(bbox[b][0], bbox[b+1][0]) - GeoEncodingUtils.TOLERANCE;
          x01 = StrictMath.max(bbox[b][0], bbox[b+1][0]) + GeoEncodingUtils.TOLERANCE;
          y00 = StrictMath.min(bbox[b][1], bbox[b+1][1]) - GeoEncodingUtils.TOLERANCE;
          y01 = StrictMath.max(bbox[b][1], bbox[b+1][1]) + GeoEncodingUtils.TOLERANCE;
          x10 = StrictMath.min(shapeLons[p], shapeLons[p+1]) - GeoEncodingUtils.TOLERANCE;
          x11 = StrictMath.max(shapeLons[p], shapeLons[p+1]) + GeoEncodingUtils.TOLERANCE;
          y10 = StrictMath.min(shapeLats[p], shapeLats[p+1]) - GeoEncodingUtils.TOLERANCE;
          y11 = StrictMath.max(shapeLats[p], shapeLats[p+1]) + GeoEncodingUtils.TOLERANCE;
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

  private static boolean lineCrossesRect(double aLat1, double aLon1,
                                         double aLat2, double aLon2,
                                         final double rMinLat, final double rMaxLat,
                                         final double rMinLon, final double rMaxLon) {
    // short-circuit: if one point inside rect, other outside
    if (pointInRectPrecise(aLat1, aLon1, rMinLat, rMaxLat, rMinLon, rMaxLon)) {
      if (pointInRectPrecise(aLat2, aLon2, rMinLat, rMaxLat, rMinLon, rMaxLon) == false) {
        return true;
      }
    } else if (pointInRectPrecise(aLat2, aLon2, rMinLat, rMaxLat, rMinLon, rMaxLon)) {
      return true;
    }

    return lineCrossesLine(aLat1, aLon1, aLat2, aLon2, rMinLat, rMinLon, rMaxLat, rMaxLon)
        || lineCrossesLine(aLat1, aLon1, aLat2, aLon2, rMaxLat, rMinLon, rMinLat, rMaxLon);
  }

  private static boolean lineCrossesLine(final double aLat1, final double aLon1, final double aLat2, final double aLon2,
                                         final double bLat1, final double bLon1, final double bLat2, final double bLon2) {
    // determine if three points are ccw (right-hand rule) by computing the determinate
    final double aX2X1d = aLon2 - aLon1;
    final double aY2Y1d = aLat2 - aLat1;
    final double bX2X1d = bLon2 - bLon1;
    final double bY2Y1d = bLat2 - bLat1;

    final double t1B = aX2X1d * (bLat2 - aLat1) - aY2Y1d * (bLon2 - aLon1);
    final double test1 = (aX2X1d * (bLat1 - aLat1) - aY2Y1d * (bLon1 - aLon1)) * t1B;
    final double t2B = bX2X1d * (aLat2 - bLat1) - bY2Y1d * (aLon2 - bLon1);
    final double test2 = (bX2X1d * (aLat1 - bLat1) - bY2Y1d * (aLon1 - bLon1)) * t2B;

    if (test1 < 0 && test2 < 0) {
      return true;
    }

    if (test1 == 0 || test2 == 0) {
      // vertically collinear
      if (aLon1 == aLon2 || bLon1 == bLon2) {
        final double minAy = Math.min(aLat1, aLat2);
        final double maxAy = Math.max(aLat1, aLat2);
        final double minBy = Math.min(bLat1, bLat2);
        final double maxBy = Math.max(bLat1, bLat2);

        return !(minBy >= maxAy || maxBy <= minAy);
      }
      // horizontally collinear
      final double minAx = Math.min(aLon1, aLon2);
      final double maxAx = Math.max(aLon1, aLon2);
      final double minBx = Math.min(bLon1, bLon2);
      final double maxBx = Math.max(bLon1, bLon2);

      return !(minBx >= maxAx || maxBx <= minAx);
    }
    return false;
  }

  /**
   * Computes whether a rectangle is within a polygon (shared boundaries not allowed) with more rigor than the
   * {@link GeoRelationUtils#rectWithinPolyApprox} counterpart
   */
  public static boolean rectWithinPolyPrecise(final double rMinLat, final double rMaxLat, final double rMinLon, final double rMaxLon,
                                              final double[] shapeLats, final double[] shapeLons, final double sMinLat,
                                              final double sMaxLat, final double sMinLon, final double sMaxLon) {
    // check if rectangle crosses poly (to handle concave/pacman polys), then check that all 4 corners
    // are contained
    return !(rectCrossesPolyPrecise(rMinLat, rMaxLat, rMinLon, rMaxLon, shapeLats, shapeLons, sMinLat, sMaxLat, sMinLon, sMaxLon) ||
        !pointInPolygon(shapeLats, shapeLons, rMinLat, rMinLon) || !pointInPolygon(shapeLats, shapeLons, rMinLat, rMaxLon) ||
        !pointInPolygon(shapeLats, shapeLons, rMaxLat, rMaxLon) || !pointInPolygon(shapeLats, shapeLons, rMaxLat, rMinLon));
  }

  /**
   * Computes whether a rectangle is within a given polygon (shared boundaries allowed)
   */
  public static boolean rectWithinPolyApprox(final double rMinLat, final double rMaxLat, final double rMinLon, final double rMaxLon,
                                             final double[] shapeLats, final double[] shapeLons, final double sMinLat,
                                             final double sMaxLat, final double sMinLon, final double sMaxLon) {
    // approximation: check if rectangle crosses poly (to handle concave/pacman polys), then check one of the corners
    // are contained

    // short-cut: if bounding boxes cross, rect is not within
    if (rectCrosses(rMinLat, rMaxLat, rMinLon, rMaxLon, sMinLat, sMaxLat, sMinLon, sMaxLon) == true) {
       return false;
     }

     return !(rectCrossesPolyApprox(rMinLat, rMaxLat, rMinLon, rMaxLon, shapeLats, shapeLons, sMinLat, sMaxLat, sMinLon, sMaxLon)
         || !pointInPolygon(shapeLats, shapeLons, rMinLat, rMinLon));
  }
}
