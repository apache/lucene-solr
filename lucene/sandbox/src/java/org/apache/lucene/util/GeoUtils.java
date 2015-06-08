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
 * Basic reusable geo-spatial utility methods
 *
 * @lucene.experimental
 */
public final class GeoUtils {
  // WGS84 earth-ellipsoid major (a) minor (b) radius, (f) flattening and eccentricity (e)
  private static final double SEMIMAJOR_AXIS = 6_378_137; // [m]
  private static final double FLATTENING = 1.0/298.257223563;
  private static final double SEMIMINOR_AXIS = SEMIMAJOR_AXIS * (1.0 - FLATTENING); //6_356_752.31420; // [m]
  private static final double ECCENTRICITY = StrictMath.sqrt((2.0 - FLATTENING) * FLATTENING);
  private static final double PI_OVER_2 = StrictMath.PI / 2.0D;
  private static final double SEMIMAJOR_AXIS2 = SEMIMAJOR_AXIS * SEMIMINOR_AXIS;
  private static final double SEMIMINOR_AXIS2 = SEMIMINOR_AXIS * SEMIMINOR_AXIS;

  private static final short MIN_LON = -180;
  private static final short MIN_LAT = -90;
  public static final short BITS = 31;
  private static final double LON_SCALE = (0x1L<<BITS)/360.0D;
  private static final double LAT_SCALE = (0x1L<<BITS)/180.0D;
  public static final double TOLERANCE = 1E-6;

  /** Minimum longitude value. */
  public static final double MIN_LON_INCL = -180.0D;

  /** Maximum longitude value. */
  public static final double MAX_LON_INCL = 180.0D;

  /** Minimum latitude value. */
  public static final double MIN_LAT_INCL = -90.0D;

  /** Maximum latitude value. */
  public static final double MAX_LAT_INCL = 90.0D;

  // No instance:
  private GeoUtils() {
  }

  public static final Long mortonHash(final double lon, final double lat) {
    return BitUtil.interleave(scaleLon(lon), scaleLat(lat));
  }

  public static final double mortonUnhashLon(final long hash) {
    return unscaleLon(BitUtil.deinterleave(hash));
  }

  public static final double mortonUnhashLat(final long hash) {
    return unscaleLat(BitUtil.deinterleave(hash >>> 1));
  }

  private static long scaleLon(final double val) {
    return (long) ((val-MIN_LON) * LON_SCALE);
  }

  private static long scaleLat(final double val) {
    return (long) ((val-MIN_LAT) * LAT_SCALE);
  }

  private static double unscaleLon(final long val) {
    return (val / LON_SCALE) + MIN_LON;
  }

  private static double unscaleLat(final long val) {
    return (val / LAT_SCALE) + MIN_LAT;
  }

  public static final double compare(final double v1, final double v2) {
    final double compare = v1-v2;
    return Math.abs(compare) <= TOLERANCE ? 0 : compare;
  }

  public static final boolean bboxContains(final double lon, final double lat, final double minLon,
                                           final double minLat, final double maxLon, final double maxLat) {
    return (compare(lon, minLon) >= 0 && compare(lon, maxLon) <= 0
          && compare(lat, minLat) >= 0 && compare(lat, maxLat) <= 0);
  }

  /**
   * Converts from geodesic lon lat alt to geocentric earth-centered earth-fixed
   * @param lon geodesic longitude
   * @param lat geodesic latitude
   * @param alt geodesic altitude
   * @param ecf reusable earth-centered earth-fixed result
   * @return either a new ecef array or the reusable ecf parameter
   */
  public static final double[] llaToECF(double lon, double lat, double alt, double[] ecf) {
    lon = StrictMath.toRadians(lon);
    lat = StrictMath.toRadians(lat);

    final double sl = StrictMath.sin(lat);
    final double s2 = sl*sl;
    final double cl = StrictMath.cos(lat);
    final double ge2 = (SEMIMAJOR_AXIS2 - SEMIMINOR_AXIS2)/(SEMIMAJOR_AXIS2);

    if (ecf == null)
      ecf = new double[3];

    if (lat < -PI_OVER_2 && lat > -1.001D * PI_OVER_2) {
      lat = -PI_OVER_2;
    } else if (lat > PI_OVER_2 && lat < 1.001D * PI_OVER_2) {
      lat = PI_OVER_2;
    }
    assert ((lat >= -PI_OVER_2) || (lat <= PI_OVER_2));

    if (lon > StrictMath.PI) {
      lon -= (2*StrictMath.PI);
    }

    final double rn = SEMIMAJOR_AXIS / StrictMath.sqrt(1.0D - ge2 * s2);
    ecf[0] = (rn+alt) * cl * StrictMath.cos(lon);
    ecf[1] = (rn+alt) * cl * StrictMath.sin(lon);
    ecf[2] = ((rn*(1.0-ge2))+alt)*sl;

    return ecf;
  }

  /**
   * Converts from geocentric earth-centered earth-fixed to geodesic lat/lon/alt
   * @param x Cartesian x coordinate
   * @param y Cartesian y coordinate
   * @param z Cartesian z coordinate
   * @param lla 0: longitude 1: latitude: 2: altitude
   * @return double array as 0: longitude 1: latitude 2: altitude
   */
  public static final double[] ecfToLLA(final double x, final double y, final double z, double[] lla) {
    boolean atPole = false;
    final double ad_c = 1.0026000D;
    final double e2 = (SEMIMAJOR_AXIS2 - SEMIMINOR_AXIS2)/(SEMIMAJOR_AXIS2);
    final double ep2 = (SEMIMAJOR_AXIS2 - SEMIMINOR_AXIS2)/(SEMIMINOR_AXIS2);
    final double cos67P5 = 0.38268343236508977D;

    if (lla == null)
      lla = new double[3];

    if (x != 0.0) {
      lla[0] = StrictMath.atan2(y,x);
    } else {
      if (y > 0) {
        lla[0] = PI_OVER_2;
      } else if (y < 0) {
        lla[0] = -PI_OVER_2;
      } else {
        atPole = true;
        lla[0] = 0.0D;
        if (z > 0.0) {
          lla[1] = PI_OVER_2;
        } else if (z < 0.0) {
          lla[1] = -PI_OVER_2;
        } else {
          lla[1] = PI_OVER_2;
          lla[2] = -SEMIMINOR_AXIS;
          return lla;
        }
      }
    }

    final double w2 = x*x + y*y;
    final double w = StrictMath.sqrt(w2);
    final double t0 = z * ad_c;
    final double s0 = StrictMath.sqrt(t0 * t0 + w2);
    final double sinB0 = t0 / s0;
    final double cosB0 = w / s0;
    final double sin3B0 = sinB0 * sinB0 * sinB0;
    final double t1 = z + SEMIMINOR_AXIS * ep2 * sin3B0;
    final double sum = w - SEMIMAJOR_AXIS * e2 * cosB0 * cosB0 * cosB0;
    final double s1 = StrictMath.sqrt(t1 * t1 + sum * sum);
    final double sinP1 = t1 / s1;
    final double cosP1 = sum / s1;
    final double rn = SEMIMAJOR_AXIS / StrictMath.sqrt(1.0D - e2 * sinP1 * sinP1);

    if (cosP1 >= cos67P5) {
      lla[2] = w / cosP1 - rn;
    } else if (cosP1 <= -cos67P5) {
      lla[2] = w / -cosP1 - rn;
    } else {
      lla[2] = z / sinP1 + rn * (e2 - 1.0);
    }
    if (!atPole) {
      lla[1] = StrictMath.atan(sinP1/cosP1);
    }
    lla[0] = StrictMath.toDegrees(lla[0]);
    lla[1] = StrictMath.toDegrees(lla[1]);

    return lla;
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

  public static String geoTermToString(long term) {
    StringBuilder s = new StringBuilder(64);
    final int numberOfLeadingZeros = Long.numberOfLeadingZeros(term);
    for (int i = 0; i < numberOfLeadingZeros; i++) {
      s.append('0');
    }
    if (term != 0)
      s.append(Long.toBinaryString(term));
    return s.toString();
  }


  public static boolean rectDisjoint(final double aMinX, final double aMinY, final double aMaxX, final double aMaxY,
                                     final double bMinX, final double bMinY, final double bMaxX, final double bMaxY) {
    return (aMaxX < bMinX || aMinX > bMaxX || aMaxY < bMinY || aMinY > bMaxY);
  }

  /**
   * Computes whether a rectangle is wholly within another rectangle (shared boundaries allowed)
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
  public static final boolean rectCrossesPoly(final double rMinX, final double rMinY, final double rMaxX,
                                              final double rMaxY, final double[] shapeX, final double[] shapeY,
                                              final double sMinX, final double sMinY, final double sMaxX,
                                              final double sMaxY) {
    // short-circuit: if the bounding boxes are disjoint then the shape does not cross
    if (rectDisjoint(rMinX, rMinY, rMaxX, rMaxY, sMinX, sMinY, sMaxX, sMaxY))
      return false;

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
          x00 = StrictMath.min(bbox[b][0], bbox[b+1][0]) - TOLERANCE;
          x01 = StrictMath.max(bbox[b][0], bbox[b+1][0]) + TOLERANCE;
          y00 = StrictMath.min(bbox[b][1], bbox[b+1][1]) - TOLERANCE;
          y01 = StrictMath.max(bbox[b][1], bbox[b+1][1]) + TOLERANCE;
          x10 = StrictMath.min(shapeX[p], shapeX[p+1]) - TOLERANCE;
          x11 = StrictMath.max(shapeX[p], shapeX[p+1]) + TOLERANCE;
          y10 = StrictMath.min(shapeY[p], shapeY[p+1]) - TOLERANCE;
          y11 = StrictMath.max(shapeY[p], shapeY[p+1]) + TOLERANCE;
          // check whether the intersection point is touching one of the line segments
          boolean touching = ((x00 == s && y00 == t) || (x01 == s && y01 == t))
              || ((x10 == s && y10 == t) || (x11 == s && y11 == t));
          // if line segments are not touching and the intersection point is within the range of either segment
          if (!(touching || x00 > s || x01 < s || y00 > t || y01 < t || x10 > s || x11 < s || y10 > t || y11 < t))
            return true;
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

  public static boolean isValidLat(double lat) {
    return Double.isNaN(lat) == false && lat >= MIN_LAT_INCL && lat <= MAX_LAT_INCL;
  }

  public static boolean isValidLon(double lon) {
    return Double.isNaN(lon) == false && lon >= MIN_LON_INCL && lon <= MAX_LON_INCL;
  }
}
