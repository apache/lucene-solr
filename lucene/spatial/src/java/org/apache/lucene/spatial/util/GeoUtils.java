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

import java.util.ArrayList;

import org.apache.lucene.util.BitUtil;

import static java.lang.StrictMath.max;
import static java.lang.StrictMath.min;
import static java.lang.StrictMath.PI;

import static org.apache.lucene.util.SloppyMath.asin;
import static org.apache.lucene.util.SloppyMath.cos;
import static org.apache.lucene.util.SloppyMath.sin;
import static org.apache.lucene.util.SloppyMath.TO_DEGREES;
import static org.apache.lucene.util.SloppyMath.TO_RADIANS;
import static org.apache.lucene.spatial.util.GeoProjectionUtils.MAX_LAT_RADIANS;
import static org.apache.lucene.spatial.util.GeoProjectionUtils.MAX_LON_RADIANS;
import static org.apache.lucene.spatial.util.GeoProjectionUtils.MIN_LAT_RADIANS;
import static org.apache.lucene.spatial.util.GeoProjectionUtils.MIN_LON_RADIANS;
import static org.apache.lucene.spatial.util.GeoProjectionUtils.pointFromLonLatBearingGreatCircle;
import static org.apache.lucene.spatial.util.GeoProjectionUtils.SEMIMAJOR_AXIS;

/**
 * Basic reusable geo-spatial utility methods
 *
 * @lucene.experimental
 */
public final class GeoUtils {
  /** number of bits used for quantizing latitude and longitude values */
  public static final short BITS = 31;
  private static final double LON_SCALE = (0x1L<<BITS)/360.0D;
  private static final double LAT_SCALE = (0x1L<<BITS)/180.0D;
  /** rounding error for quantized latitude and longitude values */
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

  /**
   * encode longitude, latitude geopoint values using morton encoding method
   * https://en.wikipedia.org/wiki/Z-order_curve
   */
  public static final Long mortonHash(final double lon, final double lat) {
    return BitUtil.interleave(scaleLon(lon), scaleLat(lat));
  }

  /** decode longitude value from morton encoded geo point */
  public static final double mortonUnhashLon(final long hash) {
    return unscaleLon(BitUtil.deinterleave(hash));
  }

  /** decode latitude value from morton encoded geo point */
  public static final double mortonUnhashLat(final long hash) {
    return unscaleLat(BitUtil.deinterleave(hash >>> 1));
  }

  private static final long scaleLon(final double val) {
    return (long) ((val-MIN_LON_INCL) * LON_SCALE);
  }

  private static final long scaleLat(final double val) {
    return (long) ((val-MIN_LAT_INCL) * LAT_SCALE);
  }

  private static final double unscaleLon(final long val) {
    return (val / LON_SCALE) + MIN_LON_INCL;
  }

  private static final double unscaleLat(final long val) {
    return (val / LAT_SCALE) + MIN_LAT_INCL;
  }

  /** Compare two position values within a {@link GeoUtils#TOLERANCE} factor */
  public static double compare(final double v1, final double v2) {
    final double delta = v1-v2;
    return Math.abs(delta) <= TOLERANCE ? 0 : delta;
  }

  /** Puts longitude in range of -180 to +180. */
  public static double normalizeLon(double lon_deg) {
    if (lon_deg >= -180 && lon_deg <= 180) {
      return lon_deg; //common case, and avoids slight double precision shifting
    }
    double off = (lon_deg + 180) % 360;
    if (off < 0) {
      return 180 + off;
    } else if (off == 0 && lon_deg > 0) {
      return 180;
    } else {
      return -180 + off;
    }
  }

  /** Puts latitude in range of -90 to 90. */
  public static double normalizeLat(double lat_deg) {
    if (lat_deg >= -90 && lat_deg <= 90) {
      return lat_deg; //common case, and avoids slight double precision shifting
    }
    double off = Math.abs((lat_deg + 90) % 360);
    return (off <= 180 ? off : 360-off) - 90;
  }

  /** Converts long value to bit string (useful for debugging) */
  public static String geoTermToString(long term) {
    StringBuilder s = new StringBuilder(64);
    final int numberOfLeadingZeros = Long.numberOfLeadingZeros(term);
    for (int i = 0; i < numberOfLeadingZeros; i++) {
      s.append('0');
    }
    if (term != 0) {
      s.append(Long.toBinaryString(term));
    }
    return s.toString();
  }

  /**
   * Converts a given circle (defined as a point/radius) to an approximated line-segment polygon
   *
   * @param lon longitudinal center of circle (in degrees)
   * @param lat latitudinal center of circle (in degrees)
   * @param radiusMeters distance radius of circle (in meters)
   * @return a list of lon/lat points representing the circle
   */
  @SuppressWarnings({"unchecked","rawtypes"})
  public static ArrayList<double[]> circleToPoly(final double lon, final double lat, final double radiusMeters) {
    double angle;
    // a little under-sampling (to limit the number of polygonal points): using archimedes estimation of pi
    final int sides = 25;
    ArrayList<double[]> geometry = new ArrayList();
    double[] lons = new double[sides];
    double[] lats = new double[sides];

    double[] pt = new double[2];
    final int sidesLen = sides-1;
    for (int i=0; i<sidesLen; ++i) {
      angle = (i*360/sides);
      pt = pointFromLonLatBearingGreatCircle(lon, lat, angle, radiusMeters, pt);
      lons[i] = pt[0];
      lats[i] = pt[1];
    }
    // close the poly
    lons[sidesLen] = lons[0];
    lats[sidesLen] = lats[0];
    geometry.add(lons);
    geometry.add(lats);

    return geometry;
  }

  /** Compute Bounding Box for a circle using WGS-84 parameters */
  public static GeoBoundingBox circleToBBox(final double centerLon, final double centerLat, final double radiusMeters) {
    final double radLat = TO_RADIANS * centerLat;
    final double radLon = TO_RADIANS * centerLon;
    double radDistance = radiusMeters / SEMIMAJOR_AXIS;
    double minLat = radLat - radDistance;
    double maxLat = radLat + radDistance;
    double minLon;
    double maxLon;

    if (minLat > MIN_LAT_RADIANS && maxLat < MAX_LAT_RADIANS) {
      double deltaLon = asin(sin(radDistance) / cos(radLat));
      minLon = radLon - deltaLon;
      if (minLon < MIN_LON_RADIANS) {
        minLon += 2d * PI;
      }
      maxLon = radLon + deltaLon;
      if (maxLon > MAX_LON_RADIANS) {
        maxLon -= 2d * PI;
      }
    } else {
      // a pole is within the distance
      minLat = max(minLat, MIN_LAT_RADIANS);
      maxLat = min(maxLat, MAX_LAT_RADIANS);
      minLon = MIN_LON_RADIANS;
      maxLon = MAX_LON_RADIANS;
    }

    return new GeoBoundingBox(TO_DEGREES * minLon, TO_DEGREES * maxLon, TO_DEGREES * minLat, TO_DEGREES * maxLat);
  }

  /** Compute Bounding Box for a polygon using WGS-84 parameters */
  public static GeoBoundingBox polyToBBox(double[] polyLons, double[] polyLats) {
    if (polyLons.length != polyLats.length) {
      throw new IllegalArgumentException("polyLons and polyLats must be equal length");
    }

    double minLon = Double.POSITIVE_INFINITY;
    double maxLon = Double.NEGATIVE_INFINITY;
    double minLat = Double.POSITIVE_INFINITY;
    double maxLat = Double.NEGATIVE_INFINITY;

    for (int i=0;i<polyLats.length;i++) {
      if (GeoUtils.isValidLon(polyLons[i]) == false) {
        throw new IllegalArgumentException("invalid polyLons[" + i + "]=" + polyLons[i]);
      }
      if (GeoUtils.isValidLat(polyLats[i]) == false) {
        throw new IllegalArgumentException("invalid polyLats[" + i + "]=" + polyLats[i]);
      }
      minLon = min(polyLons[i], minLon);
      maxLon = max(polyLons[i], maxLon);
      minLat = min(polyLats[i], minLat);
      maxLat = max(polyLats[i], maxLat);
    }
    // expand bounding box by TOLERANCE factor to handle round-off error
    return new GeoBoundingBox(max(minLon - TOLERANCE, MIN_LON_INCL), min(maxLon + TOLERANCE, MAX_LON_INCL),
        max(minLat - TOLERANCE, MIN_LAT_INCL), min(maxLat + TOLERANCE, MAX_LAT_INCL));
  }

  /** validates latitude value is within standard +/-90 coordinate bounds */
  public static boolean isValidLat(double lat) {
    return Double.isNaN(lat) == false && lat >= MIN_LAT_INCL && lat <= MAX_LAT_INCL;
  }

  /** validates longitude value is within standard +/-180 coordinate bounds */
  public static boolean isValidLon(double lon) {
    return Double.isNaN(lon) == false && lon >= MIN_LON_INCL && lon <= MAX_LON_INCL;
  }
}
