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

import org.apache.lucene.util.SloppyMath;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.PI;

import static org.apache.lucene.util.SloppyMath.asin;
import static org.apache.lucene.util.SloppyMath.cos;
import static org.apache.lucene.util.SloppyMath.TO_DEGREES;
import static org.apache.lucene.util.SloppyMath.TO_RADIANS;
import static org.apache.lucene.spatial.util.GeoEncodingUtils.TOLERANCE;
import static org.apache.lucene.spatial.util.GeoProjectionUtils.MAX_LAT_RADIANS;
import static org.apache.lucene.spatial.util.GeoProjectionUtils.MAX_LON_RADIANS;
import static org.apache.lucene.spatial.util.GeoProjectionUtils.MIN_LAT_RADIANS;
import static org.apache.lucene.spatial.util.GeoProjectionUtils.MIN_LON_RADIANS;
import static org.apache.lucene.spatial.util.GeoProjectionUtils.SEMIMAJOR_AXIS;

/**
 * Basic reusable geo-spatial utility methods
 *
 * @lucene.experimental
 */
public final class GeoUtils {
  /** Minimum longitude value. */
  public static final double MIN_LON_INCL = -180.0D;

  /** Maximum longitude value. */
  public static final double MAX_LON_INCL = 180.0D;

  /** Minimum latitude value. */
  public static final double MIN_LAT_INCL = -90.0D;

  /** Maximum latitude value. */
  public static final double MAX_LAT_INCL = 90.0D;

  // WGS84 earth-ellipsoid parameters
  /** mean earth axis in meters */
  // see http://earth-info.nga.mil/GandG/publications/tr8350.2/wgs84fin.pdf
  public static final double EARTH_MEAN_RADIUS_METERS = 6_371_008.7714;

  // No instance:
  private GeoUtils() {
  }

  /** validates latitude value is within standard +/-90 coordinate bounds */
  public static void checkLatitude(double latitude) {
    if (Double.isNaN(latitude) || latitude < MIN_LAT_INCL || latitude > MAX_LAT_INCL) {
      throw new IllegalArgumentException("invalid latitude " +  latitude + "; must be between " + MIN_LAT_INCL + " and " + MAX_LAT_INCL);
    }
  }

  /** validates longitude value is within standard +/-180 coordinate bounds */
  public static void checkLongitude(double longitude) {
    if (Double.isNaN(longitude) || longitude < MIN_LON_INCL || longitude > MAX_LON_INCL) {
      throw new IllegalArgumentException("invalid longitude " +  longitude + "; must be between " + MIN_LON_INCL + " and " + MAX_LON_INCL);
    }
  }

  /** Compute Bounding Box for a circle using WGS-84 parameters */
  public static GeoRect circleToBBox(final double centerLat, final double centerLon, final double radiusMeters) {
    final double radLat = TO_RADIANS * centerLat;
    final double radLon = TO_RADIANS * centerLon;
    double radDistance = radiusMeters / EARTH_MEAN_RADIUS_METERS;
    double minLat = radLat - radDistance;
    double maxLat = radLat + radDistance;
    double minLon;
    double maxLon;

    if (minLat > MIN_LAT_RADIANS && maxLat < MAX_LAT_RADIANS) {
      double deltaLon = asin(sloppySin(radDistance) / cos(radLat));
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

    return new GeoRect(TO_DEGREES * minLat, TO_DEGREES * maxLat, TO_DEGREES * minLon, TO_DEGREES * maxLon);
  }

  /** Compute Bounding Box for a polygon using WGS-84 parameters */
  public static GeoRect polyToBBox(double[] polyLats, double[] polyLons) {
    if (polyLats.length != polyLons.length) {
      throw new IllegalArgumentException("polyLats and polyLons must be equal length");
    }

    double minLon = Double.POSITIVE_INFINITY;
    double maxLon = Double.NEGATIVE_INFINITY;
    double minLat = Double.POSITIVE_INFINITY;
    double maxLat = Double.NEGATIVE_INFINITY;

    for (int i=0;i<polyLats.length;i++) {
      checkLatitude(polyLats[i]);
      checkLongitude(polyLons[i]);
      minLat = min(polyLats[i], minLat);
      maxLat = max(polyLats[i], maxLat);
      minLon = min(polyLons[i], minLon);
      maxLon = max(polyLons[i], maxLon);
    }
    // expand bounding box by TOLERANCE factor to handle round-off error
    return new GeoRect(max(minLat - TOLERANCE, MIN_LAT_INCL), min(maxLat + TOLERANCE, MAX_LAT_INCL),
                       max(minLon - TOLERANCE, MIN_LON_INCL), min(maxLon + TOLERANCE, MAX_LON_INCL));
  }
  
  // some sloppyish stuff, do we really need this to be done in a sloppy way?
  // unless it is performance sensitive, we should try to remove.
  static final double PIO2 = Math.PI / 2D;

  /**
   * Returns the trigonometric sine of an angle converted as a cos operation.
   * <p>
   * Note that this is not quite right... e.g. sin(0) != 0
   * <p>
   * Special cases:
   * <ul>
   *  <li>If the argument is {@code NaN} or an infinity, then the result is {@code NaN}.
   * </ul>
   * @param a an angle, in radians.
   * @return the sine of the argument.
   * @see Math#sin(double)
   */
  // TODO: deprecate/remove this? at least its no longer public.
  static double sloppySin(double a) {
    return cos(a - PIO2);
  }
  

  /**
   * Returns the trigonometric tangent of an angle converted in terms of a sin/cos operation.
   * <p>
   * Note that this is probably not quite right (untested)
   * <p>
   * Special cases:
   * <ul>
   *  <li>If the argument is {@code NaN} or an infinity, then the result is {@code NaN}.
   * </ul>
   * @param a an angle, in radians.
   * @return the tangent of the argument.
   * @see Math#sin(double) aand Math#cos(double)
   */
  // TODO: deprecate/remove this? at least its no longer public.
  static double sloppyTan(double a) {
    return sloppySin(a) / cos(a);
  }
}
