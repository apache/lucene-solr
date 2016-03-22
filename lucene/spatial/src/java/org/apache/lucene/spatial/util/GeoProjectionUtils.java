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

import static java.lang.StrictMath.sqrt;

import static org.apache.lucene.util.SloppyMath.asin;
import static org.apache.lucene.util.SloppyMath.cos;
import static org.apache.lucene.util.SloppyMath.TO_DEGREES;
import static org.apache.lucene.util.SloppyMath.TO_RADIANS;

import static org.apache.lucene.spatial.util.GeoUtils.MAX_LAT_INCL;
import static org.apache.lucene.spatial.util.GeoUtils.MAX_LON_INCL;
import static org.apache.lucene.spatial.util.GeoUtils.MIN_LAT_INCL;
import static org.apache.lucene.spatial.util.GeoUtils.MIN_LON_INCL;
import static org.apache.lucene.spatial.util.GeoUtils.PIO2;
import static org.apache.lucene.spatial.util.GeoUtils.sloppySin;
import static org.apache.lucene.spatial.util.GeoUtils.sloppyTan;

/**
 * Reusable geo-spatial projection utility methods.
 *
 * @lucene.experimental
 */
public class GeoProjectionUtils {
  // WGS84 earth-ellipsoid parameters
  /** major (a) axis in meters */
  public static final double SEMIMAJOR_AXIS = 6_378_137; // [m]
  /** earth flattening factor (f) */
  public static final double FLATTENING = 1.0/298.257223563;
  /** minor (b) axis in meters */
  public static final double SEMIMINOR_AXIS = SEMIMAJOR_AXIS * (1.0 - FLATTENING); //6_356_752.31420; // [m]
  /** first eccentricity (e) */
  public static final double ECCENTRICITY = sqrt((2.0 - FLATTENING) * FLATTENING);
  /** major axis squared (a2) */
  public static final double SEMIMAJOR_AXIS2 = SEMIMAJOR_AXIS * SEMIMAJOR_AXIS;
  /** minor axis squared (b2) */
  public static final double SEMIMINOR_AXIS2 = SEMIMINOR_AXIS * SEMIMINOR_AXIS;
  private static final double E2 = (SEMIMAJOR_AXIS2 - SEMIMINOR_AXIS2)/(SEMIMAJOR_AXIS2);
  private static final double EP2 = (SEMIMAJOR_AXIS2 - SEMIMINOR_AXIS2)/(SEMIMINOR_AXIS2);

  /** min longitude value in radians */
  public static final double MIN_LON_RADIANS = TO_RADIANS * MIN_LON_INCL;
  /** min latitude value in radians */
  public static final double MIN_LAT_RADIANS = TO_RADIANS * MIN_LAT_INCL;
  /** max longitude value in radians */
  public static final double MAX_LON_RADIANS = TO_RADIANS * MAX_LON_INCL;
  /** max latitude value in radians */
  public static final double MAX_LAT_RADIANS = TO_RADIANS * MAX_LAT_INCL;

  // No instance:
  private GeoProjectionUtils() {
  }

  /**
   * Converts from geodesic lat lon alt to geocentric earth-centered earth-fixed
   * @param lat geodesic latitude
   * @param lon geodesic longitude
   * @param alt geodesic altitude
   * @param ecf reusable earth-centered earth-fixed result
   * @return either a new ecef array or the reusable ecf parameter
   */
  public static final double[] llaToECF(double lat, double lon, double alt, double[] ecf) {
    lon = TO_RADIANS * lon;
    lat = TO_RADIANS * lat;

    final double sl = sloppySin(lat);
    final double s2 = sl*sl;
    final double cl = cos(lat);

    if (ecf == null) {
      ecf = new double[3];
    }

    if (lat < -PIO2 && lat > -1.001D * PIO2) {
      lat = -PIO2;
    } else if (lat > PIO2 && lat < 1.001D * PIO2) {
      lat = PIO2;
    }
    assert (lat >= -PIO2) || (lat <= PIO2);

    if (lon > StrictMath.PI) {
      lon -= (2*StrictMath.PI);
    }

    final double rn = SEMIMAJOR_AXIS / StrictMath.sqrt(1.0D - E2 * s2);
    ecf[0] = (rn+alt) * cl * cos(lon);
    ecf[1] = (rn+alt) * cl * sloppySin(lon);
    ecf[2] = ((rn*(1.0-E2))+alt)*sl;

    return ecf;
  }

  /**
   * Finds a point along a bearing from a given lat,lon geolocation using great circle arc
   *
   * @param lat origin latitude in degrees
   * @param lon origin longitude in degrees
   * @param bearing azimuthal bearing in degrees
   * @param dist distance in meters
   * @param pt resulting point
   * @return the point along a bearing at a given distance in meters
   */
  public static final double[] pointFromLonLatBearingGreatCircle(double lat, double lon, double bearing, double dist, double[] pt) {

    if (pt == null) {
      pt = new double[2];
    }

    lat *= TO_RADIANS;
    lon *= TO_RADIANS;
    bearing *= TO_RADIANS;

    final double cLat = cos(lat);
    final double sLat = sloppySin(lat);
    final double sinDoR = sloppySin(dist / GeoProjectionUtils.SEMIMAJOR_AXIS);
    final double cosDoR = cos(dist / GeoProjectionUtils.SEMIMAJOR_AXIS);

    pt[1] = asin(sLat*cosDoR + cLat * sinDoR * cos(bearing));
    pt[0] = TO_DEGREES * (lon + Math.atan2(sloppySin(bearing) * sinDoR * cLat, cosDoR - sLat * sloppySin(pt[1])));
    pt[1] *= TO_DEGREES;

    return pt;
  }

  /**
   * Finds the bearing (in degrees) between 2 geo points (lat, lon) using great circle arc
   * @param lat1 first point latitude in degrees
   * @param lon1 first point longitude in degrees
   * @param lat2 second point latitude in degrees
   * @param lon2 second point longitude in degrees
   * @return the bearing (in degrees) between the two provided points
   */
  public static double bearingGreatCircle(double lat1, double lon1, double lat2, double lon2) {
    double dLon = (lon2 - lon1) * TO_RADIANS;
    lat2 *= TO_RADIANS;
    lat1 *= TO_RADIANS;
    double y = sloppySin(dLon) * cos(lat2);
    double x = cos(lat1) * sloppySin(lat2) - sloppySin(lat1) * cos(lat2) * cos(dLon);
    return Math.atan2(y, x) * TO_DEGREES;
  }
}
