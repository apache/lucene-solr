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

import org.apache.lucene.util.SloppyMath;

import static org.apache.lucene.util.SloppyMath.TO_RADIANS;

/**
 * Reusable geo-spatial distance utility methods.
 *
 * @lucene.experimental
 */
public class GeoDistanceUtils {
  /** error threshold for point-distance queries (in percent) NOTE: Guideline from USGS is 0.005 **/
  public static final double DISTANCE_PCT_ERR = 0.005;

  // No instance:
  private GeoDistanceUtils() {
  }

  /**
   * Compute the great-circle distance using original haversine implementation published by Sinnot in:
   * R.W. Sinnott, "Virtues of the Haversine", Sky and Telescope, vol. 68, no. 2, 1984, p. 159
   *
   * NOTE: this differs from {@link org.apache.lucene.util.SloppyMath#haversin} in that it uses the semi-major axis
   * of the earth instead of an approximation based on the average latitude of the two points (which can introduce an
   * additional error up to .337%, or ~67.6 km at the equator)
   */
  public static double haversin(double lat1, double lon1, double lat2, double lon2) {
    double dLat = TO_RADIANS * (lat2 - lat1);
    double dLon = TO_RADIANS * (lon2 - lon1);
    lat1 = TO_RADIANS * (lat1);
    lat2 = TO_RADIANS * (lat2);

    final double sinDLatO2 = SloppyMath.sin(dLat / 2);
    final double sinDLonO2 = SloppyMath.sin(dLon / 2);

    double a = sinDLatO2*sinDLatO2 + sinDLonO2 * sinDLonO2 * SloppyMath.cos(lat1) * SloppyMath.cos(lat2);
    double c = 2 * SloppyMath.asin(Math.sqrt(a));
    return (GeoProjectionUtils.SEMIMAJOR_AXIS * c);
  }

  /**
   * Compute the distance between two geo-points using vincenty distance formula
   * Vincenty uses the oblate spheroid whereas haversine uses unit sphere, this will give roughly
   * 22m better accuracy (in worst case) than haversine
   *
   * @param lonA longitudinal coordinate of point A (in degrees)
   * @param latA latitudinal coordinate of point A (in degrees)
   * @param lonB longitudinal coordinate of point B (in degrees)
   * @param latB latitudinal coordinate of point B (in degrees)
   * @return distance (in meters) between point A and point B
   */
  public static final double vincentyDistance(final double lonA, final double latA, final double lonB, final double latB) {
    final double L = StrictMath.toRadians(lonB - lonA);
    final double oF = 1 - GeoProjectionUtils.FLATTENING;
    final double U1 = StrictMath.atan(oF * StrictMath.tan(StrictMath.toRadians(latA)));
    final double U2 = StrictMath.atan(oF * StrictMath.tan(StrictMath.toRadians(latB)));
    final double sU1 = StrictMath.sin(U1);
    final double cU1 = StrictMath.cos(U1);
    final double sU2 = StrictMath.sin(U2);
    final double cU2 = StrictMath.cos(U2);

    double sigma, sinSigma, cosSigma;
    double sinAlpha, cos2Alpha, cos2SigmaM;
    double lambda = L;
    double lambdaP;
    double iters = 100;
    double sinLambda, cosLambda, c;

    do {
      sinLambda = StrictMath.sin(lambda);
      cosLambda = Math.cos(lambda);
      sinSigma = Math.sqrt((cU2 * sinLambda) * (cU2 * sinLambda) + (cU1 * sU2 - sU1 * cU2 * cosLambda)
          * (cU1 * sU2 - sU1 * cU2 * cosLambda));
      if (sinSigma == 0) {
        return 0;
      }

      cosSigma = sU1 * sU2 + cU1 * cU2 * cosLambda;
      sigma = Math.atan2(sinSigma, cosSigma);
      sinAlpha = cU1 * cU2 * sinLambda / sinSigma;
      cos2Alpha = 1 - sinAlpha * sinAlpha;
      cos2SigmaM = cosSigma - 2 * sU1 * sU2 / cos2Alpha;

      c = GeoProjectionUtils.FLATTENING/16 * cos2Alpha * (4 + GeoProjectionUtils.FLATTENING * (4 - 3 * cos2Alpha));
      lambdaP = lambda;
      lambda = L + (1 - c) * GeoProjectionUtils.FLATTENING * sinAlpha * (sigma + c * sinSigma * (cos2SigmaM + c * cosSigma *
          (-1 + 2 * cos2SigmaM * cos2SigmaM)));
    } while (StrictMath.abs(lambda - lambdaP) > 1E-12 && --iters > 0);

    if (iters == 0) {
      return 0;
    }

    final double uSq = cos2Alpha * (GeoProjectionUtils.SEMIMAJOR_AXIS2 - GeoProjectionUtils.SEMIMINOR_AXIS2) / (GeoProjectionUtils.SEMIMINOR_AXIS2);
    final double A = 1 + uSq / 16384 * (4096 + uSq * (-768 + uSq * (320 - 175 * uSq)));
    final double B = uSq / 1024 * (256 + uSq * (-128 + uSq * (74 - 47 * uSq)));
    final double deltaSigma = B * sinSigma * (cos2SigmaM + B/4 * (cosSigma * (-1 + 2 * cos2SigmaM * cos2SigmaM) - B/6 * cos2SigmaM
        * (-3 + 4 * sinSigma * sinSigma) * (-3 + 4 * cos2SigmaM * cos2SigmaM)));

    return (GeoProjectionUtils.SEMIMINOR_AXIS * A * (sigma - deltaSigma));
  }

  /**
   * Computes distance between two points in a cartesian (x, y, {z - optional}) coordinate system
   */
  public static double linearDistance(double[] pt1, double[] pt2) {
    assert pt1 != null && pt2 != null && pt1.length == pt2.length && pt1.length > 1;
    final double d0 = pt1[0] - pt2[0];
    final double d1 = pt1[1] - pt2[1];
    if (pt1.length == 3) {
      final double d2 = pt1[2] - pt2[2];
      return Math.sqrt(d0*d0 + d1*d1 + d2*d2);
    }
    return Math.sqrt(d0*d0 + d1*d1);
  }

  /**
   * Compute the inverse haversine to determine distance in degrees longitude for provided distance in meters
   * @param lat latitude to compute delta degrees lon
   * @param distance distance in meters to convert to degrees lon
   * @return Sloppy distance in degrees longitude for provided distance in meters
   */
  public static double distanceToDegreesLon(double lat, double distance) {
    distance /= 1000.0;
    // convert latitude to radians
    lat = StrictMath.toRadians(lat);

    // get the diameter at the latitude
    final double diameter = SloppyMath.earthDiameter(StrictMath.toRadians(lat));

    // compute inverse haversine
    double a = StrictMath.sin(distance/diameter);
    double h = StrictMath.min(1, a);
    h *= h;
    double cLat = StrictMath.cos(lat);

    return StrictMath.toDegrees(StrictMath.acos(1-((2d*h)/(cLat*cLat))));
  }

  /**
   *  Finds the closest point within a rectangle (defined by rMinX, rMinY, rMaxX, rMaxY) to the given (lon, lat) point
   *  the result is provided in closestPt.  When the point is outside the rectangle, the closest point is on an edge
   *  or corner of the rectangle; else, the closest point is the point itself.
   */
  public static void closestPointOnBBox(final double rMinX, final double rMinY, final double rMaxX, final double rMaxY,
                                        final double lon, final double lat, double[] closestPt) {
    assert closestPt != null && closestPt.length == 2;

    closestPt[0] = 0;
    closestPt[1] = 0;

    boolean xSet = true;
    boolean ySet = true;

    if (lon > rMaxX) {
      closestPt[0] = rMaxX;
    } else if (lon < rMinX) {
      closestPt[0] = rMinX;
    } else {
      xSet = false;
    }

    if (lat > rMaxY) {
      closestPt[1] = rMaxY;
    } else if (lat < rMinY) {
      closestPt[1] = rMinY;
    } else {
      ySet = false;
    }

    if (closestPt[0] == 0 && xSet == false) {
      closestPt[0] = lon;
    }

    if (closestPt[1] == 0 && ySet == false) {
      closestPt[1] = lat;
    }
  }

  /** Returns the maximum distance/radius (in meters) from the point 'center' before overlapping */
  public static double maxRadialDistanceMeters(final double centerLon, final double centerLat) {
    if (Math.abs(centerLat) == GeoUtils.MAX_LAT_INCL) {
      return GeoDistanceUtils.haversin(centerLat, centerLon, 0, centerLon);
    }
    return GeoDistanceUtils.haversin(centerLat, centerLon, centerLat, (GeoUtils.MAX_LON_INCL + centerLon) % 360);
  }

  /**
   * Compute the inverse haversine to determine distance in degrees longitude for provided distance in meters
   * @param lat latitude to compute delta degrees lon
   * @param distance distance in meters to convert to degrees lon
   * @return Sloppy distance in degrees longitude for provided distance in meters
   */
  public static double distanceToDegreesLat(double lat, double distance) {
    // get the diameter at the latitude
    final double diameter = SloppyMath.earthDiameter(StrictMath.toRadians(lat));
    distance /= 1000.0;

    // compute inverse haversine
    double a = StrictMath.sin(distance/diameter);
    double h = StrictMath.min(1, a);
    h *= h;

    return StrictMath.toDegrees(StrictMath.acos(1-(2d*h)));
  }
}
