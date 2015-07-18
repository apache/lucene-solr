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
 * Reusable geo-spatial distance utility methods.
 *
 * @lucene.experimental
 */
public class GeoDistanceUtils {

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
