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
    // convert latitude to radians
    lat = StrictMath.toRadians(lat);

    // get the diameter at the latitude
    final double diameter = 2 * GeoProjectionUtils.SEMIMAJOR_AXIS;

    // compute inverse haversine
    double a = StrictMath.sin(distance/diameter);
    double h = StrictMath.min(1, a);
    h *= h;
    double cLat = StrictMath.cos(lat);

    return StrictMath.toDegrees(StrictMath.acos(1-((2d*h)/(cLat*cLat))));
  }

  /**
   *  Finds the closest point within a rectangle (defined by rMinX, rMinY, rMaxX, rMaxY) to the given (lon, lat) point
   *  the result is provided in closestPt (lat, lon).  When the point is outside the rectangle, the closest point is on an edge
   *  or corner of the rectangle; else, the closest point is the point itself.
   */
  public static void closestPointOnBBox(final double rMinY, final double rMaxY, final double rMinX, final double rMaxX,
                                        final double lat, final double lon, double[] closestPt) {
    assert closestPt != null && closestPt.length == 2;

    closestPt[0] = 0;
    closestPt[1] = 0;

    boolean xSet = true;
    boolean ySet = true;

    if (lon > rMaxX) {
      closestPt[1] = rMaxX;
    } else if (lon < rMinX) {
      closestPt[1] = rMinX;
    } else {
      xSet = false;
    }

    if (lat > rMaxY) {
      closestPt[0] = rMaxY;
    } else if (lat < rMinY) {
      closestPt[0] = rMinY;
    } else {
      ySet = false;
    }

    if (closestPt[0] == 0 && ySet == false) {
      closestPt[0] = lat;
    }

    if (closestPt[1] == 0 && xSet == false) {
      closestPt[1] = lon;
    }
  }

  /** Returns the maximum distance/radius (in meters) from the point 'center' before overlapping */
  public static double maxRadialDistanceMeters(final double centerLat, final double centerLon) {
    if (Math.abs(centerLat) == GeoUtils.MAX_LAT_INCL) {
      return SloppyMath.haversinMeters(centerLat, centerLon, 0, centerLon);
    }
    return SloppyMath.haversinMeters(centerLat, centerLon, centerLat, (GeoUtils.MAX_LON_INCL + centerLon) % 360);
  }
}
