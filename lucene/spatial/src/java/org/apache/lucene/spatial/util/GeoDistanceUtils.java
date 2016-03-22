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

/**
 * Reusable geo-spatial distance utility methods.
 *
 * @lucene.experimental
 */
public class GeoDistanceUtils {

  // No instance:
  private GeoDistanceUtils() {
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
    final double diameter = 2 * GeoUtils.SEMIMAJOR_AXIS;

    // compute inverse haversine
    double a = StrictMath.sin(distance/diameter);
    double h = StrictMath.min(1, a);
    h *= h;
    double cLat = StrictMath.cos(lat);

    return StrictMath.toDegrees(StrictMath.acos(1-((2d*h)/(cLat*cLat))));
  }

  /** Returns the maximum distance/radius (in meters) from the point 'center' before overlapping */
  public static double maxRadialDistanceMeters(final double centerLat, final double centerLon) {
    if (Math.abs(centerLat) == GeoUtils.MAX_LAT_INCL) {
      return SloppyMath.haversinMeters(centerLat, centerLon, 0, centerLon);
    }
    return SloppyMath.haversinMeters(centerLat, centerLon, centerLat, (GeoUtils.MAX_LON_INCL + centerLon) % 360);
  }
}
