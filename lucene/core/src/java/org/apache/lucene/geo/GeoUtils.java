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

package org.apache.lucene.geo;

import static org.apache.lucene.util.SloppyMath.TO_RADIANS;
import static org.apache.lucene.util.SloppyMath.cos;
import static org.apache.lucene.util.SloppyMath.haversinMeters;

import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.util.SloppyMath;

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

  /** min longitude value in radians */
  public static final double MIN_LON_RADIANS = TO_RADIANS * MIN_LON_INCL;
  /** min latitude value in radians */
  public static final double MIN_LAT_RADIANS = TO_RADIANS * MIN_LAT_INCL;
  /** max longitude value in radians */
  public static final double MAX_LON_RADIANS = TO_RADIANS * MAX_LON_INCL;
  /** max latitude value in radians */
  public static final double MAX_LAT_RADIANS = TO_RADIANS * MAX_LAT_INCL;

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

  // some sloppyish stuff, do we really need this to be done in a sloppy way?
  // unless it is performance sensitive, we should try to remove.
  private static final double PIO2 = Math.PI / 2D;

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
  public static double sloppySin(double a) {
    return cos(a - PIO2);
  }

  /**
   * binary search to find the exact sortKey needed to match the specified radius
   * any sort key lte this is a query match.
   */
  public static double distanceQuerySortKey(double radius) {
    // effectively infinite
    if (radius >= haversinMeters(Double.MAX_VALUE)) {
      return haversinMeters(Double.MAX_VALUE);
    }

    // this is a search through non-negative long space only
    long lo = 0;
    long hi = Double.doubleToRawLongBits(Double.MAX_VALUE);
    while (lo <= hi) {
      long mid = (lo + hi) >>> 1;
      double sortKey = Double.longBitsToDouble(mid);
      double midRadius = haversinMeters(sortKey);
      if (midRadius == radius) {
        return sortKey;
      } else if (midRadius > radius) {
        hi = mid - 1;
      } else {
        lo = mid + 1;
      }
    }

    // not found: this is because a user can supply an arbitrary radius, one that we will never
    // calculate exactly via our haversin method.
    double ceil = Double.longBitsToDouble(lo);
    assert haversinMeters(ceil) > radius;
    return ceil;
  }

  /**
   * Compute the relation between the provided box and distance query.
   * This only works for boxes that do not cross the dateline.
   */
  public static PointValues.Relation relate(
      double minLat, double maxLat, double minLon, double maxLon,
      double lat, double lon, double distanceSortKey, double axisLat) {

    if (minLon > maxLon) {
      throw new IllegalArgumentException("Box crosses the dateline");
    }

    if ((lon < minLon || lon > maxLon) && (axisLat + Rectangle.AXISLAT_ERROR < minLat || axisLat - Rectangle.AXISLAT_ERROR > maxLat)) {
      // circle not fully inside / crossing axis
      if (SloppyMath.haversinSortKey(lat, lon, minLat, minLon) > distanceSortKey &&
          SloppyMath.haversinSortKey(lat, lon, minLat, maxLon) > distanceSortKey &&
          SloppyMath.haversinSortKey(lat, lon, maxLat, minLon) > distanceSortKey &&
          SloppyMath.haversinSortKey(lat, lon, maxLat, maxLon) > distanceSortKey) {
        // no points inside
        return Relation.CELL_OUTSIDE_QUERY;
      }
    }

    if (within90LonDegrees(lon, minLon, maxLon) &&
        SloppyMath.haversinSortKey(lat, lon, minLat, minLon) <= distanceSortKey &&
        SloppyMath.haversinSortKey(lat, lon, minLat, maxLon) <= distanceSortKey &&
        SloppyMath.haversinSortKey(lat, lon, maxLat, minLon) <= distanceSortKey &&
        SloppyMath.haversinSortKey(lat, lon, maxLat, maxLon) <= distanceSortKey) {
      // we are fully enclosed, collect everything within this subtree
      return Relation.CELL_INSIDE_QUERY;
    }

    return Relation.CELL_CROSSES_QUERY;
  }

  /** Return whether all points of {@code [minLon,maxLon]} are within 90 degrees of {@code lon}. */
  static boolean within90LonDegrees(double lon, double minLon, double maxLon) {
    if (maxLon <= lon - 180) {
      lon -= 360;
    } else if (minLon >= lon + 180) {
      lon += 360;
    }
    return maxLon - lon < 90 && lon - minLon < 90;
  }

}
