/**
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

package org.apache.lucene.spatial;

import org.apache.lucene.spatial.geometry.DistanceUnits;
import org.apache.lucene.spatial.geometry.FloatLatLng;
import org.apache.lucene.spatial.geometry.LatLng;
import org.apache.lucene.spatial.geometry.shape.LLRect;
import org.apache.lucene.spatial.geometry.shape.Rectangle;
import org.apache.lucene.spatial.tier.InvalidGeoException;

/**
 * <p><font color="red"><b>NOTE:</b> This API is still in
 * flux and might change in incompatible ways in the next
 * release.</font>
 */

public class DistanceUtils {

  public static final double DEGREES_TO_RADIANS = Math.PI / 180.0;
  public static final double RADIANS_TO_DEGREES = 180.0 / Math.PI;
  public static final double DEG_45 = Math.PI / 4.0;
  public static final double DEG_225 = 5 * DEG_45;
  public static final double DEG_90 = Math.PI / 2;
  public static final double DEG_180 = Math.PI;
  public static final double SIN_45 = Math.sin(DEG_45);
  public static final double KM_TO_MILES = 0.621371192;
  public static final double MILES_TO_KM = 1.609344;
    /**
   * The International Union of Geodesy and Geophysics says the Earth's mean radius in KM is:
   *
   * [1] http://en.wikipedia.org/wiki/Earth_radius
   */
  public static final double EARTH_MEAN_RADIUS_KM = 6371.009;

  public static final double EARTH_MEAN_RADIUS_MI = EARTH_MEAN_RADIUS_KM / MILES_TO_KM;

  public static final double EARTH_EQUATORIAL_RADIUS_MI = 3963.205;
  public static final double EARTH_EQUATORIAL_RADIUS_KM = EARTH_EQUATORIAL_RADIUS_MI * MILES_TO_KM;


  public static double getDistanceMi(double x1, double y1, double x2, double y2) {
    return getLLMDistance(x1, y1, x2, y2);
  }

  /**
   * 
   * @param x1
   * @param y1
   * @param miles
   * @return boundary rectangle where getY/getX is top left, getMinY/getMinX is bottom right
   */
  public static Rectangle getBoundary (double x1, double y1, double miles) {

    LLRect box = LLRect.createBox( new FloatLatLng( x1, y1 ), miles, miles );
    
    //System.out.println("Box: "+maxX+" | "+ maxY+" | "+ minX + " | "+ minY);
    return box.toRectangle();

  }
  
  public static double getLLMDistance (double x1, double y1, double x2, double y2) {

    LatLng p1 = new FloatLatLng( x1, y1 );
    LatLng p2 = new FloatLatLng( x2, y2 );
    return p1.arcDistance( p2, DistanceUnits.MILES );
  }

  /**
   * distance/radius.
   * @param distance The distance travelled
   * @param radius The radius of the sphere
   * @return The angular distance, in radians
   */
  public static double angularDistance(double distance, double radius){
    return distance/radius;
  }

  /**
   * Calculate the p-norm (i.e. length) beteen two vectors
   *
   * @param vec1  The first vector
   * @param vec2  The second vector
   * @param power The power (2 for Euclidean distance, 1 for manhattan, etc.)
   * @return The length.
   *         <p/>
   *         See http://en.wikipedia.org/wiki/Lp_space
   * @see #vectorDistance(double[], double[], double, double)
   */
  public static double vectorDistance(double[] vec1, double[] vec2, double power) {
    return vectorDistance(vec1, vec2, power, 1.0 / power);
  }

  /**
   * Calculate the p-norm (i.e. length) between two vectors
   *
   * @param vec1         The first vector
   * @param vec2         The second vector
   * @param power        The power (2 for Euclidean distance, 1 for manhattan, etc.)
   * @param oneOverPower If you've precalculated oneOverPower and cached it, use this method to save one division operation over {@link #vectorDistance(double[], double[], double)}.
   * @return The length.
   */
  public static double vectorDistance(double[] vec1, double[] vec2, double power, double oneOverPower) {
    double result = 0;

    if (power == 0) {
      for (int i = 0; i < vec1.length; i++) {
        result += vec1[i] - vec2[i] == 0 ? 0 : 1;
      }

    } else if (power == 1.0) {
      for (int i = 0; i < vec1.length; i++) {
        result += vec1[i] - vec2[i];
      }
    } else if (power == 2.0) {
      result = Math.sqrt(squaredEuclideanDistance(vec1, vec2));
    } else if (power == Integer.MAX_VALUE || Double.isInfinite(power)) {//infinite norm?
      for (int i = 0; i < vec1.length; i++) {
        result = Math.max(result, Math.max(vec1[i], vec2[i]));
      }
    } else {
      for (int i = 0; i < vec1.length; i++) {
        result += Math.pow(vec1[i] - vec2[i], power);
      }
      result = Math.pow(result, oneOverPower);
    }
    return result;
  }

  /**
   * Return the coordinates of a vector that is the corner of a box (upper right or lower left), assuming a Rectangular
   * coordinate system.  Note, this does not apply for points on a sphere or ellipse (although it could be used as an approximatation).
   *
   * @param center     The center point
   * @param result Holds the result, potentially resizing if needed.
   * @param distance   The d from the center to the corner
   * @param upperRight If true, return the coords for the upper right corner, else return the lower left.
   * @return The point, either the upperLeft or the lower right
   */
  public static double[] vectorBoxCorner(double[] center, double[] result, double distance, boolean upperRight) {
    if (result == null || result.length != center.length) {
      result = new double[center.length];
    }
    if (upperRight == false) {
      distance = -distance;
    }
    //We don't care about the power here,
    // b/c we are always in a rectangular coordinate system, so any norm can be used by
    //using the definition of sine
    distance = SIN_45 * distance; // sin(Pi/4) == (2^0.5)/2 == opp/hyp == opp/distance, solve for opp, similarily for cosine
    for (int i = 0; i < center.length; i++) {
      result[i] = center[i] + distance;
    }
    return result;
  }

  /**
   * @param latCenter  In degrees
   * @param lonCenter  In degrees
   * @param distance The distance
   * @param result A preallocated array to hold the results.  If null, a new one is constructed.
   * @param upperRight If true, calculate the upper right corner, else the lower left
   * @param radius The radius of the sphere to use.
   * @return The Lat/Lon in degrees
   *
   * @see #latLonCorner(double, double, double, double[], boolean, double)
   */
  public static double[] latLonCornerDegs(double latCenter, double lonCenter,
                                          double distance, double [] result,
                                          boolean upperRight, double radius) {
    result = latLonCorner(latCenter * DEGREES_TO_RADIANS,
            lonCenter * DEGREES_TO_RADIANS, distance, result, upperRight, radius);
    result[0] = result[0] * RADIANS_TO_DEGREES;
    result[1] = result[1] * RADIANS_TO_DEGREES;
    return result;
  }

  /**
   * Uses Haversine to calculate the corner
   *
   * @param latCenter  In radians
   * @param lonCenter  In radians
   * @param distance   The distance
   * @param result A preallocated array to hold the results.  If null, a new one is constructed.
   * @param upperRight If true, give lat/lon for the upper right corner, else lower left
   * @param radius     The radius to use for the calculation
   * @return The Lat/Lon in Radians

   */
  public static double[] latLonCorner(double latCenter, double lonCenter,
                                      double distance, double [] result, boolean upperRight, double radius) {
    // Haversine formula
    double brng = upperRight ? DEG_45 : DEG_225;
    double lat2 = Math.asin(Math.sin(latCenter) * Math.cos(distance / radius) +
            Math.cos(latCenter) * Math.sin(distance / radius) * Math.cos(brng));
    double lon2 = lonCenter + Math.atan2(Math.sin(brng) * Math.sin(distance / radius) * Math.cos(latCenter),
            Math.cos(distance / radius) - Math.sin(latCenter) * Math.sin(lat2));

    /*lat2 = (lat2*180)/Math.PI;
    lon2 = (lon2*180)/Math.PI;*/
    //From Lucene.  Move back to Lucene when synced
    // normalize long first
    if (result == null || result.length != 2){
      result = new double[2];
    }
    result[0] = lat2;
    result[1] = lon2;
    normLng(result);

    // normalize lat - could flip poles
    normLat(result);

    return result;
  }

  /**
   * @param latLng The lat/lon, in radians. lat in position 0, long in position 1
   */
  public static void normLat(double[] latLng) {

    if (latLng[0] > DEG_90) {
      latLng[0] = DEG_90 - (latLng[0] - DEG_90);
      if (latLng[1] < 0) {
        latLng[1] = latLng[1] + DEG_180;
      } else {
        latLng[1] = latLng[1] - DEG_180;
      }
    } else if (latLng[0] < -DEG_90) {
      latLng[0] = -DEG_90 - (latLng[0] + DEG_90);
      if (latLng[1] < 0) {
        latLng[1] = latLng[1] + DEG_180;
      } else {
        latLng[1] = latLng[1] - DEG_180;
      }
    }

  }

  /**
   * Returns a normalized Lng rectangle shape for the bounding box
   *
   * @param latLng The lat/lon, in radians, lat in position 0, long in position 1
   */
  public static void normLng(double[] latLng) {
    if (latLng[1] > DEG_180) {
      latLng[1] = -1.0 * (DEG_180 - (latLng[1] - DEG_180));
    } else if (latLng[1] < -DEG_180) {
      latLng[1] = (latLng[1] + DEG_180) + DEG_180;
    }
  }

  /**
   * The square of the Euclidean Distance.  Not really a distance, but useful if all that matters is
   * comparing the result to another one.
   *
   * @param vec1 The first point
   * @param vec2 The second point
   * @return The squared Euclidean distance
   */
  public static double squaredEuclideanDistance(double[] vec1, double[] vec2) {
    double result = 0;
    for (int i = 0; i < vec1.length; i++) {
      double v = vec1[i] - vec2[i];
      result += v * v;
    }
    return result;
  }

  /**
   * @param x1     The x coordinate of the first point, in radians
   * @param y1     The y coordinate of the first point, in radians
   * @param x2     The x coordinate of the second point, in radians
   * @param y2     The y coordinate of the second point, in radians
   * @param radius The radius of the sphere
   * @return The distance between the two points, as determined by the Haversine formula.

   */
  public static double haversine(double x1, double y1, double x2, double y2, double radius) {
    double result = 0;
    //make sure they aren't all the same, as then we can just return 0
    if ((x1 != x2) || (y1 != y2)) {
      double diffX = x1 - x2;
      double diffY = y1 - y2;
      double hsinX = Math.sin(diffX * 0.5);
      double hsinY = Math.sin(diffY * 0.5);
      double h = hsinX * hsinX +
              (Math.cos(x1) * Math.cos(x2) * hsinY * hsinY);
      result = (radius * 2 * Math.atan2(Math.sqrt(h), Math.sqrt(1 - h)));
    }
    return result;
  }

  /**
   * Given a string containing <i>dimension</i> values encoded in it, separated by commas, return a String array of length <i>dimension</i>
   * containing the values.
   *
   * @param out         A preallocated array.  Must be size dimension.  If it is not it will be resized.
   * @param externalVal The value to parse
   * @param dimension   The expected number of values for the point
   * @return An array of the values that make up the point (aka vector)
   * @throws org.apache.lucene.spatial.tier.InvalidGeoException if the dimension specified does not match the number of values in the externalValue.
   */
  public static String[] parsePoint(String[] out, String externalVal, int dimension) throws InvalidGeoException {
    //TODO: Should we support sparse vectors?
    if (out == null || out.length != dimension) out = new String[dimension];
    int idx = externalVal.indexOf(',');
    int end = idx;
    int start = 0;
    int i = 0;
    if (idx == -1 && dimension == 1 && externalVal.length() > 0) {//we have a single point, dimension better be 1
      out[0] = externalVal.trim();
      i = 1;
    } else if (idx > 0) {//if it is zero, that is an error
      //Parse out a comma separated list of point values, as in: 73.5,89.2,7773.4
      for (; i < dimension; i++) {
        while (start < end && externalVal.charAt(start) == ' ') start++;
        while (end > start && externalVal.charAt(end - 1) == ' ') end--;
        if (start == end) {
          break;
        }
        out[i] = externalVal.substring(start, end);
        start = idx + 1;
        end = externalVal.indexOf(',', start);
        idx = end;
        if (end == -1) {
          end = externalVal.length();
        }
      }
    }
    if (i != dimension) {
      throw new InvalidGeoException("incompatible dimension (" + dimension +
              ") and values (" + externalVal + ").  Only " + i + " values specified");
    }
    return out;
  }

  /**
   * Given a string containing <i>dimension</i> values encoded in it, separated by commas, return a double array of length <i>dimension</i>
   * containing the values.
   *
   * @param out         A preallocated array.  Must be size dimension.  If it is not it will be resized.
   * @param externalVal The value to parse
   * @param dimension   The expected number of values for the point
   * @return An array of the values that make up the point (aka vector)
   * @throws InvalidGeoException if the dimension specified does not match the number of values in the externalValue.
   */
  public static double[] parsePointDouble(double[] out, String externalVal, int dimension) throws InvalidGeoException{
    if (out == null || out.length != dimension) out = new double[dimension];
    int idx = externalVal.indexOf(',');
    int end = idx;
    int start = 0;
    int i = 0;
    if (idx == -1 && dimension == 1 && externalVal.length() > 0) {//we have a single point, dimension better be 1
      out[0] = Double.parseDouble(externalVal.trim());
      i = 1;
    } else if (idx > 0) {//if it is zero, that is an error
      //Parse out a comma separated list of point values, as in: 73.5,89.2,7773.4
      for (; i < dimension; i++) {
        //TODO: abstract common code with other parsePoint
        while (start < end && externalVal.charAt(start) == ' ') start++;
        while (end > start && externalVal.charAt(end - 1) == ' ') end--;
        if (start == end) {
          break;
        }
        out[i] = Double.parseDouble(externalVal.substring(start, end));
        start = idx + 1;
        end = externalVal.indexOf(',', start);
        idx = end;
        if (end == -1) {
          end = externalVal.length();
        }
      }
    }
    if (i != dimension) {
      throw new InvalidGeoException("incompatible dimension (" + dimension +
              ") and values (" + externalVal + ").  Only " + i + " values specified");
    }
    return out;
  }

  public static final double[] parseLatitudeLongitude(String latLonStr) throws InvalidGeoException {
    return parseLatitudeLongitude(null, latLonStr);
  }

  /**
   * extract (by calling {@link #parsePoint(String[], String, int)} and validate the latitude and longitude contained
   * in the String by making sure the latitude is between 90 & -90 and longitude is between -180 and 180.
   * <p/>
   * The latitude is assumed to be the first part of the string and the longitude the second part.
   *
   * @param latLon    A preallocated array to hold the result
   * @param latLonStr The string to parse.  Latitude is the first value, longitude is the second.
   * @return The lat long
   * @throws InvalidGeoException if there was an error parsing
   */
  public static final double[] parseLatitudeLongitude(double[] latLon, String latLonStr) throws InvalidGeoException {
    if (latLon == null) {
      latLon = new double[2];
    }
    double[] toks = parsePointDouble(null, latLonStr, 2);

    if (toks[0] < -90.0 || toks[0] > 90.0) {
      throw new InvalidGeoException(
              "Invalid latitude: latitudes are range -90 to 90: provided lat: ["
                      + toks[0] + "]");
    }
    latLon[0] = toks[0];


    if (toks[1] < -180.0 || toks[1] > 180.0) {

      throw new InvalidGeoException(
              "Invalid longitude: longitudes are range -180 to 180: provided lon: ["
                      + toks[1] + "]");
    }
    latLon[1] = toks[1];

    return latLon;
  }
}
