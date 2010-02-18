package org.apache.solr.search.function.distance;

import org.apache.solr.common.SolrException;
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


/**
 * Useful distance utiltities.
 * solr-internal: subject to change w/o notification.
 */
public class DistanceUtils {
  public static final double DEGREES_TO_RADIANS = Math.PI / 180.0;
  public static final double RADIANS_TO_DEGREES = 180.0 / Math.PI;

  public static final double KM_TO_MILES = 0.621371192;
  public static final double MILES_TO_KM = 1.609344;

  /**
   * Calculate the p-norm (i.e. length) between two vectors
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
    } else if (power == Integer.MAX_VALUE || Double.isInfinite(power)) {//infininte norm?
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

  public static double squaredEuclideanDistance(double[] vec1, double[] vec2) {
    double result = 0;
    for (int i = 0; i < vec1.length; i++) {
      double v = vec1[i] - vec2[i];
      result += v * v;
    }
    return result;
  }

  /**
   * @param x1     The x coordinate of the first point
   * @param y1     The y coordinate of the first point
   * @param x2     The x coordinate of the second point
   * @param y2     The y coordinate of the second point
   * @param radius The radius of the sphere
   * @return The distance between the two points, as determined by the Haversine formula.
   * @see org.apache.solr.search.function.distance.HaversineFunction
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
   * @throws {@link SolrException} if the dimension specified does not match the number of values in the externalValue.
   */
  public static String[] parsePoint(String[] out, String externalVal, int dimension) {
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
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "incompatible dimension (" + dimension +
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
   * @throws {@link SolrException} if the dimension specified does not match the number of values in the externalValue.
   */
  public static double[] parsePointDouble(double[] out, String externalVal, int dimension) {
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
        out[i] = Double.parseDouble(externalVal.substring(start, end));
        start = idx + 1;
        end = externalVal.indexOf(',', start);
	idex = end;
        if (end == -1) {
          end = externalVal.length();
        }
      }
    }
    if (i != dimension) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "incompatible dimension (" + dimension +
              ") and values (" + externalVal + ").  Only " + i + " values specified");
    }
    return out;
  }

  /**
   * extract (by calling {@link #parsePoint(String[], String, int)} and validate the latitude and longitude contained
   * in the String by making sure the latitude is between 90 & -90 and longitude is between -180 and 180.
   * <p/>
   * The latitude is assumed to be the first part of the string and the longitude the second part.
   *
   * @param latLon    A preallocated array to hold the result
   * @param latLonStr The string to parse
   * @return The lat long
   */
  public static final double[] parseLatitudeLongitude(double[] latLon, String latLonStr) {
    if (latLon == null) {
      latLon = new double[2];
    }
    double[] toks = DistanceUtils.parsePointDouble(null, latLonStr, 2);
    latLon[0] = Double.valueOf(toks[0]);

    if (latLon[0] < -90.0 || latLon[0] > 90.0) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
              "Invalid latitude: latitudes are range -90 to 90: provided lat: ["
                      + latLon[0] + "]");
    }

    latLon[1] = Double.valueOf(toks[1]);

    if (latLon[1] < -180.0 || latLon[1] > 180.0) {

      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
              "Invalid longitude: longitudes are range -180 to 180: provided lon: ["
                      + latLon[1] + "]");
    }

    return latLon;
  }
}
