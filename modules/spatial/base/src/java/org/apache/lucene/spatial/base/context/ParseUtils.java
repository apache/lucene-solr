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

package org.apache.lucene.spatial.base.context;

import org.apache.lucene.spatial.base.exception.InvalidShapeException;

/**
 * Utility methods related to parsing shapes.
 * Methods here were formerly in DistanceUtils.
 */
public class ParseUtils {
  private ParseUtils() {
  }

  /**
   * Given a string containing <i>dimension</i> values encoded in it, separated by commas, return a String array of length <i>dimension</i>
   * containing the values.
   *
   * @param out         A preallocated array.  Must be size dimension.  If it is not it will be resized.
   * @param externalVal The value to parse
   * @param dimension   The expected number of values for the point
   * @return An array of the values that make up the point (aka vector)
   * @throws org.apache.lucene.spatial.base.exception.InvalidShapeException if the dimension specified does not match the number of values in the externalValue.
   */
  public static String[] parsePoint(String[] out, String externalVal, int dimension) throws InvalidShapeException {
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
      throw new InvalidShapeException("incompatible dimension (" + dimension +
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
   * @throws org.apache.lucene.spatial.base.exception.InvalidShapeException if the dimension specified does not match the number of values in the externalValue.
   */
  public static double[] parsePointDouble(double[] out, String externalVal, int dimension) throws InvalidShapeException{
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
      throw new InvalidShapeException("incompatible dimension (" + dimension +
              ") and values (" + externalVal + ").  Only " + i + " values specified");
    }
    return out;
  }

  public static final double[] parseLatitudeLongitude(String latLonStr) throws InvalidShapeException {
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
   * @throws org.apache.lucene.spatial.base.exception.InvalidShapeException if there was an error parsing
   */
  public static final double[] parseLatitudeLongitude(double[] latLon, String latLonStr) throws InvalidShapeException {
    if (latLon == null) {
      latLon = new double[2];
    }
    double[] toks = parsePointDouble(null, latLonStr, 2);

    if (toks[0] < -90.0 || toks[0] > 90.0) {
      throw new InvalidShapeException(
              "Invalid latitude: latitudes are range -90 to 90: provided lat: ["
                      + toks[0] + "]");
    }
    latLon[0] = toks[0];


    if (toks[1] < -180.0 || toks[1] > 180.0) {

      throw new InvalidShapeException(
              "Invalid longitude: longitudes are range -180 to 180: provided lon: ["
                      + toks[1] + "]");
    }
    latLon[1] = toks[1];

    return latLon;
  }
}
