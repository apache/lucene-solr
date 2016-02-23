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
package org.apache.lucene.document;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.spatial.util.GeoUtils;

/** Add this to a document to index lat/lon point dimensionally */
public class LatLonPoint extends Field {
  public static final FieldType TYPE = new FieldType();
  static {
    TYPE.setDimensions(2, Integer.BYTES);
    TYPE.freeze();
  }

  /** 
   * Creates a new LatLonPoint with the specified lat and lon
   * @param name field name
   * @param lat double latitude
   * @param lon double longitude
   * @throws IllegalArgumentException if the field name is null or lat or lon are out of bounds
   */
  public LatLonPoint(String name, double lat, double lon) {
    super(name, TYPE);
    if (GeoUtils.isValidLat(lat) == false) {
      throw new IllegalArgumentException("invalid lat (" + lat + "): must be -90 to 90");
    }
    if (GeoUtils.isValidLon(lon) == false) {
      throw new IllegalArgumentException("invalid lon (" + lon + "): must be -180 to 180");
    }
    byte[] bytes = new byte[8];
    NumericUtils.intToBytes(encodeLat(lat), bytes, 0);
    NumericUtils.intToBytes(encodeLon(lon), bytes, Integer.BYTES);
    fieldsData = new BytesRef(bytes);
  }

  public static final double TOLERANCE = 1E-7;

  private static final int BITS = 32;

  private static final double LON_SCALE = (0x1L<<BITS)/360.0D;
  private static final double LAT_SCALE = (0x1L<<BITS)/180.0D;

  /** Quantizes double (64 bit) latitude into 32 bits */
  public static int encodeLat(double lat) {
    assert GeoUtils.isValidLat(lat): "lat=" + lat;
    long x = (long) (lat * LAT_SCALE);
    assert x < Integer.MAX_VALUE: "lat=" + lat + " mapped to Integer.MAX_VALUE + " + (x - Integer.MAX_VALUE);
    assert x > Integer.MIN_VALUE: "lat=" + lat + " mapped to Integer.MIN_VALUE";
    return (int) x;
  }

  /** Quantizes double (64 bit) longitude into 32 bits */
  public static int encodeLon(double lon) {
    assert GeoUtils.isValidLon(lon): "lon=" + lon;
    long x = (long) (lon * LON_SCALE);
    assert x < Integer.MAX_VALUE;
    assert x > Integer.MIN_VALUE;
    return (int) x;
  }

  /** Turns quantized value from {@link #encodeLat} back into a double. */
  public static double decodeLat(int x) {
    return x / LAT_SCALE;
  }

  /** Turns quantized value from {@link #encodeLon} back into a double. */
  public static double decodeLon(int x) {
    return x / LON_SCALE;
  }
}
