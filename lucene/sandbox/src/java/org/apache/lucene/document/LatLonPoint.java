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

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.PointInPolygonQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.util.GeoUtils;

/** 
 * A field indexing geographic coordinates dimensionally such that finding
 * all documents within a range at search time is
 * efficient.  Multiple values for the same field in one document
 * is allowed. 
 * <p>
 * This field defines static factory methods for creating common queries:
 * <ul>
 *   <li>{@link #newBoxQuery newBoxQuery()} for matching points within a bounding box.
 *   <li>{@link #newPolygonQuery newPolygonQuery()} for matching points within an arbitrary polygon.
 * </ul>
 * <p>
 * <b>WARNING</b>: Values are indexed with some loss of precision, incurring up to 1E-7 error from the
 * original {@code double} values. 
 */
// TODO ^^^ that is very sandy and hurts the API, usage, and tests tremendously, because what the user passes
// to the field is not actually what gets indexed. Float would be 1E-5 error vs 1E-7, but it might be
// a better tradeoff? then it would be completely transparent to the user and lucene would be "lossless".
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

  private static final int BITS = 32;
  private static final double LON_SCALE = (0x1L<<BITS)/360.0D;
  private static final double LAT_SCALE = (0x1L<<BITS)/180.0D;
  
  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    result.append(type.toString());
    result.append('<');
    result.append(name);
    result.append(':');

    BytesRef bytes = (BytesRef) fieldsData;
    result.append(decodeLat(BytesRef.deepCopyOf(bytes).bytes, 0));
    result.append(',');
    result.append(decodeLon(BytesRef.deepCopyOf(bytes).bytes, Integer.BYTES));

    result.append('>');
    return result.toString();
  }

  // public helper methods (e.g. for queries)

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
  
  /** Turns quantized value from byte array back into a double. */
  public static double decodeLat(byte[] src, int offset) {
    return decodeLat(NumericUtils.bytesToInt(src, offset));
  }

  /** Turns quantized value from {@link #encodeLon} back into a double. */
  public static double decodeLon(int x) {
    return x / LON_SCALE;
  }

  // nocommit newSetQuery
  
  /** Turns quantized value from byte array back into a double. */
  public static double decodeLon(byte[] src, int offset) {
    return decodeLon(NumericUtils.bytesToInt(src, offset));
  }
  
  /** sugar encodes a single point as a 2D byte array */
  private static byte[][] encode(double lat, double lon) {
    byte[][] bytes = new byte[2][];
    bytes[0] = new byte[4];
    NumericUtils.intToBytes(encodeLat(lat), bytes[0], 0);
    bytes[1] = new byte[4];
    NumericUtils.intToBytes(encodeLon(lon), bytes[1], 0);
    return bytes;
  }
   
  // static methods for generating queries

  /**
   * Create a query for matching a bounding box.
   * <p>
   * The box may cross over the dateline.
   */
  public static Query newBoxQuery(String field, double minLat, double maxLat, double minLon, double maxLon) {
    if (GeoUtils.isValidLat(minLat) == false) {
      throw new IllegalArgumentException("minLat=" + minLat + " is not a valid latitude");
    }
    if (GeoUtils.isValidLat(maxLat) == false) {
      throw new IllegalArgumentException("maxLat=" + maxLat + " is not a valid latitude");
    }
    if (GeoUtils.isValidLon(minLon) == false) {
      throw new IllegalArgumentException("minLon=" + minLon + " is not a valid longitude");
    }
    if (GeoUtils.isValidLon(maxLon) == false) {
      throw new IllegalArgumentException("maxLon=" + maxLon + " is not a valid longitude");
    }
    
    byte[][] lower = encode(minLat, minLon);
    byte[][] upper = encode(maxLat, maxLon);
    // Crosses date line: we just rewrite into OR of two bboxes, with longitude as an open range:
    if (maxLon < minLon) {
      // Disable coord here because a multi-valued doc could match both rects and get unfairly boosted:
      BooleanQuery.Builder q = new BooleanQuery.Builder();
      q.setDisableCoord(true);

      // E.g.: maxLon = -179, minLon = 179
      byte[][] leftOpen = new byte[2][];
      leftOpen[0] = lower[0];
      // leave longitude open (null)
      Query left = newBoxInternal(field, leftOpen, upper);
      q.add(new BooleanClause(left, BooleanClause.Occur.SHOULD));
      byte[][] rightOpen = new byte[2][];
      rightOpen[0] = upper[0];
      // leave longitude open (null)
      Query right = newBoxInternal(field, lower, rightOpen);
      q.add(new BooleanClause(right, BooleanClause.Occur.SHOULD));
      return new ConstantScoreQuery(q.build());
    } else {
      return newBoxInternal(field, lower, upper);
    }
  }
  
  private static Query newBoxInternal(String field, byte[][] min, byte[][] max) {
    return new PointRangeQuery(field, min, new boolean[] { true, true }, max, new boolean[] { false, false }) {
      @Override
      protected String toString(int dimension, byte[] value) {
        if (dimension == 0) {
          return Double.toString(decodeLat(value, 0));
        } else if (dimension == 1) {
          return Double.toString(decodeLon(value, 0));
        } else {
          throw new AssertionError();
        }
      }
    };
  }
  
  /** 
   * Create a query for matching a polygon.
   * <p>
   * The supplied {@code polyLats}/{@code polyLons} must be clockwise or counter-clockwise.
   */
  public static Query newPolygonQuery(String field, double[] polyLats, double[] polyLons) {
    return new PointInPolygonQuery(field, polyLats, polyLons);
  }
}
