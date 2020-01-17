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

/**
 * Represents a point on the earth's surface.  You can construct the point directly with {@code double}
 * coordinates.
 * <p>
 * NOTES:
 * <ol>
 *   <li>latitude/longitude values must be in decimal degrees.
 *   <li>For more advanced GeoSpatial indexing and query operations see the {@code spatial-extras} module
 * </ol>
 */
public final class Point implements LatLonGeometry {

  /** latitude coordinate */
  private final double lat;
  /** longitude coordinate */
  private final double lon;

  /**
   * Creates a new Point from the supplied latitude/longitude.
   */
  public Point(double lat, double lon) {

    GeoUtils.checkLatitude(lat);
    GeoUtils.checkLongitude(lon);
    this.lat = lat;
    this.lon = lon;
  }

  @Override
  public double getMinLon() {
    return lon;
  }

  @Override
  public double getMaxLon() {
    return lon;
  }

  @Override
  public double getMinLat() {
    return lat;
  }

  @Override
  public double getMaxLat() {
    return lat;
  }

  /** Returns latitude value at given index */
  public double getLat() {
    return lat;
  }

  /** Returns longitude value at given index */
  public double getLon() {
    return lon;
  }

  @Override
  public Component2D toComponent2D() {
    double qLat = GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(lat));
    double qLon = GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(lon));
    return Point2D.create(new double[] {qLat, qLon});
  }

  @Override
  public String toGeoJSON() {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    sb.append(lon + "," + lat);
    sb.append("]");
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Point)) return false;
    Point point = (Point) o;
    return point.lat == lat && point.lon == lon;
  }

  @Override
  public int hashCode() {
    int result = Double.hashCode(lat);
    result = 31 * result + Double.hashCode(lon);
    return result;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Point(");
    sb.append(lon);
    sb.append(",");
    sb.append(lat);
    sb.append(')');
    return sb.toString();
  }
}
