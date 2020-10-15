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

import java.util.Arrays;

/**
 * Represents a line on the earth's surface.  You can construct the Line directly with {@code double[]}
 * coordinates.
 * <p>
 * NOTES:
 * <ol>
 *   <li>All latitude/longitude values must be in decimal degrees.
 *   <li>For more advanced GeoSpatial indexing and query operations see the {@code spatial-extras} module
 * </ol>
 */
public class Line extends LatLonGeometry {
  /** array of latitude coordinates */
  private final double[] lats;
  /** array of longitude coordinates */
  private final double[] lons;

  /** minimum latitude of this line's bounding box */
  public final double minLat;
  /** maximum latitude of this line's bounding box */
  public final double maxLat;
  /** minimum longitude of this line's bounding box */
  public final double minLon;
  /** maximum longitude of this line's bounding box */
  public final double maxLon;

  /**
   * Creates a new Line from the supplied latitude/longitude array.
   */
  public Line(double[] lats, double[] lons) {
    if (lats == null) {
      throw new IllegalArgumentException("lats must not be null");
    }
    if (lons == null) {
      throw new IllegalArgumentException("lons must not be null");
    }
    if (lats.length != lons.length) {
      throw new IllegalArgumentException("lats and lons must be equal length");
    }
    if (lats.length < 2) {
      throw new IllegalArgumentException("at least 2 line points required");
    }

    // compute bounding box
    double minLat = lats[0];
    double minLon = lons[0];
    double maxLat = lats[0];
    double maxLon = lons[0];
    for (int i = 0; i < lats.length; ++i) {
      GeoUtils.checkLatitude(lats[i]);
      GeoUtils.checkLongitude(lons[i]);
      minLat = Math.min(lats[i], minLat);
      minLon = Math.min(lons[i], minLon);
      maxLat = Math.max(lats[i], maxLat);
      maxLon = Math.max(lons[i], maxLon);
    }

    this.lats = lats.clone();
    this.lons = lons.clone();
    this.minLat = minLat;
    this.maxLat = maxLat;
    this.minLon = minLon;
    this.maxLon = maxLon;
  }

  /** returns the number of vertex points */
  public int numPoints() {
    return lats.length;
  }

  /** Returns latitude value at given index */
  public double getLat(int vertex) {
    return lats[vertex];
  }

  /** Returns longitude value at given index */
  public double getLon(int vertex) {
    return lons[vertex];
  }

  /** Returns a copy of the internal latitude array */
  public double[] getLats() {
    return lats.clone();
  }

  /** Returns a copy of the internal longitude array */
  public double[] getLons() {
    return lons.clone();
  }

  @Override
  protected Component2D toComponent2D() {
    return Line2D.create(this);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Line)) return false;
    Line line = (Line) o;
    return Arrays.equals(lats, line.lats) && Arrays.equals(lons, line.lons);
  }

  @Override
  public int hashCode() {
    int result = Arrays.hashCode(lats);
    result = 31 * result + Arrays.hashCode(lons);
    return result;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("LINE(");
    for (int i = 0; i < lats.length; i++) {
      sb.append("[")
          .append(lons[i])
          .append(", ")
          .append(lats[i])
          .append("]");
    }
    sb.append(')');
    return sb.toString();
  }

  /** prints lines as geojson */
  public String toGeoJSON() {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    sb.append(Polygon.verticesToGeoJSON(lats, lons));
    sb.append("]");
    return sb.toString();
  }
}
