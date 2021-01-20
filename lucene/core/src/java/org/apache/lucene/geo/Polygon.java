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

import java.text.ParseException;
import java.util.Arrays;
import org.apache.lucene.geo.GeoUtils.WindingOrder;

/**
 * Represents a closed polygon on the earth's surface. You can either construct the Polygon directly
 * yourself with {@code double[]} coordinates, or use {@link Polygon#fromGeoJSON} if you have a
 * polygon already encoded as a <a href="http://geojson.org/geojson-spec.html">GeoJSON</a> string.
 *
 * <p>NOTES:
 *
 * <ol>
 *   <li>Coordinates must be in clockwise order, except for holes. Holes must be in
 *       counter-clockwise order.
 *   <li>The polygon must be closed: the first and last coordinates need to have the same values.
 *   <li>The polygon must not be self-crossing, otherwise may result in unexpected behavior.
 *   <li>All latitude/longitude values must be in decimal degrees.
 *   <li>Polygons cannot cross the 180th meridian. Instead, use two polygons: one on each side.
 *   <li>For more advanced GeoSpatial indexing and query operations see the {@code spatial-extras}
 *       module
 * </ol>
 *
 * @lucene.experimental
 */
public final class Polygon extends LatLonGeometry {
  private final double[] polyLats;
  private final double[] polyLons;
  private final Polygon[] holes;

  /** minimum latitude of this polygon's bounding box area */
  public final double minLat;
  /** maximum latitude of this polygon's bounding box area */
  public final double maxLat;
  /** minimum longitude of this polygon's bounding box area */
  public final double minLon;
  /** maximum longitude of this polygon's bounding box area */
  public final double maxLon;
  /** winding order of the vertices */
  private final WindingOrder windingOrder;

  /** Creates a new Polygon from the supplied latitude/longitude array, and optionally any holes. */
  public Polygon(double[] polyLats, double[] polyLons, Polygon... holes) {
    if (polyLats == null) {
      throw new IllegalArgumentException("polyLats must not be null");
    }
    if (polyLons == null) {
      throw new IllegalArgumentException("polyLons must not be null");
    }
    if (holes == null) {
      throw new IllegalArgumentException("holes must not be null");
    }
    if (polyLats.length != polyLons.length) {
      throw new IllegalArgumentException("polyLats and polyLons must be equal length");
    }
    if (polyLats.length != polyLons.length) {
      throw new IllegalArgumentException("polyLats and polyLons must be equal length");
    }
    if (polyLats.length < 4) {
      throw new IllegalArgumentException("at least 4 polygon points required");
    }
    if (polyLats[0] != polyLats[polyLats.length - 1]) {
      throw new IllegalArgumentException(
          "first and last points of the polygon must be the same (it must close itself): polyLats[0]="
              + polyLats[0]
              + " polyLats["
              + (polyLats.length - 1)
              + "]="
              + polyLats[polyLats.length - 1]);
    }
    if (polyLons[0] != polyLons[polyLons.length - 1]) {
      throw new IllegalArgumentException(
          "first and last points of the polygon must be the same (it must close itself): polyLons[0]="
              + polyLons[0]
              + " polyLons["
              + (polyLons.length - 1)
              + "]="
              + polyLons[polyLons.length - 1]);
    }
    for (int i = 0; i < polyLats.length; i++) {
      GeoUtils.checkLatitude(polyLats[i]);
      GeoUtils.checkLongitude(polyLons[i]);
    }
    for (int i = 0; i < holes.length; i++) {
      Polygon inner = holes[i];
      if (inner.holes.length > 0) {
        throw new IllegalArgumentException("holes may not contain holes: polygons may not nest.");
      }
    }
    this.polyLats = polyLats.clone();
    this.polyLons = polyLons.clone();
    this.holes = holes.clone();

    // compute bounding box
    double minLat = polyLats[0];
    double maxLat = polyLats[0];
    double minLon = polyLons[0];
    double maxLon = polyLons[0];

    double windingSum = 0d;
    final int numPts = polyLats.length - 1;
    for (int i = 1, j = 0; i < numPts; j = i++) {
      minLat = Math.min(polyLats[i], minLat);
      maxLat = Math.max(polyLats[i], maxLat);
      minLon = Math.min(polyLons[i], minLon);
      maxLon = Math.max(polyLons[i], maxLon);
      // compute signed area
      windingSum +=
          (polyLons[j] - polyLons[numPts]) * (polyLats[i] - polyLats[numPts])
              - (polyLats[j] - polyLats[numPts]) * (polyLons[i] - polyLons[numPts]);
    }
    this.minLat = minLat;
    this.maxLat = maxLat;
    this.minLon = minLon;
    this.maxLon = maxLon;
    this.windingOrder = (windingSum < 0) ? GeoUtils.WindingOrder.CCW : GeoUtils.WindingOrder.CW;
  }

  /** returns the number of vertex points */
  public int numPoints() {
    return polyLats.length;
  }

  /** Returns a copy of the internal latitude array */
  public double[] getPolyLats() {
    return polyLats.clone();
  }

  /** Returns latitude value at given index */
  public double getPolyLat(int vertex) {
    return polyLats[vertex];
  }

  /** Returns a copy of the internal longitude array */
  public double[] getPolyLons() {
    return polyLons.clone();
  }

  /** Returns longitude value at given index */
  public double getPolyLon(int vertex) {
    return polyLons[vertex];
  }

  /** Returns a copy of the internal holes array */
  public Polygon[] getHoles() {
    return holes.clone();
  }

  Polygon getHole(int i) {
    return holes[i];
  }

  /** Returns the winding order (CW, COLINEAR, CCW) for the polygon shell */
  public WindingOrder getWindingOrder() {
    return this.windingOrder;
  }

  /** returns the number of holes for the polygon */
  public int numHoles() {
    return holes.length;
  }

  @Override
  protected Component2D toComponent2D() {
    return Polygon2D.create(this);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(holes);
    result = prime * result + Arrays.hashCode(polyLats);
    result = prime * result + Arrays.hashCode(polyLons);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    Polygon other = (Polygon) obj;
    if (!Arrays.equals(holes, other.holes)) return false;
    if (!Arrays.equals(polyLats, other.polyLats)) return false;
    if (!Arrays.equals(polyLons, other.polyLons)) return false;
    return true;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < polyLats.length; i++) {
      sb.append("[").append(polyLats[i]).append(", ").append(polyLons[i]).append("] ");
    }
    if (holes.length > 0) {
      sb.append(", holes=");
      sb.append(Arrays.toString(holes));
    }
    return sb.toString();
  }

  public static String verticesToGeoJSON(final double[] lats, final double[] lons) {
    StringBuilder sb = new StringBuilder();
    sb.append('[');
    for (int i = 0; i < lats.length; i++) {
      sb.append("[").append(lons[i]).append(", ").append(lats[i]).append("]");
      if (i != lats.length - 1) {
        sb.append(", ");
      }
    }
    sb.append(']');
    return sb.toString();
  }

  /** prints polygons as geojson */
  public String toGeoJSON() {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    sb.append(verticesToGeoJSON(polyLats, polyLons));
    for (Polygon hole : holes) {
      sb.append(",");
      sb.append(verticesToGeoJSON(hole.polyLats, hole.polyLons));
    }
    sb.append("]");
    return sb.toString();
  }

  /**
   * Parses a standard GeoJSON polygon string. The type of the incoming GeoJSON object must be a
   * Polygon or MultiPolygon, optionally embedded under a "type: Feature". A Polygon will return as
   * a length 1 array, while a MultiPolygon will be 1 or more in length.
   *
   * <p>See <a href="http://geojson.org/geojson-spec.html">the GeoJSON specification</a>.
   */
  public static Polygon[] fromGeoJSON(String geojson) throws ParseException {
    return new SimpleGeoJSONPolygonParser(geojson).parse();
  }
}
