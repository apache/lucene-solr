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
 * Represents a closed polygon on the earth's surface.
 * @lucene.experimental
 */
public final class Polygon {
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

  // TODO: refactor to GeoUtils once LUCENE-7165 is complete
  private static final double ENCODING_TOLERANCE = 1e-6;

  // TODO: we could also compute the maximal inner bounding box, to make relations faster to compute?

  /**
   * Creates a new Polygon from the supplied latitude/longitude array, and optionally any holes.
   */
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
    if (polyLats[0] != polyLats[polyLats.length-1]) {
      throw new IllegalArgumentException("first and last points of the polygon must be the same (it must close itself): polyLats[0]=" + polyLats[0] + " polyLats[" + (polyLats.length-1) + "]=" + polyLats[polyLats.length-1]);
    }
    if (polyLons[0] != polyLons[polyLons.length-1]) {
      throw new IllegalArgumentException("first and last points of the polygon must be the same (it must close itself): polyLons[0]=" + polyLons[0] + " polyLons[" + (polyLons.length-1) + "]=" + polyLons[polyLons.length-1]);
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
    double minLat = Double.POSITIVE_INFINITY;
    double maxLat = Double.NEGATIVE_INFINITY;
    double minLon = Double.POSITIVE_INFINITY;
    double maxLon = Double.NEGATIVE_INFINITY;

    for (int i = 0;i < polyLats.length; i++) {
      minLat = Math.min(polyLats[i], minLat);
      maxLat = Math.max(polyLats[i], maxLat);
      minLon = Math.min(polyLons[i], minLon);
      maxLon = Math.max(polyLons[i], maxLon);
    }
    this.minLat = minLat;
    this.maxLat = maxLat;
    this.minLon = minLon;
    this.maxLon = maxLon;
  }

  /** Returns true if the point is contained within this polygon */
  public boolean contains(double latitude, double longitude) {
    // check bounding box
    if (latitude < minLat || latitude > maxLat || longitude < minLon || longitude > maxLon) {
      return false;
    }
    /*
     * simple even-odd point in polygon computation
     *    1.  Determine if point is contained in the longitudinal range
     *    2.  Determine whether point crosses the edge by computing the latitudinal delta
     *        between the end-point of a parallel vector (originating at the point) and the
     *        y-component of the edge sink
     *
     * NOTE: Requires polygon point (x,y) order either clockwise or counter-clockwise
     */
    boolean inPoly = false;
    /*
     * Note: This is using a euclidean coordinate system which could result in
     * upwards of 110KM error at the equator.
     * TODO convert coordinates to cylindrical projection (e.g. mercator)
     */
    for (int i = 1; i < polyLats.length; i++) {
      if (polyLons[i] <= longitude && polyLons[i-1] >= longitude || polyLons[i-1] <= longitude && polyLons[i] >= longitude) {
        if (polyLats[i] + (longitude - polyLons[i]) / (polyLons[i-1] - polyLons[i]) * (polyLats[i-1] - polyLats[i]) <= latitude) {
          inPoly = !inPoly;
        }
      }
    }
    if (inPoly) {
      for (Polygon hole : holes) {
        if (hole.contains(latitude, longitude)) {
          return false;
        }
      }
      return true;
    } else {
      return false;
    }
  }

  /**
   * Computes whether a rectangle is within a polygon (shared boundaries not allowed)
   */
  public boolean contains(double minLat, double maxLat, double minLon, double maxLon) {
    // check if rectangle crosses poly (to handle concave/pacman polys), then check that all 4 corners
    // are contained
    boolean contains = crosses(minLat, maxLat, minLon, maxLon) == false &&
                       contains(minLat, minLon) &&
                       contains(minLat, maxLon) &&
                       contains(maxLat, maxLon) &&
                       contains(maxLat, minLon);

    if (contains) {
      // if we intersect with any hole, game over
      for (Polygon hole : holes) {
        if (hole.crosses(minLat, maxLat, minLon, maxLon) || hole.contains(minLat, maxLat, minLon, maxLon)) {
          return false;
        }
      }
      return true;
    } else {
      return false;
    }
  }

  /**
   * Convenience method for accurately computing whether a rectangle crosses a poly.
   */
  public boolean crosses(double minLat, double maxLat, final double minLon, final double maxLon) {
    // if the bounding boxes are disjoint then the shape does not cross
    if (maxLon < this.minLon || minLon > this.maxLon || maxLat < this.minLat || minLat > this.maxLat) {
      return false;
    }
    // if the rectangle fully encloses us, we cross.
    if (minLat <= this.minLat && maxLat >= this.maxLat && minLon <= this.minLon && maxLon >= this.maxLon) {
      return true;
    }
    // if we cross any hole, we cross
    for (Polygon hole : holes) {
      if (hole.crosses(minLat, maxLat, minLon, maxLon)) {
        return true;
      }
    }

    /*
     * Accurately compute (within restrictions of cartesian decimal degrees) whether a rectangle crosses a polygon
     */
    final double[][] bbox = new double[][] { {minLon, minLat}, {maxLon, minLat}, {maxLon, maxLat}, {minLon, maxLat}, {minLon, minLat} };
    final int polyLength = polyLons.length-1;
    double d, s, t, a1, b1, c1, a2, b2, c2;
    double x00, y00, x01, y01, x10, y10, x11, y11;

    // computes the intersection point between each bbox edge and the polygon edge
    for (short b=0; b<4; ++b) {
      a1 = bbox[b+1][1]-bbox[b][1];
      b1 = bbox[b][0]-bbox[b+1][0];
      c1 = a1*bbox[b+1][0] + b1*bbox[b+1][1];
      for (int p=0; p<polyLength; ++p) {
        a2 = polyLats[p+1]-polyLats[p];
        b2 = polyLons[p]-polyLons[p+1];
        // compute determinant
        d = a1*b2 - a2*b1;
        if (d != 0) {
          // lines are not parallel, check intersecting points
          c2 = a2*polyLons[p+1] + b2*polyLats[p+1];
          s = (1/d)*(b2*c1 - b1*c2);
          t = (1/d)*(a1*c2 - a2*c1);
          // todo TOLERANCE SHOULD MATCH EVERYWHERE this is currently blocked by LUCENE-7165
          x00 = Math.min(bbox[b][0], bbox[b+1][0]) - ENCODING_TOLERANCE;
          x01 = Math.max(bbox[b][0], bbox[b+1][0]) + ENCODING_TOLERANCE;
          y00 = Math.min(bbox[b][1], bbox[b+1][1]) - ENCODING_TOLERANCE;
          y01 = Math.max(bbox[b][1], bbox[b+1][1]) + ENCODING_TOLERANCE;
          x10 = Math.min(polyLons[p], polyLons[p+1]) - ENCODING_TOLERANCE;
          x11 = Math.max(polyLons[p], polyLons[p+1]) + ENCODING_TOLERANCE;
          y10 = Math.min(polyLats[p], polyLats[p+1]) - ENCODING_TOLERANCE;
          y11 = Math.max(polyLats[p], polyLats[p+1]) + ENCODING_TOLERANCE;
          // check whether the intersection point is touching one of the line segments
          boolean touching = ((x00 == s && y00 == t) || (x01 == s && y01 == t))
              || ((x10 == s && y10 == t) || (x11 == s && y11 == t));
          // if line segments are not touching and the intersection point is within the range of either segment
          if (!(touching || x00 > s || x01 < s || y00 > t || y01 < t || x10 > s || x11 < s || y10 > t || y11 < t)) {
            return true;
          }
        }
      } // for each poly edge
    } // for each bbox edge
    return false;
  }

  /** Returns a copy of the internal latitude array */
  public double[] getPolyLats() {
    return polyLats.clone();
  }

  /** Returns a copy of the internal longitude array */
  public double[] getPolyLons() {
    return polyLons.clone();
  }

  /** Returns a copy of the internal holes array */
  public Polygon[] getHoles() {
    return holes.clone();
  }

  /** Helper for multipolygon logic: returns true if any of the supplied polygons contain the point */
  public static boolean contains(Polygon[] polygons, double latitude, double longitude) {
    for (Polygon polygon : polygons) {
      if (polygon.contains(latitude, longitude)) {
        return true;
      }
    }
    return false;
  }

  /** Helper for multipolygon logic: returns true if any of the supplied polygons contain the rectangle */
  public static boolean contains(Polygon[] polygons, double minLat, double maxLat, double minLon, double maxLon) {
    for (Polygon polygon : polygons) {
      if (polygon.contains(minLat, maxLat, minLon, maxLon)) {
        return true;
      }
    }
    return false;
  }

  /** Helper for multipolygon logic: returns true if any of the supplied polygons crosses the rectangle */
  public static boolean crosses(Polygon[] polygons, double minLat, double maxLat, double minLon, double maxLon) {
    for (Polygon polygon : polygons) {
      if (polygon.crosses(minLat, maxLat, minLon, maxLon)) {
        return true;
      }
    }
    return false;
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
      sb.append("[")
      .append(polyLats[i])
      .append(", ")
      .append(polyLons[i])
      .append("] ");
    }
    if (holes.length > 0) {
      sb.append(", holes=");
      sb.append(Arrays.toString(holes));
    }
    return sb.toString();
  }
}
