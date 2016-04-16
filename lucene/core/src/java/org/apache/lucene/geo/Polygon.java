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

import org.apache.lucene.index.PointValues.Relation;

/**
 * Represents a closed polygon on the earth's surface.
 * <p>
 * NOTES:
 * <ol>
 *   <li>Coordinates must be in clockwise order, except for holes. Holes must be in counter-clockwise order.
 *   <li>The polygon must be closed: the first and last coordinates need to have the same values.
 *   <li>The polygon must not be self-crossing, otherwise may result in unexpected behavior.
 *   <li>All latitude/longitude values must be in decimal degrees.
 *   <li>Polygons cannot cross the 180th meridian. Instead, use two polygons: one on each side.
 *   <li>For more advanced GeoSpatial indexing and query operations see the {@code spatial-extras} module
 * </ol>
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

  /** 
   * Returns true if the point is contained within this polygon.
   * <p>
   * See <a href="https://www.ecse.rpi.edu/~wrf/Research/Short_Notes/pnpoly.html">
   * https://www.ecse.rpi.edu/~wrf/Research/Short_Notes/pnpoly.html</a> for more information.
   */
  // ported to java from https://www.ecse.rpi.edu/~wrf/Research/Short_Notes/pnpoly.html
  // original code under the BSD license (https://www.ecse.rpi.edu/~wrf/Research/Short_Notes/pnpoly.html#License%20to%20Use)
  //
  // Copyright (c) 1970-2003, Wm. Randolph Franklin
  //
  // Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated 
  // documentation files (the "Software"), to deal in the Software without restriction, including without limitation 
  // the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and 
  // to permit persons to whom the Software is furnished to do so, subject to the following conditions:
  //
  // 1. Redistributions of source code must retain the above copyright 
  //    notice, this list of conditions and the following disclaimers.
  // 2. Redistributions in binary form must reproduce the above copyright 
  //    notice in the documentation and/or other materials provided with 
  //    the distribution.
  // 3. The name of W. Randolph Franklin may not be used to endorse or 
  //    promote products derived from this Software without specific 
  //    prior written permission. 
  //
  // THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED 
  // TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL 
  // THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF 
  // CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS 
  // IN THE SOFTWARE. 
  public boolean contains(double latitude, double longitude) {
    // check bounding box
    if (latitude < minLat || latitude > maxLat || longitude < minLon || longitude > maxLon) {
      return false;
    }
    
    boolean inPoly = false;
    boolean previous = polyLats[0] > latitude;
    for (int i = 1; i < polyLats.length; i++) {
      boolean current = polyLats[i] > latitude;
      if (current != previous) {
        if (longitude < (polyLons[i-1] - polyLons[i]) * (latitude - polyLats[i]) / (polyLats[i-1] - polyLats[i]) + polyLons[i]) {
          inPoly = !inPoly;
        }
        previous = current;
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
  
  /** Returns relation to the provided rectangle */
  public Relation relate(double minLat, double maxLat, double minLon, double maxLon) {
    // if the bounding boxes are disjoint then the shape does not cross
    if (maxLon < this.minLon || minLon > this.maxLon || maxLat < this.minLat || minLat > this.maxLat) {
      return Relation.CELL_OUTSIDE_QUERY;
    }
    // if the rectangle fully encloses us, we cross.
    if (minLat <= this.minLat && maxLat >= this.maxLat && minLon <= this.minLon && maxLon >= this.maxLon) {
      return Relation.CELL_CROSSES_QUERY;
    }
    // check any holes
    for (Polygon hole : holes) {
      Relation holeRelation = hole.relate(minLat, maxLat, minLon, maxLon);
      if (holeRelation == Relation.CELL_CROSSES_QUERY) {
        return Relation.CELL_CROSSES_QUERY;
      } else if (holeRelation == Relation.CELL_INSIDE_QUERY) {
        return Relation.CELL_OUTSIDE_QUERY;
      }
    }
    // check each corner: if < 4 are present, its cheaper than crossesSlowly
    int numCorners = numberOfCorners(minLat, maxLat, minLon, maxLon);
    if (numCorners == 4) {
      if (crossesSlowly(minLat, maxLat, minLon, maxLon)) {
        return Relation.CELL_CROSSES_QUERY;
      }
      return Relation.CELL_INSIDE_QUERY;
    } else if (numCorners > 0) {
      return Relation.CELL_CROSSES_QUERY;
    }
    
    // we cross
    if (crossesSlowly(minLat, maxLat, minLon, maxLon)) {
      return Relation.CELL_CROSSES_QUERY;
    }
    
    return Relation.CELL_OUTSIDE_QUERY;
  }
  
  // returns 0, 4, or something in between
  private int numberOfCorners(double minLat, double maxLat, double minLon, double maxLon) {
    int containsCount = 0;
    if (contains(minLat, minLon)) {
      containsCount++;
    }
    if (contains(minLat, maxLon)) {
      containsCount++;
    }
    if (containsCount == 1) {
      return containsCount;
    }
    if (contains(maxLat, maxLon)) {
      containsCount++;
    }
    if (containsCount == 2) {
      return containsCount;
    }
    if (contains(maxLat, minLon)) {
      containsCount++;
    }
    return containsCount;
  }

  private boolean crossesSlowly(double minLat, double maxLat, final double minLon, final double maxLon) {
    /*
     * Accurately compute (within restrictions of cartesian decimal degrees) whether a rectangle crosses a polygon
     */
    final double[] boxLats = new double[] { minLat, minLat, maxLat, maxLat, minLat };
    final double[] boxLons = new double[] { minLon, maxLon, maxLon, minLon, minLon };

    // computes the intersection point between each bbox edge and the polygon edge
    for (int b=0; b<4; ++b) {
      double a1 = boxLats[b+1]-boxLats[b];
      double b1 = boxLons[b]-boxLons[b+1];
      double c1 = a1*boxLons[b+1] + b1*boxLats[b+1];
      for (int p=0; p<polyLons.length-1; ++p) {
        double a2 = polyLats[p+1]-polyLats[p];
        double b2 = polyLons[p]-polyLons[p+1];
        // compute determinant
        double d = a1*b2 - a2*b1;
        if (d != 0) {
          // lines are not parallel, check intersecting points
          double c2 = a2*polyLons[p+1] + b2*polyLats[p+1];
          double s = (1/d)*(b2*c1 - b1*c2);
          // todo TOLERANCE SHOULD MATCH EVERYWHERE this is currently blocked by LUCENE-7165
          double x00 = Math.min(boxLons[b], boxLons[b+1]) - ENCODING_TOLERANCE;
          if (x00 > s) {
            continue; // out of range
          }
          double x01 = Math.max(boxLons[b], boxLons[b+1]) + ENCODING_TOLERANCE;
          if (x01 < s) {
            continue; // out of range
          }
          double x10 = Math.min(polyLons[p], polyLons[p+1]) - ENCODING_TOLERANCE;
          if (x10 > s) {
            continue; // out of range
          }
          double x11 = Math.max(polyLons[p], polyLons[p+1]) + ENCODING_TOLERANCE;
          if (x11 < s) {
            continue; // out of range
          }

          double t = (1/d)*(a1*c2 - a2*c1);
          double y00 = Math.min(boxLats[b], boxLats[b+1]) - ENCODING_TOLERANCE;
          if (y00 > t || (x00 == s && y00 == t)) {
            continue; // out of range or touching
          }
          double y01 = Math.max(boxLats[b], boxLats[b+1]) + ENCODING_TOLERANCE;
          if (y01 < t || (x01 == s && y01 == t)) {
            continue; // out of range or touching
          }
          double y10 = Math.min(polyLats[p], polyLats[p+1]) - ENCODING_TOLERANCE;
          if (y10 > t || (x10 == s && y10 == t)) {
            continue; // out of range or touching
          }
          double y11 = Math.max(polyLats[p], polyLats[p+1]) + ENCODING_TOLERANCE;
          if (y11 < t || (x11 == s && y11 == t)) {
            continue; // out of range or touching
          }
          // if line segments are not touching and the intersection point is within the range of either segment
          return true;
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

  /** Returns the multipolygon relation for the rectangle */
  public static Relation relate(Polygon[] polygons, double minLat, double maxLat, double minLon, double maxLon) {
    for (Polygon polygon : polygons) {
      Relation relation = polygon.relate(minLat, maxLat, minLon, maxLon);
      if (relation != Relation.CELL_OUTSIDE_QUERY) {
        // note: we optimize for non-overlapping multipolygons. so if we cross one,
        // we won't keep iterating to try to find a contains.
        return relation;
      }
    }
    return Relation.CELL_OUTSIDE_QUERY;
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
