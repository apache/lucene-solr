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

  /** Returns true if the box crosses our polygon */
  private boolean crossesSlowly(double minLat, double maxLat, double minLon, double maxLon) {
    // we compute line intersections of every polygon edge with every box line.
    // if we find one, return true.
    // for each box line (AB):
    //   for each poly line (CD):
    //     intersects = orient(C,D,A) * orient(C,D,B) <= 0 && orient(A,B,C) * orient(A,B,D) <= 0
    for (int i = 1; i < polyLons.length; i++) {
      double cy = polyLats[i - 1];
      double dy = polyLats[i];
      double cx = polyLons[i - 1];
      double dx = polyLons[i];

      // optimization: see if the rectangle is outside of the "bounding box" of the polyline at all
      // if not, don't waste our time trying more complicated stuff
      if ((cy < minLat && dy < minLat) ||
          (cy > maxLat && dy > maxLat) ||
          (cx < minLon && dx < minLon) ||
          (cx > maxLon && dx > maxLon)) {
        continue;
      }

      // does box's top edge intersect polyline?
      // ax = minLon, bx = maxLon, ay = maxLat, by = maxLat
      if (orient(cx, cy, dx, dy, minLon, maxLat) * orient(cx, cy, dx, dy, maxLon, maxLat) <= 0 &&
          orient(minLon, maxLat, maxLon, maxLat, cx, cy) * orient(minLon, maxLat, maxLon, maxLat, dx, dy) <= 0) {
        return true;
      }

      // does box's right edge intersect polyline?
      // ax = maxLon, bx = maxLon, ay = maxLat, by = minLat
      if (orient(cx, cy, dx, dy, maxLon, maxLat) * orient(cx, cy, dx, dy, maxLon, minLat) <= 0 &&
          orient(maxLon, maxLat, maxLon, minLat, cx, cy) * orient(maxLon, maxLat, maxLon, minLat, dx, dy) <= 0) {
        return true;
      }

      // does box's bottom edge intersect polyline?
      // ax = maxLon, bx = minLon, ay = minLat, by = minLat
      if (orient(cx, cy, dx, dy, maxLon, minLat) * orient(cx, cy, dx, dy, minLon, minLat) <= 0 &&
          orient(maxLon, minLat, minLon, minLat, cx, cy) * orient(maxLon, minLat, minLon, minLat, dx, dy) <= 0) {
        return true;
      }

      // does box's left edge intersect polyline?
      // ax = minLon, bx = minLon, ay = minLat, by = maxLat
      if (orient(cx, cy, dx, dy, minLon, minLat) * orient(cx, cy, dx, dy, minLon, maxLat) <= 0 &&
          orient(minLon, minLat, minLon, maxLat, cx, cy) * orient(minLon, minLat, minLon, maxLat, dx, dy) <= 0) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns a positive value if points a, b, and c are arranged in counter-clockwise order,
   * negative value if clockwise, zero if collinear.
   */
  // see the "Orient2D" method described here:
  // http://www.cs.berkeley.edu/~jrs/meshpapers/robnotes.pdf
  // https://www.cs.cmu.edu/~quake/robust.html
  // Note that this one does not yet have the floating point tricks to be exact!
  private static int orient(double ax, double ay, double bx, double by, double cx, double cy) {
    double v1 = (bx - ax) * (cy - ay);
    double v2 = (cx - ax) * (by - ay);
    if (v1 > v2) {
      return 1;
    } else if (v1 < v2) {
      return -1;
    } else {
      return 0;
    }
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
