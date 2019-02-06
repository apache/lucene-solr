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

import org.apache.lucene.index.PointValues.Relation;

/**
 * 2D polygon implementation represented as a balanced interval tree of edges.
 * <p>
 * Loosely based on the algorithm described in <a href="http://www-ma2.upc.es/geoc/Schirra-pointPolygon.pdf">
 * http://www-ma2.upc.es/geoc/Schirra-pointPolygon.pdf</a>.
 * @lucene.internal
 */
// Both Polygon.contains() and Polygon.crossesSlowly() loop all edges, and first check that the edge is within a range.
// we just organize the edges to do the same computations on the same subset of edges more efficiently.
public final class Polygon2D extends EdgeTree {
  // each component/hole is a node in an augmented 2d kd-tree: we alternate splitting between latitude/longitude,
  // and pull up max values for both dimensions to each parent node (regardless of split).
  /** tree of holes, or null */
  private final Polygon2D holes;

  private Polygon2D(Polygon polygon, Polygon2D holes) {
    super(polygon.minLat, polygon.maxLat, polygon.minLon, polygon.maxLon, polygon.getPolyLats(), polygon.getPolyLons());
    this.holes = holes;
  }

  /**
   * Returns true if the point is contained within this polygon.
   * <p>
   * See <a href="https://www.ecse.rpi.edu/~wrf/Research/Short_Notes/pnpoly.html">
   * https://www.ecse.rpi.edu/~wrf/Research/Short_Notes/pnpoly.html</a> for more information.
   */
  public boolean contains(double latitude, double longitude) {
    if (latitude <= maxY && longitude <= maxX) {
      if (componentContains(latitude, longitude)) {
        return true;
      }
      if (left != null) {
        if (((Polygon2D)left).contains(latitude, longitude)) {
          return true;
        }
      }
      if (right != null && ((splitX == false && latitude >= minLat) || (splitX && longitude >= minLon))) {
        if (((Polygon2D)right).contains(latitude, longitude)) {
          return true;
        }
      }
    }
    return false;
  }

  /** Returns true if the point is contained within this polygon component. */
  private boolean componentContains(double latitude, double longitude) {
    // check bounding box
    if (latitude < minLat || latitude > maxLat || longitude < minLon || longitude > maxLon) {
      return false;
    }
    if (contains(tree, latitude, longitude)) {
      if (holes != null && holes.contains(latitude, longitude)) {
        return false;
      }
      return true;
    }
    return false;
  }

  @Override
  protected Relation componentRelateTriangle(double ax, double ay, double bx, double by, double cx, double cy) {
    // check any holes
    if (holes != null) {
      Relation holeRelation = holes.relateTriangle(ax, ay, bx, by, cx, cy);
      if (holeRelation == Relation.CELL_CROSSES_QUERY) {
        return Relation.CELL_CROSSES_QUERY;
      } else if (holeRelation == Relation.CELL_INSIDE_QUERY) {
        return Relation.CELL_OUTSIDE_QUERY;
      }
    }
    // check each corner: if < 3 are present, its cheaper than crossesSlowly
    int numCorners = numberOfTriangleCorners(ax, ay, bx, by, cx, cy);
    if (numCorners == 3) {
      if (tree.relateTriangle(ax, ay, bx, by, cx, cy) == Relation.CELL_CROSSES_QUERY) {
        return Relation.CELL_CROSSES_QUERY;
      }
      return Relation.CELL_INSIDE_QUERY;
    } else if (numCorners > 0) {
      return Relation.CELL_CROSSES_QUERY;
    }
    return null;
  }

  /** Returns relation to the provided rectangle for this component */
  @Override
  protected Relation componentRelate(double minLat, double maxLat, double minLon, double maxLon) {
    // check any holes
    if (holes != null) {
      Relation holeRelation = holes.relate(minLat, maxLat, minLon, maxLon);
      if (holeRelation == Relation.CELL_CROSSES_QUERY) {
        return Relation.CELL_CROSSES_QUERY;
      } else if (holeRelation == Relation.CELL_INSIDE_QUERY) {
        return Relation.CELL_OUTSIDE_QUERY;
      }
    }
    // check each corner: if < 4 are present, its cheaper than crossesSlowly
    int numCorners = numberOfCorners(minLat, maxLat, minLon, maxLon);
    if (numCorners == 4) {
      if (tree.crosses(minLat, maxLat, minLon, maxLon)) {
        return Relation.CELL_CROSSES_QUERY;
      }
      return Relation.CELL_INSIDE_QUERY;
    } else if (numCorners > 0) {
      return Relation.CELL_CROSSES_QUERY;
    }
    return null;
  }

  private int numberOfTriangleCorners(double ax, double ay, double bx, double by, double cx, double cy) {
    int containsCount = 0;
    if (componentContains(ay, ax)) {
      containsCount++;
    }
    if (componentContains(by, bx)) {
      containsCount++;
    }
    if (containsCount == 1) {
      return containsCount;
    }
    if (componentContains(cy, cx)) {
      containsCount++;
    }
    return containsCount;
  }

  // returns 0, 4, or something in between
  private int numberOfCorners(double minLat, double maxLat, double minLon, double maxLon) {
    int containsCount = 0;
    if (componentContains(minLat, minLon)) {
      containsCount++;
    }
    if (componentContains(minLat, maxLon)) {
      containsCount++;
    }
    if (containsCount == 1) {
      return containsCount;
    }
    if (componentContains(maxLat, maxLon)) {
      containsCount++;
    }
    if (containsCount == 2) {
      return containsCount;
    }
    if (componentContains(maxLat, minLon)) {
      containsCount++;
    }
    return containsCount;
  }

  /** Builds a Polygon2D from multipolygon */
  public static Polygon2D create(Polygon... polygons) {
    Polygon2D components[] = new Polygon2D[polygons.length];
    for (int i = 0; i < components.length; i++) {
      Polygon gon = polygons[i];
      Polygon gonHoles[] = gon.getHoles();
      Polygon2D holes = null;
      if (gonHoles.length > 0) {
        holes = create(gonHoles);
      }
      components[i] = new Polygon2D(gon, holes);
    }
    return (Polygon2D)createTree(components, 0, components.length - 1, false);
  }

  /**
   * Returns true if the point crosses this edge subtree an odd number of times
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
  private static boolean contains(Edge tree, double latitude, double longitude) {
    // crossings algorithm is an odd-even algorithm, so we descend the tree xor'ing results along our path
    boolean res = false;
    if (latitude <= tree.max) {
      if (tree.lat1 > latitude != tree.lat2 > latitude) {
        if (longitude < (tree.lon1 - tree.lon2) * (latitude - tree.lat2) / (tree.lat1 - tree.lat2) + tree.lon2) {
          res = true;
        }
      }
      if (tree.left != null) {
        res ^= contains(tree.left, latitude, longitude);
      }
      if (tree.right != null && latitude >= tree.low) {
        res ^= contains(tree.right, latitude, longitude);
      }
    }
    return res;
  }
}
