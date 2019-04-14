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

import java.util.concurrent.atomic.AtomicBoolean;

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
  private final AtomicBoolean containsBoundary = new AtomicBoolean(false);

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
    containsBoundary.set(false);
    if (contains(tree, latitude, longitude, containsBoundary)) {
      if (holes != null && holes.contains(latitude, longitude)) {
        return false;
      }
      return true;
    }
    return false;
  }

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
    // check each corner: if < 4 && > 0 are present, its cheaper than crossesSlowly
    int numCorners = numberOfCorners(minLat, maxLat, minLon, maxLon);
    if (numCorners == 4) {
      if (tree.crossesBox(minLat, maxLat, minLon, maxLon, false)) {
        return Relation.CELL_CROSSES_QUERY;
      }
      return Relation.CELL_INSIDE_QUERY;
    }  else if (numCorners == 0) {
      if (minLat >= tree.lat1 && maxLat <= tree.lat1 && minLon >= tree.lon2 && maxLon <= tree.lon2) {
        return Relation.CELL_CROSSES_QUERY;
      }
      if (tree.crossesBox(minLat, maxLat, minLon, maxLon, false)) {
        return Relation.CELL_CROSSES_QUERY;
      }
      return Relation.CELL_OUTSIDE_QUERY;
    }
    return Relation.CELL_CROSSES_QUERY;
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
    if (ax == bx && bx == cx && ay == by && by == cy) {
      // indexed "triangle" is a point:
      if (Rectangle.containsPoint(ay, ax, minLat, maxLat, minLon, maxLon) == false) {
        return Relation.CELL_OUTSIDE_QUERY;
      }
      // shortcut by checking contains
      return contains(ay, ax) ? Relation.CELL_INSIDE_QUERY : Relation.CELL_OUTSIDE_QUERY;
    } else if (ax == cx && ay == cy) {
      // indexed "triangle" is a line segment: shortcut by calling appropriate method
      return relateIndexedLineSegment(ax, ay, bx, by);
    }
    // indexed "triangle" is a triangle:
    return relateIndexedTriangle(ax, ay, bx, by, cx, cy);
  }

  /** relates an indexed line segment (a "flat triangle") with the polygon */
  private Relation relateIndexedLineSegment(double a2x, double a2y, double b2x, double b2y) {
    // check endpoints of the line segment
    int numCorners = 0;
    if (componentContains(a2y, a2x)) {
      ++numCorners;
    }
    if (componentContains(b2y, b2x)) {
      ++numCorners;
    }

    if (numCorners == 2) {
      if (tree.crossesLine(a2x, a2y, b2x, b2y)) {
        return Relation.CELL_CROSSES_QUERY;
      }
      return Relation.CELL_INSIDE_QUERY;
    } else if (numCorners == 0) {
      if (tree.crossesLine(a2x, a2y, b2x, b2y)) {
        return Relation.CELL_CROSSES_QUERY;
      }
      return Relation.CELL_OUTSIDE_QUERY;
    }
    return Relation.CELL_CROSSES_QUERY;
  }

  /** relates an indexed triangle with the polygon */
  private Relation relateIndexedTriangle(double ax, double ay, double bx, double by, double cx, double cy) {
    // check each corner: if < 3 && > 0 are present, its cheaper than crossesSlowly
    int numCorners = numberOfTriangleCorners(ax, ay, bx, by, cx, cy);
    if (numCorners == 3) {
      if (tree.crossesTriangle(ax, ay, bx, by, cx, cy)) {
        return Relation.CELL_CROSSES_QUERY;
      }
      return Relation.CELL_INSIDE_QUERY;
    } else if (numCorners == 0) {
      if (pointInTriangle(tree.lon1, tree.lat1, ax, ay, bx, by, cx, cy) == true) {
        return Relation.CELL_CROSSES_QUERY;
      }
      if (tree.crossesTriangle(ax, ay, bx, by, cx, cy)) {
        return Relation.CELL_CROSSES_QUERY;
      }
      return Relation.CELL_OUTSIDE_QUERY;
    }
    return Relation.CELL_CROSSES_QUERY;
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
  private static boolean contains(Edge edge, double lat, double lon, AtomicBoolean isOnEdge) {
    boolean res = false;
    if (isOnEdge.get() == false && lat <= edge.max) {
      if (lat == edge.lat1 && lat == edge.lat2 ||
          (lat <= edge.lat1 && lat >= edge.lat2) != (lat >= edge.lat1 && lat <= edge.lat2)) {
        if (GeoUtils.orient(edge.lon1, edge.lat1, edge.lon2, edge.lat2, lon, lat) == 0) {
          // if its on the boundary return true
          isOnEdge.set(true);
          return true;
        } else if (edge.lat1 > lat != edge.lat2 > lat) {
          res = lon < (edge.lon2 - edge.lon1) * (lat - edge.lat1) / (edge.lat2 - edge.lat1) + edge.lon1;
        }
      }
      if (edge.left != null) {
        res ^= contains(edge.left, lat, lon, isOnEdge);
      }

      if (edge.right != null && lat >= edge.low) {
        res ^= contains(edge.right, lat, lon, isOnEdge);
      }
    }
    return isOnEdge.get() || res;
  }
}
