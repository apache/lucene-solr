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
import java.util.Comparator;

import org.apache.lucene.geo.Polygon;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.util.ArrayUtil;

/**
 * 2D polygon implementation represented as a balanced interval tree of edges.
 * <p>
 * Construction takes {@code O(n log n)} time for sorting and tree construction.
 * {@link #contains contains()} and {@link #relate relate()} are {@code O(n)}, but for most 
 * practical polygons are much faster than brute force.
 * <p>
 * Loosely based on the algorithm described in <a href="http://www-ma2.upc.es/geoc/Schirra-pointPolygon.pdf">
 * http://www-ma2.upc.es/geoc/Schirra-pointPolygon.pdf</a>.
 * @lucene.internal
 */
// Both Polygon.contains() and Polygon.crossesSlowly() loop all edges, and first check that the edge is within a range.
// we just organize the edges to do the same computations on the same subset of edges more efficiently. 
public final class Polygon2D {
  /** minimum latitude of this polygon's bounding box area */
  public final double minLat;
  /** maximum latitude of this polygon's bounding box area */
  public final double maxLat;
  /** minimum longitude of this polygon's bounding box area */
  public final double minLon;
  /** maximum longitude of this polygon's bounding box area */
  public final double maxLon;
  
  // each component/hole is a node in an augmented 2d kd-tree: we alternate splitting between latitude/longitude,
  // and pull up max values for both dimensions to each parent node (regardless of split).

  /** maximum latitude of this component or any of its children */
  private double maxY;
  /** maximum longitude of this component or any of its children */
  private double maxX;
  /** which dimension was this node split on */
  // TODO: its implicit based on level, but boolean keeps code simple
  private boolean splitX;

  // child components, or null
  private Polygon2D left;
  private Polygon2D right;
  
  /** tree of holes, or null */
  private final Polygon2D holes;
  
  /** root node of edge tree */
  private final Edge tree;

  private Polygon2D(Polygon polygon, Polygon2D holes) {
    this.holes = holes;
    this.minLat = polygon.minLat;
    this.maxLat = polygon.maxLat;
    this.minLon = polygon.minLon;
    this.maxLon = polygon.maxLon;
    this.maxY = maxLat;
    this.maxX = maxLon;
    
    // create interval tree of edges
    this.tree = createTree(polygon.getPolyLats(), polygon.getPolyLons());
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
        if (left.contains(latitude, longitude)) {
          return true;
        }
      }
      if (right != null && ((splitX == false && latitude >= minLat) || (splitX && longitude >= minLon))) {
        if (right.contains(latitude, longitude)) {
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
    
    if (tree.contains(latitude, longitude)) {
      if (holes != null && holes.contains(latitude, longitude)) {
        return false;
      }
      return true;
    }
    
    return false;
  }
  
  /** Returns relation to the provided rectangle */
  public Relation relate(double minLat, double maxLat, double minLon, double maxLon) {
    if (minLat <= maxY && minLon <= maxX) {
      Relation relation = componentRelate(minLat, maxLat, minLon, maxLon);
      if (relation != Relation.CELL_OUTSIDE_QUERY) {
        return relation;
      }
      if (left != null) {
        relation = left.relate(minLat, maxLat, minLon, maxLon);
        if (relation != Relation.CELL_OUTSIDE_QUERY) {
          return relation;
        }
      }
      if (right != null && ((splitX == false && maxLat >= this.minLat) || (splitX && maxLon >= this.minLon))) {
        relation = right.relate(minLat, maxLat, minLon, maxLon);
        if (relation != Relation.CELL_OUTSIDE_QUERY) {
          return relation;
        }
      }
    }
    return Relation.CELL_OUTSIDE_QUERY;
  }

  /** Returns relation to the provided rectangle for this component */
  private Relation componentRelate(double minLat, double maxLat, double minLon, double maxLon) {
    // if the bounding boxes are disjoint then the shape does not cross
    if (maxLon < this.minLon || minLon > this.maxLon || maxLat < this.minLat || minLat > this.maxLat) {
      return Relation.CELL_OUTSIDE_QUERY;
    }
    // if the rectangle fully encloses us, we cross.
    if (minLat <= this.minLat && maxLat >= this.maxLat && minLon <= this.minLon && maxLon >= this.maxLon) {
      return Relation.CELL_CROSSES_QUERY;
    }
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
    
    // we cross
    if (tree.crosses(minLat, maxLat, minLon, maxLon)) {
      return Relation.CELL_CROSSES_QUERY;
    }
    
    return Relation.CELL_OUTSIDE_QUERY;
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
  
  /** Creates tree from sorted components (with range low and high inclusive) */
  private static Polygon2D createTree(Polygon2D components[], int low, int high, boolean splitX) {
    if (low > high) {
      return null;
    }
    final int mid = (low + high) >>> 1;
    if (low < high) {
      Comparator<Polygon2D> comparator;
      if (splitX) {
        comparator = (left, right) -> {
          int ret = Double.compare(left.minLon, right.minLon);
          if (ret == 0) {
            ret = Double.compare(left.maxX, right.maxX);
          }
          return ret;
        };
      } else {
        comparator = (left, right) -> {
          int ret = Double.compare(left.minLat, right.minLat);
          if (ret == 0) {
            ret = Double.compare(left.maxY, right.maxY);
          }
          return ret;
        };
      }
      ArrayUtil.select(components, low, high + 1, mid, comparator);
    }
    // add midpoint
    Polygon2D newNode = components[mid];
    newNode.splitX = splitX;
    // add children
    newNode.left = createTree(components, low, mid - 1, !splitX);
    newNode.right = createTree(components, mid + 1, high, !splitX);
    // pull up max values to this node
    if (newNode.left != null) {
      newNode.maxX = Math.max(newNode.maxX, newNode.left.maxX);
      newNode.maxY = Math.max(newNode.maxY, newNode.left.maxY);
    }
    if (newNode.right != null) {
      newNode.maxX = Math.max(newNode.maxX, newNode.right.maxX);
      newNode.maxY = Math.max(newNode.maxY, newNode.right.maxY);
    }
    return newNode;
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
    return createTree(components, 0, components.length - 1, false);
  }

  /** 
   * Internal tree node: represents polygon edge from lat1,lon1 to lat2,lon2.
   * The sort value is {@code low}, which is the minimum latitude of the edge.
   * {@code max} stores the maximum latitude of this edge or any children.
   */
  static final class Edge {
    // lat-lon pair (in original order) of the two vertices
    final double lat1, lat2;
    final double lon1, lon2;
    /** min of this edge */
    final double low;
    /** max latitude of this edge or any children */
    double max;
    
    /** left child edge, or null */
    Edge left;
    /** right child edge, or null */
    Edge right;

    Edge(double lat1, double lon1, double lat2, double lon2, double low, double max) {
      this.lat1 = lat1;
      this.lon1 = lon1;
      this.lat2 = lat2;
      this.lon2 = lon2;
      this.low = low;
      this.max = max;
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
    boolean contains(double latitude, double longitude) {
      // crossings algorithm is an odd-even algorithm, so we descend the tree xor'ing results along our path
      boolean res = false;
      if (latitude <= max) {
        if (lat1 > latitude != lat2 > latitude) {
          if (longitude < (lon1 - lon2) * (latitude - lat2) / (lat1 - lat2) + lon2) {
            res = true;
          }
        }
        if (left != null) {
          res ^= left.contains(latitude, longitude);
        }
        if (right != null && latitude >= low) {
          res ^= right.contains(latitude, longitude);
        }
      }
      return res;
    }
    
    /** Returns true if the box crosses any edge in this edge subtree */
    boolean crosses(double minLat, double maxLat, double minLon, double maxLon) {
      // we just have to cross one edge to answer the question, so we descend the tree and return when we do.
      if (minLat <= max) {
        // we compute line intersections of every polygon edge with every box line.
        // if we find one, return true.
        // for each box line (AB):
        //   for each poly line (CD):
        //     intersects = orient(C,D,A) * orient(C,D,B) <= 0 && orient(A,B,C) * orient(A,B,D) <= 0
        double cy = lat1;
        double dy = lat2;
        double cx = lon1;
        double dx = lon2;
        
        // optimization: see if the rectangle is outside of the "bounding box" of the polyline at all
        // if not, don't waste our time trying more complicated stuff
        boolean outside = (cy < minLat && dy < minLat) ||
                          (cy > maxLat && dy > maxLat) ||
                          (cx < minLon && dx < minLon) ||
                          (cx > maxLon && dx > maxLon);
        if (outside == false) {
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
        
        if (left != null) {
          if (left.crosses(minLat, maxLat, minLon, maxLon)) {
            return true;
          }
        }
        
        if (right != null && maxLat >= low) {
          if (right.crosses(minLat, maxLat, minLon, maxLon)) {
            return true;
          }
        }
      }
      return false;
    }
  }

  /** 
   * Creates an edge interval tree from a set of polygon vertices.
   * @return root node of the tree.
   */
  private static Edge createTree(double polyLats[], double polyLons[]) {
    Edge edges[] = new Edge[polyLats.length - 1];
    for (int i = 1; i < polyLats.length; i++) {
      double lat1 = polyLats[i-1];
      double lon1 = polyLons[i-1];
      double lat2 = polyLats[i];
      double lon2 = polyLons[i];
      edges[i - 1] = new Edge(lat1, lon1, lat2, lon2, Math.min(lat1, lat2), Math.max(lat1, lat2));
    }
    // sort the edges then build a balanced tree from them
    Arrays.sort(edges, (left, right) -> {
      int ret = Double.compare(left.low, right.low);
      if (ret == 0) {
        ret = Double.compare(left.max, right.max);
      }
      return ret;
    });
    return createTree(edges, 0, edges.length - 1);
  }

  /** Creates tree from sorted edges (with range low and high inclusive) */
  private static Edge createTree(Edge edges[], int low, int high) {
    if (low > high) {
      return null;
    }
    // add midpoint
    int mid = (low + high) >>> 1;
    Edge newNode = edges[mid];
    // add children
    newNode.left = createTree(edges, low, mid - 1);
    newNode.right = createTree(edges, mid + 1, high);
    // pull up max values to this node
    if (newNode.left != null) {
      newNode.max = Math.max(newNode.max, newNode.left.max);
    }
    if (newNode.right != null) {
      newNode.max = Math.max(newNode.max, newNode.right.max);
    }
    return newNode;
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
}
