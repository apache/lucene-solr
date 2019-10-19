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
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.lucene.geo.GeoUtils.lineCrossesLine;
import static org.apache.lucene.geo.GeoUtils.lineCrossesLineWithBoundary;
import static org.apache.lucene.geo.GeoUtils.orient;

/**
 * 2D line/polygon geometry implementation represented as a balanced interval tree of edges.
 * <p>
 * Construction takes {@code O(n log n)} time for sorting and tree construction.
 * {@link #relate relate()} are {@code O(n)}, but for most
 * practical lines and polygons are much faster than brute force.
 * @lucene.internal
 */
/**
 * Internal tree node: represents geometry edge from lat1,lon1 to lat2,lon2.
 * The sort value is {@code low}, which is the minimum latitude of the edge.
 * {@code max} stores the maximum latitude of this edge or any children.
 *
 * @lucene.internal
 */
public  class EdgeTree {
    // lat-lon pair (in original order) of the two vertices
    final double y1, y2;
    final double x1, x2;
    /** min of this edge */
    final double low;
    /** max latitude of this edge or any children */
    double max;
    /** left child edge, or null */
    EdgeTree left;
    /** right child edge, or null */
    EdgeTree right;

  EdgeTree(double x1, double y1, double x2, double y2, double low, double max) {
      this.y1 = y1;
      this.x1 = x1;
      this.y2 = y2;
      this.x2 = x2;
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
  protected boolean contains(double x, double y, AtomicBoolean isOnEdge) {
    boolean res = false;
    if (isOnEdge.get() == false && y <= this.max) {
      if (y == this.y1 && y == this.y2 ||
          (y <= this.y1 && y >= this.y2) != (y >= this.y1 && y <= this.y2)) {
        if ((x == this.x1 && x == this.x2) ||
            ((x <= this.x1 && x >= this.x2) != (x >= this.x1 && x <= this.x2) &&
                GeoUtils.orient(this.x1, this.y1, this.x2, this.y2, x, y) == 0)) {
          // if its on the boundary return true
          isOnEdge.set(true);
          return true;
        } else if (this.y1 > y != this.y2 > y) {
          res = x < (this.x2 - this.x1) * (y - this.y1) / (this.y2 - this.y1) + this.x1;
        }
      }
      if (this.left != null) {
        res ^= left.contains(x, y, isOnEdge);
      }

      if (this.right != null && y >= this.low) {
        res ^= right.contains(x, y, isOnEdge);
      }
    }
    return isOnEdge.get() || res;
  }

  /** returns true if the provided x, y point lies on the line */
  protected boolean isPointOnLine(double x, double y) {
    if (y <= max) {
      double a1x = x1;
      double a1y = y1;
      double b1x = x2;
      double b1y = y2;
      boolean outside = (a1y < y && b1y < y) ||
          (a1y > y && b1y > y) ||
          (a1x < x && b1x < x) ||
          (a1x > x && b1x > x);
      if (outside == false && orient(a1x, a1y, b1x, b1y, x, y) == 0) {
        return true;
      }
      if (left != null && left.isPointOnLine(x, y)) {
        return true;
      }
      if (right != null && y >= this.low && right.isPointOnLine(x, y)) {
        return true;
      }
    }
    return false;
  }


  /** Returns true if the triangle crosses any edge in this edge subtree */
  protected boolean crossesTriangle(double minX, double maxX, double minY, double maxY,
                          double ax, double ay, double bx, double by, double cx, double cy) {
      if (minY <= max) {
        double dy = y1;
        double ey = y2;
        double dx = x1;
        double ex = x2;

        // optimization: see if the rectangle is outside of the "bounding box" of the polyline at all
        // if not, don't waste our time trying more complicated stuff
        boolean outside = (dy < minY && ey < minY) ||
            (dy > maxY && ey > maxY) ||
            (dx < minX && ex < minX) ||
            (dx > maxX && ex > maxX);

        if (outside == false) {
          if (lineCrossesLine(dx, dy, ex, ey, ax, ay, bx, by) ||
              lineCrossesLine(dx, dy, ex, ey, bx, by, cx, cy) ||
              lineCrossesLine(dx, dy, ex, ey, cx, cy, ax, ay)) {
            return true;
          }
        }

        if (left != null && left.crossesTriangle(minX, maxX, minY, maxY, ax, ay, bx, by, cx, cy)) {
          return true;
        }

        if (right != null && maxY >= low && right.crossesTriangle(minX, maxX, minY, maxY, ax, ay, bx, by, cx, cy)) {
          return true;
        }
      }
      return false;
    }

  /** Returns true if the box crosses any edge in this edge subtree */
  protected boolean crossesBox(double minX, double maxX, double minY, double maxY, boolean includeBoundary) {
    // we just have to cross one edge to answer the question, so we descend the tree and return when we do.
    if (minY <= max) {
      // we compute line intersections of every polygon edge with every box line.
      // if we find one, return true.
      // for each box line (AB):
      //   for each poly line (CD):
      //     intersects = orient(C,D,A) * orient(C,D,B) <= 0 && orient(A,B,C) * orient(A,B,D) <= 0
      double cy = y1;
      double dy = y2;
      double cx = x1;
      double dx = x2;

      // optimization: see if either end of the line segment is contained by the rectangle
      if (Rectangle.containsPoint(cy, cx, minY, maxY, minX, maxX) ||
          Rectangle.containsPoint(dy, dx, minY, maxY, minX, maxX)) {
        return true;
      }

      // optimization: see if the rectangle is outside of the "bounding box" of the polyline at all
      // if not, don't waste our time trying more complicated stuff
      boolean outside = (cy < minY && dy < minY) ||
          (cy > maxY && dy > maxY) ||
          (cx < minX && dx < minX) ||
          (cx > maxX && dx > maxX);

      if (outside == false) {
        if (includeBoundary == true &&
            lineCrossesLineWithBoundary(cx, cy, dx, dy, minX, minY, maxX, minY) ||
            lineCrossesLineWithBoundary(cx, cy, dx, dy, maxX, minY, maxX, maxY) ||
            lineCrossesLineWithBoundary(cx, cy, dx, dy, maxX, maxY, minX, maxY) ||
            lineCrossesLineWithBoundary(cx, cy, dx, dy, minX, maxY, minX, minY)) {
          // include boundaries: ensures box edges that terminate on the polygon are included
          return true;
        } else if (lineCrossesLine(cx, cy, dx, dy, minX, minY, maxX, minY) ||
            lineCrossesLine(cx, cy, dx, dy, maxX, minY, maxX, maxY) ||
            lineCrossesLine(cx, cy, dx, dy, maxX, maxY, minX, maxY) ||
            lineCrossesLine(cx, cy, dx, dy, minX, maxY, minX, minY)) {
          return true;
        }
      }

      if (left != null && left.crossesBox(minX, maxX, minY, maxY, includeBoundary)) {
        return true;
      }

      if (right != null && maxY >= low && right.crossesBox(minX, maxX, minY, maxY, includeBoundary)) {
        return true;
      }
    }
    return false;
  }

  /** Returns true if the line crosses any edge in this edge subtree */
  protected boolean crossesLine(double minX, double maxX, double minY, double maxY, double a2x, double a2y, double b2x, double b2y) {
    if (minY <= max) {
      double a1x = x1;
      double a1y = y1;
      double b1x = x2;
      double b1y = y2;

      boolean outside = (a1y < minY && b1y < minY) ||
          (a1y > maxY && b1y > maxY) ||
          (a1x < minX && b1x < minX) ||
          (a1x > maxX && b1x > maxX);
      if (outside == false && lineCrossesLineWithBoundary(a1x, a1y, b1x, b1y, a2x, a2y, b2x, b2y)) {
        return true;
      }

      if (left != null && left.crossesLine(minX, maxX, minY, maxY, a2x, a2y, b2x, b2y)) {
        return true;
      }
      if (right != null && maxY >= low && right.crossesLine(minX, maxX, minY, maxY, a2x, a2y, b2x, b2y)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Creates an edge interval tree from a set of geometry vertices.
   * @return root node of the tree.
   */
  protected static EdgeTree createTree(double[] x, double[] y) {
    EdgeTree edges[] = new EdgeTree[x.length - 1];
    for (int i = 1; i < x.length; i++) {
      double x1 = x[i-1];
      double y1 = y[i-1];
      double x2 = x[i];
      double y2 = y[i];
      edges[i - 1] = new EdgeTree(x1, y1, x2, y2, Math.min(y1, y2), Math.max(y1, y2));
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
  private static EdgeTree createTree(EdgeTree edges[], int low, int high) {
    if (low > high) {
      return null;
    }
    // add midpoint
    int mid = (low + high) >>> 1;
    EdgeTree newNode = edges[mid];
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
}
