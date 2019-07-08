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
package org.apache.lucene.component2D;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.geo.GeoUtils;

import static org.apache.lucene.geo.GeoUtils.lineCrossesLine;
import static org.apache.lucene.geo.GeoUtils.lineCrossesLineWithBoundary;
import static org.apache.lucene.geo.GeoUtils.orient;

/**
 * 2D line/polygon geometry implementation represented as a balanced interval tree of edges.
 * <p>
 * Construction takes {@code O(n log n)} time for sorting and tree construction.
 * Crosses methods are {@code O(n)}, but for most
 * practical lines and polygons are much faster than brute force.
 * @lucene.internal
 */
class EdgeTree {
  final int x1, x2;
  final int y1, y2;

  /** min of this edge */
  final int low;
  /** max latitude of this edge or any children */
  int max;

  /** left child edge, or null */
  EdgeTree left;
  /** right child edge, or null */
  EdgeTree right;

  protected EdgeTree(final int x1, final int y1, final int x2, final int y2, final int low, final int max) {
    this.x1 = x1;
    this.y1 = y1;
    this.x2 = x2;
    this.y2 = y2;
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
  public boolean contains(int x, int y, AtomicBoolean isOnEdge) {
    // crossings algorithm is an odd-even algorithm, so we descend the tree xor'ing results along our path
    boolean res = false;
    if (isOnEdge.get() == false && y <= max) {
      if (y == y1 && y == y2 ||
          (y <= y1 && y >= y2) != (y >= y1 && y <= y2)) {
        if ((x == x1 && x == x2) ||
            ((x <= x1 && x >= x2) != (x >= x1 && x <= x2) &&
                GeoUtils.orient(x1, y1, x2, y2, x, y) == 0)) {
          // if its on the boundary return true
          isOnEdge.set(true);
          return true;
        } else if (y1 > y != y2 > y) {
          res = test(x, y, x1, y1, x2, y2);
        }
      }
      if (left != null) {
        res ^= left.contains(x, y, isOnEdge);
      }

      if (right != null && y >= low) {
        res ^= right.contains(x, y, isOnEdge);
      }
    }
    return isOnEdge.get() || res;
  }

  private static boolean test(double x, double y, double x1, double y1, double x2, double y2) {
    return x < (x2 - x1) * (y - y1) / (y2 - y1) + x1;
  }

  boolean crossesTriangle(int minX, int maxX, int minY, int maxY, int aX, int aY, int bX, int bY, int cX, int cY) {
    if (minY <= max) {
      int dX = x1;
      int dY = y1;
      int eX = x2;
      int eY = y2;

      // optimization: see if the rectangle is outside of the "bounding box" of the polyline at all
      // if not, don't waste our time trying more complicated stuff
      boolean outside = (dY < minY && eY < minY) ||
          (dY > maxY && eY > maxY) ||
          (dX < minX && eX < minX) ||
          (dX > maxX && eX > maxX);

      if (outside == false) {
        // does triangle's edges intersect polyline?
        if (lineCrossesLine(aX, aY, bX, bY, dX, dY, eX, eY) ||
            lineCrossesLine(bX, bY, cX, cY, dX, dY, eX, eY) ||
            lineCrossesLine(cX, cY, aX, aY, dX, dY, eX, eY)) {
          return true;
        }
      }

      if (left != null && left.crossesTriangle(minX, maxX, minY, maxY, aX, aY, bX, bY, cX, cY)) {
        return true;
      }

      if (right != null && maxY >= low && right.crossesTriangle(minX, maxX, minY, maxY, aX, aY, bX, bY, cX, cY)) {
        return true;
      }
    }
    return false;
  }

  /** Returns true if the box crosses any edge in this edge subtree */
  boolean crossesBox(int minX, int maxX, int minY, int maxY, boolean includeBoundary) {
    // we just have to cross one edge to answer the question, so we descend the tree and return when we do.
    if (minY <= max) {
      // we compute line intersections of every polygon edge with every box line.
      // if we find one, return true.
      // for each box line (AB):
      //   for each poly line (CD):
      //     intersects = orient(C,D,A) * orient(C,D,B) <= 0 && orient(A,B,C) * orient(A,B,D) <= 0
      int cX = x1;
      int cY = y1;
      int dX = x2;
      int dY = y2;

      // optimization: see if either end of the line segment is contained by the rectangle
      if (RectangleComponent2D.containsPoint(cX, cY, minX, maxX, minY, maxY) ||
          RectangleComponent2D.containsPoint(dX, dY, minX, maxX, minY, maxY)) {
        return true;
      }

      // optimization: see if the rectangle is outside of the "bounding box" of the polyline at all
      // if not, don't waste our time trying more complicated stuff
      boolean outside = (cX < minX && dX < minX) ||
          (cX > maxX && dX > maxX) ||
          (cY < minY && dY < minY) ||
          (cY > maxY && dY > maxY);

      // does rectangle's edges intersect polyline?
      if (outside == false) {
        if (includeBoundary == true &&
            lineCrossesLineWithBoundary(cX, cY, dX, dY, minX, minY, maxX, minY) ||
            lineCrossesLineWithBoundary(cX, cY, dX, dY, maxX, minY, maxX, maxY) ||
            lineCrossesLineWithBoundary(cX, cY, dX, dY, maxX, maxY, minX, maxY) ||
            lineCrossesLineWithBoundary(cX, cY, dX, dY, minX, maxY, minX, minY)) {
          // include boundaries: ensures box edges that terminate on the polygon are included
          return true;
        } else if (lineCrossesLine(cX, cY, dX, dY, minX, minY, maxX, minY) ||
            lineCrossesLine(cX, cY, dX, dY, maxX, minY, maxX, maxY) ||
            lineCrossesLine(cX, cY, dX, dY, maxX, maxY, minX, maxY) ||
            lineCrossesLine(cX, cY, dX, dY, minX, maxY, minX, minY)) {
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
  boolean crossesLine(int a2X, int a2Y, int b2X, int b2Y) {
    double minY = StrictMath.min(a2Y, b2Y);
    double maxY = StrictMath.max(a2Y, b2Y);
    if (minY <= max) {
      int a1X = x1;
      int a1Y = y1;
      int b1X = x2;
      int b1Y = y2;

      int minX = StrictMath.min(a2X, b2X);
      int maxX = StrictMath.max(a2X, b2X);

      boolean outside = (a1Y < minY && b1Y < minY) ||
          (a1Y > maxY && b1Y > maxY) ||
          (a1X < minX && b1X < minX) ||
          (a1X > maxX && b1X > maxX);
      if (outside == false && lineCrossesLineWithBoundary(a1X, a1Y, b1X, b1Y, a2X, a2Y, b2X, b2Y)) {
        return true;
      }

      if (left != null && left.crossesLine(a2X, a2Y, b2X, b2Y)) {
        return true;
      }
      if (right != null && maxY >= low && right.crossesLine(a2X, a2Y, b2X, b2Y)) {
        return true;
      }
    }
    return false;
  }

  /** returns true if the provided x, y point lies on any of the edges */
  boolean pointInEdge(int x, int y) {
    if (y <= max) {
      int minY = StrictMath.min(y1, y2);
      int maxY = StrictMath.max(y1, y2);
      int minX = StrictMath.min(x1, x2);
      int maxX = StrictMath.max(x1, x2);
      if (RectangleComponent2D.containsPoint(x, y, minX, maxX, minY, maxY) &&
          orient(x1, y1, x2, y2, x, y) == 0) {
        return true;
      }
      if (left != null && left.pointInEdge(x, y)) {
        return true;
      }
      if (right != null && maxY >= low && right.pointInEdge(x, y)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Creates an edge interval tree from a set of geometry vertices.
   * @return root node of the tree.
   */
  public static EdgeTree createTree(int[] Xs, int[] Ys) {
    EdgeTree edges[] = new EdgeTree[Ys.length - 1];
    for (int i = 1; i < Ys.length; i++) {
      int x1 = Xs[i-1];
      int y1 = Ys[i-1];
      int x2 = Xs[i];
      int y2 = Ys[i];

      edges[i - 1] = new EdgeTree(x1, y1, x2, y2, Math.min(y1, y2), Math.max(y1, y2));
    }
    // sort the edges then build a balanced tree from them
    Arrays.sort(edges, (left, right) -> {
      int ret = Integer.compare(left.low, right.low);
      if (ret == 0) {
        ret = Integer.compare(left.max, right.max);
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