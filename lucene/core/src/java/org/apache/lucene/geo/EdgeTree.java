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

import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.util.ArrayUtil;

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
public abstract class EdgeTree {
  /** minimum latitude of this geometry's bounding box area */
  public final double minLat;
  /** maximum latitude of this geometry's bounding box area */
  public final double maxLat;
  /** minimum longitude of this geometry's bounding box area */
  public final double minLon;
  /** maximum longitude of this geometry's bounding box area */
  public final double maxLon;

  // each component is a node in an augmented 2d kd-tree: we alternate splitting between latitude/longitude,
  // and pull up max values for both dimensions to each parent node (regardless of split).

  /** maximum latitude of this component or any of its children */
  protected double maxY;
  /** maximum longitude of this component or any of its children */
  protected double maxX;
  /** which dimension was this node split on */
  // TODO: its implicit based on level, but boolean keeps code simple
  protected boolean splitX;

  // child components, or null
  protected EdgeTree left;
  protected EdgeTree right;

  /** root node of edge tree */
  protected final Edge tree;

  protected EdgeTree(final double minLat, final double maxLat, final double minLon, final double maxLon, double[] lats, double[] lons) {
    this.minLat = minLat;
    this.maxLat = maxLat;
    this.minLon = minLon;
    this.maxLon = maxLon;
    this.maxY = maxLat;
    this.maxX = maxLon;

    // create interval tree of edges
    this.tree = createTree(lats, lons);
  }

  /** Returns relation to the provided triangle */
  public Relation relateTriangle(double ax, double ay, double bx, double by, double cx, double cy) {
    // compute bounding box of triangle
    double triMinLat = StrictMath.min(StrictMath.min(ay, by), cy);
    double triMinLon = StrictMath.min(StrictMath.min(ax, bx), cx);
    if (triMinLat <= maxY && triMinLon <= maxX) {
      Relation relation = internalComponentRelateTriangle(ax, ay, bx, by, cx, cy);
      if (relation != Relation.CELL_OUTSIDE_QUERY) {
        return relation;
      }
      if (left != null) {
        relation = left.relateTriangle(ax, ay, bx, by, cx, cy);
        if (relation != Relation.CELL_OUTSIDE_QUERY) {
          return relation;
        }
      }
      double triMaxLat = StrictMath.max(StrictMath.max(ay, by), cy);
      double triMaxLon = StrictMath.max(StrictMath.max(ax, bx), cx);
      if (right != null && ((splitX == false && triMaxLat >= this.minLat) || (splitX && triMaxLon >= this.minLon))) {
        relation = right.relateTriangle(ax, ay, bx, by, cx, cy);
        if (relation != Relation.CELL_OUTSIDE_QUERY) {
          return relation;
        }
      }
    }
    return Relation.CELL_OUTSIDE_QUERY;
  }

  /** Returns relation to the provided rectangle */
  public Relation relate(double minLat, double maxLat, double minLon, double maxLon) {
    if (minLat <= maxY && minLon <= maxX) {
      Relation relation = internalComponentRelate(minLat, maxLat, minLon, maxLon);
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
  protected abstract Relation componentRelate(double minLat, double maxLat, double minLon, double maxLon);

  /** Returns relation to the provided triangle for this component */
  protected abstract Relation componentRelateTriangle(double ax, double ay, double bx, double by, double cx, double cy);


  private Relation internalComponentRelateTriangle(double ax, double ay, double bx, double by, double cx, double cy) {
    // compute bounding box of triangle
    double minLat = StrictMath.min(StrictMath.min(ay, by), cy);
    double minLon = StrictMath.min(StrictMath.min(ax, bx), cx);
    double maxLat = StrictMath.max(StrictMath.max(ay, by), cy);
    double maxLon = StrictMath.max(StrictMath.max(ax, bx), cx);
    if (maxLon < this.minLon || minLon > this.maxLon || maxLat < this.minLat || minLat > this.maxLat) {
      return Relation.CELL_OUTSIDE_QUERY;
    }
    return componentRelateTriangle(ax, ay, bx, by, cx, cy);
  }


  /** Returns relation to the provided rectangle for this component */
  protected Relation internalComponentRelate(double minLat, double maxLat, double minLon, double maxLon) {
    // if the bounding boxes are disjoint then the shape does not cross
    if (maxLon < this.minLon || minLon > this.maxLon || maxLat < this.minLat || minLat > this.maxLat) {
      return Relation.CELL_OUTSIDE_QUERY;
    }
    // if the rectangle fully encloses us, we cross.
    if (minLat <= this.minLat && maxLat >= this.maxLat && minLon <= this.minLon && maxLon >= this.maxLon) {
      return Relation.CELL_CROSSES_QUERY;
    }
    return componentRelate(minLat, maxLat, minLon, maxLon);
  }

  /** Creates tree from sorted components (with range low and high inclusive) */
  protected static EdgeTree createTree(EdgeTree components[], int low, int high, boolean splitX) {
    if (low > high) {
      return null;
    }
    final int mid = (low + high) >>> 1;
    if (low < high) {
      Comparator<EdgeTree> comparator;
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
    EdgeTree newNode = components[mid];
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

  /**
   * Internal tree node: represents geometry edge from lat1,lon1 to lat2,lon2.
   * The sort value is {@code low}, which is the minimum latitude of the edge.
   * {@code max} stores the maximum latitude of this edge or any children.
   */
  static class Edge {
    // lat-lon pair (in original order) of the two vertices
    final double lat1, lat2;
    final double lon1, lon2;
    //edge belongs to the dateline
    final boolean dateline;
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
      dateline = (lon1 == GeoUtils.MIN_LON_INCL && lon2 == GeoUtils.MIN_LON_INCL)
          || (lon1 == GeoUtils.MAX_LON_INCL && lon2 == GeoUtils.MAX_LON_INCL);
    }

    /** Returns true if the triangle crosses any edge in this edge subtree */
    boolean crossesTriangle(double ax, double ay, double bx, double by, double cx, double cy) {
      // compute min lat of triangle bounding box
      double triMinLat = StrictMath.min(StrictMath.min(ay, by), cy);
      if (triMinLat <= max) {
        double dy = lat1;
        double ey = lat2;
        double dx = lon1;
        double ex = lon2;

        // compute remaining bounding box of triangle
        double triMinLon = StrictMath.min(StrictMath.min(ax, bx), cx);
        double triMaxLat = StrictMath.max(StrictMath.max(ay, by), cy);
        double triMaxLon = StrictMath.max(StrictMath.max(ax, bx), cx);

        // optimization: see if the rectangle is outside of the "bounding box" of the polyline at all
        // if not, don't waste our time trying more complicated stuff
        boolean outside = (dy < triMinLat && ey < triMinLat) ||
            (dy > triMaxLat && ey > triMaxLat) ||
            (dx < triMinLon && ex < triMinLon) ||
            (dx > triMaxLon && ex > triMaxLon);

        if (outside == false) {
          if (lineCrossesLine(dx, dy, ex, ey, ax, ay, bx, by) ||
              lineCrossesLine(dx, dy, ex, ey, bx, by, cx, cy) ||
              lineCrossesLine(dx, dy, ex, ey, cx, cy, ax, ay)) {
            return true;
          }
        }

        if (left != null && left.crossesTriangle(ax, ay, bx, by, cx, cy)) {
          return true;
        }

        if (right != null && triMaxLat >= low && right.crossesTriangle(ax, ay, bx, by, cx, cy)) {
          return true;
        }
      }
      return false;
    }

    /** Returns true if the box crosses any edge in this edge subtree */
    boolean crossesBox(double minLat, double maxLat, double minLon, double maxLon, boolean includeBoundary) {
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

        // optimization: see if either end of the line segment is contained by the rectangle
        if (Rectangle.containsPoint(cy, cx, minLat, maxLat, minLon, maxLon) ||
            Rectangle.containsPoint(dy, dx, minLat, maxLat, minLon, maxLon)) {
          return true;
        }

        // optimization: see if the rectangle is outside of the "bounding box" of the polyline at all
        // if not, don't waste our time trying more complicated stuff
        boolean outside = (cy < minLat && dy < minLat) ||
            (cy > maxLat && dy > maxLat) ||
            (cx < minLon && dx < minLon) ||
            (cx > maxLon && dx > maxLon);

        if (outside == false) {
          if (includeBoundary == true &&
              lineCrossesLineWithBoundary(cx, cy, dx, dy, minLon, minLat, maxLon, minLat) ||
              lineCrossesLineWithBoundary(cx, cy, dx, dy, maxLon, minLat, maxLon, maxLat) ||
              lineCrossesLineWithBoundary(cx, cy, dx, dy, maxLon, maxLat, maxLon, minLat) ||
              lineCrossesLineWithBoundary(cx, cy, dx, dy, minLon, maxLat, minLon, minLat)) {
            // include boundaries: ensures box edges that terminate on the polygon are included
            return true;
          } else if (lineCrossesLine(cx, cy, dx, dy, minLon, minLat, maxLon, minLat) ||
              lineCrossesLine(cx, cy, dx, dy, maxLon, minLat, maxLon, maxLat) ||
              lineCrossesLine(cx, cy, dx, dy, maxLon, maxLat, maxLon, minLat) ||
              lineCrossesLine(cx, cy, dx, dy, minLon, maxLat, minLon, minLat)) {
            return true;
          }
        }

        if (left != null && left.crossesBox(minLat, maxLat, minLon, maxLon, includeBoundary)) {
          return true;
        }

        if (right != null && maxLat >= low && right.crossesBox(minLat, maxLat, minLon, maxLon, includeBoundary)) {
          return true;
        }
      }
      return false;
    }

    /** Returns true if the line crosses any edge in this edge subtree */
    boolean crossesLine(double a2x, double a2y, double b2x, double b2y) {
      double minY = StrictMath.min(a2y, b2y);
      double maxY = StrictMath.max(a2y, b2y);
      if (minY <= max) {
        double a1x = lon1;
        double a1y = lat1;
        double b1x = lon2;
        double b1y = lat2;

        double minX = StrictMath.min(a2x, b2x);
        double maxX = StrictMath.max(a2x, b2x);

        boolean outside = (a1y < minY && b1y < minY) ||
            (a1y > maxY && b1y > maxY) ||
            (a1x < minX && b1x < minX) ||
            (a1x > maxX && b1x > maxX);
        if (outside == false && lineCrossesLineWithBoundary(a1x, a1y, b1x, b1y, a2x, a2y, b2x, b2y)) {
          return true;
        }

        if (left != null && left.crossesLine(a2x, a2y, b2x, b2y)) {
          return true;
        }
        if (right != null && maxY >= low && right.crossesLine(a2x, a2y, b2x, b2y)) {
          return true;
        }
      }
      return false;
    }
  }

  //This should be moved when LatLonShape is moved from sandbox!
  /**
   * Compute whether the given x, y point is in a triangle; uses the winding order method */
  protected static boolean pointInTriangle (double x, double y, double ax, double ay, double bx, double by, double cx, double cy) {
    double minX = StrictMath.min(ax, StrictMath.min(bx, cx));
    double minY = StrictMath.min(ay, StrictMath.min(by, cy));
    double maxX = StrictMath.max(ax, StrictMath.max(bx, cx));
    double maxY = StrictMath.max(ay, StrictMath.max(by, cy));
    //check the bounding box because if the triangle is degenerated, e.g points and lines, we need to filter out
    //coplanar points that are not part of the triangle.
    if (x >= minX && x <= maxX && y >= minY && y <= maxY ) {
      int a = orient(x, y, ax, ay, bx, by);
      int b = orient(x, y, bx, by, cx, cy);
      if (a == 0 || b == 0 || a < 0 == b < 0) {
        int c = orient(x, y, cx, cy, ax, ay);
        return c == 0 || (c < 0 == (b < 0 || a < 0));
      }
      return false;
    } else {
      return false;
    }
  }

  /**
   * Creates an edge interval tree from a set of geometry vertices.
   * @return root node of the tree.
   */
  private static Edge createTree(double[] lats, double[] lons) {
    Edge edges[] = new Edge[lats.length - 1];
    for (int i = 1; i < lats.length; i++) {
      double lat1 = lats[i-1];
      double lon1 = lons[i-1];
      double lat2 = lats[i];
      double lon2 = lons[i];
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
}
