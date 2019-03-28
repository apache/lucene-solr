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

import java.util.Comparator;

import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.util.ArrayUtil;

import static org.apache.lucene.geo.GeoUtils.orient;

/**
 * 2D line/polygon geometry implementation represented as a balanced interval tree of edges.
 * <p>
 * Construction takes {@code O(n log n)} time for sorting and tree construction.
 * {@link #relate relate()} are {@code O(n)}, but for most
 * practical lines and polygons are much faster than brute force.
 * @lucene.internal
 */
public class ComponentTree {
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
  protected ComponentTree left;
  protected ComponentTree right;

  /** root node of edge tree */
  protected final Component component;

  protected ComponentTree(Component component) {
    this.minLat = component.getBoundingBox().minLat;
    this.maxLat = component.getBoundingBox().maxLat;
    this.minLon = component.getBoundingBox().minLon;
    this.maxLon = component.getBoundingBox().maxLon;
    this.maxY = maxLat;
    this.maxX = maxLon;
    this.component = component;
  }

  /**
   * Returns true if the point is contained within this polygon.
   * <p>
   * See <a href="https://www.ecse.rpi.edu/~wrf/Research/Short_Notes/pnpoly.html">
   * https://www.ecse.rpi.edu/~wrf/Research/Short_Notes/pnpoly.html</a> for more information.
   */
  public boolean contains(double latitude, double longitude) {
    if (latitude <= maxY && longitude <= maxX) {
      if ((latitude < minLat || latitude > maxLat || longitude < minLon || longitude > maxLon) == false) {
        if (component.contains(latitude, longitude)) {
          return true;
        }
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

  /** Returns relation to the provided triangle */
  public Relation relateTriangle(double ax, double ay, double bx, double by, double cx, double cy) {
    // compute bounding box of triangle
    double minLat = StrictMath.min(StrictMath.min(ay, by), cy);
    double minLon = StrictMath.min(StrictMath.min(ax, bx), cx);
    double maxLat = StrictMath.max(StrictMath.max(ay, by), cy);
    double maxLon = StrictMath.max(StrictMath.max(ax, bx), cx);
    if (minLat <= maxY && minLon <= maxX) {
      if ((maxLon < this.minLon || minLon > this.maxLon || maxLat < this.minLat || minLat > this.maxLat) == false) {
        Relation relation = component.relateTriangle(ax, ay, bx, by, cx, cy);
        if (relation != Relation.CELL_OUTSIDE_QUERY) {
          return relation;
        }
      }
      if (left != null) {
        Relation relation = left.relateTriangle(ax, ay, bx, by, cx, cy);
        if (relation != Relation.CELL_OUTSIDE_QUERY) {
          return relation;
        }
      }
      if (right != null && ((splitX == false && maxLat >= this.minLat) || (splitX && maxLon >= this.minLon))) {
        Relation relation = right.relateTriangle(ax, ay, bx, by, cx, cy);
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
      // if the rectangle fully encloses us, we cross.
      if (minLat <= this.minLat && maxLat >= this.maxLat && minLon <= this.minLon && maxLon >= this.maxLon) {
        return Relation.CELL_CROSSES_QUERY;
      }
      if ((maxLon < this.minLon || minLon > this.maxLon || maxLat < this.minLat || minLat > this.maxLat) == false) {
        Relation relation = component.relate(minLat, maxLat, minLon, maxLon);
        if (relation != Relation.CELL_OUTSIDE_QUERY) {
          return relation;
        }
      }
      if (left != null) {
        Relation relation = left.relate(minLat, maxLat, minLon, maxLon);
        if (relation != Relation.CELL_OUTSIDE_QUERY) {
          return relation;
        }
      }
      if (right != null && ((splitX == false && maxLat >= this.minLat) || (splitX && maxLon >= this.minLon))) {
        Relation relation = right.relate(minLat, maxLat, minLon, maxLon);
        if (relation != Relation.CELL_OUTSIDE_QUERY) {
          return relation;
        }
      }
    }
    return Relation.CELL_OUTSIDE_QUERY;
  }

  /** Creates tree from sorted components (with range low and high inclusive) */
  protected static ComponentTree createTree(Component components[], int low, int high, boolean splitX) {
    if (low > high) {
      return null;
    }
    final int mid = (low + high) >>> 1;
    if (low < high) {
      Comparator<Component> comparator;
      if (splitX) {
        comparator = (left, right) -> {
          int ret = Double.compare(left.getBoundingBox().minLon, right.getBoundingBox().minLon);
          if (ret == 0) {
            ret = Double.compare(left.getBoundingBox().maxLon, right.getBoundingBox().maxLon);
          }
          return ret;
        };
      } else {
        comparator = (left, right) -> {
          int ret = Double.compare(left.getBoundingBox().minLat, right.getBoundingBox().minLat);
          if (ret == 0) {
            ret = Double.compare(left.getBoundingBox().maxLat, right.getBoundingBox().maxLat);
          }
          return ret;
        };
      }
      ArrayUtil.select(components, low, high + 1, mid, comparator);
    }
    // add midpoint
    ComponentTree newNode = new ComponentTree(components[mid]);

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

  /** Builds a Component tree from multipolygon */
  public static ComponentTree create(Component... components) {
    return ComponentTree.createTree(components, 0, components.length - 1, false);
  }
}
