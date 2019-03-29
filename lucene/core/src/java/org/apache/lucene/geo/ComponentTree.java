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

/**
 * 2D geometry collection implementation represented as a balanced interval tree of edges.
 * <p>
 * Construction takes {@code O(n log n)} time for sorting and tree construction.
 * {@link #relate relate()} are {@code O(n)}, but for most
 * collections are much faster than brute force.
 * @lucene.internal
 */
public class ComponentTree implements Component {

  /** which dimension was this node split on */
  // TODO: its implicit based on level, but boolean keeps code simple
  protected boolean splitX;

  // child components, or null
  protected ComponentTree left;
  protected ComponentTree right;

  /** root node of edge tree */
  protected final Component component;
  /** box of this component and its children */
  protected Rectangle box;

  protected ComponentTree(Component component) {
    this.box = component.getBoundingBox();
    this.component = component;
  }


  @Override
  public boolean contains(double latitude, double longitude) {
    if (latitude <= box.maxLat && longitude <= box.maxLon) {
      if ((latitude < component.getBoundingBox().minLat || latitude > component.getBoundingBox().maxLat ||
          longitude < component.getBoundingBox().minLon || longitude > component.getBoundingBox().maxLon) == false) {
        if (component.contains(latitude, longitude)) {
          return true;
        }
      }
      if (left != null) {
        if (left.contains(latitude, longitude)) {
          return true;
        }
      }
      if (right != null && ((splitX == false && latitude  >= box.minLat) || (splitX && longitude >= box.minLon))) {
        if (right.contains(latitude, longitude)) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public Rectangle getBoundingBox() {
    return box;
  }

  /** Returns relation to the provided triangle */
  @Override
  public Relation relateTriangle(double ax, double ay, double bx, double by, double cx, double cy) {
    // compute bounding box of triangle
    double minLat = StrictMath.min(StrictMath.min(ay, by), cy);
    double minLon = StrictMath.min(StrictMath.min(ax, bx), cx);
    double maxLat = StrictMath.max(StrictMath.max(ay, by), cy);
    double maxLon = StrictMath.max(StrictMath.max(ax, bx), cx);
    return relateTriangle(minLat, maxLat, minLon, maxLon, ax, ay, bx, by, cx, cy);
  }

  private Relation relateTriangle(double minLat, double maxLat, double minLon, double maxLon, double ax, double ay, double bx, double by, double cx, double cy) {

    if (minLat <= box.maxLat && minLon <= box.maxLon) {
      if ((maxLon < component.getBoundingBox().minLon || minLon > component.getBoundingBox().maxLon ||
          maxLat < component.getBoundingBox().minLat || minLat > component.getBoundingBox().maxLat) == false) {
        Relation relation = component.relateTriangle(ax, ay, bx, by, cx, cy);
        if (relation != Relation.CELL_OUTSIDE_QUERY) {
          return relation;
        }
      }
      if (left != null) {
        Relation relation = left.relateTriangle(minLat, maxLat, minLon, maxLon, ax, ay, bx, by, cx, cy);
        if (relation != Relation.CELL_OUTSIDE_QUERY) {
          return relation;
        }
      }
      if (right != null && ((splitX == false && maxLat >= box.minLat) || (splitX && maxLon >= box.minLon))) {
        Relation relation = right.relateTriangle(minLat, maxLat, minLon, maxLon, ax, ay, bx, by, cx, cy);
        if (relation != Relation.CELL_OUTSIDE_QUERY) {
          return relation;
        }
      }
    }
    return Relation.CELL_OUTSIDE_QUERY;
  }

  @Override
  public Relation relate(double minLat, double maxLat, double minLon, double maxLon) {
    if (minLat <= box.maxLat && minLon <= box.maxLon) {
      // if the rectangle fully encloses us, we cross.
      if (minLat <= component.getBoundingBox().minLat && maxLat >= component.getBoundingBox().maxLat &&
          minLon <= component.getBoundingBox().minLon && maxLon >= component.getBoundingBox().maxLon) {
        return Relation.CELL_CROSSES_QUERY;
      }
      if ((maxLon < component.getBoundingBox().minLon || minLon > component.getBoundingBox().maxLon ||
          maxLat < component.getBoundingBox().minLat || minLat > component.getBoundingBox().maxLat) == false) {
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
      if (right != null && ((splitX == false && maxLat >=  box.minLat) || (splitX && maxLon >= box.minLon))) {
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
    double minX = newNode.component.getBoundingBox().minLon;
    double maxX = newNode.component.getBoundingBox().maxLon;
    double minY = newNode.component.getBoundingBox().minLat;
    double maxY = newNode.component.getBoundingBox().maxLat;

    if (newNode.left != null) {
      maxX = Math.max(maxX, newNode.left.getBoundingBox().maxLon);
      maxY = Math.max(maxY, newNode.left.getBoundingBox().maxLat);
      minX = Math.min(minX, newNode.left.getBoundingBox().minLon);
      minY = Math.min(minY, newNode.left.getBoundingBox().minLat);
    }
    if (newNode.right != null) {
      maxX = Math.max(maxX, newNode.right.getBoundingBox().maxLon);
      maxY = Math.max(maxY, newNode.right.getBoundingBox().maxLat);
      minX = Math.min(minX, newNode.right.getBoundingBox().minLon);
      minY = Math.min(minY, newNode.right.getBoundingBox().minLat);
    }
    newNode.box = new Rectangle(minY, maxY, minX, maxX);
    return newNode;
  }

  /** Builds a Component tree from multipolygon */
  public static Component create(Component... components) {
    return ComponentTree.createTree(components, 0, components.length - 1, false);
  }
}
