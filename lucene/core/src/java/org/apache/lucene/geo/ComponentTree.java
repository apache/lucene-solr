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
 * 2D geometry collection implementation represented as a r-tree of {@link Component}.
 *
 * @lucene.internal
 */
public class ComponentTree implements Component {
  /** root node of edge tree */
  protected final Component component;
  /** box of this component and its children or null if there is no children */
  protected Rectangle box;
  // child components, or null
  protected ComponentTree left;
  protected ComponentTree right;

  protected ComponentTree(Component component) {
    this.component = component;
    this.box = null;
  }

  public boolean contains(double latitude, double longitude) {
    if (box == null || Rectangle.disjoint(box, latitude, latitude, longitude, longitude) == false) {
      if (Rectangle.disjoint(component.getBoundingBox(), latitude, latitude, longitude, longitude) == false) {
        if (component.contains(latitude, longitude)) {
          return true;
        }
      }
      if (left != null) {
        if (left.contains(latitude, longitude)) {
          return true;
        }
      }
      if (right != null) {
        if (right.contains(latitude, longitude)) {
          return true;
        }
      }
    }
    return false;
  }

  public Relation relate(double minLat, double maxLat, double minLon, double maxLon) {
    if (box == null || Rectangle.disjoint(box, minLat, maxLat, minLon, maxLon) == false) {
      // if the rectangle fully encloses us, we cross.
      if (Rectangle.within(component.getBoundingBox(), minLat, maxLat, minLon, maxLon)) {
        return Relation.CELL_CROSSES_QUERY;
      }
      if (Rectangle.disjoint(component.getBoundingBox(), minLat, maxLat, minLon, maxLon) == false) {
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
      if (right != null) {
        Relation relation = right.relate(minLat, maxLat, minLon, maxLon);
        if (relation != Relation.CELL_OUTSIDE_QUERY) {
          return relation;
        }
      }
    }
    return Relation.CELL_OUTSIDE_QUERY;
  }

  /** Returns relation to the provided triangle */
  public Relation relateTriangle(double ax, double ay, double bx, double by, double cx, double cy) {
    // compute bounding box of triangle
    double minLat = StrictMath.min(StrictMath.min(ay, by), cy);
    double minLon = StrictMath.min(StrictMath.min(ax, bx), cx);
    double maxLat = StrictMath.max(StrictMath.max(ay, by), cy);
    double maxLon = StrictMath.max(StrictMath.max(ax, bx), cx);
    return relateTriangle(minLat, maxLat, minLon, maxLon, ax, ay, bx, by, cx, cy);
  }

  @Override
  public Rectangle getBoundingBox() {
    return box == null ? component.getBoundingBox() : box;
  }

  private Relation relateTriangle(double minLat, double maxLat, double minLon, double maxLon, double ax, double ay, double bx, double by, double cx, double cy) {
    if (box == null || Rectangle.disjoint(box, minLat, maxLat, minLon, maxLon) == false) {
      if (Rectangle.disjoint(component.getBoundingBox(), minLat, maxLat, minLon, maxLon) == false) {
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
      if (right != null) {
        Relation relation = right.relateTriangle(minLat, maxLat, minLon, maxLon, ax, ay, bx, by, cx, cy);
        if (relation != Relation.CELL_OUTSIDE_QUERY) {
          return relation;
        }
      }
    }
    return Relation.CELL_OUTSIDE_QUERY;
  }

  /** Creates tree from sorted components (with full bounding box for children) */
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
    } else {
      return new ComponentTree(components[mid]);
    }
    // add midpoint
    ComponentTree newNode = new ComponentTree(components[mid]);
    // add children
    newNode.left = createTree(components, low, mid - 1, !splitX);
    newNode.right = createTree(components, mid + 1, high, !splitX);
    if (newNode.left != null || newNode.right != null) {
      // pull up bounding box values to this node
      double minX = newNode.component.getBoundingBox().minLon;
      double maxX = newNode.component.getBoundingBox().maxLon;
      double minY = newNode.component.getBoundingBox().minLat;
      double maxY = newNode.component.getBoundingBox().maxLat;

      if (newNode.left != null) {
        maxX = Math.max(maxX, newNode.left.getBoundingBox().maxLon);
        maxY = Math.max(maxY, newNode.left.getBoundingBox().maxLat);
        minX = splitX == true  ? newNode.left.getBoundingBox().minLon : Math.min(minX, newNode.left.getBoundingBox().minLon);
        minY = splitX == false ? newNode.left.getBoundingBox().minLat : Math.min(minY, newNode.left.getBoundingBox().minLat);
      }
      if (newNode.right != null) {
        maxX = Math.max(maxX, newNode.right.getBoundingBox().maxLon);
        maxY = Math.max(maxY, newNode.right.getBoundingBox().maxLat);
        minX = splitX == true  ? minX : Math.min(minX, newNode.right.getBoundingBox().minLon);
        minY = splitX == false ? minY : Math.min(minY, newNode.right.getBoundingBox().minLat);
      }
      newNode.box = new Rectangle(minY, maxY, minX, maxX);
    }
    assert newNode.left == null || (newNode.getBoundingBox().minLat <= newNode.left.getBoundingBox().minLat &&
        newNode.getBoundingBox().maxLat >= newNode.left.getBoundingBox().maxLat &&
        newNode.getBoundingBox(). minLon <= newNode.left.getBoundingBox().minLon &&
        newNode.getBoundingBox().maxLon >= newNode.left.getBoundingBox().maxLon);
    assert newNode.right == null || (newNode.getBoundingBox().minLat <= newNode.right.getBoundingBox().minLat &&
        newNode.getBoundingBox().maxLat >= newNode.right.getBoundingBox().maxLat &&
        newNode.getBoundingBox(). minLon <= newNode.right.getBoundingBox().minLon &&
        newNode.getBoundingBox().maxLon >= newNode.right.getBoundingBox().maxLon);
    return newNode;
  }

  /** Builds a Component  from multiple components in a tree structure */
  public static Component create(Component... components) {
    if (components.length == 1) {
      return components[0];
    }
    return ComponentTree.createTree(components, 0, components.length - 1, true);
  }
}
