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
 * 2D line/polygon geometry implementation represented as an R-tree of components.
 * <p>
 * Construction takes {@code O(n log n)} time for sorting and tree construction.
 *
 * @lucene.internal
 */
class ComponentTree implements Component2D {
  /** minimum latitude of this geometry's bounding box area */
  public  double minY;
  /** maximum latitude of this geometry's bounding box area */
  public  double maxY;
  /** minimum longitude of this geometry's bounding box area */
  public  double minX;
  /** maximum longitude of this geometry's bounding box area */
  public  double maxX;

  // child components, or null
  protected Component2D left;
  protected Component2D right;

  /** root node of edge tree */
  protected final Component2D component;

  protected ComponentTree(Component2D component) {
    this.minY = component.getMinY();
    this.maxY = component.getMaxY();
    this.minX = component.getMinX();
    this.maxX = component.getMaxX();
    this.component = component;
  }

  @Override
  public double getMinX() {
    return minX;
  }

  @Override
  public double getMaxX() {
    return maxX;
  }

  @Override
  public double getMinY() {
    return minY;
  }

  @Override
  public double getMaxY() {
    return maxY;
  }

  @Override
  public boolean contains(double x, double y) {
    if (x >= minX & x <= maxX && y >= minY && y <= maxY) {
      if (component.contains(x, y)) {
        return true;
      }
      if (left != null) {
        if (left.contains(x, y)) {
          return true;
        }
      }
      if (right != null) {
        if (right.contains(x, y)) {
          return true;
        }
      }
    }
    return false;
  }

  /** Returns relation to the provided triangle */
  @Override
  public Relation relateTriangle(double minX, double maxX, double minY, double maxY,
                                 double ax, double ay, double bx, double by, double cx, double cy) {
    if (disjoint(minX, maxX, minY, maxY) == false) {
      Relation relation = component.relateTriangle(minX, maxX, minY, maxY, ax, ay, bx, by, cx, cy);
      if (relation != Relation.CELL_OUTSIDE_QUERY) {
        return relation;
      }
      if (left != null) {
        relation = left.relateTriangle(minX, maxX, minY, maxY, ax, ay, bx, by, cx, cy);
        if (relation != Relation.CELL_OUTSIDE_QUERY) {
          return relation;
        }
      }
      if (right != null) {
        relation = right.relateTriangle(minX, maxX, minY, maxY, ax, ay, bx, by, cx, cy);
        if (relation != Relation.CELL_OUTSIDE_QUERY) {
          return relation;
        }
      }
    }
    return Relation.CELL_OUTSIDE_QUERY;
  }

  /** Returns relation to the provided rectangle */
  @Override
  public Relation relate(double minX, double maxX, double minY, double maxY) {
    if (disjoint(minX, maxX, minY, maxY) == false) {
      if (within(minX, maxX, minY, maxY)) {
        return Relation.CELL_CROSSES_QUERY;
      }
      Relation relation = component.relate(minX, maxX, minY, maxY);
      if (relation != Relation.CELL_OUTSIDE_QUERY) {
        return relation;
      }
      if (left != null) {
        relation = left.relate(minX, maxX, minY, maxY);
        if (relation != Relation.CELL_OUTSIDE_QUERY) {
          return relation;
        }
      }
      if (right != null) {
        relation = right.relate(minX, maxX, minY, maxY);
        if (relation != Relation.CELL_OUTSIDE_QUERY) {
          return relation;
        }
      }
    }
    return Relation.CELL_OUTSIDE_QUERY;
  }

  public static Component2D create(Component2D[] components) {
    if (components.length == 1) {
      return components[0];
    }
    return createTree(components, 0, components.length - 1, false);
  }

  /** Creates tree from sorted components (with range low and high inclusive) */
  private static Component2D createTree(Component2D[] components, int low, int high, boolean splitX) {
    if (low > high) {
      return null;
    }
    final int mid = (low + high) >>> 1;
    if (low < high) {
      Comparator<Component2D> comparator;
      if (splitX) {
        comparator = (left, right) -> {
          int ret = Double.compare(left.getMinX(), right.getMinX());
          if (ret == 0) {
            ret = Double.compare(left.getMaxX(), right.getMaxX());
          }
          return ret;
        };
      } else {
        comparator = (left, right) -> {
          int ret = Double.compare(left.getMinY(), right.getMinY());
          if (ret == 0) {
            ret = Double.compare(left.getMaxY(), right.getMaxY());
          }
          return ret;
        };
      }
      ArrayUtil.select(components, low, high + 1, mid, comparator);
    }
    // find children
    Component2D left = createTree(components, low, mid - 1, !splitX);
    Component2D right = createTree(components, mid + 1, high, !splitX);
    if (left == null && right == null) {
      // is a leaf so we can return the component
      return components[mid];
    }
    // add new tree node
    ComponentTree newNode = new ComponentTree(components[mid]);
    // pull up min / max values to this node
    if (left != null) {
      newNode.left = left;
      newNode.maxX = Math.max(newNode.maxX, newNode.left.getMaxX());
      newNode.maxY = Math.max(newNode.maxY, newNode.left.getMaxY());
      if (splitX) {
        newNode.minX = newNode.left.getMinX();
        newNode.minY = Math.min(newNode.minY, newNode.left.getMinY());
      } else {
        newNode.minX = Math.min(newNode.minX, newNode.left.getMinX());
        newNode.minY = newNode.left.getMinY();
      }
    }
    if (right != null) {
      newNode.right = right;
      newNode.maxX = Math.max(newNode.maxX, newNode.right.getMaxX());
      newNode.maxY = Math.max(newNode.maxY, newNode.right.getMaxY());
      if (splitX) {
        // minX already correct
        newNode.minY = Math.min(newNode.minY, newNode.right.getMinY());
      } else {
        newNode.minX = Math.min(newNode.minX, newNode.right.getMinX());
        // minY already correct
      }
    }
    return newNode;
  }
}
