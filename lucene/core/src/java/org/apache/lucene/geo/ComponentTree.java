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
 * 2D multi-component geometry implementation represented as an interval tree of components.
 *
 * <p>Construction takes {@code O(n log n)} time for sorting and tree construction.
 */
final class ComponentTree implements Component2D {
  /** minimum Y of this geometry's bounding box area */
  private double minY;
  /** maximum Y of this geometry's bounding box area */
  private double maxY;
  /** minimum X of this geometry's bounding box area */
  private double minX;
  /** maximum X of this geometry's bounding box area */
  private double maxX;
  // child components, or null. Note internal nodes might mot have
  // a consistent bounding box. Internal nodes should not be accessed
  // outside if this class.
  private Component2D left;
  private Component2D right;
  /** which dimension was this node split on */
  // TODO: its implicit based on level, but boolean keeps code simple
  private final boolean splitX;
  /** root node of edge tree */
  private final Component2D component;

  private ComponentTree(Component2D component, boolean splitX) {
    this.minY = component.getMinY();
    this.maxY = component.getMaxY();
    this.minX = component.getMinX();
    this.maxX = component.getMaxX();
    this.component = component;
    this.splitX = splitX;
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
    if (y <= this.maxY && x <= this.maxX) {
      if (component.contains(x, y)) {
        return true;
      }
      if (left != null) {
        if (left.contains(x, y)) {
          return true;
        }
      }
      if (right != null
          && ((splitX == false && y >= this.component.getMinY())
              || (splitX && x >= this.component.getMinX()))) {
        if (right.contains(x, y)) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public boolean intersectsLine(
      double minX,
      double maxX,
      double minY,
      double maxY,
      double aX,
      double aY,
      double bX,
      double bY) {
    if (minY <= this.maxY && minX <= this.maxX) {
      if (component.intersectsLine(minX, maxX, minY, maxY, aX, aY, bX, bY)) {
        return true;
      }
      if (left != null) {
        if (left.intersectsLine(minX, maxX, minY, maxY, aX, aY, bX, bY)) {
          return true;
        }
      }
      if (right != null
          && ((splitX == false && maxY >= this.component.getMinY())
              || (splitX && maxX >= this.component.getMinX()))) {
        if (right.intersectsLine(minX, maxX, minY, maxY, aX, aY, bX, bY)) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public boolean intersectsTriangle(
      double minX,
      double maxX,
      double minY,
      double maxY,
      double aX,
      double aY,
      double bX,
      double bY,
      double cX,
      double cY) {
    if (minY <= this.maxY && minX <= this.maxX) {
      if (component.intersectsTriangle(minX, maxX, minY, maxY, aX, aY, bX, bY, cX, cY)) {
        return true;
      }
      if (left != null) {
        if (left.intersectsTriangle(minX, maxX, minY, maxY, aX, aY, bX, bY, cX, cY)) {
          return true;
        }
      }
      if (right != null
          && ((splitX == false && maxY >= this.component.getMinY())
              || (splitX && maxX >= this.component.getMinX()))) {
        if (right.intersectsTriangle(minX, maxX, minY, maxY, aX, aY, bX, bY, cX, cY)) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public boolean containsLine(
      double minX,
      double maxX,
      double minY,
      double maxY,
      double aX,
      double aY,
      double bX,
      double bY) {
    if (minY <= this.maxY && minX <= this.maxX) {
      if (component.containsLine(minX, maxX, minY, maxY, aX, aY, bX, bY)) {
        return true;
      }
      if (left != null) {
        if (left.containsLine(minX, maxX, minY, maxY, aX, aY, bX, bY)) {
          return true;
        }
      }
      if (right != null
          && ((splitX == false && maxY >= this.component.getMinY())
              || (splitX && maxX >= this.component.getMinX()))) {
        if (right.containsLine(minX, maxX, minY, maxY, aX, aY, bX, bY)) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public boolean containsTriangle(
      double minX,
      double maxX,
      double minY,
      double maxY,
      double aX,
      double aY,
      double bX,
      double bY,
      double cX,
      double cY) {
    if (minY <= this.maxY && minX <= this.maxX) {
      if (component.containsTriangle(minX, maxX, minY, maxY, aX, aY, bX, bY, cX, cY)) {
        return true;
      }
      if (left != null) {
        if (left.containsTriangle(minX, maxX, minY, maxY, aX, aY, bX, bY, cX, cY)) {
          return true;
        }
      }
      if (right != null
          && ((splitX == false && maxY >= this.component.getMinY())
              || (splitX && maxX >= this.component.getMinX()))) {
        if (right.containsTriangle(minX, maxX, minY, maxY, aX, aY, bX, bY, cX, cY)) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public WithinRelation withinPoint(double x, double y) {
    if (left != null || right != null) {
      throw new IllegalArgumentException(
          "withinPoint is not supported for shapes with more than one component");
    }
    return component.withinPoint(x, y);
  }

  @Override
  public WithinRelation withinLine(
      double minX,
      double maxX,
      double minY,
      double maxY,
      double aX,
      double aY,
      boolean ab,
      double bX,
      double bY) {
    if (left != null || right != null) {
      throw new IllegalArgumentException(
          "withinLine is not supported for shapes with more than one component");
    }
    return component.withinLine(minX, maxX, minY, maxY, aX, aY, ab, bX, bY);
  }

  @Override
  public WithinRelation withinTriangle(
      double minX,
      double maxX,
      double minY,
      double maxY,
      double aX,
      double aY,
      boolean ab,
      double bX,
      double bY,
      boolean bc,
      double cX,
      double cY,
      boolean ca) {
    if (left != null || right != null) {
      throw new IllegalArgumentException(
          "withinTriangle is not supported for shapes with more than one component");
    }
    return component.withinTriangle(minX, maxX, minY, maxY, aX, aY, ab, bX, bY, bc, cX, cY, ca);
  }

  @Override
  public Relation relate(double minX, double maxX, double minY, double maxY) {
    if (minY <= this.maxY && minX <= this.maxX) {
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
      if (right != null
          && ((splitX == false && maxY >= this.component.getMinY())
              || (splitX && maxX >= this.component.getMinX()))) {
        relation = right.relate(minX, maxX, minY, maxY);
        if (relation != Relation.CELL_OUTSIDE_QUERY) {
          return relation;
        }
      }
    }
    return Relation.CELL_OUTSIDE_QUERY;
  }

  /** Creates tree from provided components */
  static Component2D create(Component2D[] components) {
    if (components.length == 1) {
      return components[0];
    }
    ComponentTree root = createTree(components, 0, components.length - 1, false);
    // pull up min values for the root node so it contains a consistent bounding box
    for (Component2D component : components) {
      root.minY = Math.min(root.minY, component.getMinY());
      root.minX = Math.min(root.minX, component.getMinX());
    }
    return root;
  }

  /** Creates tree from sorted components (with range low and high inclusive) */
  private static ComponentTree createTree(
      Component2D[] components, int low, int high, boolean splitX) {
    if (low > high) {
      return null;
    }
    final int mid = (low + high) >>> 1;
    if (low < high) {
      Comparator<Component2D> comparator;
      if (splitX) {
        comparator =
            (left, right) -> {
              int ret = Double.compare(left.getMinX(), right.getMinX());
              if (ret == 0) {
                ret = Double.compare(left.getMaxX(), right.getMaxX());
              }
              return ret;
            };
      } else {
        comparator =
            (left, right) -> {
              int ret = Double.compare(left.getMinY(), right.getMinY());
              if (ret == 0) {
                ret = Double.compare(left.getMaxY(), right.getMaxY());
              }
              return ret;
            };
      }
      ArrayUtil.select(components, low, high + 1, mid, comparator);
    }
    ComponentTree newNode = new ComponentTree(components[mid], splitX);
    // find children
    newNode.left = createTree(components, low, mid - 1, !splitX);
    newNode.right = createTree(components, mid + 1, high, !splitX);

    // pull up max values to this node
    if (newNode.left != null) {
      newNode.maxX = Math.max(newNode.maxX, newNode.left.getMaxX());
      newNode.maxY = Math.max(newNode.maxY, newNode.left.getMaxY());
    }
    if (newNode.right != null) {
      newNode.maxX = Math.max(newNode.maxX, newNode.right.getMaxX());
      newNode.maxY = Math.max(newNode.maxY, newNode.right.getMaxY());
    }
    return newNode;
  }
}
