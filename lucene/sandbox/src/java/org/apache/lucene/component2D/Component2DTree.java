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

import java.util.Comparator;
import java.util.Objects;

import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.util.ArrayUtil;

/**
 * 2D geometry collection implementation represented as a r-tree of {@link Component2D}.
 *
 * @lucene.internal
 */
class Component2DTree implements Component2D {
  /** root node of edge tree */
  private final Component2D component;
  /** box of this component2D and its children or null if there is no children */
  private RectangleComponent2D box;
  // child components, or null
  private Component2DTree left;
  private Component2DTree right;

  private Component2DTree(Component2D component) {
    this.component = component;
    this.box = null;
  }

  @Override
  public boolean contains(int x, int y) {
    if (box == null || box.contains(x, y)) {
      if (component.contains(x, y)) {
        return true;
      }
      if (left != null && left.contains(x, y)) {
        return true;
      }
      if (right != null && right.contains(x, y)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public Relation relate(int minX, int maxX, int minY, int maxY) {
   if (box == null || box.disjoint(minX, maxX, minY, maxY) == false) {
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

  @Override
  public RectangleComponent2D getBoundingBox() {
    return box == null ? component.getBoundingBox() : box;
  }

  /** Returns relation to the provided triangle */
  @Override
  public Relation relateTriangle(int minX, int maxX, int minY, int maxY, int aX, int aY, int bX, int bY, int cX, int cY) {
    if (box == null || box.disjoint(minX, maxX, minY, maxY) == false) {
      Relation relation = component.relateTriangle(minX, maxX, minY, maxY, aX, aY, bX, bY, cX, cY);
      if (relation != Relation.CELL_OUTSIDE_QUERY) {
        return relation;
      }
      if (left != null) {
        relation = left.relateTriangle(minX, maxX, minY, maxY, aX, aY, bX, bY, cX, cY);
        if (relation != Relation.CELL_OUTSIDE_QUERY) {
          return relation;
        }
      }
      if (right != null) {
        relation = right.relateTriangle(minX, maxX, minY, maxY, aX, aY, bX, bY, cX, cY);
        if (relation != Relation.CELL_OUTSIDE_QUERY) {
          return relation;
        }
      }
    }
    return Relation.CELL_OUTSIDE_QUERY;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Component2DTree componentTree = (Component2DTree) o;
    return Objects.equals(component, componentTree.component) &&
        Objects.equals(left, componentTree.left) &&
        Objects.equals(right, componentTree.right);
  }

  @Override
  public int hashCode() {
    int result;
    long temp;
    temp = Objects.hashCode(component);
    result = (int) (temp ^ (temp >>> 32));
    temp = Objects.hashCode(left);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    temp = Objects.hashCode(right);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return "PolygonComponent2D{" +
        "compoment=" + component + " left=" + Objects.toString(left) + " right=" + Objects.toString(right) +
        '}';
  }

  /** Creates tree from sorted components (with full bounding box for children) */
  protected static Component2DTree createTree(Component2D components[], int low, int high, boolean splitX) {
    if (low > high) {
      return null;
    }
    final int mid = (low + high) >>> 1;
    if (low < high) {
      Comparator<Component2D> comparator;
      if (splitX) {
        comparator = (left, right) -> {
          int ret = Integer.compare(left.getBoundingBox().minX, right.getBoundingBox().minX);
          if (ret == 0) {
            ret = Integer.compare(left.getBoundingBox().maxX, right.getBoundingBox().maxX);
          }
          return ret;
        };
      } else {
        comparator = (left, right) -> {
          int ret = Integer.compare(left.getBoundingBox().minY, right.getBoundingBox().minY);
          if (ret == 0) {
            ret = Integer.compare(left.getBoundingBox().maxY, right.getBoundingBox().maxY);
          }
          return ret;
        };
      }
      ArrayUtil.select(components, low, high + 1, mid, comparator);
    } else {
      return new Component2DTree(components[mid]);
    }
    // add midpoint
    Component2DTree newNode = new Component2DTree(components[mid]);
    // add children
    newNode.left = createTree(components, low, mid - 1, !splitX);
    newNode.right = createTree(components, mid + 1, high, !splitX);
    if (newNode.left != null || newNode.right != null) {
      // pull up bounding box values to this node
      int minX = newNode.component.getBoundingBox().minX;
      int maxX = newNode.component.getBoundingBox().maxX;
      int minY = newNode.component.getBoundingBox().minY;
      int maxY = newNode.component.getBoundingBox().maxY;
      if (newNode.left != null) {
        maxX = Math.max(maxX, newNode.left.getBoundingBox().maxX);
        maxY = Math.max(maxY, newNode.left.getBoundingBox().maxY);
        minX = splitX == true  ? newNode.left.getBoundingBox().minX : Math.min(minX, newNode.left.getBoundingBox().minX);
        minY = splitX == false ? newNode.left.getBoundingBox().minY : Math.min(minY, newNode.left.getBoundingBox().minY);
      }
      if (newNode.right != null) {
        maxX = Math.max(maxX, newNode.right.getBoundingBox().maxX);
        maxY = Math.max(maxY, newNode.right.getBoundingBox().maxY);
        minX = splitX == true  ? minX : Math.min(minX, newNode.right.getBoundingBox().minX);
        minY = splitX == false ? minY : Math.min(minY, newNode.right.getBoundingBox().minY);
      }
      newNode.box = RectangleComponent2D.createComponent(minX, maxX, minY, maxY);
    }
    assert newNode.left == null || (newNode.getBoundingBox().minX <= newNode.left.getBoundingBox().minX &&
        newNode.getBoundingBox().maxX >= newNode.left.getBoundingBox().maxX &&
        newNode.getBoundingBox(). minY <= newNode.left.getBoundingBox().minY &&
        newNode.getBoundingBox().maxY >= newNode.left.getBoundingBox().maxY);
    assert newNode.right == null || (newNode.getBoundingBox().minX <= newNode.right.getBoundingBox().minX &&
        newNode.getBoundingBox().maxX >= newNode.right.getBoundingBox().maxX &&
        newNode.getBoundingBox().minY <= newNode.right.getBoundingBox().minY &&
        newNode.getBoundingBox().maxY >= newNode.right.getBoundingBox().maxY);
    return newNode;
  }

  /** Builds a Component2D  from multiple components in a tree structure */
  protected static Component2D create(Component2D... components) {
    if (components.length == 1) {
      return components[0];
    }
    return Component2DTree.createTree(components, 0, components.length - 1, true);
  }
}