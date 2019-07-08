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
import org.apache.lucene.index.PointValues;

/** Represents a 2D line.
 *
 * @lucene.internal
 * */
class LineComponent2D implements Component2D {

  /** X values, used for equality and hashcode */
  private final int[] Xs;
  /** Y values, used for equality and hashcode */
  private final int[] Ys;
  /** edge tree representing the line */
  private final EdgeTree tree;
  /**  bounding box of the line */
  private final RectangleComponent2D box;

  protected LineComponent2D(int[] Xs, int[] Ys, RectangleComponent2D box) {
    this.Xs = Xs;
    this.Ys = Ys;
    this.tree = EdgeTree.createTree(Xs, Ys);
    this.box = box;
  }

  @Override
  public boolean contains(int x, int y) {
    if (box.contains(x, y)) {
      return tree.pointInEdge(x, y);
    }
    return false;
  }

  @Override
  public PointValues.Relation relate(int minX, int maxX, int minY, int maxY) {
    if (box.disjoint(minX, maxX, minY, maxY)) {
      return PointValues.Relation.CELL_OUTSIDE_QUERY;
    }
    if (box.within(minX, maxX, minY, maxY) || tree.crossesBox(minX, maxX, minY, maxY, true)) {
      return PointValues.Relation.CELL_CROSSES_QUERY;
    }
    return PointValues.Relation.CELL_OUTSIDE_QUERY;
  }

  @Override
  public PointValues.Relation relateTriangle(int minX, int maxX, int minY, int maxY, int aX, int aY, int bX, int bY, int cX, int cY) {
    if (box.disjoint(minX, maxX, minY, maxY)) {
      return PointValues.Relation.CELL_OUTSIDE_QUERY;
    }
    if (aX == bX && bX == cX && aY == bY && bY == cY) {
      // indexed "triangle" is a point: check if point lies on any line segment
      if (contains(aX, aY)) {
        return PointValues.Relation.CELL_INSIDE_QUERY;
      }
    } else if ((aX == cX && aY == cY) || (bX == cX && bY == cY)) {
      // indexed "triangle" is a line:
      if (tree.crossesLine(aX, aY, bX, bY)) {
        return PointValues.Relation.CELL_CROSSES_QUERY;
      }
      return PointValues.Relation.CELL_OUTSIDE_QUERY;
    } else if (Component2D.pointInTriangle(minX, maxX, minY, maxY, tree.x1, tree.y1, aX, aY, bX, bY, cX, cY) == true ||
        tree.crossesTriangle(minX, maxX, minY, maxY, aX, aY, bX, bY, cX, cY)) {
      // indexed "triangle" is a triangle:
      return PointValues.Relation.CELL_CROSSES_QUERY;
    }
    return PointValues.Relation.CELL_OUTSIDE_QUERY;
  }

  @Override
  public RectangleComponent2D getBoundingBox() {
    return box;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    LineComponent2D lineComponent = (LineComponent2D) o;
    return Arrays.equals(Xs, lineComponent.Xs) && Arrays.equals(Ys, lineComponent.Ys);
  }

  @Override
  public int hashCode() {
    int result;
    long temp;
    temp = Arrays.hashCode(Xs);
    result = (int) (temp ^ (temp >>> 32));
    temp =  Arrays.hashCode(Ys);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return "LineComponent2D{" +
        "Xs=" + Arrays.toString(Xs) + ", Ys=" + Arrays.toString(Ys) +
        '}';
  }
}
