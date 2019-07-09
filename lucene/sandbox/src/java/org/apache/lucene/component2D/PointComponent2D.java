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

import org.apache.lucene.index.PointValues;

/** Represents a 2D point.
 *
 *  @lucene.internal
 * */
class PointComponent2D implements Component2D {

  /** X value */
  final int x;
  /** Y value */
  final int y;
  /**  bounding box of the point */
  final RectangleComponent2D box;

  private PointComponent2D(int x, int y) {
    this.x = x;
    this.y = y;
    box = RectangleComponent2D.createComponent(x, x, y, y);
  }

  @Override
  public boolean contains(int x, int y) {
    return this.x == x && this.y == y;
  }

  @Override
  public PointValues.Relation relate(int minX, int maxX, int minY, int maxY) {
    if (box.within(minX, maxX, minY, maxY)) {
      if (minY == maxY && y == minY && minX == maxX && maxX == x) {
        return PointValues.Relation.CELL_INSIDE_QUERY;
      }
      return PointValues.Relation.CELL_CROSSES_QUERY;
    }
    return PointValues.Relation.CELL_OUTSIDE_QUERY;
  }

  @Override
  public PointValues.Relation relateTriangle(int minX, int maxX, int minY, int maxY, int aX, int aY, int bX, int bY, int cX, int cY) {
    if (aX == bX && bX == cX && aY == bY && bY == cY)  {
      // indexed "triangle" is a point: shortcut by checking contains
      return contains(minX, minY)  ? PointValues.Relation.CELL_INSIDE_QUERY : PointValues.Relation.CELL_OUTSIDE_QUERY;
    }
    if (RectangleComponent2D.containsPoint(x, y, minX, maxX, minY, maxY) &&
        Component2D.pointInTriangle(minX, maxX, minY, maxY, x, y, aX, aY, bX, bY, cX, cY)) {
      return PointValues.Relation.CELL_CROSSES_QUERY;
    }
    return PointValues.Relation.CELL_OUTSIDE_QUERY;
  }

  @Override
  public RectangleComponent2D getBoundingBox() {
    return box;
  }

  @Override
  public int hashCode() {
    int result =  Integer.hashCode(x);
    result = 31 * result + Integer.hashCode(y);
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PointComponent2D pointComponent = (PointComponent2D) o;
    return x == pointComponent.x && y == pointComponent.y;
  }

  @Override
  public String toString() {
    return "PointComponent2D{" +
        "Point=[" + x + "," + y +"]" +
        '}';
  }

  /** Builds a Component2D from a x and y */
  protected static Component2D createComponent(int x, int y) {
    return new PointComponent2D(x, y);
  }
}
