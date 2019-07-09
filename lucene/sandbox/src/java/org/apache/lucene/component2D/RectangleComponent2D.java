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

import org.apache.lucene.geo.GeoUtils;
import org.apache.lucene.index.PointValues;


/** Represents a 2D rectangle.
 *
 *  @lucene.internal
 * */
public class RectangleComponent2D implements Component2D {
  /** maximum X value */
  public final int minX;
  /** minimum X value */
  public final int maxX;
  /** maximum Y value */
  public final int minY;
  /** minimum Y value */
  public final int maxY;


  private RectangleComponent2D(int minX, int maxX, int minY, int maxY) {
    this.minX = minX;
    this.maxX = maxX;
    this.minY = minY;
    this.maxY = maxY;
    assert maxX >= minX;
    assert maxY >= maxY;
  }

  @Override
  public boolean contains(int x, int y) {
    return x >= minX && x <= maxX && y >= minY && y <= maxY;
  }

  @Override
  public PointValues.Relation relate(int minX, int maxX, int minY, int maxY) {
    if (disjoint(minX, maxX, minY, maxY)) {
      return PointValues.Relation.CELL_OUTSIDE_QUERY;
    }
    if (contains(minX, maxX, minY, maxY)) {
      return PointValues.Relation.CELL_INSIDE_QUERY;
    }
    return PointValues.Relation.CELL_CROSSES_QUERY;
  }

  @Override
  public PointValues.Relation relateTriangle(int minX, int maxX, int minY, int maxY, int aX, int aY, int bX, int bY, int cX, int cY) {
      if (disjoint(minX, maxX, minY, maxY)) {
      return PointValues.Relation.CELL_OUTSIDE_QUERY;
    }
    int numCorners = 0;
    if (contains(aX, aY)) {
      ++numCorners;
    }
    if (contains(bX, bY)) {
      ++numCorners;
    }
    if (contains(cX, cY)) {
      ++numCorners;
    }

    if (numCorners == 3) {
      return PointValues.Relation.CELL_INSIDE_QUERY;
    } else if (numCorners == 0) {
      if (Component2D.pointInTriangle(minX, maxX, minY, maxY, this.minX, this.minY, aX, aY, bX, bY, cX, cY) ||
          intersectsEdge(aX, aY, bX, bY) || intersectsEdge(bX, bY, cX, cY) || intersectsEdge(cX, cY, aX, aY)) {
        return PointValues.Relation.CELL_CROSSES_QUERY;
      }
      return PointValues.Relation.CELL_OUTSIDE_QUERY;
    }
    return PointValues.Relation.CELL_CROSSES_QUERY;
  }

  @Override
  public RectangleComponent2D getBoundingBox() {
    return this;
  }

  /** returns true if this {@link RectangleComponent2D} is disjoint with the provided rectangle (defined by minX, maxX, minY, maxY) */
  public  boolean disjoint(final int minX, final int maxX, final int minY, final int maxY) {
    return (maxX < this.minX || minX > this.maxX || maxY < this.minY || minY > this.maxY);
  }

  /** returns true if this {@link RectangleComponent2D} is within the rectangle (defined by minX, maxX, minY, maxY) */
  public boolean within(final int minX, final int maxX, final int minY, final int maxY) {
    return minX <= this.minX && maxX >= this.maxX && minY <= this.minY && maxY >= this.maxY;
  }

  /** returns true if this {@link RectangleComponent2D} contains the rectangle (defined by minX, maxX, minY, maxY) */
  public boolean contains(final int minX, final int maxX, final int minY, final int maxY) {
    return minX >= this.minX && maxX <= this.maxX && minY >= this.minY && maxY <= this.maxY;
  }

  /** returns true if the edge (defined by (aX, aY) (bX, bY)) intersects the query */
  private  boolean intersectsEdge(int aX, int aY, int bX, int bY) {
    // shortcut: check bboxes of edges are disjoint
    if (disjoint(Math.min(aX, bX), Math.max(aX, bX), Math.min(aY, bY), Math.max(aY, bY))) {
      return false;
    }

    // top
    if (GeoUtils.orient(aX, aY, bX, bY, minX, maxY) * GeoUtils.orient(aX, aY, bX, bY, maxX, maxY) <= 0 &&
        GeoUtils.orient(minX, maxY, maxX, maxY, aX, aY) * GeoUtils.orient(minX, maxY, maxX, maxY, bX, bY) <= 0) {
      return true;
    }

    // right
    if (GeoUtils.orient(aX, aY, bX, bY, maxX, maxY) * GeoUtils.orient(aX, aY, bX, bY, maxX, minY) <= 0 &&
        GeoUtils.orient(maxX, maxY, maxX, minY, aX, aY) * GeoUtils.orient(maxX, maxY, maxX, minY, bX, bY) <= 0) {
      return true;
    }

    // bottom
    if (GeoUtils.orient(aX, aY, bX, bY, maxX, minY) * GeoUtils.orient(aX, aY, bX, bY, minX, minY) <= 0 &&
        GeoUtils.orient(maxX, minY, minX, minY, aX, aY) * GeoUtils.orient(maxX, minY, minX, minY, bX, bY) <= 0) {
      return true;
    }

    // left
    if (GeoUtils.orient(aX, aY, bX, bY, minX, minY) * GeoUtils.orient(aX, aY, bX, bY, minX, maxY) <= 0 &&
        GeoUtils.orient(minX, minY, minX, maxY, aX, aY) * GeoUtils.orient(minX, minY, minX, maxY, bX, bY) <= 0) {
      return true;
    }
    return false;
  }

  /** returns true if this {@link RectangleComponent2D} contains the encoded lat lon point */
  public static  boolean containsPoint(final int x, final int y, final int minX, final int maxX, final int minY, final int maxY)  {
    return x >= minX && x <= maxX && y >= minY && y <= maxY;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    RectangleComponent2D rectangle = (RectangleComponent2D) o;
    return minX == rectangle.minX &&
           maxX == rectangle.maxX &&
           minY == rectangle.minY &&
           maxY == rectangle.maxY;
  }

  @Override
  public int hashCode() {
    int result;
    long temp;
    temp = Integer.hashCode(minX);
    result = (int) (temp ^ (temp >>> 32));
    temp = Integer.hashCode(maxX);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    temp = Integer.hashCode(minY);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    temp = Integer.hashCode(maxY);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    return result;
  }

  @Override
  public String toString() {
    StringBuilder b = new StringBuilder();
    b.append("RectangleComponent2D(lat=");
    b.append(minX);
    b.append(" TO ");
    b.append(maxX);
    b.append(" lon=");
    b.append(minY);
    b.append(" TO ");
    b.append(maxY);
    b.append(")");
    return b.toString();
  }

  /** Builds a Component2D from a rectangle */
  protected static RectangleComponent2D createComponent(int minX, int maxX, int minY, int maxY) {
    return new RectangleComponent2D(minX, maxX, minY, maxY);
  }
}
