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

import org.apache.lucene.index.PointValues;

import static org.apache.lucene.geo.GeoUtils.orient;

/**
 * 2D rectangle implementation containing cartesian spatial logic.
 *
 * @lucene.internal
 */
public class XYRectangle2D  {

  private final float minX;
  private final float maxX;
  private final float minY;
  private final float maxY;

  protected XYRectangle2D(float minX, float maxX, float minY, float maxY) {
    this.minX =  minX;
    this.maxX =  maxX;
    this.minY =  minY;
    this.maxY =  maxY;
  }

  public boolean contains(float x, float y) {
    return x >= this.minX && x <= this.maxX && y >= this.minY && y <= this.maxY;
  }

  public PointValues.Relation relate(float minX, float maxX, float minY, float maxY) {
    if (this.minX > maxX || this.maxX < minX || this.minY > maxY || this.maxY < minY) {
      return PointValues.Relation.CELL_OUTSIDE_QUERY;
    }
    if (minX >= this.minX && maxX <= this.maxX && minY >= this.minY && maxY <= this.maxY) {
      return PointValues.Relation.CELL_INSIDE_QUERY;
    }
    return PointValues.Relation.CELL_CROSSES_QUERY;
  }

  public PointValues.Relation relateTriangle(float aX, float aY, float bX, float bY, float cX, float cY) {
    // compute bounding box of triangle
    float tMinX = StrictMath.min(StrictMath.min(aX, bX), cX);
    float tMaxX = StrictMath.max(StrictMath.max(aX, bX), cX);
    float tMinY = StrictMath.min(StrictMath.min(aY, bY), cY);
    float tMaxY = StrictMath.max(StrictMath.max(aY, bY), cY);

    if (tMaxX < minX || tMinX > maxX || tMinY > maxY || tMaxY < minY) {
      return PointValues.Relation.CELL_OUTSIDE_QUERY;
    }

    int edgesContain = numberOfCorners(aX, aY, bX, bY, cX, cY);
    if (edgesContain == 3) {
      return PointValues.Relation.CELL_INSIDE_QUERY;
    } else if (edgesContain != 0) {
      return PointValues.Relation.CELL_CROSSES_QUERY;
    } else if (Tessellator.pointInTriangle(minX, minY, aX, aY, bX, bY, cX, cY)
               || edgesIntersect(aX, aY, bX, bY)
               || edgesIntersect(bX, bY, cX, cY)
               || edgesIntersect(cX, cY, aX, aY)) {
      return PointValues.Relation.CELL_CROSSES_QUERY;
    }
    return PointValues.Relation.CELL_OUTSIDE_QUERY;
  }

  private  boolean edgesIntersect(float ax, float ay, float bx, float by) {
    // shortcut: if edge is a point (occurs w/ Line shapes); simply check bbox w/ point
    if (ax == bx && ay == by) {
      return false;
    }

    // shortcut: check bboxes of edges are disjoint
    if ( Math.max(ax, bx) < minX || Math.min(ax, bx) > maxX || Math.min(ay, by) > maxY || Math.max(ay, by) < minY) {
      return false;
    }

    // top
    if (orient(ax, ay, bx, by, minX, maxY) * orient(ax, ay, bx, by, maxX, maxY) <= 0 &&
        orient(minX, maxY, maxX, maxY, ax, ay) * orient(minX, maxY, maxX, maxY, bx, by) <= 0) {
      return true;
    }

    // right
    if (orient(ax, ay, bx, by, maxX, maxY) * orient(ax, ay, bx, by, maxX, minY) <= 0 &&
        orient(maxX, maxY, maxX, minY, ax, ay) * orient(maxX, maxY, maxX, minY, bx, by) <= 0) {
      return true;
    }

    // bottom
    if (orient(ax, ay, bx, by, maxX, minY) * orient(ax, ay, bx, by, minX, minY) <= 0 &&
        orient(maxX, minY, minX, minY, ax, ay) * orient(maxX, minY, minX, minY, bx, by) <= 0) {
      return true;
    }

    // left
    if (orient(ax, ay, bx, by, minX, minY) * orient(ax, ay, bx, by, minX, maxY) <= 0 &&
        orient(minX, minY, minX, maxY, ax, ay) * orient(minX, minY, minX, maxY, bx, by) <= 0) {
      return true;
    }
    return false;
  }

  private int numberOfCorners(float ax, float ay, float bx, float by, float cx, float cy) {
    int containsCount = 0;
    if (contains(ax, ay)) {
      containsCount++;
    }
    if (contains(bx, by)) {
      containsCount++;
    }
    if (contains(cx, cy)) {
      containsCount++;
    }
    return containsCount;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("XYRectangle(x=");
    sb.append(minX);
    sb.append(" TO ");
    sb.append(maxX);
    sb.append(" y=");
    sb.append(minY);
    sb.append(" TO ");
    sb.append(maxY);
    sb.append(")");
    return sb.toString();
  }

  /** Builds a Rectangle2D from rectangle */
  public static XYRectangle2D create(XYRectangle rectangle) {
    return new XYRectangle2D((float)rectangle.minX, (float)rectangle.maxX, (float)rectangle.minY, (float)rectangle.maxY);
  }
}
