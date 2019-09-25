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


import java.util.Objects;

import org.apache.lucene.index.PointValues.Relation;

/**
 * 2D rectangle implementation containing XY spatial logic. This is WIP!!
 *
 * @lucene.internal
 */
public class XYCircle2D {
  protected final double minX;
  protected final double maxX;
  protected final double minY;
  protected final double maxY;
  protected final double x;
  protected final double y;
  protected final double distanceSquared;


  protected XYCircle2D(double x, double y, double distance) {
    this.x = x;
    this.y = y;
    minX = x - distance;
    maxX = x + distance;
    minY = y - distance;
    maxY = y + distance;
    this.distanceSquared = distance * distance;
  }

  /** Builds a Rectangle2D from rectangle */
  public static XYCircle2D create(XYCircle circle) {
    return new XYCircle2D(circle.getX(), circle.getY(), circle.getRadius());
  }


  /** Checks if the rectangle contains the provided point **/
  public boolean contains(double x, double y) {
    double diffX = this.x - x;
    double diffY = this.y - y;
    return diffX * diffX + diffY * diffY <= distanceSquared;
  }

  /** compare this to a provided rectangle bounding box **/
  public Relation relate(double minX, double maxX, double minY, double maxY) {
    if (this.minX > maxX || this.maxX < minX || this.minY > maxY || this.maxY < minY) {
      return Relation.CELL_OUTSIDE_QUERY;
    }
    if (minX >= this.minX && maxX <= this.maxX && minY >= this.minY && maxY <= this.maxY) {
      return Relation.CELL_INSIDE_QUERY;
    }
    return Relation.CELL_CROSSES_QUERY;
  }


  /** Checks if the rectangle intersects the provided triangle **/
  public Relation relateTriangle(double aX, double aY, double bX, double bY, double cX, double cY) {
    // 1. query contains any triangle points
    int numCorners = numberOfTriangleCorners(aX, aY, bX, bY, cX, cY);
    if (numCorners == 3) {
      return Relation.CELL_INSIDE_QUERY;
    } else if (numCorners == 0) {
      if (Tessellator.pointInTriangle(x, y, aX, aY, bX, bY, cX, cY)) {
        return Relation.CELL_CROSSES_QUERY;
      }
      if (intersectsLine(aX, aY, bX, bY) || intersectsLine(bX, bY, cX, cY) || intersectsLine(cX, cY, aX, aY)) {
        return Relation.CELL_CROSSES_QUERY;
      }
      return Relation.CELL_OUTSIDE_QUERY;
    }
    return Relation.CELL_CROSSES_QUERY;
  }

  private int numberOfTriangleCorners(double ax, double ay, double bx, double by, double cx, double cy) {
    int containsCount = 0;
    if (contains(ax, ay)) {
      containsCount++;
    }
    if (contains(bx, by)) {
      containsCount++;
    }
    if (containsCount == 1) {
      return containsCount;
    }
    if (contains(cx, cy)) {
      containsCount++;
    }
    return containsCount;
  }

  private boolean intersectsLine(double aX, double aY, double bX, double bY) {
    //Algorithm based on this thread : https://stackoverflow.com/questions/3120357/get-closest-point-to-a-line
    double[] vectorAP = new double[] {x - aX, y - aY};
    double[] vectorAB = new double[] {bX - aX, bY - aY};

    double magnitudeAB = vectorAB[0] * vectorAB[0] + vectorAB[1] * vectorAB[1];
    double dotProduct = vectorAP[0] * vectorAB[0] + vectorAP[1] * vectorAB[1];

    double distance = dotProduct / magnitudeAB;

    if (distance < 0 || distance > dotProduct)
    {
      return false;
    }

    double pX = aX + vectorAB[0] * distance;
    double pY = aY + vectorAB[1] * distance;

    double minX = StrictMath.min(aX, bX);
    double minY = StrictMath.min(aY, bY);
    double maxX = StrictMath.max(aX, bX);
    double maxY = StrictMath.max(aY, bY);

    if (pX >= minX && pX <= maxX && pY >= minY && pY <= maxY) {
      return contains(pX, pY);
    }
    return false;
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof XYCircle2D)) return false;
    XYCircle2D that = (XYCircle2D) o;
    return x == that.x &&
        y == that.y &&
        distanceSquared == that.distanceSquared;
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(x, y, distanceSquared);
    return result;
  }
}
