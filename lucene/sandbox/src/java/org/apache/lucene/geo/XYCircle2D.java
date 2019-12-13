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
 * 2D rectangle implementation containing XY spatial logic.
 *
 * @lucene.internal
 */
public class XYCircle2D implements Component2D {

  private final double minX;
  private final double maxX;
  private final double minY;
  private final double maxY;
  private final double x;
  private final double y;
  private final double distanceSquared;


  protected XYCircle2D(double x, double y, double distance) {
    this.x = x;
    this.y = y;
    this.minX = x - distance;
    this.maxX = x + distance;
    this.minY = y - distance;
    this.maxY = y + distance;
    this.distanceSquared = distance * distance;
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
    final double diffX = this.x - x;
    final double diffY = this.y - y;
    return diffX * diffX + diffY * diffY <= distanceSquared;
  }

  @Override
  public Relation relate(double minX, double maxX, double minY, double maxY) {
    if (this.minX > maxX || this.maxX < minX || this.minY > maxY || this.maxY < minY) {
      return Relation.CELL_OUTSIDE_QUERY;
    }
    if (minX >= this.minX && maxX <= this.maxX && minY >= this.minY && maxY <= this.maxY) {
      return Relation.CELL_INSIDE_QUERY;
    }
    return Relation.CELL_CROSSES_QUERY;
  }

  @Override
  public Relation relateTriangle(double minX, double maxX, double minY, double maxY, double ax, double ay, double bx, double by, double cx, double cy) {
    if (Component2D.disjoint(this.minX, this.maxX, this.minY, this.maxY, minX, maxX, minY, maxY)) {
      return Relation.CELL_OUTSIDE_QUERY;
    }
    if (ax == bx && bx == cx && ay == by && by == cy) {
      // indexed "triangle" is a point: shortcut by checking contains
      return contains(ax, ay) ? Relation.CELL_INSIDE_QUERY : Relation.CELL_OUTSIDE_QUERY;
    } else if (ax == cx && ay == cy) {
      // indexed "triangle" is a line segment: shortcut by calling appropriate method
      return relateIndexedLineSegment(minX, maxX, minY, maxY, ax, ay, bx, by);
    } else if (ax == bx && ay == by) {
      // indexed "triangle" is a line segment: shortcut by calling appropriate method
      return relateIndexedLineSegment(minX, maxX, minY, maxY, bx, by, cx, cy);
    } else if (bx == cx && by == cy) {
      // indexed "triangle" is a line segment: shortcut by calling appropriate method
      return relateIndexedLineSegment(minX, maxX, minY, maxY, cx, cy, ax, ay);
    }
    // indexed "triangle" is a triangle:
    return relateIndexedTriangle(minX, maxX, minY, maxY, ax, ay, bx, by, cx, cy);
  }

  @Override
  public WithinRelation withinTriangle(double minX, double maxX, double minY, double maxY, double ax, double ay, boolean ab, double bx, double by, boolean bc, double cx, double cy, boolean ca) {
    // short cut, lines and points cannot contain this type of shape
    if ((ax == bx && ay == by) || (ax == cx && ay == cy) || (bx == cx && by == cy)) {
      return WithinRelation.DISJOINT;
    }

    if (Component2D.disjoint(this.minX, this.maxX, this.minY, this.maxY, minX, maxX, minY, maxY)) {
      return WithinRelation.DISJOINT;
    }

    // if any of the points is inside the polygon, the polygon cannot be within this indexed
    // shape because points belong to the original indexed shape.
    if (contains(ax, ay) || contains(bx, by) || contains(cx, cy)) {
      return WithinRelation.NOTWITHIN;
    }

    WithinRelation relation = WithinRelation.DISJOINT;
    // if any of the edges intersects an the edge belongs to the shape then it cannot be within.
    // if it only intersects edges that do not belong to the shape, then it is a candidate
    // we skip edges at the dateline to support shapes crossing it
    if (intersectsLine(ax, ay, bx, by)) {
      if (ab == true) {
        return WithinRelation.NOTWITHIN;
      } else {
        relation = WithinRelation.CANDIDATE;
      }
    }

    if (intersectsLine(bx, by, cx, cy)) {
      if (bc == true) {
        return WithinRelation.NOTWITHIN;
      } else {
        relation = WithinRelation.CANDIDATE;
      }
    }
    if (intersectsLine(cx, cy, ax, ay)) {
      if (ca == true) {
        return WithinRelation.NOTWITHIN;
      } else {
        relation = WithinRelation.CANDIDATE;
      }
    }

    // if any of the edges crosses and edge that does not belong to the shape
    // then it is a candidate for within
    if (relation == WithinRelation.CANDIDATE) {
      return WithinRelation.CANDIDATE;
    }

    // Check if shape is within the triangle
    if (Component2D.pointInTriangle(minX, maxX, minY, maxY, x, y, ax, ay, bx, by, cx, cy) == true) {
      return WithinRelation.CANDIDATE;
    }
    return relation;
  }

  /** relates an indexed line segment (a "flat triangle") with the polygon */
  private Relation relateIndexedLineSegment(double minX, double maxX, double minY, double maxY,
                                            double a2x, double a2y, double b2x, double b2y) {
    // check endpoints of the line segment
    int numCorners = 0;
    if (contains(a2x, a2y)) {
      ++numCorners;
    }
    if (contains(b2x, b2y)) {
      ++numCorners;
    }

    //intersectsLine(aX, aY, bX, bY) || intersectsLine(bX, bY, cX, cY) || intersectsLine(cX, cY, aX, aY)
    if (numCorners == 2) {
      if (intersectsLine(a2x, a2y, b2x, b2y)) {
        return Relation.CELL_CROSSES_QUERY;
      }
      return Relation.CELL_INSIDE_QUERY;
    } else if (numCorners == 0) {
      if (intersectsLine(a2x, a2y, b2x, b2y)) {
        return Relation.CELL_CROSSES_QUERY;
      }
      return Relation.CELL_OUTSIDE_QUERY;
    }
    return Relation.CELL_CROSSES_QUERY;
  }

  /** relates an indexed triangle with the polygon */
  private Relation relateIndexedTriangle(double minX, double maxX, double minY, double maxY,
                                         double ax, double ay, double bx, double by, double cx, double cy) {
    // check each corner: if < 3 && > 0 are present, its cheaper than crossesSlowly
    int numCorners = numberOfTriangleCorners(ax, ay, bx, by, cx, cy);
    if (numCorners == 3) {
      if (intersectsLine(ax, ay, bx, by) || intersectsLine(bx, by, cx, cy) || intersectsLine(cx, cy, ax, ay)) {
        return Relation.CELL_CROSSES_QUERY;
      }
      return Relation.CELL_INSIDE_QUERY;
    } else if (numCorners == 0) {
      if (Component2D.pointInTriangle(minX, maxX, minY, maxY, x, y, ax, ay, bx, by, cx, cy) == true) {
        return Relation.CELL_CROSSES_QUERY;
      }
      if (intersectsLine(ax, ay, bx, by) || intersectsLine(bx, by, cx, cy) || intersectsLine(cx, cy, ax, ay)) {
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
    final double[] vectorAP = new double[] {x - aX, y - aY};
    final double[] vectorAB = new double[] {bX - aX, bY - aY};

    final double magnitudeAB = vectorAB[0] * vectorAB[0] + vectorAB[1] * vectorAB[1];
    final double dotProduct = vectorAP[0] * vectorAB[0] + vectorAP[1] * vectorAB[1];

    final double distance = dotProduct / magnitudeAB;

    if (distance < 0 || distance > dotProduct)
    {
      return false;
    }

    final double pX = aX + vectorAB[0] * distance;
    final double pY = aY + vectorAB[1] * distance;

    final double minX = StrictMath.min(aX, bX);
    final double minY = StrictMath.min(aY, bY);
    final double maxX = StrictMath.max(aX, bX);
    final double maxY = StrictMath.max(aY, bY);

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

  /** Builds a XYCircle2D from XYCircle */
  public static Component2D create(XYCircle circle) {
    return new XYCircle2D(circle.getX(), circle.getY(), circle.getRadius());
  }

}
