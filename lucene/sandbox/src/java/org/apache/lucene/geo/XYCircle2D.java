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
  private final double centerX;
  private final double centerY;
  private final double distanceSquared;


  protected XYCircle2D(double centerX, double centerY, double distance) {
    this.centerX = centerX;
    this.centerY = centerY;
    this.minX = centerX - distance;
    this.maxX = centerX + distance;
    this.minY = centerY - distance;
    this.maxY = centerY + distance;
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
    return cartesianDistanceSquared(x, y, this.centerX, this.centerY) <= distanceSquared;
  }

  @Override
  public Relation relate(double minX, double maxX, double minY, double maxY) {
    if (Component2D.disjoint(this.minX, this.maxX, this.minY, this.maxY, minX, maxX, minY, maxY)) {
      return Relation.CELL_OUTSIDE_QUERY;
    }
    if (Component2D.within(this.minX, this.maxX, this.minY, this.maxY, minX, maxX, minY, maxY)) {
      return Relation.CELL_CROSSES_QUERY;
    }
    return relate(minX, maxX, minY, maxY, this.centerX, this.centerY, this.distanceSquared);
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
      return relateIndexedLineSegment(ax, ay, bx, by);
    } else if (ax == bx && ay == by) {
      // indexed "triangle" is a line segment: shortcut by calling appropriate method
      return relateIndexedLineSegment(bx, by, cx, cy);
    } else if (bx == cx && by == cy) {
      // indexed "triangle" is a line segment: shortcut by calling appropriate method
      return relateIndexedLineSegment(cx, cy, ax, ay);
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
    if (intersectsLine(ax, ay, bx, by, this.centerX, this.centerY, this.distanceSquared)) {
      if (ab == true) {
        return WithinRelation.NOTWITHIN;
      } else {
        relation = WithinRelation.CANDIDATE;
      }
    }

    if (intersectsLine(bx, by, cx, cy, this.centerX, this.centerY, this.distanceSquared)) {
      if (bc == true) {
        return WithinRelation.NOTWITHIN;
      } else {
        relation = WithinRelation.CANDIDATE;
      }
    }
    if (intersectsLine(cx, cy, ax, ay, this.centerX, this.centerY, this.distanceSquared)) {
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
    if (Component2D.pointInTriangle(minX, maxX, minY, maxY, centerX, centerY, ax, ay, bx, by, cx, cy) == true) {
      return WithinRelation.CANDIDATE;
    }
    return relation;
  }

  /** relates an indexed line segment (a "flat triangle") with the polygon */
  private Relation relateIndexedLineSegment(double a2x, double a2y, double b2x, double b2y) {
    // check endpoints of the line segment
    int numCorners = 0;
    if (contains(a2x, a2y)) {
      ++numCorners;
    }
    if (contains(b2x, b2y)) {
      ++numCorners;
    }

    if (numCorners == 2) {
      return Relation.CELL_INSIDE_QUERY;
    } else if (numCorners == 0) {
      if (intersectsLine(a2x, a2y, b2x, b2y, this.centerX, this.centerY, this.distanceSquared)) {
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
      return Relation.CELL_INSIDE_QUERY;
    } else if (numCorners == 0) {
      if (Component2D.pointInTriangle(minX, maxX, minY, maxY, centerX, centerY, ax, ay, bx, by, cx, cy) == true) {
        return Relation.CELL_CROSSES_QUERY;
      }
      if (intersectsLine(ax, ay, bx, by, this.centerX, this.centerY, this.distanceSquared) ||
          intersectsLine(bx, by, cx, cy, this.centerX, this.centerY, this.distanceSquared) ||
          intersectsLine(cx, cy, ax, ay, this.centerX, this.centerY, this.distanceSquared)) {
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

  // This methods in a new helper class XYUtil?
  private static boolean intersectsLine(double aX, double aY, double bX, double bY,
                                 double centerX, double centerY, double distanceSquared) {
    //Algorithm based on this thread : https://stackoverflow.com/questions/3120357/get-closest-point-to-a-line
    final double[] vectorAP = new double[] {centerX - aX, centerY - aY};
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
      return cartesianDistanceSquared(pX, pY, centerX, centerY) <= distanceSquared;
    }
    return false;
  }

  private static double cartesianDistanceSquared(double x1, double y1, double x2, double y2) {
    final double diffX = x1 - x2;
    final double diffY = y1 - y2;
    return diffX * diffX + diffY * diffY;
  }

  private static Relation relate(
      double minX, double maxX, double minY, double maxY,
      double x, double y, double distanceSquared) {

    if (Component2D.containsPoint(x, y, minX, maxX, minY, maxY)) {
      if (cartesianDistanceSquared(x, y, minX, minY) <= distanceSquared &&
          cartesianDistanceSquared(x, y, maxX, minY) <= distanceSquared &&
          cartesianDistanceSquared(x, y, maxX, maxY) <= distanceSquared &&
          cartesianDistanceSquared(x, y, minX, maxY) <= distanceSquared) {
        // we are fully enclosed, collect everything within this subtree
        return Relation.CELL_INSIDE_QUERY;
      }
    } else {
      // circle not fully inside, compute closest distance
      double sumOfSquaredDiffs = 0.0d;
      if (x < minX) {
        double diff = minX - x;
        sumOfSquaredDiffs += diff * diff;
      } else if (x > maxX) {
        double diff = maxX - x;
        sumOfSquaredDiffs += diff * diff;
      }
      if (y < minY) {
        double diff = minY - y;
        sumOfSquaredDiffs += diff * diff;
      } else if (y > maxY) {
        double diff = maxY - y;
        sumOfSquaredDiffs += diff * diff;
      }
      if (sumOfSquaredDiffs > distanceSquared) {
        // disjoint
        return Relation.CELL_OUTSIDE_QUERY;
      }
    }
    return Relation.CELL_CROSSES_QUERY;
  }

  /** Builds a XYCircle2D from XYCircle */
  public static Component2D create(XYCircle circle) {
    return new XYCircle2D(circle.getX(), circle.getY(), circle.getRadius());
  }
}
