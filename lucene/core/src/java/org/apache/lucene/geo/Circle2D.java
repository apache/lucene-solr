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
import org.apache.lucene.util.SloppyMath;

/**
 * 2D circle implementation containing spatial logic.
 */
class Circle2D implements Component2D {

  private final DistanceCalculator calculator;

  private Circle2D(DistanceCalculator calculator) {
    this.calculator = calculator;
  }

  @Override
  public double getMinX() {
    return calculator.getMinX();
  }

  @Override
  public double getMaxX() {
    return calculator.getMaxX();
  }

  @Override
  public double getMinY() {
    return calculator.getMinY();
  }

  @Override
  public double getMaxY() {
    return calculator.getMaxY();
  }

  @Override
  public boolean contains(double x, double y) {
    return calculator.contains(x, y);
  }

  @Override
  public Relation relate(double minX, double maxX, double minY, double maxY) {
    if (calculator.disjoint(minX, maxX, minY, maxY)) {
      return Relation.CELL_OUTSIDE_QUERY;
    }
    if (calculator.within(minX, maxX, minY, maxY)) {
      return Relation.CELL_CROSSES_QUERY;
    }
    return calculator.relate(minX, maxX, minY, maxY);
  }

  @Override
  public Relation relateTriangle(double minX, double maxX, double minY, double maxY,
                                 double ax, double ay, double bx, double by, double cx, double cy) {
    if (calculator.disjoint(minX, maxX, minY, maxY)) {
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
  public WithinRelation withinTriangle(double minX, double maxX, double minY, double maxY,
                                       double ax, double ay, boolean ab, double bx, double by, boolean bc, double cx, double cy, boolean ca) {
    // short cut, lines and points cannot contain this type of shape
    if ((ax == bx && ay == by) || (ax == cx && ay == cy) || (bx == cx && by == cy)) {
      return WithinRelation.DISJOINT;
    }

    if (calculator.disjoint(minX, maxX, minY, maxY)) {
      return WithinRelation.DISJOINT;
    }

    // if any of the points is inside the circle then we cannot be within this
    // indexed shape
    if (contains(ax, ay) || contains(bx, by) || contains(cx, cy)) {
      return WithinRelation.NOTWITHIN;
    }

    // we only check edges that belong to the original polygon. If we intersect any of them, then
    // we are not within.
    if (ab == true && calculator.intersectsLine(ax, ay, bx, by)) {
      return WithinRelation.NOTWITHIN;
    }
    if (bc == true && calculator.intersectsLine(bx, by, cx, cy)) {
      return WithinRelation.NOTWITHIN;
    }
    if (ca == true && calculator.intersectsLine(cx, cy, ax, ay)) {
      return WithinRelation.NOTWITHIN;
    }

    // check if center is within the triangle. This is the only check that returns this circle as a candidate but that is ol
    // is fine as the center must be inside to be one of the triangles.
    if (Component2D.pointInTriangle(minX, maxX, minY, maxY, calculator.geX(), calculator.getY(), ax, ay, bx, by, cx, cy) == true) {
      return WithinRelation.CANDIDATE;
    }
    return WithinRelation.DISJOINT;
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
      if (calculator.intersectsLine(a2x, a2y, b2x, b2y)) {
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
      if (Component2D.pointInTriangle(minX, maxX, minY, maxY, calculator.geX(), calculator.getY(), ax, ay, bx, by, cx, cy) == true) {
        return Relation.CELL_CROSSES_QUERY;
      }
      if (calculator.intersectsLine(ax, ay, bx, by) ||
          calculator.intersectsLine(bx, by, cx, cy) ||
          calculator.intersectsLine(cx, cy, ax, ay)) {
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
      // if one point is inside and the other outside, we know
      // already that the triangle intersect.
      return containsCount;
    }
    if (contains(cx, cy)) {
      containsCount++;
    }
    return containsCount;
  }

  private static boolean intersectsLine(double centerX, double centerY, double aX, double aY, double bX, double bY, DistanceCalculator calculator) {
    //Algorithm based on this thread : https://stackoverflow.com/questions/3120357/get-closest-point-to-a-line
    final double vectorAPX = centerX - aX;
    final double vectorAPY = centerY - aY;

    final double vectorABX = bX - aX;
    final double vectorABY = bY - aY;

    final double magnitudeAB = vectorABX * vectorABX + vectorABY * vectorABY;
    final double dotProduct = vectorAPX * vectorABX + vectorAPY * vectorABY;

    final double distance = dotProduct / magnitudeAB;

    if (distance < 0 || distance > dotProduct) {
      return false;
    }

    final double pX = aX + vectorABX * distance;
    final double pY = aY + vectorABY * distance;

    final double minX = StrictMath.min(aX, bX);
    final double minY = StrictMath.min(aY, bY);
    final double maxX = StrictMath.max(aX, bX);
    final double maxY = StrictMath.max(aY, bY);

    if (pX >= minX && pX <= maxX && pY >= minY && pY <= maxY) {
      return calculator.contains(pX, pY);
    }
    return false;
  }

  private interface DistanceCalculator {

    /** check if the point is within a distance */
    boolean contains(double x, double y);
    /** check if the line is within a distance */
    boolean intersectsLine(double aX, double aY, double bX, double bY);
    /** Relates this calculator to the provided bounding box */
    Relation relate(double minX, double maxX, double minY, double maxY);
    /** check if the bounding box is disjoint with this calculator bounding box */
    boolean disjoint(double minX, double maxX, double minY, double maxY);
    /** check if the bounding box is contains this calculator bounding box */
    boolean within(double minX, double maxX, double minY, double maxY);
    /** get min X of this calculator */
    double getMinX();
    /** get max X of this calculator */
    double getMaxX();
    /** get min Y of this calculator */
    double getMinY();
    /** get max Y of this calculator */
    double getMaxY();
    /** get center X */
    double geX();
    /** get center Y */
    double getY();
  }

  private static class CartesianDistance implements DistanceCalculator {

    private final double centerX;
    private final double centerY;
    private final double radiusSquared;
    private final XYRectangle rectangle;

    public CartesianDistance(float centerX, float centerY, float radius) {
      this.centerX = centerX;
      this.centerY = centerY;
      this.rectangle = XYRectangle.fromPointDistance(centerX, centerY, radius);
      // product performed with doubles
      this.radiusSquared = (double) radius *  radius;
    }

    @Override
    public Relation relate(double minX, double maxX, double minY, double maxY) {
      if (Component2D.containsPoint(centerX, centerY, minX, maxX, minY, maxY)) {
        if (contains(minX, minY) && contains(maxX, minY) && contains(maxX, maxY) && contains(minX, maxY)) {
          // we are fully enclosed, collect everything within this subtree
          return Relation.CELL_INSIDE_QUERY;
        }
      } else {
        // circle not fully inside, compute closest distance
        double sumOfSquaredDiffs = 0.0d;
        if (centerX < minX) {
          double diff = minX - centerX;
          sumOfSquaredDiffs += diff * diff;
        } else if (centerX > maxX) {
          double diff = maxX - centerX;
          sumOfSquaredDiffs += diff * diff;
        }
        if (centerY < minY) {
          double diff = minY - centerY;
          sumOfSquaredDiffs += diff * diff;
        } else if (centerY > maxY) {
          double diff = maxY - centerY;
          sumOfSquaredDiffs += diff * diff;
        }
        if (sumOfSquaredDiffs > radiusSquared) {
          // disjoint
          return Relation.CELL_OUTSIDE_QUERY;
        }
      }
      return Relation.CELL_CROSSES_QUERY;
    }

    @Override
    public boolean contains(double x, double y) {
      final double diffX = x - this.centerX;
      final double diffY = y - this.centerY;
      return diffX * diffX + diffY * diffY <= radiusSquared;
    }

    @Override
    public boolean intersectsLine(double aX, double aY, double bX, double bY) {
      return Circle2D.intersectsLine(centerX, centerY, aX, aY, bX, bY, this);
    }

    @Override
    public boolean disjoint(double minX, double maxX, double minY, double maxY) {
      return Component2D.disjoint(rectangle.minX, rectangle.maxX, rectangle.minY, rectangle.maxY, minX, maxX, minY, maxY);
    }

    @Override
    public boolean within(double minX, double maxX, double minY, double maxY) {
      return Component2D.within(rectangle.minX, rectangle.maxX, rectangle.minY, rectangle.maxY, minX, maxX, minY, maxY);
    }

    @Override
    public double getMinX() {
      return rectangle.minX;
    }

    @Override
    public double getMaxX() {
      return rectangle.maxX;
    }

    @Override
    public double getMinY() {
      return rectangle.minY;
    }

    @Override
    public double getMaxY() {
      return rectangle.maxY;
    }

    @Override
    public double geX() {
      return centerX;
    }

    @Override
    public double getY() {
      return centerY;
    }
  }

  private static class HaversinDistance implements DistanceCalculator {

    final double centerLat;
    final double centerLon;
    final double sortKey;
    final double axisLat;
    final Rectangle rectangle;
    final boolean crossesDateline;

    public HaversinDistance(double centerLon, double centerLat, double radius) {
      this.centerLat = centerLat;
      this.centerLon = centerLon;
      this.sortKey = GeoUtils.distanceQuerySortKey(radius);
      this.axisLat = Rectangle.axisLat(centerLat, radius);
      this.rectangle = Rectangle.fromPointDistance(centerLat, centerLon, radius);
      this.crossesDateline = rectangle.minLon > rectangle.maxLon;
    }

    @Override
    public Relation relate(double minX, double maxX, double minY, double maxY) {
      return GeoUtils.relate(minY, maxY, minX, maxX, centerLat, centerLon, sortKey, axisLat);
    }

    @Override
    public boolean contains(double x, double y) {
      return SloppyMath.haversinSortKey(y, x, this.centerLat, this.centerLon) <= sortKey;
    }


    @Override
    public boolean intersectsLine(double aX, double aY, double bX, double bY) {
      if (Circle2D.intersectsLine(centerLon, centerLat, aX, aY, bX, bY, this)) {
        return true;
      }
      if (crossesDateline) {
        double newCenterLon = (centerLon > 0) ? centerLon - 360 : centerLon + 360;
        return Circle2D.intersectsLine(newCenterLon, centerLat, aX, aY, bX, bY, this);
      }
      return false;
    }

    @Override
    public boolean disjoint(double minX, double maxX, double minY, double maxY) {
      if (crossesDateline) {
        return Component2D.disjoint(rectangle.minLon, GeoUtils.MAX_LON_INCL, rectangle.minLat, rectangle.maxLat, minX, maxX, minY, maxY)
            && Component2D.disjoint(GeoUtils.MIN_LON_INCL, rectangle.maxLon, rectangle.minLat, rectangle.maxLat, minX, maxX, minY, maxY);
      } else {
        return Component2D.disjoint(rectangle.minLon, rectangle.maxLon, rectangle.minLat, rectangle.maxLat, minX, maxX, minY, maxY);
      }
    }

    @Override
    public boolean within(double minX, double maxX, double minY, double maxY) {
      if (crossesDateline) {
        return Component2D.within(rectangle.minLon, GeoUtils.MAX_LON_INCL, rectangle.minLat, rectangle.maxLat, minX, maxX, minY, maxY)
            || Component2D.within(GeoUtils.MIN_LON_INCL, rectangle.maxLon, rectangle.minLat, rectangle.maxLat, minX, maxX, minY, maxY);
      } else {
        return Component2D.within(rectangle.minLon, rectangle.maxLon, rectangle.minLat, rectangle.maxLat, minX, maxX, minY, maxY);
      }
    }

    @Override
    public double getMinX() {
      if (crossesDateline) {
        // Component2D does not support boxes that crosses the dateline
        return GeoUtils.MIN_LON_INCL;
      }
      return rectangle.minLon;
    }

    @Override
    public double getMaxX() {
      if (crossesDateline) {
        // Component2D does not support boxes that crosses the dateline
        return GeoUtils.MAX_LON_INCL;
      }
      return rectangle.maxLon;
    }

    @Override
    public double getMinY() {
      return rectangle.minLat;
    }

    @Override
    public double getMaxY() {
      return rectangle.maxLat;
    }

    @Override
    public double geX() {
      return centerLon;
    }

    @Override
    public double getY() {
      return centerLat;
    }
  }

  /** Builds a XYCircle2D from XYCircle. Distance calculations are performed using cartesian distance.*/
  static Component2D create(XYCircle circle) {
    DistanceCalculator calculator = new CartesianDistance(circle.getX(), circle.getY(), circle.getRadius());
    return new Circle2D(calculator);
  }

  /** Builds a Circle2D from Circle. Distance calculations are performed using haversin distance. */
  static Component2D create(Circle circle) {
    DistanceCalculator calculator = new HaversinDistance(circle.getLon(), circle.getLat(), circle.getRadius());
    return new Circle2D(calculator);
  }
}
