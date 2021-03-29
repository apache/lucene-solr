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
  public boolean intersectsLine(double minX, double maxX, double minY, double maxY,
                                double aX, double aY, double bX, double bY) {
    if (calculator.disjoint(minX, maxX, minY, maxY)) {
      return false;
    }
    return contains(aX, aY) || contains(bX, bY) ||
        calculator.intersectsLine(aX, aY, bX, bY);
  }

  @Override
  public boolean intersectsTriangle(double minX, double maxX, double minY, double maxY,
                                    double aX, double aY, double bX, double bY, double cX, double cY) {
    if (calculator.disjoint(minX, maxX, minY, maxY)) {
      return false;
    }
    return contains(aX, aY) || contains(bX, bY) || contains(cX, cY) ||
        Component2D.pointInTriangle(minX, maxX, minY, maxY, calculator.geX(), calculator.getY(), aX, aY, bX, bY, cX, cY) ||
        calculator.intersectsLine(aX, aY, bX, bY) ||
        calculator.intersectsLine(bX, bY, cX, cY) ||
        calculator.intersectsLine(cX, cY, aX, aY);
  }

  @Override
  public boolean containsLine(double minX, double maxX, double minY, double maxY,
                                double aX, double aY, double bX, double bY) {
    if (calculator.disjoint(minX, maxX, minY, maxY)) {
      return false;
    }
    return contains(aX, aY) && contains(bX, bY);
  }

  @Override
  public boolean containsTriangle(double minX, double maxX, double minY, double maxY,
                                    double aX, double aY, double bX, double bY, double cX, double cY) {
    if (calculator.disjoint(minX, maxX, minY, maxY)) {
      return false;
    }
    return contains(aX, aY) && contains(bX, bY) && contains(cX, cY);
  }

  @Override
  public WithinRelation withinPoint(double x, double y) {
    return contains(x, y) ? WithinRelation.NOTWITHIN : WithinRelation.DISJOINT;
  }

  @Override
  public WithinRelation withinLine(double minX, double maxX, double minY, double maxY,
                                   double aX, double aY, boolean ab, double bX, double bY) {
    if (calculator.disjoint(minX, maxX, minY, maxY)) {
      return WithinRelation.DISJOINT;
    }
    if (ab == true && calculator.intersectsLine(aX, aY, bX, bY)) {
      return WithinRelation.NOTWITHIN;
    }
    return WithinRelation.DISJOINT;
  }

  @Override
  public WithinRelation withinTriangle(double minX, double maxX, double minY, double maxY,
                                       double aX, double aY, boolean ab, double bX, double bY, boolean bc, double cX, double cY, boolean ca) {
    if (calculator.disjoint(minX, maxX, minY, maxY)) {
      return WithinRelation.DISJOINT;
    }

    // if any of the points is inside the circle then we cannot be within this
    // indexed shape
    if (contains(aX, aY) || contains(bX, bY) || contains(cX, cY)) {
      return WithinRelation.NOTWITHIN;
    }

    // we only check edges that belong to the original polygon. If we intersect any of them, then
    // we are not within.
    if (ab == true && calculator.intersectsLine(aX, aY, bX, bY)) {
      return WithinRelation.NOTWITHIN;
    }
    if (bc == true && calculator.intersectsLine(bX, bY, cX, cY)) {
      return WithinRelation.NOTWITHIN;
    }
    if (ca == true && calculator.intersectsLine(cX, cY, aX, aY)) {
      return WithinRelation.NOTWITHIN;
    }

    // check if center is within the triangle. This is the only check that returns this circle as a candidate but that is ol
    // is fine as the center must be inside to be one of the triangles.
    if (Component2D.pointInTriangle(minX, maxX, minY, maxY, calculator.geX(), calculator.getY(), aX, aY, bX, bY, cX, cY) == true) {
      return WithinRelation.CANDIDATE;
    }
    return WithinRelation.DISJOINT;
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

    if (distance < 0 || distance > 1) {
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
      if (Component2D.containsPoint(x, y, rectangle.minX, rectangle.maxX, rectangle.minY, rectangle.maxY)) {
        final double diffX = x - this.centerX;
        final double diffY = y - this.centerY;
        return diffX * diffX + diffY * diffY <= radiusSquared;
      }
      return false;
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
      if (crossesDateline) {
        if (Component2D.containsPoint(x, y, rectangle.minLon, GeoUtils.MAX_LON_INCL, rectangle.minLat, rectangle.maxLat) ||
            Component2D.containsPoint(x, y, GeoUtils.MIN_LON_INCL, rectangle.maxLon, rectangle.minLat, rectangle.maxLat)) {
          return SloppyMath.haversinSortKey(y, x, this.centerLat, this.centerLon) <= sortKey;
        }
      } else {
        if (Component2D.containsPoint(x, y, rectangle.minLon, rectangle.maxLon, rectangle.minLat, rectangle.maxLat)) {
          return SloppyMath.haversinSortKey(y, x, this.centerLat, this.centerLon) <= sortKey;
        }
      }
      return false;
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
