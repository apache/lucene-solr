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
 * 2D Geometry object that supports spatial relationships with bounding boxes,
 * triangles and points.
 *
 * @lucene.internal
 **/
public interface Component2D {

  /** min X value for the component **/
  double getMinX();

  /** max X value for the component **/
  double getMaxX();

  /** min Y value for the component **/
  double getMinY();

  /** max Y value for the component **/
  double getMaxY();

  /** relates this component2D with a point **/
  boolean contains(double x, double y);

  /** relates this component2D with a bounding box **/
  PointValues.Relation relate(double minX, double maxX, double minY, double maxY);

  /** relates this component2D with a triangle **/
  PointValues.Relation relateTriangle(double minX, double maxX, double minY, double maxY,
                                      double aX, double aY, double bX, double bY, double cX, double cY);

  /** relates this component2D with a triangle **/
  default PointValues.Relation relateTriangle(double aX, double aY, double bX, double bY, double cX, double cY) {
    double minY = StrictMath.min(StrictMath.min(aY, bY), cY);
    double minX = StrictMath.min(StrictMath.min(aX, bX), cX);
    double maxY = StrictMath.max(StrictMath.max(aY, bY), cY);
    double maxX = StrictMath.max(StrictMath.max(aX, bX), cX);
    return relateTriangle(minX, maxX, minY, maxY, aX, aY, bX, bY, cX, cY);
  }

  /** Compute whether the bounding boxes are disjoint **/
  static  boolean disjoint(double minX1, double maxX1, double minY1, double maxY1, double minX2, double maxX2, double minY2, double maxY2) {
    return (maxY1 < minY2 || minY1 > maxY2 || maxX1 < minX2 || minX1 > maxX2);
  }

  /** Compute whether the first bounding box 1 is within the second bounding box **/
  static boolean within(double minX1, double maxX1, double minY1, double maxY1, double minX2, double maxX2, double minY2, double maxY2) {
    return (minY2 <= minY1 && maxY2 >= maxY1 && minX2 <= minX1 && maxX2 >= maxX1);
  }

  /** returns true if rectangle (defined by minX, maxX, minY, maxY) contains the X Y point */
  static boolean containsPoint(final double x, final double y, final double minX, final double maxX, final double minY, final double maxY) {
    return x >= minX && x <= maxX && y >= minY && y <= maxY;
  }

  /**
   * Compute whether the given x, y point is in a triangle; uses the winding order method */
  static boolean pointInTriangle(double minX, double maxX, double minY, double maxY, double x, double y, double aX, double aY, double bX, double bY, double cX, double cY) {
    //check the bounding box because if the triangle is degenerated, e.g points and lines, we need to filter out
    //coplanar points that are not part of the triangle.
    if (x >= minX && x <= maxX && y >= minY && y <= maxY) {
      int a = orient(x, y, aX, aY, bX, bY);
      int b = orient(x, y, bX, bY, cX, cY);
      if (a == 0 || b == 0 || a < 0 == b < 0) {
        int c = orient(x, y, cX, cY, aX, aY);
        return c == 0 || (c < 0 == (b < 0 || a < 0));
      }
      return false;
    } else {
      return false;
    }
  }

}