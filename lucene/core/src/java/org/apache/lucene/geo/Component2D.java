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

  /** true if the component bounding box is disjoint with the provided bounding box **/
  default boolean disjoint(double minX, double maxX, double minY, double maxY) {
    return (getMaxY() < minY || getMinY() > maxY || getMaxX() < minX || getMinX() > maxX);
  }

  /** true if the component bounding box is within with the provided bounding box **/
  default boolean within(double minX, double maxX, double minY, double maxY) {
    return (minY < getMinY() && maxY > getMaxY() && minX < getMinX() && maxX > getMaxX());
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