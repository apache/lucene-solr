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

import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.index.PointValues;

import static org.apache.lucene.geo.GeoUtils.orient;

/**
 * 2D Geometry object that supports spatial relationships with bounding boxes,
 * triangles and points.
 *
 * @lucene.internal
 */
public interface Component2D {
  /** relates this component2D with a point **/
  boolean contains(int x, int y);

  /** relates this component2D with a bounding box **/
  PointValues.Relation relate(int minX, int maxX, int minY, int maxY);

  /** relates this component2D with a triangle **/
  PointValues.Relation relateTriangle(int minX, int maxX, int minY, int maxY,  int aX, int aY, int bX, int bY, int cX, int cY);

  /** relates this component2D with a triangle **/
  default PointValues.Relation relateTriangle(int aX, int aY, int bX, int bY, int cX, int cY) {
    int minY = StrictMath.min(StrictMath.min(aY, bY), cY);
    int minX = StrictMath.min(StrictMath.min(aX, bX), cX);
    int maxY = StrictMath.max(StrictMath.max(aY, bY), cY);
    int maxX = StrictMath.max(StrictMath.max(aX, bX), cX);
    return relateTriangle(minX, maxX, minY, maxY, aX, aY, bX, bY, cX, cY);
  }

  /** bounding box for this component2D **/
  RectangleComponent2D getBoundingBox();

  /**
   * Compute whether the given x, y point is in a triangle; uses the winding order method */
  static boolean pointInTriangle(int minX, int maxX, int minY, int maxY, int x, int y, int aX, int aY, int bX, int bY, int cX, int cY) {
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