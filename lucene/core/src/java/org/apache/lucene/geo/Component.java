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
 * Geometry object that supports spatial relationships with bounding boxes,
 * triangles and points.
 *
 * @lucene.internal
 */
public interface Component {
  /** relates this component with a point **/
  boolean contains(double lat, double lon);

  /** relates this component with a bounding box **/
  PointValues.Relation relate(double minY, double maxY, double minX, double maxX);

  /** relates this component with a triangle **/
  PointValues.Relation relateTriangle(double ax, double ay, double bx, double by, double cx, double cy);

  /** bounding box for this component **/
  Rectangle getBoundingBox();

  //This should be moved when LatLonShape is moved from sandbox!
  /**
   * Compute whether the given x, y point is in a triangle; uses the winding order method */
  static boolean pointInTriangle (double x, double y, double ax, double ay, double bx, double by, double cx, double cy) {
    double minX = StrictMath.min(ax, StrictMath.min(bx, cx));
    double minY = StrictMath.min(ay, StrictMath.min(by, cy));
    double maxX = StrictMath.max(ax, StrictMath.max(bx, cx));
    double maxY = StrictMath.max(ay, StrictMath.max(by, cy));
    //check the bounding box because if the triangle is degenerated, e.g points and lines, we need to filter out
    //coplanar points that are not part of the triangle.
    if (x >= minX && x <= maxX && y >= minY && y <= maxY ) {
      int a = orient(x, y, ax, ay, bx, by);
      int b = orient(x, y, bx, by, cx, cy);
      if (a == 0 || b == 0 || a < 0 == b < 0) {
        int c = orient(x, y, cx, cy, ax, ay);
        return c == 0 || (c < 0 == (b < 0 || a < 0));
      }
      return false;
    } else {
      return false;
    }
  }
}
