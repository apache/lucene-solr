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

import org.apache.lucene.document.TestLatLonLineShapeQueries;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.util.LuceneTestCase;

public class TestPoint2D extends LuceneTestCase {

  public void testTriangleDisjoint() {
    Component2D point2D = Point2D.create(new double[] {0, 0});
    double ax = 4;
    double ay = 4;
    double bx = 5;
    double by = 5;
    double cx = 5;
    double cy = 4;
    assertEquals(Relation.CELL_OUTSIDE_QUERY, point2D.relateTriangle(ax, ay, bx, by , cx, cy));
  }

  public void testTriangleIntersects() {
    Component2D point2D = Point2D.create(new double[] {0, 0});
    double ax = 0.0;
    double ay = 0.0;
    double bx = 1;
    double by = 0;
    double cx = 0;
    double cy = 1;
    assertEquals(Relation.CELL_INSIDE_QUERY, point2D.relateTriangle(ax, ay, bx, by , cx, cy));
  }

  public void testTriangleContains() {
    Component2D point2D = Point2D.create(new double[] {0, 0});
    double ax = 0.0;
    double ay = 0.0;
    double bx = 0;
    double by = 0;
    double cx = 0;
    double cy = 0;
    assertEquals(Relation.CELL_INSIDE_QUERY, point2D.relateTriangle(ax, ay, bx, by , cx, cy));
  }

  public void testRandomTriangles() {
    Component2D point2D = Point2D.create(new double[] {GeoTestUtil.nextLatitude(), GeoTestUtil.nextLongitude()});

    for (int i =0; i < 100; i++) {
      double ax = GeoTestUtil.nextLongitude();
      double ay = GeoTestUtil.nextLatitude();
      double bx = GeoTestUtil.nextLongitude();
      double by = GeoTestUtil.nextLatitude();
      double cx = GeoTestUtil.nextLongitude();
      double cy = GeoTestUtil.nextLatitude();

      double tMinX = StrictMath.min(StrictMath.min(ax, bx), cx);
      double tMaxX = StrictMath.max(StrictMath.max(ax, bx), cx);
      double tMinY = StrictMath.min(StrictMath.min(ay, by), cy);
      double tMaxY = StrictMath.max(StrictMath.max(ay, by), cy);

      Relation r = point2D.relate(tMinX, tMaxX, tMinY, tMaxY);
      if (r == Relation.CELL_OUTSIDE_QUERY) {
        assertEquals(Relation.CELL_OUTSIDE_QUERY, point2D.relateTriangle(ax, ay, bx, by, cx, cy));
      }
    }
  }
}
