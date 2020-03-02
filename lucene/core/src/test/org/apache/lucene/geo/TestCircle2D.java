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
import org.apache.lucene.util.LuceneTestCase;

public class TestCircle2D extends LuceneTestCase {

  public void testTriangleDisjoint() {
    Component2D circle2D;
    if (random().nextBoolean()) {
      Circle circle = new Circle(0, 0, 100);
      circle2D = LatLonGeometry.create(circle);
    } else {
      XYCircle xyCircle = new XYCircle(0, 0, 1);
      circle2D = XYGeometry.create(xyCircle);
    }
    double ax = 4;
    double ay = 4;
    double bx = 5;
    double by = 5;
    double cx = 5;
    double cy = 4;
    assertEquals(PointValues.Relation.CELL_OUTSIDE_QUERY, circle2D.relateTriangle(ax, ay, bx, by , cx, cy));
    assertEquals(Component2D.WithinRelation.DISJOINT, circle2D.withinTriangle(ax, ay, true, bx, by, true, cx, cy, true));
  }

  public void testTriangleIntersects() {
    Component2D circle2D;
    if (random().nextBoolean()) {
      Circle circle = new Circle(0, 0, 1000000);
      circle2D = LatLonGeometry.create(circle);
    } else {
      XYCircle xyCircle = new XYCircle(0, 0, 10);
      circle2D = XYGeometry.create(xyCircle);
    }
    double ax = -20;
    double ay = 1;
    double bx = 20;
    double by = 1;
    double cx = 0;
    double cy = 90;
    assertEquals(PointValues.Relation.CELL_CROSSES_QUERY, circle2D.relateTriangle(ax, ay, bx, by , cx, cy));
    assertEquals(Component2D.WithinRelation.NOTWITHIN, circle2D.withinTriangle(ax, ay, true, bx, by, true, cx, cy, true));
  }

  public void testTriangleDateLineIntersects() {
    Component2D circle2D = LatLonGeometry.create(new Circle(0, 179, 222400));
    double ax = -179;
    double ay = 1;
    double bx = -179;
    double by = -1;
    double cx = -178;
    double cy = 0;
    // we just touch the edge from the dateline
    assertEquals(PointValues.Relation.CELL_CROSSES_QUERY, circle2D.relateTriangle(ax, ay, bx, by , cx, cy));
    assertEquals(Component2D.WithinRelation.NOTWITHIN, circle2D.withinTriangle(ax, ay, true, bx, by, true, cx, cy, true));
  }

  public void testTriangleContains() {
    Component2D circle2D;
    if (random().nextBoolean()) {
      Circle circle = new Circle(0, 0, 1000000);
      circle2D = LatLonGeometry.create(circle);
    } else {
      XYCircle xyCircle = new XYCircle(0, 0, 1);
      circle2D = XYGeometry.create(xyCircle);
    }
    double ax = 0.25;
    double ay = 0.25;
    double bx = 0.5;
    double by = 0.5;
    double cx = 0.5;
    double cy = 0.25;
    assertEquals(PointValues.Relation.CELL_INSIDE_QUERY, circle2D.relateTriangle(ax, ay, bx, by , cx, cy));
    assertEquals(Component2D.WithinRelation.NOTWITHIN, circle2D.withinTriangle(ax, ay, true, bx, by, true, cx, cy, true));
  }

  public void testTriangleWithin() {
    Component2D circle2D;
    if (random().nextBoolean()) {
      Circle circle = new Circle(0, 0, 1000);
      circle2D = LatLonGeometry.create(circle);
    } else {
      XYCircle xyCircle = new XYCircle(0, 0, 1);
      circle2D = XYGeometry.create(xyCircle);
    }

    double ax = -20;
    double ay = -20;
    double bx = 20;
    double by = -20;
    double cx = 0;
    double cy = 20;
    assertEquals(PointValues.Relation.CELL_CROSSES_QUERY, circle2D.relateTriangle(ax, ay, bx, by , cx, cy));
    assertEquals(Component2D.WithinRelation.CANDIDATE, circle2D.withinTriangle(ax, ay, true, bx, by, true, cx, cy, true));
  }

  public void testRandomTriangles() {
    Component2D circle2D;
    if (random().nextBoolean()) {
      Circle circle = GeoTestUtil.nextCircle();
      circle2D = LatLonGeometry.create(circle);
    } else {
      XYCircle circle = ShapeTestUtil.nextCircle();
      circle2D = XYGeometry.create(circle);
    }
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

      PointValues.Relation r = circle2D.relate(tMinX, tMaxX, tMinY, tMaxY);
      if (r == PointValues.Relation.CELL_OUTSIDE_QUERY) {
        assertEquals(PointValues.Relation.CELL_OUTSIDE_QUERY, circle2D.relateTriangle(ax, ay, bx, by , cx, cy));
        assertEquals(Component2D.WithinRelation.DISJOINT, circle2D.withinTriangle(ax, ay, true, bx, by, true, cx, cy, true));
      } else if (r == PointValues.Relation.CELL_INSIDE_QUERY) {
        assertEquals(PointValues.Relation.CELL_INSIDE_QUERY, circle2D.relateTriangle(ax, ay, bx, by , cx, cy));
        assertNotEquals(Component2D.WithinRelation.CANDIDATE, circle2D.withinTriangle(ax, ay, true, bx, by, true, cx, cy, true));
      }
    }
  }
}
