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

public class TestXYCircle2D extends LuceneTestCase {

  public void testTriangleDisjoint() {
    XYCircle circle = new XYCircle(0, 0, 1);
    Component2D circle2D = XYCircle2D.create(circle);
    double ax = 4;
    double ay = 4;
    double bx = 5;
    double by = 5;
    double cx = 5;
    double cy = 4;
    PointValues.Relation rel = circle2D.relateTriangle(ax, ay, bx, by, cx, cy);
    assertEquals(PointValues.Relation.CELL_OUTSIDE_QUERY, rel);
    assertEquals(Component2D.WithinRelation.DISJOINT, circle2D.withinTriangle(ax, ay, true, bx, by, true, cx, cy, true));
  }

  public void testTriangleIntersects() {
    XYCircle circle = new XYCircle(0, 0, 10);
    double ax = -20;
    double ay = 1;
    double bx = 20;
    double by = 1;
    double cx = 0;
    double cy = 90;
    Component2D circle2D = XYCircle2D.create(circle);
    PointValues.Relation rel = circle2D.relateTriangle(ax, ay, bx, by, cx, cy);
    assertEquals(PointValues.Relation.CELL_CROSSES_QUERY, rel);
    assertEquals(Component2D.WithinRelation.NOTWITHIN, circle2D.withinTriangle(ax, ay, true, bx, by, true, cx, cy, true));
  }

  public void testTriangleContains() {
    XYCircle circle = new XYCircle(0, 0, 1);
    Component2D circle2D = XYCircle2D.create(circle);
    double ax = 0.25;
    double ay = 0.25;
    double bx = 0.5;
    double by = 0.5;
    double cx = 0.5;
    double cy = 0.25;
    PointValues.Relation rel = circle2D.relateTriangle(ax, ay, bx, by, cx, cy);
    assertEquals(PointValues.Relation.CELL_INSIDE_QUERY, rel);
    assertEquals(Component2D.WithinRelation.NOTWITHIN, circle2D.withinTriangle(ax, ay, true, bx, by, true, cx, cy, true));
  }

  public void testTriangleWithin() {
    XYCircle circle = new XYCircle(0, 0, 1);
    Component2D circle2D = XYCircle2D.create(circle);
    double ax = -20;
    double ay = -20;
    double bx = 20;
    double by = -20;
    double cx = 0;
    double cy = 20;
    PointValues.Relation rel = circle2D.relateTriangle(ax, ay, bx, by, cx, cy);
    assertEquals(PointValues.Relation.CELL_CROSSES_QUERY, rel);
    assertEquals(Component2D.WithinRelation.CANDIDATE, circle2D.withinTriangle(ax, ay, true, bx, by, true, cx, cy, true));
  }

  public void testRandomTriangles() {
    final float centerLat = (float)ShapeTestUtil.nextDouble(random());
    final float centerLon = (float)ShapeTestUtil.nextDouble(random());
    float radiusMeters = (float) ShapeTestUtil.nextDouble(random());
    // Is there a max value???
    while (radiusMeters <= 0 || radiusMeters >= Float.MAX_VALUE / 2) {
      radiusMeters = (float)ShapeTestUtil.nextDouble(random());
    }
    XYCircle circle = new XYCircle(centerLat, centerLon, radiusMeters);
    Component2D circle2D = XYCircle2D.create(circle);

    for (int i =0; i < 100; i++) {
      double ax = ShapeTestUtil.nextDouble(random());
      double ay = ShapeTestUtil.nextDouble(random());
      double bx = ShapeTestUtil.nextDouble(random());
      double by = ShapeTestUtil.nextDouble(random());
      double cx = ShapeTestUtil.nextDouble(random());
      double cy = ShapeTestUtil.nextDouble(random());

      double tMinX = StrictMath.min(StrictMath.min(ax, bx), cx);
      double tMaxX = StrictMath.max(StrictMath.max(ax, bx), cx);
      double tMinY = StrictMath.min(StrictMath.min(ay, by), cy);
      double tMaxY = StrictMath.max(StrictMath.max(ay, by), cy);


      PointValues.Relation r = circle2D.relate(tMinX, tMaxX, tMinY, tMaxY);
      if (r == PointValues.Relation.CELL_OUTSIDE_QUERY) {
        assertEquals(circle.toString(), PointValues.Relation.CELL_OUTSIDE_QUERY, circle2D.relateTriangle(ax, ay, bx, by, cx, cy));
        assertEquals(Component2D.WithinRelation.DISJOINT, circle2D.withinTriangle(ax, ay, true, bx, by, true, cx, cy, true));
      } else if (r == PointValues.Relation.CELL_INSIDE_QUERY) {
        assertEquals(PointValues.Relation.CELL_CROSSES_QUERY, circle2D.relateTriangle(ax, ay, bx, by , cx, cy));
        assertEquals(Component2D.WithinRelation.NOTWITHIN, circle2D.withinTriangle(ax, ay, true, bx, by, true, cx, cy, true));
      }
    }
  }
}
