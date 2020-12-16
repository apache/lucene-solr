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
import org.apache.lucene.util.LuceneTestCase;

public class TestLine2D extends LuceneTestCase {

  public void testTriangleDisjoint() {
    Line line = new Line(new double[] {0, 1, 2, 3}, new double[] {0, 0, 2, 2});
    Component2D line2D = Line2D.create(line);
    int ax = GeoEncodingUtils.encodeLongitude(4);
    int ay = GeoEncodingUtils.encodeLatitude(4);
    int bx = GeoEncodingUtils.encodeLongitude(5);
    int by = GeoEncodingUtils.encodeLatitude(5);
    int cx = GeoEncodingUtils.encodeLongitude(5);
    int cy = GeoEncodingUtils.encodeLatitude(4);
    assertFalse(line2D.intersectsTriangle(ax, ay, bx, by , cx, cy));
    assertFalse(line2D.intersectsLine(ax, ay, bx, by));
    assertFalse(line2D.containsTriangle(ax, ay, bx, by , cx, cy));
    assertFalse(line2D.containsLine(ax, ay, bx, by));
    assertEquals(Component2D.WithinRelation.DISJOINT, line2D.withinTriangle(ax, ay, true, bx, by , true, cx, cy, true));
  }

  public void testTriangleIntersects() {
    Line line = new Line(new double[] {0.5, 0, 1, 2, 3}, new double[] {0.5, 0, 0, 2, 2});
    Component2D line2D = Line2D.create(line);
    int ax = GeoEncodingUtils.encodeLongitude(0.0);
    int ay = GeoEncodingUtils.encodeLatitude(0.0);
    int bx = GeoEncodingUtils.encodeLongitude(1);
    int by = GeoEncodingUtils.encodeLatitude(0);
    int cx = GeoEncodingUtils.encodeLongitude(0);
    int cy = GeoEncodingUtils.encodeLatitude(1);
    assertTrue(line2D.intersectsTriangle(ax, ay, bx, by , cx, cy));
    assertTrue(line2D.intersectsLine(ax, ay, bx, by));
    assertFalse(line2D.containsTriangle(ax, ay, bx, by , cx, cy));
    assertFalse(line2D.containsLine(ax, ay, bx, by));
    assertEquals(Component2D.WithinRelation.NOTWITHIN, line2D.withinTriangle(ax, ay, true, bx, by , true, cx, cy, true));
  }

  public void testTriangleContains() {
    Line line = new Line(new double[] {0.5, 0, 1, 2, 3}, new double[] {0.5, 0, 0, 2, 2});
    Component2D line2D = Line2D.create(line);
    int ax = GeoEncodingUtils.encodeLongitude(-10);
    int ay = GeoEncodingUtils.encodeLatitude(-10);
    int bx = GeoEncodingUtils.encodeLongitude(4);
    int by = GeoEncodingUtils.encodeLatitude(-10);
    int cx = GeoEncodingUtils.encodeLongitude(4);
    int cy = GeoEncodingUtils.encodeLatitude(30);
    assertTrue(line2D.intersectsTriangle(ax, ay, bx, by , cx, cy));
    assertFalse(line2D.intersectsLine(bx, by, cx, cy));
    assertFalse(line2D.containsTriangle(ax, ay, bx, by , cx, cy));
    assertFalse(line2D.containsLine(bx, by, cx, cy));
    assertEquals(Component2D.WithinRelation.CANDIDATE, line2D.withinTriangle(ax, ay, true, bx, by , true, cx, cy, true));
  }

  public void testRandomTriangles() {
    Line line = GeoTestUtil.nextLine();
    Component2D line2D = Line2D.create(line);

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

      Relation r = line2D.relate(tMinX, tMaxX, tMinY, tMaxY);
      if (r == Relation.CELL_OUTSIDE_QUERY) {
        assertFalse(line2D.intersectsTriangle(ax, ay, bx, by, cx, cy));
        assertFalse(line2D.intersectsLine(ax, ay, bx, by));
        assertFalse(line2D.containsTriangle(ax, ay, bx, by , cx, cy));
        assertFalse(line2D.containsLine(ax, ay, bx, by));
        assertEquals(Component2D.WithinRelation.DISJOINT, line2D.withinTriangle(ax, ay, true, bx, by, true, cx, cy, true));
      } else if (line2D.containsTriangle(ax, ay, bx, by, cx, cy)) {
        assertNotEquals(Component2D.WithinRelation.CANDIDATE, line2D.withinTriangle(ax, ay, true, bx, by, true, cx, cy, true));
      }
    }
  }
}
