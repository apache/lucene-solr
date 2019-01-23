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
import org.apache.lucene.util.NumericUtils;

import static java.lang.Integer.BYTES;

public class TestRectangle2D extends LuceneTestCase {

  public void testTriangleDisjoint() {
    Rectangle rectangle = new Rectangle(0, 1, 0, 1);
    Rectangle2D rectangle2D = Rectangle2D.create(rectangle);
    int ax = GeoEncodingUtils.encodeLongitude(4);
    int ay = GeoEncodingUtils.encodeLatitude(4);
    int bx = GeoEncodingUtils.encodeLongitude(5);
    int by = GeoEncodingUtils.encodeLatitude(5);
    int cx = GeoEncodingUtils.encodeLongitude(5);
    int cy = GeoEncodingUtils.encodeLatitude(4);
    assertFalse(rectangle2D.intersectsTriangle(ax, ay, bx, by , cx, cy));
    assertFalse(rectangle2D.containsTriangle(ax, ay, bx, by , cx, cy));
    assertEquals(EdgeTree.WithinRelation.DISJOINT, rectangle2D.withinTriangle(ax, ay, true, bx, by , true, cx, cy, true));
  }

  public void testTriangleIntersects() {
    Rectangle rectangle = new Rectangle(0, 1, 0, 1);
    Rectangle2D rectangle2D =  Rectangle2D.create(rectangle);
    int ax = GeoEncodingUtils.encodeLongitude(0.5);
    int ay = GeoEncodingUtils.encodeLatitude(0.5);
    int bx = GeoEncodingUtils.encodeLongitude(2);
    int by = GeoEncodingUtils.encodeLatitude(2);
    int cx = GeoEncodingUtils.encodeLongitude(0.5);
    int cy = GeoEncodingUtils.encodeLatitude(2);
    assertTrue(rectangle2D.intersectsTriangle(ax, ay, bx, by , cx, cy));
    assertFalse(rectangle2D.containsTriangle(ax, ay, bx, by , cx, cy));
    assertEquals(EdgeTree.WithinRelation.NOTWITHIN, rectangle2D.withinTriangle(ax, ay, true, bx, by, true, cx, cy, true));
  }

  public void testTriangleContains() {
    Rectangle rectangle = new Rectangle(0, 1, 0, 1);
    Rectangle2D rectangle2D =  Rectangle2D.create(rectangle);
    int ax = GeoEncodingUtils.encodeLongitude(0.25);
    int ay = GeoEncodingUtils.encodeLatitude(0.25);
    int bx = GeoEncodingUtils.encodeLongitude(0.5);
    int by = GeoEncodingUtils.encodeLatitude(0.5);
    int cx = GeoEncodingUtils.encodeLongitude(0.5);
    int cy = GeoEncodingUtils.encodeLatitude(0.25);
    assertTrue(rectangle2D.intersectsTriangle(ax, ay, bx, by , cx, cy));
    assertTrue(rectangle2D.containsTriangle(ax, ay, bx, by , cx, cy));
    assertEquals(EdgeTree.WithinRelation.NOTWITHIN, rectangle2D.withinTriangle(ax, ay, true, bx, by, true, cx, cy, true));
  }

  public void testTriangleContainsEdgeCase() {
    Rectangle rectangle = new Rectangle(0, 1, 0, 1);
    Rectangle2D rectangle2D =  Rectangle2D.create(rectangle);
    int ax = GeoEncodingUtils.encodeLongitude(0.0);
    int ay = GeoEncodingUtils.encodeLatitude(0.0);
    int bx = GeoEncodingUtils.encodeLongitude(0.0);
    int by = GeoEncodingUtils.encodeLatitude(0.5);
    int cx = GeoEncodingUtils.encodeLongitude(0.5);
    int cy = GeoEncodingUtils.encodeLatitude(0.25);
    assertTrue(rectangle2D.intersectsTriangle(ax, ay, bx, by , cx, cy));
    assertTrue(rectangle2D.containsTriangle(ax, ay, bx, by , cx, cy));
    assertEquals(EdgeTree.WithinRelation.NOTWITHIN, rectangle2D.withinTriangle(ax, ay, true, bx, by, true, cx, cy, true));
  }

  public void testTriangleWithin() {
    Rectangle rectangle = new Rectangle(0, 1, 0, 1);
    Rectangle2D rectangle2D =  Rectangle2D.create(rectangle);
    int ax = GeoEncodingUtils.encodeLongitude(-10);
    int ay = GeoEncodingUtils.encodeLatitude(-10);
    int bx = GeoEncodingUtils.encodeLongitude(10);
    int by = GeoEncodingUtils.encodeLatitude(-10);
    int cx = GeoEncodingUtils.encodeLongitude(10);
    int cy = GeoEncodingUtils.encodeLatitude(20);
    assertTrue(rectangle2D.intersectsTriangle(ax, ay, bx, by , cx, cy));
    assertFalse(rectangle2D.containsTriangle(ax, ay, bx, by , cx, cy));
    assertEquals(EdgeTree.WithinRelation.CANDIDATE, rectangle2D.withinTriangle(ax, ay, true, bx, by, true, cx, cy, true));
  }

  public void testTriangleWithinCrossingDateLine() {
    Rectangle rectangle = new Rectangle(0,  2, 179, -179);
    Rectangle2D rectangle2D =  Rectangle2D.create(rectangle);
    int ax = GeoEncodingUtils.encodeLongitude(169);
    int ay = GeoEncodingUtils.encodeLatitude(-10);
    int bx = GeoEncodingUtils.encodeLongitude(180);
    int by = GeoEncodingUtils.encodeLatitude(-10);
    int cx = GeoEncodingUtils.encodeLongitude(180);
    int cy = GeoEncodingUtils.encodeLatitude(30);
    assertTrue(rectangle2D.intersectsTriangle(ax, ay, bx, by , cx, cy));
    assertFalse(rectangle2D.containsTriangle(ax, ay, bx, by , cx, cy));
    expectThrows(IllegalArgumentException.class, () -> {
      rectangle2D.withinTriangle(ax, ay, true, bx, by, true, cx, cy, true);
    });
  }

  public void testRandomTriangles() {
    Rectangle rectangle = GeoTestUtil.nextBox();
    while(rectangle.crossesDateline()) {
      rectangle = GeoTestUtil.nextBox();
    }
    Rectangle2D rectangle2D = Rectangle2D.create(rectangle);

    for (int i =0; i < 100; i++) {
      int ax = GeoEncodingUtils.encodeLongitude(GeoTestUtil.nextLongitude());
      int ay = GeoEncodingUtils.encodeLatitude(GeoTestUtil.nextLatitude());
      int bx = GeoEncodingUtils.encodeLongitude(GeoTestUtil.nextLongitude());
      int by = GeoEncodingUtils.encodeLatitude(GeoTestUtil.nextLatitude());
      int cx = GeoEncodingUtils.encodeLongitude(GeoTestUtil.nextLongitude());
      int cy = GeoEncodingUtils.encodeLatitude(GeoTestUtil.nextLatitude());

      int tMinX = StrictMath.min(StrictMath.min(ax, bx), cx);
      int tMaxX = StrictMath.max(StrictMath.max(ax, bx), cx);
      int tMinY = StrictMath.min(StrictMath.min(ay, by), cy);
      int tMaxY = StrictMath.max(StrictMath.max(ay, by), cy);

      byte[] triangle = new byte[4 * BYTES];
      NumericUtils.intToSortableBytes(tMinY, triangle, 0);
      NumericUtils.intToSortableBytes(tMinX, triangle, BYTES);
      NumericUtils.intToSortableBytes(tMaxY, triangle, 2 * BYTES);
      NumericUtils.intToSortableBytes(tMaxX, triangle, 3 * BYTES);

      PointValues.Relation r = rectangle2D.relateRangeBBox(BYTES, 0, triangle, 3 * BYTES, 2 * BYTES, triangle);
      if (r == PointValues.Relation.CELL_OUTSIDE_QUERY) {
        assertFalse(rectangle2D.intersectsTriangle(ax, ay, bx, by , cx, cy));
        assertFalse(rectangle2D.containsTriangle(ax, ay, bx, by , cx, cy));
        assertEquals(EdgeTree.WithinRelation.DISJOINT, rectangle2D.withinTriangle(ax, ay, true, bx, by, true, cx, cy, true));
      } else if (rectangle2D.containsTriangle(ax, ay, bx, by , cx, cy)) {
        assertTrue(rectangle2D.intersectsTriangle(ax, ay, bx, by , cx, cy));
        assertTrue(rectangle2D.withinTriangle(ax, ay, true, bx, by, true, cx, cy, true) !=  EdgeTree.WithinRelation.CANDIDATE);
      } else if (rectangle2D.withinTriangle(ax, ay, true, bx, by , true, cx, cy, true) ==  EdgeTree.WithinRelation.CANDIDATE) {
        assertTrue(rectangle2D.intersectsTriangle(ax, ay, bx, by , cx, cy));
        assertFalse(rectangle2D.containsTriangle(ax, ay, bx, by , cx, cy));
      }
    }
  }
}
