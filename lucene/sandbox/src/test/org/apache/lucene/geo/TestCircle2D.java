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
import org.apache.lucene.util.TestUtil;

public class TestCircle2D extends LuceneTestCase {

  public void testTriangleDisjoint() {
    Circle circle = new Circle(0, 0, 100);
    Circle2D rectangle2D = Circle2D.create(circle);
    int ax = GeoEncodingUtils.encodeLongitude(4);
    int ay = GeoEncodingUtils.encodeLatitude(4);
    int bx = GeoEncodingUtils.encodeLongitude(5);
    int by = GeoEncodingUtils.encodeLatitude(5);
    int cx = GeoEncodingUtils.encodeLongitude(5);
    int cy = GeoEncodingUtils.encodeLatitude(4);
    assertFalse(rectangle2D.intersectsTriangle(ax, ay, bx, by , cx, cy));
    assertFalse(rectangle2D.containsTriangle(ax, ay, bx, by , cx, cy));
  }

  public void testTriangleIntersects() {
    Circle circle = new Circle(0, 0, 1000000);
    Circle2D rectangle2D = Circle2D.create(circle);
    int ax = GeoEncodingUtils.encodeLongitude(-20);
    int ay = GeoEncodingUtils.encodeLatitude(1);
    int bx = GeoEncodingUtils.encodeLongitude(20);
    int by = GeoEncodingUtils.encodeLatitude(1);
    int cx = GeoEncodingUtils.encodeLongitude(0);
    int cy = GeoEncodingUtils.encodeLatitude(90);
    assertTrue(rectangle2D.intersectsTriangle(ax, ay, bx, by , cx, cy));
    assertFalse(rectangle2D.containsTriangle(ax, ay, bx, by , cx, cy));
  }

  public void testTriangleContains() {
    Circle circle = new Circle(0, 0, 1000000);
    Circle2D rectangle2D = Circle2D.create(circle);
    int ax = GeoEncodingUtils.encodeLongitude(0.25);
    int ay = GeoEncodingUtils.encodeLatitude(0.25);
    int bx = GeoEncodingUtils.encodeLongitude(0.5);
    int by = GeoEncodingUtils.encodeLatitude(0.5);
    int cx = GeoEncodingUtils.encodeLongitude(0.5);
    int cy = GeoEncodingUtils.encodeLatitude(0.25);
    assertTrue(rectangle2D.intersectsTriangle(ax, ay, bx, by , cx, cy));
    assertTrue(rectangle2D.containsTriangle(ax, ay, bx, by , cx, cy));
  }

  public void testTriangleWithin() {
    Circle circle = new Circle(0, 0, 1000);
    Circle2D rectangle2D = Circle2D.create(circle);
    int ax = GeoEncodingUtils.encodeLongitude(-20);
    int ay = GeoEncodingUtils.encodeLatitude(-20);
    int bx = GeoEncodingUtils.encodeLongitude(20);
    int by = GeoEncodingUtils.encodeLatitude(-20);
    int cx = GeoEncodingUtils.encodeLongitude(20);
    int cy = GeoEncodingUtils.encodeLatitude(20);
    assertTrue(rectangle2D.intersectsTriangle(ax, ay, bx, by , cx, cy));
    assertFalse(rectangle2D.containsTriangle(ax, ay, bx, by , cx, cy));
  }

  public void testRandomTriangles() {
    final double centerLat = GeoTestUtil.nextLatitude();
    final double centerLon = GeoTestUtil.nextLongitude();
    double radiusMeters = random().nextDouble() * Circle.MAXRADIUS;
    while (radiusMeters == 0 || radiusMeters == Circle.MAXRADIUS) {
      radiusMeters = random().nextDouble() * Circle.MAXRADIUS;
    }
    Circle circle = new Circle(centerLat, centerLon, radiusMeters);
    Circle2D circle2D = Circle2D.create(circle);
    //System.out.println("CIRCLE(" + 0 + " " + 0+ "," + radius + "))");
    for (int i =0; i < 100; i++) {
      double aLon = GeoTestUtil.nextLongitude();
      double aLat = GeoTestUtil.nextLatitude();
      double bLon = GeoTestUtil.nextLongitude();
      double bLat = GeoTestUtil.nextLatitude();
      double cLon = GeoTestUtil.nextLongitude();
      double cLat = GeoTestUtil.nextLatitude();
      int ax = GeoEncodingUtils.encodeLongitude(aLon);
      int ay = GeoEncodingUtils.encodeLatitude(aLat);
      int bx = GeoEncodingUtils.encodeLongitude(bLon);
      int by = GeoEncodingUtils.encodeLatitude(bLat);
      int cx = GeoEncodingUtils.encodeLongitude(cLon);
      int cy = GeoEncodingUtils.encodeLatitude(cLat);
      //System.out.println("POLYGON((" + aLon + " " + aLat + "," + bLon + " " + bLat + "," + cLon + " " + cLat + "))");

      int tMinX = StrictMath.min(StrictMath.min(ax, bx), cx);
      int tMaxX = StrictMath.max(StrictMath.max(ax, bx), cx);
      int tMinY = StrictMath.min(StrictMath.min(ay, by), cy);
      int tMaxY = StrictMath.max(StrictMath.max(ay, by), cy);

      byte[] triangle = new byte[4 * Integer.BYTES];
      NumericUtils.intToSortableBytes(tMinY, triangle, 0);
      NumericUtils.intToSortableBytes(tMinX, triangle, Integer.BYTES);
      NumericUtils.intToSortableBytes(tMaxY, triangle, 2 * Integer.BYTES);
      NumericUtils.intToSortableBytes(tMaxX, triangle, 3 * Integer.BYTES);

      PointValues.Relation r = circle2D.relateRangeBBox(Integer.BYTES, 0, triangle, 3 * Integer.BYTES, 2 * Integer.BYTES, triangle);
      if (r == PointValues.Relation.CELL_OUTSIDE_QUERY) {
        assertFalse(circle2D.intersectsTriangle(ax, ay, bx, by , cx, cy));
        assertFalse(circle2D.containsTriangle(ax, ay, bx, by , cx, cy));
      }
      else if (circle2D.containsTriangle(ax, ay, bx, by , cx, cy)) {
        assertTrue(circle2D.intersectsTriangle(ax, ay, bx, by , cx, cy));
      }
    }
  }
}
