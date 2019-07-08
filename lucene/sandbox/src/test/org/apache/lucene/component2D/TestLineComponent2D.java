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

import org.apache.lucene.document.TestLatLonLineShapeQueries;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.geo.GeoTestUtil;
import org.apache.lucene.geo.Line;
import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.util.LuceneTestCase;

public class TestLineComponent2D extends LuceneTestCase {

  public void testEqualsAndHashcode() {
    Line line = TestLatLonLineShapeQueries.nextLine();
    Component2D component1 = LatLonComponent2DFactory.create(line);
    Component2D component2 = LatLonComponent2DFactory.create(line);
    assertEquals(component1, component2);
    assertEquals(component1.hashCode(), component2.hashCode());
    Line otherLine = TestLatLonLineShapeQueries.nextLine();
    Component2D component3 = LatLonComponent2DFactory.create(otherLine);
    if (line.equals(otherLine)) {
      assertEquals(component1, component3);
      assertEquals(component1.hashCode(), component3.hashCode());
    } else {
      assertNotEquals(component1, component3);
      assertNotEquals(component1.hashCode(), component3.hashCode());
    }
  }

  public void testTriangleDisjoint() {
    Line line = new Line(new double[] {0, 1, 2, 3}, new double[] {0, 0, 2, 2});
    Component2D component = LatLonComponent2DFactory.create(line);
    int ax = GeoEncodingUtils.encodeLongitude(4);
    int ay = GeoEncodingUtils.encodeLatitude(4);
    int bx = GeoEncodingUtils.encodeLongitude(5);
    int by = GeoEncodingUtils.encodeLatitude(5);
    int cx = GeoEncodingUtils.encodeLongitude(5);
    int cy = GeoEncodingUtils.encodeLatitude(4);
    assertEquals(Relation.CELL_OUTSIDE_QUERY, component.relateTriangle(ax, ay, bx, by , cx, cy));;
  }

  public void testTriangleIntersects() {
    Line line = new Line(new double[] {0.5, 0, 1, 2, 3}, new double[] {0.5, 0, 0, 2, 2});
    Component2D component = LatLonComponent2DFactory.create(line);
    int ax = GeoEncodingUtils.encodeLongitude(0.0);
    int ay = GeoEncodingUtils.encodeLatitude(0.0);
    int bx = GeoEncodingUtils.encodeLongitude(1);
    int by = GeoEncodingUtils.encodeLatitude(0);
    int cx = GeoEncodingUtils.encodeLongitude(0);
    int cy = GeoEncodingUtils.encodeLatitude(1);
    assertEquals(Relation.CELL_CROSSES_QUERY, component.relateTriangle(ax, ay, bx, by , cx, cy));
  }

  public void testTriangleContains() {
    Line line = new Line(new double[] {0.5, 0, 1, 2, 3}, new double[] {0.5, 0, 0, 2, 2});
    Component2D component = LatLonComponent2DFactory.create(line);
    int ax = GeoEncodingUtils.encodeLongitude(-10);
    int ay = GeoEncodingUtils.encodeLatitude(-10);
    int bx = GeoEncodingUtils.encodeLongitude(4);
    int by = GeoEncodingUtils.encodeLatitude(-10);
    int cx = GeoEncodingUtils.encodeLongitude(4);
    int cy = GeoEncodingUtils.encodeLatitude(30);
    assertEquals(Relation.CELL_CROSSES_QUERY, component.relateTriangle(ax, ay, bx, by , cx, cy));
  }

  public void testRandomTriangles() {
    Line line = TestLatLonLineShapeQueries.nextLine();
    Component2D component = LatLonComponent2DFactory.create(line);

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

      Relation r = component.relate(tMinX, tMaxX, tMinY, tMaxY);
      if (r == Relation.CELL_OUTSIDE_QUERY) {
        assertEquals(Relation.CELL_OUTSIDE_QUERY, component.relateTriangle(ax, ay, bx, by, cx, cy));
      }
    }
  }

  public void testLineSharedLine() {
    Line l = new Line(new double[] {0, 0, 0, 0}, new double[] {-2, -1, 0, 1});
    Component2D component = LatLonComponent2DFactory.create(l);
    PointValues.Relation r = component.relateTriangle(
        GeoEncodingUtils.encodeLongitude(-5), GeoEncodingUtils.encodeLatitude(0),
        GeoEncodingUtils.encodeLongitude(5), GeoEncodingUtils.encodeLatitude(0),
        GeoEncodingUtils.encodeLongitude(-5), GeoEncodingUtils.encodeLatitude(0));
    assertEquals(PointValues.Relation.CELL_CROSSES_QUERY, r);
  }
}
