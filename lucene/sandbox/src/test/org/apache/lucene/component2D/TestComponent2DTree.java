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


import java.util.Arrays;

import org.apache.lucene.document.BaseLatLonShapeTestCase;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.geo.GeoTestUtil;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestComponent2DTree extends LuceneTestCase {

  public void testEqualsAndHashcode() {
    int numComponents = TestUtil.nextInt(random(), 2, 10);
    Object[] components = new Object[numComponents];
    for (int i =0; i < numComponents; i++) {
      components[i] = createRandomShape();
    }
    Component2D component1 = LatLonComponent2DFactory.create(components);
    Component2D component2 = LatLonComponent2DFactory.create(components);
    assertEquals(component1, component2);
    assertEquals(component1.hashCode(), component2.hashCode());
    int otherNumComponents = TestUtil.nextInt(random(), 2, 10);
    Object[] otherComponents = new Object[otherNumComponents];
    for (int i =0; i < otherNumComponents; i++) {
      otherComponents[i] = createRandomShape();
    }
    Component2D component3 = LatLonComponent2DFactory.create(otherComponents);
    if (Arrays.equals(components, otherComponents)) {
      assertEquals(component1, component3);
      assertEquals(component1.hashCode(), component3.hashCode());
    } else {
      assertNotEquals(component1, component3);
      assertNotEquals(component1.hashCode(), component3.hashCode());
    }
  }

  public void testRandomTriangles() {
    int numComponents = TestUtil.nextInt(random(), 2, 10);
    Object[] components = new Object[numComponents];
    for (int i =0; i < numComponents; i++) {
      components[i] = createRandomShape();
    }
    Component2D component = LatLonComponent2DFactory.create(components);

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

      PointValues.Relation r = component.relate(tMinX, tMaxX, tMinY, tMaxY);
      if (r == PointValues.Relation.CELL_OUTSIDE_QUERY) {
        assertEquals(PointValues.Relation.CELL_OUTSIDE_QUERY, component.relateTriangle(ax, ay, bx, by, cx, cy));
      }
      if (component.contains(ax, ay)) {
        assertEquals(PointValues.Relation.CELL_INSIDE_QUERY, component.relateTriangle(ax, ay, ax, ay, ax, ay));
      }
      if (component.contains(bx, by)) {
        assertEquals(PointValues.Relation.CELL_INSIDE_QUERY, component.relateTriangle(bx, by, bx, by, bx, by));
      }
      if (component.contains(cx, cy)) {
        assertEquals(PointValues.Relation.CELL_INSIDE_QUERY, component.relateTriangle(cx, cy, cx, cy, cx, cy));
      }
    }
  }


  private Object createRandomShape() {
    int type = random().nextInt(4);
    switch (type) {
      case 0 : return new double[] {GeoTestUtil.nextLatitude(), GeoTestUtil.nextLongitude()};
      case 1 : return GeoTestUtil.nextBox();
      case 2 : return GeoTestUtil.nextPolygon();
      case 3 : return BaseLatLonShapeTestCase.getNextLine();
      default: throw new IllegalArgumentException("Unreachable code");
    }

  }
}
