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

import java.util.Random;

import org.apache.lucene.index.PointValues;
import org.apache.lucene.util.LuceneTestCase;


public class TestRectangle2D extends LuceneTestCase {

  public void testTriangleDisjoint() {
    XYRectangle rectangle = new XYRectangle(0f, 1f, 0f, 1f);
    Component2D rectangle2D = Rectangle2D.create(rectangle);
    float ax = 4f;
    float ay = 4f;
    float bx = 5f;
    float by = 5f;
    float cx = 5f;
    float cy = 4f;
    assertFalse(rectangle2D.intersectsTriangle(ax, ay, bx, by , cx, cy));
    assertFalse(rectangle2D.intersectsLine(ax, ay, bx, by));
    assertFalse(rectangle2D.containsTriangle(ax, ay, bx, by , cx, cy));
    assertFalse(rectangle2D.containsLine(ax, ay, bx, by));
    assertEquals(Component2D.WithinRelation.DISJOINT,
        rectangle2D.withinTriangle(ax, ay, random().nextBoolean(), bx, by, random().nextBoolean(), cx, cy, random().nextBoolean()));
  }

  public void testTriangleIntersects() {
    XYRectangle rectangle = new XYRectangle(0f, 1f, 0f, 1f);
    Component2D rectangle2D = Rectangle2D.create(rectangle);
    float ax = 0.5f;
    float ay = 0.5f;
    float bx = 2f;
    float by = 2f;
    float cx = 0.5f;
    float cy = 2f;
    assertTrue(rectangle2D.intersectsTriangle(ax, ay, bx, by , cx, cy));
    assertTrue(rectangle2D.intersectsLine(ax, ay, bx, by));
    assertFalse(rectangle2D.containsTriangle(ax, ay, bx, by , cx, cy));
    assertFalse(rectangle2D.containsLine(ax, ay, bx, by));
    assertEquals(Component2D.WithinRelation.NOTWITHIN,
        rectangle2D.withinTriangle(ax, ay, true, bx, by, true, cx, cy, true));
  }

  public void testTriangleContains() {
    XYRectangle rectangle = new XYRectangle(0, 1, 0, 1);
    Component2D rectangle2D = Rectangle2D.create(rectangle);
    float ax = 0.25f;
    float ay = 0.25f;
    float bx = 0.5f;
    float by = 0.5f;
    float cx = 0.5f;
    float cy = 0.25f;
    assertTrue(rectangle2D.intersectsTriangle(ax, ay, bx, by , cx, cy));
    assertTrue(rectangle2D.intersectsLine(ax, ay, bx, by));
    assertTrue(rectangle2D.containsTriangle(ax, ay, bx, by , cx, cy));
    assertTrue(rectangle2D.containsLine(ax, ay, bx, by));
    assertEquals(Component2D.WithinRelation.NOTWITHIN, rectangle2D.withinTriangle(ax, ay, true, bx, by , true, cx, cy, true));
  }

  public void testRandomTriangles() {
    Random random = random();
    XYRectangle rectangle = ShapeTestUtil.nextBox(random);
    Component2D rectangle2D = Rectangle2D.create(rectangle);
    for (int i =0; i < 100; i++) {
      float ax = ShapeTestUtil.nextFloat(random);
      float ay = ShapeTestUtil.nextFloat(random);
      float bx = ShapeTestUtil.nextFloat(random);
      float by = ShapeTestUtil.nextFloat(random);
      float cx = ShapeTestUtil.nextFloat(random);
      float cy = ShapeTestUtil.nextFloat(random);

      float tMinX = StrictMath.min(StrictMath.min(ax, bx), cx);
      float tMaxX = StrictMath.max(StrictMath.max(ax, bx), cx);
      float tMinY = StrictMath.min(StrictMath.min(ay, by), cy);
      float tMaxY = StrictMath.max(StrictMath.max(ay, by), cy);


      PointValues.Relation r = rectangle2D.relate(tMinX, tMaxX, tMinY, tMaxY);
      if (r == PointValues.Relation.CELL_OUTSIDE_QUERY) {
        assertFalse(rectangle2D.intersectsTriangle(ax, ay, bx, by, cx, cy));
        assertFalse(rectangle2D.intersectsLine(ax, ay, bx, by));
        assertFalse(rectangle2D.containsTriangle(ax, ay, bx, by , cx, cy));
        assertFalse(rectangle2D.containsLine(ax, ay, bx, by));
        assertEquals(Component2D.WithinRelation.DISJOINT, rectangle2D.withinTriangle(ax, ay, true, bx, by, true, cx, cy, true));
      }
      else if (r == PointValues.Relation.CELL_INSIDE_QUERY) {
        assertTrue(rectangle2D.intersectsTriangle(ax, ay, bx, by, cx, cy));
        assertTrue(rectangle2D.intersectsLine(ax, ay, bx, by));
        assertTrue(rectangle2D.containsTriangle(ax, ay, bx, by , cx, cy));
        assertTrue(rectangle2D.containsLine(ax, ay, bx, by));
      }
    }
  }
}