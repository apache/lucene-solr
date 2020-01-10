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


public class TestXYRectangle2D extends LuceneTestCase {

  public void testTriangleDisjoint() {
    XYRectangle rectangle = new XYRectangle(0d, 1d, 0d, 1d);
    Component2D rectangle2D = XYRectangle2D.create(rectangle);
    float ax = 4f;
    float ay = 4f;
    float bx = 5f;
    float by = 5f;
    float cx = 5f;
    float cy = 4f;
    assertEquals(PointValues.Relation.CELL_OUTSIDE_QUERY, rectangle2D.relateTriangle(ax, ay, bx, by , cx, cy));
  }

  public void testTriangleIntersects() {
    XYRectangle rectangle = new XYRectangle(0d, 1d, 0d, 1d);
    Component2D rectangle2D =  XYRectangle2D.create(rectangle);
    float ax = 0.5f;
    float ay = 0.5f;
    float bx = 2f;
    float by = 2f;
    float cx = 0.5f;
    float cy = 2f;
    assertEquals(PointValues.Relation.CELL_CROSSES_QUERY, rectangle2D.relateTriangle(ax, ay, bx, by , cx, cy));
  }

  public void testTriangleContains() {
    XYRectangle rectangle = new XYRectangle(0, 1, 0, 1);
    Component2D rectangle2D =  XYRectangle2D.create(rectangle);
    float ax = 0.25f;
    float ay = 0.25f;
    float bx = 0.5f;
    float by = 0.5f;
    float cx = 0.5f;
    float cy = 0.25f;
    assertEquals(PointValues.Relation.CELL_INSIDE_QUERY, rectangle2D.relateTriangle(ax, ay, bx, by , cx, cy));
  }

  public void testRandomTriangles() {
    XYRectangle rectangle = ShapeTestUtil.nextBox();
    Component2D rectangle2D = XYRectangle2D.create(rectangle);
    for (int i =0; i < 100; i++) {
      float ax = (float) ShapeTestUtil.nextDouble();
      float ay = (float) ShapeTestUtil.nextDouble();
      float bx = (float) ShapeTestUtil.nextDouble();
      float by = (float) ShapeTestUtil.nextDouble();
      float cx = (float) ShapeTestUtil.nextDouble();
      float cy = (float) ShapeTestUtil.nextDouble();

      float tMinX = StrictMath.min(StrictMath.min(ax, bx), cx);
      float tMaxX = StrictMath.max(StrictMath.max(ax, bx), cx);
      float tMinY = StrictMath.min(StrictMath.min(ay, by), cy);
      float tMaxY = StrictMath.max(StrictMath.max(ay, by), cy);


      PointValues.Relation r = rectangle2D.relate(tMinX, tMaxX, tMinY, tMaxY);
      if (r == PointValues.Relation.CELL_OUTSIDE_QUERY) {
        assertEquals(PointValues.Relation.CELL_OUTSIDE_QUERY, rectangle2D.relateTriangle(ax, ay, bx, by , cx, cy));
      }
      else if (r == PointValues.Relation.CELL_INSIDE_QUERY) {
        assertEquals(PointValues.Relation.CELL_INSIDE_QUERY, rectangle2D.relateTriangle(ax, ay, bx, by , cx, cy));
      }
    }
  }
}