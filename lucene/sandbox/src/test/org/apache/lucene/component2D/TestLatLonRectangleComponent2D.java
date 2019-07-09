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

import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.geo.GeoTestUtil;
import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.index.PointValues;

public class TestLatLonRectangleComponent2D extends TestBaseLatLonComponent2D {

  @Override
  protected Object nextShape() {
    return GeoTestUtil.nextBox();
  }

  @Override
  protected Component2D getComponent(Object shape) {
    if (random().nextBoolean()) {
      return LatLonComponent2DFactory.create(shape);
    } else {
      return LatLonComponent2DFactory.create((Rectangle) shape);
    }
  }

  public void testTriangleDisjoint() {
    Rectangle rectangle = new Rectangle(0, 1, 0, 1);
    Component2D component = LatLonComponent2DFactory.create(rectangle);
    int ax = GeoEncodingUtils.encodeLongitude(4);
    int ay = GeoEncodingUtils.encodeLatitude(4);
    int bx = GeoEncodingUtils.encodeLongitude(5);
    int by = GeoEncodingUtils.encodeLatitude(5);
    int cx = GeoEncodingUtils.encodeLongitude(5);
    int cy = GeoEncodingUtils.encodeLatitude(4);
    assertEquals(PointValues.Relation.CELL_OUTSIDE_QUERY, component.relateTriangle(ax, ay, bx, by , cx, cy));
    int minX = GeoEncodingUtils.encodeLongitude(4);
    int maxX = GeoEncodingUtils.encodeLatitude(5);
    int minY = GeoEncodingUtils.encodeLongitude(4);
    int maxY = GeoEncodingUtils.encodeLatitude(5);
    assertEquals(PointValues.Relation.CELL_OUTSIDE_QUERY, component.relate(minX, maxX, minY, maxY));
  }

  public void testTriangleIntersects() {
    Rectangle rectangle = new Rectangle(0, 1, 0, 1);
    Component2D component =  LatLonComponent2DFactory.create(rectangle);
    int ax = GeoEncodingUtils.encodeLongitude(0.5);
    int ay = GeoEncodingUtils.encodeLatitude(0.5);
    int bx = GeoEncodingUtils.encodeLongitude(2);
    int by = GeoEncodingUtils.encodeLatitude(2);
    int cx = GeoEncodingUtils.encodeLongitude(0.5);
    int cy = GeoEncodingUtils.encodeLatitude(2);
    assertEquals(PointValues.Relation.CELL_CROSSES_QUERY, component.relateTriangle(ax, ay, bx, by , cx, cy));
    int minX = GeoEncodingUtils.encodeLongitude(0.5);
    int maxX = GeoEncodingUtils.encodeLatitude(2);
    int minY = GeoEncodingUtils.encodeLongitude(0.5);
    int maxY = GeoEncodingUtils.encodeLatitude(2);
    assertEquals(PointValues.Relation.CELL_CROSSES_QUERY, component.relate(minX, maxX, minY, maxY));
  }

  public void testTriangleWithin() {
    Rectangle rectangle = new Rectangle(0, 1, 0, 1);
    Component2D component =  LatLonComponent2DFactory.create(rectangle);
    int ax = GeoEncodingUtils.encodeLongitude(0.25);
    int ay = GeoEncodingUtils.encodeLatitude(0.25);
    int bx = GeoEncodingUtils.encodeLongitude(0.5);
    int by = GeoEncodingUtils.encodeLatitude(0.5);
    int cx = GeoEncodingUtils.encodeLongitude(0.5);
    int cy = GeoEncodingUtils.encodeLatitude(0.25);
    assertEquals(PointValues.Relation.CELL_INSIDE_QUERY, component.relateTriangle(ax, ay, bx, by , cx, cy));
    int minX = GeoEncodingUtils.encodeLongitude(0.25);
    int maxX = GeoEncodingUtils.encodeLatitude(0.5);
    int minY = GeoEncodingUtils.encodeLongitude(0.25);
    int maxY = GeoEncodingUtils.encodeLatitude(0.5);
    assertEquals(PointValues.Relation.CELL_INSIDE_QUERY, component.relate(minX, maxX, minY, maxY));
  }

  public void testTriangleContains() {
    Rectangle rectangle = new Rectangle(0, 1, 0, 1);
    Component2D component =  LatLonComponent2DFactory.create(rectangle);
    int ax = GeoEncodingUtils.encodeLongitude(-60.);
    int ay = GeoEncodingUtils.encodeLatitude(-1);
    int bx = GeoEncodingUtils.encodeLongitude(2);
    int by = GeoEncodingUtils.encodeLatitude(-1);
    int cx = GeoEncodingUtils.encodeLongitude(2);
    int cy = GeoEncodingUtils.encodeLatitude(60);
    assertEquals(PointValues.Relation.CELL_CROSSES_QUERY, component.relateTriangle(ax, ay, bx, by , cx, cy));
    int minX = GeoEncodingUtils.encodeLongitude(-1);
    int maxX = GeoEncodingUtils.encodeLatitude(2);
    int minY = GeoEncodingUtils.encodeLongitude(-1);
    int maxY = GeoEncodingUtils.encodeLatitude(2);
    assertEquals(PointValues.Relation.CELL_CROSSES_QUERY, component.relate(minX, maxX, minY, maxY));
  }
}
