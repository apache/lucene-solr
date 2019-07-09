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
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.util.LuceneTestCase;

public class TestLatLonLineComponent2D extends TestBaseLatLonComponent2D {

  @Override
  protected Object nextShape() {
    return TestLatLonLineShapeQueries.getNextLine();
  }

  @Override
  protected Component2D getComponent(Object shape) {
    if (random().nextBoolean()) {
      return LatLonComponent2DFactory.create(shape);
    } else {
      return LatLonComponent2DFactory.create((Line) shape);
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

  public void testLineSharedLine() {
    Line l = new Line(new double[] {0, 0, 0, 0}, new double[] {-2, -1, 0, 1});
    Component2D component = LatLonComponent2DFactory.create(l);
    Relation r = component.relateTriangle(
        GeoEncodingUtils.encodeLongitude(-5), GeoEncodingUtils.encodeLatitude(0),
        GeoEncodingUtils.encodeLongitude(5), GeoEncodingUtils.encodeLatitude(0),
        GeoEncodingUtils.encodeLongitude(-5), GeoEncodingUtils.encodeLatitude(0));
    assertEquals(Relation.CELL_CROSSES_QUERY, r);
  }
}
