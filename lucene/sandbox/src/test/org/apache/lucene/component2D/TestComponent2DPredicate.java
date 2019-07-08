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

import org.apache.lucene.document.BaseLatLonShapeTestCase;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.geo.GeoTestUtil;
import org.apache.lucene.geo.Line;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.bkd.PointValue;

public class TestComponent2DPredicate extends LuceneTestCase {

  public void testRandomPolygon() {
    Polygon polygon = GeoTestUtil.nextPolygon();
    Component2D component = LatLonComponent2DFactory.create(polygon);
    Component2DPredicate predicate = Component2DPredicate.createComponentPredicate(component);
    for (int i =0; i < 1000; i++) {
      int x = GeoEncodingUtils.encodeLongitude(GeoTestUtil.nextLongitude());
      int y = GeoEncodingUtils.encodeLatitude(GeoTestUtil.nextLatitude());
      assertEquals(component.contains(x, y), predicate.test(x, y));
      if (component.contains(x, y)) {
        assertEquals(PointValues.Relation.CELL_INSIDE_QUERY, component.relateTriangle(x, y, x, y, x, y));
      } else {
        assertEquals(PointValues.Relation.CELL_OUTSIDE_QUERY, component.relateTriangle(x, y, x, y, x, y));
      }
    }
  }

  public void testRandomRectangle() {
    Rectangle rectangle = GeoTestUtil.nextBox();
    Component2D component = LatLonComponent2DFactory.create(rectangle);
    Component2DPredicate predicate = Component2DPredicate.createComponentPredicate(component);
    for (int i =0; i < 1000; i++) {
      int x = GeoEncodingUtils.encodeLongitude(GeoTestUtil.nextLongitude());
      int y = GeoEncodingUtils.encodeLatitude(GeoTestUtil.nextLatitude());
      assertEquals(component.contains(x, y), predicate.test(x, y));
      if (component.contains(x, y)) {
        assertEquals(PointValues.Relation.CELL_INSIDE_QUERY, component.relateTriangle(x, y, x, y, x, y));
      } else {
        assertEquals(PointValues.Relation.CELL_OUTSIDE_QUERY, component.relateTriangle(x, y, x, y, x, y));
      }
    }
  }

  public void testRandomLine() {
    Line line = BaseLatLonShapeTestCase.nextLine();
    Component2D component = LatLonComponent2DFactory.create(line);
    Component2DPredicate predicate = Component2DPredicate.createComponentPredicate(component);
    for (int i =0; i < 1000; i++) {
      int x = GeoEncodingUtils.encodeLongitude(GeoTestUtil.nextLongitude());
      int y = GeoEncodingUtils.encodeLatitude(GeoTestUtil.nextLatitude());
      assertEquals(component.contains(x, y), predicate.test(x, y));
      if (component.contains(x, y)) {
        assertEquals(PointValues.Relation.CELL_INSIDE_QUERY, component.relateTriangle(x, y, x, y, x, y));
      } else {
        assertEquals(PointValues.Relation.CELL_OUTSIDE_QUERY, component.relateTriangle(x, y, x, y, x, y));
      }
    }
  }
  //-Dtests.seed=FA5F5CA075BE46E2
  public void testRandomPoint() {
    Component2D component = LatLonComponent2DFactory.create(new double[] {GeoTestUtil.nextLatitude(), GeoTestUtil.nextLongitude()});
    Component2DPredicate predicate = Component2DPredicate.createComponentPredicate(component);
    for (int i =0; i < 1000; i++) {
      int x = GeoEncodingUtils.encodeLongitude(GeoTestUtil.nextLongitude());
      int y = GeoEncodingUtils.encodeLatitude(GeoTestUtil.nextLatitude());
      assertEquals(component.contains(x, y), predicate.test(x, y));
      if (component.contains(x, y)) {
        assertEquals(PointValues.Relation.CELL_INSIDE_QUERY, component.relateTriangle(x, y, x, y, x, y));
      } else {
        assertEquals(PointValues.Relation.CELL_OUTSIDE_QUERY, component.relateTriangle(x, y, x, y, x, y));
      }
    }
  }
}
