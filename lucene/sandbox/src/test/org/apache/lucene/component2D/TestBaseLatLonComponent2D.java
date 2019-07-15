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

public abstract class TestBaseLatLonComponent2D extends TestBaseComponent2D {

  @Override
  protected Component2D getComponentInside(Component2D component) {
    for (int i =0; i < 500; i++) {
      Rectangle rectangle = GeoTestUtil.nextBoxNotCrossingDateline();
      // allowed to conservatively return false
      if (component.relate(GeoEncodingUtils.encodeLongitude(rectangle.minLon), GeoEncodingUtils.encodeLongitude(rectangle.maxLon),
          GeoEncodingUtils.encodeLatitude(rectangle.minLat), GeoEncodingUtils.encodeLatitude(rectangle.maxLat)) == PointValues.Relation.CELL_INSIDE_QUERY) {
        return LatLonComponent2DFactory.create(rectangle);
      }
    }
    return null;
  }

  @Override
  protected int nextEncodedX() {
    return GeoEncodingUtils.encodeLongitude(GeoTestUtil.nextLongitude());
  }

  @Override
  protected int nextEncodedY() {
    return GeoEncodingUtils.encodeLatitude(GeoTestUtil.nextLatitude());
  }

  public void testComponentPredicate() {
    Object shape = nextShape();
    Component2D component = getComponent(shape);
    Component2DPredicate predicate = LatLonComponent2DFactory.createComponentPredicate(component);
    for (int i =0; i < 1000; i++) {
      int x = nextEncodedX();
      int y = nextEncodedY();
      assertEquals(component.contains(x, y), predicate.test(x, y));
      if (component.contains(x, y)) {
        assertEquals(PointValues.Relation.CELL_INSIDE_QUERY, component.relateTriangle(x, y, x, y, x, y));
      } else {
        assertEquals(PointValues.Relation.CELL_OUTSIDE_QUERY, component.relateTriangle(x, y, x, y, x, y));
      }
    }
  }
}
