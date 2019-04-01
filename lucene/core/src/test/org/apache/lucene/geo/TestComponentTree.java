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

/** Test Component tree*/
public class TestComponentTree extends LuceneTestCase {

  public void testMultiPolygon() {
    int numComponents = random().nextInt(30) + 1;
    Polygon[] polygon = new Polygon[numComponents];
    for (int i =0; i < numComponents; i++) {
      polygon[i] = GeoTestUtil.nextPolygon();
    }
    Component component = Polygon2D.create(polygon);
    Component[] components = new Component[numComponents];
    for (int i =0; i < numComponents; i++) {
      components[i] = Polygon2D.create(polygon[i]);
    }
    for (int i =0; i < 100; i++) {
      double lat = GeoTestUtil.nextLatitude();
      double lon = GeoTestUtil.nextLongitude();
      assertEquals(component.contains(lat, lon), contains(components, lat, lon));
    }
    // Note that if components overlap, we can either get CELL_CROSSES_QUERY or CELL_INSIDE_QUERY
    // depending on the order.
    for (int i =0; i < 100; i++) {
      Rectangle rectangle = GeoTestUtil.nextBoxNotCrossingDateline();
      PointValues.Relation r1 = component.relate(rectangle.minLat, rectangle.maxLat, rectangle.minLon, rectangle.maxLon);
      PointValues.Relation r2 = relate(components, rectangle);
      assertTrue(r1 == r2 || (r1 != PointValues.Relation.CELL_OUTSIDE_QUERY && r2 != PointValues.Relation.CELL_OUTSIDE_QUERY));
    }
    for (int i =0; i < 100; i++) {
      double ay = GeoTestUtil.nextLatitude();
      double ax = GeoTestUtil.nextLongitude();
      double by = GeoTestUtil.nextLatitude();
      double bx = GeoTestUtil.nextLongitude();
      double cy = GeoTestUtil.nextLatitude();
      double cx = GeoTestUtil.nextLongitude();
      PointValues.Relation r1 = component.relateTriangle(ax, ay, bx, by, cx, cy);
      PointValues.Relation r2 = relateTriangle(components, ax, ay, bx, by, cx, cy);
      assertTrue(r1 == r2 || (r1 != PointValues.Relation.CELL_OUTSIDE_QUERY && r2 != PointValues.Relation.CELL_OUTSIDE_QUERY));
    }
  }

  private boolean contains(Component[] components, double lat, double lon) {
    for (Component component : components) {
      Component newComponent = ComponentTree.create(component);
      if (newComponent.contains(lat, lon)) {
        return true;
      }
    }
    return false;
  }

  private PointValues.Relation relate(Component[] components, Rectangle rectangle) {
    boolean inside = false;
    for (Component component : components) {
      Component newComponent = ComponentTree.create(component);
      PointValues.Relation relation = newComponent.relate(rectangle.minLat, rectangle.maxLat, rectangle.minLon, rectangle.maxLon);
      if (relation == PointValues.Relation.CELL_CROSSES_QUERY) {
        return relation;
      } else if (relation == PointValues.Relation.CELL_INSIDE_QUERY) {
        inside = true;
      }
    }
    return (inside) ? PointValues.Relation.CELL_INSIDE_QUERY : PointValues.Relation.CELL_OUTSIDE_QUERY;
  }

  private PointValues.Relation relateTriangle(Component[] components,double ax, double ay, double bx, double by, double cx, double cy) {
    boolean inside = false;
    for (Component component : components) {
      Component newComponent = ComponentTree.create(component);
      PointValues.Relation relation = newComponent.relateTriangle(ax, ay, bx, by, cx, cy);
      if (relation == PointValues.Relation.CELL_CROSSES_QUERY) {
        return relation;
      } else if (relation == PointValues.Relation.CELL_INSIDE_QUERY) {
        inside = true;
      }
    }
    return (inside) ? PointValues.Relation.CELL_INSIDE_QUERY : PointValues.Relation.CELL_OUTSIDE_QUERY;
  }
}
