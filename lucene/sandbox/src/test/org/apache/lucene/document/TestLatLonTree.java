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
package org.apache.lucene.document;

import org.apache.lucene.geo.GeoTestUtil;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.util.LuceneTestCase;

/** Test LatLonTree against the slower implementation for now */
public class TestLatLonTree extends LuceneTestCase {
  
  /** test that contains() works the same as brute force */
  public void testContainsRandom() {
    for (int i = 0; i < 1000; i++) {
      Polygon polygon = GeoTestUtil.nextPolygon();
      LatLonTree tree = new LatLonTree(polygon);
      for (int j = 0; j < 1000; j++) {
        double point[] = GeoTestUtil.nextPointNear(polygon);
        boolean expected = polygon.contains(point[0], point[1]);
        assertEquals(expected, tree.contains(point[0], point[1]));
      }
    }
  }
  
  /** test that relate() works the same as brute force */
  public void testRelateRandom() {
    for (int i = 0; i < 1000; i++) {
      Polygon polygon = GeoTestUtil.nextPolygon();
      LatLonTree tree = new LatLonTree(polygon);
      for (int j = 0; j < 1000; j++) {
        Rectangle box = GeoTestUtil.nextBoxNear(polygon);
        Relation expected = polygon.relate(box.minLat, box.maxLat, box.minLon, box.maxLon);
        assertEquals(expected, tree.relate(box.minLat, box.maxLat, box.minLon, box.maxLon));
      }
    }
  }
}
