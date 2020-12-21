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

import java.util.Arrays;

import org.apache.lucene.document.ShapeField.QueryRelation;
import org.apache.lucene.geo.GeoTestUtil;
import org.apache.lucene.geo.Line;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.geo.Circle;


/** Base test case for testing geospatial indexing and search functionality  for {@link LatLonShape} **/
public abstract class BaseLatLonShapeTestCase extends BaseLatLonSpatialTestCase {
  
  @Override
  protected Query newRectQuery(String field, QueryRelation queryRelation, double minLon, double maxLon, double minLat, double maxLat) {
    return LatLonShape.newBoxQuery(field, queryRelation, minLat, maxLat, minLon, maxLon);
  }
  
  @Override
  protected Query newLineQuery(String field, QueryRelation queryRelation, Object... lines) {
    return LatLonShape.newLineQuery(field, queryRelation, Arrays.stream(lines).toArray(Line[]::new));
  }
  
  @Override
  protected Query newPolygonQuery(String field, QueryRelation queryRelation, Object... polygons) {
    return LatLonShape.newPolygonQuery(field, queryRelation, Arrays.stream(polygons).toArray(Polygon[]::new));
  }
  
  @Override
  protected Query newPointsQuery(String field, QueryRelation queryRelation, Object... points) {
    return LatLonShape.newPointQuery(field, queryRelation, Arrays.stream(points).toArray(double[][]::new));
  }
  
  @Override
  protected Query newDistanceQuery(String field, QueryRelation queryRelation, Object circle) {
    return LatLonShape.newDistanceQuery(field, queryRelation, (Circle) circle);
  }

  public void testBoundingBoxQueriesEquivalence() throws Exception {
    int numShapes = atLeast(20);

    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    for (int  i =0; i < numShapes; i++) {
      indexRandomShapes(w.w, nextShape());
    }
    if (random().nextBoolean()) {
      w.forceMerge(1);
    }

    ///// search //////
    IndexReader reader = w.getReader();
    w.close();
    IndexSearcher searcher = newSearcher(reader);

    Rectangle box = GeoTestUtil.nextBox();

    Query q1 = LatLonShape.newBoxQuery(FIELD_NAME, QueryRelation.INTERSECTS, box.minLat, box.maxLat, box.minLon, box.maxLon);
    Query q2 = new LatLonShapeQuery(FIELD_NAME, QueryRelation.INTERSECTS, box);
    assertEquals(searcher.count(q1), searcher.count(q2));
    q1 = LatLonShape.newBoxQuery(FIELD_NAME, QueryRelation.WITHIN, box.minLat, box.maxLat, box.minLon, box.maxLon);
    q2 = new LatLonShapeQuery(FIELD_NAME, QueryRelation.WITHIN, box);
    assertEquals(searcher.count(q1), searcher.count(q2));
    q1 = LatLonShape.newBoxQuery(FIELD_NAME, QueryRelation.CONTAINS, box.minLat, box.maxLat, box.minLon, box.maxLon);
    if (box.crossesDateline()) {
      q2 = LatLonShape.newGeometryQuery(FIELD_NAME, QueryRelation.CONTAINS, box);
    } else {
      q2 = new LatLonShapeQuery(FIELD_NAME, QueryRelation.CONTAINS, box);
    }
    assertEquals(searcher.count(q1), searcher.count(q2));
    q1 = LatLonShape.newBoxQuery(FIELD_NAME, QueryRelation.DISJOINT, box.minLat, box.maxLat, box.minLon, box.maxLon);
    q2 = new LatLonShapeQuery(FIELD_NAME, QueryRelation.DISJOINT, box);
    assertEquals(searcher.count(q1), searcher.count(q2));

    IOUtils.close(w, reader, dir);
  }
}
