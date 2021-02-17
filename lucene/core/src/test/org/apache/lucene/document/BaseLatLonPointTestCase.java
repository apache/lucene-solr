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

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import java.util.Arrays;
import org.apache.lucene.document.ShapeField.QueryRelation;
import org.apache.lucene.geo.Circle;
import org.apache.lucene.geo.GeoTestUtil;
import org.apache.lucene.geo.Line;
import org.apache.lucene.geo.Point;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryUtils;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;

/**
 * Base test case for testing geospatial indexing and search functionality for {@link LatLonPoint} *
 */
public abstract class BaseLatLonPointTestCase extends BaseLatLonSpatialTestCase {

  @Override
  protected Query newRectQuery(
      String field,
      QueryRelation queryRelation,
      double minLon,
      double maxLon,
      double minLat,
      double maxLat) {
    return LatLonPoint.newGeometryQuery(
        field, queryRelation, new Rectangle(minLat, maxLat, minLon, maxLon));
  }

  @Override
  protected Query newLineQuery(String field, QueryRelation queryRelation, Object... lines) {
    return LatLonPoint.newGeometryQuery(
        field, queryRelation, Arrays.stream(lines).toArray(Line[]::new));
  }

  @Override
  protected Query newPolygonQuery(String field, QueryRelation queryRelation, Object... polygons) {
    return LatLonPoint.newGeometryQuery(
        field, queryRelation, Arrays.stream(polygons).toArray(Polygon[]::new));
  }

  @Override
  protected Query newDistanceQuery(String field, QueryRelation queryRelation, Object circle) {
    return LatLonPoint.newGeometryQuery(field, queryRelation, (Circle) circle);
  }

  @Override
  protected Query newPointsQuery(String field, QueryRelation queryRelation, Object... points) {
    Point[] pointsArray = new Point[points.length];
    for (int i = 0; i < points.length; i++) {
      double[] point = (double[]) points[i];
      pointsArray[i] = new Point(point[0], point[1]);
    }
    return LatLonPoint.newGeometryQuery(field, queryRelation, pointsArray);
  }

  public void testBoundingBoxQueriesEquivalence() throws Exception {
    int numShapes = atLeast(20);

    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    for (int i = 0; i < numShapes; i++) {
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

    Query q1 = LatLonPoint.newBoxQuery(FIELD_NAME, box.minLat, box.maxLat, box.minLon, box.maxLon);
    Query q2 = new LatLonPointQuery(FIELD_NAME, QueryRelation.INTERSECTS, box);
    assertEquals(searcher.count(q1), searcher.count(q2));

    IOUtils.close(w, reader, dir);
  }

  public void testQueryEqualsAndHashcode() {
    Polygon polygon = GeoTestUtil.nextPolygon();
    QueryRelation queryRelation =
        RandomPicks.randomFrom(
            random(), new QueryRelation[] {QueryRelation.INTERSECTS, QueryRelation.DISJOINT});
    String fieldName = "foo";
    Query q1 = newPolygonQuery(fieldName, queryRelation, polygon);
    Query q2 = newPolygonQuery(fieldName, queryRelation, polygon);
    QueryUtils.checkEqual(q1, q2);
    // different field name
    Query q3 = newPolygonQuery("bar", queryRelation, polygon);
    QueryUtils.checkUnequal(q1, q3);
    // different query relation
    QueryRelation newQueryRelation =
        RandomPicks.randomFrom(
            random(), new QueryRelation[] {QueryRelation.INTERSECTS, QueryRelation.DISJOINT});
    Query q4 = newPolygonQuery(fieldName, newQueryRelation, polygon);
    if (queryRelation == newQueryRelation) {
      QueryUtils.checkEqual(q1, q4);
    } else {
      QueryUtils.checkUnequal(q1, q4);
    }
    // different shape
    Polygon newPolygon = GeoTestUtil.nextPolygon();
    ;
    Query q5 = newPolygonQuery(fieldName, queryRelation, newPolygon);
    if (polygon.equals(newPolygon)) {
      QueryUtils.checkEqual(q1, q5);
    } else {
      QueryUtils.checkUnequal(q1, q5);
    }
  }
}
