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
 * Base test case for testing geospatial indexing and search functionality for {@link LatLonShape} *
 */
public abstract class BaseLatLonShapeTestCase extends BaseLatLonSpatialTestCase {

  @Override
  protected Query newRectQuery(
          String field,
          QueryRelation queryRelation,
          double minLon,
          double maxLon,
          double minLat,
          double maxLat) {
    return LatLonShape.newBoxQuery(field, queryRelation, minLat, maxLat, minLon, maxLon);
  }

  @Override
  protected Query newLineQuery(String field, QueryRelation queryRelation, Object... lines) {
    return LatLonShape.newLineQuery(
            field, queryRelation, Arrays.stream(lines).toArray(Line[]::new));
  }

  @Override
  protected Query newPolygonQuery(String field, QueryRelation queryRelation, Object... polygons) {
    return LatLonShape.newPolygonQuery(
            field, queryRelation, Arrays.stream(polygons).toArray(Polygon[]::new));
  }

  @Override
  protected Query newPointsQuery(String field, QueryRelation queryRelation, Object... points) {
    return LatLonShape.newPointQuery(
            field, queryRelation, Arrays.stream(points).toArray(double[][]::new));
  }

  @Override
  protected Query newDistanceQuery(String field, QueryRelation queryRelation, Object circle) {
    return LatLonShape.newDistanceQuery(field, queryRelation, (Circle) circle);
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

    Query q1 =
            LatLonShape.newBoxQuery(
                    FIELD_NAME, QueryRelation.INTERSECTS, box.minLat, box.maxLat, box.minLon, box.maxLon);
    Query q2 = new LatLonShapeQuery(FIELD_NAME, QueryRelation.INTERSECTS, box);
    assertEquals(searcher.count(q1), searcher.count(q2));
    q1 =
            LatLonShape.newBoxQuery(
                    FIELD_NAME, QueryRelation.WITHIN, box.minLat, box.maxLat, box.minLon, box.maxLon);
    q2 = new LatLonShapeQuery(FIELD_NAME, QueryRelation.WITHIN, box);
    assertEquals(searcher.count(q1), searcher.count(q2));
    q1 =
            LatLonShape.newBoxQuery(
                    FIELD_NAME, QueryRelation.CONTAINS, box.minLat, box.maxLat, box.minLon, box.maxLon);
    if (box.crossesDateline()) {
      q2 = LatLonShape.newGeometryQuery(FIELD_NAME, QueryRelation.CONTAINS, box);
    } else {
      q2 = new LatLonShapeQuery(FIELD_NAME, QueryRelation.CONTAINS, box);
    }
    assertEquals(searcher.count(q1), searcher.count(q2));
    q1 =
            LatLonShape.newBoxQuery(
                    FIELD_NAME, QueryRelation.DISJOINT, box.minLat, box.maxLat, box.minLon, box.maxLon);
    q2 = new LatLonShapeQuery(FIELD_NAME, QueryRelation.DISJOINT, box);
    assertEquals(searcher.count(q1), searcher.count(q2));

    IOUtils.close(w, reader, dir);
  }

  public void testBoxQueryEqualsAndHashcode() {
    Rectangle rectangle = GeoTestUtil.nextBox();
    QueryRelation queryRelation = RandomPicks.randomFrom(random(), QueryRelation.values());
    String fieldName = "foo";
    Query q1 =
            newRectQuery(
                    fieldName,
                    queryRelation,
                    rectangle.minLon,
                    rectangle.maxLon,
                    rectangle.minLat,
                    rectangle.maxLat);
    Query q2 =
            newRectQuery(
                    fieldName,
                    queryRelation,
                    rectangle.minLon,
                    rectangle.maxLon,
                    rectangle.minLat,
                    rectangle.maxLat);
    QueryUtils.checkEqual(q1, q2);
    // different field name
    Query q3 =
            newRectQuery(
                    "bar",
                    queryRelation,
                    rectangle.minLon,
                    rectangle.maxLon,
                    rectangle.minLat,
                    rectangle.maxLat);
    QueryUtils.checkUnequal(q1, q3);
    // different query relation
    QueryRelation newQueryRelation = RandomPicks.randomFrom(random(), QueryRelation.values());
    Query q4 =
            newRectQuery(
                    fieldName,
                    newQueryRelation,
                    rectangle.minLon,
                    rectangle.maxLon,
                    rectangle.minLat,
                    rectangle.maxLat);
    if (queryRelation == newQueryRelation) {
      QueryUtils.checkEqual(q1, q4);
    } else {
      QueryUtils.checkUnequal(q1, q4);
    }
    // different shape
    Rectangle newRectangle = GeoTestUtil.nextBox();
    Query q5 =
            newRectQuery(
                    fieldName,
                    queryRelation,
                    newRectangle.minLon,
                    newRectangle.maxLon,
                    newRectangle.minLat,
                    newRectangle.maxLat);
    if (rectangle.equals(newRectangle)) {
      QueryUtils.checkEqual(q1, q5);
    } else {
      QueryUtils.checkUnequal(q1, q5);
    }
  }

  public void testLineQueryEqualsAndHashcode() {
    Line line = nextLine();
    QueryRelation queryRelation = RandomPicks.randomFrom(random(), POINT_LINE_RELATIONS);
    String fieldName = "foo";
    Query q1 = newLineQuery(fieldName, queryRelation, line);
    Query q2 = newLineQuery(fieldName, queryRelation, line);
    QueryUtils.checkEqual(q1, q2);
    // different field name
    Query q3 = newLineQuery("bar", queryRelation, line);
    QueryUtils.checkUnequal(q1, q3);
    // different query relation
    QueryRelation newQueryRelation = RandomPicks.randomFrom(random(), POINT_LINE_RELATIONS);
    Query q4 = newLineQuery(fieldName, newQueryRelation, line);
    if (queryRelation == newQueryRelation) {
      QueryUtils.checkEqual(q1, q4);
    } else {
      QueryUtils.checkUnequal(q1, q4);
    }
    // different shape
    Line newLine = nextLine();
    Query q5 = newLineQuery(fieldName, queryRelation, newLine);
    if (line.equals(newLine)) {
      QueryUtils.checkEqual(q1, q5);
    } else {
      QueryUtils.checkUnequal(q1, q5);
    }
  }

  public void testPolygonQueryEqualsAndHashcode() {
    Polygon polygon = GeoTestUtil.nextPolygon();
    QueryRelation queryRelation = RandomPicks.randomFrom(random(), QueryRelation.values());
    String fieldName = "foo";
    Query q1 = newPolygonQuery(fieldName, queryRelation, polygon);
    Query q2 = newPolygonQuery(fieldName, queryRelation, polygon);
    QueryUtils.checkEqual(q1, q2);
    // different field name
    Query q3 = newPolygonQuery("bar", queryRelation, polygon);
    QueryUtils.checkUnequal(q1, q3);
    // different query relation
    QueryRelation newQueryRelation = RandomPicks.randomFrom(random(), QueryRelation.values());
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
