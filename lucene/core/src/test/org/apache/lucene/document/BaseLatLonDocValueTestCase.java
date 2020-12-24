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
import org.apache.lucene.geo.Circle;
import org.apache.lucene.geo.Line;
import org.apache.lucene.geo.Point;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.search.Query;

/**
 * Base test case for testing geospatial indexing and search functionality for {@link
 * LatLonDocValuesField} *
 */
public abstract class BaseLatLonDocValueTestCase extends BaseLatLonSpatialTestCase {

  @Override
  protected Query newRectQuery(
      String field,
      QueryRelation queryRelation,
      double minLon,
      double maxLon,
      double minLat,
      double maxLat) {
    return LatLonDocValuesField.newSlowGeometryQuery(
        field, queryRelation, new Rectangle(minLat, maxLat, minLon, maxLon));
  }

  @Override
  protected Query newLineQuery(String field, QueryRelation queryRelation, Object... lines) {
    return LatLonDocValuesField.newSlowGeometryQuery(
        field, queryRelation, Arrays.stream(lines).toArray(Line[]::new));
  }

  @Override
  protected Query newPolygonQuery(String field, QueryRelation queryRelation, Object... polygons) {
    return LatLonDocValuesField.newSlowGeometryQuery(
        field, queryRelation, Arrays.stream(polygons).toArray(Polygon[]::new));
  }

  @Override
  protected Query newDistanceQuery(String field, QueryRelation queryRelation, Object circle) {
    return LatLonDocValuesField.newSlowGeometryQuery(field, queryRelation, (Circle) circle);
  }

  @Override
  protected Query newPointsQuery(String field, QueryRelation queryRelation, Object... points) {
    Point[] pointsArray = new Point[points.length];
    for (int i = 0; i < points.length; i++) {
      double[] point = (double[]) points[i];
      pointsArray[i] = new Point(point[0], point[1]);
    }
    return LatLonDocValuesField.newSlowGeometryQuery(field, queryRelation, pointsArray);
  }
}
