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
package org.apache.lucene.search;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.document.ShapeField;
import org.apache.lucene.geo.BaseGeoPointTestCase;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.geo.Polygon;

public class TestLatLonDocValuesQueries extends BaseGeoPointTestCase {

  @Override
  protected void addPointToDoc(String field, Document doc, double lat, double lon) {
    doc.add(new LatLonDocValuesField(field, lat, lon));
  }

  @Override
  protected Query newRectQuery(String field, double minLat, double maxLat, double minLon, double maxLon) {
    return LatLonDocValuesField.newSlowBoxQuery(field, minLat, maxLat, minLon, maxLon);
  }

  @Override
  protected Query newDistanceQuery(String field, double centerLat, double centerLon, double radiusMeters) {
    return LatLonDocValuesField.newSlowDistanceQuery(field, centerLat, centerLon, radiusMeters);
  }

  @Override
  protected Query newPolygonQuery(String field, Polygon... polygons) {
    return LatLonDocValuesField.newSlowPolygonQuery(field, polygons);
  }

  @Override
  protected Query newGeometryQuery(String field, LatLonGeometry... geometry) {
    return LatLonDocValuesField.newSlowGeometryQuery(
        field, ShapeField.QueryRelation.INTERSECTS, geometry);
  }

  @Override
  protected double quantizeLat(double latRaw) {
    return GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(latRaw));
  }

  @Override
  protected double quantizeLon(double lonRaw) {
    return GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(lonRaw));
  }
}
