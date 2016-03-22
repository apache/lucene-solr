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
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.spatial.util.BaseGeoPointTestCase;
import org.apache.lucene.spatial.util.GeoRect;

public class TestLatLonPointQueries extends BaseGeoPointTestCase {

  @Override
  protected void addPointToDoc(String field, Document doc, double lat, double lon) {
    doc.add(new LatLonPoint(field, lat, lon));
  }

  @Override
  protected Query newRectQuery(String field, GeoRect rect) {
    return LatLonPoint.newBoxQuery(field, rect.minLat, rect.maxLat, rect.minLon, rect.maxLon);
  }

  @Override
  protected Query newDistanceQuery(String field, double centerLat, double centerLon, double radiusMeters) {
    return LatLonPoint.newDistanceQuery(field, centerLat, centerLon, radiusMeters);
  }

  @Override
  protected Query newDistanceRangeQuery(String field, double centerLat, double centerLon, double minRadiusMeters, double radiusMeters) {
    return null;
  }

  @Override
  protected Query newPolygonQuery(String field, double[] lats, double[] lons) {
    return LatLonPoint.newPolygonQuery(FIELD_NAME, lats, lons);
  }

  @Override
  protected double quantizeLat(double latRaw) {
    return LatLonPoint.decodeLatitude(LatLonPoint.encodeLatitude(latRaw));
  }

  @Override
  protected double quantizeLon(double lonRaw) {
    return LatLonPoint.decodeLongitude(LatLonPoint.encodeLongitude(lonRaw));
  }
}
