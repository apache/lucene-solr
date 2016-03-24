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
package org.apache.lucene.spatial.geopoint.search;

import org.apache.lucene.document.Document;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.util.GeoEncodingUtils;
import org.apache.lucene.spatial.geopoint.document.GeoPointField;
import org.apache.lucene.spatial.geopoint.document.GeoPointField.TermEncoding;
import org.apache.lucene.spatial.util.BaseGeoPointTestCase;

/**
 * random testing for GeoPoint query logic (with deprecated numeric encoding)
 * @deprecated remove this when TermEncoding.NUMERIC is removed
 */
@Deprecated
public class TestLegacyGeoPointQuery extends BaseGeoPointTestCase {
  
  @Override
  protected double quantizeLat(double lat) {
    return GeoEncodingUtils.mortonUnhashLat(GeoEncodingUtils.mortonHash(lat, 0));
  }
  
  @Override
  protected double quantizeLon(double lon) {
    return GeoEncodingUtils.mortonUnhashLon(GeoEncodingUtils.mortonHash(0, lon));
  }

  @Override
  protected void addPointToDoc(String field, Document doc, double lat, double lon) {
    doc.add(new GeoPointField(field, lat, lon, GeoPointField.NUMERIC_TYPE_NOT_STORED));
  }

  @Override
  protected Query newRectQuery(String field, double minLat, double maxLat, double minLon, double maxLon) {
    return new GeoPointInBBoxQuery(field, TermEncoding.NUMERIC, minLat, maxLat, minLon, maxLon);
  }

  @Override
  protected Query newDistanceQuery(String field, double centerLat, double centerLon, double radiusMeters) {
    return new GeoPointDistanceQuery(field, TermEncoding.NUMERIC, centerLat, centerLon, radiusMeters);
  }

  @Override
  protected Query newDistanceRangeQuery(String field, double centerLat, double centerLon, double minRadiusMeters, double radiusMeters) {
    // LUCENE-7126: currently not valid for multi-valued documents, because it rewrites to a BooleanQuery!
    // return new GeoPointDistanceRangeQuery(field, TermEncoding.NUMERIC, centerLat, centerLon, minRadiusMeters, radiusMeters);
    return null;
  }

  @Override
  protected Query newPolygonQuery(String field, double[] lats, double[] lons) {
    return new GeoPointInPolygonQuery(field, TermEncoding.NUMERIC, lats, lons);
  }

  // legacy encoding is too slow somehow for this random test, its not up to the task.
  @Override
  public void testRandomDistance() throws Exception {
    assumeTrue("legacy encoding is too slow/hangs on this test", false);
  }

  @Override
  public void testRandomDistanceHuge() throws Exception {
    assumeTrue("legacy encoding is too slow/hangs on this test", false);
  }
}
