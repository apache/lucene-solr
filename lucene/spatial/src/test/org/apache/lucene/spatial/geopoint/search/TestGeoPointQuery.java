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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.geo.BaseGeoPointTestCase;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.spatial.geopoint.document.GeoPointField;
import org.apache.lucene.spatial.geopoint.document.GeoPointField.TermEncoding;
import org.apache.lucene.store.Directory;

/**
 * random testing for GeoPoint query logic
 *
 * @lucene.experimental
 */
public class TestGeoPointQuery extends BaseGeoPointTestCase {
  
  @Override
  protected double quantizeLat(double lat) {
    return GeoPointField.decodeLatitude(GeoPointField.encodeLatLon(lat, 0));
  }
  
  @Override
  protected double quantizeLon(double lon) {
    return GeoPointField.decodeLongitude(GeoPointField.encodeLatLon(0, lon));
  }

  @Override
  protected void addPointToDoc(String field, Document doc, double lat, double lon) {
    doc.add(new GeoPointField(field, lat, lon, GeoPointField.PREFIX_TYPE_NOT_STORED));
  }

  @Override
  protected Query newRectQuery(String field, double minLat, double maxLat, double minLon, double maxLon) {
    return new GeoPointInBBoxQuery(field, TermEncoding.PREFIX, minLat, maxLat, minLon, maxLon);
  }

  @Override
  protected Query newDistanceQuery(String field, double centerLat, double centerLon, double radiusMeters) {
    return new GeoPointDistanceQuery(field, TermEncoding.PREFIX, centerLat, centerLon, radiusMeters);
  }

  @Override
  protected Query newPolygonQuery(String field, Polygon... polygons) {
    return new GeoPointInPolygonQuery(field, TermEncoding.PREFIX, polygons);
  }

  /** explicit test failure for LUCENE-7325 */
  public void testInvalidShift() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    // add a doc with a point
    Document document = new Document();
    addPointToDoc("field", document, 80, -65);
    writer.addDocument(document);

    // search and verify we found our doc
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);
    assertEquals(0, searcher.count(newRectQuery("field", 90, 90, -180, 0)));

    reader.close();
    writer.close();
    dir.close();
  }
}
