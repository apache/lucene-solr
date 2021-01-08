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

import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.document.ShapeField;
import org.apache.lucene.geo.BaseGeoPointTestCase;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.bkd.BKDConfig;

public class TestLatLonPointQueries extends BaseGeoPointTestCase {

  @Override
  protected void addPointToDoc(String field, Document doc, double lat, double lon) {
    doc.add(new LatLonPoint(field, lat, lon));
  }

  @Override
  protected Query newRectQuery(String field, double minLat, double maxLat, double minLon, double maxLon) {
    return LatLonPoint.newBoxQuery(field, minLat, maxLat, minLon, maxLon);
  }

  @Override
  protected Query newDistanceQuery(String field, double centerLat, double centerLon, double radiusMeters) {
    return LatLonPoint.newDistanceQuery(field, centerLat, centerLon, radiusMeters);
  }

  @Override
  protected Query newPolygonQuery(String field, Polygon... polygons) {
    return LatLonPoint.newPolygonQuery(field, polygons);
  }

  @Override
  protected Query newGeometryQuery(String field, LatLonGeometry... geometry) {
    return LatLonPoint.newGeometryQuery(field, ShapeField.QueryRelation.INTERSECTS, geometry);
  }

  @Override
  protected double quantizeLat(double latRaw) {
    return GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(latRaw));
  }

  @Override
  protected double quantizeLon(double lonRaw) {
    return GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(lonRaw));
  }

  public void testDistanceQueryWithInvertedIntersection() throws IOException {
    final int numMatchingDocs = atLeast(10 * BKDConfig.DEFAULT_MAX_POINTS_IN_LEAF_NODE);

    try (Directory dir = newDirectory()) {

      try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
        for (int i = 0; i < numMatchingDocs; ++i) {
          Document doc = new Document();
          addPointToDoc("field", doc, 18.313694, -65.227444);
          w.addDocument(doc);
        }

        // Add a handful of docs that don't match
        for (int i = 0; i < 11; ++i) {
          Document doc = new Document();
          addPointToDoc("field", doc, 10, -65.227444);
          w.addDocument(doc);
        }
        w.forceMerge(1);
      }

      try (IndexReader r = DirectoryReader.open(dir)) {
        IndexSearcher searcher = newSearcher(r);
        assertEquals(numMatchingDocs, searcher.count(newDistanceQuery("field", 18, -65, 50_000)));
      }
    }
  }
}
