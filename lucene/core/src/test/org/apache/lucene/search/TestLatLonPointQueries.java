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
import org.apache.lucene.geo.BaseGeoPointTestCase;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.geo.GeoTestUtil;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.bkd.BKDWriter;

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
  protected double quantizeLat(double latRaw) {
    return GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(latRaw));
  }

  @Override
  protected double quantizeLon(double lonRaw) {
    return GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(lonRaw));
  }

  public void testDistanceQueryWithInvertedIntersection() throws IOException {
    final int numMatchingDocs = atLeast(10 * BKDWriter.DEFAULT_MAX_POINTS_IN_LEAF_NODE);

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

  public void testPointInPositivePole() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    // document in positive pole
    Document doc = new Document();
    LatLonPoint latLonPoint = new LatLonPoint("test", 90, GeoTestUtil.nextLongitude());
    doc.add(latLonPoint);
    w.addDocument(doc);
    // document in close to the pole with same encoding value
    doc = new Document();
    latLonPoint = new LatLonPoint("test", 89.99999995809049, GeoTestUtil.nextLongitude());
    doc.add(latLonPoint);
    w.addDocument(doc);

    ///// search //////
    IndexReader reader = w.getReader();
    w.close();
    IndexSearcher searcher = newSearcher(reader);

    // both documents should match
    Rectangle rectangle = new Rectangle(90, 90, -180, 180);
    Query q2 = LatLonPoint.newBoxQuery("test", rectangle.minLat, rectangle.maxLat, rectangle.minLon, rectangle.maxLon);
    assertEquals(2, searcher.count(q2));

    IOUtils.close(w, reader, dir);
  }

  public void testPointInNegativePole() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    // document in negative pole
    Document doc = new Document();
    LatLonPoint latLonPoint = new LatLonPoint("test", -90, GeoTestUtil.nextLongitude());
    doc.add(latLonPoint);
    w.addDocument(doc);
    // document in close to the pole with same encoding value
    doc = new Document();
    latLonPoint = new LatLonPoint("test", -89.99999995809051, GeoTestUtil.nextLongitude());
    doc.add(latLonPoint);
    w.addDocument(doc);

    ///// search //////
    IndexReader reader = w.getReader();
    w.close();
    IndexSearcher searcher = newSearcher(reader);

    // both documents should match
    Rectangle rectangle = new Rectangle(-90, -90, -180, 180);
    Query q2 = LatLonPoint.newBoxQuery("test", rectangle.minLat, rectangle.maxLat, rectangle.minLon, rectangle.maxLon);
    assertEquals(2, searcher.count(q2));

    IOUtils.close(w, reader, dir);
  }

  public void testPointInDateLine() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    // Point on the positive dateline
    Document doc = new Document();
    LatLonPoint latLonPoint = new LatLonPoint("test", GeoTestUtil.nextLatitude(), 180);
    doc.add(latLonPoint);
    w.addDocument(doc);
    // Point on the negative dateline
    doc = new Document();
    latLonPoint = new LatLonPoint("test", GeoTestUtil.nextLatitude(), -180);
    doc.add(latLonPoint);
    w.addDocument(doc);
    // Point at (0,0)
    doc = new Document();
    latLonPoint = new LatLonPoint("test", 0, 0);
    doc.add(latLonPoint);
    w.addDocument(doc);

    ///// search //////
    IndexReader reader = w.getReader();
    w.close();
    IndexSearcher searcher = newSearcher(reader);

    // if minLon = -180 then 180 is not included
    Rectangle rectangle = new Rectangle(-90, 90, -180, -170);
    Query q = LatLonPoint.newBoxQuery("test", rectangle.minLat, rectangle.maxLat, rectangle.minLon, rectangle.maxLon);
    assertEquals(1, searcher.count(q));
    // if maxLon = 180 then -180 is not included
    rectangle = new Rectangle(-90, 90, 170, 180);
    q = LatLonPoint.newBoxQuery("test", rectangle.minLat, rectangle.maxLat, rectangle.minLon, rectangle.maxLon);
    assertEquals(1, searcher.count(q));
    // if minLon = 180 then -180 is included
    rectangle = new Rectangle(-90, 90, 180, -170);
    q = LatLonPoint.newBoxQuery("test", rectangle.minLat, rectangle.maxLat, rectangle.minLon, rectangle.maxLon);
    assertEquals(2, searcher.count(q));
    // if maxLon = -180 then 180 is included
    rectangle = new Rectangle(-90, 90, 170, -180);
    q = LatLonPoint.newBoxQuery("test", rectangle.minLat, rectangle.maxLat, rectangle.minLon, rectangle.maxLon);
    assertEquals(2, searcher.count(q));
    // if maxLon < minLon but they encode to the same value, all longitude values are included
    rectangle = new Rectangle(-90, 90, 1e-10, 0);
    q = LatLonPoint.newBoxQuery("test", rectangle.minLat, rectangle.maxLat, rectangle.minLon, rectangle.maxLon);
    assertEquals(3, searcher.count(q));

    IOUtils.close(w, reader, dir);
  }
}
