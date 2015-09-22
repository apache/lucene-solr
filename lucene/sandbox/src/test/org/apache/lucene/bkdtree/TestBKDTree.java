package org.apache.lucene.bkdtree;

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

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.lucene53.Lucene53Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.BaseGeoPointTestCase;
import org.apache.lucene.util.GeoRect;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.SloppyMath;
import org.apache.lucene.util.TestUtil;

// TODO: can test framework assert we don't leak temp files?

public class TestBKDTree extends BaseGeoPointTestCase {

  @Override
  protected void addPointToDoc(String field, Document doc, double lat, double lon) {
    doc.add(new BKDPointField(field, lat, lon));
  }

  @Override
  protected Query newBBoxQuery(String field, GeoRect rect) {
    return new BKDPointInBBoxQuery(field, rect.minLat, rect.maxLat, rect.minLon, rect.maxLon);
  }

  @Override
  protected Query newDistanceQuery(String field, double centerLat, double centerLon, double radiusMeters) {
    return new BKDDistanceQuery(field, centerLat, centerLon, radiusMeters);
  }

  @Override
  protected Query newPolygonQuery(String field, double[] lats, double[] lons) {
    return new BKDPointInPolygonQuery(FIELD_NAME, lats, lons);
  }

  @Override
  protected void initIndexWriterConfig(final String fieldName, IndexWriterConfig iwc) {
    final DocValuesFormat dvFormat = getDocValuesFormat();
    Codec codec = new Lucene53Codec() {
        @Override
        public DocValuesFormat getDocValuesFormatForField(String field) {
          if (field.equals(fieldName)) {
            return dvFormat;
          } else {
            return super.getDocValuesFormatForField(field);
          }
        }
      };
    iwc.setCodec(codec);
  }
  
  @Override
  protected Boolean rectContainsPoint(GeoRect rect, double pointLat, double pointLon) {

    assert Double.isNaN(pointLat) == false;

    int rectLatMinEnc = BKDTreeWriter.encodeLat(rect.minLat);
    int rectLatMaxEnc = BKDTreeWriter.encodeLat(rect.maxLat);
    int rectLonMinEnc = BKDTreeWriter.encodeLon(rect.minLon);
    int rectLonMaxEnc = BKDTreeWriter.encodeLon(rect.maxLon);

    int pointLatEnc = BKDTreeWriter.encodeLat(pointLat);
    int pointLonEnc = BKDTreeWriter.encodeLon(pointLon);

    if (rect.minLon < rect.maxLon) {
      return pointLatEnc >= rectLatMinEnc &&
        pointLatEnc < rectLatMaxEnc &&
        pointLonEnc >= rectLonMinEnc &&
        pointLonEnc < rectLonMaxEnc;
    } else {
      // Rect crosses dateline:
      return pointLatEnc >= rectLatMinEnc &&
        pointLatEnc < rectLatMaxEnc &&
        (pointLonEnc >= rectLonMinEnc ||
         pointLonEnc < rectLonMaxEnc);
    }
  }

  private static final double POLY_TOLERANCE = 1e-7;

  @Override
  protected Boolean polyRectContainsPoint(GeoRect rect, double pointLat, double pointLon) {
    if (Math.abs(rect.minLat-pointLat) < POLY_TOLERANCE ||
        Math.abs(rect.maxLat-pointLat) < POLY_TOLERANCE ||
        Math.abs(rect.minLon-pointLon) < POLY_TOLERANCE ||
        Math.abs(rect.maxLon-pointLon) < POLY_TOLERANCE) {
      // The poly check quantizes slightly differently, so we allow for boundary cases to disagree
      return null;
    } else {
      return rectContainsPoint(rect, pointLat, pointLon);
    }
  }

  @Override
  protected Boolean circleContainsPoint(double centerLat, double centerLon, double radiusMeters, double pointLat, double pointLon) {
    double distanceKM = SloppyMath.haversin(centerLat, centerLon, pointLat, pointLon);
    boolean result = distanceKM*1000.0 <= radiusMeters;
    //System.out.println("  shouldMatch?  centerLon=" + centerLon + " centerLat=" + centerLat + " pointLon=" + pointLon + " pointLat=" + pointLat + " result=" + result + " distanceMeters=" + (distanceKM * 1000));
    return result;
  }

  public void testEncodeDecode() throws Exception {
    int iters = atLeast(10000);
    boolean small = random().nextBoolean();
    for(int iter=0;iter<iters;iter++) {
      double lat = randomLat(small);
      double latQuantized = BKDTreeWriter.decodeLat(BKDTreeWriter.encodeLat(lat));
      assertEquals(lat, latQuantized, BKDTreeWriter.TOLERANCE);

      double lon = randomLon(small);
      double lonQuantized = BKDTreeWriter.decodeLon(BKDTreeWriter.encodeLon(lon));
      assertEquals(lon, lonQuantized, BKDTreeWriter.TOLERANCE);
    }
  }

  public void testEncodeDecodeMax() throws Exception {
    int x = BKDTreeWriter.encodeLat(Math.nextAfter(90.0, Double.POSITIVE_INFINITY));
    assertTrue(x < Integer.MAX_VALUE);

    int y = BKDTreeWriter.encodeLon(Math.nextAfter(180.0, Double.POSITIVE_INFINITY));
    assertTrue(y < Integer.MAX_VALUE);
  }

  public void testAccountableHasDelegate() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setCodec(TestUtil.alwaysDocValuesFormat(getDocValuesFormat()));
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
    Document doc = new Document();
    doc.add(new BKDPointField(FIELD_NAME, -18.2861, 147.7));
    w.addDocument(doc);
    IndexReader r = w.getReader();

    // We can't wrap with "exotic" readers because the BKD query must see the BKDDVFormat:
    IndexSearcher s = newSearcher(r, false);
    // Need to run a query so the DV field is really loaded:
    TopDocs hits = s.search(new BKDPointInBBoxQuery(FIELD_NAME, -30, 0, 140, 150), 1);
    assertEquals(1, hits.totalHits);
    assertTrue(Accountables.toString((Accountable) r.leaves().get(0).reader()).contains("delegate"));
    IOUtils.close(r, w, dir);
  }

  private static DocValuesFormat getDocValuesFormat() {
    int maxPointsInLeaf = TestUtil.nextInt(random(), 16, 2048);
    int maxPointsSortInHeap = TestUtil.nextInt(random(), maxPointsInLeaf, 1024*1024);
    if (VERBOSE) {
      System.out.println("  BKD params: maxPointsInLeaf=" + maxPointsInLeaf + " maxPointsSortInHeap=" + maxPointsSortInHeap);
    }
    return new BKDTreeDocValuesFormat(maxPointsInLeaf, maxPointsSortInHeap);
  }
}
