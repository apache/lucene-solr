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
package org.apache.lucene.bkdtree;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.lucene54.Lucene54Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.SloppyMath;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.spatial.util.BaseGeoPointTestCase;
import org.apache.lucene.spatial.util.GeoRect;

// TODO: can test framework assert we don't leak temp files?

public class TestBKDTree extends BaseGeoPointTestCase {
  // todo deconflict GeoPoint and BKD encoding methods and error tolerance
  public static final double BKD_TOLERANCE = 1e-7;

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
    // return new BKDDistanceQuery(field, centerLat, centerLon, radiusMeters);
    return null;
  }

  @Override
  protected Query newDistanceRangeQuery(String field, double centerLat, double centerLon, double minRadiusMeters, double radiusMeters) {
    return null;
  }

  @Override
  protected Query newPolygonQuery(String field, double[] lats, double[] lons) {
    return new BKDPointInPolygonQuery(FIELD_NAME, lats, lons);
  }

  @Override
  protected void initIndexWriterConfig(final String fieldName, IndexWriterConfig iwc) {
    final DocValuesFormat dvFormat = getDocValuesFormat();
    Codec codec = new Lucene54Codec() {
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

    // false positive/negatives due to quantization error exist for both rectangles and polygons
    if (compare(pointLat, rect.minLat) == 0
        || compare(pointLat, rect.maxLat) == 0
        || compare(pointLon, rect.minLon) == 0
        || compare(pointLon, rect.maxLon) == 0) {
      return null;
    }

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

  // todo reconcile with GeoUtils (see LUCENE-6996)
  public static double compare(final double v1, final double v2) {
    final double delta = v1-v2;
    return Math.abs(delta) <= BKD_TOLERANCE ? 0 : delta;
  }

  @Override
  protected Boolean polyRectContainsPoint(GeoRect rect, double pointLat, double pointLon) {
    // TODO write better random polygon tests
    return rectContainsPoint(rect, pointLat, pointLon);
  }

  @Override
  protected Boolean circleContainsPoint(double centerLat, double centerLon, double radiusMeters, double pointLat, double pointLon) {
    double distanceKM = SloppyMath.haversin(centerLat, centerLon, pointLat, pointLon);
    boolean result = distanceKM*1000.0 <= radiusMeters;
    //System.out.println("  shouldMatch?  centerLon=" + centerLon + " centerLat=" + centerLat + " pointLon=" + pointLon + " pointLat=" + pointLat + " result=" + result + " distanceMeters=" + (distanceKM * 1000));
    return result;
  }

  @Override
  protected Boolean distanceRangeContainsPoint(double centerLat, double centerLon, double minRadiusMeters, double radiusMeters, double pointLat, double pointLon) {
    final double d = SloppyMath.haversin(centerLat, centerLon, pointLat, pointLon)*1000.0;
    return d >= minRadiusMeters && d <= radiusMeters;
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
    Directory dir = getDirectory();
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

  private static Directory noVirusChecker(Directory dir) {
    if (dir instanceof MockDirectoryWrapper) {
      ((MockDirectoryWrapper) dir).setEnableVirusScanner(false);
    }
    return dir;
  }

  private static Directory getDirectory() {
    return noVirusChecker(newDirectory());
  }
}
