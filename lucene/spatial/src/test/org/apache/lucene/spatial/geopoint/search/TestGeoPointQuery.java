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

import java.util.Arrays;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.spatial.geopoint.document.GeoPointField.TermEncoding;
import org.apache.lucene.spatial.geopoint.document.GeoPointField;
import org.apache.lucene.spatial.util.BaseGeoPointTestCase;
import org.apache.lucene.spatial.util.GeoEncodingUtils;
import org.apache.lucene.spatial.util.GeoRect;
import org.apache.lucene.spatial.util.GeoRelationUtils;
import org.apache.lucene.spatial.util.GeoUtils;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.SloppyMath;
import org.apache.lucene.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import static org.apache.lucene.spatial.util.GeoDistanceUtils.DISTANCE_PCT_ERR;

/**
 * Unit testing for basic GeoPoint query logic
 *
 * @lucene.experimental
 */

public class TestGeoPointQuery extends BaseGeoPointTestCase {

  private static Directory directory = null;
  private static IndexReader reader = null;
  private static IndexSearcher searcher = null;
  private static TermEncoding termEncoding = null;
  private static FieldType fieldType = null;

  @Override
  protected boolean forceSmall() {
    return false;
  }

  @Override
  protected void addPointToDoc(String field, Document doc, double lat, double lon) {
    doc.add(new GeoPointField(field, lat, lon, fieldType));
  }

  @Override
  protected Query newRectQuery(String field, GeoRect rect) {
    return new GeoPointInBBoxQuery(field, termEncoding, rect.minLat, rect.maxLat, rect.minLon, rect.maxLon);
  }

  @Override
  protected Query newDistanceQuery(String field, double centerLat, double centerLon, double radiusMeters) {
    return new GeoPointDistanceQuery(field, termEncoding, centerLat, centerLon, radiusMeters);
  }

  @Override
  protected Query newPolygonQuery(String field, double[] lats, double[] lons) {
    return new GeoPointInPolygonQuery(field, termEncoding, lats, lons);
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    directory = newDirectory();
    termEncoding = randomTermEncoding();
    fieldType = randomFieldType();

    RandomIndexWriter writer = new RandomIndexWriter(random(), directory,
            newIndexWriterConfig(new MockAnalyzer(random()))
                    .setMaxBufferedDocs(TestUtil.nextInt(random(), 100, 1000))
                    .setMergePolicy(newLogMergePolicy()));

    // this is a simple systematic test
    GeoPointField[] pts = new GeoPointField[] {
        new GeoPointField(FIELD_NAME, 32.763420, -96.774, fieldType),
        new GeoPointField(FIELD_NAME, 32.7559529921407, -96.7759895324707, fieldType),
        new GeoPointField(FIELD_NAME, 32.77866942010977, -96.77701950073242, fieldType),
        new GeoPointField(FIELD_NAME, 32.7756745755423, -96.7706036567688, fieldType),
        new GeoPointField(FIELD_NAME, 27.703618681345585, -139.73458170890808, fieldType),
        new GeoPointField(FIELD_NAME, 32.94823588839368, -96.4538113027811, fieldType),
        new GeoPointField(FIELD_NAME, 33.06047141970814, -96.65084838867188, fieldType),
        new GeoPointField(FIELD_NAME, 32.778650, -96.7772, fieldType),
        new GeoPointField(FIELD_NAME, -88.56029371730983, -177.23537676036358, fieldType),
        new GeoPointField(FIELD_NAME, 33.541429799076354, -26.779373834241003, fieldType),
        new GeoPointField(FIELD_NAME, 26.774024500421728, -77.35379276106497, fieldType),
        new GeoPointField(FIELD_NAME, -90.0, -14.796283808944777, fieldType),
        new GeoPointField(FIELD_NAME, 32.94823588839368, -178.8538113027811, fieldType),
        new GeoPointField(FIELD_NAME, 32.94823588839368, 178.8538113027811, fieldType),
        new GeoPointField(FIELD_NAME, 40.720611, -73.998776, fieldType),
        new GeoPointField(FIELD_NAME, -44.5, -179.5, fieldType)};

    for (GeoPointField p : pts) {
        Document doc = new Document();
        doc.add(p);
        writer.addDocument(doc);
    }

    // add explicit multi-valued docs
    for (int i=0; i<pts.length; i+=2) {
      Document doc = new Document();
      doc.add(pts[i]);
      doc.add(pts[i+1]);
      writer.addDocument(doc);
    }

    // index random string documents
    for (int i=0; i<random().nextInt(10); ++i) {
      Document doc = new Document();
      doc.add(new StringField("string", Integer.toString(i), Field.Store.NO));
      writer.addDocument(doc);
    }

    reader = writer.getReader();
    searcher = newSearcher(reader);
    writer.close();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    searcher = null;
    reader.close();
    reader = null;
    directory.close();
    directory = null;
  }

  private static TermEncoding randomTermEncoding() {
    return random().nextBoolean() ? TermEncoding.NUMERIC : TermEncoding.PREFIX;
  }

  private static FieldType randomFieldType() {
    if (termEncoding == TermEncoding.PREFIX) {
      return GeoPointField.PREFIX_TYPE_NOT_STORED;
    }
    return GeoPointField.NUMERIC_TYPE_NOT_STORED;
  }

  private TopDocs bboxQuery(double minLat, double maxLat, double minLon, double maxLon, int limit) throws Exception {
    GeoPointInBBoxQuery q = new GeoPointInBBoxQuery(FIELD_NAME, termEncoding, minLat, maxLat, minLon, maxLon);
    return searcher.search(q, limit);
  }

  private TopDocs polygonQuery(double[] polyLats, double[] polyLons, int limit) throws Exception {
    GeoPointInPolygonQuery q = new GeoPointInPolygonQuery(FIELD_NAME, termEncoding, polyLats, polyLons);
    return searcher.search(q, limit);
  }

  private TopDocs geoDistanceQuery(double lat, double lon, double radius, int limit) throws Exception {
    GeoPointDistanceQuery q = new GeoPointDistanceQuery(FIELD_NAME, termEncoding, lat, lon, radius);
    return searcher.search(q, limit);
  }

  @Override
  protected Boolean rectContainsPoint(GeoRect rect, double pointLat, double pointLon) {
    if (GeoEncodingUtils.compare(pointLon, rect.minLon) == 0.0 ||
        GeoEncodingUtils.compare(pointLon, rect.maxLon) == 0.0 ||
        GeoEncodingUtils.compare(pointLat, rect.minLat) == 0.0 ||
        GeoEncodingUtils.compare(pointLat, rect.maxLat) == 0.0) {
      // Point is very close to rect boundary
      return null;
    }

    if (rect.minLon < rect.maxLon) {
      return GeoRelationUtils.pointInRectPrecise(pointLat, pointLon, rect.minLat, rect.maxLat, rect.minLon, rect.maxLon);
    } else {
      // Rect crosses dateline:
      return GeoRelationUtils.pointInRectPrecise(pointLat, pointLon, rect.minLat, rect.maxLat, -180.0, rect.maxLon)
        || GeoRelationUtils.pointInRectPrecise(pointLat, pointLon, rect.minLat, rect.maxLat, rect.minLon, 180.0);
    }
  }

  @Override
  protected Boolean polyRectContainsPoint(GeoRect rect, double pointLat, double pointLon) {
    return rectContainsPoint(rect, pointLat, pointLon);
  }

  @Override
  protected Boolean circleContainsPoint(double centerLat, double centerLon, double radiusMeters, double pointLat, double pointLon) {
    if (radiusQueryCanBeWrong(centerLat, centerLon, pointLon, pointLat, radiusMeters)) {
      return null;
    } else {
      return SloppyMath.haversinMeters(centerLat, centerLon, pointLat, pointLon) <= radiusMeters;
    }
  }

  private static boolean radiusQueryCanBeWrong(double centerLat, double centerLon, double ptLon, double ptLat,
                                               final double radius) {
    final long hashedCntr = GeoEncodingUtils.mortonHash(centerLat, centerLon);
    centerLon = GeoEncodingUtils.mortonUnhashLon(hashedCntr);
    centerLat = GeoEncodingUtils.mortonUnhashLat(hashedCntr);
    final long hashedPt = GeoEncodingUtils.mortonHash(ptLat, ptLon);
    ptLon = GeoEncodingUtils.mortonUnhashLon(hashedPt);
    ptLat = GeoEncodingUtils.mortonUnhashLat(hashedPt);

    double ptDistance = SloppyMath.haversinMeters(centerLat, centerLon, ptLat, ptLon);
    double delta = StrictMath.abs(ptDistance - radius);

    // if its within the distance error then it can be wrong
    return delta < (ptDistance*DISTANCE_PCT_ERR);
  }

  public void testRectCrossesCircle() throws Exception {
    assertTrue(GeoRelationUtils.rectCrossesCircle(-90, 0.0, -180, 180, 0.0, 0.667, 88000.0, false));
  }

  public void testBBoxQuery() throws Exception {
    TopDocs td = bboxQuery(32.778650, 32.778950, -96.7772, -96.77690000, 5);
    assertEquals("GeoBoundingBoxQuery failed", 4, td.totalHits);
  }

  public void testPolyQuery() throws Exception {
    TopDocs td = polygonQuery(
        new double[]{33.073130, 32.9942669, 32.938386, 33.0374494,
            33.1369762, 33.1162747, 33.073130, 33.073130},
        new double[]{-96.7682647, -96.8280029, -96.6288757, -96.4929199,
                     -96.6041564, -96.7449188, -96.76826477, -96.7682647},
        5);
    assertEquals("GeoPolygonQuery failed", 2, td.totalHits);
  }

  public void testPacManPolyQuery() throws Exception {
    // pacman
    double[] px = {0, 10, 10, 0, -8, -10, -8, 0, 10, 10, 0};
    double[] py = {0, 5, 9, 10, 9, 0, -9, -10, -9, -5, 0};

    // shape bbox
    double xMinA = -10;
    double xMaxA = 10;
    double yMinA = -10;
    double yMaxA = 10;

    // candidate crosses cell
    double xMin = 2;//-5;
    double xMax = 11;//0.000001;
    double yMin = -1;//0;
    double yMax = 1;//5;

    // test cell crossing poly
    assertTrue(GeoRelationUtils.rectCrossesPolyApprox(yMin, yMax, xMin, yMax, py, px, yMinA, yMaxA, xMinA, xMaxA));
    assertFalse(GeoRelationUtils.rectCrossesPolyApprox(0, 5, -5, 0.000001, py, px, yMin, yMax, xMin, xMax));
    assertTrue(GeoRelationUtils.rectWithinPolyApprox(0, 5, -5, -2, py, px, yMin, yMax, xMin, xMax));
  }

  public void testBBoxCrossDateline() throws Exception {
    TopDocs td = bboxQuery(-45.0, -44.0, 179.0, -179.0, 20);
    assertEquals("BBoxCrossDateline query failed", 2, td.totalHits);
  }

  public void testWholeMap() throws Exception {
    TopDocs td = bboxQuery(GeoUtils.MIN_LAT_INCL, GeoUtils.MAX_LAT_INCL, GeoUtils.MIN_LON_INCL, GeoUtils.MAX_LON_INCL, 20);
    assertEquals("testWholeMap failed", 24, td.totalHits);
    td = polygonQuery(new double[] {GeoUtils.MIN_LAT_INCL, GeoUtils.MAX_LAT_INCL, GeoUtils.MAX_LAT_INCL, GeoUtils.MIN_LAT_INCL, GeoUtils.MIN_LAT_INCL},
                       new double[] {GeoUtils.MIN_LON_INCL, GeoUtils.MIN_LON_INCL, GeoUtils.MAX_LON_INCL, GeoUtils.MAX_LON_INCL, GeoUtils.MIN_LON_INCL},
                       20);
    assertEquals("testWholeMap failed", 24, td.totalHits);
  }

  public void smallTest() throws Exception {
    TopDocs td = geoDistanceQuery(40.720611, -73.998776, 1, 20);
    assertEquals("smallTest failed", 2, td.totalHits);
  }

  // GeoBoundingBox should not accept invalid lat/lon
  public void testInvalidBBox() throws Exception {
    expectThrows(Exception.class, () -> {
      bboxQuery(-92.0, -91.0, 179.0, 181.0, 20);
    });
  }

  public void testGeoDistanceQuery() throws Exception {
    TopDocs td = geoDistanceQuery(32.94823588839368, -96.4538113027811, 6000, 20);
    assertEquals("GeoDistanceQuery failed", 2, td.totalHits);
  }

  /** see https://issues.apache.org/jira/browse/LUCENE-6905 */
  public void testNonEmptyTermsEnum() throws Exception {
    TopDocs td = geoDistanceQuery(-88.56029371730983, -177.23537676036358, 7757.999232959935, 20);
    assertEquals("GeoDistanceQuery failed", 2, td.totalHits);
  }

  public void testMultiValuedQuery() throws Exception {
    TopDocs td = bboxQuery(32.7559529921407, 32.7756745755423, -96.4538113027811, -96.7706036567688, 20);
    // 3 single valued docs + 2 multi-valued docs
    assertEquals("testMultiValuedQuery failed", 5, td.totalHits);
  }

  public void testTooBigRadius() throws Exception {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      geoDistanceQuery(85.0, 0.0, 4000000, 20);
    });
    assertTrue(expected.getMessage().contains("exceeds maxRadius"));
  }

  /**
   * Explicitly large
   */
  public void testGeoDistanceQueryHuge() throws Exception {
    TopDocs td = geoDistanceQuery(32.94823588839368, -96.4538113027811, 6000000, 20);
    assertEquals("GeoDistanceQuery failed", 16, td.totalHits);
  }

  public void testGeoDistanceQueryCrossDateline() throws Exception {
    TopDocs td = geoDistanceQuery(32.94823588839368, -179.9538113027811, 120000, 20);
    assertEquals("GeoDistanceQuery failed", 3, td.totalHits);
  }

  // GeoDistanceQuery should not accept invalid lat/lon as origin
  public void testInvalidGeoDistanceQuery() throws Exception {
    expectThrows(Exception.class, () -> {
      geoDistanceQuery(92.0, 181.0, 120000, 20);
    });
  }

  public void testMortonEncoding() throws Exception {
    long hash = GeoEncodingUtils.mortonHash(90, 180);
    assertEquals(180.0, GeoEncodingUtils.mortonUnhashLon(hash), 0);
    assertEquals(90.0, GeoEncodingUtils.mortonUnhashLat(hash), 0);
  }

  public void testEncodeDecode() throws Exception {
    int iters = atLeast(10000);
    boolean small = random().nextBoolean();
    for(int iter=0;iter<iters;iter++) {
      double lat = randomLat(small);
      double lon = randomLon(small);

      long enc = GeoEncodingUtils.mortonHash(lat, lon);
      double latEnc = GeoEncodingUtils.mortonUnhashLat(enc);
      double lonEnc = GeoEncodingUtils.mortonUnhashLon(enc);

      assertEquals("lat=" + lat + " latEnc=" + latEnc + " diff=" + (lat - latEnc), lat, latEnc, GeoEncodingUtils.TOLERANCE);
      assertEquals("lon=" + lon + " lonEnc=" + lonEnc + " diff=" + (lon - lonEnc), lon, lonEnc, GeoEncodingUtils.TOLERANCE);
    }
  }

  public void testScaleUnscaleIsStable() throws Exception {
    int iters = atLeast(1000);
    boolean small = random().nextBoolean();
    for(int iter=0;iter<iters;iter++) {
      double lat = randomLat(small);
      double lon = randomLon(small);

      long enc = GeoEncodingUtils.mortonHash(lat, lon);
      double latEnc = GeoEncodingUtils.mortonUnhashLat(enc);
      double lonEnc = GeoEncodingUtils.mortonUnhashLon(enc);

      long enc2 = GeoEncodingUtils.mortonHash(lat, lon);
      double latEnc2 = GeoEncodingUtils.mortonUnhashLat(enc2);
      double lonEnc2 = GeoEncodingUtils.mortonUnhashLon(enc2);
      assertEquals(latEnc, latEnc2, 0.0);
      assertEquals(lonEnc, lonEnc2, 0.0);
    }
  }

  public void testInvalidLatLon() throws Exception {
    IllegalArgumentException e;
    e= expectThrows(IllegalArgumentException.class,
                    () -> {
                      new GeoPointField("field", 180.0, 0.0, Field.Store.NO);
                    });
    assertEquals("invalid latitude 180.0; must be between -90.0 and 90.0", e.getMessage());

    e = expectThrows(IllegalArgumentException.class,
                     () -> {
                       new GeoPointField("field", 0.0, 190.0, Field.Store.NO);
                     });
    assertEquals("invalid longitude 190.0; must be between -180.0 and 180.0", e.getMessage());
  }
}
