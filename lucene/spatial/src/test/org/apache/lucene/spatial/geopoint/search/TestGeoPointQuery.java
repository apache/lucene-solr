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
import org.apache.lucene.store.Directory;
import org.apache.lucene.spatial.util.GeoEncodingUtils;
import org.apache.lucene.spatial.geopoint.document.GeoPointField;
import org.apache.lucene.spatial.geopoint.document.GeoPointField.TermEncoding;
import org.apache.lucene.spatial.util.BaseGeoPointTestCase;
import org.apache.lucene.spatial.util.GeoRect;
import org.apache.lucene.spatial.util.GeoRelationUtils;
import org.apache.lucene.spatial.util.GeoUtils;
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
  protected void addPointToDoc(String field, Document doc, double lat, double lon) {
    doc.add(new GeoPointField(field, lon, lat, fieldType));
  }

  @Override
  protected Query newBBoxQuery(String field, GeoRect rect) {
    return new GeoPointInBBoxQuery(field, termEncoding, rect.minLon, rect.minLat, rect.maxLon, rect.maxLat);
  }

  @Override
  protected Query newDistanceQuery(String field, double centerLat, double centerLon, double radiusMeters) {
    return new GeoPointDistanceQuery(field, termEncoding, centerLon, centerLat, radiusMeters);
  }

  @Override
  protected Query newDistanceRangeQuery(String field, double centerLat, double centerLon, double minRadiusMeters, double radiusMeters) {
    return new GeoPointDistanceRangeQuery(field, termEncoding, centerLon, centerLat, minRadiusMeters, radiusMeters);
  }

  @Override
  protected Query newPolygonQuery(String field, double[] lats, double[] lons) {
    return new GeoPointInPolygonQuery(field, termEncoding, lons, lats);
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
        new GeoPointField(FIELD_NAME, -96.774, 32.763420, fieldType),
        new GeoPointField(FIELD_NAME, -96.7759895324707, 32.7559529921407, fieldType),
        new GeoPointField(FIELD_NAME, -96.77701950073242, 32.77866942010977, fieldType),
        new GeoPointField(FIELD_NAME, -96.7706036567688, 32.7756745755423, fieldType),
        new GeoPointField(FIELD_NAME, -139.73458170890808, 27.703618681345585, fieldType),
        new GeoPointField(FIELD_NAME, -96.4538113027811, 32.94823588839368, fieldType),
        new GeoPointField(FIELD_NAME, -96.65084838867188, 33.06047141970814, fieldType),
        new GeoPointField(FIELD_NAME, -96.7772, 32.778650, fieldType),
        new GeoPointField(FIELD_NAME, -177.23537676036358, -88.56029371730983, fieldType),
        new GeoPointField(FIELD_NAME, -26.779373834241003, 33.541429799076354, fieldType),
        new GeoPointField(FIELD_NAME, -77.35379276106497, 26.774024500421728, fieldType),
        new GeoPointField(FIELD_NAME, -14.796283808944777, -90.0, fieldType),
        new GeoPointField(FIELD_NAME, -178.8538113027811, 32.94823588839368, fieldType),
        new GeoPointField(FIELD_NAME, 178.8538113027811, 32.94823588839368, fieldType),
        new GeoPointField(FIELD_NAME, -73.998776, 40.720611, fieldType),
        new GeoPointField(FIELD_NAME, -179.5, -44.5, fieldType)};

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

  private TopDocs bboxQuery(double minLon, double minLat, double maxLon, double maxLat, int limit) throws Exception {
    GeoPointInBBoxQuery q = new GeoPointInBBoxQuery(FIELD_NAME, termEncoding, minLon, minLat, maxLon, maxLat);
    return searcher.search(q, limit);
  }

  private TopDocs polygonQuery(double[] lon, double[] lat, int limit) throws Exception {
    GeoPointInPolygonQuery q = new GeoPointInPolygonQuery(FIELD_NAME, termEncoding, lon, lat);
    return searcher.search(q, limit);
  }

  private TopDocs geoDistanceQuery(double lon, double lat, double radius, int limit) throws Exception {
    GeoPointDistanceQuery q = new GeoPointDistanceQuery(FIELD_NAME, termEncoding, lon, lat, radius);
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
      return GeoRelationUtils.pointInRectPrecise(pointLon, pointLat, rect.minLon, rect.minLat, rect.maxLon, rect.maxLat);
    } else {
      // Rect crosses dateline:
      return GeoRelationUtils.pointInRectPrecise(pointLon, pointLat, -180.0, rect.minLat, rect.maxLon, rect.maxLat)
          || GeoRelationUtils.pointInRectPrecise(pointLon, pointLat, rect.minLon, rect.minLat, 180.0, rect.maxLat);
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
      return SloppyMath.haversin(centerLat, centerLon, pointLat, pointLon)*1000.0 <= radiusMeters;
    }
  }

  @Override
  protected Boolean distanceRangeContainsPoint(double centerLat, double centerLon, double minRadiusMeters, double radiusMeters, double pointLat, double pointLon) {
    if (radiusQueryCanBeWrong(centerLat, centerLon, pointLon, pointLat, minRadiusMeters)
        || radiusQueryCanBeWrong(centerLat, centerLon, pointLon, pointLat, radiusMeters)) {
      return null;
    } else {
      final double d = SloppyMath.haversin(centerLat, centerLon, pointLat, pointLon)*1000.0;
      return d >= minRadiusMeters && d <= radiusMeters;
    }
  }

  private static boolean radiusQueryCanBeWrong(double centerLat, double centerLon, double ptLon, double ptLat,
                                               final double radius) {
    final long hashedCntr = GeoEncodingUtils.mortonHash(centerLon, centerLat);
    centerLon = GeoEncodingUtils.mortonUnhashLon(hashedCntr);
    centerLat = GeoEncodingUtils.mortonUnhashLat(hashedCntr);
    final long hashedPt = GeoEncodingUtils.mortonHash(ptLon, ptLat);
    ptLon = GeoEncodingUtils.mortonUnhashLon(hashedPt);
    ptLat = GeoEncodingUtils.mortonUnhashLat(hashedPt);

    double ptDistance = SloppyMath.haversin(centerLat, centerLon, ptLat, ptLon)*1000.0;
    double delta = StrictMath.abs(ptDistance - radius);

    // if its within the distance error then it can be wrong
    return delta < (ptDistance*DISTANCE_PCT_ERR);
  }

  public void testRectCrossesCircle() throws Exception {
    assertTrue(GeoRelationUtils.rectCrossesCircle(-180, -90, 180, 0.0, 0.667, 0.0, 88000.0));
  }

  private TopDocs geoDistanceRangeQuery(double lon, double lat, double minRadius, double maxRadius, int limit)
      throws Exception {
    GeoPointDistanceRangeQuery q = new GeoPointDistanceRangeQuery(FIELD_NAME, termEncoding, lon, lat, minRadius, maxRadius);
    return searcher.search(q, limit);
  }

  public void testBBoxQuery() throws Exception {
    TopDocs td = bboxQuery(-96.7772, 32.778650, -96.77690000, 32.778950, 5);
    assertEquals("GeoBoundingBoxQuery failed", 4, td.totalHits);
  }

  public void testPolyQuery() throws Exception {
    TopDocs td = polygonQuery(new double[]{-96.7682647, -96.8280029, -96.6288757, -96.4929199,
            -96.6041564, -96.7449188, -96.76826477, -96.7682647},
        new double[]{33.073130, 32.9942669, 32.938386, 33.0374494,
            33.1369762, 33.1162747, 33.073130, 33.073130}, 5);
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
    assertTrue(GeoRelationUtils.rectCrossesPolyApprox(xMin, yMin, xMax, yMax, px, py, xMinA, yMinA, xMaxA, yMaxA));
    assertFalse(GeoRelationUtils.rectCrossesPolyApprox(-5, 0,  0.000001, 5, px, py, xMin, yMin, xMax, yMax));
    assertTrue(GeoRelationUtils.rectWithinPolyApprox(-5, 0, -2, 5, px, py, xMin, yMin, xMax, yMax));
  }

  public void testBBoxCrossDateline() throws Exception {
    TopDocs td = bboxQuery(179.0, -45.0, -179.0, -44.0, 20);
    assertEquals("BBoxCrossDateline query failed", 2, td.totalHits);
  }

  public void testWholeMap() throws Exception {
    TopDocs td = bboxQuery(GeoUtils.MIN_LON_INCL, GeoUtils.MIN_LAT_INCL, GeoUtils.MAX_LON_INCL, GeoUtils.MAX_LAT_INCL, 20);
    assertEquals("testWholeMap failed", 24, td.totalHits);
    td = polygonQuery(new double[] {GeoUtils.MIN_LON_INCL, GeoUtils.MIN_LON_INCL, GeoUtils.MAX_LON_INCL, GeoUtils.MAX_LON_INCL, GeoUtils.MIN_LON_INCL},
        new double[] {GeoUtils.MIN_LAT_INCL, GeoUtils.MAX_LAT_INCL, GeoUtils.MAX_LAT_INCL, GeoUtils.MIN_LAT_INCL, GeoUtils.MIN_LAT_INCL}, 20);
    assertEquals("testWholeMap failed", 24, td.totalHits);
  }

  public void smallTest() throws Exception {
    TopDocs td = geoDistanceQuery(-73.998776, 40.720611, 1, 20);
    assertEquals("smallTest failed", 2, td.totalHits);
  }

  public void testInvalidBBox() throws Exception {
    try {
      bboxQuery(179.0, -92.0, 181.0, -91.0, 20);
    } catch(Exception e) {
      return;
    }
    throw new Exception("GeoBoundingBox should not accept invalid lat/lon");
  }

  public void testGeoDistanceQuery() throws Exception {
    TopDocs td = geoDistanceQuery(-96.4538113027811, 32.94823588839368, 6000, 20);
    assertEquals("GeoDistanceQuery failed", 2, td.totalHits);
  }

  /** see https://issues.apache.org/jira/browse/LUCENE-6905 */
  public void testNonEmptyTermsEnum() throws Exception {
    TopDocs td = geoDistanceQuery(-177.23537676036358, -88.56029371730983, 7757.999232959935, 20);
    assertEquals("GeoDistanceQuery failed", 2, td.totalHits);
  }

  public void testMultiValuedQuery() throws Exception {
    TopDocs td = bboxQuery(-96.4538113027811, 32.7559529921407, -96.7706036567688, 32.7756745755423, 20);
    // 3 single valued docs + 2 multi-valued docs
    assertEquals("testMultiValuedQuery failed", 5, td.totalHits);
  }

  public void testTooBigRadius() throws Exception {
    try {
      geoDistanceQuery(0.0, 85.0, 4000000, 20);
    } catch (IllegalArgumentException e) {
      e.getMessage().contains("exceeds maxRadius");
    }
  }

  /**
   * Explicitly large
   */
  public void testGeoDistanceQueryHuge() throws Exception {
    TopDocs td = geoDistanceQuery(-96.4538113027811, 32.94823588839368, 6000000, 20);
    assertEquals("GeoDistanceQuery failed", 16, td.totalHits);
  }

  public void testGeoDistanceQueryCrossDateline() throws Exception {
    TopDocs td = geoDistanceQuery(-179.9538113027811, 32.94823588839368, 120000, 20);
    assertEquals("GeoDistanceQuery failed", 3, td.totalHits);
  }

  public void testInvalidGeoDistanceQuery() throws Exception {
    try {
      geoDistanceQuery(181.0, 92.0, 120000, 20);
    } catch (Exception e) {
      return;
    }
    throw new Exception("GeoDistanceQuery should not accept invalid lat/lon as origin");
  }

  public void testMaxDistanceRangeQuery() throws Exception {
    TopDocs td = geoDistanceRangeQuery(0.0, 0.0, 10, 20000000, 20);
    assertEquals("GeoDistanceRangeQuery failed", 24, td.totalHits);
  }

  public void testMortonEncoding() throws Exception {
    long hash = GeoEncodingUtils.mortonHash(180, 90);
    assertEquals(180.0, GeoEncodingUtils.mortonUnhashLon(hash), 0);
    assertEquals(90.0, GeoEncodingUtils.mortonUnhashLat(hash), 0);
  }

  public void testEncodeDecode() throws Exception {
    int iters = atLeast(10000);
    boolean small = random().nextBoolean();
    for(int iter=0;iter<iters;iter++) {
      double lat = randomLat(small);
      double lon = randomLon(small);

      long enc = GeoEncodingUtils.mortonHash(lon, lat);
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

      long enc = GeoEncodingUtils.mortonHash(lon, lat);
      double latEnc = GeoEncodingUtils.mortonUnhashLat(enc);
      double lonEnc = GeoEncodingUtils.mortonUnhashLon(enc);

      long enc2 = GeoEncodingUtils.mortonHash(lon, lat);
      double latEnc2 = GeoEncodingUtils.mortonUnhashLat(enc2);
      double lonEnc2 = GeoEncodingUtils.mortonUnhashLon(enc2);
      assertEquals(latEnc, latEnc2, 0.0);
      assertEquals(lonEnc, lonEnc2, 0.0);
    }
  }
}
