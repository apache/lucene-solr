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
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.spatial.geopoint.document.GeoPointField;
import org.apache.lucene.spatial.geopoint.document.GeoPointField.TermEncoding;
import org.apache.lucene.spatial.util.GeoRelationUtils;
import org.apache.lucene.spatial.util.GeoUtils;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/** Simple tests for GeoPoint with legacy numeric encoding 
 * @deprecated remove this when TermEncoding.NUMERIC is removed */
@Deprecated
public class TestLegacyGeoPointField extends LuceneTestCase {
  
  private static Directory directory = null;
  private static IndexReader reader = null;
  private static IndexSearcher searcher = null;
  private static final String FIELD_NAME = "point";

  @BeforeClass
  public static void beforeClass() throws Exception {
    directory = newDirectory();

    RandomIndexWriter writer = new RandomIndexWriter(random(), directory,
            newIndexWriterConfig(new MockAnalyzer(random()))
                    .setMaxBufferedDocs(TestUtil.nextInt(random(), 100, 1000))
                    .setMergePolicy(newLogMergePolicy()));

    // this is a simple systematic test
    GeoPointField[] pts = new GeoPointField[] {
        new GeoPointField(FIELD_NAME, -96.774, 32.763420, GeoPointField.NUMERIC_TYPE_NOT_STORED),
        new GeoPointField(FIELD_NAME, -96.7759895324707, 32.7559529921407, GeoPointField.NUMERIC_TYPE_NOT_STORED),
        new GeoPointField(FIELD_NAME, -96.77701950073242, 32.77866942010977, GeoPointField.NUMERIC_TYPE_NOT_STORED),
        new GeoPointField(FIELD_NAME, -96.7706036567688, 32.7756745755423, GeoPointField.NUMERIC_TYPE_NOT_STORED),
        new GeoPointField(FIELD_NAME, -139.73458170890808, 27.703618681345585, GeoPointField.NUMERIC_TYPE_NOT_STORED),
        new GeoPointField(FIELD_NAME, -96.4538113027811, 32.94823588839368, GeoPointField.NUMERIC_TYPE_NOT_STORED),
        new GeoPointField(FIELD_NAME, -96.65084838867188, 33.06047141970814, GeoPointField.NUMERIC_TYPE_NOT_STORED),
        new GeoPointField(FIELD_NAME, -96.7772, 32.778650, GeoPointField.NUMERIC_TYPE_NOT_STORED),
        new GeoPointField(FIELD_NAME, -177.23537676036358, -88.56029371730983, GeoPointField.NUMERIC_TYPE_NOT_STORED),
        new GeoPointField(FIELD_NAME, -26.779373834241003, 33.541429799076354, GeoPointField.NUMERIC_TYPE_NOT_STORED),
        new GeoPointField(FIELD_NAME, -77.35379276106497, 26.774024500421728, GeoPointField.NUMERIC_TYPE_NOT_STORED),
        new GeoPointField(FIELD_NAME, -14.796283808944777, -90.0, GeoPointField.NUMERIC_TYPE_NOT_STORED),
        new GeoPointField(FIELD_NAME, -178.8538113027811, 32.94823588839368, GeoPointField.NUMERIC_TYPE_NOT_STORED),
        new GeoPointField(FIELD_NAME, 178.8538113027811, 32.94823588839368, GeoPointField.NUMERIC_TYPE_NOT_STORED),
        new GeoPointField(FIELD_NAME, -73.998776, 40.720611, GeoPointField.NUMERIC_TYPE_NOT_STORED),
        new GeoPointField(FIELD_NAME, -179.5, -44.5, GeoPointField.NUMERIC_TYPE_NOT_STORED)};

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

  private TopDocs bboxQuery(double minLon, double minLat, double maxLon, double maxLat, int limit) throws Exception {
    GeoPointInBBoxQuery q = new GeoPointInBBoxQuery(FIELD_NAME, TermEncoding.NUMERIC, minLon, minLat, maxLon, maxLat);
    return searcher.search(q, limit);
  }

  private TopDocs polygonQuery(double[] lon, double[] lat, int limit) throws Exception {
    GeoPointInPolygonQuery q = new GeoPointInPolygonQuery(FIELD_NAME, TermEncoding.NUMERIC, lon, lat);
    return searcher.search(q, limit);
  }

  private TopDocs geoDistanceQuery(double lon, double lat, double radius, int limit) throws Exception {
    GeoPointDistanceQuery q = new GeoPointDistanceQuery(FIELD_NAME, TermEncoding.NUMERIC, lon, lat, radius);
    return searcher.search(q, limit);
  }

  private TopDocs geoDistanceRangeQuery(double lon, double lat, double minRadius, double maxRadius, int limit)
      throws Exception {
    GeoPointDistanceRangeQuery q = new GeoPointDistanceRangeQuery(FIELD_NAME, TermEncoding.NUMERIC, lon, lat, minRadius, maxRadius);
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

  // GeoBoundingBox should not accept invalid lat/lon
  public void testInvalidBBox() throws Exception {
    expectThrows(Exception.class, () -> {
      bboxQuery(179.0, -92.0, 181.0, -91.0, 20);
    });
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
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      geoDistanceQuery(0.0, 85.0, 4000000, 20);
    });
    assertTrue(expected.getMessage().contains("exceeds maxRadius"));
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

  // GeoDistanceQuery should not accept invalid lat/lon as origin
  public void testInvalidGeoDistanceQuery() throws Exception {
    expectThrows(Exception.class, () -> {
      geoDistanceQuery(181.0, 92.0, 120000, 20);
    });
  }

  public void testMaxDistanceRangeQuery() throws Exception {
    TopDocs td = geoDistanceRangeQuery(0.0, 0.0, 10, 20000000, 20);
    assertEquals("GeoDistanceRangeQuery failed", 24, td.totalHits);
  }
}
