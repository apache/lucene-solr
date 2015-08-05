package org.apache.lucene.search;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.GeoPointField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.GeoDistanceUtils;
import org.apache.lucene.util.GeoUtils;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.SloppyMath;
import org.apache.lucene.util.TestGeoUtils;
import org.apache.lucene.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit testing for basic GeoPoint query logic
 *
 * @lucene.experimental
 */
public class TestGeoPointQuery extends LuceneTestCase {
  private static Directory directory = null;
  private static IndexReader reader = null;
  private static IndexSearcher searcher = null;

  private static final String FIELD_NAME = "geoField";

  // error threshold for point-distance queries (in meters)
  // @todo haversine is sloppy, would be good to have a better heuristic for
  // determining the possible haversine error
  private static final int DISTANCE_ERR = 1000;

  @BeforeClass
  public static void beforeClass() throws Exception {
    directory = newDirectory();

    RandomIndexWriter writer = new RandomIndexWriter(random(), directory,
            newIndexWriterConfig(new MockAnalyzer(random()))
                    .setMaxBufferedDocs(TestUtil.nextInt(random(), 100, 1000))
                    .setMergePolicy(newLogMergePolicy()));

    // create some simple geo points
    final FieldType storedPoint = new FieldType(GeoPointField.TYPE_STORED);
    // this is a simple systematic test
    GeoPointField[] pts = new GeoPointField[] {
         new GeoPointField(FIELD_NAME, -96.774, 32.763420, storedPoint),
         new GeoPointField(FIELD_NAME, -96.7759895324707, 32.7559529921407, storedPoint),
         new GeoPointField(FIELD_NAME, -96.77701950073242, 32.77866942010977, storedPoint),
         new GeoPointField(FIELD_NAME, -96.7706036567688, 32.7756745755423, storedPoint),
         new GeoPointField(FIELD_NAME, -139.73458170890808, 27.703618681345585, storedPoint),
         new GeoPointField(FIELD_NAME, -96.4538113027811, 32.94823588839368, storedPoint),
         new GeoPointField(FIELD_NAME, -96.65084838867188, 33.06047141970814, storedPoint),
         new GeoPointField(FIELD_NAME, -96.7772, 32.778650, storedPoint),
         new GeoPointField(FIELD_NAME, -83.99724648980559, 58.29438379542874, storedPoint),
         new GeoPointField(FIELD_NAME, -26.779373834241003, 33.541429799076354, storedPoint),
         new GeoPointField(FIELD_NAME, -77.35379276106497, 26.774024500421728, storedPoint),
         new GeoPointField(FIELD_NAME, -14.796283808944777, -62.455081198245665, storedPoint),
         new GeoPointField(FIELD_NAME, -178.8538113027811, 32.94823588839368, storedPoint),
         new GeoPointField(FIELD_NAME, 178.8538113027811, 32.94823588839368, storedPoint),
         new GeoPointField(FIELD_NAME, -73.998776, 40.720611, storedPoint),
         new GeoPointField(FIELD_NAME, -179.5, -44.5, storedPoint)};

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
    GeoPointInBBoxQuery q = new GeoPointInBBoxQuery(FIELD_NAME, minLon, minLat, maxLon, maxLat);
    return searcher.search(q, limit);
  }

  private TopDocs polygonQuery(double[] lon, double[] lat, int limit) throws Exception {
    GeoPointInPolygonQuery q = new GeoPointInPolygonQuery(FIELD_NAME, lon, lat);
    return searcher.search(q, limit);
  }

  private TopDocs geoDistanceQuery(double lon, double lat, double radius, int limit) throws Exception {
    GeoPointDistanceQuery q = new GeoPointDistanceQuery(FIELD_NAME, lon, lat, radius);
    return searcher.search(q, limit);
  }

  @Test
  public void testBBoxQuery() throws Exception {
    TopDocs td = bboxQuery(-96.7772, 32.778650, -96.77690000, 32.778950, 5);
    assertEquals("GeoBoundingBoxQuery failed", 4, td.totalHits);
  }

  @Test
  public void testPolyQuery() throws Exception {
    TopDocs td = polygonQuery(new double[]{-96.7682647, -96.8280029, -96.6288757, -96.4929199,
            -96.6041564, -96.7449188, -96.76826477, -96.7682647},
        new double[]{33.073130, 32.9942669, 32.938386, 33.0374494,
            33.1369762, 33.1162747, 33.073130, 33.073130}, 5);
    assertEquals("GeoPolygonQuery failed", 2, td.totalHits);
  }

  @Test
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
    assertTrue(GeoUtils.rectCrossesPoly(xMin, yMin, xMax, yMax, px, py, xMinA, yMinA, xMaxA, yMaxA));
    assertFalse(GeoUtils.rectCrossesPoly(-5, 0,  0.000001, 5, px, py, xMin, yMin, xMax, yMax));
    assertTrue(GeoUtils.rectWithinPoly(-5, 0, -2, 5, px, py, xMin, yMin, xMax, yMax));
  }

  @Test
  public void testBBoxCrossDateline() throws Exception {
    TopDocs td = bboxQuery(179.0, -45.0, -179.0, -44.0, 20);
    assertEquals("BBoxCrossDateline query failed", 2, td.totalHits);
  }

  @Test
  public void testWholeMap() throws Exception {
    TopDocs td = bboxQuery(-179.9, -89.9, 179.9, 89.9, 20);
    assertEquals("testWholeMap failed", 24, td.totalHits);
  }

  @Test
  public void smallTest() throws Exception {
    TopDocs td = geoDistanceQuery(-73.998776, 40.720611, 1, 20);
    assertEquals("smallTest failed", 2, td.totalHits);
  }

  @Test
  public void testInvalidBBox() throws Exception {
    try {
      bboxQuery(179.0, -92.0, 181.0, -91.0, 20);
    } catch(Exception e) {
      return;
    }
    throw new Exception("GeoBoundingBox should not accept invalid lat/lon");
  }

  @Test
  public void testGeoDistanceQuery() throws Exception {
    TopDocs td = geoDistanceQuery(-96.4538113027811, 32.94823588839368, 6000, 20);
    assertEquals("GeoDistanceQuery failed", 2, td.totalHits);
  }

  @Test
  public void testMultiValuedQuery() throws Exception {
    TopDocs td = bboxQuery(-96.4538113027811, 32.7559529921407, -96.7706036567688, 32.7756745755423, 20);
    // 3 single valued docs + 2 multi-valued docs
    assertEquals("testMultiValuedQuery failed", 5, td.totalHits);
  }

  /**
   * Explicitly large
   */
  @Nightly
  public void testGeoDistanceQueryHuge() throws Exception {
    TopDocs td = geoDistanceQuery(-96.4538113027811, 32.94823588839368, 2000000, 20);
    assertEquals("GeoDistanceQuery failed", 13, td.totalHits);
  }

  @Test
  public void testGeoDistanceQueryCrossDateline() throws Exception {
    TopDocs td = geoDistanceQuery(-179.9538113027811, 32.94823588839368, 120000, 20);
    assertEquals("GeoDistanceQuery failed", 3, td.totalHits);
  }

  @Test
  public void testInvalidGeoDistanceQuery() throws Exception {
    try {
      geoDistanceQuery(181.0, 92.0, 120000, 20);
    } catch (Exception e) {
      return;
    }
    throw new Exception("GeoDistanceQuery should not accept invalid lat/lon as origin");
  }

  public void testRandomTiny() throws Exception {
    // Make sure single-leaf-node case is OK:
    doTestRandom(10);
  }

  public void testRandom() throws Exception {
    doTestRandom(10000);
  }

  @Test
  public void testMortonEncoding() throws Exception {
    long hash = GeoUtils.mortonHash(180, 90);
    assertEquals(180.0, GeoUtils.mortonUnhashLon(hash), 0);
    assertEquals(90.0, GeoUtils.mortonUnhashLat(hash), 0);
  }

  @Nightly
  public void testRandomBig() throws Exception {
    doTestRandom(200000);
  }

  private void doTestRandom(int count) throws Exception {

    int numPoints = atLeast(count);

    if (VERBOSE) {
      System.out.println("TEST: numPoints=" + numPoints);
    }

    double[] lats = new double[numPoints];
    double[] lons = new double[numPoints];

    boolean haveRealDoc = false;

    for (int docID=0;docID<numPoints;docID++) {
      int x = random().nextInt(20);
      if (x == 17) {
        // Some docs don't have a point:
        lats[docID] = Double.NaN;
        if (VERBOSE) {
          //System.out.println("  doc=" + docID + " is missing");
        }
        continue;
      }

      if (docID > 0 && x < 3 && haveRealDoc) {
        int oldDocID;
        while (true) {
          oldDocID = random().nextInt(docID);
          if (Double.isNaN(lats[oldDocID]) == false) {
            break;
          }
        }

        if (x == 0) {
          // Identical lat to old point
          lats[docID] = lats[oldDocID];
          lons[docID] = TestGeoUtils.randomLon();
          if (VERBOSE) {
            //System.out.println("  doc=" + docID + " lat=" + lats[docID] + " lon=" + lons[docID] + " (same lat as doc=" + oldDocID + ")");
          }
        } else if (x == 1) {
          // Identical lon to old point
          lats[docID] = TestGeoUtils.randomLat();
          lons[docID] = lons[oldDocID];
          if (VERBOSE) {
            //System.out.println("  doc=" + docID + " lat=" + lats[docID] + " lon=" + lons[docID] + " (same lon as doc=" + oldDocID + ")");
          }
        } else {
          assert x == 2;
          // Fully identical point:
          lats[docID] = lats[oldDocID];
          lons[docID] = lons[oldDocID];
          if (VERBOSE) {
            //System.out.println("  doc=" + docID + " lat=" + lats[docID] + " lon=" + lons[docID] + " (same lat/lon as doc=" + oldDocID + ")");
          }
        }
      } else {
        lats[docID] = TestGeoUtils.randomLat();
        lons[docID] = TestGeoUtils.randomLon();
        haveRealDoc = true;
        if (VERBOSE) {
          //System.out.println("  doc=" + docID + " lat=" + lats[docID] + " lon=" + lons[docID]);
        }
      }
    }

    verify(lats, lons);
  }

  private static void verify(final double[] lats, final double[] lons) throws Exception {
    IndexWriterConfig iwc = newIndexWriterConfig();
    Directory dir;
    if (lats.length > 100000) {
      dir = newFSDirectory(createTempDir("TestGeoPointQuery"));
      iwc.setCodec(TestUtil.getDefaultCodec());
    } else {
      dir = newDirectory();
    }
    final Set<Integer> deleted = new HashSet<>();
    // RandomIndexWriter is too slow here:
    IndexWriter w = new IndexWriter(dir, iwc);
    for(int id=0;id<lats.length;id++) {
      Document doc = new Document();
      doc.add(newStringField("id", ""+id, Field.Store.NO));
      doc.add(new NumericDocValuesField("id", id));
      if (Double.isNaN(lats[id]) == false) {
        if (VERBOSE) {
          System.out.println("  id=" + id + " lat=" + lats[id] + " lon=" + lons[id]);
        }
        doc.add(new GeoPointField(FIELD_NAME, lons[id], lats[id], Field.Store.NO));
      } else if (VERBOSE) {
        System.out.println("  id=" + id + " skipped");
      }
      w.addDocument(doc);
      if (id > 0 && random().nextInt(100) == 42) {
        int idToDelete = random().nextInt(id);
        w.deleteDocuments(new Term("id", ""+idToDelete));
        deleted.add(idToDelete);
        if (VERBOSE) {
          System.out.println("  delete id=" + idToDelete);
        }
      }
    }
    if (random().nextBoolean()) {
      w.forceMerge(1);
    }
    final IndexReader r = DirectoryReader.open(w, true);
    w.close();

    final IndexSearcher s = newSearcher(r);

    // Make sure queries are thread safe:
    int numThreads = TestUtil.nextInt(random(), 2, 5);

    List<Thread> threads = new ArrayList<>();
    final int iters = atLeast(10);

    final CountDownLatch startingGun = new CountDownLatch(1);

    for(int i=0;i<numThreads;i++) {
      Thread thread = new Thread() {
          @Override
          public void run() {
            try {
              _run();
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }

          private void _run() throws Exception {
            startingGun.await();

            NumericDocValues docIDToID = MultiDocValues.getNumericValues(r, "id");

            for (int iter=0;iter<iters;iter++) {
              if (VERBOSE) {
                System.out.println("\nTEST: iter=" + iter);
              }

              Query query;

              VerifyHits verifyHits;

              if (random().nextBoolean()) {
                final GeoBoundingBox bbox = randomBBox();

                query = new GeoPointInBBoxQuery(FIELD_NAME, bbox.minLon, bbox.minLat, bbox.maxLon, bbox.maxLat);
                verifyHits = new VerifyHits() {
                    @Override
                    protected Boolean shouldMatch(double pointLat, double pointLon) {

                      // morton encode & decode to compare apples to apples (that is, compare with same hash precision error
                      // present in the index)
                      long pointHash = GeoUtils.mortonHash(pointLon, pointLat);
                      pointLon = GeoUtils.mortonUnhashLon(pointHash);
                      pointLat = GeoUtils.mortonUnhashLat(pointHash);

                      if (bboxQueryCanBeWrong(bbox, pointLat, pointLon)) {
                        return null;
                      } else {
                        return rectContainsPointEnc(bbox, pointLat, pointLon);
                      }
                    }
                   };
              } else if (random().nextBoolean()) {
                
                // generate a random bounding box
                GeoBoundingBox bbox = randomBBox();

                final double centerLat = bbox.minLat + ((bbox.maxLat - bbox.minLat)/2.0);
                final double centerLon = bbox.minLon + ((bbox.maxLon - bbox.minLon)/2.0);

                // radius (in meters) as a function of the random generated bbox
                final double radius = GeoDistanceUtils.vincentyDistance(centerLon, centerLat, centerLon, bbox.minLat);
                //final double radius = SloppyMath.haversin(centerLat, centerLon, bbox.minLat, centerLon)*1000;
                if (VERBOSE) {
                  System.out.println("\t radius = " + radius);
                }
                // query using the centroid of the bounding box
                query = new GeoPointDistanceQuery(FIELD_NAME, centerLon, centerLat, radius);

                verifyHits = new VerifyHits() {
                    @Override
                    protected Boolean shouldMatch(double pointLat, double pointLon) {
                      if (Double.isNaN(pointLat) || Double.isNaN(pointLon)) {
                        return null;
                      }
                      if (radiusQueryCanBeWrong(centerLat, centerLon, pointLon, pointLat, radius)) {
                        return null;
                      } else {
                        return distanceContainsPt(centerLon, centerLat, pointLon, pointLat, radius);
                      }
                    }
                   };
                
              } else {
                final GeoBoundingBox bbox = randomBBox();

                double[] pLats = new double[5];
                double[] pLons = new double[5];
                pLats[0] = bbox.minLat;
                pLons[0] = bbox.minLon;
                pLats[1] = bbox.maxLat;
                pLons[1] = bbox.minLon;
                pLats[2] = bbox.maxLat;
                pLons[2] = bbox.maxLon;
                pLats[3] = bbox.minLat;
                pLons[3] = bbox.maxLon;
                pLats[4] = bbox.minLat;
                pLons[4] = bbox.minLon;
                query = new GeoPointInPolygonQuery(FIELD_NAME, pLons, pLats);

                verifyHits = new VerifyHits() {
                    @Override
                    protected Boolean shouldMatch(double pointLat, double pointLon) {
                      // morton encode & decode to compare apples to apples (that is, compare with same hash precision error
                      // present in the index)
                      long pointHash = GeoUtils.mortonHash(pointLon, pointLat);
                      pointLon = GeoUtils.mortonUnhashLon(pointHash);
                      pointLat = GeoUtils.mortonUnhashLat(pointHash);

                      if (bboxQueryCanBeWrong(bbox, pointLat, pointLon)) {
                        return null;
                      } else {
                        return rectContainsPointEnc(bbox, pointLat, pointLon);
                      }
                    }
                  };
              }

              verifyHits.test(s, docIDToID, deleted, query, lats, lons);
            }
          }
        };
      thread.setName("T" + i);
      thread.start();
      threads.add(thread);
    }

    startingGun.countDown();
    for(Thread thread : threads) {
      thread.join();
    }

    IOUtils.close(r, dir);
  }

  private static abstract class VerifyHits {

    public void test(IndexSearcher s, NumericDocValues docIDToID, Set<Integer> deleted, Query query, double[] lats, double[] lons) throws Exception {
      int maxDoc = s.getIndexReader().maxDoc();
      final FixedBitSet hits = new FixedBitSet(maxDoc);
      s.search(query, new SimpleCollector() {

          private int docBase;

          @Override
          public boolean needsScores() {
            return false;
          }

          @Override
          protected void doSetNextReader(LeafReaderContext context) throws IOException {
            docBase = context.docBase;
          }

          @Override
          public void collect(int doc) {
            hits.set(docBase+doc);
          }
        });

      for(int docID=0;docID<maxDoc;docID++) {
        int id = (int) docIDToID.get(docID);
        Boolean expected;
        if (deleted.contains(id)) {
          expected = false;
        } else {
          expected = shouldMatch(lats[id], lons[id]);
        }

        // null means it's a borderline case which is allowed to be wrong:
        if (expected != null) {

          if (hits.get(docID) != expected) {
            System.out.println(Thread.currentThread().getName() + ": id=" + id +
                               " docID=" + docID + " lat=" + lats[id] + " lon=" + lons[id] +
                               " deleted?=" + deleted.contains(id) + " expected=" + expected + " but got " + hits.get(docID) +
                               " query=" + query);
            fail("wrong hit");
          }
        }
      }
    }

    /** Return true if we definitely should match, false if we definitely
     *  should not match, and null if it's a borderline case which might
     *  go either way. */
    protected abstract Boolean shouldMatch(double lat, double lon);
  }

  private static boolean distanceContainsPt(double lonA, double latA, double lonB, double latB, final double radius) {
    final long hashedPtA = GeoUtils.mortonHash(lonA, latA);
    lonA = GeoUtils.mortonUnhashLon(hashedPtA);
    latA = GeoUtils.mortonUnhashLat(hashedPtA);
    final long hashedPtB = GeoUtils.mortonHash(lonB, latB);
    lonB = GeoUtils.mortonUnhashLon(hashedPtB);
    latB = GeoUtils.mortonUnhashLat(hashedPtB);

    return (SloppyMath.haversin(latA, lonA, latB, lonB)*1000.0 <= radius);
  }

  private static boolean rectContainsPointEnc(GeoBoundingBox bbox, double pointLat, double pointLon) {
    // We should never see a deleted doc here?
    assert Double.isNaN(pointLat) == false;
    return GeoUtils.bboxContains(pointLon, pointLat, bbox.minLon, bbox.minLat, bbox.maxLon, bbox.maxLat);
  }

  private static boolean radiusQueryCanBeWrong(double centerLat, double centerLon, double ptLon, double ptLat,
                                               final double radius) {
    final long hashedCntr = GeoUtils.mortonHash(centerLon, centerLat);
    centerLon = GeoUtils.mortonUnhashLon(hashedCntr);
    centerLat = GeoUtils.mortonUnhashLat(hashedCntr);
    final long hashedPt = GeoUtils.mortonHash(ptLon, ptLat);
    ptLon = GeoUtils.mortonUnhashLon(hashedPt);
    ptLat = GeoUtils.mortonUnhashLat(hashedPt);

    double ptDistance = SloppyMath.haversin(centerLat, centerLon, ptLat, ptLon)*1000.0;
    double delta = StrictMath.abs(ptDistance - radius);

    // if its within the distance error then it can be wrong
    return delta < DISTANCE_ERR;
  }

  private static boolean bboxQueryCanBeWrong(GeoBoundingBox bbox, double lat, double lon) {
    // we can tolerate variance at the GeoUtils.TOLERANCE decimal place
    final int tLon = (int)(lon/(GeoUtils.TOLERANCE-1));
    final int tLat = (int)(lat/(GeoUtils.TOLERANCE-1));
    final int tMinLon = (int)(bbox.minLon/(GeoUtils.TOLERANCE-1));
    final int tMinLat = (int)(bbox.minLat/(GeoUtils.TOLERANCE-1));
    final int tMaxLon = (int)(bbox.maxLon/(GeoUtils.TOLERANCE-1));
    final int tMaxLat = (int)(bbox.maxLat/(GeoUtils.TOLERANCE-1));

    return ((tMinLon - tLon) == 0 || (tMinLat - tLat) == 0
         || (tMaxLon - tLon) == 0 || (tMaxLat - tLat) == 0);
  }

  private static GeoBoundingBox randomBBox() {
    double lat0 = TestGeoUtils.randomLat();
    double lat1 = TestGeoUtils.randomLat();
    double lon0 = TestGeoUtils.randomLon();
    double lon1 = TestGeoUtils.randomLon();

    if (lat1 < lat0) {
      double x = lat0;
      lat0 = lat1;
      lat1 = x;
    }

    if (lon1 < lon0) {
      double x = lon0;
      lon0 = lon1;
      lon1 = x;
    }

    return new GeoBoundingBox(lon0, lon1, lat0, lat1);
  }
}
