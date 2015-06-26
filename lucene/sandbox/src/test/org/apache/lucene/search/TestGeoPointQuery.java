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
import org.apache.lucene.util.GeoUtils;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
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

  // Global bounding box we will "cover" in the random test; we have to make this "smallish" else the queries take very long:
  private static double originLat;
  private static double originLon;
  private static double range;

  @BeforeClass
  public static void beforeClass() throws Exception {
    directory = newDirectory();

    // when we randomly test the full lat/lon space it can result in very very slow query times, this is due to the
    // number of ranges that can be created in degenerate cases.

    // Between 1.0 and 3.0:
    range = 2*(random().nextDouble() + 0.5);
    originLon = GeoUtils.MIN_LON_INCL + range + (GeoUtils.MAX_LON_INCL - GeoUtils.MIN_LON_INCL - 2*range) * random().nextDouble();
    originLat = GeoUtils.MIN_LAT_INCL + range + (GeoUtils.MAX_LAT_INCL - GeoUtils.MIN_LAT_INCL - 2*range) * random().nextDouble();
    if (VERBOSE) {
      System.out.println("TEST: originLon=" + originLon + " originLat=" + originLat + " range=" + range);
    }
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory,
            newIndexWriterConfig(new MockAnalyzer(random()))
                    .setMaxBufferedDocs(TestUtil.nextInt(random(), 100, 1000))
                    .setMergePolicy(newLogMergePolicy()));

    // create some simple geo points
    final FieldType storedPoint = new FieldType(GeoPointField.TYPE_STORED);
    // this is a simple systematic test
    GeoPointField[] pts = new GeoPointField[] {
         new GeoPointField(FIELD_NAME, -96.4538113027811, 32.94823588839368, storedPoint),
         new GeoPointField(FIELD_NAME, -96.7759895324707, 32.7559529921407, storedPoint),
         new GeoPointField(FIELD_NAME, -96.77701950073242, 32.77866942010977, storedPoint),
         new GeoPointField(FIELD_NAME, -96.7706036567688, 32.7756745755423, storedPoint),
         new GeoPointField(FIELD_NAME, -139.73458170890808, 27.703618681345585, storedPoint),
         new GeoPointField(FIELD_NAME, -96.65084838867188, 33.06047141970814, storedPoint),
         new GeoPointField(FIELD_NAME, -96.7772, 32.778650, storedPoint),
         new GeoPointField(FIELD_NAME, -83.99724648980559, 58.29438379542874, storedPoint),
         new GeoPointField(FIELD_NAME, -26.779373834241003, 33.541429799076354, storedPoint),
         new GeoPointField(FIELD_NAME, -77.35379276106497, 26.774024500421728, storedPoint),
         new GeoPointField(FIELD_NAME, -14.796283808944777, -62.455081198245665, storedPoint)};

    for (GeoPointField p : pts) {
        Document doc = new Document();
        doc.add(p);
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

  @Test
  public void testBBoxQuery() throws Exception {
    TopDocs td = bboxQuery(-96.7772, 32.778650, -96.77690000, 32.778950, 5);
    assertEquals("GeoBoundingBoxQuery failed", 2, td.totalHits);
  }

  @Test
  public void testPolyQuery() throws Exception {
    TopDocs td = polygonQuery( new double[] {-96.7682647, -96.8280029, -96.6288757, -96.4929199,
                                             -96.6041564, -96.7449188, -96.76826477, -96.7682647},
                               new double[] { 33.073130, 32.9942669, 32.938386, 33.0374494,
                                              33.1369762, 33.1162747, 33.073130, 33.073130}, 5);
    assertEquals("GeoPolygonQuery failed", td.totalHits, 1);
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

  public void testRandomTiny() throws Exception {
    // Make sure single-leaf-node case is OK:
    doTestRandom(10);
  }

  public void testRandom() throws Exception {
    doTestRandom(10000);
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
          lons[docID] = randomLon();
          if (VERBOSE) {
            //System.out.println("  doc=" + docID + " lat=" + lats[docID] + " lon=" + lons[docID] + " (same lat as doc=" + oldDocID + ")");
          }
        } else if (x == 1) {
          // Identical lon to old point
          lats[docID] = randomLat();
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
        lats[docID] = randomLat();
        lons[docID] = randomLon();
        haveRealDoc = true;
        if (VERBOSE) {
          //System.out.println("  doc=" + docID + " lat=" + lats[docID] + " lon=" + lons[docID]);
        }
      }
    }

    verify(lats, lons);
  }

  private static void verify(double[] lats, double[] lons) throws Exception {
    IndexWriterConfig iwc = newIndexWriterConfig();
    Directory dir;
    if (lats.length > 100000) {
      dir = newFSDirectory(createTempDir("TestGeoPointQuery"));
      iwc.setCodec(TestUtil.getDefaultCodec());
    } else {
      dir = newDirectory();
    }
    Set<Integer> deleted = new HashSet<>();
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
    IndexReader r = DirectoryReader.open(w, true);
    w.close();

    IndexSearcher s = newSearcher(r);

    // Make sure queries are thread safe:
    int numThreads = TestUtil.nextInt(random(), 2, 5);

    List<Thread> threads = new ArrayList<>();
    final int iters = atLeast(100);

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
              double lat0 = randomLat();
              double lat1 = randomLat();
              double lon0 = randomLon();
              double lon1 = randomLon();

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

              if (VERBOSE) {
                System.out.println("\nTEST: iter=" + iter + " lat=" + lat0 + " TO " + lat1 + " lon=" + lon0 + " TO " + lon1);
              }

              Query query;
              boolean tooBigBBox = false;
              boolean polySearch = false;

              double bboxLat0 = lat0;
              double bboxLat1 = lat1;
              double bboxLon0 = lon0;
              double bboxLon1 = lon1;

              if (random().nextBoolean()) {
                query = new GeoPointInBBoxQuery(FIELD_NAME, lon0, lat0, lon1, lat1);
              } else {
                polySearch = true;
                if (random().nextBoolean()) {
                  // Intentionally pass a "too big" bounding box:
                  double pct = random().nextDouble()*0.5;
                  double width = lon1-lon0;
                  bboxLon0 = Math.max(-180.0, lon0-width*pct);
                  bboxLon1 = Math.min(180.0, lon1+width*pct);
                  double height = lat1-lat0;
                  bboxLat0 = Math.max(-90.0, lat0-height*pct);
                  bboxLat1 = Math.min(90.0, lat1+height*pct);
                  tooBigBBox = true;
                }
                double[] pLats = new double[5];
                double[] pLons = new double[5];
                pLats[0] = bboxLat0;
                pLons[0] = bboxLon0;
                pLats[1] = bboxLat1;
                pLons[1] = bboxLon0;
                pLats[2] = bboxLat1;
                pLons[2] = bboxLon1;
                pLats[3] = bboxLat0;
                pLons[3] = bboxLon1;
                pLats[4] = bboxLat0;
                pLons[4] = bboxLon0;
                query = new GeoPointInPolygonQuery(FIELD_NAME, bboxLon0, bboxLat0, bboxLon1, bboxLat1, pLons, pLats);
              }

              final FixedBitSet hits = new FixedBitSet(r.maxDoc());
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

              for(int docID=0;docID<r.maxDoc();docID++) {
                int id = (int) docIDToID.get(docID);
                if (polySearch) {
                  lat0 = bboxLat0;
                  lon0 = bboxLon0;
                  lat1 = bboxLat1;
                  lon1 = bboxLon1;
                }
                // morton encode & decode to compare apples to apples (that is, compare with same hash precision error
                // present in the index)
                final long pointHash = GeoUtils.mortonHash(lons[id], lats[id]);
                final double pointLon = GeoUtils.mortonUnhashLon(pointHash);
                final double pointLat = GeoUtils.mortonUnhashLat(pointHash);
                if (!tolerateIgnorance(lat0, lat1, lon0, lon1, pointLat, pointLon)) {
                  boolean expected = (deleted.contains(id) == false) &&
                      rectContainsPointEnc(lat0, lat1, lon0, lon1, pointLat, pointLon);
                  if (hits.get(docID) != expected) {
                    System.out.println(Thread.currentThread().getName() + ": iter=" + iter + " id=" + id + " docID=" + docID + " lat=" + pointLat + " lon=" + pointLon + " (bbox: lat=" + lat0 + " TO " + lat1 + " lon=" + lon0 + " TO " + lon1 + ") expected " + expected + " but got: " + hits.get(docID) + " deleted?=" + deleted.contains(id) + " query=" + query);
                    if (tooBigBBox) {
                      System.out.println("  passed too-big bbox: lat=" + bboxLat0 + " TO " + bboxLat1 + " lon=" + bboxLon0 + " TO " + bboxLon1);
                    }
                    fail("wrong result");
                  }
                }
              }
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

  private static boolean rectContainsPointEnc(double rectLatMin, double rectLatMax,
                                              double rectLonMin, double rectLonMax,
                                              double pointLat, double pointLon) {
    if (Double.isNaN(pointLat)) {
      return false;
    }
    return GeoUtils.bboxContains(pointLon, pointLat, rectLonMin, rectLatMin, rectLonMax, rectLatMax);
  }

  private static boolean tolerateIgnorance(final double minLat, final double maxLat,
                                           final double minLon, final double maxLon,
                                           final double lat, final double lon) {
    // we can tolerate variance at the GeoUtils.TOLERANCE decimal place
    final int tLon = (int)(lon/(GeoUtils.TOLERANCE-1));
    final int tLat = (int)(lat/(GeoUtils.TOLERANCE-1));
    final int tMinLon = (int)(minLon/(GeoUtils.TOLERANCE-1));
    final int tMinLat = (int)(minLat/(GeoUtils.TOLERANCE-1));
    final int tMaxLon = (int)(maxLon/(GeoUtils.TOLERANCE-1));
    final int tMaxLat = (int)(maxLat/(GeoUtils.TOLERANCE-1));

    return ((tMinLon - tLon) == 0 || (tMinLat - tLat) == 0
         || (tMaxLon - tLon) == 0 || (tMaxLat - tLat) == 0);
  }

  private static double randomLat() {
    return originLat + range * (random().nextDouble()-0.5);
  }

  private static double randomLon() {
    return originLon + range * (random().nextDouble()-0.5);
  }
}
