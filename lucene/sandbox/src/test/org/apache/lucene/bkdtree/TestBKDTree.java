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
import org.apache.lucene.document.Field;
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
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase.Nightly;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

// TODO: can test framework assert we don't leak temp files?

public class TestBKDTree extends LuceneTestCase {

  private static boolean smallBBox;

  @BeforeClass
  public static void beforeClass() {
    smallBBox = random().nextBoolean();
  }

  public void testAllLatEqual() throws Exception {
    int numPoints = atLeast(10000);
    double lat = randomLat();
    double[] lats = new double[numPoints];
    double[] lons = new double[numPoints];

    boolean haveRealDoc = false;

    for(int docID=0;docID<numPoints;docID++) {
      int x = random().nextInt(20);
      if (x == 17) {
        // Some docs don't have a point:
        lats[docID] = Double.NaN;
        if (VERBOSE) {
          System.out.println("  doc=" + docID + " is missing");
        }
        continue;
      }

      if (docID > 0 && x == 14 && haveRealDoc) {
        int oldDocID;
        while (true) {
          oldDocID = random().nextInt(docID);
          if (Double.isNaN(lats[oldDocID]) == false) {
            break;
          }
        }
            
        // Fully identical point:
        lons[docID] = lons[oldDocID];
        if (VERBOSE) {
          System.out.println("  doc=" + docID + " lat=" + lat + " lon=" + lons[docID] + " (same lat/lon as doc=" + oldDocID + ")");
        }
      } else {
        lons[docID] = randomLon();
        haveRealDoc = true;
        if (VERBOSE) {
          System.out.println("  doc=" + docID + " lat=" + lat + " lon=" + lons[docID]);
        }
      }
      lats[docID] = lat;
    }

    verify(lats, lons);
  }

  public void testAllLonEqual() throws Exception {
    int numPoints = atLeast(10000);
    double theLon = randomLon();
    double[] lats = new double[numPoints];
    double[] lons = new double[numPoints];

    boolean haveRealDoc = false;

    //System.out.println("theLon=" + theLon);

    for(int docID=0;docID<numPoints;docID++) {
      int x = random().nextInt(20);
      if (x == 17) {
        // Some docs don't have a point:
        lats[docID] = Double.NaN;
        if (VERBOSE) {
          System.out.println("  doc=" + docID + " is missing");
        }
        continue;
      }

      if (docID > 0 && x == 14 && haveRealDoc) {
        int oldDocID;
        while (true) {
          oldDocID = random().nextInt(docID);
          if (Double.isNaN(lats[oldDocID]) == false) {
            break;
          }
        }
            
        // Fully identical point:
        lats[docID] = lats[oldDocID];
        if (VERBOSE) {
          System.out.println("  doc=" + docID + " lat=" + lats[docID] + " lon=" + theLon + " (same lat/lon as doc=" + oldDocID + ")");
        }
      } else {
        lats[docID] = randomLat();
        haveRealDoc = true;
        if (VERBOSE) {
          System.out.println("  doc=" + docID + " lat=" + lats[docID] + " lon=" + theLon);
        }
      }
      lons[docID] = theLon;
    }

    verify(lats, lons);
  }

  public void testMultiValued() throws Exception {
    int numPoints = atLeast(10000);
    // Every doc has 2 points:
    double[] lats = new double[2*numPoints];
    double[] lons = new double[2*numPoints];
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    // We rely on docID order:
    iwc.setMergePolicy(newLogMergePolicy());
    int maxPointsInLeaf = TestUtil.nextInt(random(), 16, 2048);
    int maxPointsSortInHeap = TestUtil.nextInt(random(), maxPointsInLeaf, 1024*1024);
    Codec codec = TestUtil.alwaysDocValuesFormat(new BKDTreeDocValuesFormat(maxPointsInLeaf, maxPointsSortInHeap));
    iwc.setCodec(codec);
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);

    for (int docID=0;docID<numPoints;docID++) {
      Document doc = new Document();
      lats[2*docID] = randomLat();
      lons[2*docID] = randomLon();
      doc.add(new BKDPointField("point", lats[2*docID], lons[2*docID]));
      lats[2*docID+1] = randomLat();
      lons[2*docID+1] = randomLon();
      doc.add(new BKDPointField("point", lats[2*docID+1], lons[2*docID+1]));
      w.addDocument(doc);
    }

    if (random().nextBoolean()) {
      w.forceMerge(1);
    }
    IndexReader r = w.getReader();
    w.close();
    // We can't wrap with "exotic" readers because the BKD query must see the BKDDVFormat:
    IndexSearcher s = newSearcher(r, false);

    int iters = atLeast(100);
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

      Query query = new BKDPointInBBoxQuery("point", lat0, lat1, lon0, lon1);

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

      for(int docID=0;docID<lats.length/2;docID++) {
        double latDoc1 = lats[2*docID];
        double lonDoc1 = lons[2*docID];
        double latDoc2 = lats[2*docID+1];
        double lonDoc2 = lons[2*docID+1];
        boolean expected = rectContainsPointEnc(lat0, lat1, lon0, lon1, latDoc1, lonDoc1) ||
          rectContainsPointEnc(lat0, lat1, lon0, lon1, latDoc2, lonDoc2);

        if (hits.get(docID) != expected) {
          fail("docID=" + docID + " latDoc1=" + latDoc1 + " lonDoc1=" + lonDoc1 + " latDoc2=" + latDoc2 + " lonDoc2=" + lonDoc2 + " expected " + expected + " but got: " + hits.get(docID));
        }
      }
    }
    r.close();
    dir.close();
  }

  // A particularly tricky adversary:
  public void testSamePointManyTimes() throws Exception {
    int numPoints = atLeast(1000);

    // Every doc has 2 points:
    double theLat = randomLat();
    double theLon = randomLon();

    double[] lats = new double[numPoints];
    Arrays.fill(lats, theLat);

    double[] lons = new double[numPoints];
    Arrays.fill(lons, theLon);

    verify(lats, lons);
  }

  public void testRandomTiny() throws Exception {
    // Make sure single-leaf-node case is OK:
    doTestRandom(10);
  }

  public void testRandomMedium() throws Exception {
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
          System.out.println("  doc=" + docID + " is missing");
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
            System.out.println("  doc=" + docID + " lat=" + lats[docID] + " lon=" + lons[docID] + " (same lat as doc=" + oldDocID + ")");
          }
        } else if (x == 1) {
          // Identical lon to old point
          lats[docID] = randomLat();
          lons[docID] = lons[oldDocID];
          if (VERBOSE) {
            System.out.println("  doc=" + docID + " lat=" + lats[docID] + " lon=" + lons[docID] + " (same lon as doc=" + oldDocID + ")");
          }
        } else {
          assert x == 2;
          // Fully identical point:
          lats[docID] = lats[oldDocID];
          lons[docID] = lons[oldDocID];
          if (VERBOSE) {
            System.out.println("  doc=" + docID + " lat=" + lats[docID] + " lon=" + lons[docID] + " (same lat/lon as doc=" + oldDocID + ")");
          }
        }
      } else {
        lats[docID] = randomLat();
        lons[docID] = randomLon();
        haveRealDoc = true;
        if (VERBOSE) {
          System.out.println("  doc=" + docID + " lat=" + lats[docID] + " lon=" + lons[docID]);
        }
      }
    }

    verify(lats, lons);
  }

  private static final double TOLERANCE = 1e-7;

  private static void verify(final double[] lats, final double[] lons) throws Exception {

    int maxPointsInLeaf = TestUtil.nextInt(random(), 16, 2048);
    int maxPointsSortInHeap = TestUtil.nextInt(random(), maxPointsInLeaf, 1024*1024);
    IndexWriterConfig iwc = newIndexWriterConfig();
    // Else we can get O(N^2) merging:
    int mbd = iwc.getMaxBufferedDocs();
    if (mbd != -1 && mbd < lats.length/100) {
      iwc.setMaxBufferedDocs(lats.length/100);
    }
    final DocValuesFormat dvFormat = new BKDTreeDocValuesFormat(maxPointsInLeaf, maxPointsSortInHeap);
    Codec codec = new Lucene53Codec() {
        @Override
        public DocValuesFormat getDocValuesFormatForField(String field) {
          if (field.equals("point")) {
            return dvFormat;
          } else {
            return super.getDocValuesFormatForField(field);
          }
        }
      };
    iwc.setCodec(codec);
    Directory dir;
    if (lats.length > 100000) {
      dir = newFSDirectory(createTempDir("TestBKDTree"));
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
        doc.add(new BKDPointField("point", lats[id], lons[id]));
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

    // We can't wrap with "exotic" readers because the BKD query must see the BKDDVFormat:
    final IndexSearcher s = newSearcher(r, false);

    int numThreads = TestUtil.nextInt(random(), 2, 5);

    List<Thread> threads = new ArrayList<>();
    final int iters = atLeast(100);

    final CountDownLatch startingGun = new CountDownLatch(1);
    final AtomicBoolean failed = new AtomicBoolean();

    for(int i=0;i<numThreads;i++) {
      Thread thread = new Thread() {
          @Override
          public void run() {
            try {
              _run();
            } catch (Exception e) {
              failed.set(true);
              throw new RuntimeException(e);
            }
          }

          private void _run() throws Exception {
            startingGun.await();

            NumericDocValues docIDToID = MultiDocValues.getNumericValues(r, "id");

            for (int iter=0;iter<iters && failed.get() == false;iter++) {
              double lat0 = randomLat();
              double lat1 = randomLat();
              double lon0 = randomLon();
              double lon1 = randomLon();

              if (lat1 < lat0) {
                double x = lat0;
                lat0 = lat1;
                lat1 = x;
              }

              boolean crossesDateLine;
              if (lon1 < lon0) {
                if (random().nextBoolean()) {
                  double x = lon0;
                  lon0 = lon1;
                  lon1 = x;
                  crossesDateLine = false;
                } else {
                  crossesDateLine = true;
                }
              } else {
                crossesDateLine = false;
              }

              if (VERBOSE) {
                System.out.println("\nTEST: iter=" + iter + " lat=" + lat0 + " TO " + lat1 + " lon=" + lon0 + " TO " + lon1 + " crossesDateLine=" + crossesDateLine);
              }

              Query query;
              // TODO: get poly query working with dateline crossing too (how?)!
              if (crossesDateLine || random().nextBoolean()) {
                query = new BKDPointInBBoxQuery("point", lat0, lat1, lon0, lon1);
              } else {
                double[] lats = new double[5];
                double[] lons = new double[5];
                lats[0] = lat0;
                lons[0] = lon0;
                lats[1] = lat1;
                lons[1] = lon0;
                lats[2] = lat1;
                lons[2] = lon1;
                lats[3] = lat0;
                lons[3] = lon1;
                lats[4] = lat0;
                lons[4] = lon0;
                query = new BKDPointInPolygonQuery("point", lats, lons);
              }

              if (VERBOSE) {
                System.out.println("  using query: " + query);
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

              if (VERBOSE) {
                System.out.println("  hitCount: " + hits.cardinality());
              }
      
              for(int docID=0;docID<r.maxDoc();docID++) {
                int id = (int) docIDToID.get(docID);
                boolean expected = deleted.contains(id) == false && rectContainsPointEnc(lat0, lat1, lon0, lon1, lats[id], lons[id]);
                if (hits.get(docID) != expected) {
                  if (query instanceof BKDPointInPolygonQuery &&
                      (Math.abs(lat0-lats[id]) < TOLERANCE ||
                       Math.abs(lat1-lats[id]) < TOLERANCE ||
                       Math.abs(lon0-lons[id]) < TOLERANCE ||
                       Math.abs(lon1-lons[id]) < TOLERANCE)) {
                    // The poly check quantizes slightly differently, so we allow for boundary cases to disagree
                  } else {
                    // We do exact quantized comparison so the bbox query should never disagree:
                    fail(Thread.currentThread().getName() + ": iter=" + iter + " id=" + id + " docID=" + docID + " lat=" + lats[id] + " lon=" + lons[id] + " (bbox: lat=" + lat0 + " TO " + lat1 + " lon=" + lon0 + " TO " + lon1 + ") expected " + expected + " but got: " + hits.get(docID) + " deleted?=" + deleted.contains(id) + " query=" + query + " crossesDateLine=" + crossesDateLine);
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
    int rectLatMinEnc = BKDTreeWriter.encodeLat(rectLatMin);
    int rectLatMaxEnc = BKDTreeWriter.encodeLat(rectLatMax);
    int rectLonMinEnc = BKDTreeWriter.encodeLon(rectLonMin);
    int rectLonMaxEnc = BKDTreeWriter.encodeLon(rectLonMax);
    int pointLatEnc = BKDTreeWriter.encodeLat(pointLat);
    int pointLonEnc = BKDTreeWriter.encodeLon(pointLon);

    if (rectLonMin < rectLonMax) {
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

  private static double randomLat() {
    if (smallBBox) {
      return 2.0 * (random().nextDouble()-0.5);
    } else {
      return -90 + 180.0 * random().nextDouble();
    }
  }

  private static double randomLon() {
    if (smallBBox) {
      return 2.0 * (random().nextDouble()-0.5);
    } else {
      return -180 + 360.0 * random().nextDouble();
    }
  }

  public void testEncodeDecode() throws Exception {
    int iters = atLeast(10000);
    for(int iter=0;iter<iters;iter++) {
      double lat = randomLat();
      double latQuantized = BKDTreeWriter.decodeLat(BKDTreeWriter.encodeLat(lat));
      assertEquals(lat, latQuantized, BKDTreeWriter.TOLERANCE);

      double lon = randomLon();
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
    Codec codec = TestUtil.alwaysDocValuesFormat(new BKDTreeDocValuesFormat());
    iwc.setCodec(codec);
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
    Document doc = new Document();
    doc.add(new BKDPointField("field", -18.2861, 147.7));
    w.addDocument(doc);
    IndexReader r = w.getReader();

    // We can't wrap with "exotic" readers because the BKD query must see the BKDDVFormat:
    IndexSearcher s = newSearcher(r, false);
    // Need to run a query so the DV field is really loaded:
    TopDocs hits = s.search(new BKDPointInBBoxQuery("field", -30, 0, 140, 150), 1);
    assertEquals(1, hits.totalHits);
    assertTrue(Accountables.toString((Accountable) r.leaves().get(0).reader()).contains("delegate"));
    IOUtils.close(r, w, dir);
  }
}
