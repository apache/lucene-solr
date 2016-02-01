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
package org.apache.lucene.spatial.util;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

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
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.SloppyMath;
import org.apache.lucene.util.TestUtil;
import org.junit.BeforeClass;

// TODO: cutover TestGeoUtils too?

public abstract class BaseGeoPointTestCase extends LuceneTestCase {

  protected static final String FIELD_NAME = "point";

  private static final double LON_SCALE = (0x1L<< GeoEncodingUtils.BITS)/360.0D;
  private static final double LAT_SCALE = (0x1L<< GeoEncodingUtils.BITS)/180.0D;

  private static double originLat;
  private static double originLon;
  private static double lonRange;
  private static double latRange;

  @BeforeClass
  public static void beforeClassBase() throws Exception {
    // Between 1.0 and 3.0:
    lonRange = 2 * (random().nextDouble() + 0.5);
    latRange = 2 * (random().nextDouble() + 0.5);

    originLon = GeoUtils.normalizeLon(GeoUtils.MIN_LON_INCL + lonRange + (GeoUtils.MAX_LON_INCL - GeoUtils.MIN_LON_INCL - 2 * lonRange) * random().nextDouble());
    originLat = GeoUtils.normalizeLat(GeoUtils.MIN_LAT_INCL + latRange + (GeoUtils.MAX_LAT_INCL - GeoUtils.MIN_LAT_INCL - 2 * latRange) * random().nextDouble());
  }

  // A particularly tricky adversary for BKD tree:
  @Nightly
  public void testSamePointManyTimes() throws Exception {
    int numPoints = atLeast(1000);
    // TODO: GeoUtils are potentially slow if we use small=false with heavy testing
    boolean small = random().nextBoolean();

    // Every doc has 2 points:
    double theLat = randomLat(small);
    double theLon = randomLon(small);

    double[] lats = new double[numPoints];
    Arrays.fill(lats, theLat);

    double[] lons = new double[numPoints];
    Arrays.fill(lons, theLon);

    verify(small, lats, lons);
  }

  @Nightly
  public void testAllLatEqual() throws Exception {
    int numPoints = atLeast(10000);
    // TODO: GeoUtils are potentially slow if we use small=false with heavy testing
    // boolean small = random().nextBoolean();
    boolean small = true;
    double lat = randomLat(small);
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
        lons[docID] = randomLon(small);
        haveRealDoc = true;
        if (VERBOSE) {
          System.out.println("  doc=" + docID + " lat=" + lat + " lon=" + lons[docID]);
        }
      }
      lats[docID] = lat;
    }

    verify(small, lats, lons);
  }

  @Nightly
  public void testAllLonEqual() throws Exception {
    int numPoints = atLeast(10000);
    // TODO: GeoUtils are potentially slow if we use small=false with heavy testing
    // boolean small = random().nextBoolean();
    boolean small = true;
    double theLon = randomLon(small);
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
        lats[docID] = randomLat(small);
        haveRealDoc = true;
        if (VERBOSE) {
          System.out.println("  doc=" + docID + " lat=" + lats[docID] + " lon=" + theLon);
        }
      }
      lons[docID] = theLon;
    }

    verify(small, lats, lons);
  }

  @Nightly
  public void testMultiValued() throws Exception {
    int numPoints = atLeast(10000);
    // Every doc has 2 points:
    double[] lats = new double[2*numPoints];
    double[] lons = new double[2*numPoints];
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    initIndexWriterConfig(FIELD_NAME, iwc);

    // We rely on docID order:
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);

    // TODO: GeoUtils are potentially slow if we use small=false with heavy testing
    boolean small = random().nextBoolean();
    //boolean small = true;

    for (int id=0;id<numPoints;id++) {
      Document doc = new Document();
      lats[2*id] = randomLat(small);
      lons[2*id] = randomLon(small);
      doc.add(newStringField("id", ""+id, Field.Store.YES));
      addPointToDoc(FIELD_NAME, doc, lats[2*id], lons[2*id]);
      lats[2*id+1] = randomLat(small);
      lons[2*id+1] = randomLon(small);
      addPointToDoc(FIELD_NAME, doc, lats[2*id+1], lons[2*id+1]);

      if (VERBOSE) {
        System.out.println("id=" + id);
        System.out.println("  lat=" + lats[2*id] + " lon=" + lons[2*id]);
        System.out.println("  lat=" + lats[2*id+1] + " lon=" + lons[2*id+1]);
      }
      w.addDocument(doc);
    }

    if (random().nextBoolean()) {
      w.forceMerge(1);
    }
    IndexReader r = w.getReader();
    w.close();

    // We can't wrap with "exotic" readers because the BKD query must see the BKDDVFormat:
    IndexSearcher s = newSearcher(r, false);

    int iters = atLeast(75);
    for (int iter=0;iter<iters;iter++) {
      GeoRect rect = randomRect(small, small == false);

      if (VERBOSE) {
        System.out.println("\nTEST: iter=" + iter + " bbox=" + rect);
      }

      Query query = newBBoxQuery(FIELD_NAME, rect);

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

      boolean fail = false;

      for(int docID=0;docID<lats.length/2;docID++) {
        double latDoc1 = lats[2*docID];
        double lonDoc1 = lons[2*docID];
        double latDoc2 = lats[2*docID+1];
        double lonDoc2 = lons[2*docID+1];

        Boolean result1 = rectContainsPoint(rect, latDoc1, lonDoc1);
        if (result1 == null) {
          // borderline case: cannot test
          continue;
        }

        Boolean result2 = rectContainsPoint(rect, latDoc2, lonDoc2);
        if (result2 == null) {
          // borderline case: cannot test
          continue;
        }

        boolean expected = result1 == Boolean.TRUE || result2 == Boolean.TRUE;

        if (hits.get(docID) != expected) {
          String id = s.doc(docID).get("id");
          if (expected) {
            System.out.println(Thread.currentThread().getName() + ": id=" + id + " docID=" + docID + " should match but did not");
          } else {
            System.out.println(Thread.currentThread().getName() + ": id=" + id + " docID=" + docID + " should not match but did");
          }
          System.out.println("  rect=" + rect);
          System.out.println("  lat=" + latDoc1 + " lon=" + lonDoc1 + "\n  lat=" + latDoc2 + " lon=" + lonDoc2);
          System.out.println("  result1=" + result1 + " result2=" + result2);
          fail = true;
        }
      }

      if (fail) {
        fail("some hits were wrong");
      }
    }
    r.close();
    dir.close();
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
    assumeFalse("Direct codec can OOME on this test", TestUtil.getDocValuesFormat(FIELD_NAME).equals("Direct"));
    assumeFalse("Memory codec can OOME on this test", TestUtil.getDocValuesFormat(FIELD_NAME).equals("Memory"));
    doTestRandom(200000);
  }

  private void doTestRandom(int count) throws Exception {

    int numPoints = atLeast(count);

    if (VERBOSE) {
      System.out.println("TEST: numPoints=" + numPoints);
    }

    double[] lats = new double[numPoints];
    double[] lons = new double[numPoints];

    // TODO: GeoUtils are potentially slow if we use small=false with heavy testing
    boolean small = random().nextBoolean();

    boolean haveRealDoc = false;

    for (int id=0;id<numPoints;id++) {
      int x = random().nextInt(20);
      if (x == 17) {
        // Some docs don't have a point:
        lats[id] = Double.NaN;
        if (VERBOSE) {
          System.out.println("  id=" + id + " is missing");
        }
        continue;
      }

      if (id > 0 && x < 3 && haveRealDoc) {
        int oldID;
        while (true) {
          oldID = random().nextInt(id);
          if (Double.isNaN(lats[oldID]) == false) {
            break;
          }
        }

        if (x == 0) {
          // Identical lat to old point
          lats[id] = lats[oldID];
          lons[id] = randomLon(small);
          if (VERBOSE) {
            System.out.println("  id=" + id + " lat=" + lats[id] + " lon=" + lons[id] + " (same lat as doc=" + oldID + ")");
          }
        } else if (x == 1) {
          // Identical lon to old point
          lats[id] = randomLat(small);
          lons[id] = lons[oldID];
          if (VERBOSE) {
            System.out.println("  id=" + id + " lat=" + lats[id] + " lon=" + lons[id] + " (same lon as doc=" + oldID + ")");
          }
        } else {
          assert x == 2;
          // Fully identical point:
          lats[id] = lats[oldID];
          lons[id] = lons[oldID];
          if (VERBOSE) {
            System.out.println("  id=" + id + " lat=" + lats[id] + " lon=" + lons[id] + " (same lat/lon as doc=" + oldID + ")");
          }
        }
      } else {
        lats[id] = randomLat(small);
        lons[id] = randomLon(small);
        haveRealDoc = true;
        if (VERBOSE) {
          System.out.println("  id=" + id + " lat=" + lats[id] + " lon=" + lons[id]);
        }
      }
    }

    verify(small, lats, lons);
  }

  public double randomLat(boolean small) {
    double result;
    if (small) {
      result = GeoUtils.normalizeLat(originLat + latRange * (random().nextDouble() - 0.5));
    } else {
      result = -90 + 180.0 * random().nextDouble();
    }
    return result;
  }

  public double randomLon(boolean small) {
    double result;
    if (small) {
      result = GeoUtils.normalizeLon(originLon + lonRange * (random().nextDouble() - 0.5));
    } else {
      result = -180 + 360.0 * random().nextDouble();
    }
    return result;
  }

  protected GeoRect randomRect(boolean small, boolean canCrossDateLine) {
    double lat0 = randomLat(small);
    double lat1 = randomLat(small);
    double lon0 = randomLon(small);
    double lon1 = randomLon(small);

    if (lat1 < lat0) {
      double x = lat0;
      lat0 = lat1;
      lat1 = x;
    }

    if (lat0 == lat1) {
      lat1 = randomLat(small);
    }

    if (lon0 == lon1) {
      lon1 = randomLon(small);
    }

    if (canCrossDateLine == false && lon1 < lon0) {
      double x = lon0;
      lon0 = lon1;
      lon1 = x;
    }

    return new GeoRect(lon0, lon1, lat0, lat1);
  }

  protected void initIndexWriterConfig(String field, IndexWriterConfig iwc) {
  }

  protected abstract void addPointToDoc(String field, Document doc, double lat, double lon);

  protected abstract Query newBBoxQuery(String field, GeoRect bbox);

  protected abstract Query newDistanceQuery(String field, double centerLat, double centerLon, double radiusMeters);

  protected abstract Query newDistanceRangeQuery(String field, double centerLat, double centerLon, double minRadiusMeters, double radiusMeters);

  protected abstract Query newPolygonQuery(String field, double[] lats, double[] lons);

  /** Returns null if it's borderline case */
  protected abstract Boolean rectContainsPoint(GeoRect rect, double pointLat, double pointLon);

  /** Returns null if it's borderline case */
  protected abstract Boolean polyRectContainsPoint(GeoRect rect, double pointLat, double pointLon);

  /** Returns null if it's borderline case */
  protected abstract Boolean circleContainsPoint(double centerLat, double centerLon, double radiusMeters, double pointLat, double pointLon);

  protected abstract Boolean distanceRangeContainsPoint(double centerLat, double centerLon, double minRadiusMeters, double radiusMeters, double pointLat, double pointLon);

  private static abstract class VerifyHits {

    public void test(AtomicBoolean failed, boolean small, IndexSearcher s, NumericDocValues docIDToID, Set<Integer> deleted, Query query, double[] lats, double[] lons) throws Exception {
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

      boolean fail = false;

      for(int docID=0;docID<maxDoc;docID++) {
        int id = (int) docIDToID.get(docID);
        Boolean expected;
        if (deleted.contains(id)) {
          expected = false;
        } else if (Double.isNaN(lats[id])) {
          expected = false;
        } else {
          expected = shouldMatch(lats[id], lons[id]);
        }

        // null means it's a borderline case which is allowed to be wrong:
        if (expected != null && hits.get(docID) != expected) {
          if (expected) {
            System.out.println(Thread.currentThread().getName() + ": id=" + id + " should match but did not");
          } else {
            System.out.println(Thread.currentThread().getName() + ": id=" + id + " should not match but did");
          }
          System.out.println("  small=" + small + " query=" + query +
              " docID=" + docID + "\n  lat=" + lats[id] + " lon=" + lons[id] +
              "\n  deleted?=" + deleted.contains(id));
          if (Double.isNaN(lats[id]) == false) {
            describe(docID, lats[id], lons[id]);
          }
          fail = true;
        }
      }

      if (fail) {
        failed.set(true);
        fail("some hits were wrong");
      }
    }

    /** Return true if we definitely should match, false if we definitely
     *  should not match, and null if it's a borderline case which might
     *  go either way. */
    protected abstract Boolean shouldMatch(double lat, double lon);

    protected abstract void describe(int docID, double lat, double lon);
  }

  protected void verify(final boolean small, final double[] lats, final double[] lons) throws Exception {
    IndexWriterConfig iwc = newIndexWriterConfig();
    // Else we can get O(N^2) merging:
    int mbd = iwc.getMaxBufferedDocs();
    if (mbd != -1 && mbd < lats.length/100) {
      iwc.setMaxBufferedDocs(lats.length/100);
    }
    initIndexWriterConfig(FIELD_NAME, iwc);
    Directory dir;
    if (lats.length > 100000) {
      dir = newFSDirectory(createTempDir(getClass().getSimpleName()));
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
        addPointToDoc(FIELD_NAME, doc, lats[id], lons[id]);
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
    final IndexReader r = DirectoryReader.open(w);
    w.close();

    // We can't wrap with "exotic" readers because the BKD query must see the BKDDVFormat:
    final IndexSearcher s = newSearcher(r, false);

    // Make sure queries are thread safe:
    int numThreads = TestUtil.nextInt(random(), 2, 5);

    List<Thread> threads = new ArrayList<>();
    final int iters = atLeast(75);

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

            if (VERBOSE) {
              System.out.println("\nTEST: iter=" + iter + " s=" + s);
            }
            Query query;
            VerifyHits verifyHits;

            if (random().nextBoolean()) {
              // BBox: don't allow dateline crossing when testing small:
              final GeoRect bbox = randomRect(small, small == false);

              query = newBBoxQuery(FIELD_NAME, bbox);

              verifyHits = new VerifyHits() {
                @Override
                protected Boolean shouldMatch(double pointLat, double pointLon) {
                  return rectContainsPoint(bbox, pointLat, pointLon);
                }
                @Override
                protected void describe(int docID, double lat, double lon) {
                }
              };

            } else if (random().nextBoolean()) {
              // Distance
              final boolean rangeQuery = random().nextBoolean();
              final double centerLat = randomLat(small);
              final double centerLon = randomLon(small);

              final double radiusMeters;
              final double minRadiusMeters;

              if (small) {
                // Approx 3 degrees lon at the equator:
                radiusMeters = random().nextDouble() * 333000 + 1.0;
              } else {
                // So the query can cover at most 50% of the earth's surface:
                radiusMeters = random().nextDouble() * GeoProjectionUtils.SEMIMAJOR_AXIS * Math.PI / 2.0 + 1.0;
              }

              // generate a random minimum radius between 1% and 95% the max radius
              minRadiusMeters = (0.01 + 0.94 * random().nextDouble()) * radiusMeters;

              if (VERBOSE) {
                final DecimalFormat df = new DecimalFormat("#,###.00", DecimalFormatSymbols.getInstance(Locale.ENGLISH));
                System.out.println("  radiusMeters = " + df.format(radiusMeters)
                    + ((rangeQuery == true) ? " minRadiusMeters = " + df.format(minRadiusMeters) : ""));
              }

              try {
                if (rangeQuery == true) {
                  query = newDistanceRangeQuery(FIELD_NAME, centerLat, centerLon, minRadiusMeters, radiusMeters);
                } else {
                  query = newDistanceQuery(FIELD_NAME, centerLat, centerLon, radiusMeters);
                }
              } catch (IllegalArgumentException e) {
                if (e.getMessage().contains("exceeds maxRadius")) {
                  continue;
                }
                throw e;
              }

              verifyHits = new VerifyHits() {
                @Override
                protected Boolean shouldMatch(double pointLat, double pointLon) {
                  final double radius = radiusMeters;
                  final double minRadius = minRadiusMeters;
                  if (rangeQuery == false) {
                    return circleContainsPoint(centerLat, centerLon, radius, pointLat, pointLon);
                  } else {
                    return distanceRangeContainsPoint(centerLat, centerLon, minRadius, radius, pointLat, pointLon);
                  }
                }

                @Override
                protected void describe(int docID, double pointLat, double pointLon) {
                  double distanceKM = SloppyMath.haversin(centerLat, centerLon, pointLat, pointLon);
                  System.out.println("  docID=" + docID + " centerLon=" + centerLon + " centerLat=" + centerLat
                      + " pointLon=" + pointLon + " pointLat=" + pointLat + " distanceMeters=" + (distanceKM * 1000)
                      + " vs" + ((rangeQuery == true) ? " minRadiusMeters=" + minRadiusMeters : "") + " radiusMeters=" + radiusMeters);
                }
              };

              // TODO: get poly query working with dateline crossing too (how?)!
            } else {

              // TODO: poly query can't handle dateline crossing yet:
              final GeoRect bbox = randomRect(small, false);

              // Polygon
              double[] lats = new double[5];
              double[] lons = new double[5];
              lats[0] = bbox.minLat;
              lons[0] = bbox.minLon;
              lats[1] = bbox.maxLat;
              lons[1] = bbox.minLon;
              lats[2] = bbox.maxLat;
              lons[2] = bbox.maxLon;
              lats[3] = bbox.minLat;
              lons[3] = bbox.maxLon;
              lats[4] = bbox.minLat;
              lons[4] = bbox.minLon;
              query = newPolygonQuery(FIELD_NAME, lats, lons);

              verifyHits = new VerifyHits() {
                @Override
                protected Boolean shouldMatch(double pointLat, double pointLon) {
                  return polyRectContainsPoint(bbox, pointLat, pointLon);
                }

                @Override
                protected void describe(int docID, double lat, double lon) {
                }
              };
            }

            if (query != null) {

              if (VERBOSE) {
                System.out.println("  query=" + query);
              }

              verifyHits.test(failed, small, s, docIDToID, deleted, query, lats, lons);
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
    assertFalse(failed.get());
  }
}
