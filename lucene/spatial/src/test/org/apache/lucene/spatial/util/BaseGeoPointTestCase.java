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
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.codecs.PointsWriter;
import org.apache.lucene.codecs.lucene60.Lucene60PointsReader;
import org.apache.lucene.codecs.lucene60.Lucene60PointsWriter;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.SloppyMath;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.bkd.BKDWriter;
import org.junit.BeforeClass;

/**
 * Abstract class to do basic tests for a geospatial impl (high level
 * fields and queries)
 * NOTE: This test focuses on geospatial (distance queries, polygon
 * queries, etc) indexing and search, not any underlying storage
 * format or encoding: it merely supplies two hooks for the encoding
 * so that tests can be exact. The [stretch] goal is for this test to be
 * so thorough in testing a new geo impl that if this
 * test passes, then all Lucene/Solr tests should also pass.  Ie,
 * if there is some bug in a given geo impl that this
 * test fails to catch then this test needs to be improved! */
public abstract class BaseGeoPointTestCase extends LuceneTestCase {

  protected static final String FIELD_NAME = "point";

  private static double originLat;
  private static double originLon;
  private static double lonRange;
  private static double latRange;

  @BeforeClass
  public static void beforeClassBase() throws Exception {
    // Between 1.0 and 3.0:
    lonRange = 2 * (random().nextDouble() + 0.5);
    latRange = 2 * (random().nextDouble() + 0.5);

    originLon = normalizeLon(GeoUtils.MIN_LON_INCL + lonRange + (GeoUtils.MAX_LON_INCL - GeoUtils.MIN_LON_INCL - 2 * lonRange) * random().nextDouble());
    originLat = normalizeLat(GeoUtils.MIN_LAT_INCL + latRange + (GeoUtils.MAX_LAT_INCL - GeoUtils.MIN_LAT_INCL - 2 * latRange) * random().nextDouble());
  }

  /** Puts longitude in range of -180 to +180. */
  public static double normalizeLon(double lon_deg) {
    if (lon_deg >= -180 && lon_deg <= 180) {
      return lon_deg; //common case, and avoids slight double precision shifting
    }
    double off = (lon_deg + 180) % 360;
    if (off < 0) {
      return 180 + off;
    } else if (off == 0 && lon_deg > 0) {
      return 180;
    } else {
      return -180 + off;
    }
  }

  /** Puts latitude in range of -90 to 90. */
  public static double normalizeLat(double lat_deg) {
    if (lat_deg >= -90 && lat_deg <= 90) {
      return lat_deg; //common case, and avoids slight double precision shifting
    }
    double off = Math.abs((lat_deg + 90) % 360);
    return (off <= 180 ? off : 360-off) - 90;
  }
  
  /** Valid values that should not cause exception */
  public void testIndexExtremeValues() {
    Document document = new Document();
    addPointToDoc("foo", document, 90.0, 180.0);
    addPointToDoc("foo", document, 90.0, -180.0);
    addPointToDoc("foo", document, -90.0, 180.0);
    addPointToDoc("foo", document, -90.0, -180.0);
  }
  
  /** Invalid values */
  public void testIndexOutOfRangeValues() {
    Document document = new Document();
    IllegalArgumentException expected;

    expected = expectThrows(IllegalArgumentException.class, () -> {
      addPointToDoc("foo", document, Math.nextUp(90.0), 50.0);
    });
    assertTrue(expected.getMessage().contains("invalid latitude"));
    
    expected = expectThrows(IllegalArgumentException.class, () -> {
      addPointToDoc("foo", document, Math.nextDown(-90.0), 50.0);
    });
    assertTrue(expected.getMessage().contains("invalid latitude"));
    
    expected = expectThrows(IllegalArgumentException.class, () -> {
      addPointToDoc("foo", document, 90.0, Math.nextUp(180.0));
    });
    assertTrue(expected.getMessage().contains("invalid longitude"));
    
    expected = expectThrows(IllegalArgumentException.class, () -> {
      addPointToDoc("foo", document, 90.0, Math.nextDown(-180.0));
    });
    assertTrue(expected.getMessage().contains("invalid longitude"));
  }
  
  /** NaN: illegal */
  public void testIndexNaNValues() {
    Document document = new Document();
    IllegalArgumentException expected;

    expected = expectThrows(IllegalArgumentException.class, () -> {
      addPointToDoc("foo", document, Double.NaN, 50.0);
    });
    assertTrue(expected.getMessage().contains("invalid latitude"));
    
    expected = expectThrows(IllegalArgumentException.class, () -> {
      addPointToDoc("foo", document, 50.0, Double.NaN);
    });
    assertTrue(expected.getMessage().contains("invalid longitude"));
  }
  
  /** Inf: illegal */
  public void testIndexInfValues() {
    Document document = new Document();
    IllegalArgumentException expected;

    expected = expectThrows(IllegalArgumentException.class, () -> {
      addPointToDoc("foo", document, Double.POSITIVE_INFINITY, 50.0);
    });
    assertTrue(expected.getMessage().contains("invalid latitude"));
    
    expected = expectThrows(IllegalArgumentException.class, () -> {
      addPointToDoc("foo", document, Double.NEGATIVE_INFINITY, 50.0);
    });
    assertTrue(expected.getMessage().contains("invalid latitude"));
    
    expected = expectThrows(IllegalArgumentException.class, () -> {
      addPointToDoc("foo", document, 50.0, Double.POSITIVE_INFINITY);
    });
    assertTrue(expected.getMessage().contains("invalid longitude"));
    
    expected = expectThrows(IllegalArgumentException.class, () -> {
      addPointToDoc("foo", document, 50.0, Double.NEGATIVE_INFINITY);
    });
    assertTrue(expected.getMessage().contains("invalid longitude"));
  }
  
  /** Add a single point and search for it in a box */
  // NOTE: we don't currently supply an exact search, only ranges, because of the lossiness...
  public void testBoxBasics() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    // add a doc with a point
    Document document = new Document();
    addPointToDoc("field", document, 18.313694, -65.227444);
    writer.addDocument(document);
    
    // search and verify we found our doc
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);
    assertEquals(1, searcher.count(newRectQuery("field", 18, 19, -66, -65)));

    reader.close();
    writer.close();
    dir.close();
  }
  

  // box should not accept invalid lat/lon
  public void testBoxInvalidCoordinates() throws Exception {
    expectThrows(Exception.class, () -> {
      newRectQuery("field", -92.0, -91.0, 179.0, 181.0);
    });
  }

  /** test we can search for a point */
  public void testDistanceBasics() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    // add a doc with a location
    Document document = new Document();
    addPointToDoc("field", document, 18.313694, -65.227444);
    writer.addDocument(document);
    
    // search within 50km and verify we found our doc
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);
    assertEquals(1, searcher.count(newDistanceQuery("field", 18, -65, 50_000)));

    reader.close();
    writer.close();
    dir.close();
  }
  
  /** distance query should not accept invalid lat/lon as origin */
  public void testDistanceIllegal() throws Exception {
    expectThrows(Exception.class, () -> {
      newDistanceQuery("field", 92.0, 181.0, 120000);
    });
  }
  /** negative distance queries are not allowed */
  public void testDistanceNegative() {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      newDistanceQuery("field", 18, 19, -1);
    });
    assertTrue(expected.getMessage().contains("radiusMeters"));
    assertTrue(expected.getMessage().contains("invalid"));
  }
  
  /** NaN distance queries are not allowed */
  public void testDistanceNaN() {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      newDistanceQuery("field", 18, 19, Double.NaN);
    });
    assertTrue(expected.getMessage().contains("radiusMeters"));
    assertTrue(expected.getMessage().contains("invalid"));
  }
  
  /** Inf distance queries are not allowed */
  public void testDistanceInf() {
    IllegalArgumentException expected;
    
    expected = expectThrows(IllegalArgumentException.class, () -> {
      newDistanceQuery("field", 18, 19, Double.POSITIVE_INFINITY);
    });
    assertTrue(expected.getMessage().contains("radiusMeters"));
    assertTrue(expected.getMessage().contains("invalid"));
    
    expected = expectThrows(IllegalArgumentException.class, () -> {
      newDistanceQuery("field", 18, 19, Double.NEGATIVE_INFINITY);
    });
    assertTrue(expected.getMessage(), expected.getMessage().contains("radiusMeters"));
    assertTrue(expected.getMessage().contains("invalid"));
  }
  
  /** test we can search for a polygon */
  public void testPolygonBasics() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    // add a doc with a point
    Document document = new Document();
    addPointToDoc("field", document, 18.313694, -65.227444);
    writer.addDocument(document);
    
    // search and verify we found our doc
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);
    assertEquals(1, searcher.count(newPolygonQuery("field",
                                                   new double[] { 18, 18, 19, 19, 18 },
                                                   new double[] { -66, -65, -65, -66, -66 })));

    reader.close();
    writer.close();
    dir.close();
  }

  // A particularly tricky adversary for BKD tree:
  public void testSamePointManyTimes() throws Exception {
    int numPoints = atLeast(1000);
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

  public void testAllLatEqual() throws Exception {
    int numPoints = atLeast(10000);
    boolean small = random().nextBoolean();
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

  public void testAllLonEqual() throws Exception {
    int numPoints = atLeast(10000);
    boolean small = random().nextBoolean();
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

    boolean small = random().nextBoolean();

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

    // TODO: share w/ verify; just need parallel array of the expected ids
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
        System.out.println("\nTEST: iter=" + iter + " rect=" + rect);
      }

      Query query = newRectQuery(FIELD_NAME, rect.minLat, rect.maxLat, rect.minLon, rect.maxLon);

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
        
        boolean result1 = rectContainsPoint(rect, latDoc1, lonDoc1);
        boolean result2 = rectContainsPoint(rect, latDoc2, lonDoc2);

        boolean expected = result1 || result2;

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
      result = normalizeLat(originLat + latRange * (random().nextDouble() - 0.5));
    } else {
      result = -90 + 180.0 * random().nextDouble();
    }
    return quantizeLat(result);
  }

  public double randomLon(boolean small) {
    double result;
    if (small) {
      result = normalizeLon(originLon + lonRange * (random().nextDouble() - 0.5));
    } else {
      result = -180 + 360.0 * random().nextDouble();
    }
    return quantizeLon(result);
  }

  /** Override this to quantize randomly generated lat, so the test won't fail due to quantization errors, which are 1) annoying to debug,
   *  and 2) should never affect "real" usage terribly. */
  protected double quantizeLat(double lat) {
    return lat;
  }

  /** Override this to quantize randomly generated lon, so the test won't fail due to quantization errors, which are 1) annoying to debug,
   *  and 2) should never affect "real" usage terribly. */
  protected double quantizeLon(double lon) {
    return lon;
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

    if (canCrossDateLine == false && lon1 < lon0) {
      double x = lon0;
      lon0 = lon1;
      lon1 = x;
    }

    return new GeoRect(lat0, lat1, lon0, lon1);
  }

  protected void initIndexWriterConfig(String field, IndexWriterConfig iwc) {
  }

  protected abstract void addPointToDoc(String field, Document doc, double lat, double lon);

  protected abstract Query newRectQuery(String field, double minLat, double maxLat, double minLon, double maxLon);

  protected abstract Query newDistanceQuery(String field, double centerLat, double centerLon, double radiusMeters);

  protected abstract Query newDistanceRangeQuery(String field, double centerLat, double centerLon, double minRadiusMeters, double radiusMeters);

  protected abstract Query newPolygonQuery(String field, double[] lats, double[] lons);

  static final boolean rectContainsPoint(GeoRect rect, double pointLat, double pointLon) {
    assert Double.isNaN(pointLat) == false;

    if (rect.minLon < rect.maxLon) {
      return GeoRelationUtils.pointInRectPrecise(pointLat, pointLon, rect.minLat, rect.maxLat, rect.minLon, rect.maxLon);
    } else {
      // Rect crosses dateline:
      return GeoRelationUtils.pointInRectPrecise(pointLat, pointLon, rect.minLat, rect.maxLat, -180.0, rect.maxLon)
        || GeoRelationUtils.pointInRectPrecise(pointLat, pointLon, rect.minLat, rect.maxLat, rect.minLon, 180.0);
    }
  }
  
  static final boolean polygonContainsPoint(double polyLats[], double polyLons[], double pointLat, double pointLon) {
    return GeoRelationUtils.pointInPolygon(polyLats, polyLons, pointLat, pointLon);
  }

  static final boolean circleContainsPoint(double centerLat, double centerLon, double radiusMeters, double pointLat, double pointLon) {
    double distanceMeters = SloppyMath.haversinMeters(centerLat, centerLon, pointLat, pointLon);
    boolean result = distanceMeters <= radiusMeters;
    //System.out.println("  shouldMatch?  centerLon=" + centerLon + " centerLat=" + centerLat + " pointLon=" + pointLon + " pointLat=" + pointLat + " result=" + result + " distanceMeters=" + (distanceKM * 1000));
    return result;
  }

  static final boolean distanceRangeContainsPoint(double centerLat, double centerLon, double minRadiusMeters, double radiusMeters, double pointLat, double pointLon) {
    final double d = SloppyMath.haversinMeters(centerLat, centerLon, pointLat, pointLon);
    return d >= minRadiusMeters && d <= radiusMeters;
  }

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

      // Change to false to see all wrong hits:
      boolean failFast = true;

      for(int docID=0;docID<maxDoc;docID++) {
        int id = (int) docIDToID.get(docID);
        boolean expected;
        if (deleted.contains(id)) {
          expected = false;
        } else if (Double.isNaN(lats[id])) {
          expected = false;
        } else {
          expected = shouldMatch(lats[id], lons[id]);
        }

        if (hits.get(docID) != expected) {

          // Print only one failed hit; add a true || in here to see all failures:
          if (failFast == false || failed.getAndSet(true) == false) {
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
            if (failFast) {
              fail("wrong hit (first of possibly more)");
            } else {
              fail = true;
            }
          }
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
    protected abstract boolean shouldMatch(double lat, double lon);

    protected abstract void describe(int docID, double lat, double lon);
  }

  protected void verify(boolean small, double[] lats, double[] lons) throws Exception {
    IndexWriterConfig iwc = newIndexWriterConfig();
    // Else we can get O(N^2) merging:
    int mbd = iwc.getMaxBufferedDocs();
    if (mbd != -1 && mbd < lats.length/100) {
      iwc.setMaxBufferedDocs(lats.length/100);
    }
    Directory dir;
    if (lats.length > 100000) {
      dir = newFSDirectory(createTempDir(getClass().getSimpleName()));
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
    IndexSearcher s = newSearcher(r, false);

    final int iters = atLeast(75);

    final AtomicBoolean failed = new AtomicBoolean();

    NumericDocValues docIDToID = MultiDocValues.getNumericValues(r, "id");

    for (int iter=0;iter<iters && failed.get() == false;iter++) {

      if (VERBOSE) {
        System.out.println("\n" + Thread.currentThread().getName() + ": TEST: iter=" + iter + " s=" + s);
      }
      Query query;
      VerifyHits verifyHits;

      if (random().nextBoolean()) {
        // Rect: don't allow dateline crossing when testing small:
        final GeoRect rect = randomRect(small, small == false);

        query = newRectQuery(FIELD_NAME, rect.minLat, rect.maxLat, rect.minLon, rect.maxLon);

        verifyHits = new VerifyHits() {
          @Override
          protected boolean shouldMatch(double pointLat, double pointLon) {
            return rectContainsPoint(rect, pointLat, pointLon);
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

        double radiusMeters;
        double minRadiusMeters;

        if (small) {
          // Approx 3 degrees lon at the equator:
          radiusMeters = random().nextDouble() * 333000 + 1.0;
        } else {
          // So the query can cover at most 50% of the earth's surface:
          radiusMeters = random().nextDouble() * GeoUtils.SEMIMAJOR_AXIS * Math.PI / 2.0 + 1.0;
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
          protected boolean shouldMatch(double pointLat, double pointLon) {
            if (rangeQuery == false) {
              return circleContainsPoint(centerLat, centerLon, radiusMeters, pointLat, pointLon);
            } else {
              return distanceRangeContainsPoint(centerLat, centerLon, minRadiusMeters, radiusMeters, pointLat, pointLon);
            }
          }

          @Override
          protected void describe(int docID, double pointLat, double pointLon) {
            double distanceMeters = SloppyMath.haversinMeters(centerLat, centerLon, pointLat, pointLon);
            System.out.println("  docID=" + docID + " centerLat=" + centerLat + " centerLon=" + centerLon
                + " pointLat=" + pointLat + " pointLon=" + pointLon + " distanceMeters=" + distanceMeters
                + " vs" + ((rangeQuery == true) ? " minRadiusMeters=" + minRadiusMeters : "") + " radiusMeters=" + radiusMeters);
          }
        };

        // TODO: get poly query working with dateline crossing too (how?)!
      } else {

        // TODO: poly query can't handle dateline crossing yet:
        final GeoRect bbox = randomRect(small, false);

        // Polygon
        final double[] polyLats;
        final double[] polyLons;
        // TODO: factor this out, maybe if we add Polygon class?
        if (random().nextBoolean()) {
          // box
          polyLats = new double[5];
          polyLons = new double[5];
          polyLats[0] = bbox.minLat;
          polyLons[0] = bbox.minLon;
          polyLats[1] = bbox.maxLat;
          polyLons[1] = bbox.minLon;
          polyLats[2] = bbox.maxLat;
          polyLons[2] = bbox.maxLon;
          polyLats[3] = bbox.minLat;
          polyLons[3] = bbox.maxLon;
          polyLats[4] = bbox.minLat;
          polyLons[4] = bbox.minLon;
        } else {
          // right triangle
          polyLats = new double[4];
          polyLons = new double[4];
          polyLats[0] = bbox.minLat;
          polyLons[0] = bbox.minLon;
          polyLats[1] = bbox.maxLat;
          polyLons[1] = bbox.minLon;
          polyLats[2] = bbox.maxLat;
          polyLons[2] = bbox.maxLon;
          polyLats[3] = bbox.minLat;
          polyLons[3] = bbox.minLon;
        }
        query = newPolygonQuery(FIELD_NAME, polyLats, polyLons);

        verifyHits = new VerifyHits() {
          @Override
          protected boolean shouldMatch(double pointLat, double pointLon) {
            return polygonContainsPoint(polyLats, polyLons, pointLat, pointLon);
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

    IOUtils.close(r, dir);
  }

  public void testRectBoundariesAreInclusive() throws Exception {
    GeoRect rect = randomRect(random().nextBoolean(), false);
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
    for(int x=0;x<3;x++) {
      double lat;
      if (x == 0) {
        lat = rect.minLat;
      } else if (x == 1) {
        lat = quantizeLat((rect.minLat+rect.maxLat)/2.0);
      } else {
        lat = rect.maxLat;
      }
      for(int y=0;y<3;y++) {
        double lon;
        if (y == 0) {
          lon = rect.minLon;
        } else if (y == 1) {
          if (x == 1) {
            continue;
          }
          lon = quantizeLon((rect.minLon+rect.maxLon)/2.0);
        } else {
          lon = rect.maxLon;
        }

        Document doc = new Document();
        addPointToDoc(FIELD_NAME, doc, lat, lon);
        w.addDocument(doc);
      }
    }
    IndexReader r = w.getReader();
    IndexSearcher s = newSearcher(r, false);
    assertEquals(8, s.count(newRectQuery(FIELD_NAME, rect.minLat, rect.maxLat, rect.minLon, rect.maxLon)));
    r.close();
    w.close();
    dir.close();
  }
  
  /** Run a few iterations with just 10 docs, hopefully easy to debug */
  public void testRandomDistance() throws Exception {
    for (int iters = 0; iters < 100; iters++) {
      doRandomDistanceTest(10, 100);
    }
  }
    
  /** Runs with thousands of docs */
  @Nightly
  public void testRandomDistanceHuge() throws Exception {
    for (int iters = 0; iters < 10; iters++) {
      doRandomDistanceTest(2000, 100);
    }
  }
    
  private void doRandomDistanceTest(int numDocs, int numQueries) throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    int pointsInLeaf = 2 + random().nextInt(4);
    iwc.setCodec(new FilterCodec("Lucene60", TestUtil.getDefaultCodec()) {
      @Override
      public PointsFormat pointsFormat() {
        return new PointsFormat() {
          @Override
          public PointsWriter fieldsWriter(SegmentWriteState writeState) throws IOException {
            return new Lucene60PointsWriter(writeState, pointsInLeaf, BKDWriter.DEFAULT_MAX_MB_SORT_IN_HEAP);
          }
  
          @Override
          public PointsReader fieldsReader(SegmentReadState readState) throws IOException {
            return new Lucene60PointsReader(readState);
          }
        };
      }
    });
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);
  
    for (int i = 0; i < numDocs; i++) {
      double latRaw = -90 + 180.0 * random().nextDouble();
      double lonRaw = -180 + 360.0 * random().nextDouble();
      // pre-normalize up front, so we can just use quantized value for testing and do simple exact comparisons
      double lat = quantizeLat(latRaw);
      double lon = quantizeLon(lonRaw);
      Document doc = new Document();
      addPointToDoc("field", doc, lat, lon);
      doc.add(new StoredField("lat", lat));
      doc.add(new StoredField("lon", lon));
      writer.addDocument(doc);
    }
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);
  
    for (int i = 0; i < numQueries; i++) {
      double lat = -90 + 180.0 * random().nextDouble();
      double lon = -180 + 360.0 * random().nextDouble();
      double radius = 50000000D * random().nextDouble();
  
      BitSet expected = new BitSet();
      for (int doc = 0; doc < reader.maxDoc(); doc++) {
        double docLatitude = reader.document(doc).getField("lat").numericValue().doubleValue();
        double docLongitude = reader.document(doc).getField("lon").numericValue().doubleValue();
        double distance = SloppyMath.haversinMeters(lat, lon, docLatitude, docLongitude);
        if (distance <= radius) {
          expected.set(doc);
        }
      }
  
      TopDocs topDocs = searcher.search(newDistanceQuery("field", lat, lon, radius), reader.maxDoc(), Sort.INDEXORDER);
      BitSet actual = new BitSet();
      for (ScoreDoc doc : topDocs.scoreDocs) {
        actual.set(doc.doc);
      }
      
      try {
        assertEquals(expected, actual);
      } catch (AssertionError e) {
        System.out.println("center: (" + lat + "," + lon + "), radius=" + radius);
        for (int doc = 0; doc < reader.maxDoc(); doc++) {
          double docLatitude = reader.document(doc).getField("lat").numericValue().doubleValue();
          double docLongitude = reader.document(doc).getField("lon").numericValue().doubleValue();
          double distance = SloppyMath.haversinMeters(lat, lon, docLatitude, docLongitude);
          System.out.println("" + doc + ": (" + docLatitude + "," + docLongitude + "), distance=" + distance);
        }
        throw e;
      }
    }
    reader.close();
    writer.close();
    dir.close();
  }

  public void testEquals() throws Exception {   
    Query q1, q2;

    GeoRect rect = randomRect(false, true);

    q1 = newRectQuery("field", rect.minLat, rect.maxLat, rect.minLon, rect.maxLon);
    q2 = newRectQuery("field", rect.minLat, rect.maxLat, rect.minLon, rect.maxLon);
    assertEquals(q1, q2);
    assertFalse(q1.equals(newRectQuery("field2", rect.minLat, rect.maxLat, rect.minLon, rect.maxLon)));

    double lat = randomLat(false);
    double lon = randomLon(false);
    q1 = newDistanceQuery("field", lat, lon, 10000.0);
    q2 = newDistanceQuery("field", lat, lon, 10000.0);
    assertEquals(q1, q2);
    assertFalse(q1.equals(newDistanceQuery("field2", lat, lon, 10000.0)));

    q1 = newDistanceRangeQuery("field", lat, lon, 10000.0, 100000.0);
    if (q1 != null) {
      // Not all subclasses can make distance range query!
      q2 = newDistanceRangeQuery("field", lat, lon, 10000.0, 100000.0);
      assertEquals(q1, q2);
      assertFalse(q1.equals(newDistanceRangeQuery("field2", lat, lon, 10000.0, 100000.0)));
    }

    double[] lats = new double[5];
    double[] lons = new double[5];
    lats[0] = rect.minLat;
    lons[0] = rect.minLon;
    lats[1] = rect.maxLat;
    lons[1] = rect.minLon;
    lats[2] = rect.maxLat;
    lons[2] = rect.maxLon;
    lats[3] = rect.minLat;
    lons[3] = rect.maxLon;
    lats[4] = rect.minLat;
    lons[4] = rect.minLon;
    q1 = newPolygonQuery("field", lats, lons);
    q2 = newPolygonQuery("field", lats, lons);
    assertEquals(q1, q2);
    assertFalse(q1.equals(newPolygonQuery("field2", lats, lons)));
  }
  
  /** return topdocs over a small set of points in field "point" */
  private TopDocs searchSmallSet(Query query, int size) throws Exception {
    // this is a simple systematic test, indexing these points
    double[][] pts = new double[][] {
        { 32.763420,          -96.774             },
        { 32.7559529921407,   -96.7759895324707   },
        { 32.77866942010977,  -96.77701950073242  },
        { 32.7756745755423,   -96.7706036567688   },
        { 27.703618681345585, -139.73458170890808 },
        { 32.94823588839368,  -96.4538113027811   },
        { 33.06047141970814,  -96.65084838867188  },
        { 32.778650,          -96.7772            },
        { -88.56029371730983, -177.23537676036358 },
        { 33.541429799076354, -26.779373834241003 },
        { 26.774024500421728, -77.35379276106497  },
        { -90.0,              -14.796283808944777 },
        { 32.94823588839368,  -178.8538113027811  },
        { 32.94823588839368,  178.8538113027811   },
        { 40.720611,          -73.998776          },
        { -44.5,              -179.5              }
    };
    
    Directory directory = newDirectory();

    RandomIndexWriter writer = new RandomIndexWriter(random(), directory,
            newIndexWriterConfig(new MockAnalyzer(random()))
                    .setMaxBufferedDocs(TestUtil.nextInt(random(), 100, 1000))
                    .setMergePolicy(newLogMergePolicy()));

    for (double p[] : pts) {
        Document doc = new Document();
        addPointToDoc("point", doc, p[0], p[1]);
        writer.addDocument(doc);
    }

    // add explicit multi-valued docs
    for (int i=0; i<pts.length; i+=2) {
      Document doc = new Document();
      addPointToDoc("point", doc, pts[i][0], pts[i][1]);
      addPointToDoc("point", doc, pts[i+1][0], pts[i+1][1]);
      writer.addDocument(doc);
    }

    // index random string documents
    for (int i=0; i<random().nextInt(10); ++i) {
      Document doc = new Document();
      doc.add(new StringField("string", Integer.toString(i), Field.Store.NO));
      writer.addDocument(doc);
    }

    IndexReader reader = writer.getReader();
    writer.close();

    IndexSearcher searcher = newSearcher(reader);
    TopDocs topDocs = searcher.search(query, size);
    reader.close();
    directory.close();
    return topDocs;
  }
  
  public void testSmallSetRect() throws Exception {
    TopDocs td = searchSmallSet(newRectQuery("point", 32.778650, 32.778950, -96.7772, -96.77690000), 5);
    assertEquals(4, td.totalHits);
  }

  public void testSmallSetDateline() throws Exception {
    TopDocs td = searchSmallSet(newRectQuery("point", -45.0, -44.0, 179.0, -179.0), 20);
    assertEquals(2, td.totalHits);
  }

  public void testSmallSetMultiValued() throws Exception {
    TopDocs td = searchSmallSet(newRectQuery("point", 32.7559529921407, 32.7756745755423, -96.4538113027811, -96.7706036567688), 20);
    // 3 single valued docs + 2 multi-valued docs
    assertEquals(5, td.totalHits);
  }
  
  public void testSmallSetWholeMap() throws Exception {
    TopDocs td = searchSmallSet(newRectQuery("point", GeoUtils.MIN_LAT_INCL, GeoUtils.MAX_LAT_INCL, GeoUtils.MIN_LON_INCL, GeoUtils.MAX_LON_INCL), 20);
    assertEquals(24, td.totalHits);
  }
  
  public void testSmallSetPoly() throws Exception {
    TopDocs td = searchSmallSet(newPolygonQuery("point",
        new double[]{33.073130, 32.9942669, 32.938386, 33.0374494,
            33.1369762, 33.1162747, 33.073130, 33.073130},
        new double[]{-96.7682647, -96.8280029, -96.6288757, -96.4929199,
                     -96.6041564, -96.7449188, -96.76826477, -96.7682647}),
        5);
    assertEquals(2, td.totalHits);
  }

  public void testSmallSetPolyWholeMap() throws Exception {
    TopDocs td = searchSmallSet(newPolygonQuery("point",
                      new double[] {GeoUtils.MIN_LAT_INCL, GeoUtils.MAX_LAT_INCL, GeoUtils.MAX_LAT_INCL, GeoUtils.MIN_LAT_INCL, GeoUtils.MIN_LAT_INCL},
                      new double[] {GeoUtils.MIN_LON_INCL, GeoUtils.MIN_LON_INCL, GeoUtils.MAX_LON_INCL, GeoUtils.MAX_LON_INCL, GeoUtils.MIN_LON_INCL}),
                      20);    
    assertEquals("testWholeMap failed", 24, td.totalHits);
  }

  public void testSmallSetDistance() throws Exception {
    TopDocs td = searchSmallSet(newDistanceQuery("point", 32.94823588839368, -96.4538113027811, 6000), 20);
    assertEquals(2, td.totalHits);
  }
  
  public void testSmallSetTinyDistance() throws Exception {
    TopDocs td = searchSmallSet(newDistanceQuery("point", 40.720611, -73.998776, 1), 20);
    assertEquals(2, td.totalHits);
  }

  /** see https://issues.apache.org/jira/browse/LUCENE-6905 */
  public void testSmallSetDistanceNotEmpty() throws Exception {
    TopDocs td = searchSmallSet(newDistanceQuery("point", -88.56029371730983, -177.23537676036358, 7757.999232959935), 20);
    assertEquals(2, td.totalHits);
  }

  /**
   * Explicitly large
   */
  public void testSmallSetHugeDistance() throws Exception {
    TopDocs td = searchSmallSet(newDistanceQuery("point", 32.94823588839368, -96.4538113027811, 6000000), 20);
    assertEquals(16, td.totalHits);
  }

  public void testSmallSetDistanceDateline() throws Exception {
    TopDocs td = searchSmallSet(newDistanceQuery("point", 32.94823588839368, -179.9538113027811, 120000), 20);
    assertEquals(3, td.totalHits);
  }
}
