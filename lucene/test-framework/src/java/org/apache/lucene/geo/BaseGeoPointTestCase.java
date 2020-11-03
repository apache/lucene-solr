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
package org.apache.lucene.geo;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.codecs.PointsWriter;
import org.apache.lucene.codecs.lucene86.Lucene86PointsReader;
import org.apache.lucene.codecs.lucene86.Lucene86PointsWriter;
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
import org.apache.lucene.index.MultiBits;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.SloppyMath;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.bkd.BKDWriter;

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
  
  // TODO: remove these hooks once all subclasses can pass with new random!

  protected double nextLongitude() {
    return org.apache.lucene.geo.GeoTestUtil.nextLongitude();
  }
  
  protected double nextLatitude() {
    return org.apache.lucene.geo.GeoTestUtil.nextLatitude();
  }
  
  protected Rectangle nextBox() {
    return org.apache.lucene.geo.GeoTestUtil.nextBox();
  }

  protected Circle nextCircle() {
    return org.apache.lucene.geo.GeoTestUtil.nextCircle();
  }
  
  protected Polygon nextPolygon() {
    return org.apache.lucene.geo.GeoTestUtil.nextPolygon();
  }

  protected LatLonGeometry[] nextGeometry() {
    final int length = random().nextInt(4) + 1;
    final LatLonGeometry[] geometries = new LatLonGeometry[length];
    for (int i = 0; i < length; i++) {
      final LatLonGeometry geometry;
      switch (random().nextInt(3)) {
        case 0:
          geometry = nextBox();
          break;
        case 1:
          geometry = nextCircle();
          break;
        default:
          geometry = nextPolygon();
          break;
      }
      geometries[i] = geometry;
    }
    return geometries;
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

  /** null field name not allowed */
  public void testBoxNull() {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      newRectQuery(null, 18, 19, -66, -65);
    });
    assertTrue(expected.getMessage().contains("field must not be null"));
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
  
  /** null field name not allowed */
  public void testDistanceNull() {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      newDistanceQuery(null, 18, -65, 50_000);
    });
    assertTrue(expected.getMessage().contains("field must not be null"));
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
    assertEquals(1, searcher.count(newPolygonQuery("field", new Polygon(
                                                   new double[] { 18, 18, 19, 19, 18 },
                                                   new double[] { -66, -65, -65, -66, -66 }))));

    reader.close();
    writer.close();
    dir.close();
  }
  
  /** test we can search for a polygon with a hole (but still includes the doc) */
  public void testPolygonHole() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    // add a doc with a point
    Document document = new Document();
    addPointToDoc("field", document, 18.313694, -65.227444);
    writer.addDocument(document);
    
    // search and verify we found our doc
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);
    Polygon inner = new Polygon(new double[] { 18.5, 18.5, 18.7, 18.7, 18.5 },
                                new double[] { -65.7, -65.4, -65.4, -65.7, -65.7 });
    Polygon outer = new Polygon(new double[] { 18, 18, 19, 19, 18 },
                                new double[] { -66, -65, -65, -66, -66 }, inner);
    assertEquals(1, searcher.count(newPolygonQuery("field", outer)));

    reader.close();
    writer.close();
    dir.close();
  }
  
  /** test we can search for a polygon with a hole (that excludes the doc) */
  public void testPolygonHoleExcludes() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    // add a doc with a point
    Document document = new Document();
    addPointToDoc("field", document, 18.313694, -65.227444);
    writer.addDocument(document);
    
    // search and verify we found our doc
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);
    Polygon inner = new Polygon(new double[] { 18.2, 18.2, 18.4, 18.4, 18.2 },
                                new double[] { -65.3, -65.2, -65.2, -65.3, -65.3 });
    Polygon outer = new Polygon(new double[] { 18, 18, 19, 19, 18 },
                                new double[] { -66, -65, -65, -66, -66 }, inner);
    assertEquals(0, searcher.count(newPolygonQuery("field", outer)));

    reader.close();
    writer.close();
    dir.close();
  }
  
  /** test we can search for a multi-polygon */
  public void testMultiPolygonBasics() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    // add a doc with a point
    Document document = new Document();
    addPointToDoc("field", document, 18.313694, -65.227444);
    writer.addDocument(document);
    
    // search and verify we found our doc
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);
    Polygon a = new Polygon(new double[] { 28, 28, 29, 29, 28 },
                           new double[] { -56, -55, -55, -56, -56 });
    Polygon b = new Polygon(new double[] { 18, 18, 19, 19, 18 },
                            new double[] { -66, -65, -65, -66, -66 });
    assertEquals(1, searcher.count(newPolygonQuery("field", a, b)));

    reader.close();
    writer.close();
    dir.close();
  }
  
  /** null field name not allowed */
  public void testPolygonNullField() {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      newPolygonQuery(null, new Polygon(
          new double[] { 18, 18, 19, 19, 18 },
          new double[] { -66, -65, -65, -66, -66 }));
    });
    assertTrue(expected.getMessage().contains("field must not be null"));
  }

  // A particularly tricky adversary for BKD tree:
  public void testSamePointManyTimes() throws Exception {
    int numPoints = atLeast(1000);

    // Every doc has 2 points:
    double theLat = nextLatitude();
    double theLon = nextLongitude();

    double[] lats = new double[numPoints];
    Arrays.fill(lats, theLat);

    double[] lons = new double[numPoints];
    Arrays.fill(lons, theLon);

    verify(lats, lons);
  }

  // A particularly tricky adversary for BKD tree:
  public void testLowCardinality() throws Exception {
    int numPoints = atLeast(1000);
    int cardinality = TestUtil.nextInt(random(), 2, 20);

    double[] diffLons  = new double[cardinality];
    double[] diffLats = new double[cardinality];
    for (int i = 0; i< cardinality; i++) {
      diffLats[i] = nextLatitude();
      diffLons[i] = nextLongitude();
    }

    double[] lats = new double[numPoints];
    double[] lons = new double[numPoints];
    for (int i = 0; i < numPoints; i++) {
      int index = random().nextInt(cardinality);
      lats[i] = diffLats[index];
      lons[i] = diffLons[index];
    }

    verify(lats, lons);
  }

  public void testAllLatEqual() throws Exception {
    int numPoints = atLeast(1000);
    double lat = nextLatitude();
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
        lons[docID] = nextLongitude();
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
    int numPoints = atLeast(1000);
    double theLon = nextLongitude();
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
        lats[docID] = nextLatitude();
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
    int numPoints = atLeast(1000);
    // Every doc has 2 points:
    double[] lats = new double[2*numPoints];
    double[] lons = new double[2*numPoints];
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();

    // We rely on docID order:
    iwc.setMergePolicy(newLogMergePolicy());
    // and on seeds being able to reproduce:
    iwc.setMergeScheduler(new SerialMergeScheduler());
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);

    for (int id=0;id<numPoints;id++) {
      Document doc = new Document();
      lats[2*id] = quantizeLat(nextLatitude());
      lons[2*id] = quantizeLon(nextLongitude());
      doc.add(newStringField("id", ""+id, Field.Store.YES));
      addPointToDoc(FIELD_NAME, doc, lats[2*id], lons[2*id]);
      lats[2*id+1] = quantizeLat(nextLatitude());
      lons[2*id+1] = quantizeLon(nextLongitude());
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

    IndexSearcher s = newSearcher(r);

    int iters = atLeast(25);
    for (int iter=0;iter<iters;iter++) {
      Rectangle rect = nextBox();

      if (VERBOSE) {
        System.out.println("\nTEST: iter=" + iter + " rect=" + rect);
      }

      Query query = newRectQuery(FIELD_NAME, rect.minLat, rect.maxLat, rect.minLon, rect.maxLon);

      final FixedBitSet hits = searchIndex(s, query, r.maxDoc());
      
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
            System.out.println("TEST: id=" + id + " docID=" + docID + " should match but did not");
          } else {
            System.out.println("TEST: id=" + id + " docID=" + docID + " should not match but did");
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
    doTestRandom(1000);
  }

  @Nightly
  public void testRandomBig() throws Exception {
    assumeFalse("Direct codec can OOME on this test", TestUtil.getDocValuesFormat(FIELD_NAME).equals("Direct"));
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
          lons[id] = nextLongitude();
          if (VERBOSE) {
            System.out.println("  id=" + id + " lat=" + lats[id] + " lon=" + lons[id] + " (same lat as doc=" + oldID + ")");
          }
        } else if (x == 1) {
          // Identical lon to old point
          lats[id] = nextLatitude();
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
        lats[id] = nextLatitude();
        lons[id] = nextLongitude();
        haveRealDoc = true;
        if (VERBOSE) {
          System.out.println("  id=" + id + " lat=" + lats[id] + " lon=" + lons[id]);
        }
      }
    }

    verify(lats, lons);
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

  protected abstract void addPointToDoc(String field, Document doc, double lat, double lon);

  protected abstract Query newRectQuery(String field, double minLat, double maxLat, double minLon, double maxLon);

  protected abstract Query newDistanceQuery(String field, double centerLat, double centerLon, double radiusMeters);

  protected abstract Query newPolygonQuery(String field, Polygon... polygon);

  protected abstract Query newGeometryQuery(String field, LatLonGeometry... geometry);

  static final boolean rectContainsPoint(Rectangle rect, double pointLat, double pointLon) {
    assert Double.isNaN(pointLat) == false;
    
    if (pointLat < rect.minLat || pointLat > rect.maxLat) {
      return false;
    }

    if (rect.minLon <= rect.maxLon) {
      return pointLon >= rect.minLon && pointLon <= rect.maxLon;
    } else {
      // Rect crosses dateline:
      return pointLon <= rect.maxLon || pointLon >= rect.minLon;
    }
  }

  private void verify(double[] lats, double[] lons) throws Exception {
    // quantize each value the same way the index does
    // NaN means missing for the doc!!!!!
    for (int i = 0; i < lats.length; i++) {
      if (!Double.isNaN(lats[i])) {
        lats[i] = quantizeLat(lats[i]);
      }
    }
    for (int i = 0; i < lons.length; i++) {
      if (!Double.isNaN(lons[i])) {
        lons[i] = quantizeLon(lons[i]);
      }
    }
    verifyRandomRectangles(lats, lons);
    verifyRandomDistances(lats, lons);
    verifyRandomPolygons(lats, lons);
    verifyRandomGeometries(lats, lons);
  }

  protected void verifyRandomRectangles(double[] lats, double[] lons) throws Exception {
    IndexWriterConfig iwc = newIndexWriterConfig();
    // Else seeds may not reproduce:
    iwc.setMergeScheduler(new SerialMergeScheduler());
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
    indexPoints(lats, lons, deleted, w);
    
    final IndexReader r = DirectoryReader.open(w);
    w.close();

    IndexSearcher s = newSearcher(r);

    int iters = atLeast(25);

    Bits liveDocs = MultiBits.getLiveDocs(s.getIndexReader());
    int maxDoc = s.getIndexReader().maxDoc();

    for (int iter=0;iter<iters;iter++) {

      if (VERBOSE) {
        System.out.println("\nTEST: iter=" + iter + " s=" + s);
      }
      
      Rectangle rect = nextBox();

      Query query = newRectQuery(FIELD_NAME, rect.minLat, rect.maxLat, rect.minLon, rect.maxLon);

      if (VERBOSE) {
        System.out.println("  query=" + query);
      }

      final FixedBitSet hits = searchIndex(s, query, maxDoc);

      boolean fail = false;
      NumericDocValues docIDToID = MultiDocValues.getNumericValues(r, "id");
      for(int docID=0;docID<maxDoc;docID++) {
        assertEquals(docID, docIDToID.nextDoc());
        int id = (int) docIDToID.longValue();
        boolean expected;
        if (liveDocs != null && liveDocs.get(docID) == false) {
          // document is deleted
          expected = false;
        } else if (Double.isNaN(lats[id])) {
          expected = false;
        } else {
          expected = rectContainsPoint(rect, lats[id], lons[id]);
        }

        if (hits.get(docID) != expected) {
          buildError(docID, expected, id, lats, lons, query, liveDocs, (b) ->  b.append("  rect=").append(rect));
          fail = true;
        }
      }
      if (fail) {
        fail("some hits were wrong");
      }
    }

    IOUtils.close(r, dir);
  }

  protected void verifyRandomDistances(double[] lats, double[] lons) throws Exception {
    IndexWriterConfig iwc = newIndexWriterConfig();
    // Else seeds may not reproduce:
    iwc.setMergeScheduler(new SerialMergeScheduler());
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
    indexPoints(lats, lons, deleted, w);
    
    final IndexReader r = DirectoryReader.open(w);
    w.close();

    IndexSearcher s = newSearcher(r);

    int iters = atLeast(25);

    Bits liveDocs = MultiBits.getLiveDocs(s.getIndexReader());
    int maxDoc = s.getIndexReader().maxDoc();

    for (int iter=0;iter<iters;iter++) {

      if (VERBOSE) {
        System.out.println("\nTEST: iter=" + iter + " s=" + s);
      }

      // Distance
      final double centerLat = nextLatitude();
      final double centerLon = nextLongitude();

      // So the query can cover at most 50% of the earth's surface:
      final double radiusMeters = random().nextDouble() * GeoUtils.EARTH_MEAN_RADIUS_METERS * Math.PI / 2.0 + 1.0;

      if (VERBOSE) {
        final DecimalFormat df = new DecimalFormat("#,###.00", DecimalFormatSymbols.getInstance(Locale.ENGLISH));
        System.out.println("  radiusMeters = " + df.format(radiusMeters));
      }

      Query query = newDistanceQuery(FIELD_NAME, centerLat, centerLon, radiusMeters);

      if (VERBOSE) {
        System.out.println("  query=" + query);
      }

      final FixedBitSet hits = searchIndex(s, query, maxDoc);
      
      boolean fail = false;
      NumericDocValues docIDToID = MultiDocValues.getNumericValues(r, "id");
      for(int docID=0;docID<maxDoc;docID++) {
        assertEquals(docID, docIDToID.nextDoc());
        int id = (int) docIDToID.longValue();
        boolean expected;
        if (liveDocs != null && liveDocs.get(docID) == false) {
          // document is deleted
          expected = false;
        } else if (Double.isNaN(lats[id])) {
          expected = false;
        } else {
          expected = SloppyMath.haversinMeters(centerLat, centerLon, lats[id], lons[id]) <= radiusMeters;
        }

        if (hits.get(docID) != expected) {
          Consumer<StringBuilder> explain = (b) -> {
            if (Double.isNaN(lats[id]) == false) {
              double distanceMeters = SloppyMath.haversinMeters(centerLat, centerLon, lats[id], lons[id]);
              b.append("  centerLat=").append(centerLat).append(" centerLon=").append(centerLon).append(" distanceMeters=").append(distanceMeters).append(" vs radiusMeters=").append(radiusMeters);
            }
          };
          buildError(docID, expected, id, lats, lons, query, liveDocs, explain);
          fail = true;
        }
      }
      if (fail) {
        fail("some hits were wrong");
      }
    }

    IOUtils.close(r, dir);
  }

  protected void verifyRandomPolygons(double[] lats, double[] lons) throws Exception {
    IndexWriterConfig iwc = newIndexWriterConfig();
    // Else seeds may not reproduce:
    iwc.setMergeScheduler(new SerialMergeScheduler());
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
    indexPoints(lats, lons, deleted, w);
    
    final IndexReader r = DirectoryReader.open(w);
    w.close();

    // We can't wrap with "exotic" readers because points needs to work:
    IndexSearcher s = newSearcher(r);

    final int iters = atLeast(75);

    Bits liveDocs = MultiBits.getLiveDocs(s.getIndexReader());
    int maxDoc = s.getIndexReader().maxDoc();

    for (int iter=0;iter<iters;iter++) {

      if (VERBOSE) {
        System.out.println("\nTEST: iter=" + iter + " s=" + s);
      }

      // Polygon
      Polygon polygon = nextPolygon();
      Query query = newPolygonQuery(FIELD_NAME, polygon);

      if (VERBOSE) {
        System.out.println("  query=" + query);
      }

      final FixedBitSet hits = searchIndex(s, query, maxDoc);

      boolean fail = false;
      NumericDocValues docIDToID = MultiDocValues.getNumericValues(r, "id");
      for(int docID=0;docID<maxDoc;docID++) {
        assertEquals(docID, docIDToID.nextDoc());
        int id = (int) docIDToID.longValue();
        boolean expected;
        if (liveDocs != null && liveDocs.get(docID) == false) {
          // document is deleted
          expected = false;
        } else if (Double.isNaN(lats[id])) {
          expected = false;
        } else {
          expected = GeoTestUtil.containsSlowly(polygon, lats[id], lons[id]);
        }

        if (hits.get(docID) != expected) {
          buildError(docID, expected, id, lats, lons, query, liveDocs, (b) ->  b.append("  polygon=").append(polygon));
          fail = true;
        }
      }
      if (fail) {
        fail("some hits were wrong");
      }
    }

    IOUtils.close(r, dir);
  }
  
  protected void verifyRandomGeometries(double[] lats, double[] lons) throws Exception {
    IndexWriterConfig iwc = newIndexWriterConfig();
    // Else seeds may not reproduce:
    iwc.setMergeScheduler(new SerialMergeScheduler());
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
    indexPoints(lats, lons, deleted, w);
    
    final IndexReader r = DirectoryReader.open(w);
    w.close();

    // We can't wrap with "exotic" readers because points needs to work:
    IndexSearcher s = newSearcher(r);

    final int iters = atLeast(75);

    Bits liveDocs = MultiBits.getLiveDocs(s.getIndexReader());
    int maxDoc = s.getIndexReader().maxDoc();

    for (int iter=0;iter<iters;iter++) {

      if (VERBOSE) {
        System.out.println("\nTEST: iter=" + iter + " s=" + s);
      }

      // Polygon
      LatLonGeometry[] geometries = nextGeometry();
      Query query = newGeometryQuery(FIELD_NAME, geometries);

      if (VERBOSE) {
        System.out.println("  query=" + query);
      }

      final FixedBitSet hits = searchIndex(s, query, maxDoc);

      Component2D component2D = LatLonGeometry.create(geometries);

      boolean fail = false;
      NumericDocValues docIDToID = MultiDocValues.getNumericValues(r, "id");
      for(int docID=0;docID<maxDoc;docID++) {
        assertEquals(docID, docIDToID.nextDoc());
        int id = (int) docIDToID.longValue();
        boolean expected;
        if (liveDocs != null && liveDocs.get(docID) == false) {
          // document is deleted
          expected = false;
        } else if (Double.isNaN(lats[id])) {
          expected = false;
        } else {
          expected = component2D.contains(quantizeLon(lons[id]), quantizeLat(lats[id]));
        }

        if (hits.get(docID) != expected) {
          buildError(docID, expected, id, lats, lons, query, liveDocs, (b) ->  b.append("  geometry=").append(Arrays.toString(geometries)));
          fail = true;
        }
      }
      if (fail) {
        fail("some hits were wrong");
      }
    }

    IOUtils.close(r, dir);
  }

  private void indexPoints(double[] lats, double[] lons, Set<Integer> deleted, IndexWriter w) throws IOException {
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
  }

  private FixedBitSet searchIndex(IndexSearcher s, Query query, int maxDoc) throws IOException {
    final FixedBitSet hits = new FixedBitSet(maxDoc);
    s.search(query, new SimpleCollector() {

      private int docBase;

      @Override
      public ScoreMode scoreMode() {
        return ScoreMode.COMPLETE_NO_SCORES;
      }

      @Override
      protected void doSetNextReader(LeafReaderContext context)  {
        docBase = context.docBase;
      }

      @Override
      public void collect(int doc) {
        hits.set(docBase+doc);
      }
    });
    return hits;
  }

  private void buildError(int docID, boolean expected, int id, double[] lats, double[] lons, Query query,
                          Bits liveDocs, Consumer<StringBuilder> explain) {
    StringBuilder b = new StringBuilder();
    if (expected) {
      b.append("FAIL: id=").append(id).append(" should match but did not\n");
    } else {
      b.append("FAIL: id=").append(id).append(" should not match but did\n");
    }
    b.append("  query=").append(query).append(" docID=").append(docID).append("\n");
    b.append("  lat=").append(lats[id]).append(" lon=").append(lons[id]).append("\n");
    b.append("  deleted?=").append(liveDocs != null && liveDocs.get(docID) == false);
    explain.accept(b);
    if (true) {
      fail("wrong hit (first of possibly more):\n\n" + b);
    } else {
      System.out.println(b.toString());
    }
  }

  public void testRectBoundariesAreInclusive() throws Exception {
    Rectangle rect;
    // TODO: why this dateline leniency???
    while (true) {
      rect = nextBox();
      if (rect.crossesDateline() == false) {
        break;
      }
    }
    // this test works in quantized space: for testing inclusiveness of exact edges it must be aware of index-time quantization!
    rect = new Rectangle(quantizeLat(rect.minLat), quantizeLat(rect.maxLat), quantizeLon(rect.minLon), quantizeLon(rect.maxLon));
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    // Else seeds may not reproduce:
    iwc.setMergeScheduler(new SerialMergeScheduler());
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
    // exact edge cases
    assertEquals(8, s.count(newRectQuery(FIELD_NAME, rect.minLat, rect.maxLat, rect.minLon, rect.maxLon)));
    
    // expand 1 ulp in each direction if possible and test a slightly larger box!
    if (rect.minLat != -90) {
      assertEquals(8, s.count(newRectQuery(FIELD_NAME, Math.nextDown(rect.minLat), rect.maxLat, rect.minLon, rect.maxLon)));
    }
    if (rect.maxLat != 90) {
      assertEquals(8, s.count(newRectQuery(FIELD_NAME, rect.minLat, Math.nextUp(rect.maxLat), rect.minLon, rect.maxLon)));
    }
    if (rect.minLon != -180) {
      assertEquals(8, s.count(newRectQuery(FIELD_NAME, rect.minLat, rect.maxLat, Math.nextDown(rect.minLon), rect.maxLon)));
    }
    if (rect.maxLon != 180) {
      assertEquals(8, s.count(newRectQuery(FIELD_NAME, rect.minLat, rect.maxLat, rect.minLon, Math.nextUp(rect.maxLon))));
    }
    
    // now shrink 1 ulp in each direction if possible: it should not include bogus stuff
    // we can't shrink if values are already at extremes, and
    // we can't do this if rectangle is actually a line or we will create a cross-dateline query
    if (rect.minLat != 90 && rect.maxLat != -90 && rect.minLon != 80 && rect.maxLon != -180 && rect.minLon != rect.maxLon) {
      // note we put points on "sides" not just "corners" so we just shrink all 4 at once for now: it should exclude all points!
      assertEquals(0, s.count(newRectQuery(FIELD_NAME, Math.nextUp(rect.minLat), 
                                                     Math.nextDown(rect.maxLat), 
                                                     Math.nextUp(rect.minLon), 
                                                     Math.nextDown(rect.maxLon))));
    }

    r.close();
    w.close();
    dir.close();
  }
  
  /** Run a few iterations with just 10 docs, hopefully easy to debug */
  public void testRandomDistance() throws Exception {
    int numIters = atLeast(1);
    for (int iters = 0; iters < numIters; iters++) {
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
    // Else seeds may not reproduce:
    iwc.setMergeScheduler(new SerialMergeScheduler());
    int pointsInLeaf = 2 + random().nextInt(4);
    final Codec in = TestUtil.getDefaultCodec();
    iwc.setCodec(new FilterCodec(in.getName(), in) {
      @Override
      public PointsFormat pointsFormat() {
        return new PointsFormat() {
          @Override
          public PointsWriter fieldsWriter(SegmentWriteState writeState) throws IOException {
            return new Lucene86PointsWriter(writeState, pointsInLeaf, BKDWriter.DEFAULT_MAX_MB_SORT_IN_HEAP);
          }
  
          @Override
          public PointsReader fieldsReader(SegmentReadState readState) throws IOException {
            return new Lucene86PointsReader(readState);
          }
        };
      }
    });
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);
  
    for (int i = 0; i < numDocs; i++) {
      double latRaw = nextLatitude();
      double lonRaw = nextLongitude();
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
      double lat = nextLatitude();
      double lon = nextLongitude();
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

    Rectangle rect = nextBox();

    q1 = newRectQuery("field", rect.minLat, rect.maxLat, rect.minLon, rect.maxLon);
    q2 = newRectQuery("field", rect.minLat, rect.maxLat, rect.minLon, rect.maxLon);
    assertEquals(q1, q2);
    // for "impossible" ranges LatLonPoint.newBoxQuery will return MatchNoDocsQuery
    // changing the field is unrelated to that.
    if (q1 instanceof MatchNoDocsQuery == false) {
      assertFalse(q1.equals(newRectQuery("field2", rect.minLat, rect.maxLat, rect.minLon, rect.maxLon)));
    }

    double lat = nextLatitude();
    double lon = nextLongitude();
    q1 = newDistanceQuery("field", lat, lon, 10000.0);
    q2 = newDistanceQuery("field", lat, lon, 10000.0);
    assertEquals(q1, q2);
    assertFalse(q1.equals(newDistanceQuery("field2", lat, lon, 10000.0)));

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
    q1 = newPolygonQuery("field", new Polygon(lats, lons));
    q2 = newPolygonQuery("field", new Polygon(lats, lons));
    assertEquals(q1, q2);
    assertFalse(q1.equals(newPolygonQuery("field2", new Polygon(lats, lons))));
  }
  
  /** return topdocs over a small set of points in field "point" */
  private TopDocs searchSmallSet(Query query, int size) throws Exception {
    // this is a simple systematic test, indexing these points
    // TODO: fragile: does not understand quantization in any way yet uses extremely high precision!
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

    // TODO: must these simple tests really rely on docid order?
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    iwc.setMaxBufferedDocs(TestUtil.nextInt(random(), 100, 1000));
    iwc.setMergePolicy(newLogMergePolicy());
    // Else seeds may not reproduce:
    iwc.setMergeScheduler(new SerialMergeScheduler());
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory, iwc);

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
    TopDocs td = searchSmallSet(newRectQuery("point", 32.778, 32.779, -96.778, -96.777), 5);
    assertEquals(4, td.totalHits.value);
  }

  public void testSmallSetDateline() throws Exception {
    TopDocs td = searchSmallSet(newRectQuery("point", -45.0, -44.0, 179.0, -179.0), 20);
    assertEquals(2, td.totalHits.value);
  }

  public void testSmallSetMultiValued() throws Exception {
    TopDocs td = searchSmallSet(newRectQuery("point", 32.755, 32.776, -96.454, -96.770), 20);
    // 3 single valued docs + 2 multi-valued docs
    assertEquals(5, td.totalHits.value);
  }
  
  public void testSmallSetWholeMap() throws Exception {
    TopDocs td = searchSmallSet(newRectQuery("point", GeoUtils.MIN_LAT_INCL, GeoUtils.MAX_LAT_INCL, GeoUtils.MIN_LON_INCL, GeoUtils.MAX_LON_INCL), 20);
    assertEquals(24, td.totalHits.value);
  }
  
  public void testSmallSetPoly() throws Exception {
    TopDocs td = searchSmallSet(newPolygonQuery("point",
        new Polygon(
        new double[]{33.073130, 32.9942669, 32.938386, 33.0374494,
            33.1369762, 33.1162747, 33.073130, 33.073130},
        new double[]{-96.7682647, -96.8280029, -96.6288757, -96.4929199,
                     -96.6041564, -96.7449188, -96.76826477, -96.7682647})),
        5);
    assertEquals(2, td.totalHits.value);
  }

  public void testSmallSetPolyWholeMap() throws Exception {
    TopDocs td = searchSmallSet(newPolygonQuery("point",
                      new Polygon(
                      new double[] {GeoUtils.MIN_LAT_INCL, GeoUtils.MAX_LAT_INCL, GeoUtils.MAX_LAT_INCL, GeoUtils.MIN_LAT_INCL, GeoUtils.MIN_LAT_INCL},
                      new double[] {GeoUtils.MIN_LON_INCL, GeoUtils.MIN_LON_INCL, GeoUtils.MAX_LON_INCL, GeoUtils.MAX_LON_INCL, GeoUtils.MIN_LON_INCL})),
                      20);    
    assertEquals("testWholeMap failed", 24, td.totalHits.value);
  }

  public void testSmallSetDistance() throws Exception {
    TopDocs td = searchSmallSet(newDistanceQuery("point", 32.94823588839368, -96.4538113027811, 6000), 20);
    assertEquals(2, td.totalHits.value);
  }
  
  public void testSmallSetTinyDistance() throws Exception {
    TopDocs td = searchSmallSet(newDistanceQuery("point", 40.720611, -73.998776, 1), 20);
    assertEquals(2, td.totalHits.value);
  }

  /** see https://issues.apache.org/jira/browse/LUCENE-6905 */
  public void testSmallSetDistanceNotEmpty() throws Exception {
    TopDocs td = searchSmallSet(newDistanceQuery("point", -88.56029371730983, -177.23537676036358, 7757.999232959935), 20);
    assertEquals(2, td.totalHits.value);
  }

  /**
   * Explicitly large
   */
  public void testSmallSetHugeDistance() throws Exception {
    TopDocs td = searchSmallSet(newDistanceQuery("point", 32.94823588839368, -96.4538113027811, 6000000), 20);
    assertEquals(16, td.totalHits.value);
  }

  public void testSmallSetDistanceDateline() throws Exception {
    TopDocs td = searchSmallSet(newDistanceQuery("point", 32.94823588839368, -179.9538113027811, 120000), 20);
    assertEquals(3, td.totalHits.value);
  }
}
