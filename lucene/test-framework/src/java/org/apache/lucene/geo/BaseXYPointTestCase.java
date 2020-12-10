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
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.bkd.BKDWriter;

/**
 * Abstract class to do basic tests for a xy spatial impl (high level
 * fields and queries) */
public abstract class BaseXYPointTestCase extends LuceneTestCase {

  protected static final String FIELD_NAME = "point";

  // TODO: remove these hooks once all subclasses can pass with new random!

  protected float nextX() {
    return ShapeTestUtil.nextFloat(random());
  }

  protected float nextY() {
    return ShapeTestUtil.nextFloat(random());
  }

  protected XYRectangle nextBox() {
    return ShapeTestUtil.nextBox(random());
  }

  protected XYPolygon nextPolygon() {
    return ShapeTestUtil.nextPolygon();
  }

  protected XYGeometry[] nextGeometry() {
    final int len = random().nextInt(4) + 1;
    XYGeometry[] geometries = new XYGeometry[len];
    for (int i = 0; i < len; i++) {
      switch (random().nextInt(3)) {
        case 0:
          geometries[i] = new XYPoint(nextX(), nextY());
          break;
        case 1:
          geometries[i] = nextBox();
          break;
        default:
          geometries[i] = nextPolygon();
          break;
      }
    }
    return geometries;
  }

  /** Valid values that should not cause exception */
  public void testIndexExtremeValues() {
    Document document = new Document();
    addPointToDoc("foo", document, Float.MAX_VALUE, Float.MAX_VALUE);
    addPointToDoc("foo", document, Float.MAX_VALUE, -Float.MAX_VALUE);
    addPointToDoc("foo", document, -Float.MAX_VALUE, Float.MAX_VALUE);
    addPointToDoc("foo", document, -Float.MAX_VALUE, -Float.MAX_VALUE);
  }
  
  /** NaN: illegal */
  public void testIndexNaNValues() {
    Document document = new Document();
    IllegalArgumentException expected;

    expected = expectThrows(IllegalArgumentException.class, () -> {
      addPointToDoc("foo", document, Float.NaN, 50.0f);
    });
    assertTrue(expected.getMessage().contains("invalid value"));
    
    expected = expectThrows(IllegalArgumentException.class, () -> {
      addPointToDoc("foo", document, 50.0f, Float.NaN);
    });
    assertTrue(expected.getMessage().contains("invalid value"));
  }
  
  /** Inf: illegal */
  public void testIndexInfValues() {
    Document document = new Document();
    IllegalArgumentException expected;

    expected = expectThrows(IllegalArgumentException.class, () -> {
      addPointToDoc("foo", document, Float.POSITIVE_INFINITY, 0.0f);
    });
    assertTrue(expected.getMessage().contains("invalid value"));

    expected = expectThrows(IllegalArgumentException.class, () -> {
      addPointToDoc("foo", document, Float.NEGATIVE_INFINITY, 0.0f);
    });
    assertTrue(expected.getMessage().contains("invalid value"));

    expected = expectThrows(IllegalArgumentException.class, () -> {
      addPointToDoc("foo", document, 0.0f, Float.POSITIVE_INFINITY);
    });
    assertTrue(expected.getMessage().contains("invalid value"));

    expected = expectThrows(IllegalArgumentException.class, () -> {
      addPointToDoc("foo", document, 0.0f, Float.NEGATIVE_INFINITY);
    });
    assertTrue(expected.getMessage().contains("invalid value"));
  }
  
  /** Add a single point and search for it in a box */
  public void testBoxBasics() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    // add a doc with a point
    Document document = new Document();
    addPointToDoc("field", document, 18.313694f, -65.227444f);
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

  // box should not accept invalid x/y
  public void testBoxInvalidCoordinates() throws Exception {
    expectThrows(Exception.class, () -> {
      newRectQuery("field", Float.NaN, Float.NaN,Float.NaN, Float.NaN);
    });
  }

  /** test we can search for a point */
  public void testDistanceBasics() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    // add a doc with a location
    Document document = new Document();
    addPointToDoc("field", document, 18.313694f, -65.227444f);
    writer.addDocument(document);
    
    // search within 50km and verify we found our doc
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);
    assertEquals(1, searcher.count(newDistanceQuery("field", 18, -65, 20)));

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
  
  /** distance query should not accept invalid x/y as origin */
  public void testDistanceIllegal() throws Exception {
    expectThrows(Exception.class, () -> {
      newDistanceQuery("field", Float.NaN, Float.NaN, 120000);
    });
  }

  /** negative distance queries are not allowed */
  public void testDistanceNegative() {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      newDistanceQuery("field", 18, 19, -1);
    });
    assertTrue(expected.getMessage().contains("radius"));
  }
  
  /** NaN distance queries are not allowed */
  public void testDistanceNaN() {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      newDistanceQuery("field", 18, 19, Float.NaN);
    });
    assertTrue(expected.getMessage().contains("radius"));
    assertTrue(expected.getMessage().contains("NaN"));
  }
  
  /** Inf distance queries are not allowed */
  public void testDistanceInf() {
    IllegalArgumentException expected;
    
    expected = expectThrows(IllegalArgumentException.class, () -> {
      newDistanceQuery("field", 18, 19, Float.POSITIVE_INFINITY);
    });
    assertTrue(expected.getMessage(), expected.getMessage().contains("radius"));
    assertTrue(expected.getMessage(), expected.getMessage().contains("finite"));
    
    expected = expectThrows(IllegalArgumentException.class, () -> {
      newDistanceQuery("field", 18, 19, Float.NEGATIVE_INFINITY);
    });
    assertTrue(expected.getMessage(), expected.getMessage().contains("radius"));
    assertTrue(expected.getMessage(), expected.getMessage().contains("bigger than 0"));
  }
  
  /** test we can search for a polygon */
  public void testPolygonBasics() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    // add a doc with a point
    Document document = new Document();
    addPointToDoc("field", document, 18.313694f, -65.227444f);
    writer.addDocument(document);
    
    // search and verify we found our doc
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);
    assertEquals(1, searcher.count(newPolygonQuery("field", new XYPolygon(
                                                   new float[] { 18, 18, 19, 19, 18 },
                                                   new float[] { -66, -65, -65, -66, -66 }))));

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
    addPointToDoc("field", document, 18.313694f, -65.227444f);
    writer.addDocument(document);
    
    // search and verify we found our doc
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);
    XYPolygon inner = new XYPolygon(new float[] { 18.5f, 18.5f, 18.7f, 18.7f, 18.5f },
                                new float[] { -65.7f, -65.4f, -65.4f, -65.7f, -65.7f });
    XYPolygon outer = new XYPolygon(new float[] { 18, 18, 19, 19, 18 },
                                new float[] { -66, -65, -65, -66, -66 }, inner);
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
    addPointToDoc("field", document, 18.313694f, -65.227444f);
    writer.addDocument(document);
    
    // search and verify we found our doc
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);
    XYPolygon inner = new XYPolygon(new float[] { 18.2f, 18.2f, 18.4f, 18.4f, 18.2f },
                                new float[] { -65.3f, -65.2f, -65.2f, -65.3f, -65.3f });
    XYPolygon outer = new XYPolygon(new float[] { 18, 18, 19, 19, 18 },
                                new float[] { -66, -65, -65, -66, -66 }, inner);
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
    addPointToDoc("field", document, 18.313694f, -65.227444f);
    writer.addDocument(document);
    
    // search and verify we found our doc
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);
    XYPolygon a = new XYPolygon(new float[] { 28, 28, 29, 29, 28 },
                           new float[] { -56, -55, -55, -56, -56 });
    XYPolygon b = new XYPolygon(new float[] { 18, 18, 19, 19, 18 },
                            new float[] { -66, -65, -65, -66, -66 });
    assertEquals(1, searcher.count(newPolygonQuery("field", a, b)));

    reader.close();
    writer.close();
    dir.close();
  }
  
  /** null field name not allowed */
  public void testPolygonNullField() {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      newPolygonQuery(null, new XYPolygon(
          new float[] { 18, 18, 19, 19, 18 },
          new float[] { -66, -65, -65, -66, -66 }));
    });
    assertTrue(expected.getMessage().contains("field must not be null"));
  }

  // A particularly tricky adversary for BKD tree:
  public void testSamePointManyTimes() throws Exception {
    int numPoints = atLeast(1000);

    // Every doc has 2 points:
    float theX = nextX();
    float theY = nextY();

    float[] xs = new float[numPoints];
    Arrays.fill(xs, theX);

    float[] ys = new float[numPoints];
    Arrays.fill(ys, theY);

    verify(xs, ys);
  }

  // A particularly tricky adversary for BKD tree:
  public void testLowCardinality() throws Exception {
    int numPoints = atLeast(1000);
    int cardinality = TestUtil.nextInt(random(), 2, 20);

    float[] diffXs  = new float[cardinality];
    float[] diffYs = new float[cardinality];
    for (int i = 0; i< cardinality; i++) {
      diffXs[i] = nextX();
      diffYs[i] = nextY();
    }

    float[] xs = new float[numPoints];
    float[] ys = new float[numPoints];
    for (int i = 0; i < numPoints; i++) {
      int index = random().nextInt(cardinality);
      xs[i] = diffXs[index];
      ys[i] = diffYs[index];
    }
    verify(xs, ys);
  }

  public void testAllYEqual() throws Exception {
    int numPoints = atLeast(1000);
    float y = nextY();
    float[] xs = new float[numPoints];
    float[] ys = new float[numPoints];

    boolean haveRealDoc = false;

    for(int docID=0;docID<numPoints;docID++) {
      int x = random().nextInt(20);
      if (x == 17) {
        // Some docs don't have a point:
        ys[docID] = Float.NaN;
        if (VERBOSE) {
          System.out.println("  doc=" + docID + " is missing");
        }
        continue;
      }

      if (docID > 0 && x == 14 && haveRealDoc) {
        int oldDocID;
        while (true) {
          oldDocID = random().nextInt(docID);
          if (Float.isNaN(ys[oldDocID]) == false) {
            break;
          }
        }
            
        // Fully identical point:
        ys[docID] = xs[oldDocID];
        if (VERBOSE) {
          System.out.println("  doc=" + docID + " y=" + y + " x=" + xs[docID] + " (same x/y as doc=" + oldDocID + ")");
        }
      } else {
        xs[docID] = nextX();
        haveRealDoc = true;
        if (VERBOSE) {
          System.out.println("  doc=" + docID + " y=" + y + " x=" + xs[docID]);
        }
      }
      ys[docID] = y;
    }

    verify(xs, ys);
  }

  public void testAllXEqual() throws Exception {
    int numPoints = atLeast(1000);
    float theX = nextX();
    float[] xs = new float[numPoints];
    float[] ys = new float[numPoints];

    boolean haveRealDoc = false;

    for(int docID=0;docID<numPoints;docID++) {
      int x = random().nextInt(20);
      if (x == 17) {
        // Some docs don't have a point:
        ys[docID] = Float.NaN;
        if (VERBOSE) {
          System.out.println("  doc=" + docID + " is missing");
        }
        continue;
      }

      if (docID > 0 && x == 14 && haveRealDoc) {
        int oldDocID;
        while (true) {
          oldDocID = random().nextInt(docID);
          if (Float.isNaN(ys[oldDocID]) == false) {
            break;
          }
        }
            
        // Fully identical point:
        ys[docID] = ys[oldDocID];
        if (VERBOSE) {
          System.out.println("  doc=" + docID + " y=" + ys[docID] + " x=" + theX + " (same X/y as doc=" + oldDocID + ")");
        }
      } else {
        ys[docID] = nextY();
        haveRealDoc = true;
        if (VERBOSE) {
          System.out.println("  doc=" + docID + " y=" + ys[docID] + " x=" + theX);
        }
      }
      xs[docID] = theX;
    }

    verify(xs, ys);
  }

  public void testMultiValued() throws Exception {
    int numPoints = atLeast(1000);
    // Every doc has 2 points:
    float[] xs = new float[2*numPoints];
    float[] ys = new float[2*numPoints];
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();

    // We rely on docID order:
    iwc.setMergePolicy(newLogMergePolicy());
    // and on seeds being able to reproduce:
    iwc.setMergeScheduler(new SerialMergeScheduler());
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);

    for (int id=0;id<numPoints;id++) {
      Document doc = new Document();
      xs[2*id] = nextX();
      ys[2*id] = nextY();
      doc.add(newStringField("id", ""+id, Field.Store.YES));
      addPointToDoc(FIELD_NAME, doc, xs[2*id], ys[2*id]);
      xs[2*id+1] = nextX();
      ys[2*id+1] = nextY();
      addPointToDoc(FIELD_NAME, doc, xs[2*id+1], ys[2*id+1]);

      if (VERBOSE) {
        System.out.println("id=" + id);
        System.out.println("  x=" + xs[2*id] + " y=" + ys[2*id]);
        System.out.println("  x=" + xs[2*id+1] + " y=" + ys[2*id+1]);
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
      XYRectangle rect = nextBox();

      if (VERBOSE) {
        System.out.println("\nTEST: iter=" + iter + " rect=" + rect);
      }

      Query query = newRectQuery(FIELD_NAME, rect.minX, rect.maxX, rect.minY, rect.maxY);

      final FixedBitSet hits = searchIndex(s, query, r.maxDoc());

      boolean fail = false;

      for(int docID=0;docID<ys.length/2;docID++) {
        float yDoc1 = ys[2*docID];
        float xDoc1 = xs[2*docID];
        float yDoc2 = ys[2*docID+1];
        float xDoc2 = xs[2*docID+1];

        boolean result1 = rectContainsPoint(rect, xDoc1, yDoc1);
        boolean result2 = rectContainsPoint(rect, xDoc2, yDoc2);

        boolean expected = result1 || result2;

        if (hits.get(docID) != expected) {
          String id = s.doc(docID).get("id");
          if (expected) {
            System.out.println("TEST: id=" + id + " docID=" + docID + " should match but did not");
          } else {
            System.out.println("TEST: id=" + id + " docID=" + docID + " should not match but did");
          }
          System.out.println("  rect=" + rect);
          System.out.println("  x=" + xDoc1 + " y=" + yDoc1 + "\n  x=" + xDoc2 + " y" + yDoc2);
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

    float[] xs = new float[numPoints];
    float[] ys = new float[numPoints];

    boolean haveRealDoc = false;

    for (int id=0;id<numPoints;id++) {
      int x = random().nextInt(20);
      if (x == 17) {
        // Some docs don't have a point:
        ys[id] = Float.NaN;
        if (VERBOSE) {
          System.out.println("  id=" + id + " is missing");
        }
        continue;
      }

      if (id > 0 && x < 3 && haveRealDoc) {
        int oldID;
        while (true) {
          oldID = random().nextInt(id);
          if (Float.isNaN(ys[oldID]) == false) {
            break;
          }
        }
            
        if (x == 0) {
          // Identical x to old point
          ys[id] = ys[oldID];
          xs[id] = nextX();
          if (VERBOSE) {
            System.out.println("  id=" + id + " x=" + xs[id] + " y=" + ys[id] + " (same x as doc=" + oldID + ")");
          }
        } else if (x == 1) {
          // Identical y to old point
          ys[id] = nextY();
          xs[id] = xs[oldID];
          if (VERBOSE) {
            System.out.println("  id=" + id + " x=" + xs[id] + " y=" + ys[id] + " (same y as doc=" + oldID + ")");
          }
        } else {
          assert x == 2;
          // Fully identical point:
          xs[id] = xs[oldID];
          ys[id] = ys[oldID];
          if (VERBOSE) {
            System.out.println("  id=" + id + " x=" + xs[id] + " y=" + ys[id] + " (same X/y as doc=" + oldID + ")");
          }
        }
      } else {
        xs[id] = nextX();
        ys[id] = nextY();
        haveRealDoc = true;
        if (VERBOSE) {
          System.out.println("  id=" + id + " x=" + xs[id] + " y=" + ys[id]);
        }
      }
    }

    verify(xs, ys);
  }


  protected abstract void addPointToDoc(String field, Document doc, float x, float y);

  protected abstract Query newRectQuery(String field, float minX, float maxX, float minY, float maxY);

  protected abstract Query newDistanceQuery(String field, float centerX, float centerY, float radius);

  protected abstract Query newPolygonQuery(String field, XYPolygon... polygon);

  protected abstract Query newGeometryQuery(String field, XYGeometry... geometries);

  static final boolean rectContainsPoint(XYRectangle rect, double x, double y) {
    if (y < rect.minY || y > rect.maxY) {
      return false;
    }
    return x >= rect.minX && x <= rect.maxX;
  }

  static double cartesianDistance(double x1, double y1, double x2, double y2) {
    final double diffX = x1 - x2;
    final double diffY = y1 - y2;
    return Math.sqrt(diffX * diffX + diffY * diffY);
  }

  private void verify(float[] xs, float[] ys) throws Exception {
    // NaN means missing for the doc!!!!!
    verifyRandomRectangles(xs, ys);
    verifyRandomDistances(xs, ys);
    verifyRandomPolygons(xs, ys);
    verifyRandomGeometries(xs, ys);
  }

  protected void verifyRandomRectangles(float[] xs, float[] ys) throws Exception {
    IndexWriterConfig iwc = newIndexWriterConfig();
    // Else seeds may not reproduce:
    iwc.setMergeScheduler(new SerialMergeScheduler());
    // Else we can get O(N^2) merging:
    int mbd = iwc.getMaxBufferedDocs();
    if (mbd != -1 && mbd < xs.length/100) {
      iwc.setMaxBufferedDocs(xs.length/100);
    }
    Directory dir;
    if (xs.length > 100000) {
      dir = newFSDirectory(createTempDir(getClass().getSimpleName()));
    } else {
      dir = newDirectory();
    }

    Set<Integer> deleted = new HashSet<>();
    // RandomIndexWriter is too slow here:
    IndexWriter w = new IndexWriter(dir, iwc);
    indexPoints(xs, ys, deleted, w);
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
      
      XYRectangle rect = nextBox();

      Query query = newRectQuery(FIELD_NAME, rect.minX, rect.maxX, rect.minY, rect.maxY);

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
        } else if (Float.isNaN(xs[id]) || Float.isNaN(ys[id])) {
          expected = false;
        } else {
          expected = rectContainsPoint(rect, xs[id], ys[id]);
        }

        if (hits.get(docID) != expected) {
          buildError(docID, expected, id, xs, ys, query, liveDocs, (b) -> b.append("  rect=").append(rect));
          fail = true;
        }
      }
      if (fail) {
        fail("some hits were wrong");
      }
    }

    IOUtils.close(r, dir);
  }

  protected void verifyRandomDistances(float[] xs, float[] ys) throws Exception {
    IndexWriterConfig iwc = newIndexWriterConfig();
    // Else seeds may not reproduce:
    iwc.setMergeScheduler(new SerialMergeScheduler());
    // Else we can get O(N^2) merging:
    int mbd = iwc.getMaxBufferedDocs();
    if (mbd != -1 && mbd < xs.length/100) {
      iwc.setMaxBufferedDocs(xs.length/100);
    }
    Directory dir;
    if (xs.length > 100000) {
      dir = newFSDirectory(createTempDir(getClass().getSimpleName()));
    } else {
      dir = newDirectory();
    }

    Set<Integer> deleted = new HashSet<>();
    // RandomIndexWriter is too slow here:
    IndexWriter w = new IndexWriter(dir, iwc);
    indexPoints(xs, ys, deleted, w);
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
      final float centerX = nextX();
      final float centerY = nextY();

      // So the query can cover at most 50% of the cartesian space:
      final float radius = random().nextFloat() * Float.MAX_VALUE / 2;

      if (VERBOSE) {
        final DecimalFormat df = new DecimalFormat("#,###.00", DecimalFormatSymbols.getInstance(Locale.ENGLISH));
        System.out.println("  radius = " + df.format(radius));
      }

      Query query = newDistanceQuery(FIELD_NAME, centerX, centerY, radius);

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
        } else if (Float.isNaN(xs[id]) || Float.isNaN(ys[id])) {
          expected = false;
        } else {
          expected = cartesianDistance(centerX, centerY, xs[id], ys[id]) <= radius;
        }

        if (hits.get(docID) != expected) {
          Consumer<StringBuilder> explain = (b) -> {
            if (Double.isNaN(xs[id]) == false) {
              double distance = cartesianDistance(centerX, centerY, xs[id], ys[id]);
              b.append("  centerX=").append(centerX).append(" centerY=").append(centerY).append(" distance=")
                      .append(distance).append(" vs radius=").append(radius);
            }
          };
          buildError(docID, expected, id, xs, ys, query, liveDocs, explain);
          fail = true;
        }
      }
      if (fail) {
        fail("some hits were wrong");
      }
    }

    IOUtils.close(r, dir);
  }

  protected void verifyRandomPolygons(float[] xs, float[] ys) throws Exception {
    IndexWriterConfig iwc = newIndexWriterConfig();
    // Else seeds may not reproduce:
    iwc.setMergeScheduler(new SerialMergeScheduler());
    // Else we can get O(N^2) merging:
    int mbd = iwc.getMaxBufferedDocs();
    if (mbd != -1 && mbd < xs.length/100) {
      iwc.setMaxBufferedDocs(xs.length/100);
    }
    Directory dir;
    if (xs.length > 100000) {
      dir = newFSDirectory(createTempDir(getClass().getSimpleName()));
    } else {
      dir = newDirectory();
    }

    Set<Integer> deleted = new HashSet<>();
    // RandomIndexWriter is too slow here:
    IndexWriter w = new IndexWriter(dir, iwc);
    indexPoints(xs, ys, deleted, w);
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
      XYPolygon polygon = nextPolygon();
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
        } else if (Float.isNaN(xs[id]) || Float.isNaN(ys[id])) {
          expected = false;
        } else {
          expected = ShapeTestUtil.containsSlowly(polygon, xs[id], ys[id]);
        }

        if (hits.get(docID) != expected) {
          buildError(docID, expected, id, xs, ys, query, liveDocs, (b) ->  b.append("  polygon=").append(polygon));
          fail = true;
        }
      }
      if (fail) {
        fail("some hits were wrong");
      }
    }

    IOUtils.close(r, dir);
  }

  protected void verifyRandomGeometries(float[] xs, float[] ys) throws Exception {
    IndexWriterConfig iwc = newIndexWriterConfig();
    // Else seeds may not reproduce:
    iwc.setMergeScheduler(new SerialMergeScheduler());
    // Else we can get O(N^2) merging:
    int mbd = iwc.getMaxBufferedDocs();
    if (mbd != -1 && mbd < xs.length/100) {
      iwc.setMaxBufferedDocs(xs.length/100);
    }
    Directory dir;
    if (xs.length > 100000) {
      dir = newFSDirectory(createTempDir(getClass().getSimpleName()));
    } else {
      dir = newDirectory();
    }

    Set<Integer> deleted = new HashSet<>();
    // RandomIndexWriter is too slow here:
    IndexWriter w = new IndexWriter(dir, iwc);
    indexPoints(xs, ys, deleted, w);
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

      // geometries
      XYGeometry[] geometries = nextGeometry();
      Query query = newGeometryQuery(FIELD_NAME, geometries);
      Component2D component2D = XYGeometry.create(geometries);

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
        } else if (Float.isNaN(xs[id]) || Float.isNaN(ys[id])) {
          expected = false;
        } else {
          expected = component2D.contains(xs[id], ys[id]);
        }

        if (hits.get(docID) != expected) {
          buildError(docID, expected, id, xs, ys, query, liveDocs, (b) ->  b.append("  geometry=").append(Arrays.toString(geometries)));
          fail = true;
        }
      }
      if (fail) {
        fail("some hits were wrong");
      }
    }

    IOUtils.close(r, dir);
  }

  private void indexPoints(float[] xs, float[] ys, Set<Integer> deleted, IndexWriter w) throws IOException {
    for(int id=0;id<xs.length;id++) {
      Document doc = new Document();
      doc.add(newStringField("id", ""+id, Field.Store.NO));
      doc.add(new NumericDocValuesField("id", id));
      if (Float.isNaN(xs[id]) == false && Float.isNaN(ys[id]) == false) {
        addPointToDoc(FIELD_NAME, doc, xs[id], ys[id]);
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

  private void buildError(int docID, boolean expected, int id, float[] xs, float[] ys, Query query,
                          Bits liveDocs, Consumer<StringBuilder> explain) {
    StringBuilder b = new StringBuilder();
    if (expected) {
      b.append("FAIL: id=").append(id).append(" should match but did not\n");
    } else {
      b.append("FAIL: id=").append(id).append(" should not match but did\n");
    }
    b.append("  query=").append(query).append(" docID=").append(docID).append("\n");
    b.append("  x=").append(xs[id]).append(" y=").append(ys[id]).append("\n");
    b.append("  deleted?=").append(liveDocs != null && liveDocs.get(docID) == false);
    explain.accept(b);
    if (true) {
      fail("wrong hit (first of possibly more):\n\n" + b);
    } else {
      System.out.println(b.toString());
    }
  }

  public void testRectBoundariesAreInclusive() throws Exception {
    XYRectangle rect = ShapeTestUtil.nextBox(random());
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    // Else seeds may not reproduce:
    iwc.setMergeScheduler(new SerialMergeScheduler());
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
    for(int i = 0; i < 3; i++) {
      float y;
      if (i == 0) {
        y = rect.minY;
      } else if (i == 1) {
        y = (float) (((double) rect.minY + rect.maxY) / 2.0);
      } else {
        y = rect.maxY;
      }
      for(int j = 0; j < 3; j++) {
        float x;
        if (j == 0) {
          x = rect.minX;
        } else if (j == 1) {
          if (i == 1) {
            continue;
          }
          x = (float) (((double) rect.minX + rect.maxX) / 2.0);
        } else {
          x = rect.maxX;
        }

        Document doc = new Document();
        addPointToDoc(FIELD_NAME, doc, x, y);
        w.addDocument(doc);
      }
    }
    IndexReader r = w.getReader();
    IndexSearcher s = newSearcher(r, false);
    // exact edge cases
    assertEquals(8, s.count(newRectQuery(FIELD_NAME, rect.minX, rect.maxX, rect.minY, rect.maxY)));
    // expand 1 ulp in each direction if possible and test a slightly larger box!
    if (rect.minX != -Float.MAX_VALUE) {
      assertEquals(8, s.count(newRectQuery(FIELD_NAME, Math.nextDown(rect.minX), rect.maxX, rect.minY, rect.maxY)));
    }
    if (rect.maxX != Float.MAX_VALUE) {
      assertEquals(8, s.count(newRectQuery(FIELD_NAME, rect.minX, Math.nextUp(rect.maxX), rect.minY, rect.maxY)));
    }
    if (rect.minY != -Float.MAX_VALUE) {
      assertEquals(8, s.count(newRectQuery(FIELD_NAME, rect.minX, rect.maxX, Math.nextDown(rect.minY), rect.maxY)));
    }
    if (rect.maxY != Float.MAX_VALUE) {
      assertEquals(8, s.count(newRectQuery(FIELD_NAME, rect.minX, rect.maxX, rect.minY, Math.nextUp(rect.maxY))));
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
    Codec in = TestUtil.getDefaultCodec();
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
      float x = nextX();
      float y = nextY();
      // pre-normalize up front, so we can just use quantized value for testing and do simple exact comparisons

      Document doc = new Document();
      addPointToDoc("field", doc, x, y);
      doc.add(new StoredField("x", x));
      doc.add(new StoredField("y", y));
      writer.addDocument(doc);
    }
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);

    for (int i = 0; i < numQueries; i++) {
      float x = nextX();
      float y = nextY();
      float radius = (Float.MAX_VALUE / 2) * random().nextFloat();

      BitSet expected = new BitSet();
      for (int doc = 0; doc < reader.maxDoc(); doc++) {
        float docX = reader.document(doc).getField("x").numericValue().floatValue();
        float docY = reader.document(doc).getField("y").numericValue().floatValue();
        double distance = cartesianDistance(x, y, docX, docY);
        if (distance <= radius) {
          expected.set(doc);
        }
      }

      TopDocs topDocs = searcher.search(newDistanceQuery("field", x, y, radius), reader.maxDoc(), Sort.INDEXORDER);
      BitSet actual = new BitSet();
      for (ScoreDoc doc : topDocs.scoreDocs) {
        actual.set(doc.doc);
      }

      try {
        assertEquals(expected, actual);
      } catch (AssertionError e) {
        System.out.println("center: (" + x + "," + y + "), radius=" + radius);
        for (int doc = 0; doc < reader.maxDoc(); doc++) {
          float docX = reader.document(doc).getField("x").numericValue().floatValue();
          float docY = reader.document(doc).getField("y").numericValue().floatValue();
          double distance = cartesianDistance(x, y, docX, docY);
          System.out.println("" + doc + ": (" + x + "," + y + "), distance=" + distance);
        }
        throw e;
      }
    }
    reader.close();
    writer.close();
    dir.close();
  }

  public void testEquals()  {
    Query q1, q2;

    XYRectangle rect = nextBox();

    q1 = newRectQuery("field", rect.minX, rect.maxX, rect.minY, rect.maxY);
    q2 = newRectQuery("field", rect.minX, rect.maxX, rect.minY, rect.maxY);
    assertEquals(q1, q2);


    float x = nextX();
    float y = nextY();
    q1 = newDistanceQuery("field", x, y, 10000.0f);
    q2 = newDistanceQuery("field", x, y, 10000.0f);
    assertEquals(q1, q2);
    assertFalse(q1.equals(newDistanceQuery("field2", x, y, 10000.0f)));

    float[] xs = new float[5];
    float[] ys = new float[5];
    xs[0] = rect.minX;
    ys[0] = rect.minY;
    xs[1] = rect.maxX;
    ys[1] = rect.minY;
    xs[2] = rect.maxX;
    ys[2] = rect.maxY;
    xs[3] = rect.minX;
    ys[3] = rect.maxY;
    xs[4] = rect.minX;
    ys[4] = rect.minY;
    q1 = newPolygonQuery("field", new XYPolygon(xs, ys));
    q2 = newPolygonQuery("field", new XYPolygon(xs, ys));
    assertEquals(q1, q2);
    assertFalse(q1.equals(newPolygonQuery("field2", new XYPolygon(xs, ys))));
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

    // TODO: must these simple tests really rely on docid order?
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    iwc.setMaxBufferedDocs(TestUtil.nextInt(random(), 100, 1000));
    iwc.setMergePolicy(newLogMergePolicy());
    // Else seeds may not reproduce:
    iwc.setMergeScheduler(new SerialMergeScheduler());
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory, iwc);

    for (double p[] : pts) {
        Document doc = new Document();
        addPointToDoc("point", doc, (float) p[0], (float) p[1]);
        writer.addDocument(doc);
    }

    // add explicit multi-valued docs
    for (int i=0; i<pts.length; i+=2) {
      Document doc = new Document();
      addPointToDoc("point", doc, (float) pts[i][0], (float) pts[i][1]);
      addPointToDoc("point", doc, (float) pts[i+1][0], (float) pts[i+1][1]);
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
    TopDocs td = searchSmallSet(newRectQuery("point", 32.778f, 32.779f, -96.778f, -96.777f), 5);
    assertEquals(4, td.totalHits.value);
  }

  public void testSmallSetRect2() throws Exception {
    TopDocs td = searchSmallSet(newRectQuery("point", -45.0f, -44.0f, -180.0f, 180.0f), 20);
    assertEquals(2, td.totalHits.value);
  }

  public void testSmallSetMultiValued() throws Exception {
    TopDocs td = searchSmallSet(newRectQuery("point", 32.755f, 32.776f, -180f, 180.770f), 20);
    // 3 single valued docs + 2 multi-valued docs
    assertEquals(5, td.totalHits.value);
  }

  public void testSmallSetWholeSpace() throws Exception {
    TopDocs td = searchSmallSet(newRectQuery("point", -Float.MAX_VALUE, Float.MAX_VALUE, -Float.MAX_VALUE, Float.MAX_VALUE), 20);
    assertEquals(24, td.totalHits.value);
  }

  public void testSmallSetPoly() throws Exception {
    TopDocs td = searchSmallSet(newPolygonQuery("point",
        new XYPolygon(
        new float[]{33.073130f, 32.9942669f, 32.938386f, 33.0374494f,
            33.1369762f, 33.1162747f, 33.073130f, 33.073130f},
        new float[]{-96.7682647f, -96.8280029f, -96.6288757f, -96.4929199f,
                     -96.6041564f, -96.7449188f, -96.76826477f, -96.7682647f})),
        5);
    assertEquals(2, td.totalHits.value);
  }

  public void testSmallSetPolyWholeSpace() throws Exception {
    TopDocs td = searchSmallSet(newPolygonQuery("point",
                      new XYPolygon(
                      new float[] {-Float.MAX_VALUE, Float.MAX_VALUE, Float.MAX_VALUE, -Float.MAX_VALUE, -Float.MAX_VALUE},
                      new float[] {-Float.MAX_VALUE, -Float.MAX_VALUE, Float.MAX_VALUE, Float.MAX_VALUE, -Float.MAX_VALUE})),
                      20);
    assertEquals("testWholeMap failed", 24, td.totalHits.value);
  }

  public void testSmallSetDistance() throws Exception {
    TopDocs td = searchSmallSet(newDistanceQuery("point", 32.94823588839368f, -96.4538113027811f, 6.0f), 20);
    assertEquals(11, td.totalHits.value);
  }

  public void testSmallSetTinyDistance() throws Exception {
    TopDocs td = searchSmallSet(newDistanceQuery("point", 40.720611f, -73.998776f, 0.1f), 20);
    assertEquals(2, td.totalHits.value);
  }

  /**
   * Explicitly large
   */
  public void testSmallSetHugeDistance() throws Exception {
    TopDocs td = searchSmallSet(newDistanceQuery("point", 32.94823588839368f, -96.4538113027811f, Float.MAX_VALUE), 20);
    assertEquals(24, td.totalHits.value);
  }
}
