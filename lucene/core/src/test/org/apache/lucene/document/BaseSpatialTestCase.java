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
package org.apache.lucene.document;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomBoolean;
import static com.carrotsearch.randomizedtesting.RandomizedTest.randomIntBetween;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.lucene.document.ShapeField.QueryRelation;
import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.GeoUtils;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiBits;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

/**
 * Base test case for testing spherical and cartesian geometry indexing and search functionality
 *
 * <p>This class is implemented by {@link BaseXYShapeTestCase} for testing XY cartesian geometry and
 * {@link BaseLatLonSpatialTestCase} for testing Lat Lon geospatial geometry
 */
public abstract class BaseSpatialTestCase extends LuceneTestCase {

  /** name of the LatLonShape indexed field */
  protected static final String FIELD_NAME = "shape";

  public final Encoder ENCODER;
  public final Validator VALIDATOR;
  protected static final QueryRelation[] POINT_LINE_RELATIONS = {
          QueryRelation.INTERSECTS, QueryRelation.DISJOINT, QueryRelation.CONTAINS
  };

  public BaseSpatialTestCase() {
    ENCODER = getEncoder();
    VALIDATOR = getValidator();
  }

  // A particularly tricky adversary for BKD tree:
  public void testSameShapeManyTimes() throws Exception {
    int numShapes = TEST_NIGHTLY ? atLeast(50) : atLeast(3);

    // Every doc has 2 points:
    Object theShape = nextShape();

    Object[] shapes = new Object[numShapes];
    Arrays.fill(shapes, theShape);

    verify(shapes);
  }

  // Force low cardinality leaves
  @Slow
  public void testLowCardinalityShapeManyTimes() throws Exception {
    int numShapes = atLeast(20);
    int cardinality = TestUtil.nextInt(random(), 2, 20);

    Object[] diffShapes = new Object[cardinality];
    for (int i = 0; i < cardinality; i++) {
      diffShapes[i] = nextShape();
    }

    Object[] shapes = new Object[numShapes];
    for (int i = 0; i < numShapes; i++) {
      shapes[i] = diffShapes[random().nextInt(cardinality)];
    }

    verify(shapes);
  }

  public void testRandomTiny() throws Exception {
    // Make sure single-leaf-node case is OK:
    doTestRandom(10);
  }

  @Slow
  public void testRandomMedium() throws Exception {
    doTestRandom(atLeast(20));
  }

  @Slow
  @Nightly
  public void testRandomBig() throws Exception {
    doTestRandom(20000);
  }

  protected void doTestRandom(int count) throws Exception {
    int numShapes = atLeast(count);

    if (VERBOSE) {
      System.out.println("TEST: number of " + getShapeType() + " shapes=" + numShapes);
    }

    Object[] shapes = new Object[numShapes];
    for (int id = 0; id < numShapes; ++id) {
      int x = randomIntBetween(0, 20);
      if (x == 17) {
        shapes[id] = null;
        if (VERBOSE) {
          System.out.println("  id=" + id + " is missing");
        }
      } else {
        // create a new shape
        shapes[id] = nextShape();
      }
    }
    verify(shapes);
  }

  protected abstract Object getShapeType();

  protected abstract Object nextShape();

  protected abstract Encoder getEncoder();

  /** creates the array of LatLonShape.Triangle values that are used to index the shape */
  protected abstract Field[] createIndexableFields(String field, Object shape);

  /** adds a shape to a provided document */
  private void addShapeToDoc(String field, Document doc, Object shape) {
    Field[] fields = createIndexableFields(field, shape);
    for (Field f : fields) {
      doc.add(f);
    }
  }

  /** return a semi-random line used for queries * */
  protected abstract Object nextLine();

  protected abstract Object nextPolygon();

  protected abstract Object randomQueryBox();

  protected abstract Object[] nextPoints();

  protected abstract Object nextCircle();

  protected abstract double rectMinX(Object rect);

  protected abstract double rectMaxX(Object rect);

  protected abstract double rectMinY(Object rect);

  protected abstract double rectMaxY(Object rect);

  protected abstract boolean rectCrossesDateline(Object rect);

  /**
   * return a semi-random line used for queries
   *
   * <p>note: shapes parameter may be used to ensure some queries intersect indexed shapes
   */
  protected Object randomQueryLine(Object... shapes) {
    return nextLine();
  }

  protected Object randomQueryPolygon() {
    return nextPolygon();
  }

  protected Object randomQueryCircle() {
    return nextCircle();
  }

  /** factory method to create a new bounding box query */
  protected abstract Query newRectQuery(
          String field,
          QueryRelation queryRelation,
          double minX,
          double maxX,
          double minY,
          double maxY);

  /** factory method to create a new line query */
  protected abstract Query newLineQuery(String field, QueryRelation queryRelation, Object... lines);

  /** factory method to create a new polygon query */
  protected abstract Query newPolygonQuery(
          String field, QueryRelation queryRelation, Object... polygons);

  /** factory method to create a new point query */
  protected abstract Query newPointsQuery(
          String field, QueryRelation queryRelation, Object... points);

  /** factory method to create a new distance query */
  protected abstract Query newDistanceQuery(
          String field, QueryRelation queryRelation, Object circle);

  protected abstract Component2D toLine2D(Object... line);

  protected abstract Component2D toPolygon2D(Object... polygon);

  protected abstract Component2D toPoint2D(Object... points);

  protected abstract Component2D toCircle2D(Object circle);

  protected abstract Component2D toRectangle2D(double minX, double maxX, double minY, double maxY);

  private void verify(Object... shapes) throws Exception {
    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setMergeScheduler(new SerialMergeScheduler());
    int mbd = iwc.getMaxBufferedDocs();
    if (mbd != -1 && mbd < shapes.length / 100) {
      iwc.setMaxBufferedDocs(shapes.length / 100);
    }
    Directory dir;
    if (shapes.length > 1000) {
      dir = newFSDirectory(createTempDir(getClass().getSimpleName()));
    } else {
      dir = newDirectory();
    }
    IndexWriter w = new IndexWriter(dir, iwc);

    // index random polygons
    indexRandomShapes(w, shapes);

    // query testing
    final IndexReader reader = DirectoryReader.open(w);
    // test random bbox queries
    verifyRandomQueries(reader, shapes);
    IOUtils.close(w, reader, dir);
  }

  protected void indexRandomShapes(IndexWriter w, Object... shapes) throws Exception {
    Set<Integer> deleted = new HashSet<>();
    for (int id = 0; id < shapes.length; ++id) {
      Document doc = new Document();
      doc.add(newStringField("id", "" + id, Field.Store.NO));
      doc.add(new NumericDocValuesField("id", id));
      if (shapes[id] != null) {
        addShapeToDoc(FIELD_NAME, doc, shapes[id]);
      }
      w.addDocument(doc);
      if (id > 0 && random().nextInt(100) == 42) {
        int idToDelete = random().nextInt(id);
        w.deleteDocuments(new Term("id", "" + idToDelete));
        deleted.add(idToDelete);
        if (VERBOSE) {
          System.out.println("   delete id=" + idToDelete);
        }
      }
    }

    if (randomBoolean()) {
      w.forceMerge(1);
    }
  }

  protected void verifyRandomQueries(IndexReader reader, Object... shapes) throws Exception {
    // test random bbox queries
    verifyRandomBBoxQueries(reader, shapes);
    // test random line queries
    verifyRandomLineQueries(reader, shapes);
    // test random polygon queries
    verifyRandomPolygonQueries(reader, shapes);
    // test random point queries
    verifyRandomPointQueries(reader, shapes);
    // test random distance queries
    verifyRandomDistanceQueries(reader, shapes);
  }

  /** test random generated bounding boxes */
  protected void verifyRandomBBoxQueries(IndexReader reader, Object... shapes) throws Exception {
    IndexSearcher s = newSearcher(reader);

    final int iters = scaledIterationCount(shapes.length);

    Bits liveDocs = MultiBits.getLiveDocs(s.getIndexReader());
    int maxDoc = s.getIndexReader().maxDoc();

    for (int iter = 0; iter < iters; ++iter) {
      if (VERBOSE) {
        System.out.println("\nTEST: iter=" + (iter + 1) + " of " + iters + " s=" + s);
      }

      // BBox
      Object rect = randomQueryBox();
      QueryRelation queryRelation = RandomPicks.randomFrom(random(), QueryRelation.values());
      Query query =
              newRectQuery(
                      FIELD_NAME,
                      queryRelation,
                      rectMinX(rect),
                      rectMaxX(rect),
                      rectMinY(rect),
                      rectMaxY(rect));

      if (VERBOSE) {
        System.out.println("  query=" + query + ", relation=" + queryRelation);
      }

      final FixedBitSet hits = searchIndex(s, query, maxDoc);

      boolean fail = false;
      NumericDocValues docIDToID = MultiDocValues.getNumericValues(reader, "id");
      for (int docID = 0; docID < maxDoc; ++docID) {
        assertEquals(docID, docIDToID.nextDoc());
        int id = (int) docIDToID.longValue();
        boolean expected;
        double minX = rectMinX(rect);
        double maxX = rectMaxX(rect);
        double minY = rectMinY(rect);
        double maxY = rectMaxY(rect);
        if (liveDocs != null && liveDocs.get(docID) == false) {
          // document is deleted
          expected = false;
        } else if (shapes[id] == null) {
          expected = false;
        } else {
          if (queryRelation == QueryRelation.CONTAINS && rectCrossesDateline(rect)) {
            // For contains we need to call the validator for each section.
            // It is only expected if both sides are contained.
            Component2D left = toRectangle2D(minX, GeoUtils.MAX_LON_INCL, minY, maxY);
            Component2D right = toRectangle2D(GeoUtils.MIN_LON_INCL, maxX, minY, maxY);
            expected =
                    VALIDATOR.setRelation(queryRelation).testComponentQuery(left, shapes[id])
                            && VALIDATOR.setRelation(queryRelation).testComponentQuery(right, shapes[id]);
          } else {
            Component2D component2D = toRectangle2D(minX, maxX, minY, maxY);
            expected =
                    VALIDATOR.setRelation(queryRelation).testComponentQuery(component2D, shapes[id]);
          }
        }

        if (hits.get(docID) != expected) {
          StringBuilder b = new StringBuilder();

          if (expected) {
            b.append("FAIL: id=" + id + " should match but did not\n");
          } else {
            b.append("FAIL: id=" + id + " should not match but did\n");
          }
          b.append("  relation=" + queryRelation + "\n");
          b.append("  query=" + query + " docID=" + docID + "\n");
          if (shapes[id] instanceof Object[]) {
            b.append("  shape=" + Arrays.toString((Object[]) shapes[id]) + "\n");
          } else {
            b.append("  shape=" + shapes[id] + "\n");
          }
          b.append("  deleted?=" + (liveDocs != null && liveDocs.get(docID) == false));
          b.append(
                  "  rect=Rectangle(lat="
                          + ENCODER.quantizeYCeil(rectMinY(rect))
                          + " TO "
                          + ENCODER.quantizeY(rectMaxY(rect))
                          + " lon="
                          + minX
                          + " TO "
                          + ENCODER.quantizeX(rectMaxX(rect))
                          + ")\n");
          if (true) {
            fail("wrong hit (first of possibly more):\n\n" + b);
          } else {
            System.out.println(b.toString());
            fail = true;
          }
        }
      }
      if (fail) {
        fail("some hits were wrong");
      }
    }
  }

  /** test random generated lines */
  protected void verifyRandomLineQueries(IndexReader reader, Object... shapes) throws Exception {
    IndexSearcher s = newSearcher(reader);

    final int iters = scaledIterationCount(shapes.length);

    Bits liveDocs = MultiBits.getLiveDocs(s.getIndexReader());
    int maxDoc = s.getIndexReader().maxDoc();

    for (int iter = 0; iter < iters; ++iter) {
      if (VERBOSE) {
        System.out.println("\nTEST: iter=" + (iter + 1) + " of " + iters + " s=" + s);
      }

      // line
      Object queryLine = randomQueryLine(shapes);
      Component2D queryLine2D = toLine2D(queryLine);
      QueryRelation queryRelation = RandomPicks.randomFrom(random(), POINT_LINE_RELATIONS);
      Query query = newLineQuery(FIELD_NAME, queryRelation, queryLine);

      if (VERBOSE) {
        System.out.println("  query=" + query + ", relation=" + queryRelation);
      }

      final FixedBitSet hits = searchIndex(s, query, maxDoc);

      boolean fail = false;
      NumericDocValues docIDToID = MultiDocValues.getNumericValues(reader, "id");
      for (int docID = 0; docID < maxDoc; ++docID) {
        assertEquals(docID, docIDToID.nextDoc());
        int id = (int) docIDToID.longValue();
        boolean expected;
        if (liveDocs != null && liveDocs.get(docID) == false) {
          // document is deleted
          expected = false;
        } else if (shapes[id] == null) {
          expected = false;
        } else {
          expected =
                  VALIDATOR.setRelation(queryRelation).testComponentQuery(queryLine2D, shapes[id]);
        }

        if (hits.get(docID) != expected) {
          StringBuilder b = new StringBuilder();

          if (expected) {
            b.append("FAIL: id=" + id + " should match but did not\n");
          } else {
            b.append("FAIL: id=" + id + " should not match but did\n");
          }
          b.append("  relation=" + queryRelation + "\n");
          b.append("  query=" + query + " docID=" + docID + "\n");
          if (shapes[id] instanceof Object[]) {
            b.append("  shape=" + Arrays.toString((Object[]) shapes[id]) + "\n");
          } else {
            b.append("  shape=" + shapes[id] + "\n");
          }
          b.append("  deleted?=" + (liveDocs != null && liveDocs.get(docID) == false));
          b.append("  queryPolygon=" + queryLine);
          if (true) {
            fail("wrong hit (first of possibly more):\n\n" + b);
          } else {
            System.out.println(b.toString());
            fail = true;
          }
        }
      }
      if (fail) {
        fail("some hits were wrong");
      }
    }
  }

  /** test random generated polygons */
  protected void verifyRandomPolygonQueries(IndexReader reader, Object... shapes) throws Exception {
    IndexSearcher s = newSearcher(reader);

    final int iters = scaledIterationCount(shapes.length);

    Bits liveDocs = MultiBits.getLiveDocs(s.getIndexReader());
    int maxDoc = s.getIndexReader().maxDoc();

    for (int iter = 0; iter < iters; ++iter) {
      if (VERBOSE) {
        System.out.println("\nTEST: iter=" + (iter + 1) + " of " + iters + " s=" + s);
      }

      // Polygon
      Object queryPolygon = randomQueryPolygon();
      Component2D queryPoly2D = toPolygon2D(queryPolygon);
      QueryRelation queryRelation = RandomPicks.randomFrom(random(), QueryRelation.values());
      Query query = newPolygonQuery(FIELD_NAME, queryRelation, queryPolygon);

      if (VERBOSE) {
        System.out.println("  query=" + query + ", relation=" + queryRelation);
      }

      final FixedBitSet hits = searchIndex(s, query, maxDoc);

      boolean fail = false;
      NumericDocValues docIDToID = MultiDocValues.getNumericValues(reader, "id");
      for (int docID = 0; docID < maxDoc; ++docID) {
        assertEquals(docID, docIDToID.nextDoc());
        int id = (int) docIDToID.longValue();
        boolean expected;
        if (liveDocs != null && liveDocs.get(docID) == false) {
          // document is deleted
          expected = false;
        } else if (shapes[id] == null) {
          expected = false;
        } else {
          expected =
                  VALIDATOR.setRelation(queryRelation).testComponentQuery(queryPoly2D, shapes[id]);
        }

        if (hits.get(docID) != expected) {
          StringBuilder b = new StringBuilder();

          if (expected) {
            b.append("FAIL: id=" + id + " should match but did not\n");
          } else {
            b.append("FAIL: id=" + id + " should not match but did\n");
          }
          b.append("  relation=" + queryRelation + "\n");
          b.append("  query=" + query + " docID=" + docID + "\n");
          if (shapes[id] instanceof Object[]) {
            b.append("  shape=" + Arrays.toString((Object[]) shapes[id]) + "\n");
          } else {
            b.append("  shape=" + shapes[id] + "\n");
          }
          b.append("  deleted?=" + (liveDocs != null && liveDocs.get(docID) == false));
          b.append("  queryPolygon=" + queryPolygon);
          if (true) {
            fail("wrong hit (first of possibly more):\n\n" + b);
          } else {
            System.out.println(b.toString());
            fail = true;
          }
        }
      }
      if (fail) {
        fail("some hits were wrong");
      }
    }
  }

  /** test random generated point queries */
  protected void verifyRandomPointQueries(IndexReader reader, Object... shapes) throws Exception {
    IndexSearcher s = newSearcher(reader);

    final int iters = scaledIterationCount(shapes.length);

    Bits liveDocs = MultiBits.getLiveDocs(s.getIndexReader());
    int maxDoc = s.getIndexReader().maxDoc();

    for (int iter = 0; iter < iters; ++iter) {
      if (VERBOSE) {
        System.out.println("\nTEST: iter=" + (iter + 1) + " of " + iters + " s=" + s);
      }

      Object[] queryPoints = nextPoints();
      QueryRelation queryRelation = RandomPicks.randomFrom(random(), QueryRelation.values());
      Component2D queryPoly2D;
      Query query;
      if (queryRelation == QueryRelation.CONTAINS) {
        queryPoly2D = toPoint2D(queryPoints[0]);
        query = newPointsQuery(FIELD_NAME, queryRelation, queryPoints[0]);
      } else {
        queryPoly2D = toPoint2D(queryPoints);
        query = newPointsQuery(FIELD_NAME, queryRelation, queryPoints);
      }

      if (VERBOSE) {
        System.out.println("  query=" + query + ", relation=" + queryRelation);
      }

      final FixedBitSet hits = searchIndex(s, query, maxDoc);

      boolean fail = false;
      NumericDocValues docIDToID = MultiDocValues.getNumericValues(reader, "id");
      for (int docID = 0; docID < maxDoc; ++docID) {
        assertEquals(docID, docIDToID.nextDoc());
        int id = (int) docIDToID.longValue();
        boolean expected;

        if (liveDocs != null && liveDocs.get(docID) == false) {
          // document is deleted
          expected = false;
        } else if (shapes[id] == null) {
          expected = false;
        } else {
          expected =
                  VALIDATOR.setRelation(queryRelation).testComponentQuery(queryPoly2D, shapes[id]);
        }

        if (hits.get(docID) != expected) {
          StringBuilder b = new StringBuilder();

          if (expected) {
            b.append("FAIL: id=" + id + " should match but did not\n");
          } else {
            b.append("FAIL: id=" + id + " should not match but did\n");
          }
          b.append("  relation=" + queryRelation + "\n");
          b.append("  query=" + query + " docID=" + docID + "\n");
          if (shapes[id] instanceof Object[]) {
            b.append("  shape=" + Arrays.toString((Object[]) shapes[id]) + "\n");
          } else {
            b.append("  shape=" + shapes[id] + "\n");
          }
          b.append("  deleted?=" + (liveDocs != null && liveDocs.get(docID) == false));
          b.append("  rect=Points(" + Arrays.toString(queryPoints) + ")\n");
          if (true) {
            fail("wrong hit (first of possibly more):\n\n" + b);
          } else {
            System.out.println(b.toString());
            fail = true;
          }
        }
      }
      if (fail) {
        fail("some hits were wrong");
      }
    }
  }

  /** test random generated circles */
  protected void verifyRandomDistanceQueries(IndexReader reader, Object... shapes)
          throws Exception {
    IndexSearcher s = newSearcher(reader);

    final int iters = scaledIterationCount(shapes.length);

    Bits liveDocs = MultiBits.getLiveDocs(s.getIndexReader());
    int maxDoc = s.getIndexReader().maxDoc();

    for (int iter = 0; iter < iters; ++iter) {
      if (VERBOSE) {
        System.out.println("\nTEST: iter=" + (iter + 1) + " of " + iters + " s=" + s);
      }

      // Polygon
      Object queryCircle = randomQueryCircle();
      Component2D queryCircle2D = toCircle2D(queryCircle);
      QueryRelation queryRelation = RandomPicks.randomFrom(random(), QueryRelation.values());
      Query query = newDistanceQuery(FIELD_NAME, queryRelation, queryCircle);

      if (VERBOSE) {
        System.out.println("  query=" + query + ", relation=" + queryRelation);
      }

      final FixedBitSet hits = searchIndex(s, query, maxDoc);

      boolean fail = false;
      NumericDocValues docIDToID = MultiDocValues.getNumericValues(reader, "id");
      for (int docID = 0; docID < maxDoc; ++docID) {
        assertEquals(docID, docIDToID.nextDoc());
        int id = (int) docIDToID.longValue();
        boolean expected;
        if (liveDocs != null && liveDocs.get(docID) == false) {
          // document is deleted
          expected = false;
        } else if (shapes[id] == null) {
          expected = false;
        } else {
          expected =
                  VALIDATOR.setRelation(queryRelation).testComponentQuery(queryCircle2D, shapes[id]);
        }

        if (hits.get(docID) != expected) {
          StringBuilder b = new StringBuilder();

          if (expected) {
            b.append("FAIL: id=" + id + " should match but did not\n");
          } else {
            b.append("FAIL: id=" + id + " should not match but did\n");
          }
          b.append("  relation=" + queryRelation + "\n");
          b.append("  query=" + query + " docID=" + docID + "\n");
          if (shapes[id] instanceof Object[]) {
            b.append("  shape=" + Arrays.toString((Object[]) shapes[id]) + "\n");
          } else {
            b.append("  shape=" + shapes[id] + "\n");
          }
          b.append("  deleted?=" + (liveDocs != null && liveDocs.get(docID) == false));
          b.append("  distanceQuery=" + queryCircle.toString());
          if (true) {
            fail("wrong hit (first of possibly more):\n\n" + b);
          } else {
            System.out.println(b.toString());
            fail = true;
          }
        }
      }
      if (fail) {
        fail("some hits were wrong");
      }
    }
  }

  private FixedBitSet searchIndex(IndexSearcher s, Query query, int maxDoc) throws IOException {
    final FixedBitSet hits = new FixedBitSet(maxDoc);
    s.search(
            query,
            new SimpleCollector() {

              private int docBase;

              @Override
              public ScoreMode scoreMode() {
                return ScoreMode.COMPLETE_NO_SCORES;
              }

              @Override
              protected void doSetNextReader(LeafReaderContext context) {
                docBase = context.docBase;
              }

              @Override
              public void collect(int doc) {
                hits.set(docBase + doc);
              }
            });
    return hits;
  }

  protected abstract Validator getValidator();

  protected abstract static class Encoder {
    abstract double decodeX(int encoded);

    abstract double decodeY(int encoded);

    abstract double quantizeX(double raw);

    abstract double quantizeXCeil(double raw);

    abstract double quantizeY(double raw);

    abstract double quantizeYCeil(double raw);
  }

  private int scaledIterationCount(int shapes) {
    if (shapes < 500) {
      return atLeast(50);
    } else if (shapes < 5000) {
      return atLeast(25);
    } else if (shapes < 25000) {
      return atLeast(5);
    } else {
      return atLeast(2);
    }
  }

  /** validator class used to test query results against "ground truth" */
  protected abstract static class Validator {
    Encoder encoder;

    Validator(Encoder encoder) {
      this.encoder = encoder;
    }

    protected QueryRelation queryRelation = QueryRelation.INTERSECTS;

    public abstract boolean testComponentQuery(Component2D line2d, Object shape);

    public Validator setRelation(QueryRelation relation) {
      this.queryRelation = relation;
      return this;
    }

    public boolean testComponentQuery(Component2D query, Field[] fields) {
      ShapeField.DecodedTriangle decodedTriangle = new ShapeField.DecodedTriangle();
      for (Field field : fields) {
        boolean intersects;
        boolean contains;
        ShapeField.decodeTriangle(field.binaryValue().bytes, decodedTriangle);
        switch (decodedTriangle.type) {
          case POINT:
          {
            double y = encoder.decodeY(decodedTriangle.aY);
            double x = encoder.decodeX(decodedTriangle.aX);
            intersects = query.contains(x, y);
            contains = intersects;
            break;
          }
          case LINE:
          {
            double aY = encoder.decodeY(decodedTriangle.aY);
            double aX = encoder.decodeX(decodedTriangle.aX);
            double bY = encoder.decodeY(decodedTriangle.bY);
            double bX = encoder.decodeX(decodedTriangle.bX);
            intersects = query.intersectsLine(aX, aY, bX, bY);
            contains = query.containsLine(aX, aY, bX, bY);
            break;
          }
          case TRIANGLE:
          {
            double aY = encoder.decodeY(decodedTriangle.aY);
            double aX = encoder.decodeX(decodedTriangle.aX);
            double bY = encoder.decodeY(decodedTriangle.bY);
            double bX = encoder.decodeX(decodedTriangle.bX);
            double cY = encoder.decodeY(decodedTriangle.cY);
            double cX = encoder.decodeX(decodedTriangle.cX);
            intersects = query.intersectsTriangle(aX, aY, bX, bY, cX, cY);
            contains = query.containsTriangle(aX, aY, bX, bY, cX, cY);
            break;
          }
          default:
            throw new IllegalArgumentException(
                    "Unsupported triangle type :[" + decodedTriangle.type + "]");
        }
        assertTrue((contains == intersects) || (contains == false && intersects == true));
        if (queryRelation == QueryRelation.DISJOINT && intersects) {
          return false;
        } else if (queryRelation == QueryRelation.WITHIN && contains == false) {
          return false;
        } else if (queryRelation == QueryRelation.INTERSECTS && intersects) {
          return true;
        }
      }
      return queryRelation == QueryRelation.INTERSECTS ? false : true;
    }

    protected Component2D.WithinRelation testWithinQuery(Component2D query, Field[] fields) {
      Component2D.WithinRelation answer = Component2D.WithinRelation.DISJOINT;
      ShapeField.DecodedTriangle decodedTriangle = new ShapeField.DecodedTriangle();
      for (Field field : fields) {
        ShapeField.decodeTriangle(field.binaryValue().bytes, decodedTriangle);
        Component2D.WithinRelation relation;
        switch (decodedTriangle.type) {
          case POINT:
          {
            double y = encoder.decodeY(decodedTriangle.aY);
            double x = encoder.decodeX(decodedTriangle.aX);
            relation = query.withinPoint(x, y);
            break;
          }
          case LINE:
          {
            double aY = encoder.decodeY(decodedTriangle.aY);
            double aX = encoder.decodeX(decodedTriangle.aX);
            double bY = encoder.decodeY(decodedTriangle.bY);
            double bX = encoder.decodeX(decodedTriangle.bX);
            relation = query.withinLine(aX, aY, decodedTriangle.ab, bX, bY);
            break;
          }
          case TRIANGLE:
          {
            double aY = encoder.decodeY(decodedTriangle.aY);
            double aX = encoder.decodeX(decodedTriangle.aX);
            double bY = encoder.decodeY(decodedTriangle.bY);
            double bX = encoder.decodeX(decodedTriangle.bX);
            double cY = encoder.decodeY(decodedTriangle.cY);
            double cX = encoder.decodeX(decodedTriangle.cX);
            relation =
                    query.withinTriangle(
                            aX,
                            aY,
                            decodedTriangle.ab,
                            bX,
                            bY,
                            decodedTriangle.bc,
                            cX,
                            cY,
                            decodedTriangle.ca);
            break;
          }
          default:
            throw new IllegalArgumentException(
                    "Unsupported triangle type :[" + decodedTriangle.type + "]");
        }
        if (relation == Component2D.WithinRelation.NOTWITHIN) {
          return relation;
        } else if (relation == Component2D.WithinRelation.CANDIDATE) {
          answer = Component2D.WithinRelation.CANDIDATE;
        }
      }
      return answer;
    }
  }
}
