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

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.apache.lucene.document.ShapeField.QueryRelation;
import org.apache.lucene.geo.Component2D;
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

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomBoolean;
import static com.carrotsearch.randomizedtesting.RandomizedTest.randomIntBetween;

/**
 * Base test case for testing spherical and cartesian geometry indexing and search functionality
 * <p>
 * This class is implemented by {@link BaseXYShapeTestCase} for testing XY cartesian geometry
 * and {@link BaseLatLonShapeTestCase} for testing Lat Lon geospatial geometry
 **/
public abstract class BaseShapeTestCase extends LuceneTestCase {

  /** name of the LatLonShape indexed field */
  protected static final String FIELD_NAME = "shape";
  public final Encoder ENCODER;
  public final Validator VALIDATOR;
  protected static final QueryRelation[] POINT_LINE_RELATIONS = {QueryRelation.INTERSECTS, QueryRelation.DISJOINT};

  public BaseShapeTestCase() {
    ENCODER = getEncoder();
    VALIDATOR = getValidator();
  }

  // A particularly tricky adversary for BKD tree:
  public void testSameShapeManyTimes() throws Exception {
    int numShapes = atLeast(500);

    // Every doc has 2 points:
    Object theShape = nextShape();

    Object[] shapes = new Object[numShapes];
    Arrays.fill(shapes, theShape);

    verify(shapes);
  }

  // Force low cardinality leaves
  public void testLowCardinalityShapeManyTimes() throws Exception {
    int numShapes = atLeast(500);
    int cardinality = TestUtil.nextInt(random(), 2, 20);

    Object[] diffShapes = new Object[cardinality];
    for (int i = 0; i < cardinality; i++) {
      diffShapes[i] = nextShape();
    }

    Object[] shapes = new Object[numShapes];
    for (int i = 0; i < numShapes; i++) {
      shapes[i] =  diffShapes[random().nextInt(cardinality)];
    }

    verify(shapes);
  }

  public void testRandomTiny() throws Exception {
    // Make sure single-leaf-node case is OK:
    doTestRandom(10);
  }

  @Slow
  public void testRandomMedium() throws Exception {
    doTestRandom(1000);
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

  /** return a semi-random line used for queries **/
  protected abstract Object nextLine();

  protected abstract Object nextPolygon();

  protected abstract Object randomQueryBox();

  protected abstract double rectMinX(Object rect);
  protected abstract double rectMaxX(Object rect);
  protected abstract double rectMinY(Object rect);
  protected abstract double rectMaxY(Object rect);
  protected abstract boolean rectCrossesDateline(Object rect);

  /**
   * return a semi-random line used for queries
   *
   * note: shapes parameter may be used to ensure some queries intersect indexed shapes
   **/
  protected Object randomQueryLine(Object... shapes) {
    return nextLine();
  }

  protected Object randomQueryPolygon() {
    return nextPolygon();
  }

  /** factory method to create a new bounding box query */
  protected abstract Query newRectQuery(String field, QueryRelation queryRelation, double minX, double maxX, double minY, double maxY);

  /** factory method to create a new line query */
  protected abstract Query newLineQuery(String field, QueryRelation queryRelation, Object... lines);

  /** factory method to create a new polygon query */
  protected abstract Query newPolygonQuery(String field, QueryRelation queryRelation, Object... polygons);

  protected abstract Component2D toLine2D(Object... line);

  protected abstract Component2D toPolygon2D(Object... polygon);

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
        w.deleteDocuments(new Term("id", ""+idToDelete));
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
  }

  /** test random generated bounding boxes */
  protected void verifyRandomBBoxQueries(IndexReader reader, Object... shapes) throws Exception {
    IndexSearcher s = newSearcher(reader);

    final int iters = scaledIterationCount(shapes.length);

    Bits liveDocs = MultiBits.getLiveDocs(s.getIndexReader());
    int maxDoc = s.getIndexReader().maxDoc();

    for (int iter = 0; iter < iters; ++iter) {
      if (VERBOSE) {
        System.out.println("\nTEST: iter=" + (iter+1) + " of " + iters + " s=" + s);
      }

      // BBox
      Object rect = randomQueryBox();
      QueryRelation queryRelation = RandomPicks.randomFrom(random(), QueryRelation.values());
      Query query = newRectQuery(FIELD_NAME, queryRelation, rectMinX(rect), rectMaxX(rect), rectMinY(rect), rectMaxY(rect));

      if (VERBOSE) {
        System.out.println("  query=" + query + ", relation=" + queryRelation);
      }

      final FixedBitSet hits = new FixedBitSet(maxDoc);
      s.search(query, new SimpleCollector() {

        private int docBase;

        @Override
        public ScoreMode scoreMode() {
          return ScoreMode.COMPLETE_NO_SCORES;
        }

        @Override
        protected void doSetNextReader(LeafReaderContext context) throws IOException {
          docBase = context.docBase;
        }

        @Override
        public void collect(int doc) throws IOException {
          hits.set(docBase+doc);
        }
      });

      boolean fail = false;
      NumericDocValues docIDToID = MultiDocValues.getNumericValues(reader, "id");
      for (int docID = 0; docID < maxDoc; ++docID) {
        assertEquals(docID, docIDToID.nextDoc());
        int id = (int) docIDToID.longValue();
        boolean expected;
        double qMinLon = ENCODER.quantizeXCeil(rectMinX(rect));
        double qMaxLon = ENCODER.quantizeX(rectMaxX(rect));
        double qMinLat = ENCODER.quantizeYCeil(rectMinY(rect));
        double qMaxLat = ENCODER.quantizeY(rectMaxY(rect));
        if (liveDocs != null && liveDocs.get(docID) == false) {
          // document is deleted
          expected = false;
        } else if (shapes[id] == null) {
          expected = false;
        } else {
          // check quantized poly against quantized query
          if (qMinLon > qMaxLon && rectCrossesDateline(rect) == false) {
            // if the quantization creates a false dateline crossing (because of encodeCeil):
            // then do not use encodeCeil
            qMinLon = ENCODER.quantizeX(rectMinX(rect));
          }

          if (qMinLat > qMaxLat) {
            qMinLat = ENCODER.quantizeY(rectMaxY(rect));
          }
          expected = VALIDATOR.setRelation(queryRelation).testBBoxQuery(qMinLat, qMaxLat, qMinLon, qMaxLon, shapes[id]);
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
          b.append("  rect=Rectangle(lat=" + ENCODER.quantizeYCeil(rectMinY(rect)) + " TO " + ENCODER.quantizeY(rectMaxY(rect)) + " lon=" + qMinLon + " TO " + ENCODER.quantizeX(rectMaxX(rect)) + ")\n");
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

      final FixedBitSet hits = new FixedBitSet(maxDoc);
      s.search(query, new SimpleCollector() {

        private int docBase;

        @Override
        public ScoreMode scoreMode() {
          return ScoreMode.COMPLETE_NO_SCORES;
        }

        @Override
        protected void doSetNextReader(LeafReaderContext context) throws IOException {
          docBase = context.docBase;
        }

        @Override
        public void collect(int doc) throws IOException {
          hits.set(docBase+doc);
        }
      });

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
          expected = VALIDATOR.setRelation(queryRelation).testComponentQuery(queryLine2D, shapes[id]);
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

      final FixedBitSet hits = new FixedBitSet(maxDoc);
      s.search(query, new SimpleCollector() {

        private int docBase;

        @Override
        public ScoreMode scoreMode() {
          return ScoreMode.COMPLETE_NO_SCORES;
        }

        @Override
        protected void doSetNextReader(LeafReaderContext context) throws IOException {
          docBase = context.docBase;
        }

        @Override
        public void collect(int doc) throws IOException {
          hits.set(docBase+doc);
        }
      });

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
          expected = VALIDATOR.setRelation(queryRelation).testComponentQuery(queryPoly2D, shapes[id]);
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

  protected abstract Validator getValidator();

  protected static abstract class Encoder {
    abstract double quantizeX(double raw);
    abstract double quantizeXCeil(double raw);
    abstract double quantizeY(double raw);
    abstract double quantizeYCeil(double raw);
    abstract double[] quantizeTriangle(double ax, double ay, boolean ab, double bx, double by, boolean bc, double cx, double cy, boolean ca);
    abstract ShapeField.DecodedTriangle encodeDecodeTriangle(double ax, double ay, boolean ab, double bx, double by, boolean bc, double cx, double cy, boolean ca);
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
  protected static abstract class Validator {
    Encoder encoder;
    Validator(Encoder encoder) {
      this.encoder = encoder;
    }

    protected QueryRelation queryRelation = QueryRelation.INTERSECTS;
    public abstract boolean testBBoxQuery(double minLat, double maxLat, double minLon, double maxLon, Object shape);
    public abstract boolean testComponentQuery(Component2D line2d, Object shape);

    public Validator setRelation(QueryRelation relation) {
      this.queryRelation = relation;
      return this;
    }
  }
}
