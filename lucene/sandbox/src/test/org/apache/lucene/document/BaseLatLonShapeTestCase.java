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
import org.apache.lucene.document.LatLonShape.QueryRelation;
import org.apache.lucene.geo.GeoTestUtil;
import org.apache.lucene.geo.Line;
import org.apache.lucene.geo.Line2D;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.geo.Polygon2D;
import org.apache.lucene.geo.Rectangle;
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

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomBoolean;
import static com.carrotsearch.randomizedtesting.RandomizedTest.randomIntBetween;
import static org.apache.lucene.geo.GeoEncodingUtils.decodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.decodeLongitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitudeCeil;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitudeCeil;
import static org.apache.lucene.geo.GeoTestUtil.nextLatitude;
import static org.apache.lucene.geo.GeoTestUtil.nextLongitude;

/** base test class for {@link TestLatLonLineShapeQueries}, {@link TestLatLonPointShapeQueries},
 * and {@link TestLatLonPolygonShapeQueries} */
public abstract class BaseLatLonShapeTestCase extends LuceneTestCase {

  /** name of the LatLonShape indexed field */
  protected static final String FIELD_NAME = "shape";
  private static final QueryRelation[] POINT_LINE_RELATIONS = {QueryRelation.INTERSECTS, QueryRelation.DISJOINT};

  protected abstract ShapeType getShapeType();

  protected Object nextShape() {
    return getShapeType().nextShape();
  }

  /** quantizes a latitude value to be consistent with index encoding */
  protected static double quantizeLat(double rawLat) {
    return decodeLatitude(encodeLatitude(rawLat));
  }

  /** quantizes a provided latitude value rounded up to the nearest encoded integer */
  protected static double quantizeLatCeil(double rawLat) {
    return decodeLatitude(encodeLatitudeCeil(rawLat));
  }

  /** quantizes a longitude value to be consistent with index encoding */
  protected static double quantizeLon(double rawLon) {
    return decodeLongitude(encodeLongitude(rawLon));
  }

  /** quantizes a provided longitude value rounded up to the nearest encoded integer */
  protected static double quantizeLonCeil(double rawLon) {
    return decodeLongitude(encodeLongitudeCeil(rawLon));
  }

  /** quantizes a triangle to be consistent with index encoding */
  protected static double[] quantizeTriangle(double ax, double ay, double bx, double by, double cx, double cy) {
    int[] decoded = encodeDecodeTriangle(ax, ay, bx, by, cx, cy);
    return new double[]{decodeLatitude(decoded[0]), decodeLongitude(decoded[1]), decodeLatitude(decoded[2]), decodeLongitude(decoded[3]), decodeLatitude(decoded[4]), decodeLongitude(decoded[5])};
  }

  /** encode/decode a triangle */
  protected static int[] encodeDecodeTriangle(double ax, double ay, double bx, double by, double cx, double cy) {
    byte[] encoded = new byte[7 * LatLonShape.BYTES];
    LatLonShape.encodeTriangle(encoded, encodeLatitude(ay), encodeLongitude(ax), encodeLatitude(by), encodeLongitude(bx), encodeLatitude(cy), encodeLongitude(cx));
    int[] decoded = new int[6];
    LatLonShape.decodeTriangle(encoded, decoded);
    return decoded;
  }

  /** use {@link GeoTestUtil#nextPolygon()} to create a random line; TODO: move to GeoTestUtil */
  public static Line nextLine() {
    Polygon poly = GeoTestUtil.nextPolygon();
    double[] lats = new double[poly.numPoints() - 1];
    double[] lons = new double[lats.length];
    System.arraycopy(poly.getPolyLats(), 0, lats, 0, lats.length);
    System.arraycopy(poly.getPolyLons(), 0, lons, 0, lons.length);

    return new Line(lats, lons);
  }

  /**
   * return a semi-random line used for queries
   *
   * note: shapes parameter may be used to ensure some queries intersect indexed shapes
   **/
  protected Line randomQueryLine(Object... shapes) {
    return nextLine();
  }

  /** creates the array of LatLonShape.Triangle values that are used to index the shape */
  protected abstract Field[] createIndexableFields(String field, Object shape);

  /** adds a shape to a provided document */
  private void addShapeToDoc(String field, Document doc, Object shape) {
    Field[] fields = createIndexableFields(field, shape);
    for (Field f : fields) {
      doc.add(f);
    }
  }

  /** factory method to create a new bounding box query */
  protected Query newRectQuery(String field, QueryRelation queryRelation, double minLat, double maxLat, double minLon, double maxLon) {
    return LatLonShape.newBoxQuery(field, queryRelation, minLat, maxLat, minLon, maxLon);
  }

  /** factory method to create a new line query */
  protected Query newLineQuery(String field, QueryRelation queryRelation, Line... lines) {
    return LatLonShape.newLineQuery(field, queryRelation, lines);
  }

  /** factory method to create a new polygon query */
  protected Query newPolygonQuery(String field, QueryRelation queryRelation, Polygon... polygons) {
    return LatLonShape.newPolygonQuery(field, queryRelation, polygons);
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
    ShapeType type = getShapeType();

    if (VERBOSE) {
      System.out.println("TEST: number of " + type.name() + " shapes=" + numShapes);
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
    verifyRandomBBoxQueries(reader, shapes);
    // test random line queries
    verifyRandomLineQueries(reader, shapes);
    // test random polygon queries
    verifyRandomPolygonQueries(reader, shapes);

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
      Rectangle rect = GeoTestUtil.nextBox();
      QueryRelation queryRelation = RandomPicks.randomFrom(random(), QueryRelation.values());
      Query query = newRectQuery(FIELD_NAME, queryRelation, rect.minLat, rect.maxLat, rect.minLon, rect.maxLon);

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
        double qMinLon = quantizeLonCeil(rect.minLon);
        double qMaxLon = quantizeLon(rect.maxLon);
        double qMinLat = quantizeLatCeil(rect.minLat);
        double qMaxLat = quantizeLat(rect.maxLat);
        if (liveDocs != null && liveDocs.get(docID) == false) {
          // document is deleted
          expected = false;
        } else if (shapes[id] == null) {
          expected = false;
        } else {
          // check quantized poly against quantized query
          if (qMinLon > qMaxLon && rect.crossesDateline() == false) {
            // if the quantization creates a false dateline crossing (because of encodeCeil):
            // then do not use encodeCeil
            qMinLon = quantizeLon(rect.minLon);
          }

          if (qMinLat > qMaxLat) {
            qMinLat = quantizeLat(rect.maxLat);
          }
          expected = getValidator(queryRelation).testBBoxQuery(qMinLat, qMaxLat, qMinLon, qMaxLon, shapes[id]);
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
          b.append("  rect=Rectangle(lat=" + quantizeLatCeil(rect.minLat) + " TO " + quantizeLat(rect.maxLat) + " lon=" + qMinLon + " TO " + quantizeLon(rect.maxLon) + ")\n");          if (true) {
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
      Line queryLine = randomQueryLine(shapes);
      Line2D queryLine2D = Line2D.create(queryLine);
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
          expected = getValidator(queryRelation).testLineQuery(queryLine2D, shapes[id]);
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
          b.append("  queryPolygon=" + queryLine.toGeoJSON());
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
      Polygon queryPolygon = GeoTestUtil.nextPolygon();
      Polygon2D queryPoly2D = Polygon2D.create(queryPolygon);
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
          expected = getValidator(queryRelation).testPolygonQuery(queryPoly2D, shapes[id]);
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
          b.append("  queryPolygon=" + queryPolygon.toGeoJSON());
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

  protected abstract Validator getValidator(QueryRelation relation);

  /** internal point class for testing point shapes */
  protected static class Point {
    double lat;
    double lon;

    public Point(double lat, double lon) {
      this.lat = lat;
      this.lon = lon;
    }

    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("POINT(");
      sb.append(lon);
      sb.append(',');
      sb.append(lat);
      sb.append(')');
      return sb.toString();
    }
  }

  /** internal shape type for testing different shape types */
  protected enum ShapeType {
    POINT() {
      public Point nextShape() {
        return new Point(nextLatitude(), nextLongitude());
      }
    },
    LINE() {
      public Line nextShape() {
        Polygon p = GeoTestUtil.nextPolygon();
        double[] lats = new double[p.numPoints() - 1];
        double[] lons = new double[lats.length];
        for (int i = 0; i < lats.length; ++i) {
          lats[i] = p.getPolyLat(i);
          lons[i] = p.getPolyLon(i);
        }
        return new Line(lats, lons);
      }
    },
    POLYGON() {
      public Polygon nextShape() {
        return GeoTestUtil.nextPolygon();
      }
    },
    MIXED() {
      public Object nextShape() {
        return RandomPicks.randomFrom(random(), subList).nextShape();
      }
    };

    static ShapeType[] subList;
    static {
      subList = new ShapeType[] {POINT, LINE, POLYGON};
    }

    public abstract Object nextShape();

    static ShapeType fromObject(Object shape) {
      if (shape instanceof Point) {
        return POINT;
      } else if (shape instanceof Line) {
        return LINE;
      } else if (shape instanceof Polygon) {
        return POLYGON;
      }
      throw new IllegalArgumentException("invalid shape type from " + shape.toString());
    }
  }

  /** validator class used to test query results against "ground truth" */
  protected static abstract class Validator {
    protected QueryRelation queryRelation = QueryRelation.INTERSECTS;
    public abstract boolean testBBoxQuery(double minLat, double maxLat, double minLon, double maxLon, Object shape);
    public abstract boolean testLineQuery(Line2D line2d, Object shape);
    public abstract boolean testPolygonQuery(Polygon2D poly2d, Object shape);

    public void setRelation(QueryRelation relation) {
      this.queryRelation = relation;
    }
  }
}
