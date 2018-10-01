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
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.geo.Polygon2D;
import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.MultiFields;
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
import static com.carrotsearch.randomizedtesting.RandomizedTest.randomInt;
import static org.apache.lucene.geo.GeoEncodingUtils.decodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.decodeLongitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitudeCeil;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitudeCeil;
import static org.apache.lucene.geo.GeoTestUtil.nextLatitude;
import static org.apache.lucene.geo.GeoTestUtil.nextLongitude;

public abstract class BaseLatLonShapeTestCase extends LuceneTestCase {

  protected static final String FIELD_NAME = "shape";

  protected abstract ShapeType getShapeType();

  protected Object nextShape() {
    return getShapeType().nextShape();
  }

  protected double quantizeLat(double rawLat) {
    return decodeLatitude(encodeLatitude(rawLat));
  }

  protected double quantizeLatCeil(double rawLat) {
    return decodeLatitude(encodeLatitudeCeil(rawLat));
  }

  protected double quantizeLon(double rawLon) {
    return decodeLongitude(encodeLongitude(rawLon));
  }

  protected double quantizeLonCeil(double rawLon) {
    return decodeLongitude(encodeLongitudeCeil(rawLon));
  }

  protected Polygon quantizePolygon(Polygon polygon) {
    double[] lats = new double[polygon.numPoints()];
    double[] lons = new double[polygon.numPoints()];
    for (int i = 0; i < lats.length; ++i) {
      lats[i] = quantizeLat(polygon.getPolyLat(i));
      lons[i] = quantizeLon(polygon.getPolyLon(i));
    }
    return new Polygon(lats, lons);
  }

  protected Line quantizeLine(Line line) {
    double[] lats = new double[line.numPoints()];
    double[] lons = new double[line.numPoints()];
    for (int i = 0; i < lats.length; ++i) {
      lats[i] = quantizeLat(line.getLat(i));
      lons[i] = quantizeLon(line.getLon(i));
    }
    return new Line(lats, lons);
  }

  protected abstract Field[] createIndexableFields(String field, Object shape);

  private void addShapeToDoc(String field, Document doc, Object shape) {
    Field[] fields = createIndexableFields(field, shape);
    for (Field f : fields) {
      doc.add(f);
    }
  }

  protected Query newRectQuery(String field, QueryRelation queryRelation, double minLat, double maxLat, double minLon, double maxLon) {
    return LatLonShape.newBoxQuery(field, queryRelation, minLat, maxLat, minLon, maxLon);
  }

  protected Query newPolygonQuery(String field, QueryRelation queryRelation, Polygon... polygons) {
    return LatLonShape.newPolygonQuery(field, queryRelation, polygons);
  }

  // A particularly tricky adversary for BKD tree:
  public void testSameShapeManyTimes() throws Exception {
    int numShapes = atLeast(1000);

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

  public void testRandomMedium() throws Exception {
    doTestRandom(10000);
  }

  @Nightly
  public void testRandomBig() throws Exception {
    doTestRandom(50000);
  }

  protected void doTestRandom(int count) throws Exception {
    int numShapes = atLeast(count);
    ShapeType type = getShapeType();

    if (VERBOSE) {
      System.out.println("TEST: number of " + type.name() + " shapes=" + numShapes);
    }

    Object[] shapes = new Object[numShapes];
    for (int id = 0; id < numShapes; ++id) {
      int x = randomInt(20);
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
    // test random polygon queires
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
      if (id > 0 && randomInt(100) == 42) {
        int idToDelete = randomInt(id);
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

  protected void verifyRandomBBoxQueries(IndexReader reader, Object... shapes) throws Exception {
    IndexSearcher s = newSearcher(reader);

    final int iters = atLeast(75);

    Bits liveDocs = MultiFields.getLiveDocs(s.getIndexReader());
    int maxDoc = s.getIndexReader().maxDoc();

    for (int iter = 0; iter < iters; ++iter) {
      if (VERBOSE) {
        System.out.println("\nTEST: iter=" + (iter+1) + " of " + iters + " s=" + s);
      }

      // BBox
      Rectangle rect;
      // quantizing the bbox may end up w/ bounding boxes crossing dateline...
      // todo add support for bounding boxes crossing dateline
      while (true) {
        rect = GeoTestUtil.nextBoxNotCrossingDateline();
        if (decodeLongitude(encodeLongitudeCeil(rect.minLon)) <= decodeLongitude(encodeLongitude(rect.maxLon)) &&
            decodeLatitude(encodeLatitudeCeil(rect.minLat)) <= decodeLatitude(encodeLatitude(rect.maxLat))) {
          break;
        }
      }
      QueryRelation queryRelation = RandomPicks.randomFrom(random(), QueryRelation.values());
      Query query = newRectQuery(FIELD_NAME, queryRelation, rect.minLat, rect.maxLat, rect.minLon, rect.maxLon);

      if (VERBOSE) {
        System.out.println("  query=" + query);
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
          // check quantized poly against quantized query
          expected = getValidator(queryRelation).testBBoxQuery(quantizeLatCeil(rect.minLat), quantizeLat(rect.maxLat),
              quantizeLonCeil(rect.minLon), quantizeLon(rect.maxLon), shapes[id]);
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
          b.append("  shape=" + shapes[id] + "\n");
          b.append("  deleted?=" + (liveDocs != null && liveDocs.get(docID) == false));
          b.append("  rect=Rectangle(" + quantizeLatCeil(rect.minLat) + " TO " + quantizeLat(rect.maxLat) + " lon=" + quantizeLonCeil(rect.minLon) + " TO " + quantizeLon(rect.maxLon) + ")\n");
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

  protected void verifyRandomPolygonQueries(IndexReader reader, Object... shapes) throws Exception {
    IndexSearcher s = newSearcher(reader);

    final int iters = atLeast(75);

    Bits liveDocs = MultiFields.getLiveDocs(s.getIndexReader());
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
        System.out.println("  query=" + query);
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
          b.append("  shape=" + shapes[id] + "\n");
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

  protected abstract class Validator {
    protected QueryRelation queryRelation = QueryRelation.INTERSECTS;
    public abstract boolean testBBoxQuery(double minLat, double maxLat, double minLon, double maxLon, Object shape);
    public abstract boolean testPolygonQuery(Polygon2D poly2d, Object shape);

    public void setRelation(QueryRelation relation) {
      this.queryRelation = relation;
    }
  }
}
