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

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import org.apache.lucene.document.ShapeField.QueryRelation;
import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.GeoTestUtil;
import org.apache.lucene.geo.Line;
import org.apache.lucene.geo.Line2D;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.geo.Polygon2D;
import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.geo.Rectangle2D;
import org.apache.lucene.geo.Tessellator;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.junit.Ignore;

import static org.apache.lucene.geo.GeoEncodingUtils.decodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.decodeLongitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitude;

/** Test case for indexing polygons and querying by bounding box */
public class TestLatLonShape extends LuceneTestCase {
  protected static String FIELDNAME = "field";

  /** quantizes a latitude value to be consistent with index encoding */
  protected static double quantizeLat(double rawLat) {
    return decodeLatitude(encodeLatitude(rawLat));
  }

  /** quantizes a longitude value to be consistent with index encoding */
  protected static double quantizeLon(double rawLon) {
    return decodeLongitude(encodeLongitude(rawLon));
  }

  protected void addPolygonsToDoc(String field, Document doc, Polygon polygon) {
    Field[] fields = LatLonShape.createIndexableFields(field, polygon);
    for (Field f : fields) {
      doc.add(f);
    }
  }

  protected void addLineToDoc(String field, Document doc, Line line) {
    Field[] fields = LatLonShape.createIndexableFields(field, line);
    for (Field f : fields) {
      doc.add(f);
    }
  }

  protected Query newRectQuery(String field, double minLat, double maxLat, double minLon, double maxLon) {
    return LatLonShape.newBoxQuery(field, QueryRelation.INTERSECTS, minLat, maxLat, minLon, maxLon);
  }

  @Ignore
  public void testRandomPolygons() throws Exception {
    int numVertices;
    int numPolys = RandomNumbers.randomIntBetween(random(), 10, 20);

    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    Polygon polygon;
    Document document;
    for (int i = 0; i < numPolys;) {
      document = new Document();
      numVertices = TestUtil.nextInt(random(), 100000, 200000);
      polygon = GeoTestUtil.createRegularPolygon(0, 0, atLeast(1000000), numVertices);
      addPolygonsToDoc(FIELDNAME, document, polygon);
      writer.addDocument(document);
    }

    // search and verify we found our doc
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);
    assertEquals(0, searcher.count(newRectQuery("field", -89.9, -89.8, -179.9, -179.8d)));

    reader.close();
    writer.close();
    dir.close();
  }

  /** test we can search for a point with a standard number of vertices*/
  public void testBasicIntersects() throws Exception {
    int numVertices = TestUtil.nextInt(random(), 50, 100);
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    // add a random polygon document
    Polygon p = GeoTestUtil.createRegularPolygon(0, 90, atLeast(1000000), numVertices);
    Document document = new Document();
    addPolygonsToDoc(FIELDNAME, document, p);
    writer.addDocument(document);

    // add a line document
    document = new Document();
    // add a line string
    double lats[] = new double[p.numPoints() - 1];
    double lons[] = new double[p.numPoints() - 1];
    for (int i = 0; i < lats.length; ++i) {
      lats[i] = p.getPolyLat(i);
      lons[i] = p.getPolyLon(i);
    }
    Line l = new Line(lats, lons);
    addLineToDoc(FIELDNAME, document, l);
    writer.addDocument(document);

    ////// search /////
    // search an intersecting bbox
    IndexReader reader = writer.getReader();
    writer.close();
    IndexSearcher searcher = newSearcher(reader);
    double minLat = Math.min(lats[0], lats[1]);
    double minLon = Math.min(lons[0], lons[1]);
    double maxLat = Math.max(lats[0], lats[1]);
    double maxLon = Math.max(lons[0], lons[1]);
    Query q = newRectQuery(FIELDNAME, minLat, maxLat, minLon, maxLon);
    assertEquals(2, searcher.count(q));

    // search a disjoint bbox
    q = newRectQuery(FIELDNAME, p.minLat-1d, p.minLat+1, p.minLon-1d, p.minLon+1d);
    assertEquals(0, searcher.count(q));

    IOUtils.close(reader, dir);
  }

  /** test random polygons with a single hole */
  public void testPolygonWithHole() throws Exception {
    int numVertices = TestUtil.nextInt(random(), 50, 100);
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    // add a random polygon with a hole
    Polygon inner = new Polygon(new double[] {-1d, -1d, 1d, 1d, -1d},
        new double[] {-91d, -89d, -89d, -91.0, -91.0});
    Polygon outer = GeoTestUtil.createRegularPolygon(0, -90, atLeast(1000000), numVertices);

    Document document = new Document();
    addPolygonsToDoc(FIELDNAME, document, new Polygon(outer.getPolyLats(), outer.getPolyLons(), inner));
    writer.addDocument(document);

    ///// search //////
    IndexReader reader = writer.getReader();
    writer.close();
    IndexSearcher searcher = newSearcher(reader);

    // search a bbox in the hole
    Query q = newRectQuery(FIELDNAME, inner.minLat + 1e-6, inner.maxLat - 1e-6, inner.minLon + 1e-6, inner.maxLon - 1e-6);
    assertEquals(0, searcher.count(q));

    IOUtils.close(reader, dir);
  }

  /** test we can search for a point with a large number of vertices*/
  public void testLargeVertexPolygon() throws Exception {
    int numVertices = TestUtil.nextInt(random(), 200000, 500000);
    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setMergeScheduler(new SerialMergeScheduler());
    int mbd = iwc.getMaxBufferedDocs();
    if (mbd != -1 && mbd < numVertices/100) {
      iwc.setMaxBufferedDocs(numVertices/100);
    }
    Directory dir = newFSDirectory(createTempDir(getClass().getSimpleName()));
    IndexWriter writer = new IndexWriter(dir, iwc);

    // add a random polygon without a hole
    Polygon p = GeoTestUtil.createRegularPolygon(0, 90, atLeast(1000000), numVertices);
    Document document = new Document();
    addPolygonsToDoc(FIELDNAME, document, p);
    writer.addDocument(document);

    // add a random polygon with a hole
    Polygon inner = new Polygon(new double[] {-1d, -1d, 1d, 1d, -1d},
        new double[] {-91d, -89d, -89d, -91.0, -91.0});
    Polygon outer = GeoTestUtil.createRegularPolygon(0, -90, atLeast(1000000), numVertices);

    document = new Document();
    addPolygonsToDoc(FIELDNAME, document, new Polygon(outer.getPolyLats(), outer.getPolyLons(), inner));
    writer.addDocument(document);

    ////// search /////
    // search an intersecting bbox
    IndexReader reader = DirectoryReader.open(writer);
    writer.close();
    IndexSearcher searcher = newSearcher(reader);
    Query q = newRectQuery(FIELDNAME, -1d, 1d, p.minLon, p.maxLon);
    assertEquals(1, searcher.count(q));

    // search a disjoint bbox
    q = newRectQuery(FIELDNAME, p.minLat-1d, p.minLat+1, p.minLon-1d, p.minLon+1d);
    assertEquals(0, searcher.count(q));

    // search a bbox in the hole
    q = newRectQuery(FIELDNAME, inner.minLat + 1e-6, inner.maxLat - 1e-6, inner.minLon + 1e-6, inner.maxLon - 1e-6);
    assertEquals(0, searcher.count(q));

    IOUtils.close(reader, dir);
  }

  public void testLUCENE8454() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    Polygon poly = new Polygon(new double[] {-1.490648725633769E-132d, 90d, 90d, -1.490648725633769E-132d},
        new double[] {0d, 0d, 180d, 0d});

    Rectangle rectangle = new Rectangle(-29.46555603761226d, 0.0d, 8.381903171539307E-8d, 0.9999999403953552d);
    Rectangle2D rectangle2D = Rectangle2D.create(rectangle);

    Tessellator.Triangle t = Tessellator.tessellate(poly).get(0);

    byte[] encoded = new byte[7 * ShapeField.BYTES];
    ShapeField.encodeTriangle(encoded, encodeLatitude(t.getY(0)), encodeLongitude(t.getX(0)), t.isEdgefromPolygon(0),
                                       encodeLatitude(t.getY(1)), encodeLongitude(t.getX(1)), t.isEdgefromPolygon(1),
                                       encodeLatitude(t.getY(2)), encodeLongitude(t.getX(2)), t.isEdgefromPolygon(2));
    ShapeField.DecodedTriangle decoded = new ShapeField.DecodedTriangle();
    ShapeField.decodeTriangle(encoded, decoded);

    int expected =rectangle2D.intersectsTriangle(decoded.aX, decoded.aY, decoded.bX, decoded.bY, decoded.cX, decoded.cY) ? 0 : 1;

    Document document = new Document();
    addPolygonsToDoc(FIELDNAME, document, poly);
    writer.addDocument(document);

    ///// search //////
    IndexReader reader = writer.getReader();
    writer.close();
    IndexSearcher searcher = newSearcher(reader);

    // search a bbox in the hole
    Query q = LatLonShape.newBoxQuery(FIELDNAME, QueryRelation.DISJOINT,-29.46555603761226d, 0.0d, 8.381903171539307E-8d, 0.9999999403953552d);
    assertEquals(expected, searcher.count(q));

    IOUtils.close(reader, dir);
  }

  public void testPointIndexAndQuery() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document document = new Document();
    BaseLatLonShapeTestCase.Point p = (BaseLatLonShapeTestCase.Point) BaseLatLonShapeTestCase.ShapeType.POINT.nextShape();
    Field[] fields = LatLonShape.createIndexableFields(FIELDNAME, p.lat, p.lon);
    for (Field f : fields) {
      document.add(f);
    }
    writer.addDocument(document);

    //// search
    IndexReader r = writer.getReader();
    writer.close();
    IndexSearcher s = newSearcher(r);

    // search by same point
    Query q = LatLonShape.newBoxQuery(FIELDNAME, QueryRelation.INTERSECTS, p.lat, p.lat, p.lon, p.lon);
    assertEquals(1, s.count(q));
    IOUtils.close(r, dir);
  }

  public void testLUCENE8669() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();

    Polygon indexPoly1 = new Polygon(
        new double[] {-7.5d, 15d, 15d, 0d, -7.5d},
        new double[] {-180d, -180d, -176d, -176d, -180d}
    );

    Polygon indexPoly2 = new Polygon(
        new double[] {15d, -7.5d, -15d, -10d, 15d, 15d},
        new double[] {180d, 180d, 176d, 174d, 176d, 180d}
    );

    addPolygonsToDoc(FIELDNAME, doc, indexPoly1);
    addPolygonsToDoc(FIELDNAME, doc, indexPoly2);
    w.addDocument(doc);
    w.forceMerge(1);

    ///// search //////
    IndexReader reader = w.getReader();
    w.close();
    IndexSearcher searcher = newSearcher(reader);

    Polygon[] searchPoly = new Polygon[] {
        new Polygon(new double[] {-20d, 20d, 20d, -20d, -20d},
            new double[] {-180d, -180d, -170d, -170d, -180d}),
        new Polygon(new double[] {20d, -20d, -20d, 20d, 20d},
            new double[] {180d, 180d, 170d, 170d, 180d})
    };

    Query q = LatLonShape.newPolygonQuery(FIELDNAME, QueryRelation.WITHIN, searchPoly);
    assertEquals(1, searcher.count(q));

    q = LatLonShape.newPolygonQuery(FIELDNAME, QueryRelation.INTERSECTS, searchPoly);
    assertEquals(1, searcher.count(q));

    q = LatLonShape.newPolygonQuery(FIELDNAME, QueryRelation.DISJOINT, searchPoly);
    assertEquals(0, searcher.count(q));

    q = LatLonShape.newBoxQuery(FIELDNAME, QueryRelation.WITHIN, -20, 20, 170, -170);
    assertEquals(1, searcher.count(q));

    q = LatLonShape.newBoxQuery(FIELDNAME, QueryRelation.INTERSECTS, -20, 20, 170, -170);
    assertEquals(1, searcher.count(q));

    q = LatLonShape.newBoxQuery(FIELDNAME, QueryRelation.DISJOINT, -20, 20, 170, -170);
    assertEquals(0, searcher.count(q));

    IOUtils.close(w, reader, dir);
  }

  public void testLUCENE8679() {
    double alat = 1.401298464324817E-45;
    double alon = 24.76789767911785;
    double blat = 34.26468306870807;
    double blon = -52.67048754768767;
    Polygon polygon = new Polygon(new double[] {-14.448264200949083, 0, 0, -14.448264200949083, -14.448264200949083},
        new double[] {0.9999999403953552, 0.9999999403953552, 124.50086371762484, 124.50086371762484, 0.9999999403953552});
    Component2D polygon2D = Polygon2D.create(polygon);
    PointValues.Relation rel = polygon2D.relateTriangle(
        quantizeLon(alon), quantizeLat(blat),
        quantizeLon(blon), quantizeLat(blat),
        quantizeLon(alon), quantizeLat(alat));

    assertEquals(PointValues.Relation.CELL_CROSSES_QUERY, rel);

    rel = polygon2D.relateTriangle(
        quantizeLon(alon), quantizeLat(blat),
        quantizeLon(alon), quantizeLat(alat),
        quantizeLon(blon), quantizeLat(blat));

    assertEquals(PointValues.Relation.CELL_CROSSES_QUERY, rel);
  }

  public void testTriangleTouchingEdges() {
    Polygon p = new Polygon(new double[] {0, 0, 1, 1, 0}, new double[] {0, 1, 1, 0, 0});
    Component2D polygon2D = Polygon2D.create(p);
    //3 shared points
    PointValues.Relation rel = polygon2D.relateTriangle(
        quantizeLon(0.5), quantizeLat(0),
        quantizeLon(1), quantizeLat(0.5),
        quantizeLon(0.5), quantizeLat(1));
    assertEquals(PointValues.Relation.CELL_INSIDE_QUERY, rel);
    //2 shared points
    rel = polygon2D.relateTriangle(
        quantizeLon(0.5), quantizeLat(0),
        quantizeLon(1), quantizeLat(0.5),
        quantizeLon(0.5), quantizeLat(0.75));
    assertEquals(PointValues.Relation.CELL_INSIDE_QUERY, rel);
    //1 shared point
    rel = polygon2D.relateTriangle(
        quantizeLon(0.5), quantizeLat(0.5),
        quantizeLon(0.5), quantizeLat(0),
        quantizeLon(0.75), quantizeLat(0.75));
    assertEquals(PointValues.Relation.CELL_INSIDE_QUERY, rel);
    // 1 shared point but out
    rel = polygon2D.relateTriangle(
        quantizeLon(1), quantizeLat(0.5),
        quantizeLon(2), quantizeLat(0),
        quantizeLon(2), quantizeLat(2));
    assertEquals(PointValues.Relation.CELL_CROSSES_QUERY, rel);
    // 1 shared point but crossing
    rel = polygon2D.relateTriangle(
        quantizeLon(0.5), quantizeLat(0),
        quantizeLon(2), quantizeLat(0.5),
        quantizeLon(0.5), quantizeLat(1));
    assertEquals(PointValues.Relation.CELL_CROSSES_QUERY, rel);
    //share one edge
    rel = polygon2D.relateTriangle(
        quantizeLon(0), quantizeLat(0),
        quantizeLon(0), quantizeLat(1),
        quantizeLon(0.5), quantizeLat(0.5));
    assertEquals(PointValues.Relation.CELL_INSIDE_QUERY, rel);
    //share one edge outside
    rel = polygon2D.relateTriangle(
        quantizeLon(0), quantizeLat(1),
        quantizeLon(1.5), quantizeLat(1.5),
        quantizeLon(1), quantizeLat(1));
    assertEquals(PointValues.Relation.CELL_CROSSES_QUERY, rel);
  }

  public void testLUCENE8736() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    // test polygons:
    Polygon indexPoly1 = new Polygon(
        new double[] {4d, 4d, 3d, 3d, 4d},
        new double[] {3d, 4d, 4d, 3d, 3d}
    );

    Polygon indexPoly2 = new Polygon(
        new double[] {2d, 2d, 1d, 1d, 2d},
        new double[] {6d, 7d, 7d, 6d, 6d}
    );

    Polygon indexPoly3 = new Polygon(
        new double[] {1d, 1d, 0d, 0d, 1d},
        new double[] {3d, 4d, 4d, 3d, 3d}
    );

    Polygon indexPoly4 = new Polygon(
        new double[] {2d, 2d, 1d, 1d, 2d},
        new double[] {0d, 1d, 1d, 0d, 0d}
    );

    // index polygons:
    Document doc;
    addPolygonsToDoc(FIELDNAME, doc = new Document(), indexPoly1);
    w.addDocument(doc);
    addPolygonsToDoc(FIELDNAME, doc = new Document(), indexPoly2);
    w.addDocument(doc);
    addPolygonsToDoc(FIELDNAME, doc = new Document(), indexPoly3);
    w.addDocument(doc);
    addPolygonsToDoc(FIELDNAME, doc = new Document(), indexPoly4);
    w.addDocument(doc);
    w.forceMerge(1);

    ///// search //////
    IndexReader reader = w.getReader();
    w.close();
    IndexSearcher searcher = newSearcher(reader);

    Polygon[] searchPoly = new Polygon[] {
        new Polygon(new double[] {4d, 4d, 0d, 0d, 4d},
            new double[] {0d, 7d, 7d, 0d, 0d})
    };

    Query q = LatLonShape.newPolygonQuery(FIELDNAME, QueryRelation.WITHIN, searchPoly);
    assertEquals(4, searcher.count(q));

    IOUtils.close(w, reader, dir);
  }

  public void testTriangleCrossingPolygonVertices() {
    Polygon p = new Polygon(new double[] {0, 0, -5, -10, -5, 0}, new double[] {-1, 1, 5, 0, -5, -1});
    Component2D polygon2D = Polygon2D.create(p);
    PointValues.Relation rel = polygon2D.relateTriangle(
        quantizeLon(-5), quantizeLat(0),
        quantizeLon(10), quantizeLat(0),
        quantizeLon(-5), quantizeLat(-15));
    assertEquals(PointValues.Relation.CELL_CROSSES_QUERY, rel);
  }

  public void testLineCrossingPolygonVertices() {
    Polygon p = new Polygon(new double[] {0, -1, 0, 1, 0}, new double[] {-1, 0, 1, 0, -1});
    Component2D polygon2D = Polygon2D.create(p);
    PointValues.Relation rel = polygon2D.relateTriangle(
        quantizeLon(-1.5), quantizeLat(0),
        quantizeLon(1.5), quantizeLat(0),
        quantizeLon(-1.5), quantizeLat(0));
    assertEquals(PointValues.Relation.CELL_CROSSES_QUERY, rel);
  }

  public void testLineSharedLine() {
    Line l = new Line(new double[] {0, 0, 0, 0}, new double[] {-2, -1, 0, 1});
    Component2D l2d = Line2D.create(l);
    PointValues.Relation r = l2d.relateTriangle(
        quantizeLon(-5), quantizeLat(0),
        quantizeLon(5), quantizeLat(0),
        quantizeLon(-5), quantizeLat(0));
    assertEquals(PointValues.Relation.CELL_CROSSES_QUERY, r);
  }
}
