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

import java.util.Arrays;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import org.apache.lucene.document.LatLonShape.QueryRelation;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.geo.GeoTestUtil;
import org.apache.lucene.geo.GeoUtils;
import org.apache.lucene.geo.Line;
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

import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitude;

/** Test case for indexing polygons and querying by bounding box */
public class TestLatLonShape extends LuceneTestCase {
  protected static String FIELDNAME = "field";
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

    byte[] encoded = new byte[7 * LatLonShape.BYTES];
    LatLonShape.encodeTriangle(encoded, encodeLatitude(t.getLat(0)), encodeLongitude(t.getLon(0)),
        encodeLatitude(t.getLat(1)), encodeLongitude(t.getLon(1)), encodeLatitude(t.getLat(2)), encodeLongitude(t.getLon(2)));
    int[] decoded = new int[6];
    LatLonShape.decodeTriangle(encoded, decoded);

    int expected =rectangle2D.intersectsTriangle(decoded[1], decoded[0], decoded[3], decoded[2], decoded[5], decoded[4]) ? 0 : 1;

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

  //One shared point with MBR -> MinLat, MinLon
  public void testPolygonEncodingMinLatMinLon() {
    double alat = 0.0;
    double alon = 0.0;
    double blat = 1.0;
    double blon = 2.0;
    double clat = 2.0;
    double clon = 1.0;
    int alatEnc = GeoEncodingUtils.encodeLatitude(alat);
    int alonEnc = GeoEncodingUtils.encodeLongitude(alon);
    int blatEnc = GeoEncodingUtils.encodeLatitude(blat);
    int blonEnc = GeoEncodingUtils.encodeLongitude(blon);
    int clatEnc = GeoEncodingUtils.encodeLatitude(clat);
    int clonEnc = GeoEncodingUtils.encodeLongitude(clon);
    verifyEncodingPermutations(alatEnc, alonEnc, blatEnc, blonEnc, clatEnc, clonEnc);
    byte[] b = new byte[7 * LatLonShape.BYTES];
    LatLonShape.encodeTriangle(b, alatEnc, alonEnc, blatEnc, blonEnc, clatEnc, clonEnc);
    int[] encoded = new int[6];
    LatLonShape.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == alatEnc);
    assertTrue(encoded[1] == alonEnc);
    assertTrue(encoded[2] == blatEnc);
    assertTrue(encoded[3] == blonEnc);
    assertTrue(encoded[4] == clatEnc);
    assertTrue(encoded[5] == clonEnc);
  }

  //One shared point with MBR -> MinLat, MaxLon
  public void testPolygonEncodingMinLatMaxLon() {
    double alat = 1.0;
    double alon = 0.0;
    double blat = 0.0;
    double blon = 2.0;
    double clat = 2.0;
    double clon = 1.0;
    int alatEnc = GeoEncodingUtils.encodeLatitude(alat);
    int alonEnc = GeoEncodingUtils.encodeLongitude(alon);
    int blatEnc = GeoEncodingUtils.encodeLatitude(blat);
    int blonEnc = GeoEncodingUtils.encodeLongitude(blon);
    int clatEnc = GeoEncodingUtils.encodeLatitude(clat);
    int clonEnc = GeoEncodingUtils.encodeLongitude(clon);
    verifyEncodingPermutations(alatEnc, alonEnc, blatEnc, blonEnc, clatEnc, clonEnc);
    byte[] b = new byte[7 * LatLonShape.BYTES];
    LatLonShape.encodeTriangle(b, alatEnc, alonEnc, blatEnc, blonEnc, clatEnc, clonEnc);
    int[] encoded = new int[6];
    LatLonShape.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == alatEnc);
    assertTrue(encoded[1] == alonEnc);
    assertTrue(encoded[2] == blatEnc);
    assertTrue(encoded[3] == blonEnc);
    assertTrue(encoded[4] == clatEnc);
    assertTrue(encoded[5] == clonEnc);
  }

  //One shared point with MBR -> MaxLat, MaxLon
  public void testPolygonEncodingMaxLatMaxLon() {
    double alat = 1.0;
    double alon = 0.0;
    double blat = 2.0;
    double blon = 2.0;
    double clat = 0.0;
    double clon = 1.0;
    int alatEnc = GeoEncodingUtils.encodeLatitude(alat);
    int alonEnc = GeoEncodingUtils.encodeLongitude(alon);
    int blatEnc = GeoEncodingUtils.encodeLatitude(clat);
    int blonEnc = GeoEncodingUtils.encodeLongitude(clon);
    int clatEnc = GeoEncodingUtils.encodeLatitude(blat);
    int clonEnc = GeoEncodingUtils.encodeLongitude(blon);
    verifyEncodingPermutations(alatEnc, alonEnc, blatEnc, blonEnc, clatEnc, clonEnc);
    byte[] b = new byte[7 * LatLonShape.BYTES];
    LatLonShape.encodeTriangle(b, alatEnc, alonEnc, blatEnc, blonEnc, clatEnc, clonEnc);
    int[] encoded = new int[6];
    LatLonShape.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == alatEnc);
    assertTrue(encoded[1] == alonEnc);
    assertTrue(encoded[2] == blatEnc);
    assertTrue(encoded[3] == blonEnc);
    assertTrue(encoded[4] == clatEnc);
    assertTrue(encoded[5] == clonEnc);
  }

  //One shared point with MBR -> MaxLat, MinLon
  public void testPolygonEncodingMaxLatMinLon() {
    double alat = 2.0;
    double alon = 0.0;
    double blat = 1.0;
    double blon = 2.0;
    double clat = 0.0;
    double clon = 1.0;
    int alatEnc = GeoEncodingUtils.encodeLatitude(alat);
    int alonEnc = GeoEncodingUtils.encodeLongitude(alon);
    int blatEnc = GeoEncodingUtils.encodeLatitude(clat);
    int blonEnc = GeoEncodingUtils.encodeLongitude(clon);
    int clatEnc = GeoEncodingUtils.encodeLatitude(blat);
    int clonEnc = GeoEncodingUtils.encodeLongitude(blon);
    verifyEncodingPermutations(alatEnc, alonEnc, blatEnc, blonEnc, clatEnc, clonEnc);
    byte[] b = new byte[7 * LatLonShape.BYTES];
    LatLonShape.encodeTriangle(b, alatEnc, alonEnc, blatEnc, blonEnc, clatEnc, clonEnc);
    int[] encoded = new int[6];
    LatLonShape.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == alatEnc);
    assertTrue(encoded[1] == alonEnc);
    assertTrue(encoded[2] == blatEnc);
    assertTrue(encoded[3] == blonEnc);
    assertTrue(encoded[4] == clatEnc);
    assertTrue(encoded[5] == clonEnc);
  }

  //Two shared point with MBR -> [MinLat, MinLon], [MaxLat, MaxLon], third point below
  public void testPolygonEncodingMinLatMinLonMaxLatMaxLonBelow() {
    double alat = 0.0;
    double alon = 0.0;
    double blat = 0.25;
    double blon = 0.75;
    double clat = 2.0;
    double clon = 2.0;
    int alatEnc = GeoEncodingUtils.encodeLatitude(alat);
    int alonEnc = GeoEncodingUtils.encodeLongitude(alon);
    int blatEnc = GeoEncodingUtils.encodeLatitude(blat);
    int blonEnc = GeoEncodingUtils.encodeLongitude(blon);
    int clatEnc = GeoEncodingUtils.encodeLatitude(clat);
    int clonEnc = GeoEncodingUtils.encodeLongitude(clon);
    verifyEncodingPermutations(alatEnc, alonEnc, blatEnc, blonEnc, clatEnc, clonEnc);
    byte[] b = new byte[7 * LatLonShape.BYTES];
    LatLonShape.encodeTriangle(b, alatEnc, alonEnc, blatEnc, blonEnc, clatEnc, clonEnc);
    int[] encoded = new int[6];
    LatLonShape.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == alatEnc);
    assertTrue(encoded[1] == alonEnc);
    assertTrue(encoded[2] == blatEnc);
    assertTrue(encoded[3] == blonEnc);
    assertTrue(encoded[4] == clatEnc);
    assertTrue(encoded[5] == clonEnc);
  }

  //Two shared point with MBR -> [MinLat, MinLon], [MaxLat, MaxLon], third point above
  public void testPolygonEncodingMinLatMinLonMaxLatMaxLonAbove() {
    double alat = 0.0;
    double alon = 0.0;
    double blat = 2.0;
    double blon = 2.0;
    double clat = 1.75;
    double clon = 1.25;
    int alatEnc = GeoEncodingUtils.encodeLatitude(alat);
    int alonEnc = GeoEncodingUtils.encodeLongitude(alon);
    int blatEnc = GeoEncodingUtils.encodeLatitude(blat);
    int blonEnc = GeoEncodingUtils.encodeLongitude(blon);
    int clatEnc = GeoEncodingUtils.encodeLatitude(clat);
    int clonEnc = GeoEncodingUtils.encodeLongitude(clon);
    verifyEncodingPermutations(alatEnc, alonEnc, blatEnc, blonEnc, clatEnc, clonEnc);
    byte[] b = new byte[7 * LatLonShape.BYTES];
    LatLonShape.encodeTriangle(b, alatEnc, alonEnc, blatEnc, blonEnc, clatEnc, clonEnc);
    int[] encoded = new int[6];
    LatLonShape.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == alatEnc);
    assertTrue(encoded[1] == alonEnc);
    assertTrue(encoded[2] == blatEnc);
    assertTrue(encoded[3] == blonEnc);
    assertTrue(encoded[4] == clatEnc);
    assertTrue(encoded[5] == clonEnc);
  }

  //Two shared point with MBR -> [MinLat, MaxLon], [MaxLat, MinLon], third point below
  public void testPolygonEncodingMinLatMaxLonMaxLatMinLonBelow() {
    double alat = 2.0;
    double alon = 0.0;
    double blat = 0.25;
    double blon = 0.75;
    double clat = 0.0;
    double clon = 2.0;
    int alatEnc = GeoEncodingUtils.encodeLatitude(alat);
    int alonEnc = GeoEncodingUtils.encodeLongitude(alon);
    int blatEnc = GeoEncodingUtils.encodeLatitude(blat);
    int blonEnc = GeoEncodingUtils.encodeLongitude(blon);
    int clatEnc = GeoEncodingUtils.encodeLatitude(clat);
    int clonEnc = GeoEncodingUtils.encodeLongitude(clon);
    verifyEncodingPermutations(alatEnc, alonEnc, blatEnc, blonEnc, clatEnc, clonEnc);
    byte[] b = new byte[7 * LatLonShape.BYTES];
    LatLonShape.encodeTriangle(b, alatEnc, alonEnc, blatEnc, blonEnc, clatEnc, clonEnc);
    int[] encoded = new int[6];
    LatLonShape.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == alatEnc);
    assertTrue(encoded[1] == alonEnc);
    assertTrue(encoded[2] == blatEnc);
    assertTrue(encoded[3] == blonEnc);
    assertTrue(encoded[4] == clatEnc);
    assertTrue(encoded[5] == clonEnc);
  }

  //Two shared point with MBR -> [MinLat, MaxLon], [MaxLat, MinLon], third point above
  public void testPolygonEncodingMinLatMaxLonMaxLatMinLonAbove() {
    double alat = 2.0;
    double alon = 0.0;
    double blat = 0.0;
    double blon = 2.0;
    double clat = 1.75;
    double clon = 1.25;
    int alatEnc = GeoEncodingUtils.encodeLatitude(alat);
    int alonEnc = GeoEncodingUtils.encodeLongitude(alon);
    int blatEnc = GeoEncodingUtils.encodeLatitude(blat);
    int blonEnc = GeoEncodingUtils.encodeLongitude(blon);
    int clatEnc = GeoEncodingUtils.encodeLatitude(clat);
    int clonEnc = GeoEncodingUtils.encodeLongitude(clon);
    verifyEncodingPermutations(alatEnc, alonEnc, blatEnc, blonEnc, clatEnc, clonEnc);
    byte[] b = new byte[7 * LatLonShape.BYTES];
    LatLonShape.encodeTriangle(b, alatEnc, alonEnc, blatEnc, blonEnc, clatEnc, clonEnc);
    int[] encoded = new int[6];
    LatLonShape.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == alatEnc);
    assertTrue(encoded[1] == alonEnc);
    assertTrue(encoded[2] == blatEnc);
    assertTrue(encoded[3] == blonEnc);
    assertTrue(encoded[4] == clatEnc);
    assertTrue(encoded[5] == clonEnc);
  }

  //all points shared with MBR
  public void testPolygonEncodingAllSharedAbove() {
    double alat = 0.0;
    double alon = 0.0;
    double blat = 0.0;
    double blon = 2.0;
    double clat = 2.0;
    double clon = 2.0;
    int alatEnc = GeoEncodingUtils.encodeLatitude(alat);
    int alonEnc = GeoEncodingUtils.encodeLongitude(alon);
    int blatEnc = GeoEncodingUtils.encodeLatitude(blat);
    int blonEnc = GeoEncodingUtils.encodeLongitude(blon);
    int clatEnc = GeoEncodingUtils.encodeLatitude(clat);
    int clonEnc = GeoEncodingUtils.encodeLongitude(clon);
    verifyEncodingPermutations(alatEnc, alonEnc, blatEnc, blonEnc, clatEnc, clonEnc);
    byte[] b = new byte[7 * LatLonShape.BYTES];
    LatLonShape.encodeTriangle(b, alatEnc, alonEnc, blatEnc, blonEnc, clatEnc, clonEnc);
    int[] encoded = new int[6];
    LatLonShape.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == alatEnc);
    assertTrue(encoded[1] == alonEnc);
    assertTrue(encoded[2] == blatEnc);
    assertTrue(encoded[3] == blonEnc);
    assertTrue(encoded[4] == clatEnc);
    assertTrue(encoded[5] == clonEnc);
  }

  //all points shared with MBR
  public void testPolygonEncodingAllSharedBelow() {
    double alat = 2.0;
    double alon = 0.0;
    double blat = 0.0;
    double blon = 0.0;
    double clat = 2.0;
    double clon = 2.0;
    int alatEnc = GeoEncodingUtils.encodeLatitude(alat);
    int alonEnc = GeoEncodingUtils.encodeLongitude(alon);
    int blatEnc = GeoEncodingUtils.encodeLatitude(blat);
    int blonEnc = GeoEncodingUtils.encodeLongitude(blon);
    int clatEnc = GeoEncodingUtils.encodeLatitude(clat);
    int clonEnc = GeoEncodingUtils.encodeLongitude(clon);
    byte[] b = new byte[7 * LatLonShape.BYTES];
    LatLonShape.encodeTriangle(b, alatEnc, alonEnc, blatEnc, blonEnc, clatEnc, clonEnc);
    int[] encoded = new int[6];
    LatLonShape.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == alatEnc);
    assertTrue(encoded[1] == alonEnc);
    assertTrue(encoded[2] == blatEnc);
    assertTrue(encoded[3] == blonEnc);
    assertTrue(encoded[4] == clatEnc);
    assertTrue(encoded[5] == clonEnc);
  }

  //[a,b,c] == [c,a,b] == [b,c,a] == [c,b,a] == [b,a,c] == [a,c,b]
  public void verifyEncodingPermutations(int alatEnc, int alonEnc, int blatEnc, int blonEnc, int clatEnc, int clonEnc) {
    //this is only valid when points are not co-planar
    assertTrue(GeoUtils.orient(alatEnc, alonEnc, blatEnc, blonEnc, clatEnc, clonEnc) != 0);
    byte[] b = new byte[7 * LatLonShape.BYTES];
    //[a,b,c]
    LatLonShape.encodeTriangle(b, alatEnc, alonEnc, blatEnc, blonEnc, clatEnc, clonEnc);
    int[] encodedABC = new int[6];
    LatLonShape.decodeTriangle(b, encodedABC);
    //[c,a,b]
    LatLonShape.encodeTriangle(b, clatEnc, clonEnc, alatEnc, alonEnc, blatEnc, blonEnc);
    int[] encodedCAB = new int[6];
    LatLonShape.decodeTriangle(b, encodedCAB);
    assertTrue(Arrays.equals(encodedABC, encodedCAB));
    //[b,c,a]
    LatLonShape.encodeTriangle(b, blatEnc, blonEnc, clatEnc, clonEnc, alatEnc, alonEnc);
    int[] encodedBCA = new int[6];
    LatLonShape.decodeTriangle(b, encodedBCA);
    assertTrue(Arrays.equals(encodedABC, encodedBCA));
    //[c,b,a]
    LatLonShape.encodeTriangle(b, clatEnc, clonEnc, blatEnc, blonEnc, alatEnc, alonEnc);
    int[] encodedCBA= new int[6];
    LatLonShape.decodeTriangle(b, encodedCBA);
    assertTrue(Arrays.equals(encodedABC, encodedCBA));
    //[b,a,c]
    LatLonShape.encodeTriangle(b, blatEnc, blonEnc, alatEnc, alonEnc, clatEnc, clonEnc);
    int[] encodedBAC= new int[6];
    LatLonShape.decodeTriangle(b, encodedBAC);
    assertTrue(Arrays.equals(encodedABC, encodedBAC));
    //[a,c,b]
    LatLonShape.encodeTriangle(b, alatEnc, alonEnc, clatEnc, clonEnc, blatEnc, blonEnc);
    int[] encodedACB= new int[6];
    LatLonShape.decodeTriangle(b, encodedACB);
    assertTrue(Arrays.equals(encodedABC, encodedACB));
  }

  public void testPointEncoding() {
    double lat = 45.0;
    double lon = 45.0;
    int latEnc = GeoEncodingUtils.encodeLatitude(lat);
    int lonEnc = GeoEncodingUtils.encodeLongitude(lon);
    byte[] b = new byte[7 * LatLonShape.BYTES];
    LatLonShape.encodeTriangle(b, latEnc, lonEnc, latEnc, lonEnc, latEnc, lonEnc);
    int[] encoded = new int[6];
    LatLonShape.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == latEnc && encoded[2] == latEnc && encoded[4] == latEnc);
    assertTrue(encoded[1] == lonEnc && encoded[3] == lonEnc && encoded[5] == lonEnc);
  }

  public void testLineEncodingSameLat() {
    double lat = 2.0;
    double alon = 0.0;
    double blon = 2.0;
    int latEnc = GeoEncodingUtils.encodeLatitude(lat);
    int alonEnc = GeoEncodingUtils.encodeLongitude(alon);
    int blonEnc = GeoEncodingUtils.encodeLongitude(blon);
    byte[] b = new byte[7 * LatLonShape.BYTES];
    LatLonShape.encodeTriangle(b, latEnc, alonEnc, latEnc, blonEnc, latEnc, alonEnc);
    int[] encoded = new int[6];
    LatLonShape.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == latEnc);
    assertTrue(encoded[1] == alonEnc);
    assertTrue(encoded[2] == latEnc);
    assertTrue(encoded[3] == blonEnc);
    assertTrue(encoded[4] == latEnc);
    assertTrue(encoded[5] == alonEnc);
    LatLonShape.encodeTriangle(b, latEnc, alonEnc, latEnc, alonEnc, latEnc, blonEnc);
    encoded = new int[6];
    LatLonShape.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == latEnc);
    assertTrue(encoded[1] == alonEnc);
    assertTrue(encoded[2] == latEnc);
    assertTrue(encoded[3] == alonEnc);
    assertTrue(encoded[4] == latEnc);
    assertTrue(encoded[5] == blonEnc);
    LatLonShape.encodeTriangle(b, latEnc, blonEnc, latEnc, alonEnc, latEnc, alonEnc);
    encoded = new int[6];
    LatLonShape.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == latEnc);
    assertTrue(encoded[1] == alonEnc);
    assertTrue(encoded[2] == latEnc);
    assertTrue(encoded[3] == blonEnc);
    assertTrue(encoded[4] == latEnc);
    assertTrue(encoded[5] == alonEnc);
  }

  public void testLineEncodingSameLon() {
    double alat = 0.0;
    double blat = 2.0;
    double lon = 2.0;
    int alatEnc = GeoEncodingUtils.encodeLatitude(alat);
    int blatEnc = GeoEncodingUtils.encodeLatitude(blat);
    int lonEnc = GeoEncodingUtils.encodeLongitude(lon);
    byte[] b = new byte[7 * LatLonShape.BYTES];
    LatLonShape.encodeTriangle(b, alatEnc, lonEnc, blatEnc, lonEnc, alatEnc, lonEnc);
    int[] encoded = new int[6];
    LatLonShape.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == alatEnc);
    assertTrue(encoded[1] == lonEnc);
    assertTrue(encoded[2] == blatEnc);
    assertTrue(encoded[3] == lonEnc);
    assertTrue(encoded[4] == alatEnc);
    assertTrue(encoded[5] == lonEnc);
    LatLonShape.encodeTriangle(b, alatEnc, lonEnc, alatEnc, lonEnc, blatEnc, lonEnc);
    encoded = new int[6];
    LatLonShape.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == alatEnc);
    assertTrue(encoded[1] == lonEnc);
    assertTrue(encoded[2] == alatEnc);
    assertTrue(encoded[3] == lonEnc);
    assertTrue(encoded[4] == blatEnc);
    assertTrue(encoded[5] == lonEnc);
    LatLonShape.encodeTriangle(b, blatEnc, lonEnc, alatEnc, lonEnc, alatEnc, lonEnc);
    encoded = new int[6];
    LatLonShape.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == alatEnc);
    assertTrue(encoded[1] == lonEnc);
    assertTrue(encoded[2] == blatEnc);
    assertTrue(encoded[3] == lonEnc);
    assertTrue(encoded[4] == alatEnc);
    assertTrue(encoded[5] == lonEnc);
  }

  public void testLineEncoding() {
    double alat = 0.0;
    double blat = 2.0;
    double alon = 0.0;
    double blon = 2.0;
    int alatEnc = GeoEncodingUtils.encodeLatitude(alat);
    int blatEnc = GeoEncodingUtils.encodeLatitude(blat);
    int alonEnc = GeoEncodingUtils.encodeLongitude(alon);
    int blonEnc = GeoEncodingUtils.encodeLongitude(blon);
    byte[] b = new byte[7 * LatLonShape.BYTES];
    LatLonShape.encodeTriangle(b, alatEnc, alonEnc, blatEnc, blonEnc, alatEnc, alonEnc);
    int[] encoded = new int[6];
    LatLonShape.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == alatEnc);
    assertTrue(encoded[1] == alonEnc);
    assertTrue(encoded[2] == blatEnc);
    assertTrue(encoded[3] == blonEnc);
    assertTrue(encoded[4] == alatEnc);
    assertTrue(encoded[5] == alonEnc);
    LatLonShape.encodeTriangle(b, alatEnc, alonEnc, alatEnc, alonEnc, blatEnc, blonEnc);
    encoded = new int[6];
    LatLonShape.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == alatEnc);
    assertTrue(encoded[1] == alonEnc);
    assertTrue(encoded[2] == alatEnc);
    assertTrue(encoded[3] == alonEnc);
    assertTrue(encoded[4] == blatEnc);
    assertTrue(encoded[5] == blonEnc);
    LatLonShape.encodeTriangle(b, blatEnc, blonEnc, alatEnc, alonEnc, alatEnc, alonEnc);
    encoded = new int[6];
    LatLonShape.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == alatEnc);
    assertTrue(encoded[1] == alonEnc);
    assertTrue(encoded[2] == blatEnc);
    assertTrue(encoded[3] == blonEnc);
    assertTrue(encoded[4] == alatEnc);
    assertTrue(encoded[5] == alonEnc);
  }

  public void testRandomPointEncoding() {
    double alat = GeoTestUtil.nextLatitude();
    double alon = GeoTestUtil.nextLongitude();
    verifyEncoding(alat, alon, alat, alon, alat, alon);
  }

  public void testRandomLineEncoding() {
    double alat = GeoTestUtil.nextLatitude();
    double alon = GeoTestUtil.nextLongitude();
    double blat = GeoTestUtil.nextLatitude();
    double blon = GeoTestUtil.nextLongitude();
    verifyEncoding(alat, alon, blat, blon, alat, alon);
  }

  public void testRandomPolygonEncoding() {
    double alat = GeoTestUtil.nextLatitude();
    double alon = GeoTestUtil.nextLongitude();
    double blat = GeoTestUtil.nextLatitude();
    double blon = GeoTestUtil.nextLongitude();
    double clat = GeoTestUtil.nextLatitude();
    double clon = GeoTestUtil.nextLongitude();
    verifyEncoding(alat, alon, blat, blon, clat, clon);
  }

  private void verifyEncoding(double alat, double alon, double blat, double blon, double clat, double clon) {
    int[] original = new int[]{GeoEncodingUtils.encodeLatitude(alat),
        GeoEncodingUtils.encodeLongitude(alon),
        GeoEncodingUtils.encodeLatitude(blat),
        GeoEncodingUtils.encodeLongitude(blon),
        GeoEncodingUtils.encodeLatitude(clat),
        GeoEncodingUtils.encodeLongitude(clon)};

    //quantize the triangle
    byte[] b = new byte[7 * LatLonShape.BYTES];
    LatLonShape.encodeTriangle(b, original[0], original[1], original[2], original[3], original[4], original[5]);
    int[] encoded = new int[6];
    LatLonShape.decodeTriangle(b, encoded);
    double[] encodedQuantize = new double[] {GeoEncodingUtils.decodeLatitude(encoded[0]),
        GeoEncodingUtils.decodeLongitude(encoded[1]),
        GeoEncodingUtils.decodeLatitude(encoded[2]),
        GeoEncodingUtils.decodeLongitude(encoded[3]),
        GeoEncodingUtils.decodeLatitude(encoded[4]),
        GeoEncodingUtils.decodeLongitude(encoded[5])};

    int orientation = GeoUtils.orient(original[1], original[0], original[3], original[2], original[5], original[4]);
    //quantize original
    double[] originalQuantize;
    //we need to change the orientation if CW
    if (orientation == -1) {
      originalQuantize = new double[] {GeoEncodingUtils.decodeLatitude(original[4]),
          GeoEncodingUtils.decodeLongitude(original[5]),
          GeoEncodingUtils.decodeLatitude(original[2]),
          GeoEncodingUtils.decodeLongitude(original[3]),
          GeoEncodingUtils.decodeLatitude(original[0]),
          GeoEncodingUtils.decodeLongitude(original[1])};
    } else {
      originalQuantize = new double[] {GeoEncodingUtils.decodeLatitude(original[0]),
          GeoEncodingUtils.decodeLongitude(original[1]),
          GeoEncodingUtils.decodeLatitude(original[2]),
          GeoEncodingUtils.decodeLongitude(original[3]),
          GeoEncodingUtils.decodeLatitude(original[4]),
          GeoEncodingUtils.decodeLongitude(original[5])};
    }

    for (int i =0; i < 100; i ++) {
      Polygon polygon = GeoTestUtil.nextPolygon();
      Polygon2D polygon2D = Polygon2D.create(polygon);
      PointValues.Relation originalRelation = polygon2D.relateTriangle(originalQuantize[1], originalQuantize[0], originalQuantize[3], originalQuantize[2], originalQuantize[5], originalQuantize[4]);
      PointValues.Relation encodedRelation = polygon2D.relateTriangle(encodedQuantize[1], encodedQuantize[0], encodedQuantize[3], encodedQuantize[2], encodedQuantize[5], encodedQuantize[4]);
      assertTrue(originalRelation == encodedRelation);
    }
  }

  public void testDegeneratedTriangle() {
    double alat = 1e-26d;
    double alon = 0.0d;
    double blat = -1.0d;
    double blon = 0.0d;
    double clat = 1.0d;
    double clon = 0.0d;
    int alatEnc = GeoEncodingUtils.encodeLatitude(alat);
    int alonEnc = GeoEncodingUtils.encodeLongitude(alon);
    int blatEnc = GeoEncodingUtils.encodeLatitude(blat);
    int blonEnc = GeoEncodingUtils.encodeLongitude(blon);
    int clatEnc = GeoEncodingUtils.encodeLatitude(clat);
    int clonEnc = GeoEncodingUtils.encodeLongitude(clon);
    byte[] b = new byte[7 * LatLonShape.BYTES];
    LatLonShape.encodeTriangle(b, alatEnc, alonEnc, blatEnc, blonEnc, clatEnc, clonEnc);
    int[] encoded = new int[6];
    LatLonShape.decodeTriangle(b, encoded);
    assertTrue(encoded[0] == blatEnc);
    assertTrue(encoded[1] == blonEnc);
    assertTrue(encoded[2] == clatEnc);
    assertTrue(encoded[3] == clonEnc);
    assertTrue(encoded[4] == alatEnc);
    assertTrue(encoded[5] == alonEnc);
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

    Field[] fields = LatLonShape.createIndexableFields("test", indexPoly1);
    for (Field f : fields) {
      doc.add(f);
    }
    fields = LatLonShape.createIndexableFields("test", indexPoly2);
    for (Field f : fields) {
      doc.add(f);
    }
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

    Query q = LatLonShape.newPolygonQuery("test", QueryRelation.WITHIN, searchPoly);
    assertEquals(1, searcher.count(q));

    IOUtils.close(w, reader, dir);
  }

  public void testLUCENE8679() {
    double alat = 1.401298464324817E-45;
    double alon = 24.76789767911785;
    double blat = 34.26468306870807;
    double blon = -52.67048754768767;
    Polygon polygon = new Polygon(new double[] {-14.448264200949083, 0, 0, -14.448264200949083, -14.448264200949083},
                                  new double[] {0.9999999403953552, 0.9999999403953552, 124.50086371762484, 124.50086371762484, 0.9999999403953552});
    Polygon2D polygon2D = Polygon2D.create(polygon);
    PointValues.Relation rel = polygon2D.relateTriangle(GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(alon)),
        GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(blat)),
        GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(blon)),
        GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(blat)),
        GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(alon)),
        GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(alat)));

    assertEquals(PointValues.Relation.CELL_OUTSIDE_QUERY, rel);

    rel = polygon2D.relateTriangle(GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(alon)),
        GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(blat)),
        GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(alon)),
        GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(alat)),
        GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(blon)),
        GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(blat)));

    assertEquals(PointValues.Relation.CELL_OUTSIDE_QUERY, rel);
  }

  public void testTriangleTouchingEdges() {
    Polygon p = new Polygon(new double[] {0, 0, 1, 1, 0}, new double[] {0, 1, 1, 0, 0});
    Polygon2D polygon2D = Polygon2D.create(p);
    //3 shared points
    PointValues.Relation rel = polygon2D.relateTriangle(GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(0.5)),
        GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(0)),
        GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(1)),
        GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(0.5)),
        GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(0.5)),
        GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(1)));
    assertEquals(PointValues.Relation.CELL_INSIDE_QUERY, rel);
    //2 shared points
    rel = polygon2D.relateTriangle(GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(0.5)),
        GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(0)),
        GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(1)),
        GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(0.5)),
        GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(0.5)),
        GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(0.75)));
    assertEquals(PointValues.Relation.CELL_INSIDE_QUERY, rel);
    //1 shared point
    rel = polygon2D.relateTriangle(
        GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(0.5)),
        GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(0.5)),
        GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(0.5)),
        GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(0)),
        GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(0.75)),
        GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(0.75)));
    assertEquals(PointValues.Relation.CELL_INSIDE_QUERY, rel);
    // 1 shared point but out
    rel = polygon2D.relateTriangle(GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(1)),
        GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(0.5)),
        GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(2)),
        GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(0)),
        GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(2)),
        GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(2)));
    assertEquals(PointValues.Relation.CELL_CROSSES_QUERY, rel);
    // 1 shared point but crossing
    rel = polygon2D.relateTriangle(GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(0.5)),
        GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(0)),
        GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(2)),
        GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(0.5)),
        GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(0.5)),
        GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(1)));
    assertEquals(PointValues.Relation.CELL_CROSSES_QUERY, rel);
    //share one edge
    rel = polygon2D.relateTriangle(GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(0)),
        GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(0)),
        GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(0)),
        GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(1)),
        GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(0.5)),
        GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(0.5)));
    assertEquals(PointValues.Relation.CELL_INSIDE_QUERY, rel);
    //share one edge outside
    rel = polygon2D.relateTriangle(GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(0)),
        GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(1)),
        GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(1.5)),
        GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(1.5)),
        GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(1)),
        GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(1)));
    assertEquals(PointValues.Relation.CELL_CROSSES_QUERY, rel);
  }

}
