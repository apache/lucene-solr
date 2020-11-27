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

import java.util.Random;

import org.apache.lucene.document.ShapeField.QueryRelation;
import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.ShapeTestUtil;
import org.apache.lucene.geo.Tessellator;
import org.apache.lucene.geo.XYCircle;
import org.apache.lucene.geo.XYGeometry;
import org.apache.lucene.geo.XYLine;
import org.apache.lucene.geo.XYPoint;
import org.apache.lucene.geo.XYPolygon;
import org.apache.lucene.geo.XYRectangle;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

/** Test case for indexing cartesian shapes and search by bounding box, lines, and polygons */
public class TestXYShape extends LuceneTestCase {

  protected static String FIELDNAME = "field";
  protected static void addPolygonsToDoc(String field, Document doc, XYPolygon polygon) {
    Field[] fields = XYShape.createIndexableFields(field, polygon);
    for (Field f : fields) {
      doc.add(f);
    }
  }

  protected static void addLineToDoc(String field, Document doc, XYLine line) {
    Field[] fields = XYShape.createIndexableFields(field, line);
    for (Field f : fields) {
      doc.add(f);
    }
  }

  protected Query newRectQuery(String field, float minX, float maxX, float minY, float maxY) {
    return XYShape.newBoxQuery(field, QueryRelation.INTERSECTS, minX, maxX, minY, maxY);
  }

  /** test we can search for a point with a standard number of vertices*/
  public void testBasicIntersects() throws Exception {
    int numVertices = TestUtil.nextInt(random(), 50, 100);
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    // add a random polygon document
    XYPolygon p = ShapeTestUtil.createRegularPolygon(0, 90, atLeast(1000000), numVertices);
    Document document = new Document();
    addPolygonsToDoc(FIELDNAME, document, p);
    writer.addDocument(document);

    // add a line document
    document = new Document();
    // add a line string
    float x[] = new float[p.numPoints() - 1];
    float y[] = new float[p.numPoints() - 1];
    for (int i = 0; i < x.length; ++i) {
      x[i] = p.getPolyX(i);
      y[i] = p.getPolyY(i);
    }
    XYLine l = new XYLine(x, y);
    addLineToDoc(FIELDNAME, document, l);
    writer.addDocument(document);

    ////// search /////
    // search an intersecting bbox
    IndexReader reader = writer.getReader();
    writer.close();
    IndexSearcher searcher = newSearcher(reader);
    float minX = Math.min(x[0], x[1]);
    float minY = Math.min(y[0], y[1]);
    float maxX = Math.max(x[0], x[1]);
    float maxY = Math.max(y[0], y[1]);
    Query q = newRectQuery(FIELDNAME, minX, maxX, minY, maxY);
    assertEquals(2, searcher.count(q));

    // search a disjoint bbox
    q = newRectQuery(FIELDNAME, p.minX-1f, p.minX + 1f, p.minY - 1f, p.minY + 1f);
    assertEquals(0, searcher.count(q));

    // search w/ an intersecting polygon
    q = XYShape.newPolygonQuery(FIELDNAME, QueryRelation.INTERSECTS, new XYPolygon(
        new float[] {minX, minX, maxX, maxX, minX},
        new float[] {minY, maxY, maxY, minY, minY}
    ));
    assertEquals(2, searcher.count(q));

    // search w/ an intersecting line
    q = XYShape.newLineQuery(FIELDNAME, QueryRelation.INTERSECTS, new XYLine(
       new float[] {minX, minX, maxX, maxX},
       new float[] {minY, maxY, maxY, minY}
    ));
    assertEquals(2, searcher.count(q));

    IOUtils.close(reader, dir);
  }

  public void testBoundingBoxQueries() throws Exception {
    Random random = random();
    XYRectangle r1 = ShapeTestUtil.nextBox(random);
    XYRectangle r2 = ShapeTestUtil.nextBox(random);
    XYPolygon p;
    //find two boxes so that r1 contains r2
    while (true) {
      // TODO: Should XYRectangle hold values as float?
      if (areBoxDisjoint(r1, r2)) {
        p = toPolygon(r2);
        try {
          Tessellator.tessellate(p);
          break;
        } catch (Exception e) {
          // ignore, try other combination
        }
      }
      r1 = ShapeTestUtil.nextBox(random);
      r2 = ShapeTestUtil.nextBox(random);
    }

    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random, dir);

    // add the polygon to the index
    Document document = new Document();
    addPolygonsToDoc(FIELDNAME, document, p);
    writer.addDocument(document);

    ////// search /////
    IndexReader reader = writer.getReader();
    writer.close();
    IndexSearcher searcher = newSearcher(reader);
    // Query by itself should match
    Query q = newRectQuery(FIELDNAME, r2.minX, r2.maxX, r2.minY, r2.maxY);
    assertEquals(1, searcher.count(q));
    // r1 contains r2, intersects should match
    q = newRectQuery(FIELDNAME, r1.minX, r1.maxX, r1.minY, r1.maxY);
    assertEquals(1, searcher.count(q));
    // r1 contains r2, WITHIN should match
    q = XYShape.newBoxQuery(FIELDNAME, QueryRelation.WITHIN, r1.minX, r1.maxX, r1.minY, r1.maxY);
    assertEquals(1, searcher.count(q));

    IOUtils.close(reader, dir);
  }

  public void testPointIndexAndDistanceQuery() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document document = new Document();
    float pX = ShapeTestUtil.nextFloat(random());
    float py = ShapeTestUtil.nextFloat(random());
    Field[] fields = XYShape.createIndexableFields(FIELDNAME, pX, py);
    for (Field f : fields) {
      document.add(f);
    }
    writer.addDocument(document);

    //// search
    IndexReader r = writer.getReader();
    writer.close();
    IndexSearcher s = newSearcher(r);
    XYCircle circle = ShapeTestUtil.nextCircle();
    Component2D circle2D = XYGeometry.create(circle);
    int expected;
    int expectedDisjoint;
    if (circle2D.contains(pX, py))  {
      expected = 1;
      expectedDisjoint = 0;
    } else {
      expected = 0;
      expectedDisjoint = 1;
    }

    Query q = XYShape.newDistanceQuery(FIELDNAME, QueryRelation.INTERSECTS, circle);
    assertEquals(expected, s.count(q));

    q = XYShape.newDistanceQuery(FIELDNAME, QueryRelation.WITHIN, circle);
    assertEquals(expected, s.count(q));

    q = XYShape.newDistanceQuery(FIELDNAME, QueryRelation.DISJOINT, circle);
    assertEquals(expectedDisjoint, s.count(q));

    IOUtils.close(r, dir);
  }

  public void testContainsWrappingBooleanQuery() throws Exception {

    float[] ys = new float[] {-30, -30, 30, 30, -30};
    float[] xs = new float[] {-30, 30, 30, -30, -30};
    XYPolygon polygon = new XYPolygon(xs, ys);

    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document document = new Document();
    addPolygonsToDoc(FIELDNAME, document, polygon);
    writer.addDocument(document);

    //// search
    IndexReader r = writer.getReader();
    writer.close();
    IndexSearcher s = newSearcher(r);

    XYGeometry[] geometries = new XYGeometry[] { new XYRectangle(0, 1, 0, 1), new XYPoint(4, 4) };
    // geometries within the polygon
    Query q = XYShape.newGeometryQuery(FIELDNAME, QueryRelation.CONTAINS, geometries);
    TopDocs topDocs = s.search(q, 1);
    assertEquals(1, topDocs.scoreDocs.length);
    assertEquals(1.0, topDocs.scoreDocs[0].score, 0.0);
    IOUtils.close(r, dir);
  }

  public void testContainsIndexedGeometryCollection() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    XYPolygon polygon = new XYPolygon(new float[] {-132, 132, 132, -132, -132}, new float[] {-64, -64, 64, 64, -64});
    Field[] polygonFields = XYShape.createIndexableFields(FIELDNAME, polygon);
    // POINT(5, 5) inside the indexed polygon
    Field[] pointFields = XYShape.createIndexableFields(FIELDNAME, 5, 5);
    int numDocs = random().nextInt(1000);
    // index the same multi geometry many times
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      for (Field f : polygonFields) {
        doc.add(f);
      }
      for(int j = 0; j < 10; j++) {
        for (Field f : pointFields) {
          doc.add(f);
        }
      }
      w.addDocument(doc);
    }
    w.forceMerge(1);

    ///// search //////
    IndexReader reader = w.getReader();
    w.close();
    IndexSearcher searcher = newSearcher(reader);
    // Contains is only true if the query geometry is inside a geometry and does not intersect with any other geometry 
    // belonging to the same document. In this case the query geometry contains the indexed polygon but the point is 
    // inside the query as well, hence the result is 0.
    XYPolygon polygonQuery = new XYPolygon(new float[] {4, 6, 6, 4, 4}, new float[] {4, 4, 6, 6, 4});
    Query query = XYShape.newGeometryQuery(FIELDNAME, QueryRelation.CONTAINS, polygonQuery);
    assertEquals(0, searcher.count(query));

    XYRectangle rectangle = new XYRectangle(4, 6, 4, 6);
    query = XYShape.newGeometryQuery(FIELDNAME, QueryRelation.CONTAINS, rectangle);
    assertEquals(0, searcher.count(query));

    XYCircle circle = new XYCircle(5, 5, 1);
    query = XYShape.newGeometryQuery(FIELDNAME, QueryRelation.CONTAINS, circle);
    assertEquals(0, searcher.count(query));

    IOUtils.close(w, reader, dir);
  }

  private static boolean areBoxDisjoint(XYRectangle r1, XYRectangle r2) {
    return ( r1.minX <=  r2.minX &&  r1.minY <= r2.minY && r1.maxX >= r2.maxX && r1.maxY >= r2.maxY);
  }

  private static XYPolygon toPolygon(XYRectangle r) {
    return new XYPolygon(new float[]{ r.minX, r.maxX, r.maxX, r.minX, r.minX},
                         new float[]{ r.minY, r.minY, r.maxY, r.maxY, r.minY});
  }
}
