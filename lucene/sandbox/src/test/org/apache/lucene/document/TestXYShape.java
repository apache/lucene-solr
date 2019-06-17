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

import org.apache.lucene.document.ShapeField.QueryRelation;
import org.apache.lucene.geo.XYLine;
import org.apache.lucene.geo.XYPolygon;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;

/** Test case for indexing cartesian shapes and search by bounding box, lines, and polygons */
public class TestXYShape extends LuceneTestCase {

  protected static String FIELDNAME = "field";
  protected void addPolygonsToDoc(String field, Document doc, XYPolygon polygon) {
    Field[] fields = XYShape.createIndexableFields(field, polygon);
    for (Field f : fields) {
      doc.add(f);
    }
  }

  protected Query newRectQuery(String field, double minX, double maxX, double minY, double maxY) {
    return XYShape.newBoxQuery(field, QueryRelation.INTERSECTS, minX, maxX, minY, maxY);
  }

  /** test we can search for a point with a standard number of vertices*/
  public void testBasicIntersects() throws Exception {
//    int numVertices = TestUtil.nextInt(random(), 50, 100);
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    // add a random polygon document
//    Polygon p = GeoTestUtil.createRegularPolygon(0, 90, atLeast(1000000), numVertices);
//    XYPolygon xyp = new XYPolygon(p.getPolyLons(), p.getPolyLats(), XYShape.XYShapeType.INTEGER);

    XYPolygon xyp = new XYPolygon(new double[] {-150000.2343233d, -43234323.23432d, -73453345.23432d, -150000.2343233d},
        new double[] {-10000.23432d, -5000.234323d, 1000000.023432d, -10000.23432d});

    Document document = new Document();
    addPolygonsToDoc(FIELDNAME, document, xyp);
    writer.addDocument(document);
    writer.forceMerge(1);

    ////// search /////
    // search an intersecting bbox
    IndexReader reader = writer.getReader();
    writer.close();
    IndexSearcher searcher = newSearcher(reader);
    Query q = newRectQuery(FIELDNAME, -150010.2343233d, -150000.2343233d, -10100d, -10000.23432d);
    assertEquals(1, searcher.count(q));

    // search a disjoint bbox
    q = XYShape.newBoxQuery(FIELDNAME, QueryRelation.DISJOINT, -73453355, -73453350, -20000, -10001);
    assertEquals(1, searcher.count(q));

    q = XYShape.newPolygonQuery(FIELDNAME, QueryRelation.INTERSECTS, new XYPolygon(
        new double[] {-150010.2343233d, -150000.2343233d, -150000.2343233d, -150010.2343233d, -150010.2343233d},
        new double[] {-10100d, -10100d, -10000.23432d, -10000.23432d, -10100d}
    ));
    assertEquals(1, searcher.count(q));

    q = XYShape.newLineQuery(FIELDNAME, QueryRelation.INTERSECTS, new XYLine(
        new double[] {-150010.2343233d, -150000.2343233d, -150000.2343233d, -150010.2343233d},
        new double[] {-10100d, -10100d, -10000.23432d, -10000.23432d}
    ));
    assertEquals(1, searcher.count(q));

    IOUtils.close(reader, dir);
  }
}
