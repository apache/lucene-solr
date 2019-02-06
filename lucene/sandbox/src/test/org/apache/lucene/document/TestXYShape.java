/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lucene.document;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.geo.GeoTestUtil;
import org.apache.lucene.geo.Line;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.geo.XYPolygon;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestXYShape extends LuceneTestCase {

  protected static String FIELDNAME = "field";
  protected void addPolygonsToDoc(String field, Document doc, XYPolygon polygon) {
    Field[] fields = XYShape.createIndexableFields(field, polygon);
    for (Field f : fields) {
      doc.add(f);
    }
  }

  protected Query newRectQuery(String field, int minX, int maxX, int minY, int maxY) {
    return XYShape.newBoxQuery(field, XYShape.QueryRelation.INTERSECTS, minX, maxX, minY, maxY);
  }

  protected Query newRectQuery(String field, float minX, float maxX, float minY, float maxY) {
    return XYShape.newBoxQuery(field, XYShape.QueryRelation.INTERSECTS, minX, maxX, minY, maxY);
  }

  /** test we can search for a point with a standard number of vertices*/
  public void testBasicIntersects() throws Exception {
    int numVertices = TestUtil.nextInt(random(), 50, 100);
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    // add a random polygon document
    Polygon p = GeoTestUtil.createRegularPolygon(0, 90, atLeast(1000000), numVertices);
    XYPolygon xyp = new XYPolygon(p.getPolyLons(), p.getPolyLats(), XYShape.XYShapeType.INTEGER);
    Document document = new Document();
    addPolygonsToDoc(FIELDNAME, document, xyp);
    writer.addDocument(document);

    ////// search /////
    // search an intersecting bbox
    IndexReader reader = writer.getReader();
    writer.close();
    IndexSearcher searcher = newSearcher(reader);
    Query q = newRectQuery(FIELDNAME, GeoEncodingUtils.encodeLongitude(-180), GeoEncodingUtils.encodeLongitude(180),
        GeoEncodingUtils.encodeLatitude(-90),  GeoEncodingUtils.encodeLatitude(90));
    assertEquals(1, searcher.count(q));

    // search a disjoint bbox
    q = XYShape.newBoxQuery(FIELDNAME, XYShape.QueryRelation.DISJOINT, -1000, -500, -1000, 500);
    assertEquals(1, searcher.count(q));

    IOUtils.close(reader, dir);
  }
}
