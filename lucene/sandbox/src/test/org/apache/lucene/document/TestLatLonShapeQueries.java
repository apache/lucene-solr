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
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.geo.GeoTestUtil;
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
import org.apache.lucene.index.PointValues.Relation;
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
import org.apache.lucene.util.LuceneTestCase.AwaitsFix;

import static org.apache.lucene.geo.GeoEncodingUtils.decodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.decodeLongitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitudeCeil;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitudeCeil;

/** base Test case for {@link LatLonShape} indexing and search */
@AwaitsFix(bugUrl="https://issues.apache.org/jira/browse/LUCENE-8399")
public class TestLatLonShapeQueries extends LuceneTestCase {
  protected static final String FIELD_NAME = "shape";

  private Polygon quantizePolygon(Polygon polygon) {
    double[] lats = new double[polygon.numPoints()];
    double[] lons = new double[polygon.numPoints()];
    for (int i = 0; i < lats.length; ++i) {
      lats[i] = quantizeLat(polygon.getPolyLat(i));
      lons[i] = quantizeLon(polygon.getPolyLon(i));
    }
    return new Polygon(lats, lons);
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

  protected void addPolygonsToDoc(String field, Document doc, Polygon polygon) {
    Field[] fields = LatLonShape.createIndexableFields(field, polygon);
    for (Field f : fields) {
      doc.add(f);
    }
  }

  protected Query newRectQuery(String field, double minLat, double maxLat, double minLon, double maxLon) {
    return LatLonShape.newBoxQuery(field, minLat, maxLat, minLon, maxLon);
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
    doTestRandom(200000);
  }

  private void doTestRandom(int count) throws Exception {
    int numPolygons = atLeast(count);

    if (VERBOSE) {
      System.out.println("TEST: numPolygons=" + numPolygons);
    }

    Polygon[] polygons = new Polygon[numPolygons];
    for (int id = 0; id < numPolygons; ++id) {
      int x = random().nextInt(20);
      if (x == 17) {
        polygons[id] = null;
        if (VERBOSE) {
          System.out.println("  id=" + id + " is missing");
        }
      } else {
        // create a polygon that does not cross the dateline
        polygons[id] = GeoTestUtil.nextPolygon();
      }
    }
    verify(polygons);
  }

  private void verify(Polygon... polygons) throws Exception {
    verifyRandomBBoxes(polygons);
  }

  protected void verifyRandomBBoxes(Polygon... polygons) throws Exception {
    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setMergeScheduler(new SerialMergeScheduler());
    int mbd = iwc.getMaxBufferedDocs();
    if (mbd != -1 && mbd < polygons.length / 100) {
      iwc.setMaxBufferedDocs(polygons.length / 100);
    }
    Directory dir;
    if (polygons.length > 1000) {
      dir = newFSDirectory(createTempDir(getClass().getSimpleName()));
    } else {
      dir = newDirectory();
    }

    Set<Integer> deleted = new HashSet<>();
    IndexWriter w = new IndexWriter(dir, iwc);
    Polygon2D[] poly2D = new Polygon2D[polygons.length];
    for (int id = 0; id < polygons.length; ++id) {
      Document doc = new Document();
      doc.add(newStringField("id", "" + id, Field.Store.NO));
      doc.add(new NumericDocValuesField("id", id));
      if (polygons[id] != null) {
        try {
          addPolygonsToDoc(FIELD_NAME, doc, polygons[id]);
        } catch (IllegalArgumentException e) {
          // GeoTestUtil will occassionally create invalid polygons
          // invalid polygons will not tessellate
          // we skip those polygons that will not tessellate, relying on the TestTessellator class
          // to ensure the Tessellator correctly identified a malformed shape and its not a bug
          if (VERBOSE) {
            System.out.println("  id=" + id + " could not tessellate. Malformed shape " + polygons[id] + " detected");
          }
          // remove and skip the malformed shape
          polygons[id] = null;
          continue;
        }
        poly2D[id] = Polygon2D.create(quantizePolygon(polygons[id]));
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

    if (random().nextBoolean()) {
      w.forceMerge(1);
    }
    final IndexReader r = DirectoryReader.open(w);
    w.close();

    IndexSearcher s = newSearcher(r);

    final int iters = atLeast(75);

    Bits liveDocs = MultiFields.getLiveDocs(s.getIndexReader());
    int maxDoc = s.getIndexReader().maxDoc();

    for (int iter = 0; iter < iters; ++iter) {
      if (VERBOSE) {
        System.out.println("\nTEST: iter=" + (iter+1) + " of " + iters + " s=" + s);
      }

      // BBox
      Rectangle rect = GeoTestUtil.nextBoxNotCrossingDateline();
      Query query = newRectQuery(FIELD_NAME, rect.minLat, rect.maxLat, rect.minLon, rect.maxLon);

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
      NumericDocValues docIDToID = MultiDocValues.getNumericValues(r, "id");
      for (int docID = 0; docID < maxDoc; ++docID) {
        assertEquals(docID, docIDToID.nextDoc());
        int id = (int) docIDToID.longValue();
        boolean expected;
        if (liveDocs != null && liveDocs.get(docID) == false) {
          // document is deleted
          expected = false;
        } else if (polygons[id] == null) {
          expected = false;
        } else {
          // check quantized poly against quantized query
          expected = poly2D[id].relate(quantizeLatCeil(rect.minLat), quantizeLat(rect.maxLat),
              quantizeLonCeil(rect.minLon), quantizeLon(rect.maxLon)) != Relation.CELL_OUTSIDE_QUERY;
        }

        if (hits.get(docID) != expected) {
          StringBuilder b = new StringBuilder();

          if (expected) {
            b.append("FAIL: id=" + id + " should match but did not\n");
          } else {
            b.append("FAIL: id=" + id + " should not match but did\n");
          }
          b.append("  query=" + query + " docID=" + docID + "\n");
          b.append("  polygon=" + quantizePolygon(polygons[id]) + "\n");
          b.append("  deleted?=" + (liveDocs != null && liveDocs.get(docID) == false));
          b.append("  rect=Rectangle(" + quantizeLatCeil(rect.minLat) + " TO " + quantizeLat(rect.maxLat) + " lon=" + quantizeLonCeil(rect.minLon) + " TO " + quantizeLon(rect.maxLon) + ")");
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
    IOUtils.close(r, dir);
  }
}
