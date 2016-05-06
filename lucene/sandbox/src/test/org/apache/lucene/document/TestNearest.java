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
import java.util.Comparator;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.NearestNeighbor.NearestHit;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.geo.GeoTestUtil;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.SloppyMath;
import org.apache.lucene.util.TestUtil;
 
public class TestNearest extends LuceneTestCase {

  public void testNearestNeighborWithDeletedDocs() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, getIndexWriterConfig());
    Document doc = new Document();
    doc.add(new LatLonPoint("point", 40.0, 50.0));
    doc.add(new StringField("id", "0", Field.Store.YES));
    w.addDocument(doc);

    doc = new Document();
    doc.add(new LatLonPoint("point", 45.0, 55.0));
    doc.add(new StringField("id", "1", Field.Store.YES));
    w.addDocument(doc);

    DirectoryReader r = w.getReader();
    // can't wrap because we require Lucene60PointsFormat directly but e.g. ParallelReader wraps with its own points impl:
    IndexSearcher s = newSearcher(r, false);
    FieldDoc hit = (FieldDoc) LatLonPoint.nearest(s, "point", 40.0, 50.0, 1).scoreDocs[0];
    assertEquals("0", r.document(hit.doc).getField("id").stringValue());
    r.close();

    w.deleteDocuments(new Term("id", "0"));
    r = w.getReader();
    // can't wrap because we require Lucene60PointsFormat directly but e.g. ParallelReader wraps with its own points impl:
    s = newSearcher(r, false);
    hit = (FieldDoc) LatLonPoint.nearest(s, "point", 40.0, 50.0, 1).scoreDocs[0];
    assertEquals("1", r.document(hit.doc).getField("id").stringValue());
    r.close();
    w.close();
    dir.close();
  }

  public void testNearestNeighborWithAllDeletedDocs() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, getIndexWriterConfig());
    Document doc = new Document();
    doc.add(new LatLonPoint("point", 40.0, 50.0));
    doc.add(new StringField("id", "0", Field.Store.YES));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new LatLonPoint("point", 45.0, 55.0));
    doc.add(new StringField("id", "1", Field.Store.YES));
    w.addDocument(doc);

    DirectoryReader r = w.getReader();
    // can't wrap because we require Lucene60PointsFormat directly but e.g. ParallelReader wraps with its own points impl:
    IndexSearcher s = newSearcher(r, false);
    FieldDoc hit = (FieldDoc) LatLonPoint.nearest(s, "point", 40.0, 50.0, 1).scoreDocs[0];
    assertEquals("0", r.document(hit.doc).getField("id").stringValue());
    r.close();

    w.deleteDocuments(new Term("id", "0"));
    w.deleteDocuments(new Term("id", "1"));
    r = w.getReader();
    // can't wrap because we require Lucene60PointsFormat directly but e.g. ParallelReader wraps with its own points impl:
    s = newSearcher(r, false);
    assertEquals(0, LatLonPoint.nearest(s, "point", 40.0, 50.0, 1).scoreDocs.length);
    r.close();
    w.close();
    dir.close();
  }

  public void testTieBreakByDocID() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, getIndexWriterConfig());
    Document doc = new Document();
    doc.add(new LatLonPoint("point", 40.0, 50.0));
    doc.add(new StringField("id", "0", Field.Store.YES));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new LatLonPoint("point", 40.0, 50.0));
    doc.add(new StringField("id", "1", Field.Store.YES));
    w.addDocument(doc);

    DirectoryReader r = DirectoryReader.open(w);
    // can't wrap because we require Lucene60PointsFormat directly but e.g. ParallelReader wraps with its own points impl:
    ScoreDoc[] hits = LatLonPoint.nearest(newSearcher(r, false), "point", 45.0, 50.0, 2).scoreDocs;
    assertEquals("0", r.document(hits[0].doc).getField("id").stringValue());
    assertEquals("1", r.document(hits[1].doc).getField("id").stringValue());

    r.close();
    w.close();
    dir.close();
  }

  public void testNearestNeighborWithNoDocs() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, getIndexWriterConfig());
    DirectoryReader r = w.getReader();
    // can't wrap because we require Lucene60PointsFormat directly but e.g. ParallelReader wraps with its own points impl:
    assertEquals(0, LatLonPoint.nearest(newSearcher(r, false), "point", 40.0, 50.0, 1).scoreDocs.length);
    r.close();
    w.close();
    dir.close();
  }

  private double quantizeLat(double latRaw) {
    return GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(latRaw));
  }

  private double quantizeLon(double lonRaw) {
    return GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(lonRaw));
  }

  public void testNearestNeighborRandom() throws Exception {
    
    int numPoints = atLeast(5000);
    Directory dir;
    if (numPoints > 100000) {
      dir = newFSDirectory(createTempDir(getClass().getSimpleName()));
    } else {
      dir = newDirectory();
    }
    double[] lats = new double[numPoints];
    double[] lons = new double[numPoints];

    IndexWriterConfig iwc = getIndexWriterConfig();
    iwc.setMergePolicy(newLogMergePolicy());
    iwc.setMergeScheduler(new SerialMergeScheduler());
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
    for(int id=0;id<numPoints;id++) {
      lats[id] = quantizeLat(GeoTestUtil.nextLatitude());
      lons[id] = quantizeLon(GeoTestUtil.nextLongitude());
      Document doc = new Document();
      doc.add(new LatLonPoint("point", lats[id], lons[id]));
      doc.add(new LatLonDocValuesField("point", lats[id], lons[id]));
      doc.add(new StoredField("id", id));
      w.addDocument(doc);
    }

    if (random().nextBoolean()) {
      w.forceMerge(1);
    }

    DirectoryReader r = w.getReader();
    if (VERBOSE) {      
      System.out.println("TEST: reader=" + r);
    }
    // can't wrap because we require Lucene60PointsFormat directly but e.g. ParallelReader wraps with its own points impl:
    IndexSearcher s = newSearcher(r, false);
    int iters = atLeast(100);
    for(int iter=0;iter<iters;iter++) {
      if (VERBOSE) {      
        System.out.println("\nTEST: iter=" + iter);
      }
      double pointLat = GeoTestUtil.nextLatitude();
      double pointLon = GeoTestUtil.nextLongitude();

      // dumb brute force search to get the expected result:
      NearestHit[] expectedHits = new NearestHit[lats.length];
      for(int id=0;id<lats.length;id++) {
        NearestHit hit = new NearestHit();
        hit.distanceMeters = SloppyMath.haversinMeters(pointLat, pointLon, lats[id], lons[id]);
        hit.docID = id;
        expectedHits[id] = hit;
      }

      Arrays.sort(expectedHits, new Comparator<NearestHit>() {
          @Override
          public int compare(NearestHit a, NearestHit b) {
            int cmp = Double.compare(a.distanceMeters, b.distanceMeters);
            if (cmp != 0) {
              return cmp;
            }
            // tie break by smaller docID:
            return a.docID - b.docID;
          }
        });

      int topN = TestUtil.nextInt(random(), 1, lats.length);

      if (VERBOSE) {
        System.out.println("\nhits for pointLat=" + pointLat + " pointLon=" + pointLon);
      }

      // Also test with MatchAllDocsQuery, sorting by distance:
      TopFieldDocs fieldDocs = s.search(new MatchAllDocsQuery(), topN, new Sort(LatLonDocValuesField.newDistanceSort("point", pointLat, pointLon)));

      ScoreDoc[] hits = LatLonPoint.nearest(s, "point", pointLat, pointLon, topN).scoreDocs;
      for(int i=0;i<topN;i++) {
        NearestHit expected = expectedHits[i];
        FieldDoc expected2 = (FieldDoc) fieldDocs.scoreDocs[i];
        FieldDoc actual = (FieldDoc) hits[i];
        Document actualDoc = r.document(actual.doc);

        if (VERBOSE) {
          System.out.println("hit " + i);
          System.out.println("  expected id=" + expected.docID + " lat=" + lats[expected.docID] + " lon=" + lons[expected.docID] + " distance=" + expected.distanceMeters + " meters");
          System.out.println("  actual id=" + actualDoc.getField("id") + " distance=" + actual.fields[0] + " meters");
        }

        assertEquals(expected.docID, actual.doc);
        assertEquals(expected.distanceMeters, ((Double) actual.fields[0]).doubleValue(), 0.0);

        assertEquals(expected.docID, expected.docID);
        assertEquals(((Double) expected2.fields[0]).doubleValue(), expected.distanceMeters, 0.0);
      }
    }

    r.close();
    w.close();
    dir.close();
  }

  private IndexWriterConfig getIndexWriterConfig() {
    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setCodec(Codec.forName("Lucene62"));
    return iwc;
  }
}
