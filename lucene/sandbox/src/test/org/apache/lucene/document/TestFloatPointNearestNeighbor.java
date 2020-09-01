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

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LatLonPointPrototypeQueries;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestFloatPointNearestNeighbor extends LuceneTestCase {

  public void testNearestNeighborWithDeletedDocs() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, getIndexWriterConfig());
    Document doc = new Document();
    doc.add(new FloatPoint("point", 40.0f, 50.0f));
    doc.add(new StringField("id", "0", Field.Store.YES));
    w.addDocument(doc);

    doc = new Document();
    doc.add(new FloatPoint("point", 45.0f, 55.0f));
    doc.add(new StringField("id", "1", Field.Store.YES));
    w.addDocument(doc);

    DirectoryReader r = w.getReader();
    // can't wrap because we require Lucene60PointsFormat directly but e.g. ParallelReader wraps with its own points impl:
    IndexSearcher s = newSearcher(r, false);
    FieldDoc hit = (FieldDoc)FloatPointNearestNeighbor.nearest(s, "point", 1, 40.0f, 50.0f).scoreDocs[0];
    assertEquals("0", r.document(hit.doc).getField("id").stringValue());
    r.close();

    w.deleteDocuments(new Term("id", "0"));
    r = w.getReader();
    // can't wrap because we require Lucene60PointsFormat directly but e.g. ParallelReader wraps with its own points impl:
    s = newSearcher(r, false);
    hit = (FieldDoc) LatLonPointPrototypeQueries.nearest(s, "point", 40.0, 50.0, 1).scoreDocs[0];
    assertEquals("1", r.document(hit.doc).getField("id").stringValue());
    r.close();
    w.close();
    dir.close();
  }

  public void testNearestNeighborWithAllDeletedDocs() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, getIndexWriterConfig());
    Document doc = new Document();
    doc.add(new FloatPoint("point", 40.0f, 50.0f));
    doc.add(new StringField("id", "0", Field.Store.YES));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new FloatPoint("point", 45.0f, 55.0f));
    doc.add(new StringField("id", "1", Field.Store.YES));
    w.addDocument(doc);

    DirectoryReader r = w.getReader();
    // can't wrap because we require Lucene60PointsFormat directly but e.g. ParallelReader wraps with its own points impl:
    IndexSearcher s = newSearcher(r, false);
    FieldDoc hit = (FieldDoc)FloatPointNearestNeighbor.nearest(s, "point", 1, 40.0f, 50.0f).scoreDocs[0];
    assertEquals("0", r.document(hit.doc).getField("id").stringValue());
    r.close();

    w.deleteDocuments(new Term("id", "0"));
    w.deleteDocuments(new Term("id", "1"));
    r = w.getReader();
    // can't wrap because we require Lucene60PointsFormat directly but e.g. ParallelReader wraps with its own points impl:
    s = newSearcher(r, false);
    assertEquals(0, FloatPointNearestNeighbor.nearest(s, "point", 1, 40.0f, 50.0f).scoreDocs.length);
    r.close();
    w.close();
    dir.close();
  }

  public void testTieBreakByDocID() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, getIndexWriterConfig());
    Document doc = new Document();
    doc.add(new FloatPoint("point", 40.0f, 50.0f));
    doc.add(new StringField("id", "0", Field.Store.YES));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new FloatPoint("point", 40.0f, 50.0f));
    doc.add(new StringField("id", "1", Field.Store.YES));
    w.addDocument(doc);

    DirectoryReader r = DirectoryReader.open(w);
    // can't wrap because we require Lucene60PointsFormat directly but e.g. ParallelReader wraps with its own points impl:
    ScoreDoc[] hits = FloatPointNearestNeighbor.nearest(newSearcher(r, false), "point", 2, 45.0f, 50.0f).scoreDocs;
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
    assertEquals(0, FloatPointNearestNeighbor.nearest(newSearcher(r, false), "point", 1, 40.0f, 50.0f).scoreDocs.length);
    r.close();
    w.close();
    dir.close();
  }

  public void testNearestNeighborRandom() throws Exception {
    Directory dir;
    int numPoints = atLeast(5000);
    if (numPoints > 100000) {
      dir = newFSDirectory(createTempDir(getClass().getSimpleName()));
    } else {
      dir = newDirectory();
    }
    IndexWriterConfig iwc = getIndexWriterConfig();
    iwc.setMergePolicy(newLogMergePolicy());
    iwc.setMergeScheduler(new SerialMergeScheduler());
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);

    int dims = TestUtil.nextInt(random(), 1, PointValues.MAX_INDEX_DIMENSIONS);
    float[][] values = new float[numPoints][dims];
    for (int id = 0 ; id < numPoints ; ++id) {
      for (int dim = 0 ; dim < dims ; ++dim) {
        Float f = Float.NaN;
        while (f.isNaN()) {
          f = Float.intBitsToFloat(random().nextInt());
        }
        values[id][dim] = f;
      }
      Document doc = new Document();
      doc.add(new FloatPoint("point", values[id]));
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
    for (int iter = 0 ; iter < iters ; ++iter) {
      if (VERBOSE) {
        System.out.println("\nTEST: iter=" + iter);
      }
      float[] origin = new float[dims];
      for (int dim = 0 ; dim < dims ; ++dim) {
        Float f = Float.NaN;
        while (f.isNaN()) {
          f = Float.intBitsToFloat(random().nextInt());
        }
        origin[dim] = f;
      }

      // dumb brute force search to get the expected result:
      FloatPointNearestNeighbor.NearestHit[] expectedHits = new FloatPointNearestNeighbor.NearestHit[numPoints];
      for (int id = 0 ; id < numPoints ; ++id) {
        FloatPointNearestNeighbor.NearestHit hit = new FloatPointNearestNeighbor.NearestHit();
        hit.distanceSquared = euclideanDistanceSquared(origin, values[id]);
        hit.docID = id;
        expectedHits[id] = hit;
      }

      Arrays.sort(expectedHits, (a, b) -> {
        int cmp = Double.compare(a.distanceSquared, b.distanceSquared);
        return cmp != 0 ? cmp : a.docID - b.docID; // tie break by smaller id
      });

      int topK = TestUtil.nextInt(random(), 1, numPoints);

      if (VERBOSE) {
        System.out.println("\nhits for origin=" + Arrays.toString(origin));
      }

      ScoreDoc[] hits = FloatPointNearestNeighbor.nearest(s, "point", topK, origin).scoreDocs;
      assertEquals("fewer than expected hits: ", topK, hits.length);

      if (VERBOSE) {
        for (int i = 0 ; i < topK ; ++i) {
          FloatPointNearestNeighbor.NearestHit expected = expectedHits[i];
          FieldDoc actual = (FieldDoc)hits[i];
          Document actualDoc = r.document(actual.doc);
          System.out.println("hit " + i);
          System.out.println("  expected id=" + expected.docID + "  " + Arrays.toString(values[expected.docID])
              + "  distance=" + (float)Math.sqrt(expected.distanceSquared) + "  distanceSquared=" + expected.distanceSquared);
          System.out.println("  actual id=" + actualDoc.getField("id") + " distance=" + actual.fields[0]);
        }
      }

      for (int i = 0 ; i < topK ; ++i) {
        FloatPointNearestNeighbor.NearestHit expected = expectedHits[i];
        FieldDoc actual = (FieldDoc)hits[i];
        assertEquals("hit " + i + ":", expected.docID, actual.doc);
        assertEquals("hit " + i + ":", (float)Math.sqrt(expected.distanceSquared), (Float)actual.fields[0], 0.000001);
      }
    }

    r.close();
    w.close();
    dir.close();
  }

  private static double euclideanDistanceSquared(float[] a, float[] b) {
    double sumOfSquaredDifferences = 0.0d;
    for (int d = 0 ; d < a.length ; ++d) {
      double diff = (double)a[d] - (double)b[d];
      sumOfSquaredDifferences += diff * diff;
    }
    return sumOfSquaredDifferences;
  }

  private IndexWriterConfig getIndexWriterConfig() {
    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setCodec(TestUtil.getDefaultCodec());
    return iwc;
  }
}
