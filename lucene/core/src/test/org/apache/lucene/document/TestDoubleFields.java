package org.apache.lucene.document;

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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.TestUtil;

public class TestDoubleFields extends LuceneTestCase {

  static double IOTA = 0.0;

  public void testBasicRange() throws Exception {
    Directory dir = newDirectory();

    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldTypes fieldTypes = w.getFieldTypes();
    //System.out.println("id type: " + fieldTypes.getFieldType("id"));

    Document doc = w.newDocument();
    doc.addDouble("num", 3d);
    doc.addAtom("id", "one");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addDouble("num", 2d);
    doc.addAtom("id", "two");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addDouble("num", 7d);
    doc.addAtom("id", "three");
    w.addDocument(doc);

    IndexReader r = DirectoryReader.open(w, true);
    IndexSearcher s = newSearcher(r);

    // Make sure range query hits the right number of hits
    assertEquals(2, s.search(new MatchAllDocsQuery(), fieldTypes.newDoubleRangeFilter("num", 0d, true, 3d, true), 1).totalHits);
    assertEquals(2, s.search(new MatchAllDocsQuery(), fieldTypes.newDoubleDocValuesRangeFilter("num", 0d, true, 3d, true), 1).totalHits);
    assertEquals(3, s.search(new MatchAllDocsQuery(), fieldTypes.newDoubleRangeFilter("num", 0d, true, 10d, true), 1).totalHits);
    assertEquals(3, s.search(new MatchAllDocsQuery(), fieldTypes.newDoubleDocValuesRangeFilter("num", 0d, true, 10d, true), 1).totalHits);
    assertEquals(1, s.search(new MatchAllDocsQuery(), fieldTypes.newDoubleRangeFilter("num", 1d, true, 2.5d, true), 1).totalHits);
    assertEquals(1, s.search(new MatchAllDocsQuery(), fieldTypes.newDoubleDocValuesRangeFilter("num", 1d, true, 2.5d, true), 1).totalHits);
    r.close();
    w.close();
    dir.close();
  }

  public void testRandom() throws Exception {
    int iters = atLeast(10000);
    for(int iter=0;iter<iters;iter++) {
      double v = random().nextDouble();
      long x = NumericUtils.doubleToLong(v);
      double v2 = NumericUtils.longToDouble(x);
      assertEquals(v, v2, IOTA);
    }
  }

  public void testNaN() throws Exception {
    assertEquals(Double.NaN, NumericUtils.longToDouble(NumericUtils.doubleToLong(Double.NaN)), 0.0d);
  }

  public void testPositiveInfinity() throws Exception {
    assertEquals(Double.POSITIVE_INFINITY, NumericUtils.longToDouble(NumericUtils.doubleToLong(Double.POSITIVE_INFINITY)), 0.0d);
  }

  public void testNegativeInfinity() throws Exception {
    assertEquals(Double.NEGATIVE_INFINITY, NumericUtils.longToDouble(NumericUtils.doubleToLong(Double.NEGATIVE_INFINITY)), 0.0d);
  }

  public void testBasicSort() throws Exception {
    Directory dir = newDirectory();

    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldTypes fieldTypes = w.getFieldTypes();
    //System.out.println("id type: " + fieldTypes.getFieldType("id"));

    Document doc = w.newDocument();
    doc.addDouble("num", 3d);
    doc.addAtom("id", "one");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addDouble("num", 2d);
    doc.addAtom("id", "two");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addDouble("num", 7d);
    doc.addAtom("id", "three");
    w.addDocument(doc);

    IndexReader r = DirectoryReader.open(w, true);
    IndexSearcher s = newSearcher(r);

    TopDocs hits = s.search(new MatchAllDocsQuery(), 3, fieldTypes.newSort("num"));
    assertEquals(3, hits.totalHits);
    assertEquals("two", r.document(hits.scoreDocs[0].doc).getString("id"));
    assertEquals("one", r.document(hits.scoreDocs[1].doc).getString("id"));
    assertEquals("three", r.document(hits.scoreDocs[2].doc).getString("id"));
    r.close();
    w.close();
    dir.close();
  }

  public void testSortMissingFirst() throws Exception {
    Directory dir = newDirectory();

    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldTypes fieldTypes = w.getFieldTypes();

    Document doc = w.newDocument();
    doc.addDouble("num", 3d);
    doc.addAtom("id", "one");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addAtom("id", "two");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addDouble("num", 7d);
    doc.addAtom("id", "three");
    w.addDocument(doc);

    IndexReader r = DirectoryReader.open(w, true);
    IndexSearcher s = newSearcher(r);
    fieldTypes = s.getFieldTypes();
    fieldTypes.setSortMissingFirst("num");

    TopDocs hits = s.search(new MatchAllDocsQuery(), 3, fieldTypes.newSort("num"));
    assertEquals(3, hits.totalHits);
    assertEquals("two", r.document(hits.scoreDocs[0].doc).getString("id"));
    assertEquals("one", r.document(hits.scoreDocs[1].doc).getString("id"));
    assertEquals("three", r.document(hits.scoreDocs[2].doc).getString("id"));
    r.close();
    w.close();
    dir.close();
  }

  public void testSortMissingLast() throws Exception {
    Directory dir = newDirectory();

    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldTypes fieldTypes = w.getFieldTypes();

    Document doc = w.newDocument();
    doc.addDouble("num", 3d);
    doc.addAtom("id", "one");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addAtom("id", "two");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addDouble("num", 7d);
    doc.addAtom("id", "three");
    w.addDocument(doc);

    IndexReader r = DirectoryReader.open(w, true);
    IndexSearcher s = newSearcher(r);
    fieldTypes = s.getFieldTypes();
    fieldTypes.setSortMissingLast("num");

    TopDocs hits = s.search(new MatchAllDocsQuery(), 3, fieldTypes.newSort("num"));
    assertEquals(3, hits.totalHits);
    assertEquals("one", r.document(hits.scoreDocs[0].doc).getString("id"));
    assertEquals("three", r.document(hits.scoreDocs[1].doc).getString("id"));
    assertEquals("two", r.document(hits.scoreDocs[2].doc).getString("id"));
    r.close();
    w.close();
    dir.close();
  }

  public void testRandomRangeAndSort() throws Exception {
    Directory dir = newDirectory();
    int numDocs = atLeast(100);
    RandomIndexWriter w = newRandomIndexWriter(dir);
    List<Double> values = new ArrayList<>();
    for(int i=0;i<numDocs;i++) {
      Document doc = w.newDocument();
      doc.addUniqueInt("id", i);
      Double num = random().nextDouble();
      values.add(num);
      doc.addDouble("num", num);
      w.addDocument(doc);
      if (VERBOSE) {
        System.out.println("TEST: id=" + i + " num=" + num);
      }
    }

    IndexReader r = w.getReader();
    FieldTypes fieldTypes = r.getFieldTypes();

    IndexSearcher s = newSearcher(r);
    int iters = atLeast(1000);
    for(int iter=0;iter<iters;iter++) {
      double x = random().nextDouble();
      double y = random().nextDouble();

      double min, max;
      if (x < y) {
        min = x;
        max = y;
      } else {
        min = y;
        max = x;
      }
      Set<Integer> expected = new HashSet<>();
      for(int i=0;i<values.size();i++) {
        float value = values.get(i).floatValue();
        if (value >= min && value <= max) {
          expected.add(i);
        }
      }
      if (VERBOSE) {
        System.out.println("TEST: iter " + iter + " count=" + expected.size() + " min=" + min + " max=" + max);
        for(int value : expected) {
          System.out.println("  " + value);
        }
      }
      
      Set<Integer> actual = new HashSet<>();
      Filter filter;
      if (random().nextBoolean()) {
        filter = fieldTypes.newDoubleRangeFilter("num", min, true, max, true);
      } else {
        filter = fieldTypes.newDoubleDocValuesRangeFilter("num", min, true, max, true);
      }

      boolean reversed = random().nextBoolean();
      Sort sort = fieldTypes.newSort("num", reversed);
      if (VERBOSE) {
        System.out.println("TEST: filter=" + filter + " reversed=" + reversed + " sort=" + sort);
      }
      TopDocs hits = s.search(new MatchAllDocsQuery(), filter, numDocs, sort);
      Double last = null;
      boolean wrongValues = false;
      for(ScoreDoc hit : hits.scoreDocs) {
        Document doc = s.doc(hit.doc);
        actual.add(doc.getInt("id"));
        Double v = doc.getDouble("num");
        if (isClose(v, (Double) ((FieldDoc) hit).fields[0]) == false) {
          System.out.println("  wrong: " + v + " vs " + ((FieldDoc) hit).fields[0]);
          wrongValues = true;
        }
        if (VERBOSE) {
          System.out.println("   hit doc=" + doc);
        }
        if (last != null) {
          int cmp;
          if (isClose(last, v)) {
            cmp = 0;
          } else {
            cmp = last.compareTo(v);
          }
          assertTrue((reversed && cmp >= 0) || (reversed == false && cmp <= 0));
        }
        last = v;
      }

      assertEquals(expected, actual);
      assertFalse(wrongValues);
    }

    r.close();
    w.close();
    dir.close();
  }

  static boolean isClose(double v1, double v2) {
    return Math.abs(v1 - v2) <= IOTA;
  }

  public void testMultiValuedSort() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = newRandomIndexWriter(dir);
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setMultiValued("num");

    Document doc = w.newDocument();
    doc.addUniqueInt("id", 0);
    doc.addDouble("num", 45d);
    doc.addDouble("num", -22d);
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addUniqueInt("id", 1);
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addUniqueInt("id", 2);
    doc.addDouble("num", -2d);
    doc.addDouble("num", 14d);
    w.addDocument(doc);

    IndexReader r = w.getReader();
    fieldTypes = r.getFieldTypes();

    IndexSearcher s = newSearcher(r);
    TopDocs hits = s.search(new MatchAllDocsQuery(), 10, fieldTypes.newSort("num"));

    // Default selector is MIN:
    assertEquals(0, s.doc(hits.scoreDocs[0].doc).get("id"));
    assertEquals(2, s.doc(hits.scoreDocs[1].doc).get("id"));
    assertEquals(1, s.doc(hits.scoreDocs[2].doc).get("id"));

    fieldTypes.setMultiValuedNumericSortSelector("num", SortedNumericSelector.Type.MAX);
    hits = s.search(new MatchAllDocsQuery(), 10, fieldTypes.newSort("num"));
    assertEquals(2, s.doc(hits.scoreDocs[0].doc).get("id"));
    assertEquals(0, s.doc(hits.scoreDocs[1].doc).get("id"));
    assertEquals(1, s.doc(hits.scoreDocs[2].doc).get("id"));

    r.close();
    w.close();
    dir.close();
  }

  public void testMultiValuedRange() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = newRandomIndexWriter(dir);
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setMultiValued("num");

    Document doc = w.newDocument();
    doc.addUniqueInt("id", 0);
    doc.addDouble("num", 45d);
    doc.addDouble("num", -22d);
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addUniqueInt("id", 1);
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addUniqueInt("id", 2);
    doc.addDouble("num", -2d);
    doc.addDouble("num", 14d);
    w.addDocument(doc);

    IndexReader r = w.getReader();
    fieldTypes = r.getFieldTypes();

    IndexSearcher s = newSearcher(r);
    assertEquals(2, s.search(new MatchAllDocsQuery(), fieldTypes.newDoubleRangeFilter("num", -100d, true, 100d, true), 1).totalHits);
    assertEquals(1, s.search(new MatchAllDocsQuery(), fieldTypes.newDoubleRangeFilter("num", 40d, true, 45d, true), 1).totalHits);
    r.close();
    w.close();
    dir.close();
  }

  public void testTermQuery() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = newIndexWriter(dir);
    Document doc = w.newDocument();
    doc.addDouble("num", 180d);
    w.addDocument(doc);
    DirectoryReader r = DirectoryReader.open(w, true);
    IndexSearcher s = newSearcher(r);
    FieldTypes fieldTypes = s.getFieldTypes();
    assertEquals(1, s.search(fieldTypes.newExactDoubleQuery("num", 180d), 1).totalHits);
    r.close();
    w.close();
    dir.close();
  }

  public void testJustStored() throws Exception {
    IndexWriter w = newIndexWriter(dir);
    Document doc = w.newDocument();
    doc.addStoredDouble("num", 180d);
    w.addDocument(doc);
    DirectoryReader r = DirectoryReader.open(w, true);
    IndexSearcher s = newSearcher(r);
    doc = s.doc(0);
    assertEquals(180d, doc.getDouble("num"), 0.0d);
    r.close();
    w.close();
  }

  public void testExcIndexedThenStored() throws Exception {
    IndexWriter w = newIndexWriter(dir);
    Document doc = w.newDocument();
    doc.addDouble("num", 100.);
    w.addDocument(doc);
    final Document doc2 = w.newDocument();
    shouldFail(() -> doc2.addStoredDouble("num", 200.),
               "field \"num\": cannot addStored: field was already added non-stored");
    w.close();
  }

  public void testExcStoredThenIndexed() throws Exception {
    IndexWriter w = newIndexWriter(dir);
    Document doc = w.newDocument();
    doc.addStoredDouble("num", 100d);
    w.addDocument(doc);
    final Document doc2 = w.newDocument();
    shouldFail(() -> doc2.addDouble("num", 200d),
               "field \"num\": this field is only stored; use addStoredXXX instead");
    w.close();
  }
}
