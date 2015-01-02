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
import org.apache.lucene.util.HalfFloat;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.TestUtil;

public class TestHalfFloatFields extends LuceneTestCase {

  // We can use constant IOTA because all our random floats are 0.0 - 1.0:
  static float IOTA = .0005F;

  public void testBasicRange() throws Exception {
    Directory dir = newDirectory();

    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldTypes fieldTypes = w.getFieldTypes();
    //System.out.println("id type: " + fieldTypes.getFieldType("id"));

    Document doc = w.newDocument();
    doc.addHalfFloat("num", 3f);
    doc.addAtom("id", "one");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addHalfFloat("num", 2f);
    doc.addAtom("id", "two");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addHalfFloat("num", 7f);
    doc.addAtom("id", "three");
    w.addDocument(doc);

    IndexReader r = DirectoryReader.open(w, true);
    IndexSearcher s = newSearcher(r);

    // Make sure range query hits the right number of hits
    assertEquals(2, s.search(new MatchAllDocsQuery(), fieldTypes.newHalfFloatRangeFilter("num", 0f, true, 3f, true), 1).totalHits);
    assertEquals(2, s.search(new MatchAllDocsQuery(), fieldTypes.newHalfFloatDocValuesRangeFilter("num", 0f, true, 3f, true), 1).totalHits);
    assertEquals(3, s.search(new MatchAllDocsQuery(), fieldTypes.newHalfFloatRangeFilter("num", 0f, true, 10f, true), 1).totalHits);
    assertEquals(3, s.search(new MatchAllDocsQuery(), fieldTypes.newHalfFloatDocValuesRangeFilter("num", 0f, true, 10f, true), 1).totalHits);
    assertEquals(1, s.search(new MatchAllDocsQuery(), fieldTypes.newHalfFloatRangeFilter("num", 1f, true,2.5f, true), 1).totalHits);
    assertEquals(1, s.search(new MatchAllDocsQuery(), fieldTypes.newHalfFloatDocValuesRangeFilter("num", 1f, true,2.5f, true), 1).totalHits);
    r.close();
    w.close();
    dir.close();
  }

  public void testRandom() throws Exception {
    int iters = atLeast(10000);
    for(int iter=0;iter<iters;iter++) {
      float v = random().nextFloat();
      short x = NumericUtils.halfFloatToShort(v);
      float v2 = NumericUtils.shortToHalfFloat(x);
      assertEquals(v, v2, IOTA);
    }
  }

  public void testNaN() throws Exception {
    assertEquals(Float.NaN, NumericUtils.shortToHalfFloat(NumericUtils.halfFloatToShort(Float.NaN)), 0.0f);
  }

  public void testPositiveInfinity() throws Exception {
    assertEquals(Float.POSITIVE_INFINITY, NumericUtils.shortToHalfFloat(NumericUtils.halfFloatToShort(Float.POSITIVE_INFINITY)), 0.0f);
  }

  public void testNegativeInfinity() throws Exception {
    assertEquals(Float.NEGATIVE_INFINITY, NumericUtils.shortToHalfFloat(NumericUtils.halfFloatToShort(Float.NEGATIVE_INFINITY)), 0.0f);
  }

  public void testBasicSort() throws Exception {
    Directory dir = newDirectory();

    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldTypes fieldTypes = w.getFieldTypes();
    //System.out.println("id type: " + fieldTypes.getFieldType("id"));

    Document doc = w.newDocument();
    doc.addHalfFloat("num", 3f);
    doc.addAtom("id", "one");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addHalfFloat("num", 2f);
    doc.addAtom("id", "two");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addHalfFloat("num", 7f);
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
    doc.addHalfFloat("num", 3f);
    doc.addAtom("id", "one");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addAtom("id", "two");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addHalfFloat("num", 7f);
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
    doc.addHalfFloat("num", 3f);
    doc.addAtom("id", "one");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addAtom("id", "two");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addHalfFloat("num", 7f);
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
    List<Float> values = new ArrayList<>();
    for(int i=0;i<numDocs;i++) {
      Document doc = w.newDocument();
      doc.addUniqueInt("id", i);
      Float num = random().nextFloat();
      values.add(num);
      doc.addHalfFloat("num", num);
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
      float x = random().nextFloat();
      float y = random().nextFloat();

      float min, max;
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
      Filter filter = fieldTypes.newHalfFloatRangeFilter("num", min, true, max, true);

      boolean reversed = random().nextBoolean();
      Sort sort = fieldTypes.newSort("num", reversed);
      if (VERBOSE) {
        System.out.println("TEST: filter=" + filter + " reversed=" + reversed + " sort=" + sort);
      }
      TopDocs hits = s.search(new MatchAllDocsQuery(), filter, numDocs, sort);
      Float last = null;
      boolean wrongValues = false;
      for(ScoreDoc hit : hits.scoreDocs) {
        Document doc = s.doc(hit.doc);
        actual.add(doc.getInt("id"));
        Float v = doc.getHalfFloat("num");
        if (isClose(v, (Float) ((FieldDoc) hit).fields[0]) == false) {
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

      for (Integer id : expected) {
        if (actual.contains(id) == false) {
          float value = values.get(id);
          assertTrue("extra expected: value=" + value + " vs min=" + min + " max=" + max, isClose(value, min) || isClose(value, max));
        }
      }

      for (Integer id : actual) {
        if (expected.contains(id) == false) {
          float value = values.get(id);
          assertTrue("extra actual: value=" + value + " vs min=" + min + " max=" + max, isClose(value, min) || isClose(value, max));
        }
      }

      assertFalse(wrongValues);
    }

    r.close();
    w.close();
    dir.close();
  }

  static boolean isClose(float v1, float v2) {
    return Math.abs(v1-v2) < IOTA;
  }

  public void testMultiValuedSort() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = newRandomIndexWriter(dir);
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setMultiValued("num");

    Document doc = w.newDocument();
    doc.addUniqueInt("id", 0);
    doc.addHalfFloat("num", 45F);
    doc.addHalfFloat("num", -22F);
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addUniqueInt("id", 1);
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addUniqueInt("id", 2);
    doc.addHalfFloat("num", -2F);
    doc.addHalfFloat("num", 14F);
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
    doc.addHalfFloat("num", 45F);
    doc.addHalfFloat("num", -22F);
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addUniqueInt("id", 1);
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addUniqueInt("id", 2);
    doc.addHalfFloat("num", -2F);
    doc.addHalfFloat("num", 14F);
    w.addDocument(doc);

    IndexReader r = w.getReader();
    fieldTypes = r.getFieldTypes();

    IndexSearcher s = newSearcher(r);
    assertEquals(2, s.search(new MatchAllDocsQuery(), fieldTypes.newHalfFloatRangeFilter("num", -100F, true, 100F, true), 1).totalHits);
    assertEquals(1, s.search(new MatchAllDocsQuery(), fieldTypes.newHalfFloatRangeFilter("num", 40F, true, 45F, true), 1).totalHits);
    r.close();
    w.close();
    dir.close();
  }

  public void testExcIndexedThenStored() throws Exception {
    IndexWriter w = newIndexWriter(dir);
    Document doc = w.newDocument();
    doc.addHalfFloat("num", 100f);
    w.addDocument(doc);
    final Document doc2 = w.newDocument();
    shouldFail(() -> doc2.addStoredHalfFloat("num", 200f),
               "field \"num\": cannot addStored: field was already added non-stored");
    w.close();
  }

  public void testExcStoredThenIndexed() throws Exception {
    IndexWriter w = newIndexWriter(dir);
    Document doc = w.newDocument();
    doc.addStoredHalfFloat("num", 100f);
    w.addDocument(doc);
    final Document doc2 = w.newDocument();
    shouldFail(() -> doc2.addHalfFloat("num", 200f),
               "field \"num\": this field is only stored; use addStoredXXX instead");
    w.close();
  }
}
