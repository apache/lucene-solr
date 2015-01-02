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

public class TestIntFields extends LuceneTestCase {

  public void testBasicRange() throws Exception {
    Directory dir = newDirectory();

    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldTypes fieldTypes = w.getFieldTypes();
    //System.out.println("id type: " + fieldTypes.getFieldType("id"));

    Document doc = w.newDocument();
    doc.addInt("num", 3);
    doc.addAtom("id", "one");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addInt("num", 2);
    doc.addAtom("id", "two");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addInt("num", 7);
    doc.addAtom("id", "three");
    w.addDocument(doc);

    IndexReader r = DirectoryReader.open(w, true);
    IndexSearcher s = newSearcher(r);

    // Make sure range query hits the right number of hits
    assertEquals(2, s.search(new MatchAllDocsQuery(), fieldTypes.newIntRangeFilter("num", 0, true, 3, true), 1).totalHits);
    assertEquals(2, s.search(new MatchAllDocsQuery(), fieldTypes.newIntDocValuesRangeFilter("num", 0, true, 3, true), 1).totalHits);
    assertEquals(3, s.search(new MatchAllDocsQuery(), fieldTypes.newIntRangeFilter("num", 0, true, 10, true), 1).totalHits);
    assertEquals(3, s.search(new MatchAllDocsQuery(), fieldTypes.newIntDocValuesRangeFilter("num", 0, true, 10, true), 1).totalHits);
    r.close();
    w.close();
    dir.close();
  }

  public void testBasicSort() throws Exception {
    Directory dir = newDirectory();

    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldTypes fieldTypes = w.getFieldTypes();
    //System.out.println("id type: " + fieldTypes.getFieldType("id"));

    Document doc = w.newDocument();
    doc.addInt("num", 3);
    doc.addAtom("id", "one");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addInt("num", 2);
    doc.addAtom("id", "two");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addInt("num", 7);
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
    doc.addInt("num", 3);
    doc.addAtom("id", "one");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addAtom("id", "two");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addInt("num", 7);
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
    doc.addInt("num", 3);
    doc.addAtom("id", "one");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addAtom("id", "two");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addInt("num", 7);
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
    List<Integer> values = new ArrayList<>();
    for(int i=0;i<numDocs;i++) {
      Document doc = w.newDocument();
      doc.addUniqueInt("id", i);
      Integer num = random().nextInt();
      values.add(num);
      doc.addInt("num", num);
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
      int x = random().nextInt();
      int y = random().nextInt();

      int min, max;
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
        filter = fieldTypes.newIntRangeFilter("num", min, true, max, true);
      } else {
        filter = fieldTypes.newIntDocValuesRangeFilter("num", min, true, max, true);
      }

      boolean reversed = random().nextBoolean();
      Sort sort = fieldTypes.newSort("num", reversed);
      if (VERBOSE) {
        System.out.println("TEST: filter=" + filter + " reversed=" + reversed + " sort=" + sort);
      }
      TopDocs hits = s.search(new MatchAllDocsQuery(), filter, numDocs, sort);
      Integer last = null;
      boolean wrongValues = false;
      for(ScoreDoc hit : hits.scoreDocs) {
        Document doc = s.doc(hit.doc);
        actual.add(doc.getInt("id"));
        Integer v = doc.getInt("num");
        if (v.intValue() != ((Integer) ((FieldDoc) hit).fields[0]).intValue()) {
          System.out.println("  wrong: " + v + " vs " + ((FieldDoc) hit).fields[0]);
          wrongValues = true;
        }
        if (VERBOSE) {
          System.out.println("   hit doc=" + doc);
        }
        if (last != null) {
          int cmp;
          if (v.equals(last) == false) {
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

  public void testMultiValuedSort() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = newRandomIndexWriter(dir);
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setMultiValued("num");

    Document doc = w.newDocument();
    doc.addUniqueInt("id", 0);
    doc.addInt("num", 45);
    doc.addInt("num", -22);
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addUniqueInt("id", 1);
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addUniqueInt("id", 2);
    doc.addInt("num", -2);
    doc.addInt("num", 14);
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
    doc.addInt("num", 45);
    doc.addInt("num", -22);
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addUniqueInt("id", 1);
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addUniqueInt("id", 2);
    doc.addInt("num", -2);
    doc.addInt("num", 14);
    w.addDocument(doc);

    IndexReader r = w.getReader();
    fieldTypes = r.getFieldTypes();

    IndexSearcher s = newSearcher(r);
    assertEquals(2, s.search(new MatchAllDocsQuery(), fieldTypes.newIntRangeFilter("num", -100, true, 100, true), 1).totalHits);
    assertEquals(1, s.search(new MatchAllDocsQuery(), fieldTypes.newIntRangeFilter("num", 40, true, 45, true), 1).totalHits);
    r.close();
    w.close();
    dir.close();
  }

  public void testTermQuery() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = newIndexWriter(dir);
    Document doc = w.newDocument();
    doc.addInt("num", 180);
    w.addDocument(doc);
    DirectoryReader r = DirectoryReader.open(w, true);
    IndexSearcher s = newSearcher(r);
    FieldTypes fieldTypes = s.getFieldTypes();
    assertEquals(1, s.search(fieldTypes.newIntTermQuery("num", 180), 1).totalHits);
    r.close();
    w.close();
    dir.close();
  }

  public void testJustStored() throws Exception {
    IndexWriter w = newIndexWriter(dir);
    Document doc = w.newDocument();
    doc.addStoredInt("num", 180);
    w.addDocument(doc);
    DirectoryReader r = DirectoryReader.open(w, true);
    IndexSearcher s = newSearcher(r);
    doc = s.doc(0);
    assertEquals(180, doc.getInt("num").intValue());
    r.close();
    w.close();
  }

  public void testExcIndexedThenStored() throws Exception {
    IndexWriter w = newIndexWriter(dir);
    Document doc = w.newDocument();
    doc.addInt("num", 100);
    w.addDocument(doc);
    final Document doc2 = w.newDocument();
    shouldFail(() -> doc2.addStoredInt("num", 200),
               "field \"num\": cannot addStored: field was already added non-stored");
    w.close();
  }

  public void testExcStoredThenIndexed() throws Exception {
    IndexWriter w = newIndexWriter(dir);
    Document doc = w.newDocument();
    doc.addStoredInt("num", 100);
    w.addDocument(doc);
    final Document doc2 = w.newDocument();
    shouldFail(() -> doc2.addInt("num", 200),
               "field \"num\": this field is only stored; use addStoredXXX instead");
    w.close();
  }
}
