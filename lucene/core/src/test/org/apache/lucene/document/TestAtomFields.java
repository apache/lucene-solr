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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.HalfFloat;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.TestUtil;

public class TestAtomFields extends LuceneTestCase {

  /** Make sure that if we index an ATOM field, at search time we get KeywordAnalyzer for it. */
  public void testFieldUsesKeywordAnalyzer() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    IndexWriter w = new IndexWriter(dir, iwc);
    FieldTypes fieldTypes = w.getFieldTypes();
    Document doc = w.newDocument();
    doc.addAtom("id", "foo bar");
    w.addDocument(doc);
    BaseTokenStreamTestCase.assertTokenStreamContents(fieldTypes.getQueryAnalyzer().tokenStream("id", "foo bar"), new String[] {"foo bar"}, new int[1], new int[] {7});
    w.close();
    dir.close();
  }

  public void testBinaryAtomSort() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    w.commit();
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setSortMissingFirst("atom");

    Document doc = w.newDocument();
    doc.addAtom("atom", new BytesRef("z"));
    doc.addAtom("id", "0");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addAtom("atom", new BytesRef("a"));
    doc.addAtom("id", "1");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addAtom("id", "2");
    w.addDocument(doc);
    
    DirectoryReader r = DirectoryReader.open(w, true);
    fieldTypes = r.getFieldTypes();
    IndexSearcher s = newSearcher(r);
    TopDocs hits = s.search(new MatchAllDocsQuery(), 3, fieldTypes.newSort("atom"));
    assertEquals(3, hits.totalHits);
    assertEquals("2", s.doc(hits.scoreDocs[0].doc).getString("id"));
    assertEquals("1", s.doc(hits.scoreDocs[1].doc).getString("id"));
    assertEquals("0", s.doc(hits.scoreDocs[2].doc).getString("id"));
    r.close();
    w.close();
    dir.close();
  }

  public void testSortMissingFirst() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    w.commit();
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setSortMissingFirst("atom");

    Document doc = w.newDocument();
    doc.addAtom("atom", "z");
    doc.addAtom("id", "0");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addAtom("atom", "a");
    doc.addAtom("id", "1");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addAtom("id", "2");
    w.addDocument(doc);
    
    DirectoryReader r = DirectoryReader.open(w, true);
    fieldTypes = r.getFieldTypes();
    IndexSearcher s = newSearcher(r);
    TopDocs hits = s.search(new MatchAllDocsQuery(), 3, fieldTypes.newSort("atom"));
    assertEquals(3, hits.totalHits);
    assertEquals("2", s.doc(hits.scoreDocs[0].doc).getString("id"));
    assertEquals("1", s.doc(hits.scoreDocs[1].doc).getString("id"));
    assertEquals("0", s.doc(hits.scoreDocs[2].doc).getString("id"));
    r.close();
    w.close();
    dir.close();
  }

  public void testSortMissingFirstReversed() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    w.commit();
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.enableSorting("atom", true);
    fieldTypes.setSortMissingFirst("atom");

    Document doc = w.newDocument();
    doc.addAtom("atom", "z");
    doc.addAtom("id", "0");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addAtom("atom", "a");
    doc.addAtom("id", "1");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addAtom("id", "2");
    w.addDocument(doc);
    
    DirectoryReader r = DirectoryReader.open(w, true);
    fieldTypes = r.getFieldTypes();
    IndexSearcher s = newSearcher(r);
    TopDocs hits = s.search(new MatchAllDocsQuery(), 3, fieldTypes.newSort("atom"));
    assertEquals(3, hits.totalHits);
    assertEquals("2", s.doc(hits.scoreDocs[0].doc).getString("id"));
    assertEquals("0", s.doc(hits.scoreDocs[1].doc).getString("id"));
    assertEquals("1", s.doc(hits.scoreDocs[2].doc).getString("id"));
    r.close();
    w.close();
    dir.close();
  }

  public void testSortMissingLast() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    w.commit();
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.enableSorting("atom");
    fieldTypes.setSortMissingLast("atom");

    Document doc = w.newDocument();
    doc.addAtom("atom", "z");
    doc.addAtom("id", "0");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addAtom("atom", "a");
    doc.addAtom("id", "1");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addAtom("id", "2");
    w.addDocument(doc);
    
    DirectoryReader r = DirectoryReader.open(w, true);
    fieldTypes = r.getFieldTypes();
    IndexSearcher s = newSearcher(r);
    TopDocs hits = s.search(new MatchAllDocsQuery(), 3, fieldTypes.newSort("atom"));
    assertEquals(3, hits.totalHits);
    assertEquals("1", s.doc(hits.scoreDocs[0].doc).getString("id"));
    assertEquals("0", s.doc(hits.scoreDocs[1].doc).getString("id"));
    assertEquals("2", s.doc(hits.scoreDocs[2].doc).getString("id"));
    r.close();
    w.close();
    dir.close();
  }

  public void testSortMissingLastReversed() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    w.commit();
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.enableSorting("atom", true);
    fieldTypes.setSortMissingLast("atom");

    Document doc = w.newDocument();
    doc.addAtom("atom", "z");
    doc.addAtom("id", "0");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addAtom("atom", "a");
    doc.addAtom("id", "1");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addAtom("id", "2");
    w.addDocument(doc);
    
    DirectoryReader r = DirectoryReader.open(w, true);
    fieldTypes = r.getFieldTypes();
    IndexSearcher s = newSearcher(r);
    TopDocs hits = s.search(new MatchAllDocsQuery(), 3, fieldTypes.newSort("atom"));
    assertEquals(3, hits.totalHits);
    assertEquals("0", s.doc(hits.scoreDocs[0].doc).getString("id"));
    assertEquals("1", s.doc(hits.scoreDocs[1].doc).getString("id"));
    assertEquals("2", s.doc(hits.scoreDocs[2].doc).getString("id"));
    r.close();
    w.close();
    dir.close();
  }

  public void testSortMissingDefault() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    w.commit();

    Document doc = w.newDocument();
    doc.addAtom("atom", "z");
    doc.addAtom("id", "0");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addAtom("atom", "a");
    doc.addAtom("id", "1");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addAtom("id", "2");
    w.addDocument(doc);
    
    DirectoryReader r = DirectoryReader.open(w, true);
    FieldTypes fieldTypes = r.getFieldTypes();
    IndexSearcher s = newSearcher(r);
    TopDocs hits = s.search(new MatchAllDocsQuery(), 3, fieldTypes.newSort("atom"));
    assertEquals(3, hits.totalHits);
    assertEquals("1", s.doc(hits.scoreDocs[0].doc).getString("id"));
    assertEquals("0", s.doc(hits.scoreDocs[1].doc).getString("id"));
    assertEquals("2", s.doc(hits.scoreDocs[2].doc).getString("id"));
    r.close();
    w.close();
    dir.close();
  }

  public void testSortMissingDefaultReversed() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    w.commit();

    Document doc = w.newDocument();
    doc.addAtom("atom", "z");
    doc.addAtom("id", "0");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addAtom("atom", "a");
    doc.addAtom("id", "1");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addAtom("id", "2");
    w.addDocument(doc);
    
    DirectoryReader r = DirectoryReader.open(w, true);
    FieldTypes fieldTypes = r.getFieldTypes();
    IndexSearcher s = newSearcher(r);
    TopDocs hits = s.search(new MatchAllDocsQuery(), 3, fieldTypes.newSort("atom", true));
    assertEquals(3, hits.totalHits);
    assertEquals("0", s.doc(hits.scoreDocs[0].doc).getString("id"));
    assertEquals("1", s.doc(hits.scoreDocs[1].doc).getString("id"));
    assertEquals("2", s.doc(hits.scoreDocs[2].doc).getString("id"));
    r.close();
    w.close();
    dir.close();
  }

  public void testMinMaxAtom() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setMinMaxTokenLength("field", 2, 7);
    fieldTypes.setMultiValued("field");
    Document doc = w.newDocument();
    doc.addAtom("field", "a");
    doc.addAtom("field", "ab");
    doc.addAtom("field", "goodbyeyou");
    w.addDocument(doc);

    DirectoryReader r = DirectoryReader.open(w, true);
    fieldTypes = r.getFieldTypes();

    IndexSearcher s = newSearcher(r);
    assertEquals(0, hitCount(s, fieldTypes.newExactStringQuery("field", "a")));
    assertEquals(1, hitCount(s, fieldTypes.newExactStringQuery("field", "ab")));
    assertEquals(0, hitCount(s, fieldTypes.newExactStringQuery("field", "goodbyeyou")));
    r.close();
    w.close();
    dir.close();
  }

  public void testMinMaxBinaryAtom() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setMinMaxTokenLength("field", 2, 7);
    fieldTypes.setMultiValued("field");
    Document doc = w.newDocument();
    doc.addAtom("field", new BytesRef(new byte[1]));
    doc.addAtom("field", new BytesRef(new byte[2]));
    doc.addAtom("field", new BytesRef(new byte[10]));
    w.addDocument(doc);

    DirectoryReader r = DirectoryReader.open(w, true);
    fieldTypes = r.getFieldTypes();

    IndexSearcher s = newSearcher(r);
    assertEquals(0, hitCount(s, fieldTypes.newExactBinaryQuery("field", new byte[1])));
    assertEquals(1, hitCount(s, fieldTypes.newExactBinaryQuery("field", new byte[2])));
    assertEquals(0, hitCount(s, fieldTypes.newExactBinaryQuery("field", new byte[10])));
    r.close();
    w.close();
    dir.close();
  }

  public void testReversedStringAtom() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setReversedTerms("field");
    Document doc = w.newDocument();
    doc.addAtom("field", "Foobar");
    w.addDocument(doc);

    DirectoryReader r = DirectoryReader.open(w, true);
    fieldTypes = r.getFieldTypes();

    IndexSearcher s = newSearcher(r);
    assertEquals(1, hitCount(s, fieldTypes.newExactStringQuery("field", "rabooF")));
    r.close();
    w.close();
    dir.close();
  }

  public void testReversedBinaryAtom() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setReversedTerms("field");
    Document doc = w.newDocument();
    doc.addAtom("field", new BytesRef("Foobar"));
    w.addDocument(doc);

    DirectoryReader r = DirectoryReader.open(w, true);
    fieldTypes = r.getFieldTypes();

    IndexSearcher s = newSearcher(r);
    assertEquals(1, hitCount(s, fieldTypes.newExactBinaryQuery("field", new BytesRef("rabooF"))));
    r.close();
    w.close();
    dir.close();
  }

  public void testMultiValuedSort() throws Exception {
    assumeTrue("DV format not supported", Arrays.asList("Memory", "SimpleText").contains(TestUtil.getDocValuesFormat("field")) == false);

    RandomIndexWriter w = newRandomIndexWriter(dir);
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setMultiValued("field");

    Document doc = w.newDocument();
    doc.addUniqueInt("id", 0);
    doc.addAtom("field", "zzz");
    doc.addAtom("field", "a");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addUniqueInt("id", 1);
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addUniqueInt("id", 2);
    doc.addAtom("field", "d");
    doc.addAtom("field", "m");
    w.addDocument(doc);

    IndexReader r = w.getReader();
    fieldTypes = r.getFieldTypes();

    IndexSearcher s = newSearcher(r);
    TopDocs hits = s.search(new MatchAllDocsQuery(), 10, fieldTypes.newSort("field"));

    // Default selector is MIN:
    assertEquals(0, s.doc(hits.scoreDocs[0].doc).get("id"));
    assertEquals(2, s.doc(hits.scoreDocs[1].doc).get("id"));
    assertEquals(1, s.doc(hits.scoreDocs[2].doc).get("id"));

    fieldTypes.setMultiValuedStringSortSelector("field", SortedSetSelector.Type.MAX);
    hits = s.search(new MatchAllDocsQuery(), 10, fieldTypes.newSort("field"));
    assertEquals(2, s.doc(hits.scoreDocs[0].doc).get("id"));
    assertEquals(0, s.doc(hits.scoreDocs[1].doc).get("id"));
    assertEquals(1, s.doc(hits.scoreDocs[2].doc).get("id"));

    r.close();
    w.close();
  }
}
