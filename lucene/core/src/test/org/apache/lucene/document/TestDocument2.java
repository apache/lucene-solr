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

import java.io.StringReader;
import java.util.Arrays;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.blocktree.Stats;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo.DocValuesType;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredDocument;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.junit.Ignore;

public class TestDocument2 extends LuceneTestCase {

  public void testBasic() throws Exception {
    Directory dir = newDirectory();
    Analyzer a = new MockAnalyzer(random());
    FieldTypes types = new FieldTypes(a);

    IndexWriter w = new IndexWriter(dir, types.getDefaultIndexWriterConfig());
    types.setIndexWriter(w);

    Document2 doc = new Document2(types);
    doc.addLargeText("body", "some text");
    doc.addShortText("title", "a title");
    doc.addAtom("id", "29jafnn");
    doc.addStored("bytes", new byte[7]);
    doc.addNumber("number", 17);
    w.addDocument(doc);
    w.close();
    dir.close();
  }

  public void testBinaryAtom() throws Exception {
    Directory dir = newDirectory();
    Analyzer a = new MockAnalyzer(random());
    FieldTypes types = new FieldTypes(a);

    IndexWriter w = new IndexWriter(dir, types.getDefaultIndexWriterConfig());
    types.setIndexWriter(w);

    Document2 doc = new Document2(types);
    doc.addAtom("binary", new byte[5]);
    w.addDocument(doc);
    IndexReader r = DirectoryReader.open(w, true);
    IndexSearcher s = newSearcher(r);
    assertEquals(1, s.search(types.newTermQuery("binary", new byte[5]), 1).totalHits);
    r.close();
    w.close();
    dir.close();
  }

  public void testBinaryAtomSort() throws Exception {
    Directory dir = newDirectory();
    Analyzer a = new MockAnalyzer(random());
    FieldTypes types = new FieldTypes(a);

    IndexWriter w = new IndexWriter(dir, types.getDefaultIndexWriterConfig());
    types.setIndexWriter(w);
    types.enableStored("id");
    types.enableSorted("binary");

    Document2 doc = new Document2(types);
    byte[] value = new byte[5];
    value[0] = 1;
    doc.addAtom("id", "0");
    doc.addAtom("binary", value);
    w.addDocument(doc);

    doc = new Document2(types);
    doc.addAtom("id", "1");
    doc.addAtom("binary", new byte[5]);
    w.addDocument(doc);
    
    IndexReader r = DirectoryReader.open(w, true);
    IndexSearcher s = newSearcher(r);
    TopDocs hits = s.search(new MatchAllDocsQuery(), 2, types.newSort("binary"));
    assertEquals(2, hits.scoreDocs.length);
    assertEquals("1", r.document(hits.scoreDocs[0].doc).get("id"));
    assertEquals("0", r.document(hits.scoreDocs[1].doc).get("id"));
    r.close();
    w.close();
    dir.close();
  }

  public void testBinaryStored() throws Exception {
    Directory dir = newDirectory();
    Analyzer a = new MockAnalyzer(random());
    FieldTypes types = new FieldTypes(a);

    IndexWriter w = new IndexWriter(dir, types.getDefaultIndexWriterConfig());
    types.setIndexWriter(w);

    Document2 doc = new Document2(types);
    doc.addStored("binary", new byte[5]);
    w.addDocument(doc);
    IndexReader r = DirectoryReader.open(w, true);
    IndexSearcher s = newSearcher(r);
    assertEquals(new BytesRef(new byte[5]), r.document(0).getBinaryValue("binary"));
    r.close();
    w.close();
    dir.close();
  }

  public void testSortedSetDocValues() throws Exception {
    Directory dir = newDirectory();
    Analyzer a = new MockAnalyzer(random());
    FieldTypes types = new FieldTypes(a);

    IndexWriter w = new IndexWriter(dir, types.getDefaultIndexWriterConfig());
    types.setIndexWriter(w);
    types.setDocValuesType("sortedset", DocValuesType.SORTED_SET);

    Document2 doc = new Document2(types);
    doc.addAtom("sortedset", "one");
    doc.addAtom("sortedset", "two");
    doc.addAtom("sortedset", "three");
    w.addDocument(doc);
    IndexReader r = DirectoryReader.open(w, true);
    SortedSetDocValues ssdv = MultiDocValues.getSortedSetValues(r, "sortedset");
    ssdv.setDocument(0);

    long ord = ssdv.nextOrd();
    assertTrue(ord != SortedSetDocValues.NO_MORE_ORDS);
    assertEquals(new BytesRef("one"), ssdv.lookupOrd(ord));

    ord = ssdv.nextOrd();
    assertTrue(ord != SortedSetDocValues.NO_MORE_ORDS);
    assertEquals(new BytesRef("three"), ssdv.lookupOrd(ord));

    ord = ssdv.nextOrd();
    assertTrue(ord != SortedSetDocValues.NO_MORE_ORDS);
    assertEquals(new BytesRef("two"), ssdv.lookupOrd(ord));

    assertEquals(SortedSetDocValues.NO_MORE_ORDS, ssdv.nextOrd());
    w.close();
    r.close();
    dir.close();
  }

  public void testSortedNumericDocValues() throws Exception {
    Directory dir = newDirectory();
    Analyzer a = new MockAnalyzer(random());
    FieldTypes types = new FieldTypes(a);

    IndexWriter w = new IndexWriter(dir, types.getDefaultIndexWriterConfig());
    types.setIndexWriter(w);
    types.setDocValuesType("sortednumeric", DocValuesType.SORTED_NUMERIC);

    Document2 doc = new Document2(types);
    doc.addNumber("sortednumeric", 3);
    doc.addNumber("sortednumeric", 1);
    doc.addNumber("sortednumeric", 2);
    w.addDocument(doc);
    IndexReader r = DirectoryReader.open(w, true);
    SortedNumericDocValues sndv = MultiDocValues.getSortedNumericValues(r, "sortednumeric");
    sndv.setDocument(0);

    assertEquals(3, sndv.count());
    assertEquals(1, sndv.valueAt(0));
    assertEquals(2, sndv.valueAt(1));
    assertEquals(3, sndv.valueAt(2));
    w.close();
    r.close();
    dir.close();
  }

  public void testFloatRangeQuery() throws Exception {
    Directory dir = newDirectory();
    Analyzer a = new MockAnalyzer(random());
    FieldTypes types = new FieldTypes(a);

    IndexWriter w = new IndexWriter(dir, types.getDefaultIndexWriterConfig());
    types.setIndexWriter(w);
    types.enableStored("id");
    types.enableSorted("id");
    //System.out.println("id type: " + types.getFieldType("id"));

    Document2 doc = new Document2(types);
    doc.addNumber("float", 3f);
    doc.addAtom("id", "one");
    w.addDocument(doc);

    doc = new Document2(types);
    doc.addNumber("float", 2f);
    doc.addAtom("id", "two");
    w.addDocument(doc);

    doc = new Document2(types);
    doc.addNumber("float", 7f);
    doc.addAtom("id", "three");
    w.addDocument(doc);

    IndexReader r = DirectoryReader.open(w, true);
    IndexSearcher s = newSearcher(r);

    // Make sure range query hits the right number of hits
    assertEquals(2, s.search(types.newRangeQuery("float", 0f, true, 3f, true), 1).totalHits);
    assertEquals(3, s.search(types.newRangeQuery("float", 0f, true, 10f, true), 1).totalHits);
    assertEquals(1, s.search(types.newRangeQuery("float", 1f, true,2.5f, true), 1).totalHits);

    // Make sure doc values shows the correct float values:
    TopDocs hits = s.search(new MatchAllDocsQuery(), 3, types.newSort("id"));
    assertEquals(3, hits.totalHits);
    NumericDocValues ndv = MultiDocValues.getNumericValues(r, "float");
    assertNotNull(ndv);
    ScoreDoc hit = hits.scoreDocs[0];
    StoredDocument storedDoc = r.document(hit.doc);
    assertEquals("one", storedDoc.get("id"));
    assertEquals(3f, Float.intBitsToFloat((int) ndv.get(hit.doc)), .001f);

    hit = hits.scoreDocs[1];
    storedDoc = r.document(hit.doc);
    assertEquals("three", storedDoc.get("id"));
    assertEquals(7f, Float.intBitsToFloat((int) ndv.get(hit.doc)), .001f);

    hit = hits.scoreDocs[2];
    storedDoc = r.document(hit.doc);
    assertEquals("two", storedDoc.get("id"));
    assertEquals(2f, Float.intBitsToFloat((int) ndv.get(hit.doc)), .001f);

    // Make sure we can sort by the field:
    hits = s.search(new MatchAllDocsQuery(), 3, types.newSort("float"));
    assertEquals(3, hits.totalHits);
    assertEquals("two", r.document(hits.scoreDocs[0].doc).get("id"));
    assertEquals("one", r.document(hits.scoreDocs[1].doc).get("id"));
    assertEquals("three", r.document(hits.scoreDocs[2].doc).get("id"));

    w.close();
    r.close();
    dir.close();
  }

  // Cannot change a field from INT to DOUBLE
  public void testInvalidNumberTypeChange() throws Exception {
    Directory dir = newDirectory();
    Analyzer a = new MockAnalyzer(random());
    FieldTypes types = new FieldTypes(a);

    IndexWriter w = new IndexWriter(dir, types.getDefaultIndexWriterConfig());
    types.setIndexWriter(w);

    Document2 doc = new Document2(types);
    doc.addNumber("int", 3);
    w.addDocument(doc);

    doc = new Document2(types);
    try {
      doc.addNumber("int", 2.0);
      fail("did not hit exception");
    } catch (IllegalStateException ise) {
      // expected
    }
    w.close();
    dir.close();
  }

  public void testIntRangeQuery() throws Exception {
    Directory dir = newDirectory();
    Analyzer a = new MockAnalyzer(random());
    FieldTypes types = new FieldTypes(a);

    IndexWriter w = new IndexWriter(dir, types.getDefaultIndexWriterConfig());
    types.setIndexWriter(w);

    Document2 doc = new Document2(types);
    doc.addNumber("int", 3);
    w.addDocument(doc);

    doc = new Document2(types);
    doc.addNumber("int", 2);
    w.addDocument(doc);

    doc = new Document2(types);
    doc.addNumber("int", 7);
    w.addDocument(doc);

    IndexReader r = DirectoryReader.open(w, true);
    IndexSearcher s = newSearcher(r);

    assertEquals(2, s.search(types.newRangeQuery("int", 0, true, 3, true), 1).totalHits);
    assertEquals(3, s.search(types.newRangeQuery("int", 0, true, 10, true), 1).totalHits);
    w.close();
    r.close();
    dir.close();
  }

  public void testExcMissingSetIndexWriter() throws Exception {
    Analyzer a = new MockAnalyzer(random());
    FieldTypes types = new FieldTypes(a);
    Document2 doc = new Document2(types);
    try {
      doc.addLargeText("body", "some text");
      fail("did not hit expected exception");
    } catch (IllegalStateException ise) {
      // expected
    }
  }

  public void testExcAnalyzerForAtomField() throws Exception {
    Directory dir = newDirectory();
    Analyzer a = new MockAnalyzer(random());
    FieldTypes types = new FieldTypes(a);
    IndexWriter w = new IndexWriter(dir, types.getDefaultIndexWriterConfig());
    types.setIndexWriter(w);
    types.setAnalyzer("atom", a);
    Document2 doc = new Document2(types);
    try {
      doc.addAtom("atom", "blahblah");
      fail("did not hit expected exception");
    } catch (IllegalStateException ise) {
      // expected
    }
    w.close();
    dir.close();
  }

  // Can't ask for SORTED dv but then add the field as a number
  public void testExcInvalidDocValuesTypeFirst() throws Exception {
    Directory dir = newDirectory();
    Analyzer a = new MockAnalyzer(random());
    FieldTypes types = new FieldTypes(a);
    IndexWriter w = new IndexWriter(dir, types.getDefaultIndexWriterConfig());
    types.setIndexWriter(w);
    types.setDocValuesType("string", DocValuesType.SORTED);
    Document2 doc = new Document2(types);
    try {
      doc.addNumber("string", 17);
      fail("did not hit expected exception");
    } catch (IllegalStateException ise) {
      // expected
    }
    doc.addAtom("string", "a string");
    w.addDocument(doc);
    w.close();
    dir.close();
  }

  // Can't ask for BINARY dv but then add the field as a number
  public void testExcInvalidBinaryDocValuesTypeFirst() throws Exception {
    Directory dir = newDirectory();
    Analyzer a = new MockAnalyzer(random());
    FieldTypes types = new FieldTypes(a);
    IndexWriter w = new IndexWriter(dir, types.getDefaultIndexWriterConfig());
    types.setIndexWriter(w);
    types.setDocValuesType("binary", DocValuesType.BINARY);
    Document2 doc = new Document2(types);
    try {
      doc.addNumber("binary", 17);
      fail("did not hit expected exception");
    } catch (IllegalStateException ise) {
      // expected
    }
    doc.addAtom("binary", new byte[7]);
    w.addDocument(doc);
    w.close();
    dir.close();
  }

  // Cannot store Reader:
  public void testExcStoreReaderFields() throws Exception {
    Directory dir = newDirectory();
    Analyzer a = new MockAnalyzer(random());
    FieldTypes types = new FieldTypes(a);
    IndexWriter w = new IndexWriter(dir, types.getDefaultIndexWriterConfig());
    types.setIndexWriter(w);
    types.enableStored("body");
    Document2 doc = new Document2(types);
    try {
      doc.addLargeText("body", new StringReader("a small string"));
      fail("did not hit expected exception");
    } catch (IllegalStateException ise) {
      // expected
    }
    doc.addLargeText("body", "a string");
    w.addDocument(doc);
    w.close();
    dir.close();
  }

  // Cannot store TokenStream:
  public void testExcStorePreTokenizedFields() throws Exception {
    Directory dir = newDirectory();
    Analyzer a = new MockAnalyzer(random());
    FieldTypes types = new FieldTypes(a);
    IndexWriter w = new IndexWriter(dir, types.getDefaultIndexWriterConfig());
    types.setIndexWriter(w);
    types.enableStored("body");
    Document2 doc = new Document2(types);
    try {
      doc.addLargeText("body", new CannedTokenStream());
      fail("did not hit expected exception");
    } catch (IllegalStateException ise) {
      // expected
    }
    doc.addLargeText("body", "a string");
    w.addDocument(doc);
    w.close();
    dir.close();
  }

  public void testSortable() throws Exception {
    Directory dir = newDirectory();
    Analyzer a = new MockAnalyzer(random());
    FieldTypes types = new FieldTypes(a);
    IndexWriter w = new IndexWriter(dir, types.getDefaultIndexWriterConfig());
    types.setIndexWriter(w);

    // Normally sorting is not enabled for atom fields:
    types.enableSorted("id");
    types.enableStored("id");

    Document2 doc = new Document2(types);

    doc.addAtom("id", "two");
    w.addDocument(doc);
    doc = new Document2(types);
    doc.addAtom("id", "one");
    w.addDocument(doc);
    IndexReader r = DirectoryReader.open(w, true);
    IndexSearcher s = newSearcher(r);
    TopDocs hits = s.search(new MatchAllDocsQuery(), 2, types.newSort("id"));
    assertEquals(2, hits.scoreDocs.length);
    assertEquals("one", r.document(hits.scoreDocs[0].doc).get("id"));
    assertEquals("two", r.document(hits.scoreDocs[1].doc).get("id"));
    r.close();
    w.close();
    dir.close();
  }

  public void testMultiValuedNumeric() throws Exception {
    Directory dir = newDirectory();
    Analyzer a = new MockAnalyzer(random());
    FieldTypes types = new FieldTypes(a);
    IndexWriter w = new IndexWriter(dir, types.getDefaultIndexWriterConfig());
    types.setIndexWriter(w);

    types.setMultiValued("numbers");
    types.enableSorted("numbers");
    types.enableStored("id");

    Document2 doc = new Document2(types);
    doc.addNumber("numbers", 1);
    doc.addNumber("numbers", 2);
    doc.addAtom("id", "one");
    w.addDocument(doc);

    doc = new Document2(types);
    doc.addNumber("numbers", -10);
    doc.addNumber("numbers", -20);
    doc.addAtom("id", "two");
    w.addDocument(doc);

    IndexReader r = DirectoryReader.open(w, true);
    IndexSearcher s = newSearcher(r);
    TopDocs hits = s.search(new MatchAllDocsQuery(), 2, types.newSort("numbers"));
    assertEquals(2, hits.scoreDocs.length);
    assertEquals("two", r.document(hits.scoreDocs[0].doc).get("id"));
    assertEquals("one", r.document(hits.scoreDocs[1].doc).get("id"));
    r.close();
    w.close();
    dir.close();
  }

  public void testMultiValuedString() throws Exception {
    Directory dir = newDirectory();
    Analyzer a = new MockAnalyzer(random());
    FieldTypes types = new FieldTypes(a);
    IndexWriter w = new IndexWriter(dir, types.getDefaultIndexWriterConfig());
    types.setIndexWriter(w);

    types.setMultiValued("strings");
    types.enableSorted("strings");
    types.enableStored("id");

    Document2 doc = new Document2(types);
    doc.addAtom("strings", "abc");
    doc.addAtom("strings", "baz");
    doc.addAtom("id", "one");
    w.addDocument(doc);

    doc = new Document2(types);
    doc.addAtom("strings", "aaa");
    doc.addAtom("strings", "bbb");
    doc.addAtom("id", "two");
    w.addDocument(doc);

    IndexReader r = DirectoryReader.open(w, true);
    IndexSearcher s = newSearcher(r);
    TopDocs hits = s.search(new MatchAllDocsQuery(), 2, types.newSort("strings"));
    assertEquals(2, hits.scoreDocs.length);
    assertEquals("two", r.document(hits.scoreDocs[0].doc).get("id"));
    assertEquals("one", r.document(hits.scoreDocs[1].doc).get("id"));
    r.close();
    w.close();
    dir.close();
  }

  // You cannot have multi-valued DocValuesType.BINARY
  public void testExcMultiValuedDVBinary() throws Exception {
    Directory dir = newDirectory();
    Analyzer a = new MockAnalyzer(random());
    FieldTypes types = new FieldTypes(a);
    IndexWriter w = new IndexWriter(dir, types.getDefaultIndexWriterConfig());
    types.setIndexWriter(w);
    types.setDocValuesType("binary", DocValuesType.BINARY);
    try {
      types.setMultiValued("binary");
      fail("did not hit expected exception");
    } catch (IllegalStateException ise) {
      // expected
    }
    assertFalse(types.getMultiValued("binary"));
    Document2 doc = new Document2(types);
    doc.addStored("binary", new byte[7]);
    w.addDocument(doc);
    w.close();
    dir.close();
  }

  // You cannot have multi-valued DocValuesType.SORTED
  public void testExcMultiValuedDVSorted() throws Exception {
    Directory dir = newDirectory();
    Analyzer a = new MockAnalyzer(random());
    FieldTypes types = new FieldTypes(a);
    IndexWriter w = new IndexWriter(dir, types.getDefaultIndexWriterConfig());
    types.setIndexWriter(w);
    types.setDocValuesType("sorted", DocValuesType.SORTED);
    try {
      types.setMultiValued("sorted");
      fail("did not hit expected exception");
    } catch (IllegalStateException ise) {
      // expected
    }
    assertFalse(types.getMultiValued("sorted"));
    Document2 doc = new Document2(types);
    doc.addStored("binary", new byte[7]);
    w.addDocument(doc);
    w.close();
    dir.close();
  }

  // You cannot have multi-valued DocValuesType.NUMERIC
  public void testExcMultiValuedDVNumeric() throws Exception {
    Directory dir = newDirectory();
    Analyzer a = new MockAnalyzer(random());
    FieldTypes types = new FieldTypes(a);
    IndexWriter w = new IndexWriter(dir, types.getDefaultIndexWriterConfig());
    types.setIndexWriter(w);
    types.setDocValuesType("numeric", DocValuesType.NUMERIC);
    try {
      types.setMultiValued("numeric");
      fail("did not hit expected exception");
    } catch (IllegalStateException ise) {
      // expected
    }
    assertFalse(types.getMultiValued("numeric"));
    Document2 doc = new Document2(types);
    doc.addNumber("numeric", 17);
    w.addDocument(doc);
    w.close();
    dir.close();
  }

  public void testPostingsFormat() throws Exception {
    Directory dir = newDirectory();
    Analyzer a = new MockAnalyzer(random());
    FieldTypes types = new FieldTypes(a);
    IndexWriterConfig iwc = types.getDefaultIndexWriterConfig();
    IndexWriter w = new IndexWriter(dir, iwc);
    types.setIndexWriter(w);

    types.setPostingsFormat("id", "Memory");
    types.enableStored("id");

    Document2 doc = new Document2(types);
    doc.addAtom("id", "0");
    w.addDocument(doc);

    doc = new Document2(types);
    doc.addAtom("id", "1");
    w.addDocument(doc);

    IndexReader r = DirectoryReader.open(w, true);
    IndexSearcher s = newSearcher(r);
    TopDocs hits = s.search(types.newTermQuery("id", "0"), 1);
    assertEquals(1, hits.scoreDocs.length);
    assertEquals("0", r.document(hits.scoreDocs[0].doc).get("id"));
    hits = s.search(types.newTermQuery("id", "1"), 1);
    assertEquals(1, hits.scoreDocs.length);
    assertEquals("1", r.document(hits.scoreDocs[0].doc).get("id"));
    r.close();
    w.close();
    dir.close();
  }

  public void testLongTermQuery() throws Exception {
    Directory dir = newDirectory();
    Analyzer a = new MockAnalyzer(random());
    FieldTypes types = new FieldTypes(a);
    IndexWriterConfig iwc = types.getDefaultIndexWriterConfig();
    IndexWriter w = new IndexWriter(dir, iwc);
    types.setIndexWriter(w);

    Document2 doc = new Document2(types);
    doc.addNumber("id", 1L);
    w.addDocument(doc);

    IndexReader r = DirectoryReader.open(w, true);
    IndexSearcher s = newSearcher(r);
    TopDocs hits = s.search(types.newTermQuery("id", 1L), 1);
    assertEquals(1, hits.scoreDocs.length);
    r.close();
    w.close();
    dir.close();
  }

  public void testIntTermQuery() throws Exception {
    Directory dir = newDirectory();
    Analyzer a = new MockAnalyzer(random());
    FieldTypes types = new FieldTypes(a);
    IndexWriterConfig iwc = types.getDefaultIndexWriterConfig();
    IndexWriter w = new IndexWriter(dir, iwc);
    types.setIndexWriter(w);

    Document2 doc = new Document2(types);
    doc.addNumber("id", 1);
    w.addDocument(doc);

    IndexReader r = DirectoryReader.open(w, true);
    IndexSearcher s = newSearcher(r);
    TopDocs hits = s.search(types.newTermQuery("id", 1), 1);
    assertEquals(1, hits.scoreDocs.length);
    r.close();
    w.close();
    dir.close();
  }

  public void testBinaryTermQuery() throws Exception {
    Directory dir = newDirectory();
    Analyzer a = new MockAnalyzer(random());
    FieldTypes types = new FieldTypes(a);
    IndexWriterConfig iwc = types.getDefaultIndexWriterConfig();
    IndexWriter w = new IndexWriter(dir, iwc);
    types.setIndexWriter(w);
    types.setIndexOptions("id", IndexOptions.DOCS_ONLY);

    Document2 doc = new Document2(types);
    doc.addStored("id", new byte[1]);
    w.addDocument(doc);

    IndexReader r = DirectoryReader.open(w, true);
    IndexSearcher s = newSearcher(r);
    TopDocs hits = s.search(types.newTermQuery("id", new byte[1]), 1);
    assertEquals(1, hits.scoreDocs.length);
    r.close();
    w.close();
    dir.close();
  }

  public void testDocValuesFormat() throws Exception {
    Directory dir = newDirectory();
    Analyzer a = new MockAnalyzer(random());
    FieldTypes types = new FieldTypes(a);
    IndexWriterConfig iwc = types.getDefaultIndexWriterConfig();
    IndexWriter w = new IndexWriter(dir, iwc);
    types.setIndexWriter(w);

    types.setDocValuesFormat("id", "Memory");
    types.enableStored("id");
    types.enableSorted("id");

    Document2 doc = new Document2(types);
    doc.addAtom("id", "1");
    w.addDocument(doc);

    doc = new Document2(types);
    doc.addAtom("id", "0");
    w.addDocument(doc);

    IndexReader r = DirectoryReader.open(w, true);
    IndexSearcher s = newSearcher(r);
    TopDocs hits = s.search(types.newTermQuery("id", "0"), 1, types.newSort("id"));
    assertEquals(1, hits.scoreDocs.length);
    assertEquals("0", r.document(hits.scoreDocs[0].doc).get("id"));
    hits = s.search(types.newTermQuery("id", "1"), 1);
    assertEquals(1, hits.scoreDocs.length);
    assertEquals("1", r.document(hits.scoreDocs[0].doc).get("id"));
    r.close();
    w.close();
    dir.close();
  }

  public void testTermsDictTermsPerBlock() throws Exception {
    Directory dir = newDirectory();
    Analyzer a = new MockAnalyzer(random());
    FieldTypes types = new FieldTypes(a);
    IndexWriterConfig iwc = types.getDefaultIndexWriterConfig();
    IndexWriter w = new IndexWriter(dir, iwc);
    types.setIndexWriter(w);
    types.setIndexOptions("id", IndexOptions.DOCS_ONLY);

    types.setTermsDictBlockSize("id", 10);
    for(int i=0;i<10;i++) {
      Document2 doc = new Document2(types);
      doc.addAtom("id", "0" + i);
      w.addDocument(doc);
    }
    for(int i=0;i<10;i++) {
      Document2 doc = new Document2(types);
      doc.addAtom("id", "1" + i);
      w.addDocument(doc);
    }
    w.close();

    // Use CheckIndex to verify we got 2 terms blocks:
    CheckIndex.Status checked = TestUtil.checkIndex(dir);
    assertEquals(1, checked.segmentInfos.size());
    CheckIndex.Status.SegmentInfoStatus segment = checked.segmentInfos.get(0);
    assertNotNull(segment.termIndexStatus.blockTreeStats);
    Stats btStats = segment.termIndexStatus.blockTreeStats.get("id");
    assertNotNull(btStats);
    assertEquals(2, btStats.termsOnlyBlockCount);
    assertEquals(1, btStats.subBlocksOnlyBlockCount);
    assertEquals(3, btStats.totalBlockCount);
    w.close();
    dir.close();
  }

  public void testExcInvalidDocValuesFormat() throws Exception {
    Directory dir = newDirectory();
    Analyzer a = new MockAnalyzer(random());
    FieldTypes types = new FieldTypes(a);
    IndexWriterConfig iwc = types.getDefaultIndexWriterConfig();
    IndexWriter w = new IndexWriter(dir, iwc);
    types.setIndexWriter(w);
    try {
      types.setDocValuesFormat("id", "foobar");
      fail("did not hit exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
    types.setDocValuesFormat("id", "Memory");
    w.close();
    dir.close();
  }

  public void testExcInvalidPostingsFormat() throws Exception {
    Directory dir = newDirectory();
    Analyzer a = new MockAnalyzer(random());
    FieldTypes types = new FieldTypes(a);
    IndexWriterConfig iwc = types.getDefaultIndexWriterConfig();
    IndexWriter w = new IndexWriter(dir, iwc);
    types.setIndexWriter(w);
    try {
      types.setPostingsFormat("id", "foobar");
      fail("did not hit exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
    types.setPostingsFormat("id", "Memory");
    w.close();
    dir.close();
  }

  /** Make sure that if we index an ATOM field, at search time we get KeywordAnalyzer for it. */
  public void testAtomFieldUsesKeywordAnalyzer() throws Exception {
    Directory dir = newDirectory();
    Analyzer a = new MockAnalyzer(random());
    FieldTypes types = new FieldTypes(a);
    IndexWriterConfig iwc = types.getDefaultIndexWriterConfig();
    IndexWriter w = new IndexWriter(dir, iwc);
    types.setIndexWriter(w);
    Document2 doc = new Document2(types);
    doc.addAtom("id", "foo bar");
    w.addDocument(doc);
    BaseTokenStreamTestCase.assertTokenStreamContents(types.getAnalyzer().tokenStream("id", "foo bar"), new String[] {"foo bar"}, new int[1], new int[] {7});
    w.close();
    dir.close();
  }

  // nocommit test per-field analyzers

  // nocommit test per-field sims

  // nocommit test for pre-analyzed

  // nocommit test multi-valued
}
