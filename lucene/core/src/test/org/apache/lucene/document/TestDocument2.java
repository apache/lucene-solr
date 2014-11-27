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

import java.io.IOException;
import java.io.StringReader;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.TimeZone;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.blocktree.Stats;
import org.apache.lucene.codecs.lucene50.Lucene50Codec;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.NotUniqueException;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.Version;

public class TestDocument2 extends LuceneTestCase {

  public void setUp() throws Exception {
    super.setUp();
    Codec.setDefault(new Lucene50Codec());
  }

  public void testBasic() throws Exception {
    Directory dir = newDirectory();

    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    Document2 doc = w.newDocument();
    doc.addLargeText("body", "some text");
    doc.addShortText("title", "a title");
    doc.addAtom("id", "29jafnn");
    doc.addStored("bytes", new BytesRef(new byte[7]));
    doc.addInt("int", 17);
    w.addDocument(doc);
    w.close();
    dir.close();
  }

  public void testBinaryAtom() throws Exception {
    Directory dir = newDirectory();

    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldTypes fieldTypes = w.getFieldTypes();

    Document2 doc = w.newDocument();
    doc.addAtom("binary", new BytesRef(new byte[5]));
    w.addDocument(doc);
    IndexReader r = DirectoryReader.open(w, true);
    IndexSearcher s = newSearcher(r);
    assertEquals(1, s.search(fieldTypes.newBinaryTermQuery("binary", new byte[5]), 1).totalHits);
    r.close();
    w.close();
    dir.close();
  }

  public void testBinaryAtomSort() throws Exception {
    Directory dir = newDirectory();

    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.enableStored("id");
    // Sort reverse by default:
    fieldTypes.enableSorting("binary", true);

    Document2 doc = w.newDocument();
    byte[] value = new byte[5];
    value[0] = 1;
    doc.addAtom("id", "0");
    doc.addAtom("binary", new BytesRef(value));
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addAtom("id", "1");
    doc.addAtom("binary", new BytesRef(new byte[5]));
    w.addDocument(doc);
    
    IndexReader r = DirectoryReader.open(w, true);
    IndexSearcher s = newSearcher(r);
    TopDocs hits = s.search(new MatchAllDocsQuery(), 2, fieldTypes.newSort("binary"));
    assertEquals(2, hits.scoreDocs.length);
    assertEquals("0", r.document(hits.scoreDocs[0].doc).get("id"));
    assertEquals("1", r.document(hits.scoreDocs[1].doc).get("id"));
    r.close();
    w.close();
    dir.close();
  }

  public void testBinaryStored() throws Exception {
    Directory dir = newDirectory();

    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    Document2 doc = w.newDocument();
    doc.addStored("binary", new BytesRef(new byte[5]));
    w.addDocument(doc);
    IndexReader r = DirectoryReader.open(w, true);
    assertEquals(new BytesRef(new byte[5]), r.document(0).getBinary("binary"));
    r.close();
    w.close();
    dir.close();
  }

  public void testSortedSetDocValues() throws Exception {
    Directory dir = newDirectory();

    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setMultiValued("sortedset");
    fieldTypes.setDocValuesType("sortedset", DocValuesType.SORTED_SET);

    Document2 doc = w.newDocument();
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

    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setDocValuesType("sortednumeric", DocValuesType.SORTED_NUMERIC);
    fieldTypes.setMultiValued("sortednumeric");

    Document2 doc = w.newDocument();
    doc.addInt("sortednumeric", 3);
    doc.addInt("sortednumeric", 1);
    doc.addInt("sortednumeric", 2);
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

    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.enableStored("id");
    fieldTypes.enableSorting("id");
    //System.out.println("id type: " + fieldTypes.getFieldType("id"));

    Document2 doc = w.newDocument();
    doc.addFloat("float", 3f);
    doc.addAtom("id", "one");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addFloat("float", 2f);
    doc.addAtom("id", "two");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addFloat("float", 7f);
    doc.addAtom("id", "three");
    w.addDocument(doc);

    IndexReader r = DirectoryReader.open(w, true);
    IndexSearcher s = newSearcher(r);

    // Make sure range query hits the right number of hits
    assertEquals(2, search(s, fieldTypes.newRangeFilter("float", 0f, true, 3f, true), 1).totalHits);
    assertEquals(3, search(s, fieldTypes.newRangeFilter("float", 0f, true, 10f, true), 1).totalHits);
    assertEquals(1, search(s, fieldTypes.newRangeFilter("float", 1f, true,2.5f, true), 1).totalHits);

    // Make sure doc values shows the correct float values:
    TopDocs hits = s.search(new MatchAllDocsQuery(), 3, fieldTypes.newSort("id"));
    assertEquals(3, hits.totalHits);
    NumericDocValues ndv = MultiDocValues.getNumericValues(r, "float");
    assertNotNull(ndv);
    ScoreDoc hit = hits.scoreDocs[0];
    Document2 storedDoc = r.document(hit.doc);
    assertEquals("one", storedDoc.getString("id"));
    assertEquals(3f, Float.intBitsToFloat((int) ndv.get(hit.doc)), .001f);

    hit = hits.scoreDocs[1];
    storedDoc = r.document(hit.doc);
    assertEquals("three", storedDoc.getString("id"));
    assertEquals(7f, Float.intBitsToFloat((int) ndv.get(hit.doc)), .001f);

    hit = hits.scoreDocs[2];
    storedDoc = r.document(hit.doc);
    assertEquals("two", storedDoc.getString("id"));
    assertEquals(2f, Float.intBitsToFloat((int) ndv.get(hit.doc)), .001f);

    // Make sure we can sort by the field:
    hits = s.search(new MatchAllDocsQuery(), 3, fieldTypes.newSort("float"));
    assertEquals(3, hits.totalHits);
    assertEquals("two", r.document(hits.scoreDocs[0].doc).get("id"));
    assertEquals("one", r.document(hits.scoreDocs[1].doc).get("id"));
    assertEquals("three", r.document(hits.scoreDocs[2].doc).get("id"));

    w.close();
    r.close();
    dir.close();
  }

  private TopDocs search(IndexSearcher s, Filter filter, int count) throws IOException {
    return s.search(new ConstantScoreQuery(filter), count);
  }

  // Cannot change a field from INT to DOUBLE
  public void testInvalidNumberTypeChange() throws Exception {
    Directory dir = newDirectory();

    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    Document2 doc = w.newDocument();
    doc.addInt("int", 3);
    w.addDocument(doc);
    w.close();
    dir.close();
  }

  // nocommit testTermRangeQuery
  // nocommit test range exc
  

  public void testIntRangeQuery() throws Exception {
    Directory dir = newDirectory();

    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldTypes fieldTypes = w.getFieldTypes();

    Document2 doc = w.newDocument();
    doc.addInt("int", 3);
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addInt("int", 2);
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addInt("int", 7);
    w.addDocument(doc);

    IndexReader r = DirectoryReader.open(w, true);
    IndexSearcher s = newSearcher(r);

    assertEquals(2, search(s, fieldTypes.newRangeFilter("int", 0, true, 3, true), 1).totalHits);
    assertEquals(3, search(s, fieldTypes.newRangeFilter("int", 0, true, 10, true), 1).totalHits);
    w.close();
    r.close();
    dir.close();
  }

  public void testExcAnalyzerForAtomField() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldTypes fieldTypes = w.getFieldTypes();
    try {
      fieldTypes.setAnalyzer("atom", new MockAnalyzer(random()));
      // nocommit fixme
      // fail("did not hit expected exception");
    } catch (IllegalStateException ise) {
      // Expected
      assertEquals("wrong exception message: " + ise.getMessage(), "field \"atom\": can only setIndexAnalyzer if the field is indexed", ise.getMessage());
    }
    w.close();
    dir.close();
  }

  // Can't ask for SORTED dv but then add the field as a number
  public void testExcInvalidDocValuesTypeFirst() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setDocValuesType("string", DocValuesType.SORTED);
    Document2 doc = w.newDocument();
    try {
      doc.addInt("string", 17);
      fail("did not hit expected exception");
    } catch (IllegalStateException ise) {
      // Expected
      assertEquals("wrong exception message: " + ise.getMessage(), "field \"string\": type INT must use NUMERIC docValuesType (got: SORTED)", ise.getMessage());
    }
    doc.addAtom("string", "a string");
    w.addDocument(doc);
    w.close();
    dir.close();
  }

  // Can't ask for BINARY dv but then add the field as a number
  public void testExcInvalidBinaryDocValuesTypeFirst() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setDocValuesType("binary", DocValuesType.BINARY);
    Document2 doc = w.newDocument();
    try {
      doc.addInt("binary", 17);
      fail("did not hit expected exception");
    } catch (IllegalStateException ise) {
      // Expected
      assertEquals("wrong exception message: " + ise.getMessage(), "field \"binary\": type INT must use NUMERIC docValuesType (got: BINARY)", ise.getMessage());
    }
    doc.addAtom("binary", new BytesRef(new byte[7]));
    w.addDocument(doc);
    w.close();
    dir.close();
  }

  // Cannot store Reader:
  public void testExcStoreReaderFields() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.enableStored("body");
    Document2 doc = w.newDocument();
    try {
      doc.addLargeText("body", new StringReader("a small string"));
      fail("did not hit expected exception");
    } catch (IllegalStateException ise) {
      // Expected
      assertEquals("wrong exception message: " + ise.getMessage(), "field \"body\": can only store String large text fields", ise.getMessage());
    }
    doc.addLargeText("body", "a string");
    w.addDocument(doc);
    w.close();
    dir.close();
  }

  // Cannot store TokenStream:
  public void testExcStorePreTokenizedFields() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.enableStored("body");
    Document2 doc = w.newDocument();
    try {
      doc.addLargeText("body", new CannedTokenStream());
      fail("did not hit expected exception");
    } catch (IllegalStateException ise) {
      // Expected
      assertEquals("wrong exception message: " + ise.getMessage(), "field \"body\": can only store String large text fields", ise.getMessage());
    }
    doc.addLargeText("body", "a string");
    w.addDocument(doc);
    w.close();
    dir.close();
  }

  public void testSortable() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldTypes fieldTypes = w.getFieldTypes();

    // Normally sorting is not enabled for atom fields:
    fieldTypes.enableSorting("id", true);
    fieldTypes.enableStored("id");

    Document2 doc = w.newDocument();

    doc.addAtom("id", "two");
    w.addDocument(doc);
    doc = w.newDocument();
    doc.addAtom("id", "one");
    w.addDocument(doc);
    IndexReader r = DirectoryReader.open(w, true);
    IndexSearcher s = newSearcher(r);
    TopDocs hits = s.search(new MatchAllDocsQuery(), 2, fieldTypes.newSort("id"));
    assertEquals(2, hits.scoreDocs.length);
    assertEquals("two", r.document(hits.scoreDocs[0].doc).get("id"));
    assertEquals("one", r.document(hits.scoreDocs[1].doc).get("id"));
    r.close();
    w.close();
    dir.close();
  }

  public void testMultiValuedNumeric() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldTypes fieldTypes = w.getFieldTypes();

    fieldTypes.setMultiValued("numbers");
    fieldTypes.enableSorting("numbers");
    fieldTypes.enableStored("id");

    Document2 doc = w.newDocument();
    doc.addInt("numbers", 1);
    doc.addInt("numbers", 2);
    doc.addAtom("id", "one");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addInt("numbers", -10);
    doc.addInt("numbers", -20);
    doc.addAtom("id", "two");
    w.addDocument(doc);

    IndexReader r = DirectoryReader.open(w, true);
    IndexSearcher s = newSearcher(r);
    TopDocs hits = s.search(new MatchAllDocsQuery(), 2, fieldTypes.newSort("numbers"));
    assertEquals(2, hits.scoreDocs.length);
    assertEquals("two", r.document(hits.scoreDocs[0].doc).get("id"));
    assertEquals("one", r.document(hits.scoreDocs[1].doc).get("id"));
    r.close();
    w.close();
    dir.close();
  }

  public void testMultiValuedString() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldTypes fieldTypes = w.getFieldTypes();

    fieldTypes.setMultiValued("strings");
    fieldTypes.enableSorting("strings");
    fieldTypes.enableStored("id");

    Document2 doc = w.newDocument();
    doc.addAtom("strings", "abc");
    doc.addAtom("strings", "baz");
    doc.addAtom("id", "one");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addAtom("strings", "aaa");
    doc.addAtom("strings", "bbb");
    doc.addAtom("id", "two");
    w.addDocument(doc);

    IndexReader r = DirectoryReader.open(w, true);
    IndexSearcher s = newSearcher(r);
    TopDocs hits = s.search(new MatchAllDocsQuery(), 2, fieldTypes.newSort("strings"));
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
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setDocValuesType("binary", DocValuesType.BINARY);
    try {
      fieldTypes.setMultiValued("binary");
      fail("did not hit expected exception");
    } catch (IllegalStateException ise) {
      // Expected
      assertEquals("wrong exception message: " + ise.getMessage(), "field \"binary\": DocValuesType=BINARY cannot be multi-valued", ise.getMessage());
    }
    assertFalse(fieldTypes.getMultiValued("binary"));
    Document2 doc = w.newDocument();
    doc.addStored("binary", new BytesRef(new byte[7]));
    w.addDocument(doc);
    w.close();
    dir.close();
  }

  // You cannot have multi-valued DocValuesType.SORTED
  public void testExcMultiValuedDVSorted() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setDocValuesType("sorted", DocValuesType.SORTED);
    try {
      fieldTypes.setMultiValued("sorted");
      fail("did not hit expected exception");
    } catch (IllegalStateException ise) {
      // Expected
      assertEquals("wrong exception message: " + ise.getMessage(), "field \"sorted\": DocValuesType=SORTED cannot be multi-valued", ise.getMessage());
    }
    assertFalse(fieldTypes.getMultiValued("sorted"));
    Document2 doc = w.newDocument();
    doc.addStored("binary", new BytesRef(new byte[7]));
    w.addDocument(doc);
    w.close();
    dir.close();
  }

  // You cannot have multi-valued DocValuesType.NUMERIC
  public void testExcMultiValuedDVNumeric() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setDocValuesType("numeric", DocValuesType.NUMERIC);
    try {
      fieldTypes.setMultiValued("numeric");
      fail("did not hit expected exception");
    } catch (IllegalStateException ise) {
      // Expected
      assertEquals("wrong exception message: " + ise.getMessage(), "field \"numeric\": DocValuesType=NUMERIC cannot be multi-valued", ise.getMessage());
    }
    assertFalse(fieldTypes.getMultiValued("numeric"));
    Document2 doc = w.newDocument();
    doc.addInt("numeric", 17);
    w.addDocument(doc);
    w.close();
    dir.close();
  }

  public void testPostingsFormat() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    IndexWriter w = new IndexWriter(dir, iwc);

    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setPostingsFormat("id", "Memory");
    fieldTypes.enableStored("id");
    fieldTypes.disableFastRanges("id");

    Document2 doc = w.newDocument();
    doc.addAtom("id", "0");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addAtom("id", "1");
    w.addDocument(doc);

    IndexReader r = DirectoryReader.open(w, true);
    IndexSearcher s = newSearcher(r);
    TopDocs hits = s.search(fieldTypes.newStringTermQuery("id", "0"), 1);
    assertEquals(1, hits.scoreDocs.length);
    assertEquals("0", r.document(hits.scoreDocs[0].doc).get("id"));
    hits = s.search(fieldTypes.newStringTermQuery("id", "1"), 1);
    assertEquals(1, hits.scoreDocs.length);
    assertEquals("1", r.document(hits.scoreDocs[0].doc).get("id"));
    r.close();
    w.close();
    dir.close();
  }

  public void testLongTermQuery() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    IndexWriter w = new IndexWriter(dir, iwc);
    FieldTypes fieldTypes = w.getFieldTypes();

    Document2 doc = w.newDocument();
    doc.addLong("id", 1L);
    w.addDocument(doc);

    IndexReader r = DirectoryReader.open(w, true);
    IndexSearcher s = newSearcher(r);
    TopDocs hits = s.search(fieldTypes.newLongTermQuery("id", 1L), 1);
    assertEquals(1, hits.scoreDocs.length);
    r.close();
    w.close();
    dir.close();
  }

  public void testIntTermQuery() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    IndexWriter w = new IndexWriter(dir, iwc);
    FieldTypes fieldTypes = w.getFieldTypes();

    Document2 doc = w.newDocument();
    doc.addInt("id", 1);
    w.addDocument(doc);

    IndexReader r = DirectoryReader.open(w, true);
    IndexSearcher s = newSearcher(r);
    TopDocs hits = s.search(fieldTypes.newIntTermQuery("id", 1), 1);
    assertEquals(1, hits.scoreDocs.length);
    r.close();
    w.close();
    dir.close();
  }

  public void testBinaryTermQuery() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    IndexWriter w = new IndexWriter(dir, iwc);
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setIndexOptions("id", IndexOptions.DOCS);

    Document2 doc = w.newDocument();
    doc.addStored("id", new BytesRef(new byte[1]));
    w.addDocument(doc);

    IndexReader r = DirectoryReader.open(w, true);
    IndexSearcher s = newSearcher(r);
    TopDocs hits = s.search(fieldTypes.newBinaryTermQuery("id", new byte[1]), 1);
    assertEquals(1, hits.scoreDocs.length);
    r.close();
    w.close();
    dir.close();
  }

  public void testDocValuesFormat() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    IndexWriter w = new IndexWriter(dir, iwc);
    FieldTypes fieldTypes = w.getFieldTypes();

    fieldTypes.setDocValuesFormat("id", "Memory");
    fieldTypes.enableStored("id");
    fieldTypes.enableSorting("id");

    Document2 doc = w.newDocument();
    doc.addAtom("id", "1");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addAtom("id", "0");
    w.addDocument(doc);

    IndexReader r = DirectoryReader.open(w, true);
    IndexSearcher s = newSearcher(r);
    TopDocs hits = s.search(fieldTypes.newStringTermQuery("id", "0"), 1, fieldTypes.newSort("id"));
    assertEquals(1, hits.scoreDocs.length);
    assertEquals("0", r.document(hits.scoreDocs[0].doc).get("id"));
    hits = s.search(fieldTypes.newStringTermQuery("id", "1"), 1);
    assertEquals(1, hits.scoreDocs.length);
    assertEquals("1", r.document(hits.scoreDocs[0].doc).get("id"));
    r.close();
    w.close();
    dir.close();
  }

  public void testTermsDictTermsPerBlock() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    IndexWriter w = new IndexWriter(dir, iwc);
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setIndexOptions("id", IndexOptions.DOCS);

    fieldTypes.setTermsDictBlockSize("id", 10);
    for(int i=0;i<10;i++) {
      Document2 doc = w.newDocument();
      doc.addAtom("id", "0" + i);
      w.addDocument(doc);
    }
    for(int i=0;i<10;i++) {
      Document2 doc = w.newDocument();
      doc.addAtom("id", "1" + i);
      w.addDocument(doc);
    }
    w.forceMerge(1);
    w.close();

    // Use CheckIndex to verify we got 2 terms blocks:
    CheckIndex.Status checked = TestUtil.checkIndex(dir);
    assertEquals(1, checked.segmentInfos.size());
    CheckIndex.Status.SegmentInfoStatus segment = checked.segmentInfos.get(0);
    assertNotNull(segment.termIndexStatus.blockTreeStats);
    Stats btStats = (Stats) segment.termIndexStatus.blockTreeStats.get("id");
    assertNotNull(btStats);
    assertEquals(2, btStats.termsOnlyBlockCount);
    assertEquals(1, btStats.subBlocksOnlyBlockCount);
    assertEquals(3, btStats.totalBlockCount);
    w.close();
    dir.close();
  }

  public void testExcInvalidDocValuesFormat() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    IndexWriter w = new IndexWriter(dir, iwc);
    FieldTypes fieldTypes = w.getFieldTypes();
    try {
      fieldTypes.setDocValuesFormat("id", "foobar");
      fail("did not hit exception");
    } catch (IllegalArgumentException iae) {
      // Expected
      assertTrue("wrong exception message: " + iae.getMessage(), iae.getMessage().startsWith("field \"id\": An SPI class of type org.apache.lucene.codecs.DocValuesFormat with name 'foobar' does not exist"));
    }
    fieldTypes.setDocValuesFormat("id", "Memory");
    w.close();
    dir.close();
  }

  public void testExcInvalidDocValuesType() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    IndexWriter w = new IndexWriter(dir, iwc);
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setDocValuesType("id", DocValuesType.BINARY);
    Document2 doc = w.newDocument();
    try {
      doc.addInt("id", 17);
      fail("did not hit exception");
    } catch (IllegalStateException ise) {
      // Expected
      assertEquals("wrong exception message: " + ise.getMessage(), "field \"id\": type INT must use NUMERIC docValuesType (got: BINARY)", ise.getMessage());
    }
    fieldTypes.setPostingsFormat("id", "Memory");
    w.close();
    dir.close();
  }

  public void testExcInvalidPostingsFormat() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    IndexWriter w = new IndexWriter(dir, iwc);
    FieldTypes fieldTypes = w.getFieldTypes();
    try {
      fieldTypes.setPostingsFormat("id", "foobar");
      fail("did not hit exception");
    } catch (IllegalArgumentException iae) {
      // Expected
      assertTrue("wrong exception message: " + iae.getMessage(), iae.getMessage().startsWith("field \"id\": An SPI class of type org.apache.lucene.codecs.PostingsFormat with name 'foobar' does not exist"));
    }
    fieldTypes.setPostingsFormat("id", "Memory");
    w.close();
    dir.close();
  }

  /** Make sure that if we index an ATOM field, at search time we get KeywordAnalyzer for it. */
  public void testAtomFieldUsesKeywordAnalyzer() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    IndexWriter w = new IndexWriter(dir, iwc);
    FieldTypes fieldTypes = w.getFieldTypes();
    Document2 doc = w.newDocument();
    doc.addAtom("id", "foo bar");
    w.addDocument(doc);
    BaseTokenStreamTestCase.assertTokenStreamContents(fieldTypes.getQueryAnalyzer().tokenStream("id", "foo bar"), new String[] {"foo bar"}, new int[1], new int[] {7});
    w.close();
    dir.close();
  }

  public void testHighlight() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    IndexWriter w = new IndexWriter(dir, iwc);
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.disableHighlighting("no_highlight");

    Document2 doc = w.newDocument();
    doc.addLargeText("highlight", "here is some content");
    doc.addLargeText("no_highlight", "here is some content");
    w.addDocument(doc);

    // nocommit: we can't actually run highlighter ... w/o being outside core ... maybe this test should be elsewhere?
    IndexReader r = DirectoryReader.open(w, true);
    assertTrue(MultiFields.getTerms(r, "highlight").hasOffsets());
    assertFalse(MultiFields.getTerms(r, "no_highlight").hasOffsets());
    r.close();
    w.close();
    dir.close();
  }

  public void testAnalyzerPositionGap() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    IndexWriter w = new IndexWriter(dir, iwc);
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setIndexOptions("nogap", IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    fieldTypes.setMultiValued("nogap");
    fieldTypes.disableHighlighting("nogap");
    fieldTypes.setAnalyzerPositionGap("nogap", 0);

    fieldTypes.setIndexOptions("onegap", IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    fieldTypes.setMultiValued("onegap");
    fieldTypes.disableHighlighting("onegap");
    fieldTypes.setAnalyzerPositionGap("onegap", 1);

    Document2 doc = w.newDocument();
    doc.addLargeText("nogap", "word1");
    doc.addLargeText("nogap", "word2");
    doc.addLargeText("onegap", "word1");
    doc.addLargeText("onegap", "word2");
    w.addDocument(doc);

    IndexReader r = DirectoryReader.open(w, true);
    IndexSearcher s = newSearcher(r);   
    
    PhraseQuery q = new PhraseQuery();
    q.add(new Term("nogap", "word1"));
    q.add(new Term("nogap", "word2"));
    assertEquals(1, s.search(q, 1).totalHits);

    q = new PhraseQuery();
    q.add(new Term("onegap", "word1"));
    q.add(new Term("onegap", "word2"));
    assertEquals(0, s.search(q, 1).totalHits);
    r.close();
    w.close();
    dir.close();
  }

  public void testExcFieldTypesAreSaved() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    Document2 doc = w.newDocument();
    doc.addAtom("id", new BytesRef(new byte[5]));
    w.addDocument(doc);
    w.close();

    w = new IndexWriter(dir, newIndexWriterConfig());
    doc = w.newDocument();
    try {
      doc.addInt("id", 7);
      fail("did not hit exception");
    } catch (IllegalStateException iae) {
      // Expected
      assertEquals("wrong exception message: " + iae.getMessage(), "field \"id\": cannot change from value type ATOM to INT", iae.getMessage());
    }
    doc.addAtom("id", new BytesRef(new byte[7]));
    w.addDocument(doc);
    w.close();
    dir.close();
  }

  public void testDisableIndexing() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    IndexWriter w = new IndexWriter(dir, iwc);
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setIndexOptions("foo", IndexOptions.NONE);

    Document2 doc = w.newDocument();
    doc.addAtom("foo", "bar");
    w.addDocument(doc);

    IndexReader r = DirectoryReader.open(w, true);
    try {
      fieldTypes.newStringTermQuery("foo", "bar");
      fail("did not hit exception");
    } catch (IllegalStateException ise) {
      // Expected
      assertEquals("wrong exception message: " + ise.getMessage(), "field \"foo\": cannot create term query: this field was not indexed", ise.getMessage());
    }
    r.close();
    w.close();
    dir.close();
  }

  public void testExcDisableDocValues() throws Exception {

    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    IndexWriter w = new IndexWriter(dir, iwc);
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setDocValuesType("foo", DocValuesType.NONE);

    Document2 doc = w.newDocument();
    doc.addInt("foo", 17);
    w.addDocument(doc);

    IndexReader r = DirectoryReader.open(w, true);
    try {
      fieldTypes.newSort("foo");
      fail("did not hit exception");
    } catch (IllegalStateException ise) {
      // Expected
      assertEquals("wrong exception message: " + ise.getMessage(), "field \"foo\": this field was not indexed for sorting", ise.getMessage());
    }
    r.close();
    w.close();
    dir.close();
  }

  public void testExcRangeQuery() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.disableFastRanges("int");
    Document2 doc = w.newDocument();
    doc.addInt("int", 17);
    w.addDocument(doc);
    try {
      fieldTypes.newRangeFilter("int", 0, true, 7, true);
      fail("did not hit exception");
    } catch (IllegalStateException ise) {
      assertEquals("field \"int\": this field was not indexed for fast ranges", ise.getMessage());
    }
    w.close();
    dir.close();
  }

  public void testIndexCreatedVersion() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    assertEquals(Version.LATEST, w.getFieldTypes().getIndexCreatedVersion());
    w.close();
    dir.close();
  }

  public void testBooleanType() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    IndexWriter w = new IndexWriter(dir, iwc);

    FieldTypes fieldTypes = w.getFieldTypes();

    Document2 doc = w.newDocument();
    doc.addBoolean("onsale", true);
    w.addDocument(doc);

    //w.close();
    //DirectoryReader r = DirectoryReader.open(dir);
    DirectoryReader r = DirectoryReader.open(w, true);
    IndexSearcher s = newSearcher(r);
    TopDocs hits = s.search(fieldTypes.newBooleanTermQuery("onsale", true), 1);
    assertEquals(1, hits.totalHits);
    doc = s.doc(hits.scoreDocs[0].doc);
    assertEquals(true, doc.getBoolean("onsale"));
    assertEquals(0, s.search(fieldTypes.newBooleanTermQuery("onsale", false), 1).totalHits);
    r.close();
    w.close();
    dir.close();
  }

  public void testOnlyChangeFieldTypes() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    w.commit();
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.enableSorting("sorted");
    w.close();

    w = new IndexWriter(dir, newIndexWriterConfig());
    fieldTypes = w.getFieldTypes();
    assertTrue(fieldTypes.getSorted("sorted"));
    w.close();
    dir.close();
  }

  // nocommit need to test other numeric types:
  public void testIntSortMissingFirst() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    w.commit();
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.enableSorting("int");
    fieldTypes.setSortMissingFirst("int");

    Document2 doc = w.newDocument();
    doc.addInt("int", 7);
    doc.addAtom("id", "0");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addInt("int", -7);
    doc.addAtom("id", "1");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addAtom("id", "2");
    w.addDocument(doc);
    
    DirectoryReader r = DirectoryReader.open(w, true);
    fieldTypes = r.getFieldTypes();
    IndexSearcher s = newSearcher(r);
    TopDocs hits = s.search(new MatchAllDocsQuery(), 3, fieldTypes.newSort("int"));
    assertEquals(3, hits.totalHits);
    assertEquals("2", s.doc(hits.scoreDocs[0].doc).getString("id"));
    assertEquals("1", s.doc(hits.scoreDocs[1].doc).getString("id"));
    assertEquals("0", s.doc(hits.scoreDocs[2].doc).getString("id"));
    r.close();
    w.close();
    dir.close();
  }

  // nocommit need to test other numeric types:
  public void testIntSortMissingFirstReversed() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    w.commit();
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.enableSorting("int", true);
    fieldTypes.setSortMissingFirst("int");

    Document2 doc = w.newDocument();
    doc.addInt("int", 7);
    doc.addAtom("id", "0");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addInt("int", -7);
    doc.addAtom("id", "1");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addAtom("id", "2");
    w.addDocument(doc);
    
    DirectoryReader r = DirectoryReader.open(w, true);
    fieldTypes = r.getFieldTypes();
    IndexSearcher s = newSearcher(r);
    TopDocs hits = s.search(new MatchAllDocsQuery(), 3, fieldTypes.newSort("int"));
    assertEquals(3, hits.totalHits);
    assertEquals("2", s.doc(hits.scoreDocs[0].doc).getString("id"));
    assertEquals("0", s.doc(hits.scoreDocs[1].doc).getString("id"));
    assertEquals("1", s.doc(hits.scoreDocs[2].doc).getString("id"));
    r.close();
    w.close();
    dir.close();
  }

  // nocommit need to test other numeric types:
  public void testIntSortMissingLast() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    w.commit();
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.enableSorting("int");
    fieldTypes.setSortMissingLast("int");

    Document2 doc = w.newDocument();
    doc.addInt("int", 7);
    doc.addAtom("id", "0");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addInt("int", -7);
    doc.addAtom("id", "1");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addAtom("id", "2");
    w.addDocument(doc);
    
    DirectoryReader r = DirectoryReader.open(w, true);
    fieldTypes = r.getFieldTypes();
    IndexSearcher s = newSearcher(r);
    TopDocs hits = s.search(new MatchAllDocsQuery(), 3, fieldTypes.newSort("int"));
    assertEquals(3, hits.totalHits);
    assertEquals("1", s.doc(hits.scoreDocs[0].doc).getString("id"));
    assertEquals("0", s.doc(hits.scoreDocs[1].doc).getString("id"));
    assertEquals("2", s.doc(hits.scoreDocs[2].doc).getString("id"));
    r.close();
    w.close();
    dir.close();
  }

  // nocommit need to test other numeric types:
  public void testIntSortMissingLastReversed() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    w.commit();
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.enableSorting("int", true);
    fieldTypes.setSortMissingLast("int");

    Document2 doc = w.newDocument();
    doc.addInt("int", 7);
    doc.addAtom("id", "0");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addInt("int", -7);
    doc.addAtom("id", "1");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addAtom("id", "2");
    w.addDocument(doc);
    
    DirectoryReader r = DirectoryReader.open(w, true);
    fieldTypes = r.getFieldTypes();
    IndexSearcher s = newSearcher(r);
    TopDocs hits = s.search(new MatchAllDocsQuery(), 3, fieldTypes.newSort("int"));
    assertEquals(3, hits.totalHits);
    assertEquals("0", s.doc(hits.scoreDocs[0].doc).getString("id"));
    assertEquals("1", s.doc(hits.scoreDocs[1].doc).getString("id"));
    assertEquals("2", s.doc(hits.scoreDocs[2].doc).getString("id"));
    r.close();
    w.close();
    dir.close();
  }

  // nocommit need to test other numeric types:
  public void testIntSortMissingDefault() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    w.commit();

    Document2 doc = w.newDocument();
    doc.addInt("int", 7);
    doc.addAtom("id", "0");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addInt("int", -7);
    doc.addAtom("id", "1");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addAtom("id", "2");
    w.addDocument(doc);
    
    DirectoryReader r = DirectoryReader.open(w, true);
    FieldTypes fieldTypes = r.getFieldTypes();
    IndexSearcher s = newSearcher(r);
    TopDocs hits = s.search(new MatchAllDocsQuery(), 3, fieldTypes.newSort("int"));
    assertEquals(3, hits.totalHits);
    assertEquals("1", s.doc(hits.scoreDocs[0].doc).getString("id"));
    assertEquals("0", s.doc(hits.scoreDocs[1].doc).getString("id"));
    assertEquals("2", s.doc(hits.scoreDocs[2].doc).getString("id"));
    r.close();
    w.close();
    dir.close();
  }

  // nocommit need to test other numeric types:
  public void testIntSortMissingDefaultReversed() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    w.commit();

    Document2 doc = w.newDocument();
    doc.addInt("int", 7);
    doc.addAtom("id", "0");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addInt("int", -7);
    doc.addAtom("id", "1");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addAtom("id", "2");
    w.addDocument(doc);
    
    DirectoryReader r = DirectoryReader.open(w, true);
    FieldTypes fieldTypes = r.getFieldTypes();
    IndexSearcher s = newSearcher(r);
    TopDocs hits = s.search(new MatchAllDocsQuery(), 3, fieldTypes.newSort("int", true));
    assertEquals(3, hits.totalHits);
    assertEquals("0", s.doc(hits.scoreDocs[0].doc).getString("id"));
    assertEquals("1", s.doc(hits.scoreDocs[1].doc).getString("id"));
    assertEquals("2", s.doc(hits.scoreDocs[2].doc).getString("id"));
    r.close();
    w.close();
    dir.close();
  }



  // nocommit need to test other string sort types:
  public void testAtomSortMissingFirst() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    w.commit();
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.enableSorting("atom");
    fieldTypes.setSortMissingFirst("atom");

    Document2 doc = w.newDocument();
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

  // nocommit need to test other string sort types:
  public void testAtomSortMissingFirstReversed() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    w.commit();
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.enableSorting("atom", true);
    fieldTypes.setSortMissingFirst("atom");

    Document2 doc = w.newDocument();
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

  // nocommit need to test other string sort types:
  public void testAtomSortMissingLast() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    w.commit();
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.enableSorting("atom");
    fieldTypes.setSortMissingLast("atom");

    Document2 doc = w.newDocument();
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

  // nocommit need to test other string sort types:
  public void testAtomSortMissingLastReversed() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    w.commit();
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.enableSorting("atom", true);
    fieldTypes.setSortMissingLast("atom");

    Document2 doc = w.newDocument();
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

  // nocommit need to test other string sort types:
  public void testAtomSortMissingDefault() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    w.commit();

    Document2 doc = w.newDocument();
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

  // nocommit need to test other string sort types:
  public void testAtomSortMissingDefaultReversed() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    w.commit();

    Document2 doc = w.newDocument();
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

  public void testMinMaxTokenLength() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    w.getFieldTypes().setMinMaxTokenLength("field", 2, 7);
    w.commit();

    Document2 doc = w.newDocument();
    doc.addLargeText("field", "hello a toobigterm");
    w.addDocument(doc);

    DirectoryReader r = DirectoryReader.open(w, true);
    FieldTypes fieldTypes = r.getFieldTypes();
    assertEquals(2, fieldTypes.getMinTokenLength("field").intValue());
    assertEquals(7, fieldTypes.getMaxTokenLength("field").intValue());

    IndexSearcher s = newSearcher(r);
    assertEquals(1, s.search(fieldTypes.newStringTermQuery("field", "hello"), 1).totalHits);
    assertEquals(0, s.search(fieldTypes.newStringTermQuery("field", "a"), 1).totalHits);
    assertEquals(0, s.search(fieldTypes.newStringTermQuery("field", "toobigterm"),1 ).totalHits);
    r.close();
    w.close();
    dir.close();
  }

  public void testMaxTokenCount() throws Exception {
    Directory dir = newDirectory();
    MockAnalyzer a = new MockAnalyzer(random());
    // MockAnalyzer is angry that we don't consume all tokens:
    a.setEnableChecks(false);
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(a));
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setMaxTokenCount("field", 3);
    w.commit();

    Document2 doc = w.newDocument();
    doc.addLargeText("field", "hello a toobigterm goodbye");
    w.addDocument(doc);

    DirectoryReader r = DirectoryReader.open(w, true);
    fieldTypes = r.getFieldTypes();
    assertEquals(3, fieldTypes.getMaxTokenCount("field").intValue());

    IndexSearcher s = newSearcher(r);
    assertEquals(1, s.search(fieldTypes.newStringTermQuery("field", "hello"), 1).totalHits);
    assertEquals(1, s.search(fieldTypes.newStringTermQuery("field", "a"), 1).totalHits);
    assertEquals(1, s.search(fieldTypes.newStringTermQuery("field", "toobigterm"), 1).totalHits);
    assertEquals(0, s.search(fieldTypes.newStringTermQuery("field", "goodbye"), 1).totalHits);
    r.close();
    w.close();
    dir.close();
  }

  public void testMaxTokenCountConsumeAll() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setMaxTokenCount("field", 3, true);

    Document2 doc = w.newDocument();
    doc.addLargeText("field", "hello a toobigterm goodbye");
    w.addDocument(doc);

    DirectoryReader r = DirectoryReader.open(w, true);
    fieldTypes = r.getFieldTypes();
    assertEquals(3, fieldTypes.getMaxTokenCount("field").intValue());

    IndexSearcher s = newSearcher(r);
    assertEquals(1, s.search(fieldTypes.newStringTermQuery("field", "hello"), 1).totalHits);
    assertEquals(1, s.search(fieldTypes.newStringTermQuery("field", "a"), 1).totalHits);
    assertEquals(1, s.search(fieldTypes.newStringTermQuery("field", "toobigterm"), 1).totalHits);
    assertEquals(0, s.search(fieldTypes.newStringTermQuery("field", "goodbye"), 1).totalHits);
    r.close();
    w.close();
    dir.close();
  }

  public void testExcSuddenlyEnableDocValues() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setDocValuesType("field", DocValuesType.NONE);

    Document2 doc = w.newDocument();
    doc.addInt("field", 17);
    w.addDocument(doc);

    try {
      fieldTypes.setDocValuesType("field", DocValuesType.NUMERIC);
      fail("did not hit exception");
    } catch (IllegalStateException ise) {
      assertEquals("field \"field\": cannot change docValuesType from NONE to NUMERIC", ise.getMessage());
    }
    w.close();
    dir.close();
  }

  public void testDateSort() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    SimpleDateFormat parser = new SimpleDateFormat("MM/dd/yyyy", Locale.ROOT);
    parser.setTimeZone(TimeZone.getTimeZone("GMT"));

    Document2 doc = w.newDocument();
    Date date0 = parser.parse("10/22/2014");
    doc.addDate("date", date0);
    doc.addAtom("id", "0");
    w.addDocument(doc);

    doc = w.newDocument();
    Date date1 = parser.parse("10/21/2015");
    doc.addDate("date", date1);
    doc.addAtom("id", "1");
    w.addDocument(doc);

    w.getFieldTypes().enableSorting("date", true);

    DirectoryReader r = DirectoryReader.open(w, true);
    FieldTypes fieldTypes = r.getFieldTypes();

    IndexSearcher s = newSearcher(r);
    TopDocs hits = s.search(new MatchAllDocsQuery(), 10, fieldTypes.newSort("date"));
    assertEquals(2, hits.totalHits);
    Document2 hit = s.doc(hits.scoreDocs[0].doc);
    assertEquals("1", hit.getString("id"));
    assertEquals(date1, hit.getDate("date"));
    hit = s.doc(hits.scoreDocs[1].doc);
    assertEquals("0", hit.getString("id"));
    assertEquals(date0, hit.getDate("date"));
    r.close();
    w.close();
    dir.close();
  }

  public void testDateRangeFilter() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    SimpleDateFormat parser = new SimpleDateFormat("MM/dd/yyyy", Locale.ROOT);
    parser.setTimeZone(TimeZone.getTimeZone("GMT"));

    Document2 doc = w.newDocument();
    Date date0 = parser.parse("10/22/2014");
    doc.addDate("date", date0);
    doc.addAtom("id", "0");
    w.addDocument(doc);

    doc = w.newDocument();
    Date date1 = parser.parse("10/21/2015");
    doc.addDate("date", date1);
    doc.addAtom("id", "1");
    w.addDocument(doc);

    DirectoryReader r = DirectoryReader.open(w, true);
    FieldTypes fieldTypes = r.getFieldTypes();

    IndexSearcher s = newSearcher(r);
    assertEquals(2, search(s, fieldTypes.newRangeFilter("date", date0, true, date1, true), 1).totalHits);
    assertEquals(1, search(s, fieldTypes.newRangeFilter("date", date0, true, date1, false), 1).totalHits);
    assertEquals(0, search(s, fieldTypes.newRangeFilter("date", date0, false, date1, false), 1).totalHits);
    assertEquals(1, search(s, fieldTypes.newRangeFilter("date", parser.parse("10/21/2014"), false, parser.parse("10/23/2014"), false), 1).totalHits);
    r.close();
    w.close();
    dir.close();
  }

  public void testInetAddressSort() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    Document2 doc = w.newDocument();
    InetAddress inet0 = InetAddress.getByName("10.17.4.10");
    doc.addInetAddress("inet", inet0);
    doc.addAtom("id", "0");
    w.addDocument(doc);

    doc = w.newDocument();
    InetAddress inet1 = InetAddress.getByName("10.17.4.22");
    doc.addInetAddress("inet", inet1);
    doc.addAtom("id", "1");
    w.addDocument(doc);

    DirectoryReader r = DirectoryReader.open(w, true);
    FieldTypes fieldTypes = r.getFieldTypes();

    IndexSearcher s = newSearcher(r);
    TopDocs hits = s.search(new MatchAllDocsQuery(), 10, fieldTypes.newSort("inet"));
    assertEquals(2, hits.totalHits);
    Document2 hit = s.doc(hits.scoreDocs[0].doc);
    assertEquals("0", hit.getString("id"));
    assertEquals(inet0, hit.getInetAddress("inet"));
    hit = s.doc(hits.scoreDocs[1].doc);
    assertEquals("1", hit.getString("id"));
    assertEquals(inet1, hit.getInetAddress("inet"));
    r.close();
    w.close();
    dir.close();
  }

  public void testInetAddressRangeFilter() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    Document2 doc = w.newDocument();
    InetAddress inet0 = InetAddress.getByName("10.17.4.10");
    doc.addInetAddress("inet", inet0);
    doc.addAtom("id", "0");
    w.addDocument(doc);

    doc = w.newDocument();
    InetAddress inet1 = InetAddress.getByName("10.17.4.22");
    doc.addInetAddress("inet", inet1);
    doc.addAtom("id", "1");
    w.addDocument(doc);

    DirectoryReader r = DirectoryReader.open(w, true);
    FieldTypes fieldTypes = r.getFieldTypes();

    IndexSearcher s = newSearcher(r);
    assertEquals(2, search(s, fieldTypes.newRangeFilter("inet", inet0, true, inet1, true), 1).totalHits);
    assertEquals(1, search(s, fieldTypes.newRangeFilter("inet", inet0, true, inet1, false), 1).totalHits);
    assertEquals(0, search(s, fieldTypes.newRangeFilter("inet", inet0, false, inet1, false), 1).totalHits);
    assertEquals(1, search(s, fieldTypes.newRangeFilter("inet", InetAddress.getByName("10.17.0.0"), true, InetAddress.getByName("10.17.4.20"), false), 1).totalHits);
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
    Document2 doc = w.newDocument();
    doc.addAtom("field", "a");
    doc.addAtom("field", "ab");
    doc.addAtom("field", "goodbyeyou");
    w.addDocument(doc);

    DirectoryReader r = DirectoryReader.open(w, true);
    fieldTypes = r.getFieldTypes();

    IndexSearcher s = newSearcher(r);
    assertEquals(0, hitCount(s, fieldTypes.newStringTermQuery("field", "a")));
    assertEquals(1, hitCount(s, fieldTypes.newStringTermQuery("field", "ab")));
    assertEquals(0, hitCount(s, fieldTypes.newStringTermQuery("field", "goodbyeyou")));
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
    Document2 doc = w.newDocument();
    doc.addAtom("field", new BytesRef(new byte[1]));
    doc.addAtom("field", new BytesRef(new byte[2]));
    doc.addAtom("field", new BytesRef(new byte[10]));
    w.addDocument(doc);

    DirectoryReader r = DirectoryReader.open(w, true);
    fieldTypes = r.getFieldTypes();

    IndexSearcher s = newSearcher(r);
    assertEquals(0, hitCount(s, fieldTypes.newBinaryTermQuery("field", new byte[1])));
    assertEquals(1, hitCount(s, fieldTypes.newBinaryTermQuery("field", new byte[2])));
    assertEquals(0, hitCount(s, fieldTypes.newBinaryTermQuery("field", new byte[10])));
    r.close();
    w.close();
    dir.close();
  }

  public void testExcCannotStoreTokenStream() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.disableNorms("field");
    fieldTypes.enableStored("field");

    Document2 doc = w.newDocument();
    try {
      doc.addLargeText("field", new TokenStream() {
          @Override
          public boolean incrementToken() {
            return false;
          }
        });
      fail("did not hit exception");
    } catch (IllegalStateException ise) {
      assertEquals("field \"field\": can only store String large text fields", ise.getMessage());
    }
    w.close();
    dir.close();
  }

  public void testFieldExistsFilter() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    Document2 doc = w.newDocument();
    doc.addAtom("field1", "field");
    doc.addAtom("field2", "field");
    doc.addAtom("id", "0");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addAtom("field1", "field");
    doc.addAtom("id", "1");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addAtom("field2", "field");
    doc.addAtom("id", "2");
    w.addDocument(doc);

    DirectoryReader r = DirectoryReader.open(w, true);
    FieldTypes fieldTypes = r.getFieldTypes();

    IndexSearcher s = newSearcher(r);
    TopDocs hits = s.search(new MatchAllDocsQuery(),
                            fieldTypes.newFieldExistsFilter("field1"), 2);
    assertEquals(2, hits.totalHits);
    Set<String> ids = getIDs(r, hits);
    assertTrue(ids.contains("0"));
    assertTrue(ids.contains("1"));

    hits = s.search(new MatchAllDocsQuery(),
                    fieldTypes.newFieldExistsFilter("field2"), 2);
    assertEquals(2, hits.totalHits);
    ids = getIDs(r, hits);
    assertTrue(ids.contains("0"));
    assertTrue(ids.contains("2"));
    assertEquals(0, s.search(new MatchAllDocsQuery(),
                             fieldTypes.newFieldExistsFilter("field3"), 1).totalHits);;
    r.close();
    w.close();
    dir.close();
  }

  private Set<String> getIDs(IndexReader r, TopDocs hits) throws IOException {
    Set<String> ids = new HashSet<>();
    for(ScoreDoc scoreDoc : hits.scoreDocs) {
      ids.add(r.document(scoreDoc.doc).getString("id"));
    }
    return ids;
  }

  private static int hitCount(IndexSearcher s, Query q) throws IOException {
    // TODO: use TotalHitCountCollector sometimes
    return s.search(q, 1).totalHits;
  }

  public void testMultiValuedUnique() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setMultiValued("field");
    Document2 doc = w.newDocument();
    doc.addUniqueAtom("field", "foo");
    doc.addUniqueAtom("field", "bar");
    w.addDocument(doc);

    DirectoryReader r = DirectoryReader.open(w, true);

    IndexSearcher s = newSearcher(r);
    assertEquals(1, hitCount(s, fieldTypes.newStringTermQuery("field", "foo")));
    assertEquals(1, hitCount(s, fieldTypes.newStringTermQuery("field", "bar")));
    r.close();
    w.close();
    dir.close();
  }

  public void testExcStoredThenIndexed() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = newIndexWriter(dir);
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setMultiValued("field");
    Document2 doc = w.newDocument();
    doc.addStored("field", "bar");
    // nocommit why no exc here?
    doc.addLargeText("field", "bar");
    w.addDocument(doc);
    w.close();
    dir.close();
  }

  public void testDoubleTermQuery() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = newIndexWriter(dir);
    Document2 doc = w.newDocument();
    doc.addDouble("double", 180.0);
    w.addDocument(doc);
    DirectoryReader r = DirectoryReader.open(w, true);
    IndexSearcher s = newSearcher(r);
    assertEquals(1, s.search(new TermQuery(new Term("double",
                                                    Document2.longToBytes(Document2.sortableDoubleBits(Double.doubleToLongBits(180.0))))),
                             1).totalHits);
    r.close();
    w.close();
    dir.close();
  }

  // nocommit test per-field analyzers

  // nocommit test per-field sims

  // nocommit test for pre-analyzed

  // nocommit test multi-valued

  // nocommit multi-valued sort, w/ missings
}
