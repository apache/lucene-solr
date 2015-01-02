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
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.Token;
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
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.Version;

public class TestDocument extends LuceneTestCase {

  public void setUp() throws Exception {
    super.setUp();
    Codec.setDefault(new Lucene50Codec());
  }

  public void testBasic() throws Exception {
    Directory dir = newDirectory();

    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    Document doc = w.newDocument();
    doc.addLargeText("body", "some text");
    doc.addShortText("title", "a title");
    doc.addAtom("id", "29jafnn");
    doc.addStoredBinary("bytes", new BytesRef(new byte[7]));
    doc.addInt("int", 17);
    w.addDocument(doc);
    w.close();
    dir.close();
  }

  public void testBinaryAtom() throws Exception {
    Directory dir = newDirectory();

    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldTypes fieldTypes = w.getFieldTypes();

    Document doc = w.newDocument();
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

    Document doc = w.newDocument();
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

    Document doc = w.newDocument();
    doc.addStoredBinary("binary", new BytesRef(new byte[5]));
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

    Document doc = w.newDocument();
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

  private TopDocs search(IndexSearcher s, Filter filter, int count) throws IOException {
    return s.search(new ConstantScoreQuery(filter), count);
  }

  public void testExcAnalyzerForAtomField() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldTypes fieldTypes = w.getFieldTypes();
    Document doc = w.newDocument();
    doc.addAtom("atom", "foo");
    shouldFail(() -> fieldTypes.setAnalyzer("atom", new MockAnalyzer(random())),
               "field \"atom\": type ATOM cannot have an indexAnalyzer");
    w.close();
    dir.close();
  }

  // Can't ask for SORTED dv but then add the field as a number
  public void testExcInvalidDocValuesTypeFirst() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setDocValuesType("string", DocValuesType.SORTED);
    Document doc = w.newDocument();
    shouldFail(() -> doc.addInt("string", 17),
               "field \"string\": type INT must use NUMERIC or SORTED_NUMERIC docValuesType; got: SORTED");
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
    Document doc = w.newDocument();
    shouldFail(() -> doc.addInt("binary", 17),
               "field \"binary\": type INT must use NUMERIC or SORTED_NUMERIC docValuesType; got: BINARY");
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
    Document doc = w.newDocument();
    shouldFail(() -> doc.addLargeText("body", new StringReader("a small string")),
               "field \"body\": can only store String large text fields");
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
    Document doc = w.newDocument();
    shouldFail(() -> doc.addLargeText("body", new CannedTokenStream()),
               "field \"body\": can only store String large text fields");
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

    Document doc = w.newDocument();

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

  public void testMultiValuedString() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldTypes fieldTypes = w.getFieldTypes();

    fieldTypes.setMultiValued("strings");
    fieldTypes.enableSorting("strings");
    fieldTypes.enableStored("id");

    Document doc = w.newDocument();
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
    shouldFail(() -> fieldTypes.setMultiValued("binary"),
               "field \"binary\": DocValuesType=BINARY cannot be multi-valued");
    assertFalse(fieldTypes.getMultiValued("binary"));
    Document doc = w.newDocument();
    doc.addStoredBinary("binary", new BytesRef(new byte[7]));
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
    shouldFail(() -> fieldTypes.setMultiValued("sorted"),
               "field \"sorted\": DocValuesType=SORTED cannot be multi-valued");
    assertFalse(fieldTypes.getMultiValued("sorted"));
    Document doc = w.newDocument();
    doc.addStoredBinary("binary", new BytesRef(new byte[7]));
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
    shouldFail(() -> fieldTypes.setMultiValued("numeric"),
               "field \"numeric\": DocValuesType=NUMERIC cannot be multi-valued");
    assertFalse(fieldTypes.getMultiValued("numeric"));
    Document doc = w.newDocument();
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

    Document doc = w.newDocument();
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

  public void testBinaryTermQuery() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    IndexWriter w = new IndexWriter(dir, iwc);

    Document doc = w.newDocument();
    doc.addAtom("id", new BytesRef(new byte[1]));
    w.addDocument(doc);

    IndexReader r = DirectoryReader.open(w, true);
    IndexSearcher s = newSearcher(r);
    FieldTypes fieldTypes = s.getFieldTypes();
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

    Document doc = w.newDocument();
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
      Document doc = w.newDocument();
      doc.addAtom("id", "0" + i);
      w.addDocument(doc);
    }
    for(int i=0;i<10;i++) {
      Document doc = w.newDocument();
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
    shouldFail(() -> fieldTypes.setDocValuesFormat("id", "foobar"),
               "field \"id\": An SPI class of type org.apache.lucene.codecs.DocValuesFormat with name 'foobar' does not exist");
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
    Document doc = w.newDocument();
    shouldFail(() -> doc.addInt("id", 17),
               "field \"id\": type INT must use NUMERIC or SORTED_NUMERIC docValuesType; got: BINARY");
    fieldTypes.setPostingsFormat("id", "Memory");
    w.close();
    dir.close();
  }

  public void testExcInvalidPostingsFormat() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    IndexWriter w = new IndexWriter(dir, iwc);
    FieldTypes fieldTypes = w.getFieldTypes();
    shouldFail(() -> fieldTypes.setPostingsFormat("id", "foobar"),
               "field \"id\": An SPI class of type org.apache.lucene.codecs.PostingsFormat with name 'foobar' does not exist");
    fieldTypes.setPostingsFormat("id", "Memory");
    w.close();
    dir.close();
  }

  public void testHighlightOffsets() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    IndexWriter w = new IndexWriter(dir, iwc);
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.disableHighlighting("no_highlight");

    Document doc = w.newDocument();
    doc.addLargeText("highlight", "here is some content");
    doc.addLargeText("no_highlight", "here is some content");
    w.addDocument(doc);

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

    Document doc = w.newDocument();
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
    Document doc = w.newDocument();
    doc.addAtom("id", new BytesRef(new byte[5]));
    w.addDocument(doc);
    w.close();

    w = new IndexWriter(dir, newIndexWriterConfig());
    Document doc2 = w.newDocument();
    shouldFail(() -> doc2.addInt("id", 7),
               "field \"id\": cannot change from value type ATOM to INT");
    doc2.addAtom("id", new BytesRef(new byte[7]));
    w.addDocument(doc2);
    w.close();
    dir.close();
  }

  public void testDisableIndexing() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    IndexWriter w = new IndexWriter(dir, iwc);
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setIndexOptions("foo", IndexOptions.NONE);

    Document doc = w.newDocument();
    doc.addAtom("foo", "bar");
    w.addDocument(doc);

    IndexReader r = DirectoryReader.open(w, true);
    shouldFail(() -> fieldTypes.newStringTermQuery("foo", "bar"),
               "field \"foo\": cannot create term query: this field was not indexed");
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

    Document doc = w.newDocument();
    doc.addInt("foo", 17);
    w.addDocument(doc);

    IndexReader r = DirectoryReader.open(w, true);
    shouldFail(() -> fieldTypes.newSort("foo"),
               "field \"foo\": this field was not indexed for sorting");
    r.close();
    w.close();
    dir.close();
  }

  public void testExcRangeQuery() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.disableFastRanges("int");
    Document doc = w.newDocument();
    doc.addInt("int", 17);
    w.addDocument(doc);
    shouldFail(() -> fieldTypes.newIntRangeFilter("int", 0, true, 7, true),
               "field \"int\": cannot create range filter: this field was not indexed for fast ranges");
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

    Document doc = w.newDocument();
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

  public void testMinMaxTokenLength() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    w.getFieldTypes().setMinMaxTokenLength("field", 2, 7);
    w.commit();

    Document doc = w.newDocument();
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

    Document doc = w.newDocument();
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

    Document doc = w.newDocument();
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

    Document doc = w.newDocument();
    doc.addInt("field", 17);
    w.addDocument(doc);

    shouldFail(() -> fieldTypes.setDocValuesType("field", DocValuesType.NUMERIC),
               "field \"field\": cannot change docValuesType from NONE to NUMERIC");
    w.close();
    dir.close();
  }

  public void testExcCannotStoreTokenStream() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.disableNorms("field");
    fieldTypes.enableStored("field");

    Document doc = w.newDocument();
    shouldFail(() ->
      doc.addLargeText("field", new TokenStream() {
          @Override
          public boolean incrementToken() {
            return false;
          }
        }),
               "field \"field\": can only store String large text fields");
    w.close();
    dir.close();
  }

  public void testExcStoredThenIndexed() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = newIndexWriter(dir);
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setMultiValued("field");
    Document doc = w.newDocument();
    doc.addStoredString("field", "bar");
    shouldFail(() -> doc.addLargeText("field", "bar"),
               "field \"field\": this field is already disabled for indexing");
    w.addDocument(doc);
    w.close();
    dir.close();
  }

  public void testExcIndexedThenStored() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = newIndexWriter(dir);
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setMultiValued("field");
    Document doc = w.newDocument();
    doc.addLargeText("field", "bar");
    shouldFail(() -> doc.addStoredString("field", "bar"),
               "field \"field\": this field is already indexed with indexOptions=DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS");
    w.addDocument(doc);
    w.close();
    dir.close();
  }

  public void testStoredAfterLargeText() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = newIndexWriter(dir);

    Document doc = w.newDocument();
    doc.addLargeText("field", "ABC");
    shouldFail(() -> doc.addStoredString("field", "foo"),
               "field \"field\": this field is already indexed with indexOptions=DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS");
    w.close();
    dir.close();
  }

  public void testSortKey() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = newIndexWriter(dir);

    Document doc = w.newDocument();
    doc.addAtom("sev", "cosmetic");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addAtom("sev", "major");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addAtom("sev", "critical");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addAtom("sev", "minor");
    w.addDocument(doc);

    // missing
    doc = w.newDocument();
    w.addDocument(doc);

    DirectoryReader r = DirectoryReader.open(w, true);
    FieldTypes fieldTypes = r.getFieldTypes();

    IndexSearcher s = newSearcher(r);
    TopDocs hits = s.search(new MatchAllDocsQuery(), 5, fieldTypes.newSort("sev"));
    assertEquals(5, hits.totalHits);
    assertEquals("cosmetic", s.doc(hits.scoreDocs[0].doc).getString("sev"));
    assertEquals("critical", s.doc(hits.scoreDocs[1].doc).getString("sev"));
    assertEquals("major", s.doc(hits.scoreDocs[2].doc).getString("sev"));
    assertEquals("minor", s.doc(hits.scoreDocs[3].doc).getString("sev"));
    assertNull(s.doc(hits.scoreDocs[4].doc).getInt("sev"));

    final Map<BytesRef,Integer> sortMap = new HashMap<>();
    sortMap.put(new BytesRef("critical"), 0);
    sortMap.put(new BytesRef("major"), 1);
    sortMap.put(new BytesRef("minor"), 2);
    sortMap.put(new BytesRef("cosmetic"), 3);
    fieldTypes.setSortKey("sev", v -> sortMap.get(v));

    hits = s.search(new MatchAllDocsQuery(), 5, fieldTypes.newSort("sev"));
    assertEquals(5, hits.totalHits);
    assertEquals("critical", s.doc(hits.scoreDocs[0].doc).getString("sev"));
    assertEquals("major", s.doc(hits.scoreDocs[1].doc).getString("sev"));
    assertEquals("minor", s.doc(hits.scoreDocs[2].doc).getString("sev"));
    assertEquals("cosmetic", s.doc(hits.scoreDocs[3].doc).getString("sev"));
    assertNull(s.doc(hits.scoreDocs[4].doc).getInt("sev"));

    r.close();
    w.close();
    dir.close();
  }

  public void testExcMixedBinaryStringAtom() throws Exception {
    IndexWriter w = newIndexWriter(dir);
    FieldTypes fieldTypes = w.getFieldTypes();
    Document doc = w.newDocument();
    doc.addAtom("field", "bar");

    Document doc2 = w.newDocument();
    shouldFail(() -> doc2.addAtom("field", new BytesRef("bar")),
               "field \"field\": cannot change from string to binary ATOM");
    w.close();
  }

  public void testPerFieldAnalyzers() throws Exception {
    IndexWriter w = newIndexWriter(dir);
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setIndexAnalyzer("foo", new MockAnalyzer(random(), MockTokenizer.WHITESPACE, true));
    fieldTypes.setIndexAnalyzer("bar", new MockAnalyzer(random(), MockTokenizer.KEYWORD, false));

    Document doc = w.newDocument();
    doc.addShortText("foo", "DOO dah");
    doc.addShortText("bar", "doo DAH");
    w.addDocument(doc);
    IndexReader r = DirectoryReader.open(w);
    fieldTypes = r.getFieldTypes();

    IndexSearcher s = newSearcher(r);

    assertEquals(1, s.search(fieldTypes.newStringTermQuery("foo", "doo"), 1).totalHits);
    assertEquals(1, s.search(fieldTypes.newStringTermQuery("bar", "doo DAH"), 1).totalHits);
    r.close();
    w.close();
  }

  public void testPreAnalyzed() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    Document doc = w.newDocument();
    doc.addLargeText("body", new CannedTokenStream(new Token[] {new Token("foo", 0, 3),
                                                                new Token("BAR", 4, 6)}));
    w.addDocument(doc);
    IndexReader r = DirectoryReader.open(w);
    IndexSearcher s = newSearcher(r);
    FieldTypes fieldTypes = r.getFieldTypes();
    assertEquals(1, s.search(fieldTypes.newStringTermQuery("body", "BAR"), 1).totalHits);
    r.close();
    w.close();
    dir.close();
  }
}
