package org.apache.lucene.index;

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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LowSchemaField;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

/** Holds test cases that make schema changes only "allowed" by the low schema. */

public class TestAbuseSchema extends LuceneTestCase {

  // LUCENE-1010
  public void testNoTermVectorAfterTermVectorMerge() throws IOException {
    Directory dir = newDirectory();
    Analyzer a = new MockAnalyzer(random());
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(a));
    List<LowSchemaField> document = new ArrayList<>();
    LowSchemaField field = new LowSchemaField(a, "tvtest", "a b c", IndexOptions.DOCS, false);
    field.enableTermVectors(false, false, false);
    document.add(field);
    iw.addDocument(document);
    iw.commit();

    document = new ArrayList<>();
    document.add(new LowSchemaField(a, "tvtest", "a b c", IndexOptions.DOCS, false));
    iw.addDocument(document);
    // Make first segment
    iw.commit();

    iw.forceMerge(1);

    document = new ArrayList<>();
    document.add(field);
    iw.addDocument(document);
    // Make 2nd segment
    iw.commit();
    iw.forceMerge(1);

    iw.close();
    dir.close();
  }

  /** 
   * In a single doc, for the same field, mix the term vectors up 
   */
  public void testInconsistentTermVectorOptions() throws IOException {

    LowSchemaField f1, f2;

    // no vectors + vectors
    Analyzer a = new MockAnalyzer(random());
    f1 = new LowSchemaField(a, "field", "value1", IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, true);
    f2 = new LowSchemaField(a, "field", "value2", IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, true);
    f2.enableTermVectors(false, false, false);
    doTestMixup(f1, f2);
    
    // vectors + vectors with pos
    a = new MockAnalyzer(random());
    f1 = new LowSchemaField(a, "field", "value1", IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, true);
    f1.enableTermVectors(false, false, false);
    f2 = new LowSchemaField(a, "field", "value2", IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, true);
    f2.enableTermVectors(true, false, false);
    doTestMixup(f1, f2);
    
    // vectors + vectors with off
    a = new MockAnalyzer(random());
    f1 = new LowSchemaField(a, "field", "value1", IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, true);
    f1.enableTermVectors(false, false, false);
    f2 = new LowSchemaField(a, "field", "value2", IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, true);
    f2.enableTermVectors(false, true, false);
    doTestMixup(f1, f2);
    
    // vectors with pos + vectors with pos + off
    a = new MockAnalyzer(random());
    f1 = new LowSchemaField(a, "field", "value1", IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, true);
    f1.enableTermVectors(true, false, false);
    f2 = new LowSchemaField(a, "field", "value2", IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, true);
    f2.enableTermVectors(true, true, false);
    doTestMixup(f1, f2);

    // vectors with pos + vectors with pos + pay
    a = new MockAnalyzer(random());
    f1 = new LowSchemaField(a, "field", "value1", IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, true);
    f1.enableTermVectors(true, false, false);
    f2 = new LowSchemaField(a, "field", "value2", IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, true);
    f2.enableTermVectors(true, false, true);
    doTestMixup(f1, f2);
  }
  
  private void doTestMixup(LowSchemaField f1, LowSchemaField f2) throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    
    // add 3 good docs
    for (int i = 0; i < 3; i++) {
      Document doc = iw.newDocument();
      doc.addAtom("id", Integer.toString(i));
      iw.addDocument(doc);
    }

    // add broken doc
    List<LowSchemaField> doc = new ArrayList<>();
    doc.add(f1);
    doc.add(f2);
    
    // ensure broken doc hits exception
    try {
      iw.addDocument(doc);
      fail("didn't hit expected exception");
    } catch (IllegalArgumentException iae) {
      assertNotNull(iae.getMessage());
      assertTrue(iae.getMessage().startsWith("all instances of a given field name must have the same term vectors settings"));
    }
    
    // ensure good docs are still ok
    IndexReader ir = iw.getReader();
    assertEquals(3, ir.numDocs());
    
    ir.close();
    iw.close();
    dir.close();
  }

  // LUCENE-5611: don't abort segment when term vector settings are wrong
  public void testNoAbortOnBadTVSettings() throws Exception {
    Directory dir = newDirectory();
    // Don't use RandomIndexWriter because we want to be sure both docs go to 1 seg:
    Analyzer a = new MockAnalyzer(random());
    IndexWriterConfig iwc = new IndexWriterConfig(a);
    IndexWriter iw = new IndexWriter(dir, iwc);

    List<LowSchemaField> doc = new ArrayList<>();
    iw.addDocument(doc);
    LowSchemaField field = new LowSchemaField(a, "field", "value", IndexOptions.NONE, false);
    field.enableTermVectors(false, false, false);
    doc.add(field);
    try {
      iw.addDocument(doc);
      fail("should have hit exc");
    } catch (IllegalArgumentException iae) {
      // expected
    }
    IndexReader r = DirectoryReader.open(iw, true);

    // Make sure the exc didn't lose our first document:
    assertEquals(1, r.numDocs());
    iw.close();
    r.close();
    dir.close();
  }

  public void testPostingsOffsetsWithUnindexedFields() throws Exception {
    Directory dir = newDirectory();
    Analyzer a = new MockAnalyzer(random());
    RandomIndexWriter riw = newRandomIndexWriter(dir, a);
    for (int i = 0; i < 100; i++) {
      // ensure at least one doc is indexed with offsets
      LowSchemaField field;
      if (i < 99 && random().nextInt(2) == 0) {
        // stored only
        field = new LowSchemaField(a, "foo", "boo!", IndexOptions.NONE, false);
      } else {
        field = new LowSchemaField(a, "foo", "boo!", IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS, true);
        if (random().nextBoolean()) {
          // store some term vectors for the checkindex cross-check
          field.enableTermVectors(random().nextBoolean(), random().nextBoolean(), false);
        }
      }
      riw.addDocument(Collections.singletonList(field));
    }
    CompositeReader ir = riw.getReader();
    LeafReader slow = SlowCompositeReaderWrapper.wrap(ir);
    FieldInfos fis = slow.getFieldInfos();
    assertEquals(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS, fis.fieldInfo("foo").getIndexOptions());
    slow.close();
    ir.close();
    riw.close();
    dir.close();
  }
  
  /**
   * Tests various combinations of omitNorms=true/false, the field not existing at all,
   * ensuring that only omitNorms is 'viral'.
   * Internally checks that MultiNorms.norms() is consistent (returns the same bytes)
   * as the fully merged equivalent.
   */
  public void testOmitNormsCombos() throws IOException {
    Analyzer a = new MockAnalyzer(random());
    // indexed with norms
    LowSchemaField norms = new LowSchemaField(a, "foo", "a", IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, true);

    // indexed without norms
    LowSchemaField noNorms = new LowSchemaField(a, "foo", "a", IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, true);
    noNorms.disableNorms();

    // not indexed, but stored
    LowSchemaField noIndex = new LowSchemaField(a, "foo", "a", IndexOptions.NONE, false);

    // not indexed but stored, omitNorms is set
    LowSchemaField noNormsNoIndex = new LowSchemaField(a, "foo", "a", IndexOptions.NONE, false);
    noNormsNoIndex.disableNorms();

    // not indexed nor stored (doesnt exist at all, we index a different field instead)
    LowSchemaField emptyNorms = new LowSchemaField(a, "bar", "a", IndexOptions.NONE, false);
    
    assertNotNull(getNorms("foo", norms, norms));
    assertNull(getNorms("foo", norms, noNorms));
    assertNotNull(getNorms("foo", norms, noIndex));
    assertNotNull(getNorms("foo", norms, noNormsNoIndex));
    assertNotNull(getNorms("foo", norms, emptyNorms));
    assertNull(getNorms("foo", noNorms, noNorms));
    assertNull(getNorms("foo", noNorms, noIndex));
    assertNull(getNorms("foo", noNorms, noNormsNoIndex));
    assertNull(getNorms("foo", noNorms, emptyNorms));
    assertNull(getNorms("foo", noIndex, noIndex));
    assertNull(getNorms("foo", noIndex, noNormsNoIndex));
    assertNull(getNorms("foo", noIndex, emptyNorms));
    assertNull(getNorms("foo", noNormsNoIndex, noNormsNoIndex));
    assertNull(getNorms("foo", noNormsNoIndex, emptyNorms));
    assertNull(getNorms("foo", emptyNorms, emptyNorms));
  }

  /**
   * Indexes at least 1 document with f1, and at least 1 document with f2.
   * returns the norms for "field".
   */
  NumericDocValues getNorms(String field, LowSchemaField f1, LowSchemaField f2) throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()))
                              .setMergePolicy(newLogMergePolicy());
    RandomIndexWriter riw = new RandomIndexWriter(random(), dir, iwc);
    
    // add f1
    riw.addDocument(Collections.singletonList(f1));
    
    // add f2
    riw.addDocument(Collections.singletonList(f2));
    
    // add a mix of f1's and f2's
    int numExtraDocs = TestUtil.nextInt(random(), 1, 1000);
    for (int i = 0; i < numExtraDocs; i++) {
      riw.addDocument(Collections.singletonList(random().nextBoolean() ? f1 : f2));
    }

    IndexReader ir1 = riw.getReader();
    // todo: generalize
    NumericDocValues norms1 = MultiDocValues.getNormValues(ir1, field);
    
    // fully merge and validate MultiNorms against single segment.
    riw.forceMerge(1);
    DirectoryReader ir2 = riw.getReader();
    NumericDocValues norms2 = getOnlySegmentReader(ir2).getNormValues(field);

    if (norms1 == null) {
      assertNull(norms2);
    } else {
      for(int docID=0;docID<ir1.maxDoc();docID++) {
        assertEquals(norms1.get(docID), norms2.get(docID));
      }
    }
    ir1.close();
    ir2.close();
    riw.close();
    dir.close();
    return norms1;
  }

  public void testSameFieldNameForPostingAndDocValue() throws Exception {
    // LUCENE-5192: FieldInfos.Builder neglected to update
    // globalFieldNumbers.docValuesType map if the field existed, resulting in
    // potentially adding the same field with different DV types.
    Directory dir = newDirectory();
    Analyzer a = new MockAnalyzer(random());
    IndexWriterConfig conf = newIndexWriterConfig(a);
    IndexWriter writer = new IndexWriter(dir, conf);
    List<LowSchemaField> doc = new ArrayList<>();

    LowSchemaField field = new LowSchemaField(a, "f", "mock-value", IndexOptions.DOCS, false);
    field.disableNorms();
    field.doNotStore();
    doc.add(field);

    field = new LowSchemaField(a, "f", 5, IndexOptions.NONE, false);
    field.setDocValuesType(DocValuesType.NUMERIC);
    doc.add(field);
    writer.addDocument(doc);
    writer.commit();
    
    doc = new ArrayList<>();
    field = new LowSchemaField(a, "f", new BytesRef("mock"), IndexOptions.NONE, false);
    field.setDocValuesType(DocValuesType.BINARY);
    doc.add(field);

    try {
      writer.addDocument(doc);
      fail("should not have succeeded to add a field with different DV type than what already exists");
    } catch (IllegalArgumentException e) {
      writer.rollback();
    }
    
    dir.close();
  }

  // LUCENE-6049
  public void testExcIndexingDocBeforeDocValues() throws Exception {
    Directory dir = newDirectory();
    Analyzer a = new MockAnalyzer(random());
    IndexWriterConfig iwc = new IndexWriterConfig(a);
    IndexWriter w = new IndexWriter(dir, iwc);
    List<LowSchemaField> doc = new ArrayList<>();
    LowSchemaField field = new LowSchemaField(a, "test", "value", IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, true);
    field.setDocValuesType(DocValuesType.SORTED);
    field.doNotStore();
    field.setTokenStream(new TokenStream() {
        @Override
        public boolean incrementToken() {
          throw new RuntimeException("no");
        }
      });
    doc.add(field);
    try {
      w.addDocument(doc);
      fail("did not hit exception");
    } catch (RuntimeException re) {
      // expected
    }
    w.addDocument(w.newDocument());
    w.close();
    dir.close();
  }


  public void testSameFieldNumbersAcrossSegments() throws Exception {
    for (int i = 0; i < 2; i++) {
      Directory dir = newDirectory();
      Analyzer a = new MockAnalyzer(random());
      IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(a)
                                                   .setMergePolicy(NoMergePolicy.INSTANCE));

      List<LowSchemaField> d1 = new ArrayList<>();
      d1.add(new LowSchemaField(a, "f1", "first field", IndexOptions.DOCS, false));
      d1.add(new LowSchemaField(a, "f2", "second field", IndexOptions.DOCS, false));
      writer.addDocument(d1);

      if (i == 1) {
        writer.close();
        writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
                                         .setMergePolicy(NoMergePolicy.INSTANCE));
      } else {
        writer.commit();
      }

      List<LowSchemaField> d2 = new ArrayList<>();
      d2.add(new LowSchemaField(a, "f2", "second field", IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, true));
      LowSchemaField field = new LowSchemaField(a, "f1", "first field", IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, true);
      field.enableTermVectors(false, false, false);
      d2.add(field);
      d2.add(new LowSchemaField(a, "f3", "third field", IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, true));
      d2.add(new LowSchemaField(a, "f4", "fourth field", IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, true));
      writer.addDocument(d2);

      writer.close();

      SegmentInfos sis = SegmentInfos.readLatestCommit(dir);
      assertEquals(2, sis.size());

      FieldInfos fis1 = IndexWriter.readFieldInfos(sis.info(0));
      FieldInfos fis2 = IndexWriter.readFieldInfos(sis.info(1));

      assertEquals("f1", fis1.fieldInfo(0).name);
      assertEquals("f2", fis1.fieldInfo(1).name);
      assertEquals("f1", fis2.fieldInfo(0).name);
      assertEquals("f2", fis2.fieldInfo(1).name);
      assertEquals("f3", fis2.fieldInfo(2).name);
      assertEquals("f4", fis2.fieldInfo(3).name);

      writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
      writer.forceMerge(1);
      writer.close();

      sis = SegmentInfos.readLatestCommit(dir);
      assertEquals(1, sis.size());

      FieldInfos fis3 = IndexWriter.readFieldInfos(sis.info(0));

      assertEquals("f1", fis3.fieldInfo(0).name);
      assertEquals("f2", fis3.fieldInfo(1).name);
      assertEquals("f3", fis3.fieldInfo(2).name);
      assertEquals("f4", fis3.fieldInfo(3).name);


      dir.close();
    }
  }

  public void testEnablingNorms() throws IOException {
    Directory dir = newDirectory();
    Analyzer a = new MockAnalyzer(random());
    IndexWriter writer  = new IndexWriter(dir, newIndexWriterConfig(a)
                                          .setMaxBufferedDocs(10));
    // Enable norms for only 1 doc, pre flush
    for(int j=0;j<10;j++) {
      List<LowSchemaField> doc = new ArrayList<>();
      LowSchemaField f;
      if (j != 8) {
        f = new LowSchemaField(a, "field", "aaa", IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, true);
        f.disableNorms();
      } else {
        f = new LowSchemaField(a, "field", "aaa", IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, true);
        f.doNotStore();
      }
      doc.add(f);
      writer.addDocument(doc);
    }
    writer.close();

    Term searchTerm = new Term("field", "aaa");

    IndexReader reader = DirectoryReader.open(dir);
    IndexSearcher searcher = newSearcher(reader);
    ScoreDoc[] hits = searcher.search(new TermQuery(searchTerm), null, 1000).scoreDocs;
    assertEquals(10, hits.length);
    reader.close();

    writer = new IndexWriter(dir, newIndexWriterConfig(a)
                             .setOpenMode(IndexWriterConfig.OpenMode.CREATE).setMaxBufferedDocs(10));
    // Enable norms for only 1 doc, post flush
    for(int j=0;j<27;j++) {
      List<LowSchemaField> doc = new ArrayList<>();
      LowSchemaField f;
      if (j != 26) {
        f = new LowSchemaField(a, "field", "aaa", IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, true);
        f.disableNorms();
      } else {
        f = new LowSchemaField(a, "field", "aaa", IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, true);
        f.doNotStore();
      }
      doc.add(f);
      writer.addDocument(doc);
    }
    writer.close();
    reader = DirectoryReader.open(dir);
    searcher = newSearcher(reader);
    hits = searcher.search(new TermQuery(searchTerm), null, 1000).scoreDocs;
    assertEquals(27, hits.length);
    reader.close();

    reader = DirectoryReader.open(dir);
    reader.close();

    dir.close();
  }

  public void testVariableSchema() throws Exception {
    Directory dir = newDirectory();
    for(int i=0;i<20;i++) {
      if (VERBOSE) {
        System.out.println("TEST: iter=" + i);
      }
      Analyzer a = new MockAnalyzer(random());
      IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(a)
                                                  .setMaxBufferedDocs(2)
                                                  .setMergePolicy(newLogMergePolicy()));
      //LogMergePolicy lmp = (LogMergePolicy) writer.getConfig().getMergePolicy();
      //lmp.setMergeFactor(2);
      //lmp.setNoCFSRatio(0.0);
      List<LowSchemaField> doc = new ArrayList<>();
      String contents = "aa bb cc dd ee ff gg hh ii jj kk";

      if (i == 7) {
        // Add empty docs here
        LowSchemaField field = new LowSchemaField(a, "content3", "", IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, true);
        field.doNotStore();
        doc.add(field);
      } else {
        if (i%2 == 0) {
          doc.add(new LowSchemaField(a, "content4", contents, IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, true));
          doc.add(new LowSchemaField(a, "content5", "", IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, true));
        } else {
          LowSchemaField field = new LowSchemaField(a, "content5", "", IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, true);
          field.doNotStore();
          doc.add(field);
        }
        LowSchemaField field = new LowSchemaField(a, "content1", contents, IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, true);
        field.doNotStore();
        doc.add(field);
        doc.add(new LowSchemaField(a, "content3", "", IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, true));
      }

      for(int j=0;j<4;j++) {
        writer.addDocument(doc);
      }

      writer.close();

      if (0 == i % 4) {
        writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
        //LogMergePolicy lmp2 = (LogMergePolicy) writer.getConfig().getMergePolicy();
        //lmp2.setNoCFSRatio(0.0);
        writer.forceMerge(1);
        writer.close();
      }
    }
    dir.close();
  }

  public void testIndexStoreCombos() throws Exception {
    Directory dir = newDirectory();
    Analyzer a = new MockAnalyzer(random());
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(a));
    byte[] b = new byte[50];
    for(int i=0;i<50;i++) {
      b[i] = (byte) (i+77);
    }

    List<LowSchemaField> doc = new ArrayList<>();

    LowSchemaField f = new LowSchemaField(a, "binary", new BytesRef(b, 10, 17), IndexOptions.DOCS, true);
    final MockTokenizer doc1field1 = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    doc1field1.setReader(new StringReader("doc1field1"));
    f.setTokenStream(doc1field1);

    LowSchemaField f2 = new LowSchemaField(a, "string", "value", IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, true);
    final MockTokenizer doc1field2 = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    doc1field2.setReader(new StringReader("doc1field2"));
    f2.setTokenStream(doc1field2);
    doc.add(f);
    doc.add(f2);
    w.addDocument(doc);

    // add 2 docs to test in-memory merging
    final MockTokenizer doc2field1 = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    doc2field1.setReader(new StringReader("doc2field1"));
    f.setTokenStream(doc2field1);
    final MockTokenizer doc2field2 = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    doc2field2.setReader(new StringReader("doc2field2"));
    f2.setTokenStream(doc2field2);
    w.addDocument(doc);

    // force segment flush so we can force a segment merge with doc3 later.
    w.commit();

    final MockTokenizer doc3field1 = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    doc3field1.setReader(new StringReader("doc3field1"));
    f.setTokenStream(doc3field1);
    final MockTokenizer doc3field2 = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    doc3field2.setReader(new StringReader("doc3field2"));
    f2.setTokenStream(doc3field2);

    w.addDocument(doc);
    w.commit();
    w.forceMerge(1);   // force segment merge.
    w.close();

    IndexReader ir = DirectoryReader.open(dir);
    Document doc2 = ir.document(0);
    IndexableField f3 = doc2.getField("binary");
    b = f3.binaryValue().bytes;
    assertTrue(b != null);
    assertEquals(17, b.length, 17);
    assertEquals(87, b[0]);

    assertTrue(ir.document(0).getField("binary").binaryValue()!=null);
    assertTrue(ir.document(1).getField("binary").binaryValue()!=null);
    assertTrue(ir.document(2).getField("binary").binaryValue()!=null);

    assertEquals("value", ir.document(0).get("string"));
    assertEquals("value", ir.document(1).get("string"));
    assertEquals("value", ir.document(2).get("string"));


    // test that the terms were indexed.
    assertTrue(TestUtil.docs(random(), ir, "binary", new BytesRef("doc1field1"), null, null, DocsEnum.FLAG_NONE).nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    assertTrue(TestUtil.docs(random(), ir, "binary", new BytesRef("doc2field1"), null, null, DocsEnum.FLAG_NONE).nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    assertTrue(TestUtil.docs(random(), ir, "binary", new BytesRef("doc3field1"), null, null, DocsEnum.FLAG_NONE).nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    assertTrue(TestUtil.docs(random(), ir, "string", new BytesRef("doc1field2"), null, null, DocsEnum.FLAG_NONE).nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    assertTrue(TestUtil.docs(random(), ir, "string", new BytesRef("doc2field2"), null, null, DocsEnum.FLAG_NONE).nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    assertTrue(TestUtil.docs(random(), ir, "string", new BytesRef("doc3field2"), null, null, DocsEnum.FLAG_NONE).nextDoc() != DocIdSetIterator.NO_MORE_DOCS);

    ir.close();
    dir.close();
  }

  // Tests whether the DocumentWriter correctly enable the
  // omitTermFreqAndPositions bit in the FieldInfo
  public void testPositions() throws Exception {
    Directory ram = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriter writer = new IndexWriter(ram, newIndexWriterConfig(analyzer));
    List<LowSchemaField> d = new ArrayList<>();
        
    // f1,f2,f3: docs only
    d.add(new LowSchemaField(analyzer, "f1", "This field has docs only", IndexOptions.DOCS, true));
    d.add(new LowSchemaField(analyzer, "f2", "This field has docs only", IndexOptions.DOCS, true));
    d.add(new LowSchemaField(analyzer, "f3", "This field has docs only", IndexOptions.DOCS, true));

    d.add(new LowSchemaField(analyzer, "f4", "This field has docs and freqs", IndexOptions.DOCS_AND_FREQS, true));
    d.add(new LowSchemaField(analyzer, "f5", "This field has docs and freqs", IndexOptions.DOCS_AND_FREQS, true));
    d.add(new LowSchemaField(analyzer, "f6", "This field has docs and freqs", IndexOptions.DOCS_AND_FREQS, true));
    
    d.add(new LowSchemaField(analyzer, "f7", "This field has docs and freqs and positions", IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, true));
    d.add(new LowSchemaField(analyzer, "f8", "This field has docs and freqs and positions", IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, true));
    d.add(new LowSchemaField(analyzer, "f9", "This field has docs and freqs and positions", IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, true));
        
    writer.addDocument(d);
    writer.forceMerge(1);

    // now we add another document which has docs-only for f1, f4, f7, docs/freqs for f2, f5, f8, 
    // and docs/freqs/positions for f3, f6, f9
    d = new ArrayList<>();
    
    // f1,f4,f7: docs only
    d.add(new LowSchemaField(analyzer, "f1", "This field has docs only", IndexOptions.DOCS, true));
    d.add(new LowSchemaField(analyzer, "f4", "This field has docs only", IndexOptions.DOCS, true));
    d.add(new LowSchemaField(analyzer, "f7", "This field has docs only", IndexOptions.DOCS, true));

    // f2, f5, f8: docs and freqs
    d.add(new LowSchemaField(analyzer, "f2", "This field has docs and freqs", IndexOptions.DOCS_AND_FREQS, true));
    d.add(new LowSchemaField(analyzer, "f5", "This field has docs and freqs", IndexOptions.DOCS_AND_FREQS, true));
    d.add(new LowSchemaField(analyzer, "f8", "This field has docs and freqs", IndexOptions.DOCS_AND_FREQS, true));
    
    // f3, f6, f9: docs and freqs and positions
    d.add(new LowSchemaField(analyzer, "f3", "This field has docs and freqs and positions", IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, true));
    d.add(new LowSchemaField(analyzer, "f6", "This field has docs and freqs and positions", IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, true));
    d.add(new LowSchemaField(analyzer, "f9", "This field has docs and freqs and positions", IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, true));
    writer.addDocument(d);

    // force merge
    writer.forceMerge(1);
    // flush
    writer.close();

    SegmentReader reader = getOnlySegmentReader(DirectoryReader.open(ram));
    FieldInfos fi = reader.getFieldInfos();
    // docs + docs = docs
    assertEquals(IndexOptions.DOCS, fi.fieldInfo("f1").getIndexOptions());
    // docs + docs/freqs = docs
    assertEquals(IndexOptions.DOCS, fi.fieldInfo("f2").getIndexOptions());
    // docs + docs/freqs/pos = docs
    assertEquals(IndexOptions.DOCS, fi.fieldInfo("f3").getIndexOptions());
    // docs/freqs + docs = docs
    assertEquals(IndexOptions.DOCS, fi.fieldInfo("f4").getIndexOptions());
    // docs/freqs + docs/freqs = docs/freqs
    assertEquals(IndexOptions.DOCS_AND_FREQS, fi.fieldInfo("f5").getIndexOptions());
    // docs/freqs + docs/freqs/pos = docs/freqs
    assertEquals(IndexOptions.DOCS_AND_FREQS, fi.fieldInfo("f6").getIndexOptions());
    // docs/freqs/pos + docs = docs
    assertEquals(IndexOptions.DOCS, fi.fieldInfo("f7").getIndexOptions());
    // docs/freqs/pos + docs/freqs = docs/freqs
    assertEquals(IndexOptions.DOCS_AND_FREQS, fi.fieldInfo("f8").getIndexOptions());
    // docs/freqs/pos + docs/freqs/pos = docs/freqs/pos
    assertEquals(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, fi.fieldInfo("f9").getIndexOptions());
    
    reader.close();
    ram.close();
  }
  
  // Verifies no *.prx exists when all fields omit term positions:
  public void testNoPrxFile() throws Throwable {
    Directory ram = newDirectory();
    if (ram instanceof MockDirectoryWrapper) {
      // we verify some files get deleted
      ((MockDirectoryWrapper)ram).setEnableVirusScanner(false);
    }
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriter writer = new IndexWriter(ram, newIndexWriterConfig(analyzer)
                                                .setMaxBufferedDocs(3)
                                                .setMergePolicy(newLogMergePolicy()));
    LogMergePolicy lmp = (LogMergePolicy) writer.getConfig().getMergePolicy();
    lmp.setMergeFactor(2);
    lmp.setNoCFSRatio(0.0);

    List<LowSchemaField> d = new ArrayList<>();
    d.add(new LowSchemaField(analyzer, "f1", "This field has term freqs", IndexOptions.DOCS_AND_FREQS, true));
    for(int i=0;i<30;i++) {
      writer.addDocument(d);
    }

    writer.commit();

    assertNoPrx(ram);
    
    // now add some documents with positions, and check there is no prox after optimization
    d = new ArrayList<>();
    d.add(new LowSchemaField(analyzer, "f1", "This field has term freqs", IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, true));
    
    for(int i=0;i<30;i++) {
      writer.addDocument(d);
    }

    // force merge
    writer.forceMerge(1);
    // flush
    writer.close();

    assertNoPrx(ram);
    ram.close();
  }

  private void assertNoPrx(Directory dir) throws Throwable {
    final String[] files = dir.listAll();
    for(int i=0;i<files.length;i++) {
      assertFalse(files[i].endsWith(".prx"));
      assertFalse(files[i].endsWith(".pos"));
    }
  }
  
  /** make sure we downgrade positions and payloads correctly */
  public void testMixing() throws Exception {
    Directory dir = newDirectory();
    Analyzer a = new MockAnalyzer(random());

    RandomIndexWriter iw = newRandomIndexWriter(dir, a);
    
    for (int i = 0; i < 20; i++) {
      List<LowSchemaField> doc = new ArrayList<>();
      if (i < 19 && random().nextBoolean()) {
        for (int j = 0; j < 50; j++) {
          doc.add(new LowSchemaField(a, "foo", "i have positions", IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, true));
        }
      } else {
        for (int j = 0; j < 50; j++) {
          doc.add(new LowSchemaField(a, "foo", "i have no positions", IndexOptions.DOCS_AND_FREQS, true));
        }
      }
      iw.addDocument(doc);
      iw.commit();
    }
    
    if (random().nextBoolean()) {
      iw.forceMerge(1);
    }
    
    DirectoryReader ir = iw.getReader();
    FieldInfos fis = MultiFields.getMergedFieldInfos(ir);
    assertEquals(IndexOptions.DOCS_AND_FREQS, fis.fieldInfo("foo").getIndexOptions());
    assertFalse(fis.fieldInfo("foo").hasPayloads());
    iw.close();
    ir.close();
    dir.close(); // checkindex
  }

  public void testTypeChangeViaAddIndexesIR2() throws Exception {
    Directory dir = newDirectory();
    Analyzer a = new MockAnalyzer(random());
    IndexWriterConfig conf = newIndexWriterConfig(a);
    IndexWriter writer = new IndexWriter(dir, conf);
    LowSchemaField field = new LowSchemaField(a, "dv", 0L, IndexOptions.NONE, false);
    field.setDocValuesType(DocValuesType.NUMERIC);
    List<LowSchemaField> doc = new ArrayList<>();
    doc.add(field);
    writer.addDocument(doc);
    writer.close();

    Directory dir2 = newDirectory();
    conf = newIndexWriterConfig(new MockAnalyzer(random()));
    writer = new IndexWriter(dir2, conf);
    DirectoryReader reader = DirectoryReader.open(dir);
    TestUtil.addIndexesSlowly(writer, reader);
    reader.close();
    field = new LowSchemaField(a, "dv", new BytesRef("foo"), IndexOptions.NONE, false);
    field.setDocValuesType(DocValuesType.BINARY);
    doc = new ArrayList<>();
    doc.add(field);
    try {
      writer.addDocument(doc);
      fail("did not hit exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
    writer.close();
    dir2.close();
    dir.close();
  }

  // LUCENE-1008
  public void testNoTermVectorAfterTermVector() throws IOException {
    Directory dir = newDirectory();
    Analyzer a = new MockAnalyzer(random());    
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));

    List<LowSchemaField> document = new ArrayList<>();
    LowSchemaField field = new LowSchemaField(a, "tvtest", "a b c", IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, true);
    field.enableTermVectors(true, true, true);
    document.add(field);
    iw.addDocument(document);

    document = new ArrayList<>();
    field = new LowSchemaField(a, "tvtest", "x y z", IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, true);
    field.enableTermVectors(true, true, true);
    document.add(field);
    iw.addDocument(document);

    // Make first segment
    iw.commit();

    document = new ArrayList<>();
    field = new LowSchemaField(a, "tvtest", "a b c", IndexOptions.NONE, false);
    document.add(field);
    iw.addDocument(document);
    // Make 2nd segment
    iw.commit();

    iw.forceMerge(1);
    iw.close();
    dir.close();
  }

  /**
   * Test adding two fields with the same name, one indexed
   * the other stored only. The omitNorms and omitTermFreqAndPositions setting
   * of the stored field should not affect the indexed one (LUCENE-1590)
   */
  public void testLUCENE_1590() throws Exception {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    Analyzer a = new MockAnalyzer(random());

    List<LowSchemaField> doc = new ArrayList<>();
    LowSchemaField field = new LowSchemaField(a, "f1", "v1", IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, true);
    field.disableNorms();
    doc.add(field);

    field = new LowSchemaField(a, "f1", "v2", IndexOptions.NONE, false);
    doc.add(field);

    // f2 has no TF
    field = new LowSchemaField(a, "f2", "v1", IndexOptions.DOCS, true);
    doc.add(field);

    field = new LowSchemaField(a, "f2", "v2", IndexOptions.NONE, false);
    doc.add(field);

    writer.addDocument(doc);
    writer.forceMerge(1); // be sure to have a single segment
    writer.close();

    TestUtil.checkIndex(dir);

    SegmentReader reader = getOnlySegmentReader(DirectoryReader.open(dir));
    FieldInfos fi = reader.getFieldInfos();
    // f1
    assertFalse("f1 should have no norms", fi.fieldInfo("f1").hasNorms());
    assertEquals("omitTermFreqAndPositions field bit should not be set for f1", IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, fi.fieldInfo("f1").getIndexOptions());
    // f2
    assertTrue("f2 should have norms", fi.fieldInfo("f2").hasNorms());
    assertEquals("omitTermFreqAndPositions field bit should be set for f2", IndexOptions.DOCS, fi.fieldInfo("f2").getIndexOptions());
    reader.close();
    dir.close();
  }


  public void testMixedTypesAfterReopenAppend1() throws Exception {
    Directory dir = newDirectory();
    Analyzer a = new MockAnalyzer(random());
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(a));
    List<LowSchemaField> doc = new ArrayList<>();
    LowSchemaField field = new LowSchemaField(a, "foo", 0, IndexOptions.NONE, false);
    field.setDocValuesType(DocValuesType.NUMERIC);
    doc.add(field);
    w.addDocument(doc);
    w.close();

    w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    doc = new ArrayList<>();
    field = new LowSchemaField(a, "foo", new BytesRef("hello"), IndexOptions.NONE, false);
    field.setDocValuesType(DocValuesType.SORTED);
    doc.add(field);
    try {
      w.addDocument(doc);
      fail("did not get expected exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
    w.close();
    dir.close();
  }

  public void testMixedTypesAfterReopenAppend2() throws Exception {
    Directory dir = newDirectory();
    Analyzer a = new MockAnalyzer(random());
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(a));
    List<LowSchemaField> doc = new ArrayList<>();
    LowSchemaField field = new LowSchemaField(a, "foo", new BytesRef("foo"), IndexOptions.NONE, false);
    field.setDocValuesType(DocValuesType.SORTED_SET);
    doc.add(field);
    w.addDocument(doc);
    w.close();

    w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    doc = new ArrayList<>();
    field = new LowSchemaField(a, "foo", "bar", IndexOptions.DOCS, false);
    doc.add(field);
    field = new LowSchemaField(a, "foo", new BytesRef("foo"), IndexOptions.NONE, false);
    field.setDocValuesType(DocValuesType.BINARY);
    doc.add(field);
    try {
      w.addDocument(doc);
      fail("did not get expected exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
    w.close();
    dir.close();
  }

  public void testMixedTypesAfterReopenAppend3() throws Exception {
    Directory dir = newDirectory();
    Analyzer a = new MockAnalyzer(random());
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(a));
    List<LowSchemaField> doc = new ArrayList<>();
    LowSchemaField field = new LowSchemaField(a, "foo", new BytesRef("foo"), IndexOptions.NONE, false);
    field.setDocValuesType(DocValuesType.SORTED_SET);
    doc.add(field);
    w.addDocument(doc);
    w.close();

    w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    doc = new ArrayList<>();
    field = new LowSchemaField(a, "foo", "bar", IndexOptions.DOCS, false);
    doc.add(field);
    field = new LowSchemaField(a, "foo", new BytesRef("foo"), IndexOptions.NONE, false);
    field.setDocValuesType(DocValuesType.BINARY);
    doc.add(field);
    try {
      w.addDocument(doc);
      fail("did not get expected exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
    // Also add another document so there is a segment to write here:
    w.addDocument(w.newDocument());
    w.close();
    dir.close();
  }

  public void testUntokenizedReader() throws Exception {
    IndexWriter w = newIndexWriter();
    List<LowSchemaField> doc = new ArrayList<>();
    doc.add(new LowSchemaField(new MockAnalyzer(random()), "field", new StringReader("string"), IndexOptions.DOCS, false));
    shouldFail(() -> w.addDocument(doc),
               "field \"field\" is stored but does not have binaryValue, stringValue nor numericValue");
    w.close();
  }

  public void testUpdateNumericDVFieldWithSameNameAsPostingField() throws Exception {
    // this used to fail because FieldInfos.Builder neglected to update
    // globalFieldMaps.docValuesTypes map
    Analyzer a = new MockAnalyzer(random());
    IndexWriterConfig conf = newIndexWriterConfig(a);
    IndexWriter writer = new IndexWriter(dir, conf);

    List<LowSchemaField> doc = new ArrayList<>();
    LowSchemaField field = new LowSchemaField(a, "f", "mock-value", IndexOptions.DOCS, false);
    doc.add(field);
    
    field = new LowSchemaField(a, "f", 5, IndexOptions.NONE, false);
    field.setDocValuesType(DocValuesType.NUMERIC);
    doc.add(field);
    writer.addDocument(doc);

    writer.commit();
    writer.updateNumericDocValue(new Term("f", "mock-value"), "f", 17L);
    writer.close();
    
    DirectoryReader r = DirectoryReader.open(dir);
    NumericDocValues ndv = r.leaves().get(0).reader().getNumericDocValues("f");
    assertEquals(17, ndv.get(0));
    r.close();
  }

  static BytesRef toBytes(long value) {
    BytesRef bytes = new BytesRef(10); // negative longs may take 10 bytes
    while ((value & ~0x7FL) != 0L) {
      bytes.bytes[bytes.length++] = (byte) ((value & 0x7FL) | 0x80L);
      value >>>= 7;
    }
    bytes.bytes[bytes.length++] = (byte) value;
    return bytes;
  }

  static long getValue(BinaryDocValues bdv, int idx) {
    BytesRef term = bdv.get(idx);
    idx = term.offset;
    byte b = term.bytes[idx++];
    long value = b & 0x7FL;
    for (int shift = 7; (b & 0x80L) != 0; shift += 7) {
      b = term.bytes[idx++];
      value |= (b & 0x7FL) << shift;
    }
    return value;
  }

  public void testUpdateBinaryDVFieldWithSameNameAsPostingField() throws Exception {
    // this used to fail because FieldInfos.Builder neglected to update
    // globalFieldMaps.docValuesTypes map
    Analyzer a = new MockAnalyzer(random());
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir, conf);
    
    List<LowSchemaField> doc = new ArrayList<>();
    LowSchemaField field = new LowSchemaField(a, "f", "mock-value", IndexOptions.DOCS, false);
    doc.add(field);

    field = new LowSchemaField(a, "f", toBytes(5L), IndexOptions.NONE, false);
    field.setDocValuesType(DocValuesType.BINARY);
    doc.add(field);

    writer.addDocument(doc);
    writer.commit();
    writer.updateBinaryDocValue(new Term("f", "mock-value"), "f", toBytes(17L));
    writer.close();
    
    DirectoryReader r = DirectoryReader.open(dir);
    BinaryDocValues bdv = r.leaves().get(0).reader().getBinaryDocValues("f");
    assertEquals(17, getValue(bdv, 0));
    r.close();
  }

  public void testHasUncommittedChangesAfterException() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    // we don't use RandomIndexWriter because it might add more docvalues than we expect !!!!
    IndexWriterConfig iwc = newIndexWriterConfig(analyzer);
    iwc.setMergePolicy(newLogMergePolicy());
    IndexWriter iwriter = new IndexWriter(directory, iwc);
    List<LowSchemaField> doc = new ArrayList<>();
    LowSchemaField field = new LowSchemaField(analyzer, "dv", "foo!", IndexOptions.NONE, false);
    field.setDocValuesType(DocValuesType.SORTED);
    doc.add(field);

    field = new LowSchemaField(analyzer, "dv", "bar!", IndexOptions.NONE, false);
    field.setDocValuesType(DocValuesType.SORTED);
    doc.add(field);

    try {
      iwriter.addDocument(doc);
      fail("didn't hit expected exception");
    } catch (IllegalArgumentException expected) {
      // expected
    }
    iwriter.commit();
    assertFalse(iwriter.hasUncommittedChanges());
    iwriter.close();
    directory.close();
  }

}
