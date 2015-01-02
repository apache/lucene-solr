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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CachingTokenFilter;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldTypes;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

/** tests for writing term vectors */
public class TestTermVectorsWriter extends LuceneTestCase {
  // LUCENE-1442
  public void testDoubleOffsetCounting() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setIndexOptions("field", IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    fieldTypes.setMultiValued("field");
    fieldTypes.setAnalyzerOffsetGap("field", 0);
    fieldTypes.setAnalyzerPositionGap("field", 0);
    fieldTypes.setDocValuesType("field", DocValuesType.NONE);
    fieldTypes.disableFastRanges("field");
    fieldTypes.enableTermVectors("field");
    fieldTypes.enableTermVectorOffsets("field");
    fieldTypes.enableTermVectorPositions("field");
    Document doc = w.newDocument();
    doc.addAtom("field", "abcd");
    doc.addAtom("field", "abcd");
    doc.addAtom("field", "");
    doc.addAtom("field", "abcd");
    w.addDocument(doc);
    w.close();

    IndexReader r = DirectoryReader.open(dir);
    Terms vector = r.getTermVectors(0).terms("field");
    assertNotNull(vector);
    TermsEnum termsEnum = vector.iterator(null);
    assertNotNull(termsEnum.next());
    assertEquals("", termsEnum.term().utf8ToString());

    // Token "" occurred once
    assertEquals(1, termsEnum.totalTermFreq());

    DocsAndPositionsEnum dpEnum = termsEnum.docsAndPositions(null, null);
    assertTrue(dpEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    dpEnum.nextPosition();
    assertEquals(8, dpEnum.startOffset());
    assertEquals(8, dpEnum.endOffset());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, dpEnum.nextDoc());

    // Token "abcd" occurred three times
    assertEquals(new BytesRef("abcd"), termsEnum.next());
    dpEnum = termsEnum.docsAndPositions(null, dpEnum);
    assertEquals(3, termsEnum.totalTermFreq());

    assertTrue(dpEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    dpEnum.nextPosition();
    assertEquals(0, dpEnum.startOffset());
    assertEquals(4, dpEnum.endOffset());

    dpEnum.nextPosition();
    assertEquals(4, dpEnum.startOffset());
    assertEquals(8, dpEnum.endOffset());

    dpEnum.nextPosition();
    assertEquals(8, dpEnum.startOffset());
    assertEquals(12, dpEnum.endOffset());

    assertEquals(DocIdSetIterator.NO_MORE_DOCS, dpEnum.nextDoc());
    assertNull(termsEnum.next());
    r.close();
    dir.close();
  }

  // LUCENE-1442
  public void testDoubleOffsetCounting2() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.enableTermVectors("field");
    fieldTypes.enableTermVectorOffsets("field");
    fieldTypes.enableTermVectorPositions("field");
    fieldTypes.setMultiValued("field");
    Document doc = w.newDocument();
    doc.addLargeText("field", "abcd");
    doc.addLargeText("field", "abcd");
    w.addDocument(doc);
    w.close();

    IndexReader r = DirectoryReader.open(dir);
    TermsEnum termsEnum = r.getTermVectors(0).terms("field").iterator(null);
    assertNotNull(termsEnum.next());
    DocsAndPositionsEnum dpEnum = termsEnum.docsAndPositions(null, null);
    assertEquals(2, termsEnum.totalTermFreq());

    assertTrue(dpEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    dpEnum.nextPosition();
    assertEquals(0, dpEnum.startOffset());
    assertEquals(4, dpEnum.endOffset());

    dpEnum.nextPosition();
    assertEquals(5, dpEnum.startOffset());
    assertEquals(9, dpEnum.endOffset());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, dpEnum.nextDoc());

    r.close();
    dir.close();
  }

  // LUCENE-1448
  public void testEndOffsetPositionCharAnalyzer() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.enableTermVectors("field");
    fieldTypes.enableTermVectorOffsets("field");
    fieldTypes.enableTermVectorPositions("field");
    fieldTypes.setMultiValued("field");
    Document doc = w.newDocument();
    doc.addLargeText("field", "abcd   ");
    doc.addLargeText("field", "abcd   ");
    w.addDocument(doc);
    w.close();

    IndexReader r = DirectoryReader.open(dir);
    TermsEnum termsEnum = r.getTermVectors(0).terms("field").iterator(null);
    assertNotNull(termsEnum.next());
    DocsAndPositionsEnum dpEnum = termsEnum.docsAndPositions(null, null);
    assertEquals(2, termsEnum.totalTermFreq());

    assertTrue(dpEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    dpEnum.nextPosition();
    assertEquals(0, dpEnum.startOffset());
    assertEquals(4, dpEnum.endOffset());

    dpEnum.nextPosition();
    assertEquals(8, dpEnum.startOffset());
    assertEquals(12, dpEnum.endOffset());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, dpEnum.nextDoc());

    r.close();
    dir.close();
  }

  // LUCENE-1448
  public void testEndOffsetPositionWithCachingTokenFilter() throws Exception {
    Directory dir = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(analyzer));
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.enableTermVectors("field");
    fieldTypes.enableTermVectorOffsets("field");
    fieldTypes.enableTermVectorPositions("field");
    fieldTypes.setMultiValued("field");
    Document doc = w.newDocument();
    try (TokenStream stream = analyzer.tokenStream("field", "abcd   ")) {
      stream.reset(); // TODO: weird to reset before wrapping with CachingTokenFilter... correct?
      TokenStream cachedStream = new CachingTokenFilter(stream);
      doc.addLargeText("field", cachedStream);
      doc.addLargeText("field", cachedStream);
      w.addDocument(doc);
    }
    w.close();

    IndexReader r = DirectoryReader.open(dir);
    TermsEnum termsEnum = r.getTermVectors(0).terms("field").iterator(null);
    assertNotNull(termsEnum.next());
    DocsAndPositionsEnum dpEnum = termsEnum.docsAndPositions(null, null);
    assertEquals(2, termsEnum.totalTermFreq());

    assertTrue(dpEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    dpEnum.nextPosition();
    assertEquals(0, dpEnum.startOffset());
    assertEquals(4, dpEnum.endOffset());

    dpEnum.nextPosition();
    assertEquals(8, dpEnum.startOffset());
    assertEquals(12, dpEnum.endOffset());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, dpEnum.nextDoc());

    r.close();
    dir.close();
  }
  
  // LUCENE-1448
  public void testEndOffsetPositionStopFilter() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.SIMPLE, true, MockTokenFilter.ENGLISH_STOPSET)));
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.enableTermVectors("field");
    fieldTypes.enableTermVectorOffsets("field");
    fieldTypes.enableTermVectorPositions("field");
    fieldTypes.setMultiValued("field");
    Document doc = w.newDocument();
    doc.addLargeText("field", "abcd the");
    doc.addLargeText("field", "abcd the");
    w.addDocument(doc);
    w.close();

    IndexReader r = DirectoryReader.open(dir);
    TermsEnum termsEnum = r.getTermVectors(0).terms("field").iterator(null);
    assertNotNull(termsEnum.next());
    DocsAndPositionsEnum dpEnum = termsEnum.docsAndPositions(null, null);
    assertEquals(2, termsEnum.totalTermFreq());

    assertTrue(dpEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    dpEnum.nextPosition();
    assertEquals(0, dpEnum.startOffset());
    assertEquals(4, dpEnum.endOffset());

    dpEnum.nextPosition();
    assertEquals(9, dpEnum.startOffset());
    assertEquals(13, dpEnum.endOffset());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, dpEnum.nextDoc());

    r.close();
    dir.close();
  }

  // LUCENE-1448
  public void testEndOffsetPositionStandard() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.enableTermVectors("field");
    fieldTypes.enableTermVectorOffsets("field");
    fieldTypes.enableTermVectorPositions("field");
    fieldTypes.setMultiValued("field");
    Document doc = w.newDocument();
    doc.addLargeText("field", "abcd the  ");
    doc.addLargeText("field", "crunch man");
    w.addDocument(doc);
    w.close();

    IndexReader r = DirectoryReader.open(dir);
    TermsEnum termsEnum = r.getTermVectors(0).terms("field").iterator(null);
    assertNotNull(termsEnum.next());
    DocsAndPositionsEnum dpEnum = termsEnum.docsAndPositions(null, null);

    assertTrue(dpEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    dpEnum.nextPosition();
    assertEquals(0, dpEnum.startOffset());
    assertEquals(4, dpEnum.endOffset());

    assertNotNull(termsEnum.next());
    dpEnum = termsEnum.docsAndPositions(null, dpEnum);
    assertTrue(dpEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    dpEnum.nextPosition();
    assertEquals(11, dpEnum.startOffset());
    assertEquals(17, dpEnum.endOffset());

    assertNotNull(termsEnum.next());
    dpEnum = termsEnum.docsAndPositions(null, dpEnum);
    assertTrue(dpEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    dpEnum.nextPosition();
    assertEquals(18, dpEnum.startOffset());
    assertEquals(21, dpEnum.endOffset());

    r.close();
    dir.close();
  }

  // LUCENE-1448
  public void testEndOffsetPositionStandardEmptyField() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    Document doc = w.newDocument();
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.enableTermVectors("field");
    fieldTypes.enableTermVectorOffsets("field");
    fieldTypes.enableTermVectorPositions("field");
    fieldTypes.setMultiValued("field");
    doc.addLargeText("field", "");
    doc.addLargeText("field", "crunch man");
    w.addDocument(doc);
    w.close();

    IndexReader r = DirectoryReader.open(dir);
    TermsEnum termsEnum = r.getTermVectors(0).terms("field").iterator(null);
    assertNotNull(termsEnum.next());
    DocsAndPositionsEnum dpEnum = termsEnum.docsAndPositions(null, null);

    assertEquals(1, (int) termsEnum.totalTermFreq());
    assertTrue(dpEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    dpEnum.nextPosition();
    assertEquals(1, dpEnum.startOffset());
    assertEquals(7, dpEnum.endOffset());

    assertNotNull(termsEnum.next());
    dpEnum = termsEnum.docsAndPositions(null, dpEnum);
    assertTrue(dpEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    dpEnum.nextPosition();
    assertEquals(8, dpEnum.startOffset());
    assertEquals(11, dpEnum.endOffset());

    r.close();
    dir.close();
  }

  // LUCENE-1448
  public void testEndOffsetPositionStandardEmptyField2() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    Document doc = w.newDocument();
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.enableTermVectors("field");
    fieldTypes.enableTermVectorOffsets("field");
    fieldTypes.enableTermVectorPositions("field");
    fieldTypes.setMultiValued("field");
    doc.addLargeText("field", "abcd");
    doc.addLargeText("field", "");
    doc.addLargeText("field", "crunch");
    w.addDocument(doc);
    w.close();

    IndexReader r = DirectoryReader.open(dir);
    TermsEnum termsEnum = r.getTermVectors(0).terms("field").iterator(null);
    assertNotNull(termsEnum.next());
    DocsAndPositionsEnum dpEnum = termsEnum.docsAndPositions(null, null);

    assertEquals(1, (int) termsEnum.totalTermFreq());
    assertTrue(dpEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    dpEnum.nextPosition();
    assertEquals(0, dpEnum.startOffset());
    assertEquals(4, dpEnum.endOffset());

    assertNotNull(termsEnum.next());
    dpEnum = termsEnum.docsAndPositions(null, dpEnum);
    assertTrue(dpEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    dpEnum.nextPosition();
    assertEquals(6, dpEnum.startOffset());
    assertEquals(12, dpEnum.endOffset());


    r.close();
    dir.close();
  }
  
  // LUCENE-1168
  public void testTermVectorCorruption() throws IOException {

    Directory dir = newDirectory();
    for(int iter=0;iter<2;iter++) {
      IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
          .setMaxBufferedDocs(2)
          .setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH)
          .setMergeScheduler(new SerialMergeScheduler())
          .setMergePolicy(new LogDocMergePolicy()));

      FieldTypes fieldTypes = writer.getFieldTypes();
      fieldTypes.enableTermVectors("termVector");
      fieldTypes.enableTermVectorOffsets("termVector");
      fieldTypes.enableTermVectorPositions("termVector");

      Document document = writer.newDocument();
      document.addStoredString("stored", "stored");
      writer.addDocument(document);
      writer.addDocument(document);

      document = writer.newDocument();
      document.addStoredString("stored", "stored");
      document.addLargeText("termVector", "termVector");
      writer.addDocument(document);
      writer.forceMerge(1);
      writer.close();

      IndexReader reader = DirectoryReader.open(dir);
      for(int i=0;i<reader.numDocs();i++) {
        reader.document(i);
        reader.getTermVectors(i);
      }
      reader.close();

      writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
          .setMaxBufferedDocs(2)
          .setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH)
          .setMergeScheduler(new SerialMergeScheduler())
          .setMergePolicy(new LogDocMergePolicy()));

      Directory[] indexDirs = {new MockDirectoryWrapper(random(), new RAMDirectory(dir, newIOContext(random())))};
      writer.addIndexes(indexDirs);
      writer.forceMerge(1);
      writer.close();
    }
    dir.close();
  }

  // LUCENE-1168
  public void testTermVectorCorruption2() throws IOException {
    Directory dir = newDirectory();
    for(int iter=0;iter<2;iter++) {
      IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
          .setMaxBufferedDocs(2)
          .setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH)
          .setMergeScheduler(new SerialMergeScheduler())
          .setMergePolicy(new LogDocMergePolicy()));

      FieldTypes fieldTypes = writer.getFieldTypes();
      fieldTypes.enableTermVectors("termVector");
      fieldTypes.enableTermVectorOffsets("termVector");
      fieldTypes.enableTermVectorPositions("termVector");

      Document document = writer.newDocument();
      document.addStoredString("stored", "stored");
      writer.addDocument(document);
      writer.addDocument(document);

      document = writer.newDocument();
      document.addStoredString("stored", "stored");
      document.addLargeText("termVector", "termVector");
      writer.addDocument(document);
      writer.forceMerge(1);
      writer.close();

      IndexReader reader = DirectoryReader.open(dir);
      assertNull(reader.getTermVectors(0));
      assertNull(reader.getTermVectors(1));
      assertNotNull(reader.getTermVectors(2));
      reader.close();
    }
    dir.close();
  }

  // LUCENE-1168
  public void testTermVectorCorruption3() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
        .setMaxBufferedDocs(2)
        .setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH)
        .setMergeScheduler(new SerialMergeScheduler())
        .setMergePolicy(new LogDocMergePolicy()));
    FieldTypes fieldTypes = writer.getFieldTypes();
    fieldTypes.enableTermVectors("termVector");
    fieldTypes.enableTermVectorOffsets("termVector");
    fieldTypes.enableTermVectorPositions("termVector");

    Document document = writer.newDocument();
    document.addStoredString("stored", "stored");
    document.addLargeText("termVector", "termVector");
    for(int i=0;i<10;i++) {
      writer.addDocument(document);
    }
    writer.close();

    writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
        .setMaxBufferedDocs(2)
        .setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH)
        .setMergeScheduler(new SerialMergeScheduler())
        .setMergePolicy(new LogDocMergePolicy()));
    document = writer.newDocument();
    document.addStoredString("stored", "stored");
    document.addLargeText("termVector", "termVector");
    for(int i=0;i<6;i++) {
      writer.addDocument(document);
    }
    writer.forceMerge(1);
    writer.close();

    IndexReader reader = DirectoryReader.open(dir);
    for(int i=0;i<10;i++) {
      reader.getTermVectors(i);
      reader.document(i);
    }
    reader.close();
    dir.close();
  }
  
  // LUCENE-5611: don't abort segment when term vector settings are wrong
  public void testNoAbortOnBadTVSettings() throws Exception {
    Directory dir = newDirectory();
    // Don't use RandomIndexWriter because we want to be sure both docs go to 1 seg:
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter iw = new IndexWriter(dir, iwc);
    FieldTypes fieldTypes = iw.getFieldTypes();
    fieldTypes.enableTermVectors("field");

    Document doc = iw.newDocument();
    iw.addDocument(doc);
    try {
      doc.addStoredString("field", "value");
      fail("should have hit exc");
    } catch (IllegalStateException ise) {
      assertEquals("field \"field\": cannot enable term vectors when indexOptions is NONE", ise.getMessage());
    }
    IndexReader r = DirectoryReader.open(iw, true);

    // Make sure the exc didn't lose our first document:
    assertEquals(1, r.numDocs());
    iw.close();
    r.close();
    dir.close();
  }

}
