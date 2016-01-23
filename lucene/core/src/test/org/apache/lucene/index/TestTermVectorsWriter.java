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
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

/** tests for writing term vectors */
public class TestTermVectorsWriter extends LuceneTestCase {
  // LUCENE-1442
  public void testDoubleOffsetCounting() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    Document doc = new Document();
    FieldType customType = new FieldType(StringField.TYPE_NOT_STORED);
    customType.setStoreTermVectors(true);
    customType.setStoreTermVectorPositions(true);
    customType.setStoreTermVectorOffsets(true);
    Field f = newField("field", "abcd", customType);
    doc.add(f);
    doc.add(f);
    Field f2 = newField("field", "", customType);
    doc.add(f2);
    doc.add(f);
    w.addDocument(doc);
    w.close();

    IndexReader r = DirectoryReader.open(dir);
    Terms vector = r.getTermVectors(0).terms("field");
    assertNotNull(vector);
    TermsEnum termsEnum = vector.iterator();
    assertNotNull(termsEnum.next());
    assertEquals("", termsEnum.term().utf8ToString());

    // Token "" occurred once
    assertEquals(1, termsEnum.totalTermFreq());

    PostingsEnum dpEnum = termsEnum.postings(null, PostingsEnum.ALL);
    assertTrue(dpEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    dpEnum.nextPosition();
    assertEquals(8, dpEnum.startOffset());
    assertEquals(8, dpEnum.endOffset());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, dpEnum.nextDoc());

    // Token "abcd" occurred three times
    assertEquals(new BytesRef("abcd"), termsEnum.next());
    dpEnum = termsEnum.postings(dpEnum, PostingsEnum.ALL);
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
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    Document doc = new Document();
    FieldType customType = new FieldType(TextField.TYPE_NOT_STORED);
    customType.setStoreTermVectors(true);
    customType.setStoreTermVectorPositions(true);
    customType.setStoreTermVectorOffsets(true);
    Field f = newField("field", "abcd", customType);
    doc.add(f);
    doc.add(f);
    w.addDocument(doc);
    w.close();

    IndexReader r = DirectoryReader.open(dir);
    TermsEnum termsEnum = r.getTermVectors(0).terms("field").iterator();
    assertNotNull(termsEnum.next());
    PostingsEnum dpEnum = termsEnum.postings(null, PostingsEnum.ALL);
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
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    Document doc = new Document();
    FieldType customType = new FieldType(TextField.TYPE_NOT_STORED);
    customType.setStoreTermVectors(true);
    customType.setStoreTermVectorPositions(true);
    customType.setStoreTermVectorOffsets(true);
    Field f = newField("field", "abcd   ", customType);
    doc.add(f);
    doc.add(f);
    w.addDocument(doc);
    w.close();

    IndexReader r = DirectoryReader.open(dir);
    TermsEnum termsEnum = r.getTermVectors(0).terms("field").iterator();
    assertNotNull(termsEnum.next());
    PostingsEnum dpEnum = termsEnum.postings(null, PostingsEnum.ALL);
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
    Document doc = new Document();
    try (TokenStream stream = new CachingTokenFilter(analyzer.tokenStream("field", "abcd   "))) {
      FieldType customType = new FieldType(TextField.TYPE_NOT_STORED);
      customType.setStoreTermVectors(true);
      customType.setStoreTermVectorPositions(true);
      customType.setStoreTermVectorOffsets(true);
      Field f = new Field("field", stream, customType);
      doc.add(f);
      doc.add(f);
      w.addDocument(doc);
    }
    w.close();

    IndexReader r = DirectoryReader.open(dir);
    TermsEnum termsEnum = r.getTermVectors(0).terms("field").iterator();
    assertNotNull(termsEnum.next());
    PostingsEnum dpEnum = termsEnum.postings(null, PostingsEnum.ALL);
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
    Document doc = new Document();
    FieldType customType = new FieldType(TextField.TYPE_NOT_STORED);
    customType.setStoreTermVectors(true);
    customType.setStoreTermVectorPositions(true);
    customType.setStoreTermVectorOffsets(true);
    Field f = newField("field", "abcd the", customType);
    doc.add(f);
    doc.add(f);
    w.addDocument(doc);
    w.close();

    IndexReader r = DirectoryReader.open(dir);
    TermsEnum termsEnum = r.getTermVectors(0).terms("field").iterator();
    assertNotNull(termsEnum.next());
    PostingsEnum dpEnum = termsEnum.postings(null, PostingsEnum.ALL);
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
    Document doc = new Document();
    FieldType customType = new FieldType(TextField.TYPE_NOT_STORED);
    customType.setStoreTermVectors(true);
    customType.setStoreTermVectorPositions(true);
    customType.setStoreTermVectorOffsets(true);
    Field f = newField("field", "abcd the  ", customType);
    Field f2 = newField("field", "crunch man", customType);
    doc.add(f);
    doc.add(f2);
    w.addDocument(doc);
    w.close();

    IndexReader r = DirectoryReader.open(dir);
    TermsEnum termsEnum = r.getTermVectors(0).terms("field").iterator();
    assertNotNull(termsEnum.next());
    PostingsEnum dpEnum = termsEnum.postings(null, PostingsEnum.ALL);

    assertTrue(dpEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    dpEnum.nextPosition();
    assertEquals(0, dpEnum.startOffset());
    assertEquals(4, dpEnum.endOffset());

    assertNotNull(termsEnum.next());
    dpEnum = termsEnum.postings(dpEnum, PostingsEnum.ALL);
    assertTrue(dpEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    dpEnum.nextPosition();
    assertEquals(11, dpEnum.startOffset());
    assertEquals(17, dpEnum.endOffset());

    assertNotNull(termsEnum.next());
    dpEnum = termsEnum.postings(dpEnum, PostingsEnum.ALL);
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
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    Document doc = new Document();
    FieldType customType = new FieldType(TextField.TYPE_NOT_STORED);
    customType.setStoreTermVectors(true);
    customType.setStoreTermVectorPositions(true);
    customType.setStoreTermVectorOffsets(true);
    Field f = newField("field", "", customType);
    Field f2 = newField("field", "crunch man", customType);
    doc.add(f);
    doc.add(f2);
    w.addDocument(doc);
    w.close();

    IndexReader r = DirectoryReader.open(dir);
    TermsEnum termsEnum = r.getTermVectors(0).terms("field").iterator();
    assertNotNull(termsEnum.next());
    PostingsEnum dpEnum = termsEnum.postings(null, PostingsEnum.ALL);

    assertEquals(1, (int) termsEnum.totalTermFreq());
    assertTrue(dpEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    dpEnum.nextPosition();
    assertEquals(1, dpEnum.startOffset());
    assertEquals(7, dpEnum.endOffset());

    assertNotNull(termsEnum.next());
    dpEnum = termsEnum.postings(dpEnum, PostingsEnum.ALL);
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
    Document doc = new Document();
    FieldType customType = new FieldType(TextField.TYPE_NOT_STORED);
    customType.setStoreTermVectors(true);
    customType.setStoreTermVectorPositions(true);
    customType.setStoreTermVectorOffsets(true);

    Field f = newField("field", "abcd", customType);
    doc.add(f);
    doc.add(newField("field", "", customType));

    Field f2 = newField("field", "crunch", customType);
    doc.add(f2);

    w.addDocument(doc);
    w.close();

    IndexReader r = DirectoryReader.open(dir);
    TermsEnum termsEnum = r.getTermVectors(0).terms("field").iterator();
    assertNotNull(termsEnum.next());
    PostingsEnum dpEnum = termsEnum.postings(null, PostingsEnum.ALL);

    assertEquals(1, (int) termsEnum.totalTermFreq());
    assertTrue(dpEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    dpEnum.nextPosition();
    assertEquals(0, dpEnum.startOffset());
    assertEquals(4, dpEnum.endOffset());

    assertNotNull(termsEnum.next());
    dpEnum = termsEnum.postings(dpEnum, PostingsEnum.ALL);
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

      Document document = new Document();
      FieldType customType = new FieldType();
      customType.setStored(true);

      Field storedField = newField("stored", "stored", customType);
      document.add(storedField);
      writer.addDocument(document);
      writer.addDocument(document);

      document = new Document();
      document.add(storedField);
      FieldType customType2 = new FieldType(StringField.TYPE_NOT_STORED);
      customType2.setStoreTermVectors(true);
      customType2.setStoreTermVectorPositions(true);
      customType2.setStoreTermVectorOffsets(true);
      Field termVectorField = newField("termVector", "termVector", customType2);

      document.add(termVectorField);
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

      Directory[] indexDirs = {new MockDirectoryWrapper(random(), TestUtil.ramCopyOf(dir))};
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

      Document document = new Document();

      FieldType customType = new FieldType();
      customType.setStored(true);

      Field storedField = newField("stored", "stored", customType);
      document.add(storedField);
      writer.addDocument(document);
      writer.addDocument(document);

      document = new Document();
      document.add(storedField);
      FieldType customType2 = new FieldType(StringField.TYPE_NOT_STORED);
      customType2.setStoreTermVectors(true);
      customType2.setStoreTermVectorPositions(true);
      customType2.setStoreTermVectorOffsets(true);
      Field termVectorField = newField("termVector", "termVector", customType2);
      document.add(termVectorField);
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

    Document document = new Document();
    FieldType customType = new FieldType();
    customType.setStored(true);

    Field storedField = newField("stored", "stored", customType);
    document.add(storedField);
    FieldType customType2 = new FieldType(StringField.TYPE_NOT_STORED);
    customType2.setStoreTermVectors(true);
    customType2.setStoreTermVectorPositions(true);
    customType2.setStoreTermVectorOffsets(true);
    Field termVectorField = newField("termVector", "termVector", customType2);
    document.add(termVectorField);
    for(int i=0;i<10;i++)
      writer.addDocument(document);
    writer.close();

    writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
        .setMaxBufferedDocs(2)
        .setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH)
        .setMergeScheduler(new SerialMergeScheduler())
        .setMergePolicy(new LogDocMergePolicy()));
    for(int i=0;i<6;i++)
      writer.addDocument(document);

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
  
  // LUCENE-1008
  public void testNoTermVectorAfterTermVector() throws IOException {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    Document document = new Document();
    FieldType customType2 = new FieldType(StringField.TYPE_NOT_STORED);
    customType2.setStoreTermVectors(true);
    customType2.setStoreTermVectorPositions(true);
    customType2.setStoreTermVectorOffsets(true);
    document.add(newField("tvtest", "a b c", customType2));
    iw.addDocument(document);
    document = new Document();
    document.add(newTextField("tvtest", "x y z", Field.Store.NO));
    iw.addDocument(document);
    // Make first segment
    iw.commit();

    FieldType customType = new FieldType(StringField.TYPE_NOT_STORED);
    customType.setStoreTermVectors(true);
    document = new Document();
    document.add(newField("tvtest", "a b c", customType));
    iw.addDocument(document);
    // Make 2nd segment
    iw.commit();

    iw.forceMerge(1);
    iw.close();
    dir.close();
  }

  // LUCENE-1010
  public void testNoTermVectorAfterTermVectorMerge() throws IOException {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    Document document = new Document();
    FieldType customType = new FieldType(StringField.TYPE_NOT_STORED);
    customType.setStoreTermVectors(true);
    document.add(newField("tvtest", "a b c", customType));
    iw.addDocument(document);
    iw.commit();

    document = new Document();
    document.add(newTextField("tvtest", "x y z", Field.Store.NO));
    iw.addDocument(document);
    // Make first segment
    iw.commit();

    iw.forceMerge(1);

    FieldType customType2 = new FieldType(StringField.TYPE_NOT_STORED);
    customType2.setStoreTermVectors(true);
    document.add(newField("tvtest", "a b c", customType2));
    document = new Document();
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
    FieldType a, b;
    
    // no vectors + vectors
    a = new FieldType(TextField.TYPE_NOT_STORED);   
    b = new FieldType(TextField.TYPE_NOT_STORED);
    b.setStoreTermVectors(true);
    doTestMixup(a, b);
    
    // vectors + vectors with pos
    a = new FieldType(TextField.TYPE_NOT_STORED);   
    a.setStoreTermVectors(true);
    b = new FieldType(TextField.TYPE_NOT_STORED);
    b.setStoreTermVectors(true);
    b.setStoreTermVectorPositions(true);
    doTestMixup(a, b);
    
    // vectors + vectors with off
    a = new FieldType(TextField.TYPE_NOT_STORED);   
    a.setStoreTermVectors(true);
    b = new FieldType(TextField.TYPE_NOT_STORED);
    b.setStoreTermVectors(true);
    b.setStoreTermVectorOffsets(true);
    doTestMixup(a, b);
    
    // vectors with pos + vectors with pos + off
    a = new FieldType(TextField.TYPE_NOT_STORED);   
    a.setStoreTermVectors(true);
    a.setStoreTermVectorPositions(true);
    b = new FieldType(TextField.TYPE_NOT_STORED);
    b.setStoreTermVectors(true);
    b.setStoreTermVectorPositions(true);
    b.setStoreTermVectorOffsets(true);
    doTestMixup(a, b);
    
    // vectors with pos + vectors with pos + pay
    a = new FieldType(TextField.TYPE_NOT_STORED);   
    a.setStoreTermVectors(true);
    a.setStoreTermVectorPositions(true);
    b = new FieldType(TextField.TYPE_NOT_STORED);
    b.setStoreTermVectors(true);
    b.setStoreTermVectorPositions(true);
    b.setStoreTermVectorPayloads(true);
    doTestMixup(a, b);
  }
  
  private void doTestMixup(FieldType ft1, FieldType ft2) throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    
    // add 3 good docs
    for (int i = 0; i < 3; i++) {
      Document doc = new Document();
      doc.add(new StringField("id", Integer.toString(i), Field.Store.NO));
      iw.addDocument(doc);
    }
    
    // add broken doc
    Document doc = new Document();
    doc.add(new Field("field", "value1", ft1));
    doc.add(new Field("field", "value2", ft2));
    
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
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter iw = new IndexWriter(dir, iwc);

    Document doc = new Document();
    iw.addDocument(doc);
    FieldType ft = new FieldType(StoredField.TYPE);
    ft.setStoreTermVectors(true);
    ft.freeze();
    doc.add(new Field("field", "value", ft));
    try {
      iw.addDocument(doc);
      fail("should have hit exc");
    } catch (IllegalArgumentException iae) {
      // expected
    }
    IndexReader r = DirectoryReader.open(iw);

    // Make sure the exc didn't lose our first document:
    assertEquals(1, r.numDocs());
    iw.close();
    r.close();
    dir.close();
  }
}
