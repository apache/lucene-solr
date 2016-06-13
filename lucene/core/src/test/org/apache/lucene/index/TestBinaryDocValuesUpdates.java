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
package org.apache.lucene.index;

import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.asserting.AssertingCodec;
import org.apache.lucene.codecs.asserting.AssertingDocValuesFormat;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NRTCachingDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.junit.Test;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;


public class TestBinaryDocValuesUpdates extends LuceneTestCase {

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
  
  // encodes a long into a BytesRef as VLong so that we get varying number of bytes when we update
  static BytesRef toBytes(long value) {
//    long orig = value;
    BytesRef bytes = new BytesRef(10); // negative longs may take 10 bytes
    while ((value & ~0x7FL) != 0L) {
      bytes.bytes[bytes.length++] = (byte) ((value & 0x7FL) | 0x80L);
      value >>>= 7;
    }
    bytes.bytes[bytes.length++] = (byte) value;
//    System.err.println("[" + Thread.currentThread().getName() + "] value=" + orig + ", bytes=" + bytes);
    return bytes;
  }
  
  private Document doc(int id) {
    Document doc = new Document();
    doc.add(new StringField("id", "doc-" + id, Store.NO));
    doc.add(new BinaryDocValuesField("val", toBytes(id + 1)));
    return doc;
  }
  
  public void testUpdatesAreFlushed() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false))
                                                .setRAMBufferSizeMB(0.00000001));
    writer.addDocument(doc(0)); // val=1
    writer.addDocument(doc(1)); // val=2
    writer.addDocument(doc(3)); // val=2
    writer.commit();
    assertEquals(1, writer.getFlushDeletesCount());
    writer.updateBinaryDocValue(new Term("id", "doc-0"), "val", toBytes(5));
    assertEquals(2, writer.getFlushDeletesCount());
    writer.updateBinaryDocValue(new Term("id", "doc-1"), "val", toBytes(6));
    assertEquals(3, writer.getFlushDeletesCount());
    writer.updateBinaryDocValue(new Term("id", "doc-2"), "val", toBytes(7)); 
    assertEquals(4, writer.getFlushDeletesCount());
    writer.getConfig().setRAMBufferSizeMB(1000d);
    writer.updateBinaryDocValue(new Term("id", "doc-2"), "val", toBytes(7));
    assertEquals(4, writer.getFlushDeletesCount());
    writer.close();
    dir.close();
  }
  
  public void testSimple() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    // make sure random config doesn't flush on us
    conf.setMaxBufferedDocs(10);
    conf.setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
    IndexWriter writer = new IndexWriter(dir, conf);
    writer.addDocument(doc(0)); // val=1
    writer.addDocument(doc(1)); // val=2
    if (random().nextBoolean()) { // randomly commit before the update is sent
      writer.commit();
    }
    writer.updateBinaryDocValue(new Term("id", "doc-0"), "val", toBytes(2)); // doc=0, exp=2
    
    final DirectoryReader reader;
    if (random().nextBoolean()) { // not NRT
      writer.close();
      reader = DirectoryReader.open(dir);
    } else { // NRT
      reader = DirectoryReader.open(writer);
      writer.close();
    }
    
    assertEquals(1, reader.leaves().size());
    LeafReader r = reader.leaves().get(0).reader();
    BinaryDocValues bdv = r.getBinaryDocValues("val");
    assertEquals(2, getValue(bdv, 0));
    assertEquals(2, getValue(bdv, 1));
    reader.close();
    
    dir.close();
  }
  
  public void testUpdateFewSegments() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    conf.setMaxBufferedDocs(2); // generate few segments
    conf.setMergePolicy(NoMergePolicy.INSTANCE); // prevent merges for this test
    IndexWriter writer = new IndexWriter(dir, conf);
    int numDocs = 10;
    long[] expectedValues = new long[numDocs];
    for (int i = 0; i < numDocs; i++) {
      writer.addDocument(doc(i));
      expectedValues[i] = i + 1;
    }
    writer.commit();
    
    // update few docs
    for (int i = 0; i < numDocs; i++) {
      if (random().nextDouble() < 0.4) {
        long value = (i + 1) * 2;
        writer.updateBinaryDocValue(new Term("id", "doc-" + i), "val", toBytes(value));
        expectedValues[i] = value;
      }
    }
    
    final DirectoryReader reader;
    if (random().nextBoolean()) { // not NRT
      writer.close();
      reader = DirectoryReader.open(dir);
    } else { // NRT
      reader = DirectoryReader.open(writer);
      writer.close();
    }
    
    for (LeafReaderContext context : reader.leaves()) {
      LeafReader r = context.reader();
      BinaryDocValues bdv = r.getBinaryDocValues("val");
      assertNotNull(bdv);
      for (int i = 0; i < r.maxDoc(); i++) {
        long expected = expectedValues[i + context.docBase];
        long actual = getValue(bdv, i);
        assertEquals(expected, actual);
      }
    }
    
    reader.close();
    dir.close();
  }
  
  public void testReopen() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir, conf);
    writer.addDocument(doc(0));
    writer.addDocument(doc(1));
    
    final boolean isNRT = random().nextBoolean();
    final DirectoryReader reader1;
    if (isNRT) {
      reader1 = DirectoryReader.open(writer);
    } else {
      writer.commit();
      reader1 = DirectoryReader.open(dir);
    }

    // update doc
    writer.updateBinaryDocValue(new Term("id", "doc-0"), "val", toBytes(10)); // update doc-0's value to 10
    if (!isNRT) {
      writer.commit();
    }
    
    // reopen reader and assert only it sees the update
    final DirectoryReader reader2 = DirectoryReader.openIfChanged(reader1);
    assertNotNull(reader2);
    assertTrue(reader1 != reader2);

    BinaryDocValues bdv1 = reader1.leaves().get(0).reader().getBinaryDocValues("val");
    BinaryDocValues bdv2 = reader2.leaves().get(0).reader().getBinaryDocValues("val");
    assertEquals(1, getValue(bdv1, 0));
    assertEquals(10, getValue(bdv2, 0));

    writer.close();
    IOUtils.close(reader1, reader2, dir);
  }
  
  public void testUpdatesAndDeletes() throws Exception {
    // create an index with a segment with only deletes, a segment with both
    // deletes and updates and a segment with only updates
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    conf.setMaxBufferedDocs(10); // control segment flushing
    conf.setMergePolicy(NoMergePolicy.INSTANCE); // prevent merges for this test
    IndexWriter writer = new IndexWriter(dir, conf);
    
    for (int i = 0; i < 6; i++) {
      writer.addDocument(doc(i));
      if (i % 2 == 1) {
        writer.commit(); // create 2-docs segments
      }
    }
    
    // delete doc-1 and doc-2
    writer.deleteDocuments(new Term("id", "doc-1"), new Term("id", "doc-2")); // 1st and 2nd segments
    
    // update docs 3 and 5
    writer.updateBinaryDocValue(new Term("id", "doc-3"), "val", toBytes(17L));
    writer.updateBinaryDocValue(new Term("id", "doc-5"), "val", toBytes(17L));
    
    final DirectoryReader reader;
    if (random().nextBoolean()) { // not NRT
      writer.close();
      reader = DirectoryReader.open(dir);
    } else { // NRT
      reader = DirectoryReader.open(writer);
      writer.close();
    }
    
    Bits liveDocs = MultiFields.getLiveDocs(reader);
    boolean[] expectedLiveDocs = new boolean[] { true, false, false, true, true, true };
    for (int i = 0; i < expectedLiveDocs.length; i++) {
      assertEquals(expectedLiveDocs[i], liveDocs.get(i));
    }
    
    long[] expectedValues = new long[] { 1, 2, 3, 17, 5, 17};
    BinaryDocValues bdv = MultiDocValues.getBinaryValues(reader, "val");
    for (int i = 0; i < expectedValues.length; i++) {
      assertEquals(expectedValues[i], getValue(bdv, i));
    }
    
    reader.close();
    dir.close();
  }
  
  public void testUpdatesWithDeletes() throws Exception {
    // update and delete different documents in the same commit session
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    conf.setMaxBufferedDocs(10); // control segment flushing
    IndexWriter writer = new IndexWriter(dir, conf);
    
    writer.addDocument(doc(0));
    writer.addDocument(doc(1));
    
    if (random().nextBoolean()) {
      writer.commit();
    }
    
    writer.deleteDocuments(new Term("id", "doc-0"));
    writer.updateBinaryDocValue(new Term("id", "doc-1"), "val", toBytes(17L));
    
    final DirectoryReader reader;
    if (random().nextBoolean()) { // not NRT
      writer.close();
      reader = DirectoryReader.open(dir);
    } else { // NRT
      reader = DirectoryReader.open(writer);
      writer.close();
    }
    
    LeafReader r = reader.leaves().get(0).reader();
    assertFalse(r.getLiveDocs().get(0));
    assertEquals(17, getValue(r.getBinaryDocValues("val"), 1));
    
    reader.close();
    dir.close();
  }
  
  public void testMultipleDocValuesTypes() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    conf.setMaxBufferedDocs(10); // prevent merges
    IndexWriter writer = new IndexWriter(dir, conf);
    
    for (int i = 0; i < 4; i++) {
      Document doc = new Document();
      doc.add(new StringField("dvUpdateKey", "dv", Store.NO));
      doc.add(new NumericDocValuesField("ndv", i));
      doc.add(new BinaryDocValuesField("bdv", new BytesRef(Integer.toString(i))));
      doc.add(new SortedDocValuesField("sdv", new BytesRef(Integer.toString(i))));
      doc.add(new SortedSetDocValuesField("ssdv", new BytesRef(Integer.toString(i))));
      doc.add(new SortedSetDocValuesField("ssdv", new BytesRef(Integer.toString(i * 2))));
      writer.addDocument(doc);
    }
    writer.commit();
    
    // update all docs' bdv field
    writer.updateBinaryDocValue(new Term("dvUpdateKey", "dv"), "bdv", toBytes(17L));
    writer.close();
    
    final DirectoryReader reader = DirectoryReader.open(dir);
    LeafReader r = reader.leaves().get(0).reader();
    NumericDocValues ndv = r.getNumericDocValues("ndv");
    BinaryDocValues bdv = r.getBinaryDocValues("bdv");
    SortedDocValues sdv = r.getSortedDocValues("sdv");
    SortedSetDocValues ssdv = r.getSortedSetDocValues("ssdv");
    for (int i = 0; i < r.maxDoc(); i++) {
      assertEquals(i, ndv.get(i));
      assertEquals(17, getValue(bdv, i));
      BytesRef term = sdv.get(i);
      assertEquals(new BytesRef(Integer.toString(i)), term);
      ssdv.setDocument(i);
      long ord = ssdv.nextOrd();
      term = ssdv.lookupOrd(ord);
      assertEquals(i, Integer.parseInt(term.utf8ToString()));
      if (i != 0) {
        ord = ssdv.nextOrd();
        term = ssdv.lookupOrd(ord);
        assertEquals(i * 2, Integer.parseInt(term.utf8ToString()));
      }
      assertEquals(SortedSetDocValues.NO_MORE_ORDS, ssdv.nextOrd());
    }
    
    reader.close();
    dir.close();
  }
  
  public void testMultipleBinaryDocValues() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    conf.setMaxBufferedDocs(10); // prevent merges
    IndexWriter writer = new IndexWriter(dir, conf);
    
    for (int i = 0; i < 2; i++) {
      Document doc = new Document();
      doc.add(new StringField("dvUpdateKey", "dv", Store.NO));
      doc.add(new BinaryDocValuesField("bdv1", toBytes(i)));
      doc.add(new BinaryDocValuesField("bdv2", toBytes(i)));
      writer.addDocument(doc);
    }
    writer.commit();
    
    // update all docs' bdv1 field
    writer.updateBinaryDocValue(new Term("dvUpdateKey", "dv"), "bdv1", toBytes(17L));
    writer.close();
    
    final DirectoryReader reader = DirectoryReader.open(dir);
    LeafReader r = reader.leaves().get(0).reader();
    
    BinaryDocValues bdv1 = r.getBinaryDocValues("bdv1");
    BinaryDocValues bdv2 = r.getBinaryDocValues("bdv2");
    for (int i = 0; i < r.maxDoc(); i++) {
      assertEquals(17, getValue(bdv1, i));
      assertEquals(i, getValue(bdv2, i));
    }
    
    reader.close();
    dir.close();
  }
  
  public void testDocumentWithNoValue() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir, conf);
    
    for (int i = 0; i < 2; i++) {
      Document doc = new Document();
      doc.add(new StringField("dvUpdateKey", "dv", Store.NO));
      if (i == 0) { // index only one document with value
        doc.add(new BinaryDocValuesField("bdv", toBytes(5L)));
      }
      writer.addDocument(doc);
    }
    writer.commit();
    
    // update all docs' bdv field
    writer.updateBinaryDocValue(new Term("dvUpdateKey", "dv"), "bdv", toBytes(17L));
    writer.close();
    
    final DirectoryReader reader = DirectoryReader.open(dir);
    LeafReader r = reader.leaves().get(0).reader();
    BinaryDocValues bdv = r.getBinaryDocValues("bdv");
    for (int i = 0; i < r.maxDoc(); i++) {
      assertEquals(17, getValue(bdv, i));
    }
    
    reader.close();
    dir.close();
  }
  
  public void testUpdateNonBinaryDocValuesField() throws Exception {
    // we don't support adding new fields or updating existing non-binary-dv
    // fields through binary updates
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir, conf);
    
    Document doc = new Document();
    doc.add(new StringField("key", "doc", Store.NO));
    doc.add(new StringField("foo", "bar", Store.NO));
    writer.addDocument(doc); // flushed document
    writer.commit();
    writer.addDocument(doc); // in-memory document
    
    expectThrows(IllegalArgumentException.class, () -> {
      writer.updateBinaryDocValue(new Term("key", "doc"), "bdv", toBytes(17L));
    });
    
    expectThrows(IllegalArgumentException.class, () -> {
      writer.updateBinaryDocValue(new Term("key", "doc"), "foo", toBytes(17L));
    });
    
    writer.close();
    dir.close();
  }
  
  public void testDifferentDVFormatPerField() throws Exception {
    // test relies on separate instances of "same thing"
    assert TestUtil.getDefaultDocValuesFormat() != TestUtil.getDefaultDocValuesFormat();
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    conf.setCodec(new AssertingCodec() {
      @Override
      public DocValuesFormat getDocValuesFormatForField(String field) {
        return TestUtil.getDefaultDocValuesFormat();
      }
    });
    IndexWriter writer = new IndexWriter(dir, conf);
    
    Document doc = new Document();
    doc.add(new StringField("key", "doc", Store.NO));
    doc.add(new BinaryDocValuesField("bdv", toBytes(5L)));
    doc.add(new SortedDocValuesField("sorted", new BytesRef("value")));
    writer.addDocument(doc); // flushed document
    writer.commit();
    writer.addDocument(doc); // in-memory document
    
    writer.updateBinaryDocValue(new Term("key", "doc"), "bdv", toBytes(17L));
    writer.close();
    
    final DirectoryReader reader = DirectoryReader.open(dir);
    
    BinaryDocValues bdv = MultiDocValues.getBinaryValues(reader, "bdv");
    SortedDocValues sdv = MultiDocValues.getSortedValues(reader, "sorted");
    for (int i = 0; i < reader.maxDoc(); i++) {
      assertEquals(17, getValue(bdv, i));
      BytesRef term = sdv.get(i);
      assertEquals(new BytesRef("value"), term);
    }
    
    reader.close();
    dir.close();
  }
  
  public void testUpdateSameDocMultipleTimes() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir, conf);
    
    Document doc = new Document();
    doc.add(new StringField("key", "doc", Store.NO));
    doc.add(new BinaryDocValuesField("bdv", toBytes(5L)));
    writer.addDocument(doc); // flushed document
    writer.commit();
    writer.addDocument(doc); // in-memory document
    
    writer.updateBinaryDocValue(new Term("key", "doc"), "bdv", toBytes(17L)); // update existing field
    writer.updateBinaryDocValue(new Term("key", "doc"), "bdv", toBytes(3L)); // update existing field 2nd time in this commit
    writer.close();
    
    final DirectoryReader reader = DirectoryReader.open(dir);
    BinaryDocValues bdv = MultiDocValues.getBinaryValues(reader, "bdv");
    for (int i = 0; i < reader.maxDoc(); i++) {
      assertEquals(3, getValue(bdv, i));
    }
    reader.close();
    dir.close();
  }
  
  public void testSegmentMerges() throws Exception {
    Directory dir = newDirectory();
    Random random = random();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random));
    IndexWriter writer = new IndexWriter(dir, conf);
    
    int docid = 0;
    int numRounds = atLeast(10);
    for (int rnd = 0; rnd < numRounds; rnd++) {
      Document doc = new Document();
      doc.add(new StringField("key", "doc", Store.NO));
      doc.add(new BinaryDocValuesField("bdv", toBytes(-1)));
      int numDocs = atLeast(30);
      for (int i = 0; i < numDocs; i++) {
        doc.removeField("id");
        doc.add(new StringField("id", Integer.toString(docid++), Store.NO));
        writer.addDocument(doc);
      }
      
      long value = rnd + 1;
      writer.updateBinaryDocValue(new Term("key", "doc"), "bdv", toBytes(value));
      
      if (random.nextDouble() < 0.2) { // randomly delete some docs
        writer.deleteDocuments(new Term("id", Integer.toString(random.nextInt(docid))));
      }
      
      // randomly commit or reopen-IW (or nothing), before forceMerge
      if (random.nextDouble() < 0.4) {
        writer.commit();
      } else if (random.nextDouble() < 0.1) {
        writer.close();
        conf = newIndexWriterConfig(new MockAnalyzer(random));
        writer = new IndexWriter(dir, conf);
      }

      // add another document with the current value, to be sure forceMerge has
      // something to merge (for instance, it could be that CMS finished merging
      // all segments down to 1 before the delete was applied, so when
      // forceMerge is called, the index will be with one segment and deletes
      // and some MPs might now merge it, thereby invalidating test's
      // assumption that the reader has no deletes).
      doc = new Document();
      doc.add(new StringField("id", Integer.toString(docid++), Store.NO));
      doc.add(new StringField("key", "doc", Store.NO));
      doc.add(new BinaryDocValuesField("bdv", toBytes(value)));
      writer.addDocument(doc);

      writer.forceMerge(1, true);
      final DirectoryReader reader;
      if (random.nextBoolean()) {
        writer.commit();
        reader = DirectoryReader.open(dir);
      } else {
        reader = DirectoryReader.open(writer);
      }
      
      assertEquals(1, reader.leaves().size());
      final LeafReader r = reader.leaves().get(0).reader();
      assertNull("index should have no deletes after forceMerge", r.getLiveDocs());
      BinaryDocValues bdv = r.getBinaryDocValues("bdv");
      assertNotNull(bdv);
      for (int i = 0; i < r.maxDoc(); i++) {
        assertEquals(value, getValue(bdv, i));
      }
      reader.close();
    }
    
    writer.close();
    dir.close();
  }
  
  public void testUpdateDocumentByMultipleTerms() throws Exception {
    // make sure the order of updates is respected, even when multiple terms affect same document
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir, conf);
    
    Document doc = new Document();
    doc.add(new StringField("k1", "v1", Store.NO));
    doc.add(new StringField("k2", "v2", Store.NO));
    doc.add(new BinaryDocValuesField("bdv", toBytes(5L)));
    writer.addDocument(doc); // flushed document
    writer.commit();
    writer.addDocument(doc); // in-memory document
    
    writer.updateBinaryDocValue(new Term("k1", "v1"), "bdv", toBytes(17L));
    writer.updateBinaryDocValue(new Term("k2", "v2"), "bdv", toBytes(3L));
    writer.close();
    
    final DirectoryReader reader = DirectoryReader.open(dir);
    BinaryDocValues bdv = MultiDocValues.getBinaryValues(reader, "bdv");
    for (int i = 0; i < reader.maxDoc(); i++) {
      assertEquals(3, getValue(bdv, i));
    }
    reader.close();
    dir.close();
  }

  public void testManyReopensAndFields() throws Exception {
    Directory dir = newDirectory();
    final Random random = random();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random));
    LogMergePolicy lmp = newLogMergePolicy();
    lmp.setMergeFactor(3); // merge often
    conf.setMergePolicy(lmp);
    IndexWriter writer = new IndexWriter(dir, conf);
    
    final boolean isNRT = random.nextBoolean();
    DirectoryReader reader;
    if (isNRT) {
      reader = DirectoryReader.open(writer);
    } else {
      writer.commit();
      reader = DirectoryReader.open(dir);
    }
    //System.out.println("TEST: isNRT=" + isNRT);
    
    final int numFields = random.nextInt(4) + 3; // 3-7
    final long[] fieldValues = new long[numFields];
    for (int i = 0; i < fieldValues.length; i++) {
      fieldValues[i] = 1;
    }
    
    int numRounds = atLeast(15);
    int docID = 0;
    for (int i = 0; i < numRounds; i++) {
      int numDocs = atLeast(5);
      //System.out.println("[" + Thread.currentThread().getName() + "]: round=" + i + ", numDocs=" + numDocs);
      for (int j = 0; j < numDocs; j++) {
        Document doc = new Document();
        doc.add(new StringField("id", "doc-" + docID, Store.NO));
        doc.add(new StringField("key", "all", Store.NO)); // update key
        // add all fields with their current value
        for (int f = 0; f < fieldValues.length; f++) {
          doc.add(new BinaryDocValuesField("f" + f, toBytes(fieldValues[f])));
        }
        writer.addDocument(doc);
        ++docID;
      }
      
      int fieldIdx = random.nextInt(fieldValues.length);
      String updateField = "f" + fieldIdx;
//      System.out.println("[" + Thread.currentThread().getName() + "]: updated field '" + updateField + "' to value " + fieldValues[fieldIdx]);
      writer.updateBinaryDocValue(new Term("key", "all"), updateField, toBytes(++fieldValues[fieldIdx]));
      
      if (random.nextDouble() < 0.2) {
        int deleteDoc = random.nextInt(docID); // might also delete an already deleted document, ok!
        writer.deleteDocuments(new Term("id", "doc-" + deleteDoc));
//        System.out.println("[" + Thread.currentThread().getName() + "]: deleted document: doc-" + deleteDoc);
      }
      
      // verify reader
      if (!isNRT) {
        writer.commit();
      }
      
//      System.out.println("[" + Thread.currentThread().getName() + "]: reopen reader: " + reader);
      DirectoryReader newReader = DirectoryReader.openIfChanged(reader);
      assertNotNull(newReader);
      reader.close();
      reader = newReader;
//      System.out.println("[" + Thread.currentThread().getName() + "]: reopened reader: " + reader);
      assertTrue(reader.numDocs() > 0); // we delete at most one document per round
      for (LeafReaderContext context : reader.leaves()) {
        LeafReader r = context.reader();
//        System.out.println(((SegmentReader) r).getSegmentName());
        Bits liveDocs = r.getLiveDocs();
        for (int field = 0; field < fieldValues.length; field++) {
          String f = "f" + field;
          BinaryDocValues bdv = r.getBinaryDocValues(f);
          Bits docsWithField = r.getDocsWithField(f);
          assertNotNull(bdv);
          int maxDoc = r.maxDoc();
          for (int doc = 0; doc < maxDoc; doc++) {
            if (liveDocs == null || liveDocs.get(doc)) {
//              System.out.println("doc=" + (doc + context.docBase) + " f='" + f + "' vslue=" + getValue(bdv, doc, scratch));
              assertTrue(docsWithField.get(doc));
              assertEquals("invalid value for doc=" + doc + ", field=" + f + ", reader=" + r, fieldValues[field], getValue(bdv, doc));
            }
          }
        }
      }
//      System.out.println();
    }

    writer.close();
    IOUtils.close(reader, dir);
  }
  
  public void testUpdateSegmentWithNoDocValues() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    // prevent merges, otherwise by the time updates are applied
    // (writer.close()), the segments might have merged and that update becomes
    // legit.
    conf.setMergePolicy(NoMergePolicy.INSTANCE);
    IndexWriter writer = new IndexWriter(dir, conf);
    
    // first segment with BDV
    Document doc = new Document();
    doc.add(new StringField("id", "doc0", Store.NO));
    doc.add(new BinaryDocValuesField("bdv", toBytes(3L)));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new StringField("id", "doc4", Store.NO)); // document without 'bdv' field
    writer.addDocument(doc);
    writer.commit();
    
    // second segment with no BDV
    doc = new Document();
    doc.add(new StringField("id", "doc1", Store.NO));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new StringField("id", "doc2", Store.NO)); // document that isn't updated
    writer.addDocument(doc);
    writer.commit();
    
    // update document in the first segment - should not affect docsWithField of
    // the document without BDV field
    writer.updateBinaryDocValue(new Term("id", "doc0"), "bdv", toBytes(5L));
    
    // update document in the second segment - field should be added and we should
    // be able to handle the other document correctly (e.g. no NPE)
    writer.updateBinaryDocValue(new Term("id", "doc1"), "bdv", toBytes(5L));
    writer.close();

    DirectoryReader reader = DirectoryReader.open(dir);
    for (LeafReaderContext context : reader.leaves()) {
      LeafReader r = context.reader();
      BinaryDocValues bdv = r.getBinaryDocValues("bdv");
      Bits docsWithField = r.getDocsWithField("bdv");
      assertNotNull(docsWithField);
      assertTrue(docsWithField.get(0));
      assertEquals(5L, getValue(bdv, 0));
      assertFalse(docsWithField.get(1));
      BytesRef term = bdv.get(1);
      assertEquals(0, term.length);
    }
    reader.close();

    dir.close();
  }
  
  public void testUpdateSegmentWithPostingButNoDocValues() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    // prevent merges, otherwise by the time updates are applied
    // (writer.close()), the segments might have merged and that update becomes
    // legit.
    conf.setMergePolicy(NoMergePolicy.INSTANCE);
    IndexWriter writer = new IndexWriter(dir, conf);
    
    // first segment with BDV
    Document doc = new Document();
    doc.add(new StringField("id", "doc0", Store.NO));
    doc.add(new StringField("bdv", "mock-value", Store.NO));
    doc.add(new BinaryDocValuesField("bdv", toBytes(5L)));
    writer.addDocument(doc);
    writer.commit();
    
    // second segment with no BDV
    doc = new Document();
    doc.add(new StringField("id", "doc1", Store.NO));
    doc.add(new StringField("bdv", "mock-value", Store.NO));
    writer.addDocument(doc);
    writer.commit();
    
    // update document in the second segment
    writer.updateBinaryDocValue(new Term("id", "doc1"), "bdv", toBytes(5L));
    writer.close();

    DirectoryReader reader = DirectoryReader.open(dir);
    for (LeafReaderContext context : reader.leaves()) {
      LeafReader r = context.reader();
      BinaryDocValues bdv = r.getBinaryDocValues("bdv");
      for (int i = 0; i < r.maxDoc(); i++) {
        assertEquals(5L, getValue(bdv, i));
      }
    }
    reader.close();
    
    dir.close();
  }
  
  public void testUpdateBinaryDVFieldWithSameNameAsPostingField() throws Exception {
    // this used to fail because FieldInfos.Builder neglected to update
    // globalFieldMaps.docValuesTypes map
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir, conf);
    
    Document doc = new Document();
    doc.add(new StringField("f", "mock-value", Store.NO));
    doc.add(new BinaryDocValuesField("f", toBytes(5L)));
    writer.addDocument(doc);
    writer.commit();
    writer.updateBinaryDocValue(new Term("f", "mock-value"), "f", toBytes(17L));
    writer.close();
    
    DirectoryReader r = DirectoryReader.open(dir);
    BinaryDocValues bdv = r.leaves().get(0).reader().getBinaryDocValues("f");
    assertEquals(17, getValue(bdv, 0));
    r.close();
    
    dir.close();
  }
  
  public void testStressMultiThreading() throws Exception {
    final Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    final IndexWriter writer = new IndexWriter(dir, conf);
    
    // create index
    final int numFields = TestUtil.nextInt(random(), 1, 4);
    final int numDocs = atLeast(2000);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      doc.add(new StringField("id", "doc" + i, Store.NO));
      double group = random().nextDouble();
      String g;
      if (group < 0.1) g = "g0";
      else if (group < 0.5) g = "g1";
      else if (group < 0.8) g = "g2";
      else g = "g3";
      doc.add(new StringField("updKey", g, Store.NO));
      for (int j = 0; j < numFields; j++) {
        long value = random().nextInt();
        doc.add(new BinaryDocValuesField("f" + j, toBytes(value)));
        doc.add(new BinaryDocValuesField("cf" + j, toBytes(value * 2))); // control, always updated to f * 2
      }
      writer.addDocument(doc);
    }
    
    final int numThreads = TestUtil.nextInt(random(), 3, 6);
    final CountDownLatch done = new CountDownLatch(numThreads);
    final AtomicInteger numUpdates = new AtomicInteger(atLeast(100));
    
    // same thread updates a field as well as reopens
    Thread[] threads = new Thread[numThreads];
    for (int i = 0; i < threads.length; i++) {
      threads[i] = new Thread("UpdateThread-" + i) {
        @Override
        public void run() {
          DirectoryReader reader = null;
          boolean success = false;
          try {
            Random random = random();
            while (numUpdates.getAndDecrement() > 0) {
              double group = random.nextDouble();
              Term t;
              if (group < 0.1) t = new Term("updKey", "g0");
              else if (group < 0.5) t = new Term("updKey", "g1");
              else if (group < 0.8) t = new Term("updKey", "g2");
              else t = new Term("updKey", "g3");

              final int field = random().nextInt(numFields);
              final String f = "f" + field;
              final String cf = "cf" + field;
//              System.out.println("[" + Thread.currentThread().getName() + "] numUpdates=" + numUpdates + " updateTerm=" + t + " field=" + field);
              long updValue = random.nextInt();
              writer.updateDocValues(t, new BinaryDocValuesField(f, toBytes(updValue)), new BinaryDocValuesField(cf, toBytes(updValue*2)));
              
              if (random.nextDouble() < 0.2) {
                // delete a random document
                int doc = random.nextInt(numDocs);
//                System.out.println("[" + Thread.currentThread().getName() + "] deleteDoc=doc" + doc);
                writer.deleteDocuments(new Term("id", "doc" + doc));
              }
  
              if (random.nextDouble() < 0.05) { // commit every 20 updates on average
//                  System.out.println("[" + Thread.currentThread().getName() + "] commit");
                writer.commit();
              }
              
              if (random.nextDouble() < 0.1) { // reopen NRT reader (apply updates), on average once every 10 updates
                if (reader == null) {
//                  System.out.println("[" + Thread.currentThread().getName() + "] open NRT");
                  reader = DirectoryReader.open(writer);
                } else {
//                  System.out.println("[" + Thread.currentThread().getName() + "] reopen NRT");
                  DirectoryReader r2 = DirectoryReader.openIfChanged(reader, writer);
                  if (r2 != null) {
                    reader.close();
                    reader = r2;
                  }
                }
              }
            }
//            System.out.println("[" + Thread.currentThread().getName() + "] DONE");
            success = true;
          } catch (IOException e) {
            throw new RuntimeException(e);
          } finally {
            if (reader != null) {
              try {
                reader.close();
              } catch (IOException e) {
                if (success) { // suppress this exception only if there was another exception
                  throw new RuntimeException(e);
                }
              }
            }
            done.countDown();
          }
        }
      };
    }
    
    for (Thread t : threads) t.start();
    done.await();
    writer.close();
    
    DirectoryReader reader = DirectoryReader.open(dir);
    for (LeafReaderContext context : reader.leaves()) {
      LeafReader r = context.reader();
      for (int i = 0; i < numFields; i++) {
        BinaryDocValues bdv = r.getBinaryDocValues("f" + i);
        BinaryDocValues control = r.getBinaryDocValues("cf" + i);
        Bits docsWithBdv = r.getDocsWithField("f" + i);
        Bits docsWithControl = r.getDocsWithField("cf" + i);
        Bits liveDocs = r.getLiveDocs();
        for (int j = 0; j < r.maxDoc(); j++) {
          if (liveDocs == null || liveDocs.get(j)) {
            assertTrue(docsWithBdv.get(j));
            assertTrue(docsWithControl.get(j));
            assertEquals(getValue(control, j), getValue(bdv, j) * 2);
          }
        }
      }
    }
    reader.close();
    
    dir.close();
  }

  public void testUpdateDifferentDocsInDifferentGens() throws Exception {
    // update same document multiple times across generations
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    conf.setMaxBufferedDocs(4);
    IndexWriter writer = new IndexWriter(dir, conf);
    final int numDocs = atLeast(10);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      doc.add(new StringField("id", "doc" + i, Store.NO));
      long value = random().nextInt();
      doc.add(new BinaryDocValuesField("f", toBytes(value)));
      doc.add(new BinaryDocValuesField("cf", toBytes(value * 2)));
      writer.addDocument(doc);
    }
    
    int numGens = atLeast(5);
    for (int i = 0; i < numGens; i++) {
      int doc = random().nextInt(numDocs);
      Term t = new Term("id", "doc" + doc);
      long value = random().nextLong();
      writer.updateDocValues(t, new BinaryDocValuesField("f", toBytes(value)), new BinaryDocValuesField("cf", toBytes(value*2)));
      DirectoryReader reader = DirectoryReader.open(writer);
      for (LeafReaderContext context : reader.leaves()) {
        LeafReader r = context.reader();
        BinaryDocValues fbdv = r.getBinaryDocValues("f");
        BinaryDocValues cfbdv = r.getBinaryDocValues("cf");
        for (int j = 0; j < r.maxDoc(); j++) {
          assertEquals(getValue(cfbdv, j), getValue(fbdv, j) * 2);
        }
      }
      reader.close();
    }
    writer.close();
    dir.close();
  }

  public void testChangeCodec() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    conf.setMergePolicy(NoMergePolicy.INSTANCE); // disable merges to simplify test assertions.
    conf.setCodec(new AssertingCodec() {
      @Override
      public DocValuesFormat getDocValuesFormatForField(String field) {
        return TestUtil.getDefaultDocValuesFormat();
      }
    });
    IndexWriter writer = new IndexWriter(dir, conf);
    Document doc = new Document();
    doc.add(new StringField("id", "d0", Store.NO));
    doc.add(new BinaryDocValuesField("f1", toBytes(5L)));
    doc.add(new BinaryDocValuesField("f2", toBytes(13L)));
    writer.addDocument(doc);
    writer.close();
    
    // change format
    conf = newIndexWriterConfig(new MockAnalyzer(random()));
    conf.setMergePolicy(NoMergePolicy.INSTANCE); // disable merges to simplify test assertions.
    conf.setCodec(new AssertingCodec() {
      @Override
      public DocValuesFormat getDocValuesFormatForField(String field) {
        return new AssertingDocValuesFormat();
      }
    });
    writer = new IndexWriter(dir, conf);
    doc = new Document();
    doc.add(new StringField("id", "d1", Store.NO));
    doc.add(new BinaryDocValuesField("f1", toBytes(17L)));
    doc.add(new BinaryDocValuesField("f2", toBytes(2L)));
    writer.addDocument(doc);
    writer.updateBinaryDocValue(new Term("id", "d0"), "f1", toBytes(12L));
    writer.close();
    
    DirectoryReader reader = DirectoryReader.open(dir);
    BinaryDocValues f1 = MultiDocValues.getBinaryValues(reader, "f1");
    BinaryDocValues f2 = MultiDocValues.getBinaryValues(reader, "f2");
    assertEquals(12L, getValue(f1, 0));
    assertEquals(13L, getValue(f2, 0));
    assertEquals(17L, getValue(f1, 1));
    assertEquals(2L, getValue(f2, 1));
    reader.close();
    dir.close();
  }

  public void testAddIndexes() throws Exception {
    Directory dir1 = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir1, conf);
    
    final int numDocs = atLeast(50);
    final int numTerms = TestUtil.nextInt(random(), 1, numDocs / 5);
    Set<String> randomTerms = new HashSet<>();
    while (randomTerms.size() < numTerms) {
      randomTerms.add(TestUtil.randomSimpleString(random()));
    }

    // create first index
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      doc.add(new StringField("id", RandomPicks.randomFrom(random(), randomTerms), Store.NO));
      doc.add(new BinaryDocValuesField("bdv", toBytes(4L)));
      doc.add(new BinaryDocValuesField("control", toBytes(8L)));
      writer.addDocument(doc);
    }
    
    if (random().nextBoolean()) {
      writer.commit();
    }
    
    // update some docs to a random value
    long value = random().nextInt();
    Term term = new Term("id", RandomPicks.randomFrom(random(), randomTerms));
    writer.updateDocValues(term, new BinaryDocValuesField("bdv", toBytes(value)), new BinaryDocValuesField("control", toBytes(value * 2)));
    writer.close();
    
    Directory dir2 = newDirectory();
    conf = newIndexWriterConfig(new MockAnalyzer(random()));
    writer = new IndexWriter(dir2, conf);
    if (random().nextBoolean()) {
      writer.addIndexes(dir1);
    } else {
      DirectoryReader reader = DirectoryReader.open(dir1);
      TestUtil.addIndexesSlowly(writer, reader);
      reader.close();
    }
    writer.close();
    
    DirectoryReader reader = DirectoryReader.open(dir2);
    for (LeafReaderContext context : reader.leaves()) {
      LeafReader r = context.reader();
      BinaryDocValues bdv = r.getBinaryDocValues("bdv");
      BinaryDocValues control = r.getBinaryDocValues("control");
      for (int i = 0; i < r.maxDoc(); i++) {
        assertEquals(getValue(bdv, i)*2, getValue(control, i));
      }
    }
    reader.close();
    
    IOUtils.close(dir1, dir2);
  }

  public void testDeleteUnusedUpdatesFiles() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir, conf);
    
    Document doc = new Document();
    doc.add(new StringField("id", "d0", Store.NO));
    doc.add(new BinaryDocValuesField("f1", toBytes(1L)));
    doc.add(new BinaryDocValuesField("f2", toBytes(1L)));
    writer.addDocument(doc);

    // update each field twice to make sure all unneeded files are deleted
    for (String f : new String[] { "f1", "f2" }) {
      writer.updateBinaryDocValue(new Term("id", "d0"), f, toBytes(2L));
      writer.commit();
      int numFiles = dir.listAll().length;
      
      // update again, number of files shouldn't change (old field's gen is
      // removed) 
      writer.updateBinaryDocValue(new Term("id", "d0"), f, toBytes(3L));
      writer.commit();
      
      assertEquals(numFiles, dir.listAll().length);
    }

    writer.close();
    dir.close();
  }

  @Nightly
  public void testTonsOfUpdates() throws Exception {
    // LUCENE-5248: make sure that when there are many updates, we don't use too much RAM
    Directory dir = newDirectory();
    final Random random = random();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random));
    conf.setRAMBufferSizeMB(IndexWriterConfig.DEFAULT_RAM_BUFFER_SIZE_MB);
    conf.setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH); // don't flush by doc
    IndexWriter writer = new IndexWriter(dir, conf);
    
    // test data: lots of documents (few 10Ks) and lots of update terms (few hundreds)
    final int numDocs = atLeast(20000);
    final int numBinaryFields = atLeast(5);
    final int numTerms = TestUtil.nextInt(random, 10, 100); // terms should affect many docs
    Set<String> updateTerms = new HashSet<>();
    while (updateTerms.size() < numTerms) {
      updateTerms.add(TestUtil.randomSimpleString(random));
    }

//    System.out.println("numDocs=" + numDocs + " numBinaryFields=" + numBinaryFields + " numTerms=" + numTerms);
    
    // build a large index with many BDV fields and update terms
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      int numUpdateTerms = TestUtil.nextInt(random, 1, numTerms / 10);
      for (int j = 0; j < numUpdateTerms; j++) {
        doc.add(new StringField("upd", RandomPicks.randomFrom(random, updateTerms), Store.NO));
      }
      for (int j = 0; j < numBinaryFields; j++) {
        long val = random.nextInt();
        doc.add(new BinaryDocValuesField("f" + j, toBytes(val)));
        doc.add(new BinaryDocValuesField("cf" + j, toBytes(val * 2)));
      }
      writer.addDocument(doc);
    }
    
    writer.commit(); // commit so there's something to apply to
    
    // set to flush every 2048 bytes (approximately every 12 updates), so we get
    // many flushes during binary updates
    writer.getConfig().setRAMBufferSizeMB(2048.0 / 1024 / 1024);
    final int numUpdates = atLeast(100);
//    System.out.println("numUpdates=" + numUpdates);
    for (int i = 0; i < numUpdates; i++) {
      int field = random.nextInt(numBinaryFields);
      Term updateTerm = new Term("upd", RandomPicks.randomFrom(random, updateTerms));
      long value = random.nextInt();
      writer.updateDocValues(updateTerm, new BinaryDocValuesField("f" + field, toBytes(value)), 
          new BinaryDocValuesField("cf" + field, toBytes(value * 2)));
    }

    writer.close();
    
    DirectoryReader reader = DirectoryReader.open(dir);
    for (LeafReaderContext context : reader.leaves()) {
      for (int i = 0; i < numBinaryFields; i++) {
        LeafReader r = context.reader();
        BinaryDocValues f = r.getBinaryDocValues("f" + i);
        BinaryDocValues cf = r.getBinaryDocValues("cf" + i);
        for (int j = 0; j < r.maxDoc(); j++) {
          assertEquals("reader=" + r + ", field=f" + i + ", doc=" + j, getValue(cf, j), getValue(f, j) * 2);
        }
      }
    }
    reader.close();
    
    dir.close();
  }
  
  public void testUpdatesOrder() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir, conf);
    
    Document doc = new Document();
    doc.add(new StringField("upd", "t1", Store.NO));
    doc.add(new StringField("upd", "t2", Store.NO));
    doc.add(new BinaryDocValuesField("f1", toBytes(1L)));
    doc.add(new BinaryDocValuesField("f2", toBytes(1L)));
    writer.addDocument(doc);
    writer.updateBinaryDocValue(new Term("upd", "t1"), "f1", toBytes(2L)); // update f1 to 2
    writer.updateBinaryDocValue(new Term("upd", "t1"), "f2", toBytes(2L)); // update f2 to 2
    writer.updateBinaryDocValue(new Term("upd", "t2"), "f1", toBytes(3L)); // update f1 to 3
    writer.updateBinaryDocValue(new Term("upd", "t2"), "f2", toBytes(3L)); // update f2 to 3
    writer.updateBinaryDocValue(new Term("upd", "t1"), "f1", toBytes(4L)); // update f1 to 4 (but not f2)
    writer.close();
    
    DirectoryReader reader = DirectoryReader.open(dir);
    assertEquals(4, getValue(reader.leaves().get(0).reader().getBinaryDocValues("f1"), 0));
    assertEquals(3, getValue(reader.leaves().get(0).reader().getBinaryDocValues("f2"), 0));
    reader.close();
    
    dir.close();
  }
  
  public void testUpdateAllDeletedSegment() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir, conf);
    
    Document doc = new Document();
    doc.add(new StringField("id", "doc", Store.NO));
    doc.add(new BinaryDocValuesField("f1", toBytes(1L)));
    writer.addDocument(doc);
    writer.addDocument(doc);
    writer.commit();
    writer.deleteDocuments(new Term("id", "doc")); // delete all docs in the first segment
    writer.addDocument(doc);
    writer.updateBinaryDocValue(new Term("id", "doc"), "f1", toBytes(2L));
    writer.close();
    
    DirectoryReader reader = DirectoryReader.open(dir);
    assertEquals(1, reader.leaves().size());
    assertEquals(2L, getValue(reader.leaves().get(0).reader().getBinaryDocValues("f1"), 0));
    reader.close();
    
    dir.close();
  }

  public void testUpdateTwoNonexistingTerms() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir, conf);
    
    Document doc = new Document();
    doc.add(new StringField("id", "doc", Store.NO));
    doc.add(new BinaryDocValuesField("f1", toBytes(1L)));
    writer.addDocument(doc);
    // update w/ multiple nonexisting terms in same field
    writer.updateBinaryDocValue(new Term("c", "foo"), "f1", toBytes(2L));
    writer.updateBinaryDocValue(new Term("c", "bar"), "f1", toBytes(2L));
    writer.close();
    
    DirectoryReader reader = DirectoryReader.open(dir);
    assertEquals(1, reader.leaves().size());
    assertEquals(1L, getValue(reader.leaves().get(0).reader().getBinaryDocValues("f1"), 0));
    reader.close();
    
    dir.close();
  }

  @Test
  public void testIOContext() throws Exception {
    // LUCENE-5591: make sure we pass an IOContext with an approximate
    // segmentSize in FlushInfo
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    // we want a single large enough segment so that a doc-values update writes a large file
    conf.setMergePolicy(NoMergePolicy.INSTANCE);
    conf.setMaxBufferedDocs(Integer.MAX_VALUE); // manually flush
    conf.setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
    IndexWriter writer = new IndexWriter(dir, conf);
    for (int i = 0; i < 100; i++) {
      writer.addDocument(doc(i));
    }
    writer.commit();
    writer.close();
    
    NRTCachingDirectory cachingDir = new NRTCachingDirectory(dir, 100, 1/(1024.*1024.));
    conf = newIndexWriterConfig(new MockAnalyzer(random()));
    // we want a single large enough segment so that a doc-values update writes a large file
    conf.setMergePolicy(NoMergePolicy.INSTANCE);
    conf.setMaxBufferedDocs(Integer.MAX_VALUE); // manually flush
    conf.setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
    writer = new IndexWriter(cachingDir, conf);
    writer.updateBinaryDocValue(new Term("id", "doc-0"), "val", toBytes(100L));
    DirectoryReader reader = DirectoryReader.open(writer); // flush
    assertEquals(0, cachingDir.listCachedFiles().length);
    
    IOUtils.close(reader, writer, cachingDir);
  }
  
}
