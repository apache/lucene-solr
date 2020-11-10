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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NRTCachingDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

@SuppressWarnings("resource")
public class TestNumericDocValuesUpdates extends LuceneTestCase {

  private Document doc(int id) {
    // make sure we don't set the doc's value to 0, to not confuse with a document that's missing values
    return doc(id, id +1);
  }
  
  private Document doc(int id, long val) {
    Document doc = new Document();
    doc.add(new StringField("id", "doc-" + id, Store.NO));
    doc.add(new NumericDocValuesField("val", val));
    return doc;
  }

  public void testMultipleUpdatesSameDoc() throws Exception {

    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    
    conf.setMaxBufferedDocs(3); // small number of docs, so use a tiny maxBufferedDocs

    IndexWriter writer = new IndexWriter(dir, conf);

    writer.updateDocument       (new Term("id","doc-1"), doc(1, 1000000000L ));
    writer.updateNumericDocValue(new Term("id","doc-1"), "val", 1000001111L );
    writer.updateDocument       (new Term("id","doc-2"), doc(2, 2000000000L ));
    writer.updateDocument       (new Term("id","doc-2"), doc(2, 2222222222L ));
    writer.updateNumericDocValue(new Term("id","doc-1"), "val", 1111111111L );

    final DirectoryReader reader;
    if (random().nextBoolean()) {
      writer.commit();
      reader = DirectoryReader.open(dir);
    } else {
      reader = DirectoryReader.open(writer);
    }
    final IndexSearcher searcher = new IndexSearcher(reader);
    TopFieldDocs td;
    
    td = searcher.search(new TermQuery(new Term("id", "doc-1")), 1, 
                         new Sort(new SortField("val", SortField.Type.LONG)));
    assertEquals("doc-1 missing?", 1, td.scoreDocs.length);
    assertEquals("doc-1 value", 1111111111L, ((FieldDoc)td.scoreDocs[0]).fields[0]);
    
    td = searcher.search(new TermQuery(new Term("id", "doc-2")), 1, 
                        new Sort(new SortField("val", SortField.Type.LONG)));
    assertEquals("doc-2 missing?", 1, td.scoreDocs.length);
    assertEquals("doc-2 value", 2222222222L, ((FieldDoc)td.scoreDocs[0]).fields[0]);
    
    IOUtils.close(reader, writer, dir);
  }

  public void testBiasedMixOfRandomUpdates() throws Exception {
    // 3 types of operations: add, updated, updateDV.
    // rather then randomizing equally, we'll pick (random) cutoffs so each test run is biased,
    // in terms of some ops happen more often then others
    final int ADD_CUTOFF = TestUtil.nextInt(random(), 1, 98);
    final int UPD_CUTOFF = TestUtil.nextInt(random(), ADD_CUTOFF+1, 99);

    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));

    IndexWriter writer = new IndexWriter(dir, conf);
    
    final int numOperations = atLeast(1000);
    final Map<Integer,Long> expected = new HashMap<>(numOperations / 3);

    // start with at least one doc before any chance of updates
    final int numSeedDocs = atLeast(1); 
    for (int i = 0; i < numSeedDocs; i++) {
      final long val = random().nextLong();
      expected.put(i, val);
      writer.addDocument(doc(i, val));
    }

    int numDocUpdates = 0;
    int numValueUpdates = 0;

    for (int i = 0; i < numOperations; i++) {
      final int op = TestUtil.nextInt(random(), 1, 100);
      final long val = random().nextLong();
      if (op <= ADD_CUTOFF) {
        final int id = expected.size();
        expected.put(id, val);
        writer.addDocument(doc(id, val));
      } else {
        final int id = TestUtil.nextInt(random(), 0, expected.size()-1);
        expected.put(id, val);
        if (op <= UPD_CUTOFF) {
          numDocUpdates++;
          writer.updateDocument(new Term("id","doc-" + id), doc(id, val));
        } else {
          numValueUpdates++;
          writer.updateNumericDocValue(new Term("id","doc-" + id), "val", val);
        }
      }
    }

    writer.commit();
    
    final DirectoryReader reader = DirectoryReader.open(dir);
    final IndexSearcher searcher = new IndexSearcher(reader);

    // TODO: make more efficient if max numOperations is going to be increased much
    for (Map.Entry<Integer,Long> expect : expected.entrySet()) {
      String id = "doc-" + expect.getKey();
      TopFieldDocs td = searcher.search(new TermQuery(new Term("id", id)), 1, 
                                        new Sort(new SortField("val", SortField.Type.LONG)));
      assertEquals(id + " missing?", 1, td.totalHits.value);
      assertEquals(id + " value", expect.getValue(), ((FieldDoc)td.scoreDocs[0]).fields[0]);
    }
    
    IOUtils.close(reader, writer, dir);
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
    writer.updateNumericDocValue(new Term("id", "doc-0"), "val", 5L);
    assertEquals(2, writer.getFlushDeletesCount());
    writer.updateNumericDocValue(new Term("id", "doc-1"), "val", 6L);
    assertEquals(3, writer.getFlushDeletesCount());
    writer.updateNumericDocValue(new Term("id", "doc-2"), "val", 7L); 
    assertEquals(4, writer.getFlushDeletesCount());
    writer.getConfig().setRAMBufferSizeMB(1000d);
    writer.updateNumericDocValue(new Term("id", "doc-2"), "val", 7L);
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
    writer.updateNumericDocValue(new Term("id", "doc-0"), "val", 2L); // doc=0, exp=2
    
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
    NumericDocValues ndv = r.getNumericDocValues("val");
    assertEquals(0, ndv.nextDoc());
    assertEquals(2, ndv.longValue());
    assertEquals(1, ndv.nextDoc());
    assertEquals(2, ndv.longValue());
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
        writer.updateNumericDocValue(new Term("id", "doc-" + i), "val", value);
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
      NumericDocValues ndv = r.getNumericDocValues("val");
      assertNotNull(ndv);
      for (int i = 0; i < r.maxDoc(); i++) {
        long expected = expectedValues[i + context.docBase];
        assertEquals(i, ndv.nextDoc());
        long actual = ndv.longValue();
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
    if (VERBOSE) {
      System.out.println("TEST: isNRT=" + isNRT);
    }

    // update doc
    writer.updateNumericDocValue(new Term("id", "doc-0"), "val", 10L); // update doc-0's value to 10
    if (!isNRT) {
      writer.commit();
    }
    
    // reopen reader and assert only it sees the update
    if (VERBOSE) {
      System.out.println("TEST: openIfChanged");
    }
    final DirectoryReader reader2 = DirectoryReader.openIfChanged(reader1);
    assertNotNull(reader2);
    assertTrue(reader1 != reader2);
    NumericDocValues dvs1 = reader1.leaves().get(0).reader().getNumericDocValues("val");
    assertEquals(0, dvs1.nextDoc());
    assertEquals(1, dvs1.longValue());

    NumericDocValues dvs2 = reader2.leaves().get(0).reader().getNumericDocValues("val");
    assertEquals(0, dvs2.nextDoc());
    assertEquals(10, dvs2.longValue());

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
    writer.updateNumericDocValue(new Term("id", "doc-3"), "val", 17L);
    writer.updateNumericDocValue(new Term("id", "doc-5"), "val", 17L);
    
    final DirectoryReader reader;
    if (random().nextBoolean()) { // not NRT
      writer.close();
      reader = DirectoryReader.open(dir);
    } else { // NRT
      reader = DirectoryReader.open(writer);
      writer.close();
    }
    
    Bits liveDocs = MultiBits.getLiveDocs(reader);
    boolean[] expectedLiveDocs = new boolean[] { true, false, false, true, true, true };
    for (int i = 0; i < expectedLiveDocs.length; i++) {
      assertEquals(expectedLiveDocs[i], liveDocs.get(i));
    }
    
    long[] expectedValues = new long[] { 1, 2, 3, 17, 5, 17};
    NumericDocValues ndv = MultiDocValues.getNumericValues(reader, "val");
    for (int i = 0; i < expectedValues.length; i++) {
      assertEquals(i, ndv.nextDoc());
      assertEquals(expectedValues[i], ndv.longValue());
    }
    
    reader.close();
    dir.close();
  }
  
  public void testUpdatesWithDeletes() throws Exception {
    // update and delete different documents in the same commit session
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    conf.setMergePolicy(NoMergePolicy.INSTANCE); // otherwise a singleton merge could get rid of the delete
    conf.setMaxBufferedDocs(10); // control segment flushing
    IndexWriter writer = new IndexWriter(dir, conf);
    
    writer.addDocument(doc(0));
    writer.addDocument(doc(1));
    
    if (random().nextBoolean()) {
      writer.commit();
    }
    
    writer.deleteDocuments(new Term("id", "doc-0"));
    writer.updateNumericDocValue(new Term("id", "doc-1"), "val", 17L);
    
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
    NumericDocValues values = r.getNumericDocValues("val");
    assertEquals(1, values.advance(1));
    assertEquals(17, values.longValue());
    
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
    
    // update all docs' ndv field
    writer.updateNumericDocValue(new Term("dvUpdateKey", "dv"), "ndv", 17L);
    writer.close();
    
    final DirectoryReader reader = DirectoryReader.open(dir);
    LeafReader r = reader.leaves().get(0).reader();
    NumericDocValues ndv = r.getNumericDocValues("ndv");
    BinaryDocValues bdv = r.getBinaryDocValues("bdv");
    SortedDocValues sdv = r.getSortedDocValues("sdv");
    SortedSetDocValues ssdv = r.getSortedSetDocValues("ssdv");
    for (int i = 0; i < r.maxDoc(); i++) {
      assertEquals(i, ndv.nextDoc());
      assertEquals(17, ndv.longValue());
      assertEquals(i, bdv.nextDoc());
      BytesRef term = bdv.binaryValue();
      assertEquals(new BytesRef(Integer.toString(i)), term);
      assertEquals(i, sdv.nextDoc());
      term = sdv.binaryValue();
      assertEquals(new BytesRef(Integer.toString(i)), term);
      assertEquals(i, ssdv.nextDoc());

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
  
  public void testMultipleNumericDocValues() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    conf.setMaxBufferedDocs(10); // prevent merges
    IndexWriter writer = new IndexWriter(dir, conf);
    
    for (int i = 0; i < 2; i++) {
      Document doc = new Document();
      doc.add(new StringField("dvUpdateKey", "dv", Store.NO));
      doc.add(new NumericDocValuesField("ndv1", i));
      doc.add(new NumericDocValuesField("ndv2", i));
      writer.addDocument(doc);
    }
    writer.commit();
    
    // update all docs' ndv1 field
    writer.updateNumericDocValue(new Term("dvUpdateKey", "dv"), "ndv1", 17L);
    writer.close();
    
    final DirectoryReader reader = DirectoryReader.open(dir);
    LeafReader r = reader.leaves().get(0).reader();
    NumericDocValues ndv1 = r.getNumericDocValues("ndv1");
    NumericDocValues ndv2 = r.getNumericDocValues("ndv2");
    for (int i = 0; i < r.maxDoc(); i++) {
      assertEquals(i, ndv1.nextDoc());
      assertEquals(17, ndv1.longValue());
      assertEquals(i, ndv2.nextDoc());
      assertEquals(i, ndv2.longValue());
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
        doc.add(new NumericDocValuesField("ndv", 5));
      }
      writer.addDocument(doc);
    }
    writer.commit();
    if (VERBOSE) {
      System.out.println("TEST: first commit");
    }
    
    // update all docs' ndv field
    writer.updateNumericDocValue(new Term("dvUpdateKey", "dv"), "ndv", 17L);
    if (VERBOSE) {
      System.out.println("TEST: first close");
    }
    writer.close();
    if (VERBOSE) {
      System.out.println("TEST: done close");
    }
    
    final DirectoryReader reader = DirectoryReader.open(dir);
    if (VERBOSE) {
      System.out.println("TEST: got reader=reader");
    }
    LeafReader r = reader.leaves().get(0).reader();
    NumericDocValues ndv = r.getNumericDocValues("ndv");
    for (int i = 0; i < r.maxDoc(); i++) {
      assertEquals(i, ndv.nextDoc());
      assertEquals("doc=" + i + " has wrong numeric doc value", 17, ndv.longValue());
    }
    
    reader.close();
    dir.close();
  }
  
  public void testUpdateNonNumericDocValuesField() throws Exception {
    // we don't support adding new fields or updating existing non-numeric-dv
    // fields through numeric updates
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
      writer.updateNumericDocValue(new Term("key", "doc"), "ndv", 17L);
    });
    
    expectThrows(IllegalArgumentException.class, () -> {
      writer.updateNumericDocValue(new Term("key", "doc"), "foo", 17L);
    });
    
    writer.close();
    dir.close();
  }
  
  public void testDifferentDVFormatPerField() throws Exception {
    // test relies on separate instances of the "same thing"
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
    doc.add(new NumericDocValuesField("ndv", 5));
    doc.add(new SortedDocValuesField("sorted", new BytesRef("value")));
    writer.addDocument(doc); // flushed document
    writer.commit();
    writer.addDocument(doc); // in-memory document
    
    writer.updateNumericDocValue(new Term("key", "doc"), "ndv", 17L);
    writer.close();
    
    final DirectoryReader reader = DirectoryReader.open(dir);
    
    NumericDocValues ndv = MultiDocValues.getNumericValues(reader, "ndv");
    SortedDocValues sdv = MultiDocValues.getSortedValues(reader, "sorted");
    for (int i = 0; i < reader.maxDoc(); i++) {
      assertEquals(i, ndv.nextDoc());
      assertEquals(17, ndv.longValue());
      assertEquals(i, sdv.nextDoc());
      final BytesRef term = sdv.binaryValue();
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
    doc.add(new NumericDocValuesField("ndv", 5));
    writer.addDocument(doc); // flushed document
    writer.commit();
    writer.addDocument(doc); // in-memory document
    
    writer.updateNumericDocValue(new Term("key", "doc"), "ndv", 17L); // update existing field
    writer.updateNumericDocValue(new Term("key", "doc"), "ndv", 3L); // update existing field 2nd time in this commit
    writer.close();
    
    final DirectoryReader reader = DirectoryReader.open(dir);
    NumericDocValues ndv = MultiDocValues.getNumericValues(reader, "ndv");
    for (int i = 0; i < reader.maxDoc(); i++) {
      assertEquals(i, ndv.nextDoc());
      assertEquals(3, ndv.longValue());
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
    if (VERBOSE) {
      System.out.println("TEST: " + numRounds + " rounds");
    }
    for (int rnd = 0; rnd < numRounds; rnd++) {
      if (VERBOSE) {
        System.out.println("\nTEST: round=" + rnd);
      }
      Document doc = new Document();
      doc.add(new StringField("key", "doc", Store.NO));
      doc.add(new NumericDocValuesField("ndv", -1));
      int numDocs = atLeast(30);
      if (VERBOSE) {
        System.out.println("TEST: " + numDocs + " docs");
      }
      for (int i = 0; i < numDocs; i++) {
        doc.removeField("id");
        doc.add(new StringField("id", Integer.toString(docid), Store.YES));
        if (VERBOSE) {
          System.out.println("TEST: add doc id=" + docid);
        }
        writer.addDocument(doc);
        docid++;
      }
      
      long value = rnd + 1;
      if (VERBOSE) {
        System.out.println("TEST: update all ndv values to " + value);
      }
      writer.updateNumericDocValue(new Term("key", "doc"), "ndv", value);
      
      if (random.nextDouble() < 0.2) { // randomly delete one doc
        int delID = random.nextInt(docid);
        if (VERBOSE) {
          System.out.println("TEST: delete random doc id=" + delID);
        }
        writer.deleteDocuments(new Term("id", Integer.toString(delID)));
      }
      
      // randomly commit or reopen-IW (or nothing), before forceMerge
      if (random.nextDouble() < 0.4) {
        if (VERBOSE) {
          System.out.println("\nTEST: commit writer");
        }
        writer.commit();
      } else if (random.nextDouble() < 0.1) {
        if (VERBOSE) {
          System.out.println("\nTEST: close writer");
        }
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
      doc.add(new StringField("id", Integer.toString(docid), Store.YES));
      doc.add(new StringField("key", "doc", Store.NO));
      doc.add(new NumericDocValuesField("ndv", value));
      if (VERBOSE) {
        System.out.println("\nTEST: add one more doc id=" + docid);
      }
      writer.addDocument(doc);
      docid++;

      if (VERBOSE) {
        System.out.println("\nTEST: force merge");
      }
      writer.forceMerge(1, true);
      
      final DirectoryReader reader;
      if (random.nextBoolean()) {
        if (VERBOSE) {
          System.out.println("\nTEST: commit and open non-NRT reader");
        }
        writer.commit();
        reader = DirectoryReader.open(dir);
      } else {
        if (VERBOSE) {
          System.out.println("\nTEST: open NRT reader");
        }
        reader = DirectoryReader.open(writer);
      }
      if (VERBOSE) {
        System.out.println("TEST: got reader=" + reader);
      }
      
      assertEquals(1, reader.leaves().size());
      final LeafReader r = reader.leaves().get(0).reader();
      assertNull("index should have no deletes after forceMerge", r.getLiveDocs());
      NumericDocValues ndv = r.getNumericDocValues("ndv");
      assertNotNull(ndv);
      if (VERBOSE) {
        System.out.println("TEST: maxDoc=" + r.maxDoc());
      }
      for (int i = 0; i < r.maxDoc(); i++) {
        Document rdoc = r.document(i);
        assertEquals(i, ndv.nextDoc());
        assertEquals("docid=" + i + " has wrong ndv value; doc=" + rdoc, value, ndv.longValue());
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
    doc.add(new NumericDocValuesField("ndv", 5));
    writer.addDocument(doc); // flushed document
    writer.commit();
    writer.addDocument(doc); // in-memory document
    
    writer.updateNumericDocValue(new Term("k1", "v1"), "ndv", 17L);
    writer.updateNumericDocValue(new Term("k2", "v2"), "ndv", 3L);
    writer.close();
    
    final DirectoryReader reader = DirectoryReader.open(dir);
    NumericDocValues ndv = MultiDocValues.getNumericValues(reader, "ndv");
    for (int i = 0; i < reader.maxDoc(); i++) {
      assertEquals(i, ndv.nextDoc());
      assertEquals(3, ndv.longValue());
    }
    reader.close();
    dir.close();
  }

  static class OneSortDoc implements Comparable<OneSortDoc> {
    public long value;
    public final long sortValue;
    public final int id;
    public boolean deleted;

    public OneSortDoc(int id, long value, long sortValue) {
      this.value = value;
      this.sortValue = sortValue;
      this.id = id;
    }

    @Override
    public int compareTo(OneSortDoc other) {
      int cmp = Long.compare(sortValue, other.sortValue);
      if (cmp == 0) {
        cmp = Integer.compare(id, other.id);
        assert cmp != 0;
      }
      return cmp;
    }
  }

  public void testSortedIndex() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setIndexSort(new Sort(new SortField("sort", SortField.Type.LONG)));
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);

    int valueRange = TestUtil.nextInt(random(), 1, 1000);
    int sortValueRange = TestUtil.nextInt(random(), 1, 1000);

    int refreshChance = TestUtil.nextInt(random(), 5, 200);
    int deleteChance = TestUtil.nextInt(random(), 2, 100);

    int idUpto = 0;
    int deletedCount = 0;
    
    List<OneSortDoc> docs = new ArrayList<>();
    DirectoryReader r = w.getReader();

    int numIters = atLeast(1000);
    for(int iter=0;iter<numIters;iter++) {
      int value = random().nextInt(valueRange);
      if (docs.isEmpty() || random().nextInt(3) == 1) {
        int id = docs.size();
        // add new doc
        Document doc = new Document();
        doc.add(newStringField("id", Integer.toString(id), Field.Store.YES));
        doc.add(new NumericDocValuesField("number", value));
        int sortValue = random().nextInt(sortValueRange);
        doc.add(new NumericDocValuesField("sort", sortValue));
        if (VERBOSE) {
          System.out.println("TEST: iter=" + iter + " add doc id=" + id + " sortValue=" + sortValue + " value=" + value);
        }
        w.addDocument(doc);

        docs.add(new OneSortDoc(id, value, sortValue));
      } else {
        // update existing doc value
        int idToUpdate = random().nextInt(docs.size());
        if (VERBOSE) {
          System.out.println("TEST: iter=" + iter + " update doc id=" + idToUpdate + " new value=" + value);
        }
        w.updateNumericDocValue(new Term("id", Integer.toString(idToUpdate)), "number", (long) value);

        docs.get(idToUpdate).value = value;
      }

      if (random().nextInt(deleteChance) == 0) {
        int idToDelete = random().nextInt(docs.size());
        if (VERBOSE) {
          System.out.println("TEST: delete doc id=" + idToDelete);
        }
        w.deleteDocuments(new Term("id", Integer.toString(idToDelete)));
        if (docs.get(idToDelete).deleted == false) {
          docs.get(idToDelete).deleted = true;
          deletedCount++;
        }
      }

      if (random().nextInt(refreshChance) == 0) {
        if (VERBOSE) {
          System.out.println("TEST: now get reader; old reader=" + r);
        }
        DirectoryReader r2 = w.getReader();
        r.close();
        r = r2;

        if (VERBOSE) {
          System.out.println("TEST: got reader=" + r);
        }

        int liveCount = 0;

        for (LeafReaderContext ctx : r.leaves()) {
          LeafReader leafReader = ctx.reader();
          NumericDocValues values = leafReader.getNumericDocValues("number");
          NumericDocValues sortValues = leafReader.getNumericDocValues("sort");
          Bits liveDocs = leafReader.getLiveDocs();

          long lastSortValue = Long.MIN_VALUE;
          for (int i=0;i<leafReader.maxDoc();i++) {

            Document doc = leafReader.document(i);
            OneSortDoc sortDoc = docs.get(Integer.parseInt(doc.get("id")));

            assertEquals(i, values.nextDoc());
            assertEquals(i, sortValues.nextDoc());

            if (liveDocs != null && liveDocs.get(i) == false) {
              assertTrue(sortDoc.deleted);
              continue;
            }
            assertFalse(sortDoc.deleted);
        
            assertEquals(sortDoc.value, values.longValue());

            long sortValue = sortValues.longValue();
            assertEquals(sortDoc.sortValue, sortValue);
            
            assertTrue(sortValue >= lastSortValue);
            lastSortValue = sortValue;
            liveCount++;
          }
        }

        assertEquals(docs.size() - deletedCount, liveCount);
      }
    }

    IOUtils.close(r, w, dir);
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
    if (VERBOSE) {
      System.out.println("TEST: isNRT=" + isNRT);
    }
    DirectoryReader reader;
    if (isNRT) {
      reader = DirectoryReader.open(writer);
    } else {
      writer.commit();
      reader = DirectoryReader.open(dir);
    }
    
    final int numFields = random.nextInt(4) + 3; // 3-7
    final long[] fieldValues = new long[numFields];
    for (int i = 0; i < fieldValues.length; i++) {
      fieldValues[i] = 1;
    }
    
    int numRounds = atLeast(15);
    int docID = 0;
    for (int i = 0; i < numRounds; i++) {
      int numDocs = atLeast(5);
      if (VERBOSE) {
        System.out.println("TEST: round=" + i + ", numDocs=" + numDocs);
      }
      for (int j = 0; j < numDocs; j++) {
        Document doc = new Document();
        doc.add(new StringField("id", "doc-" + docID, Store.YES));
        doc.add(new StringField("key", "all", Store.NO)); // update key
        // add all fields with their current value
        for (int f = 0; f < fieldValues.length; f++) {
          doc.add(new NumericDocValuesField("f" + f, fieldValues[f]));
        }
        writer.addDocument(doc);
        if (VERBOSE) {
          System.out.println("TEST add doc id=" + docID);
        }
        ++docID;
      }
      
      int fieldIdx = random.nextInt(fieldValues.length);

      String updateField = "f" + fieldIdx;
      if (VERBOSE) {
        System.out.println("TEST: update field=" + updateField + " for all docs to value=" + (fieldValues[fieldIdx]+1));
      }
      writer.updateNumericDocValue(new Term("key", "all"), updateField, ++fieldValues[fieldIdx]);
      
      if (random.nextDouble() < 0.2) {
        int deleteDoc = random.nextInt(numDocs); // might also delete an already deleted document, ok!
        if (VERBOSE) {
          System.out.println("TEST: delete doc id=" + deleteDoc);
        }
        writer.deleteDocuments(new Term("id", "doc-" + deleteDoc));
      }
      
      // verify reader
      if (isNRT == false) {
        if (VERBOSE) {
          System.out.println("TEST: now commit");
        }
        writer.commit();
      }
      
      DirectoryReader newReader = DirectoryReader.openIfChanged(reader);
      assertNotNull(newReader);
      reader.close();
      reader = newReader;
      if (VERBOSE) {
        System.out.println("TEST: got reader maxDoc=" + reader.maxDoc() + " " + reader);
      }
      assertTrue(reader.numDocs() > 0); // we delete at most one document per round
      for (LeafReaderContext context : reader.leaves()) {
        LeafReader r = context.reader();
        Bits liveDocs = r.getLiveDocs();
        for (int field = 0; field < fieldValues.length; field++) {
          String f = "f" + field;
          NumericDocValues ndv = r.getNumericDocValues(f);
          assertNotNull(ndv);
          int maxDoc = r.maxDoc();
          for (int doc = 0; doc < maxDoc; doc++) {
            if (liveDocs == null || liveDocs.get(doc)) {
              assertEquals("advanced to wrong doc in seg=" + r, doc, ndv.advance(doc));
              assertEquals("invalid value for docID=" + doc + " id=" + r.document(doc).get("id") + ", field=" + f + ", reader=" + r + " doc=" + r.document(doc), fieldValues[field], ndv.longValue());
            }
          }
        }
      }
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
    
    // first segment with NDV
    Document doc = new Document();
    doc.add(new StringField("id", "doc0", Store.NO));
    doc.add(new NumericDocValuesField("ndv", 3));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new StringField("id", "doc4", Store.NO)); // document without 'ndv' field
    writer.addDocument(doc);
    writer.commit();
    
    // second segment with no NDV
    doc = new Document();
    doc.add(new StringField("id", "doc1", Store.NO));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new StringField("id", "doc2", Store.NO)); // document that isn't updated
    writer.addDocument(doc);
    writer.commit();
    
    // update document in the first segment - should not affect docsWithField of
    // the document without NDV field
    writer.updateNumericDocValue(new Term("id", "doc0"), "ndv", 5L);
    
    // update document in the second segment - field should be added and we should
    // be able to handle the other document correctly (e.g. no NPE)
    writer.updateNumericDocValue(new Term("id", "doc1"), "ndv", 5L);
    writer.close();

    DirectoryReader reader = DirectoryReader.open(dir);
    for (LeafReaderContext context : reader.leaves()) {
      LeafReader r = context.reader();
      NumericDocValues ndv = r.getNumericDocValues("ndv");
      assertEquals(0, ndv.nextDoc());
      assertEquals(5L, ndv.longValue());
      // docID 1 has no ndv value
      assertTrue(ndv.nextDoc() > 1);
    }
    reader.close();

    dir.close();
  }
  
  public void testUpdateSegmentWithNoDocValues2() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    // prevent merges, otherwise by the time updates are applied
    // (writer.close()), the segments might have merged and that update becomes
    // legit.
    conf.setMergePolicy(NoMergePolicy.INSTANCE);
    IndexWriter writer = new IndexWriter(dir, conf);
    
    // first segment with NDV
    Document doc = new Document();
    doc.add(new StringField("id", "doc0", Store.NO));
    doc.add(new NumericDocValuesField("ndv", 3));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new StringField("id", "doc4", Store.NO)); // document without 'ndv' field
    writer.addDocument(doc);
    writer.commit();
    
    // second segment with no NDV, but another dv field "foo"
    doc = new Document();
    doc.add(new StringField("id", "doc1", Store.NO));
    doc.add(new NumericDocValuesField("foo", 3));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new StringField("id", "doc2", Store.NO)); // document that isn't updated
    writer.addDocument(doc);
    writer.commit();
    
    // update document in the first segment - should not affect docsWithField of
    // the document without NDV field
    writer.updateNumericDocValue(new Term("id", "doc0"), "ndv", 5L);
    
    // update document in the second segment - field should be added and we should
    // be able to handle the other document correctly (e.g. no NPE)
    writer.updateNumericDocValue(new Term("id", "doc1"), "ndv", 5L);
    writer.close();

    DirectoryReader reader = DirectoryReader.open(dir);
    for (LeafReaderContext context : reader.leaves()) {
      LeafReader r = context.reader();
      NumericDocValues ndv = r.getNumericDocValues("ndv");
      assertEquals(0, ndv.nextDoc());
      assertEquals(5L, ndv.longValue());
      assertTrue(ndv.nextDoc() > 1);
    }
    reader.close();
    
    TestUtil.checkIndex(dir);
    
    conf = newIndexWriterConfig(new MockAnalyzer(random()));
    writer = new IndexWriter(dir, conf);
    writer.forceMerge(1);
    writer.close();
    
    reader = DirectoryReader.open(dir);
    LeafReader ar = getOnlyLeafReader(reader);
    assertEquals(DocValuesType.NUMERIC, ar.getFieldInfos().fieldInfo("foo").getDocValuesType());
    IndexSearcher searcher = new IndexSearcher(reader);
    TopFieldDocs td;
    // doc0
    td = searcher.search(new TermQuery(new Term("id", "doc0")), 1, 
                         new Sort(new SortField("ndv", SortField.Type.LONG)));
    assertEquals(5L, ((FieldDoc)td.scoreDocs[0]).fields[0]);
    // doc1
    td = searcher.search(new TermQuery(new Term("id", "doc1")), 1, 
                         new Sort(new SortField("ndv", SortField.Type.LONG), new SortField("foo", SortField.Type.LONG)));
    assertEquals(5L, ((FieldDoc)td.scoreDocs[0]).fields[0]);
    assertEquals(3L, ((FieldDoc)td.scoreDocs[0]).fields[1]);
    // doc2
    td = searcher.search(new TermQuery(new Term("id", "doc2")), 1, 
        new Sort(new SortField("ndv", SortField.Type.LONG)));
    assertEquals(0L, ((FieldDoc)td.scoreDocs[0]).fields[0]);
    // doc4
    td = searcher.search(new TermQuery(new Term("id", "doc4")), 1, 
        new Sort(new SortField("ndv", SortField.Type.LONG)));
    assertEquals(0L, ((FieldDoc)td.scoreDocs[0]).fields[0]);
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
    
    // first segment with NDV
    Document doc = new Document();
    doc.add(new StringField("id", "doc0", Store.NO));
    doc.add(new StringField("ndv", "mock-value", Store.NO));
    doc.add(new NumericDocValuesField("ndv", 5));
    writer.addDocument(doc);
    writer.commit();
    
    // second segment with no NDV
    doc = new Document();
    doc.add(new StringField("id", "doc1", Store.NO));
    doc.add(new StringField("ndv", "mock-value", Store.NO));
    writer.addDocument(doc);
    writer.commit();
    
    // update document in the second segment
    writer.updateNumericDocValue(new Term("id", "doc1"), "ndv", 5L);
    writer.close();

    DirectoryReader reader = DirectoryReader.open(dir);
    for (LeafReaderContext context : reader.leaves()) {
      LeafReader r = context.reader();
      NumericDocValues ndv = r.getNumericDocValues("ndv");
      for (int i = 0; i < r.maxDoc(); i++) {
        assertEquals(i, ndv.nextDoc());
        assertEquals(5L, ndv.longValue());
      }
    }
    reader.close();
    
    dir.close();
  }
  
  public void testUpdateNumericDVFieldWithSameNameAsPostingField() throws Exception {
    // this used to fail because FieldInfos.Builder neglected to update
    // globalFieldMaps.docValuesTypes map
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir, conf);
    
    Document doc = new Document();
    doc.add(new StringField("f", "mock-value", Store.NO));
    doc.add(new NumericDocValuesField("f", 5));
    writer.addDocument(doc);
    writer.commit();
    writer.updateNumericDocValue(new Term("f", "mock-value"), "f", 17L);
    writer.close();
    
    DirectoryReader r = DirectoryReader.open(dir);
    NumericDocValues ndv = r.leaves().get(0).reader().getNumericDocValues("f");
    assertEquals(0, ndv.nextDoc());
    assertEquals(17, ndv.longValue());
    r.close();
    
    dir.close();
  }
  
  public void testStressMultiThreading() throws Exception {
    final Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    final IndexWriter writer = new IndexWriter(dir, conf);
    
    // create index
    final int numFields = TestUtil.nextInt(random(), 1, 4);
    final int numDocs = TEST_NIGHTLY ? atLeast(2000) : atLeast(200);
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
        doc.add(new NumericDocValuesField("f" + j, value));
        doc.add(new NumericDocValuesField("cf" + j, value * 2)); // control, always updated to f * 2
      }
      writer.addDocument(doc);
    }
    
    final int numThreads = TEST_NIGHTLY ? TestUtil.nextInt(random(), 3, 6) : 2;
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
              long updValue = random.nextInt();
              writer.updateDocValues(t, new NumericDocValuesField(f, updValue), new NumericDocValuesField(cf, updValue*2));
              
              if (random.nextDouble() < 0.2) {
                // delete a random document
                int doc = random.nextInt(numDocs);
                writer.deleteDocuments(new Term("id", "doc" + doc));
              }
  
              if (random.nextDouble() < 0.05) { // commit every 20 updates on average
                writer.commit();
              }
              
              if (random.nextDouble() < 0.1) { // reopen NRT reader (apply updates), on average once every 10 updates
                if (reader == null) {
                  reader = DirectoryReader.open(writer);
                } else {
                  DirectoryReader r2 = DirectoryReader.openIfChanged(reader, writer);
                  if (r2 != null) {
                    reader.close();
                    reader = r2;
                  }
                }
              }
            }
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
        NumericDocValues ndv = r.getNumericDocValues("f" + i);
        NumericDocValues control = r.getNumericDocValues("cf" + i);
        Bits liveDocs = r.getLiveDocs();
        for (int j = 0; j < r.maxDoc(); j++) {
          if (liveDocs == null || liveDocs.get(j)) {
            assertEquals(j, ndv.advance(j));
            assertEquals(j, control.advance(j));
            assertEquals(control.longValue(), ndv.longValue() * 2);
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
      doc.add(new NumericDocValuesField("f", value));
      doc.add(new NumericDocValuesField("cf", value * 2));
      writer.addDocument(doc);
    }
    
    int numGens = atLeast(5);
    for (int i = 0; i < numGens; i++) {
      int doc = random().nextInt(numDocs);
      Term t = new Term("id", "doc" + doc);
      long value = random().nextLong();
      writer.updateDocValues(t, new NumericDocValuesField("f", value), new NumericDocValuesField("cf", value*2));
      DirectoryReader reader = DirectoryReader.open(writer);
      for (LeafReaderContext context : reader.leaves()) {
        LeafReader r = context.reader();
        NumericDocValues fndv = r.getNumericDocValues("f");
        NumericDocValues cfndv = r.getNumericDocValues("cf");
        for (int j = 0; j < r.maxDoc(); j++) {
          assertEquals(j, fndv.nextDoc());
          assertEquals(j, cfndv.nextDoc());
          assertEquals(cfndv.longValue(), fndv.longValue() * 2);
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
    doc.add(new NumericDocValuesField("f1", 5L));
    doc.add(new NumericDocValuesField("f2", 13L));
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
    doc.add(new NumericDocValuesField("f1", 17L));
    doc.add(new NumericDocValuesField("f2", 2L));
    writer.addDocument(doc);
    writer.updateNumericDocValue(new Term("id", "d0"), "f1", 12L);
    writer.close();
    
    DirectoryReader reader = DirectoryReader.open(dir);
    NumericDocValues f1 = MultiDocValues.getNumericValues(reader, "f1");
    NumericDocValues f2 = MultiDocValues.getNumericValues(reader, "f2");
    assertEquals(0, f1.nextDoc());
    assertEquals(12L, f1.longValue());
    assertEquals(0, f2.nextDoc());
    assertEquals(13L, f2.longValue());
    assertEquals(1, f1.nextDoc());
    assertEquals(17L, f1.longValue());
    assertEquals(1, f2.nextDoc());
    assertEquals(2L, f2.longValue());
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
      doc.add(new NumericDocValuesField("ndv", 4L));
      doc.add(new NumericDocValuesField("control", 8L));
      writer.addDocument(doc);
    }
    
    if (random().nextBoolean()) {
      writer.commit();
    }
    
    // update some docs to a random value
    long value = random().nextInt();
    Term term = new Term("id", RandomPicks.randomFrom(random(), randomTerms));
    writer.updateDocValues(term, new NumericDocValuesField("ndv", value), new NumericDocValuesField("control", value*2));
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
      NumericDocValues ndv = r.getNumericDocValues("ndv");
      NumericDocValues control = r.getNumericDocValues("control");
      for (int i = 0; i < r.maxDoc(); i++) {
        assertEquals(i, ndv.nextDoc());
        assertEquals(i, control.nextDoc());
        assertEquals(ndv.longValue()*2, control.longValue());
      }
    }
    reader.close();
    
    IOUtils.close(dir1, dir2);
  }

  public void testAddNewFieldAfterAddIndexes() throws Exception {
    Directory dir1 = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(NoMergePolicy.INSTANCE);
    final int numDocs = atLeast(50);
    try (IndexWriter writer = new IndexWriter(dir1, conf)) {
      // create first index
      for (int i = 0; i < numDocs; i++) {
        Document doc = new Document();
        doc.add(new StringField("id", Integer.toString(i), Store.NO));
        doc.add(new NumericDocValuesField("a1", 0L));
        doc.add(new NumericDocValuesField("a2", 1L));
        writer.addDocument(doc);
      }
    }

    Directory dir2 = newDirectory();
    conf = newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(NoMergePolicy.INSTANCE);
    try (IndexWriter writer = new IndexWriter(dir2, conf)) {
      // create second index
      for (int i = 0; i < numDocs; i++) {
        Document doc = new Document();
        doc.add(new StringField("id", Integer.toString(i), Store.NO));
        doc.add(new NumericDocValuesField("i1", 0L));
        doc.add(new NumericDocValuesField("i2", 1L));
        doc.add(new NumericDocValuesField("i3", 2L));
        writer.addDocument(doc);
      }
    }

    Directory mainDir = newDirectory();
    conf = newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(NoMergePolicy.INSTANCE);
    try (IndexWriter writer = new IndexWriter(mainDir, conf)) {
      writer.addIndexes(dir1, dir2);

      List<FieldInfos> originalFieldInfos = new ArrayList<>();
      try (DirectoryReader reader = DirectoryReader.open(writer)) {
        for (LeafReaderContext leaf : reader.leaves()) {
          originalFieldInfos.add(leaf.reader().getFieldInfos());
        }
      }
      assertTrue(originalFieldInfos.size() > 0);

      // update all doc values
      long value = random().nextInt();
      NumericDocValuesField[] update = new NumericDocValuesField[numDocs];
      for (int i = 0; i < numDocs; i++) {
        Term term = new Term("id", new BytesRef(Integer.toString(i)));
        writer.updateDocValues(term, new NumericDocValuesField("ndv", value));
      }

      try (DirectoryReader reader = DirectoryReader.open(writer)) {
        for (int i = 0; i < reader.leaves().size(); i++) {
          LeafReader leafReader = reader.leaves().get(i).reader();
          FieldInfos origFieldInfos = originalFieldInfos.get(i);
          FieldInfos newFieldInfos = leafReader.getFieldInfos();
          ensureConsistentFieldInfos(origFieldInfos, newFieldInfos);
          assertEquals(DocValuesType.NUMERIC, newFieldInfos.fieldInfo("ndv").getDocValuesType());
          NumericDocValues ndv = leafReader.getNumericDocValues("ndv");
          for (int docId = 0; docId < leafReader.maxDoc(); docId++) {
            assertEquals(docId, ndv.nextDoc());
            assertEquals(ndv.longValue(), value);
          }
        }
      }
    }
    IOUtils.close(dir1, dir2, mainDir);
  }

  public void testUpdatesAfterAddIndexes() throws Exception {
    Directory dir1 = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(NoMergePolicy.INSTANCE);
    final int numDocs = atLeast(50);
    try (IndexWriter writer = new IndexWriter(dir1, conf)) {
      // create first index
      for (int i = 0; i < numDocs; i++) {
        Document doc = new Document();
        doc.add(new StringField("id", Integer.toString(i), Store.NO));
        doc.add(new NumericDocValuesField("ndv", 4L));
        doc.add(new NumericDocValuesField("control", 8L));
        doc.add(new LongPoint("i1", 4L));
        writer.addDocument(doc);
      }
    }

    Directory dir2 = newDirectory();
    conf = newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(NoMergePolicy.INSTANCE);
    try (IndexWriter writer = new IndexWriter(dir2, conf)) {
      // create second index
      for (int i = numDocs; i < numDocs * 2; i++) {
        Document doc = new Document();
        doc.add(new StringField("id", Integer.toString(i), Store.NO));
        doc.add(new NumericDocValuesField("ndv", 2L));
        doc.add(new NumericDocValuesField("control", 4L));
        doc.add(new LongPoint("i2", 16L));
        doc.add(new LongPoint("i2", 24L));
        writer.addDocument(doc);
      }
    }

    Directory mainDir = newDirectory();
    conf = newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(NoMergePolicy.INSTANCE);
    try (IndexWriter writer = new IndexWriter(mainDir, conf)) {
      writer.addIndexes(dir1, dir2);

      List<FieldInfos> originalFieldInfos = new ArrayList<>();
      try (DirectoryReader reader = DirectoryReader.open(writer)) {
        for (LeafReaderContext leaf : reader.leaves()) {
          originalFieldInfos.add(leaf.reader().getFieldInfos());
        }
      }
      assertTrue(originalFieldInfos.size() > 0);

      // update some docs to a random value
      long value = random().nextInt();
      Term term = new Term("id", new BytesRef(Integer.toString(random().nextInt(numDocs) * 2)));
      writer.updateDocValues(term, new NumericDocValuesField("ndv", value),
          new NumericDocValuesField("control", value*2));

      try (DirectoryReader reader = DirectoryReader.open(writer)) {
        for (int i = 0; i < reader.leaves().size(); i++) {
          LeafReader leafReader = reader.leaves().get(i).reader();
          FieldInfos origFieldInfos = originalFieldInfos.get(i);
          FieldInfos newFieldInfos = leafReader.getFieldInfos();
          ensureConsistentFieldInfos(origFieldInfos, newFieldInfos);
          assertNotNull(newFieldInfos.fieldInfo("ndv"));
          assertEquals(DocValuesType.NUMERIC, newFieldInfos.fieldInfo("ndv").getDocValuesType());
          assertEquals(DocValuesType.NUMERIC, newFieldInfos.fieldInfo("control").getDocValuesType());
          NumericDocValues ndv = leafReader.getNumericDocValues("ndv");
          NumericDocValues control = leafReader.getNumericDocValues("control");
          for (int docId = 0; docId < leafReader.maxDoc(); docId++) {
            assertEquals(docId, ndv.nextDoc());
            assertEquals(docId, control.nextDoc());
            assertEquals(ndv.longValue()*2, control.longValue());
          }
        }
        IndexSearcher searcher = new IndexSearcher(reader);
        assertEquals(numDocs, searcher.count(LongPoint.newExactQuery("i1", 4L)));
        assertEquals(numDocs, searcher.count(LongPoint.newExactQuery("i2", 16L)));
        assertEquals(numDocs, searcher.count(LongPoint.newExactQuery("i2", 24L)));
      }
    }
    IOUtils.close(dir1, dir2, mainDir);
  }

  private void ensureConsistentFieldInfos(FieldInfos old, FieldInfos after) {
    for (FieldInfo fi : old) {
      assertNotNull(after.fieldInfo(fi.number));
      assertNotNull(after.fieldInfo(fi.name));
      assertEquals(fi.number, after.fieldInfo(fi.name).number);
      assertTrue(fi.getDocValuesGen() <= after.fieldInfo(fi.name).getDocValuesGen());
    }
  }


  public void testDeleteUnusedUpdatesFiles() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir, conf);
    
    Document doc = new Document();
    doc.add(new StringField("id", "d0", Store.NO));
    doc.add(new NumericDocValuesField("f1", 1L));
    doc.add(new NumericDocValuesField("f2", 1L));
    writer.addDocument(doc);

    // update each field twice to make sure all unneeded files are deleted
    for (String f : new String[] { "f1", "f2" }) {
      writer.updateNumericDocValue(new Term("id", "d0"), f, 2L);
      writer.commit();
      int numFiles = dir.listAll().length;
      
      // update again, number of files shouldn't change (old field's gen is
      // removed) 
      writer.updateNumericDocValue(new Term("id", "d0"), f, 3L);
      writer.commit();
      
      assertEquals(numFiles, dir.listAll().length);
    }
    
    writer.close();
    dir.close();
  }

  public void testTonsOfUpdates() throws Exception {
    // LUCENE-5248: make sure that when there are many updates, we don't use too much RAM
    Directory dir = newDirectory();
    final Random random = random();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random));
    conf.setRAMBufferSizeMB(IndexWriterConfig.DEFAULT_RAM_BUFFER_SIZE_MB);
    conf.setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH); // don't flush by doc
    IndexWriter writer = new IndexWriter(dir, conf);
    
    // test data: lots of documents (few 10Ks) and lots of update terms (few hundreds)
    final int numDocs = TEST_NIGHTLY ? atLeast(20000) : atLeast(200);
    final int numNumericFields = atLeast(5);
    final int numTerms = TestUtil.nextInt(random, 10, 100); // terms should affect many docs
    Set<String> updateTerms = new HashSet<>();
    while (updateTerms.size() < numTerms) {
      updateTerms.add(TestUtil.randomSimpleString(random));
    }

    // build a large index with many NDV fields and update terms
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      int numUpdateTerms = TestUtil.nextInt(random, 1, numTerms / 10);
      for (int j = 0; j < numUpdateTerms; j++) {
        doc.add(new StringField("upd", RandomPicks.randomFrom(random, updateTerms), Store.NO));
      }
      for (int j = 0; j < numNumericFields; j++) {
        long val = random.nextInt();
        doc.add(new NumericDocValuesField("f" + j, val));
        doc.add(new NumericDocValuesField("cf" + j, val * 2));
      }
      writer.addDocument(doc);
    }
    
    writer.commit(); // commit so there's something to apply to
    
    // set to flush every 2048 bytes (approximately every 12 updates), so we get
    // many flushes during numeric updates
    writer.getConfig().setRAMBufferSizeMB(2048.0 / 1024 / 1024);
    final int numUpdates = atLeast(100);
    for (int i = 0; i < numUpdates; i++) {
      int field = random.nextInt(numNumericFields);
      Term updateTerm = new Term("upd", RandomPicks.randomFrom(random, updateTerms));
      long value = random.nextInt();
      writer.updateDocValues(updateTerm, new NumericDocValuesField("f"+field, value), new NumericDocValuesField("cf"+field, value*2));
    }

    writer.close();
    
    DirectoryReader reader = DirectoryReader.open(dir);
    for (LeafReaderContext context : reader.leaves()) {
      for (int i = 0; i < numNumericFields; i++) {
        LeafReader r = context.reader();
        NumericDocValues f = r.getNumericDocValues("f" + i);
        NumericDocValues cf = r.getNumericDocValues("cf" + i);
        for (int j = 0; j < r.maxDoc(); j++) {
          assertEquals(j, f.nextDoc());
          assertEquals(j, cf.nextDoc());
          assertEquals("reader=" + r + ", field=f" + i + ", doc=" + j, cf.longValue(), f.longValue() * 2);
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
    doc.add(new NumericDocValuesField("f1", 1L));
    doc.add(new NumericDocValuesField("f2", 1L));
    writer.addDocument(doc);
    writer.updateNumericDocValue(new Term("upd", "t1"), "f1", 2L); // update f1 to 2
    writer.updateNumericDocValue(new Term("upd", "t1"), "f2", 2L); // update f2 to 2
    writer.updateNumericDocValue(new Term("upd", "t2"), "f1", 3L); // update f1 to 3
    writer.updateNumericDocValue(new Term("upd", "t2"), "f2", 3L); // update f2 to 3
    writer.updateNumericDocValue(new Term("upd", "t1"), "f1", 4L); // update f1 to 4 (but not f2)
    if (VERBOSE) {
      System.out.println("TEST: now close");
    }
    writer.close();
    
    DirectoryReader reader = DirectoryReader.open(dir);
    NumericDocValues dvs = reader.leaves().get(0).reader().getNumericDocValues("f1");
    assertEquals(0, dvs.nextDoc());
    assertEquals(4, dvs.longValue());
    dvs = reader.leaves().get(0).reader().getNumericDocValues("f2");
    assertEquals(0, dvs.nextDoc());
    assertEquals(3, dvs.longValue());
    reader.close();
    
    dir.close();
  }
  
  public void testUpdateAllDeletedSegment() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir, conf);
    
    Document doc = new Document();
    doc.add(new StringField("id", "doc", Store.NO));
    doc.add(new NumericDocValuesField("f1", 1L));
    writer.addDocument(doc);
    writer.addDocument(doc);
    writer.commit();
    writer.deleteDocuments(new Term("id", "doc")); // delete all docs in the first segment
    writer.addDocument(doc);
    writer.updateNumericDocValue(new Term("id", "doc"), "f1", 2L);
    writer.close();
    
    DirectoryReader reader = DirectoryReader.open(dir);
    assertEquals(1, reader.leaves().size());
    NumericDocValues dvs = reader.leaves().get(0).reader().getNumericDocValues("f1");
    assertEquals(0, dvs.nextDoc());
    assertEquals(2, dvs.longValue());
    
    reader.close();
    
    dir.close();
  }

  public void testUpdateTwoNonexistingTerms() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir, conf);
    
    Document doc = new Document();
    doc.add(new StringField("id", "doc", Store.NO));
    doc.add(new NumericDocValuesField("f1", 1L));
    writer.addDocument(doc);
    // update w/ multiple nonexisting terms in same field
    writer.updateNumericDocValue(new Term("c", "foo"), "f1", 2L);
    writer.updateNumericDocValue(new Term("c", "bar"), "f1", 2L);
    writer.close();
    
    DirectoryReader reader = DirectoryReader.open(dir);
    assertEquals(1, reader.leaves().size());
    NumericDocValues dvs = reader.leaves().get(0).reader().getNumericDocValues("f1");
    assertEquals(0, dvs.nextDoc());
    assertEquals(1, dvs.longValue());
    reader.close();
    
    dir.close();
  }

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
    writer.updateNumericDocValue(new Term("id", "doc-0"), "val", 100L);
    DirectoryReader reader = DirectoryReader.open(writer); // flush
    assertEquals(0, cachingDir.listCachedFiles().length);
    
    IOUtils.close(reader, writer, cachingDir);
  }
  
}
