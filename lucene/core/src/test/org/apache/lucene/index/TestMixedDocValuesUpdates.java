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
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;


public class TestMixedDocValuesUpdates extends LuceneTestCase {

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
    
    final int numFields = random.nextInt(4) + 3; // 3-7
    final int numNDVFields = random.nextInt(numFields/2) + 1; // 1-3
    final long[] fieldValues = new long[numFields];
    for (int i = 0; i < fieldValues.length; i++) {
      fieldValues[i] = 1;
    }
    
    int numRounds = atLeast(15);
    int docID = 0;
    for (int i = 0; i < numRounds; i++) {
      int numDocs = atLeast(5);
      // System.out.println("TEST: round=" + i + ", numDocs=" + numDocs);
      for (int j = 0; j < numDocs; j++) {
        Document doc = new Document();
        doc.add(new StringField("id", "doc-" + docID, Store.NO));
        doc.add(new StringField("key", "all", Store.NO)); // update key
        // add all fields with their current value
        for (int f = 0; f < fieldValues.length; f++) {
          if (f < numNDVFields) {
            doc.add(new NumericDocValuesField("f" + f, fieldValues[f]));
          } else {
            doc.add(new BinaryDocValuesField("f" + f, TestBinaryDocValuesUpdates.toBytes(fieldValues[f])));
          }
        }
        writer.addDocument(doc);
        ++docID;
      }
      
      int fieldIdx = random.nextInt(fieldValues.length);
      String updateField = "f" + fieldIdx;
      if (fieldIdx < numNDVFields) {
        writer.updateNumericDocValue(new Term("key", "all"), updateField, ++fieldValues[fieldIdx]);
      } else {
        writer.updateBinaryDocValue(new Term("key", "all"), updateField, TestBinaryDocValuesUpdates.toBytes(++fieldValues[fieldIdx]));
      }
      //System.out.println("TEST: updated field '" + updateField + "' to value " + fieldValues[fieldIdx]);

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
          NumericDocValues ndv = r.getNumericDocValues(f);
          if (field < numNDVFields) {
            assertNotNull(ndv);
            assertNull(bdv);
          } else {
            assertNull(ndv);
            assertNotNull(bdv);
          }
          int maxDoc = r.maxDoc();
          for (int doc = 0; doc < maxDoc; doc++) {
            if (liveDocs == null || liveDocs.get(doc)) {
//              System.out.println("doc=" + (doc + context.docBase) + " f='" + f + "' vslue=" + getValue(bdv, doc, scratch));
              if (field < numNDVFields) {
                assertEquals(doc, ndv.advance(doc));
                assertEquals("invalid numeric value for doc=" + doc + ", field=" + f + ", reader=" + r, fieldValues[field], ndv.longValue());
              } else {
                assertEquals(doc, bdv.advance(doc));
                assertEquals("invalid binary value for doc=" + doc + ", field=" + f + ", reader=" + r, fieldValues[field], TestBinaryDocValuesUpdates.getValue(bdv));
              }
            }
          }
        }
      }
//      System.out.println();
    }
    
    writer.close();
    IOUtils.close(reader, dir);
  }
  
  public void testStressMultiThreading() throws Exception {
    final Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    final IndexWriter writer = new IndexWriter(dir, conf);
    
    // create index
    final int numFields = TestUtil.nextInt(random(), 2, 4);
    final int numThreads = TestUtil.nextInt(random(), 3, 6);
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
        doc.add(new BinaryDocValuesField("f" + j, TestBinaryDocValuesUpdates.toBytes(value)));
        doc.add(new NumericDocValuesField("cf" + j, value * 2)); // control, always updated to f * 2
      }
      writer.addDocument(doc);
    }
    
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
//              System.out.println("[" + Thread.currentThread().getName() + "] numUpdates=" + numUpdates + " updateTerm=" + t);
              int field = random().nextInt(numFields);
              final String f = "f" + field;
              final String cf = "cf" + field;
              long updValue = random.nextInt();
//              System.err.println("[" + Thread.currentThread().getName() + "] t=" + t + ", f=" + f + ", updValue=" + updValue);
              writer.updateDocValues(t, new BinaryDocValuesField(f, TestBinaryDocValuesUpdates.toBytes(updValue)),
                  new NumericDocValuesField(cf, updValue*2));
              
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
        NumericDocValues control = r.getNumericDocValues("cf" + i);
        Bits liveDocs = r.getLiveDocs();
        for (int j = 0; j < r.maxDoc(); j++) {
          if (liveDocs == null || liveDocs.get(j)) {
            assertEquals(j, control.advance(j));
            long ctrlValue = control.longValue();
            assertEquals(j, bdv.advance(j));
            long bdvValue = TestBinaryDocValuesUpdates.getValue(bdv) * 2;
//              if (ctrlValue != bdvValue) {
//                System.out.println("seg=" + r + ", f=f" + i + ", doc=" + j + ", group=" + r.document(j).get("updKey") + ", ctrlValue=" + ctrlValue + ", bdvBytes=" + scratch);
//              }
            assertEquals(ctrlValue, bdvValue);
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
      doc.add(new BinaryDocValuesField("f", TestBinaryDocValuesUpdates.toBytes(value)));
      doc.add(new NumericDocValuesField("cf", value * 2));
      writer.addDocument(doc);
    }
    
    int numGens = atLeast(5);
    for (int i = 0; i < numGens; i++) {
      int doc = random().nextInt(numDocs);
      Term t = new Term("id", "doc" + doc);
      long value = random().nextLong();
      if (random().nextBoolean()) {
        doUpdate(t, writer, new BinaryDocValuesField("f", TestBinaryDocValuesUpdates.toBytes(value)),
            new NumericDocValuesField("cf", value*2));
      } else {
        writer.updateDocValues(t, new BinaryDocValuesField("f", TestBinaryDocValuesUpdates.toBytes(value)),
            new NumericDocValuesField("cf", value*2));
      }

      DirectoryReader reader = DirectoryReader.open(writer);
      for (LeafReaderContext context : reader.leaves()) {
        LeafReader r = context.reader();
        BinaryDocValues fbdv = r.getBinaryDocValues("f");
        NumericDocValues cfndv = r.getNumericDocValues("cf");
        for (int j = 0; j < r.maxDoc(); j++) {
          assertEquals(j, cfndv.nextDoc());
          assertEquals(j, fbdv.nextDoc());
          assertEquals(cfndv.longValue(), TestBinaryDocValuesUpdates.getValue(fbdv) * 2);
        }
      }
      reader.close();
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
        doc.add(new BinaryDocValuesField("f" + j, TestBinaryDocValuesUpdates.toBytes(val)));
        doc.add(new NumericDocValuesField("cf" + j, val * 2));
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
      writer.updateDocValues(updateTerm, new BinaryDocValuesField("f"+field, TestBinaryDocValuesUpdates.toBytes(value)),
          new NumericDocValuesField("cf"+field, value*2));
    }

    writer.close();
    
    DirectoryReader reader = DirectoryReader.open(dir);
    for (LeafReaderContext context : reader.leaves()) {
      for (int i = 0; i < numBinaryFields; i++) {
        LeafReader r = context.reader();
        BinaryDocValues f = r.getBinaryDocValues("f" + i);
        NumericDocValues cf = r.getNumericDocValues("cf" + i);
        for (int j = 0; j < r.maxDoc(); j++) {
          assertEquals(j, cf.nextDoc());
          assertEquals(j, f.nextDoc());
          assertEquals("reader=" + r + ", field=f" + i + ", doc=" + j, cf.longValue(), TestBinaryDocValuesUpdates.getValue(f) * 2);
        }
      }
    }
    reader.close();
    
    dir.close();
  }

  public void testTryUpdateDocValues() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig();
    IndexWriter writer = new IndexWriter(dir, conf);
    int numDocs = 1 + random().nextInt(128);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      doc.add(new StringField("id", "" + i, Store.YES));
      doc.add(new NumericDocValuesField("id", i));
      doc.add(new BinaryDocValuesField("binaryId", new BytesRef(new byte[] {(byte)i})));
      writer.addDocument(doc);
      if (random().nextBoolean()) {
        writer.flush();
      }
    }
    int doc = random().nextInt(numDocs);
    doUpdate(new Term("id", "" + doc), writer, new NumericDocValuesField("id", doc + 1),
        new BinaryDocValuesField("binaryId", new BytesRef(new byte[]{(byte) (doc + 1)})));
    IndexReader reader = writer.getReader();
    NumericDocValues idValues = null;
    BinaryDocValues binaryIdValues = null;
    for (LeafReaderContext c : reader.leaves()) {
      TopDocs topDocs = new IndexSearcher(c.reader()).search(new TermQuery(new Term("id", "" + doc)), 10);
      if (topDocs.totalHits.value == 1) {
        assertNull(idValues);
        assertNull(binaryIdValues);
        idValues = c.reader().getNumericDocValues("id");
        assertEquals(topDocs.scoreDocs[0].doc, idValues.advance(topDocs.scoreDocs[0].doc));
        binaryIdValues = c.reader().getBinaryDocValues("binaryId");
        assertEquals(topDocs.scoreDocs[0].doc, binaryIdValues.advance(topDocs.scoreDocs[0].doc));
      } else {
        assertEquals(0, topDocs.totalHits.value);
      }
    }

    assertNotNull(idValues);
    assertNotNull(binaryIdValues);

    assertEquals(doc+1, idValues.longValue());
    assertEquals(new BytesRef(new byte[] {(byte)(doc+1)}), binaryIdValues.binaryValue());
    IOUtils.close(reader, writer, dir);
  }

  public void testTryUpdateMultiThreaded() throws IOException, BrokenBarrierException, InterruptedException {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig();
    IndexWriter writer = new IndexWriter(dir, conf);
    ReentrantLock[] locks = new ReentrantLock[25 + random().nextInt(50)];
    Long[] values = new Long[locks.length];

    for (int i = 0; i < locks.length; i++) {
      locks[i] = new ReentrantLock();
      Document doc = new Document();
      values[i] = random().nextLong();
      doc.add(new StringField("id", Integer.toString(i), Store.NO));
      doc.add(new NumericDocValuesField("value", values[i]));
      writer.addDocument(doc);
    }

    int numThreads = TEST_NIGHTLY ? 2 + random().nextInt(3) : 2;
    Thread[] threads = new Thread[numThreads];
    CyclicBarrier barrier = new CyclicBarrier(threads.length + 1);
    for (int i = 0; i < threads.length; i++) {
      threads[i] = new Thread(() -> {
        try {
          barrier.await();
          for (int doc = 0; doc < 1000; doc++) {
            int docId = random().nextInt(locks.length);
            locks[docId].lock();
            try {
              Long value = rarely() ? null : random().nextLong(); // sometimes reset it
              if (random().nextBoolean()) {
                writer.updateDocValues(new Term("id", docId + ""), new NumericDocValuesField("value", value));
              } else {
                doUpdate(new Term("id", docId + ""), writer, new NumericDocValuesField("value", value));
              }
              values[docId] = value;
            } catch (IOException e) {
              throw new AssertionError(e);
            } finally {
              locks[docId].unlock();
            }
            if (rarely()) {
              writer.flush();
            }
          }
        } catch (Exception e) {
          throw new AssertionError(e);
        }
      });
      threads[i].start();
    }

    barrier.await();
    for (Thread t : threads) {
      t.join();
    }
    try (DirectoryReader reader = writer.getReader()) {
      for (int i = 0; i < locks.length; i++) {
        locks[i].lock();
        try {
          Long value = values[i];
          TopDocs topDocs = new IndexSearcher(reader).search(new TermQuery(new Term("id", "" + i)), 10);
          assertEquals(topDocs.totalHits.value, 1);
          int docID = topDocs.scoreDocs[0].doc;
          List<LeafReaderContext> leaves = reader.leaves();
          int subIndex = ReaderUtil.subIndex(docID, leaves);
          LeafReader leafReader = leaves.get(subIndex).reader();
          docID -= leaves.get(subIndex).docBase;
          NumericDocValues numericDocValues = leafReader.getNumericDocValues("value");
          if (value == null) {
            assertFalse("docID: " + docID, numericDocValues.advanceExact(docID));
          } else {
            assertTrue("docID: " + docID, numericDocValues.advanceExact(docID));
            assertEquals(numericDocValues.longValue(), value.longValue());
          }
        } finally {
          locks[i].unlock();
        }
      }
    }

    IOUtils.close(writer, dir);
  }

  static void doUpdate(Term doc, IndexWriter writer, Field... fields) throws IOException {
    long seqId = -1;
    do { // retry if we just committing a merge
      try (DirectoryReader reader = writer.getReader()) {
        TopDocs topDocs = new IndexSearcher(reader).search(new TermQuery(doc), 10);
        assertEquals(1, topDocs.totalHits.value);
        int theDoc = topDocs.scoreDocs[0].doc;
        seqId = writer.tryUpdateDocValue(reader, theDoc, fields);
      }
    } while (seqId == -1);
  }

  public void testResetValue() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir, conf);
    Document doc = new Document();
    doc.add(new StringField("id", "0", Store.NO));
    doc.add(new NumericDocValuesField("val", 5));
    doc.add(new BinaryDocValuesField("val-bin", new BytesRef(new byte[] {(byte)5})));
    writer.addDocument(doc);

    if (random().nextBoolean()) {
      writer.commit();
    }
    try(DirectoryReader reader = writer.getReader()) {
      assertEquals(1, reader.leaves().size());
      LeafReader r = reader.leaves().get(0).reader();
      NumericDocValues ndv = r.getNumericDocValues("val");
      assertEquals(0, ndv.nextDoc());
      assertEquals(5, ndv.longValue());
      assertEquals(DocIdSetIterator.NO_MORE_DOCS, ndv.nextDoc());

      BinaryDocValues bdv = r.getBinaryDocValues("val-bin");
      assertEquals(0, bdv.nextDoc());
      assertEquals(new BytesRef(new byte[]{(byte) 5}), bdv.binaryValue());
      assertEquals(DocIdSetIterator.NO_MORE_DOCS, bdv.nextDoc());
    }

    writer.updateDocValues(new Term("id", "0"), new BinaryDocValuesField("val-bin", null));
    try(DirectoryReader reader = writer.getReader()) {
      assertEquals(1, reader.leaves().size());
      LeafReader r = reader.leaves().get(0).reader();
      NumericDocValues ndv = r.getNumericDocValues("val");
      assertEquals(0, ndv.nextDoc());
      assertEquals(5, ndv.longValue());
      assertEquals(DocIdSetIterator.NO_MORE_DOCS, ndv.nextDoc());

      BinaryDocValues bdv = r.getBinaryDocValues("val-bin");
      assertEquals(DocIdSetIterator.NO_MORE_DOCS, bdv.nextDoc());
    }
    IOUtils.close(writer, dir);
  }

  public void testResetValueMultipleDocs() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir, conf);
    int numDocs = 10 + random().nextInt(50);
    int currentSeqId = 0;
    int[] seqId = new int[] {-1, -1, -1, -1, -1};
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      int id = random().nextInt(5);
      seqId[id] = currentSeqId;
      doc.add(new StringField("id",  "" + id, Store.YES));
      doc.add(new NumericDocValuesField("seqID", currentSeqId++));
      doc.add(new NumericDocValuesField("is_live", 1));
      if (i > 0) {
        writer.updateDocValues(new Term("id", "" + id), new NumericDocValuesField("is_live", null));
      }
      writer.addDocument(doc);
      if (random().nextBoolean()) {
        writer.flush();
      }
    }

    if (random().nextBoolean()) {
      writer.commit();
    }
    int numHits = 0; // check if every doc has been selected at least once
    for (int i : seqId) {
      if (i > -1) {
        numHits++;
      }
    }
    try(DirectoryReader reader = writer.getReader()) {
      IndexSearcher searcher = new IndexSearcher(reader);

      TopDocs is_live = searcher.search(new DocValuesFieldExistsQuery("is_live"), 5);
      assertEquals(numHits, is_live.totalHits.value);
      for (ScoreDoc doc : is_live.scoreDocs) {
        int id = Integer.parseInt(reader.document(doc.doc).get("id"));
        int i = ReaderUtil.subIndex(doc.doc, reader.leaves());
        assertTrue(i >= 0);
        LeafReaderContext leafReaderContext = reader.leaves().get(i);
        NumericDocValues seqID = leafReaderContext.reader().getNumericDocValues("seqID");
        assertNotNull(seqID);
        assertTrue(seqID.advanceExact(doc.doc - leafReaderContext.docBase));
        assertEquals(seqId[id], seqID.longValue());
      }
    }
    IOUtils.close(writer, dir);
  }

  public void testUpdateNotExistingFieldDV() throws IOException {
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, conf)) {
      Document doc = new Document();
      doc.add(new StringField("id", "1", Store.YES));
      doc.add(new NumericDocValuesField("test", 1));
      writer.addDocument(doc);
      if (random().nextBoolean()) {
        writer.commit();
      }
      writer.updateDocValues(new Term("id", "1"), new NumericDocValuesField("not_existing", 1));

      Document doc1 = new Document();
      doc1.add(new StringField("id", "2", Store.YES));
      doc1.add(new BinaryDocValuesField("not_existing", new BytesRef()));
      IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () ->
          writer.addDocument(doc1)
      );
      assertEquals("cannot change DocValues type from NUMERIC to BINARY for field \"not_existing\"", iae.getMessage());

      iae = expectThrows(IllegalArgumentException.class, () ->
          writer.updateDocValues(new Term("id", "1"), new BinaryDocValuesField("not_existing", new BytesRef()))
      );
      assertEquals("cannot change DocValues type from NUMERIC to BINARY for field \"not_existing\"", iae.getMessage());
    }
  }

  public void testUpdateFieldWithNoPreviousDocValues() throws IOException {
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, conf)) {
      Document doc = new Document();
      doc.add(new StringField("id", "1", Store.YES));
      writer.addDocument(doc);
      if (random().nextBoolean()) {
        try (DirectoryReader reader = writer.getReader()) {
          NumericDocValues id = reader.leaves().get(0).reader().getNumericDocValues("id");
          assertNull(id);
        }
      } else if (random().nextBoolean()) {
        writer.commit();
      }
      writer.updateDocValues(new Term("id", "1"), new NumericDocValuesField("id", 1));
      try (DirectoryReader reader = writer.getReader()) {
        NumericDocValues id = reader.leaves().get(0).reader().getNumericDocValues("id");
        assertNotNull(id);
        assertTrue(id.advanceExact(0));
        assertEquals(1, id.longValue());
      }
    }
  }
}

