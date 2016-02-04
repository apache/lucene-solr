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
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.Nightly;
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
          Bits docsWithField = r.getDocsWithField(f);
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
              assertTrue(docsWithField.get(doc));
              if (field < numNDVFields) {
                assertEquals("invalid numeric value for doc=" + doc + ", field=" + f + ", reader=" + r, fieldValues[field], ndv.get(doc));
              } else {
                assertEquals("invalid binary value for doc=" + doc + ", field=" + f + ", reader=" + r, fieldValues[field], TestBinaryDocValuesUpdates.getValue(bdv, doc));
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
    BytesRef scratch = new BytesRef();
    for (LeafReaderContext context : reader.leaves()) {
      LeafReader r = context.reader();
      for (int i = 0; i < numFields; i++) {
        BinaryDocValues bdv = r.getBinaryDocValues("f" + i);
        NumericDocValues control = r.getNumericDocValues("cf" + i);
        Bits docsWithBdv = r.getDocsWithField("f" + i);
        Bits docsWithControl = r.getDocsWithField("cf" + i);
        Bits liveDocs = r.getLiveDocs();
        for (int j = 0; j < r.maxDoc(); j++) {
          if (liveDocs == null || liveDocs.get(j)) {
            assertTrue(docsWithBdv.get(j));
            assertTrue(docsWithControl.get(j));
            long ctrlValue = control.get(j);
            long bdvValue = TestBinaryDocValuesUpdates.getValue(bdv, j) * 2;
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
      writer.updateDocValues(t, new BinaryDocValuesField("f", TestBinaryDocValuesUpdates.toBytes(value)),
          new NumericDocValuesField("cf", value*2));
      DirectoryReader reader = DirectoryReader.open(writer);
      for (LeafReaderContext context : reader.leaves()) {
        LeafReader r = context.reader();
        BinaryDocValues fbdv = r.getBinaryDocValues("f");
        NumericDocValues cfndv = r.getNumericDocValues("cf");
        for (int j = 0; j < r.maxDoc(); j++) {
          assertEquals(cfndv.get(j), TestBinaryDocValuesUpdates.getValue(fbdv, j) * 2);
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
          assertEquals("reader=" + r + ", field=f" + i + ", doc=" + j, cf.get(j), TestBinaryDocValuesUpdates.getValue(f, j) * 2);
        }
      }
    }
    reader.close();
    
    dir.close();
  }
  
}
