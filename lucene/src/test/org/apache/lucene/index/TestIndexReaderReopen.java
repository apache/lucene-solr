package org.apache.lucene.index;

/**
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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.DefaultSimilarity;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.search.SimilarityProvider;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BitVector;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

public class TestIndexReaderReopen extends LuceneTestCase {
    
  private File indexDir;
  
  public void testReopen() throws Exception {
    final Directory dir1 = newDirectory();
    
    createIndex(random, dir1, false);
    performDefaultTests(new TestReopen() {

      @Override
      protected void modifyIndex(int i) throws IOException {
        TestIndexReaderReopen.modifyIndex(i, dir1);
      }

      @Override
      protected IndexReader openReader() throws IOException {
        return IndexReader.open(dir1, false);
      }
      
    });
    dir1.close();
    
    final Directory dir2 = newDirectory();
    
    createIndex(random, dir2, true);
    performDefaultTests(new TestReopen() {

      @Override
      protected void modifyIndex(int i) throws IOException {
        TestIndexReaderReopen.modifyIndex(i, dir2);
      }

      @Override
      protected IndexReader openReader() throws IOException {
        return IndexReader.open(dir2, false);
      }
      
    });
    dir2.close();
  }
  
  public void testParallelReaderReopen() throws Exception {
    final Directory dir1 = newDirectory();
    createIndex(random, dir1, true);
    final Directory dir2 = newDirectory();
    createIndex(random, dir2, true);
    
    performDefaultTests(new TestReopen() {

      @Override
      protected void modifyIndex(int i) throws IOException {
        TestIndexReaderReopen.modifyIndex(i, dir1);
        TestIndexReaderReopen.modifyIndex(i, dir2);
      }

      @Override
      protected IndexReader openReader() throws IOException {
        ParallelReader pr = new ParallelReader();
        pr.add(IndexReader.open(dir1, false));
        pr.add(IndexReader.open(dir2, false));
        return pr;
      }
      
    });
    dir1.close();
    dir2.close();
    
    final Directory dir3 = newDirectory();
    createIndex(random, dir3, true);
    final Directory dir4 = newDirectory();
    createIndex(random, dir4, true);

    performTestsWithExceptionInReopen(new TestReopen() {

      @Override
      protected void modifyIndex(int i) throws IOException {
        TestIndexReaderReopen.modifyIndex(i, dir3);
        TestIndexReaderReopen.modifyIndex(i, dir4);
      }

      @Override
      protected IndexReader openReader() throws IOException {
        ParallelReader pr = new ParallelReader();
        pr.add(IndexReader.open(dir3, false));
        pr.add(IndexReader.open(dir4, false));
        // Does not implement reopen, so
        // hits exception:
        pr.add(new FilterIndexReader(IndexReader.open(dir3, false)));
        return pr;
      }
      
    });
    dir3.close();
    dir4.close();
  }

  // LUCENE-1228: IndexWriter.commit() does not update the index version
  // populate an index in iterations.
  // at the end of every iteration, commit the index and reopen/recreate the reader.
  // in each iteration verify the work of previous iteration. 
  // try this once with reopen once recreate, on both RAMDir and FSDir.
  public void testCommitReopenFS () throws IOException {
    Directory dir = newFSDirectory(indexDir);
    doTestReopenWithCommit(random, dir, true);
    dir.close();
  }
  public void testCommitRecreateFS () throws IOException {
    Directory dir = newFSDirectory(indexDir);
    doTestReopenWithCommit(random, dir, false);
    dir.close();
  }
  public void testCommitReopenRAM () throws IOException {
    Directory dir = newDirectory();
    doTestReopenWithCommit(random, dir, true);
    dir.close();
  }
  public void testCommitRecreateRAM () throws IOException {
    Directory dir = newDirectory();
    doTestReopenWithCommit(random, dir, false);
    dir.close();
  }

  private void doTestReopenWithCommit (Random random, Directory dir, boolean withReopen) throws IOException {
    IndexWriter iwriter = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random)).setOpenMode(
                                                              OpenMode.CREATE).setMergeScheduler(new SerialMergeScheduler()).setMergePolicy(newLogMergePolicy()));
    iwriter.commit();
    IndexReader reader = IndexReader.open(dir, false);
    try {
      int M = 3;
      for (int i=0; i<4; i++) {
        for (int j=0; j<M; j++) {
          Document doc = new Document();
          doc.add(newField("id", i+"_"+j, Store.YES, Index.NOT_ANALYZED));
          doc.add(newField("id2", i+"_"+j, Store.YES, Index.NOT_ANALYZED_NO_NORMS));
          doc.add(newField("id3", i+"_"+j, Store.YES, Index.NO));
          iwriter.addDocument(doc);
          if (i>0) {
            int k = i-1;
            int n = j + k*M;
            Document prevItereationDoc = reader.document(n);
            assertNotNull(prevItereationDoc);
            String id = prevItereationDoc.get("id");
            assertEquals(k+"_"+j, id);
          }
        }
        iwriter.commit();
        if (withReopen) {
          // reopen
          IndexReader r2 = reader.reopen();
          if (reader != r2) {
            reader.close();
            reader = r2;
          }
        } else {
          // recreate
          reader.close();
          reader = IndexReader.open(dir, false);
        }
      }
    } finally {
      iwriter.close();
      reader.close();
    }
  }
  
  public void testMultiReaderReopen() throws Exception {
    final Directory dir1 = newDirectory();
    createIndex(random, dir1, true);

    final Directory dir2 = newDirectory();
    createIndex(random, dir2, true);

    performDefaultTests(new TestReopen() {

      @Override
      protected void modifyIndex(int i) throws IOException {
        TestIndexReaderReopen.modifyIndex(i, dir1);
        TestIndexReaderReopen.modifyIndex(i, dir2);
      }

      @Override
      protected IndexReader openReader() throws IOException {
        return new MultiReader(IndexReader.open(dir1, false),
            IndexReader.open(dir2, false));
      }
      
    });

    dir1.close();
    dir2.close();
    
    final Directory dir3 = newDirectory();
    createIndex(random, dir3, true);

    final Directory dir4 = newDirectory();
    createIndex(random, dir4, true);

    performTestsWithExceptionInReopen(new TestReopen() {

      @Override
      protected void modifyIndex(int i) throws IOException {
        TestIndexReaderReopen.modifyIndex(i, dir3);
        TestIndexReaderReopen.modifyIndex(i, dir4);
      }

      @Override
      protected IndexReader openReader() throws IOException {
        return new MultiReader(IndexReader.open(dir3, false),
            IndexReader.open(dir4, false),
            // Does not implement reopen, so
            // hits exception:
            new FilterIndexReader(IndexReader.open(dir3, false)));
      }
      
    });
    dir3.close();
    dir4.close();
  }

  public void testMixedReaders() throws Exception {
    final Directory dir1 = newDirectory();
    createIndex(random, dir1, true);
    final Directory dir2 = newDirectory();
    createIndex(random, dir2, true);
    final Directory dir3 = newDirectory();
    createIndex(random, dir3, false);
    final Directory dir4 = newDirectory();
    createIndex(random, dir4, true);
    final Directory dir5 = newDirectory();
    createIndex(random, dir5, false);
    
    performDefaultTests(new TestReopen() {

      @Override
      protected void modifyIndex(int i) throws IOException {
        // only change norms in this index to maintain the same number of docs for each of ParallelReader's subreaders
        if (i == 1) TestIndexReaderReopen.modifyIndex(i, dir1);  
        
        TestIndexReaderReopen.modifyIndex(i, dir4);
        TestIndexReaderReopen.modifyIndex(i, dir5);
      }

      @Override
      protected IndexReader openReader() throws IOException {
        ParallelReader pr = new ParallelReader();
        pr.add(IndexReader.open(dir1, false));
        pr.add(IndexReader.open(dir2, false));
        MultiReader mr = new MultiReader(IndexReader.open(dir3, false), IndexReader.open(dir4, false));
        return new MultiReader(pr, mr, IndexReader.open(dir5, false));
      }
    });
    dir1.close();
    dir2.close();
    dir3.close();
    dir4.close();
    dir5.close();
  }  
  
  private void performDefaultTests(TestReopen test) throws Exception {

    IndexReader index1 = test.openReader();
    IndexReader index2 = test.openReader();
        
    TestIndexReader.assertIndexEquals(index1, index2);

    // verify that reopen() does not return a new reader instance
    // in case the index has no changes
    ReaderCouple couple = refreshReader(index2, false);
    assertTrue(couple.refreshedReader == index2);
    
    couple = refreshReader(index2, test, 0, true);
    index1.close();
    index1 = couple.newReader;

    IndexReader index2_refreshed = couple.refreshedReader;
    index2.close();
    
    // test if refreshed reader and newly opened reader return equal results
    TestIndexReader.assertIndexEquals(index1, index2_refreshed);

    index2_refreshed.close();
    assertReaderClosed(index2, true, true);
    assertReaderClosed(index2_refreshed, true, true);

    index2 = test.openReader();
    
    for (int i = 1; i < 4; i++) {
      
      index1.close();
      couple = refreshReader(index2, test, i, true);
      // refresh IndexReader
      index2.close();
      
      index2 = couple.refreshedReader;
      index1 = couple.newReader;
      TestIndexReader.assertIndexEquals(index1, index2);
    }
    
    index1.close();
    index2.close();
    assertReaderClosed(index1, true, true);
    assertReaderClosed(index2, true, true);
  }
  
  public void testReferenceCounting() throws IOException {
    for (int mode = 0; mode < 4; mode++) {
      Directory dir1 = newDirectory();
      createIndex(random, dir1, true);
     
      IndexReader reader0 = IndexReader.open(dir1, false);
      assertRefCountEquals(1, reader0);

      assertTrue(reader0 instanceof DirectoryReader);
      IndexReader[] subReaders0 = reader0.getSequentialSubReaders();
      for (int i = 0; i < subReaders0.length; i++) {
        assertRefCountEquals(1, subReaders0[i]);
      }
      
      // delete first document, so that only one of the subReaders have to be re-opened
      IndexReader modifier = IndexReader.open(dir1, false);
      modifier.deleteDocument(0);
      modifier.close();
      
      IndexReader reader1 = refreshReader(reader0, true).refreshedReader;
      assertTrue(reader1 instanceof DirectoryReader);
      IndexReader[] subReaders1 = reader1.getSequentialSubReaders();
      assertEquals(subReaders0.length, subReaders1.length);
      
      for (int i = 0; i < subReaders0.length; i++) {
        if (subReaders0[i] != subReaders1[i]) {
          assertRefCountEquals(1, subReaders0[i]);
          assertRefCountEquals(1, subReaders1[i]);
        } else {
          assertRefCountEquals(2, subReaders0[i]);
        }
      }

      // delete first document, so that only one of the subReaders have to be re-opened
      modifier = IndexReader.open(dir1, false);
      modifier.deleteDocument(1);
      modifier.close();

      IndexReader reader2 = refreshReader(reader1, true).refreshedReader;
      assertTrue(reader2 instanceof DirectoryReader);
      IndexReader[] subReaders2 = reader2.getSequentialSubReaders();
      assertEquals(subReaders1.length, subReaders2.length);
      
      for (int i = 0; i < subReaders2.length; i++) {
        if (subReaders2[i] == subReaders1[i]) {
          if (subReaders1[i] == subReaders0[i]) {
            assertRefCountEquals(3, subReaders2[i]);
          } else {
            assertRefCountEquals(2, subReaders2[i]);
          }
        } else {
          assertRefCountEquals(1, subReaders2[i]);
          if (subReaders0[i] == subReaders1[i]) {
            assertRefCountEquals(2, subReaders2[i]);
            assertRefCountEquals(2, subReaders0[i]);
          } else {
            assertRefCountEquals(1, subReaders0[i]);
            assertRefCountEquals(1, subReaders1[i]);
          }
        }
      }
      
      IndexReader reader3 = refreshReader(reader0, true).refreshedReader;
      assertTrue(reader3 instanceof DirectoryReader);
      IndexReader[] subReaders3 = reader3.getSequentialSubReaders();
      assertEquals(subReaders3.length, subReaders0.length);
      
      // try some permutations
      switch (mode) {
      case 0:
        reader0.close();
        reader1.close();
        reader2.close();
        reader3.close();
        break;
      case 1:
        reader3.close();
        reader2.close();
        reader1.close();
        reader0.close();
        break;
      case 2:
        reader2.close();
        reader3.close();
        reader0.close();
        reader1.close();
        break;
      case 3:
        reader1.close();
        reader3.close();
        reader2.close();
        reader0.close();
        break;
      }      
      
      assertReaderClosed(reader0, true, true);
      assertReaderClosed(reader1, true, true);
      assertReaderClosed(reader2, true, true);
      assertReaderClosed(reader3, true, true);

      dir1.close();
    }
  }


  public void testReferenceCountingMultiReader() throws IOException {
    for (int mode = 0; mode <=1; mode++) {
      Directory dir1 = newDirectory();
      createIndex(random, dir1, false);
      Directory dir2 = newDirectory();
      createIndex(random, dir2, true);
      
      IndexReader reader1 = IndexReader.open(dir1, false);
      assertRefCountEquals(1, reader1);

      IndexReader initReader2 = IndexReader.open(dir2, false);
      IndexReader multiReader1 = new MultiReader(new IndexReader[] {reader1, initReader2}, (mode == 0));
      modifyIndex(0, dir2);
      assertRefCountEquals(1 + mode, reader1);
      
      IndexReader multiReader2 = multiReader1.reopen();
      // index1 hasn't changed, so multiReader2 should share reader1 now with multiReader1
      assertRefCountEquals(2 + mode, reader1);
      
      modifyIndex(0, dir1);
      IndexReader reader2 = reader1.reopen();
      assertRefCountEquals(2 + mode, reader1);

      if (mode == 1) {
        initReader2.close();
      }
      
      modifyIndex(1, dir1);
      IndexReader reader3 = reader2.reopen();
      assertRefCountEquals(2 + mode, reader1);
      assertRefCountEquals(1, reader2);
      
      multiReader1.close();
      assertRefCountEquals(1 + mode, reader1);
      
      multiReader1.close();
      assertRefCountEquals(1 + mode, reader1);

      if (mode == 1) {
        initReader2.close();
      }
      
      reader1.close();
      assertRefCountEquals(1, reader1);
      
      multiReader2.close();
      assertRefCountEquals(0, reader1);
      
      multiReader2.close();
      assertRefCountEquals(0, reader1);
      
      reader3.close();
      assertRefCountEquals(0, reader1);
      assertReaderClosed(reader1, true, false);
      
      reader2.close();
      assertRefCountEquals(0, reader1);
      assertReaderClosed(reader1, true, false);
      
      reader2.close();
      assertRefCountEquals(0, reader1);
      
      reader3.close();
      assertRefCountEquals(0, reader1);
      assertReaderClosed(reader1, true, true);
      dir1.close();
      dir2.close();
    }

  }

  public void testReferenceCountingParallelReader() throws IOException {
    for (int mode = 0; mode <=1; mode++) {
      Directory dir1 = newDirectory();
      createIndex(random, dir1, false);
      Directory dir2 = newDirectory();
      createIndex(random, dir2, true);
      
      IndexReader reader1 = IndexReader.open(dir1, false);
      assertRefCountEquals(1, reader1);
      
      ParallelReader parallelReader1 = new ParallelReader(mode == 0);
      parallelReader1.add(reader1);
      IndexReader initReader2 = IndexReader.open(dir2, false);
      parallelReader1.add(initReader2);
      modifyIndex(1, dir2);
      assertRefCountEquals(1 + mode, reader1);
      
      IndexReader parallelReader2 = parallelReader1.reopen();
      // index1 hasn't changed, so parallelReader2 should share reader1 now with multiReader1
      assertRefCountEquals(2 + mode, reader1);
      
      modifyIndex(0, dir1);
      modifyIndex(0, dir2);
      IndexReader reader2 = reader1.reopen();
      assertRefCountEquals(2 + mode, reader1);

      if (mode == 1) {
        initReader2.close();
      }
      
      modifyIndex(4, dir1);
      IndexReader reader3 = reader2.reopen();
      assertRefCountEquals(2 + mode, reader1);
      assertRefCountEquals(1, reader2);
      
      parallelReader1.close();
      assertRefCountEquals(1 + mode, reader1);
      
      parallelReader1.close();
      assertRefCountEquals(1 + mode, reader1);

      if (mode == 1) {
        initReader2.close();
      }
      
      reader1.close();
      assertRefCountEquals(1, reader1);
      
      parallelReader2.close();
      assertRefCountEquals(0, reader1);
      
      parallelReader2.close();
      assertRefCountEquals(0, reader1);
      
      reader3.close();
      assertRefCountEquals(0, reader1);
      assertReaderClosed(reader1, true, false);
      
      reader2.close();
      assertRefCountEquals(0, reader1);
      assertReaderClosed(reader1, true, false);
      
      reader2.close();
      assertRefCountEquals(0, reader1);
      
      reader3.close();
      assertRefCountEquals(0, reader1);
      assertReaderClosed(reader1, true, true);

      dir1.close();
      dir2.close();
    }

  }
  
  public void testNormsRefCounting() throws IOException {
    Directory dir1 = newDirectory();
    createIndex(random, dir1, false);
    
    IndexReader reader1 = IndexReader.open(dir1, false);
    SegmentReader segmentReader1 = getOnlySegmentReader(reader1);
    IndexReader modifier = IndexReader.open(dir1, false);
    modifier.deleteDocument(0);
    modifier.close();
    
    IndexReader reader2 = reader1.reopen();
    modifier = IndexReader.open(dir1, false);
    Similarity sim = new DefaultSimilarity();
    modifier.setNorm(1, "field1", sim.encodeNormValue(50f));
    modifier.setNorm(1, "field2", sim.encodeNormValue(50f));
    modifier.close();
    
    IndexReader reader3 = reader2.reopen();
    SegmentReader segmentReader3 = getOnlySegmentReader(reader3);
    modifier = IndexReader.open(dir1, false);
    modifier.deleteDocument(2);
    modifier.close();

    IndexReader reader4 = reader3.reopen();
    modifier = IndexReader.open(dir1, false);
    modifier.deleteDocument(3);
    modifier.close();

    IndexReader reader5 = reader3.reopen();
    
    // Now reader2-reader5 references reader1. reader1 and reader2
    // share the same norms. reader3, reader4, reader5 also share norms.
    assertRefCountEquals(1, reader1);
    assertFalse(segmentReader1.normsClosed());

    reader1.close();

    assertRefCountEquals(0, reader1);
    assertFalse(segmentReader1.normsClosed());

    reader2.close();
    assertRefCountEquals(0, reader1);

    // now the norms for field1 and field2 should be closed
    assertTrue(segmentReader1.normsClosed("field1"));
    assertTrue(segmentReader1.normsClosed("field2"));

    // but the norms for field3 and field4 should still be open
    assertFalse(segmentReader1.normsClosed("field3"));
    assertFalse(segmentReader1.normsClosed("field4"));
    
    reader3.close();
    assertRefCountEquals(0, reader1);
    assertFalse(segmentReader3.normsClosed());
    reader5.close();
    assertRefCountEquals(0, reader1);
    assertFalse(segmentReader3.normsClosed());
    reader4.close();
    assertRefCountEquals(0, reader1);
    
    // and now all norms that reader1 used should be closed
    assertTrue(segmentReader1.normsClosed());
    
    // now that reader3, reader4 and reader5 are closed,
    // the norms that those three readers shared should be
    // closed as well
    assertTrue(segmentReader3.normsClosed());

    dir1.close();
  }
  
  private void performTestsWithExceptionInReopen(TestReopen test) throws Exception {
    IndexReader index1 = test.openReader();
    IndexReader index2 = test.openReader();

    TestIndexReader.assertIndexEquals(index1, index2);
    
    try {
      refreshReader(index1, test, 0, true);
      fail("Expected exception not thrown.");
    } catch (Exception e) {
      // expected exception
    }
    
    // index2 should still be usable and unaffected by the failed reopen() call
    TestIndexReader.assertIndexEquals(index1, index2);

    index1.close();
    index2.close();
  }
  
  public void testThreadSafety() throws Exception {
    final Directory dir = newDirectory();
    final int n = 30 * RANDOM_MULTIPLIER;
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random)));
    for (int i = 0; i < n; i++) {
      writer.addDocument(createDocument(i, 3));
    }
    writer.optimize();
    writer.close();

    final TestReopen test = new TestReopen() {      
      @Override
      protected void modifyIndex(int i) throws IOException {
        if (i % 3 == 0) {
          IndexReader modifier = IndexReader.open(dir, false);
          Similarity sim = new DefaultSimilarity();
          modifier.setNorm(i, "field1", sim.encodeNormValue(50f));
          modifier.close();
        } else if (i % 3 == 1) {
          IndexReader modifier = IndexReader.open(dir, false);
          modifier.deleteDocument(i % modifier.maxDoc());
          modifier.close();
        } else {
          IndexWriter modifier = new IndexWriter(dir, new IndexWriterConfig(
              TEST_VERSION_CURRENT, new MockAnalyzer(random)));
          modifier.addDocument(createDocument(n + i, 6));
          modifier.close();
        }
      }

      @Override
      protected IndexReader openReader() throws IOException {
        return IndexReader.open(dir, false);
      }      
    };
    
    final List<ReaderCouple> readers = Collections.synchronizedList(new ArrayList<ReaderCouple>());
    IndexReader firstReader = IndexReader.open(dir, false);
    IndexReader reader = firstReader;
    final Random rnd = random;
    
    ReaderThread[] threads = new ReaderThread[n];
    final Set<IndexReader> readersToClose = Collections.synchronizedSet(new HashSet<IndexReader>());
    
    for (int i = 0; i < n; i++) {
      if (i % 2 == 0) {
        IndexReader refreshed = reader.reopen();
        if (refreshed != reader) {
          readersToClose.add(reader);
        }
        reader = refreshed;
      }
      final IndexReader r = reader;
      
      final int index = i;    
      
      ReaderThreadTask task;
      
      if (i < 4 || (i >=10 && i < 14) || i > 18) {
        task = new ReaderThreadTask() {
          
          @Override
          public void run() throws Exception {
            while (!stopped) {
              if (index % 2 == 0) {
                // refresh reader synchronized
                ReaderCouple c = (refreshReader(r, test, index, true));
                readersToClose.add(c.newReader);
                readersToClose.add(c.refreshedReader);
                readers.add(c);
                // prevent too many readers
                break;
              } else {
                // not synchronized
                IndexReader refreshed = r.reopen();
                
                IndexSearcher searcher = newSearcher(refreshed);
                ScoreDoc[] hits = searcher.search(
                    new TermQuery(new Term("field1", "a" + rnd.nextInt(refreshed.maxDoc()))),
                    null, 1000).scoreDocs;
                if (hits.length > 0) {
                  searcher.doc(hits[0].doc);
                }
                searcher.close();
                if (refreshed != r) {
                  refreshed.close();
                }
              }
              synchronized(this) {
                wait(_TestUtil.nextInt(random, 1, 100));
              }
            }
          }
          
        };
      } else {
        task = new ReaderThreadTask() {
          @Override
          public void run() throws Exception {
            while (!stopped) {
              int numReaders = readers.size();
              if (numReaders > 0) {
                ReaderCouple c =  readers.get(rnd.nextInt(numReaders));
                TestIndexReader.assertIndexEquals(c.newReader, c.refreshedReader);
              }
              
              synchronized(this) {
                wait(_TestUtil.nextInt(random, 1, 100));
              }
            }
          }
        };
      }
      
      threads[i] = new ReaderThread(task);
      threads[i].start();
    }
    
    synchronized(this) {
      wait(1000);
    }
    
    for (int i = 0; i < n; i++) {
      if (threads[i] != null) {
        threads[i].stopThread();
      }
    }
    
    for (int i = 0; i < n; i++) {
      if (threads[i] != null) {
        threads[i].join();
        if (threads[i].error != null) {
          String msg = "Error occurred in thread " + threads[i].getName() + ":\n" + threads[i].error.getMessage();
          fail(msg);
        }
      }
      
    }
    
    for (final IndexReader readerToClose : readersToClose) {
      readerToClose.close();
    }
    
    firstReader.close();
    reader.close();
    
    for (final IndexReader readerToClose : readersToClose) {
      assertReaderClosed(readerToClose, true, true);
    }

    assertReaderClosed(reader, true, true);
    assertReaderClosed(firstReader, true, true);

    dir.close();
  }
  
  private static class ReaderCouple {
    ReaderCouple(IndexReader r1, IndexReader r2) {
      newReader = r1;
      refreshedReader = r2;
    }
    
    IndexReader newReader;
    IndexReader refreshedReader;
  }
  
  private abstract static class ReaderThreadTask {
    protected volatile boolean stopped;
    public void stop() {
      this.stopped = true;
    }
    
    public abstract void run() throws Exception;
  }
  
  private static class ReaderThread extends Thread {
    private ReaderThreadTask task;
    private Throwable error;
    
    
    ReaderThread(ReaderThreadTask task) {
      this.task = task;
    }
    
    public void stopThread() {
      this.task.stop();
    }
    
    @Override
    public void run() {
      try {
        this.task.run();
      } catch (Throwable r) {
        r.printStackTrace(System.out);
        this.error = r;
      }
    }
  }
  
  private Object createReaderMutex = new Object();
  
  private ReaderCouple refreshReader(IndexReader reader, boolean hasChanges) throws IOException {
    return refreshReader(reader, null, -1, hasChanges);
  }
  
  ReaderCouple refreshReader(IndexReader reader, TestReopen test, int modify, boolean hasChanges) throws IOException {
    synchronized (createReaderMutex) {
      IndexReader r = null;
      if (test != null) {
        test.modifyIndex(modify);
        r = test.openReader();
      }
      
      IndexReader refreshed = null;
      try {
        refreshed = reader.reopen();
      } finally {
        if (refreshed == null && r != null) {
          // Hit exception -- close opened reader
          r.close();
        }
      }
      
      if (hasChanges) {
        if (refreshed == reader) {
          fail("No new IndexReader instance created during refresh.");
        }
      } else {
        if (refreshed != reader) {
          fail("New IndexReader instance created during refresh even though index had no changes.");
        }
      }
      
      return new ReaderCouple(r, refreshed);
    }
  }
  
  public static void createIndex(Random random, Directory dir, boolean multiSegment) throws IOException {
    IndexWriter.unlock(dir);
    IndexWriter w = new IndexWriter(dir, LuceneTestCase.newIndexWriterConfig(random,
        TEST_VERSION_CURRENT, new MockAnalyzer(random))
        .setMergePolicy(new LogDocMergePolicy()));
    
    for (int i = 0; i < 100; i++) {
      w.addDocument(createDocument(i, 4));
      if (multiSegment && (i % 10) == 0) {
        w.commit();
      }
    }
    
    if (!multiSegment) {
      w.optimize();
    }
    
    w.close();

    IndexReader r = IndexReader.open(dir, false);
    if (multiSegment) {
      assertTrue(r.getSequentialSubReaders().length > 1);
    } else {
      assertTrue(r.getSequentialSubReaders().length == 1);
    }
    r.close();
  }

  public static Document createDocument(int n, int numFields) {
    StringBuilder sb = new StringBuilder();
    Document doc = new Document();
    sb.append("a");
    sb.append(n);
    doc.add(new Field("field1", sb.toString(), Store.YES, Index.ANALYZED));
    doc.add(new Field("fielda", sb.toString(), Store.YES, Index.NOT_ANALYZED_NO_NORMS));
    doc.add(new Field("fieldb", sb.toString(), Store.YES, Index.NO));
    sb.append(" b");
    sb.append(n);
    for (int i = 1; i < numFields; i++) {
      doc.add(new Field("field" + (i+1), sb.toString(), Store.YES, Index.ANALYZED));
    }
    return doc;
  }

  static void modifyIndex(int i, Directory dir) throws IOException {
    switch (i) {
      case 0: {
        if (VERBOSE) {
          System.out.println("TEST: modify index");
        }
        IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)));
        w.setInfoStream(VERBOSE ? System.out : null);
        w.deleteDocuments(new Term("field2", "a11"));
        w.deleteDocuments(new Term("field2", "b30"));
        w.close();
        break;
      }
      case 1: {
        IndexReader reader = IndexReader.open(dir, false);
        Similarity sim = new DefaultSimilarity();
        reader.setNorm(4, "field1", sim.encodeNormValue(123f));
        reader.setNorm(44, "field2", sim.encodeNormValue(222f));
        reader.setNorm(44, "field4", sim.encodeNormValue(22f));
        reader.close();
        break;
      }
      case 2: {
        IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)));
        w.optimize();
        w.close();
        break;
      }
      case 3: {
        IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)));
        w.addDocument(createDocument(101, 4));
        w.optimize();
        w.addDocument(createDocument(102, 4));
        w.addDocument(createDocument(103, 4));
        w.close();
        break;
      }
      case 4: {
        IndexReader reader = IndexReader.open(dir, false);
        Similarity sim = new DefaultSimilarity();
        reader.setNorm(5, "field1", sim.encodeNormValue(123f));
        reader.setNorm(55, "field2", sim.encodeNormValue(222f));
        reader.close();
        break;
      }
      case 5: {
        IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)));
        w.addDocument(createDocument(101, 4));
        w.close();
        break;
      }
    }
  }  
  
  private void assertReaderClosed(IndexReader reader, boolean checkSubReaders, boolean checkNormsClosed) {
    assertEquals(0, reader.getRefCount());
    
    if (checkNormsClosed && reader instanceof SegmentReader) {
      assertTrue(((SegmentReader) reader).normsClosed());
    }
    
    if (checkSubReaders) {
      if (reader instanceof DirectoryReader) {
        IndexReader[] subReaders = reader.getSequentialSubReaders();
        for (int i = 0; i < subReaders.length; i++) {
          assertReaderClosed(subReaders[i], checkSubReaders, checkNormsClosed);
        }
      }
      
      if (reader instanceof MultiReader) {
        IndexReader[] subReaders = reader.getSequentialSubReaders();
        for (int i = 0; i < subReaders.length; i++) {
          assertReaderClosed(subReaders[i], checkSubReaders, checkNormsClosed);
        }
      }
      
      if (reader instanceof ParallelReader) {
        IndexReader[] subReaders = ((ParallelReader) reader).getSubReaders();
        for (int i = 0; i < subReaders.length; i++) {
          assertReaderClosed(subReaders[i], checkSubReaders, checkNormsClosed);
        }
      }
    }
  }

  /*
  private void assertReaderOpen(IndexReader reader) {
    reader.ensureOpen();
    
    if (reader instanceof DirectoryReader) {
      IndexReader[] subReaders = reader.getSequentialSubReaders();
      for (int i = 0; i < subReaders.length; i++) {
        assertReaderOpen(subReaders[i]);
      }
    }
  }
  */

  private void assertRefCountEquals(int refCount, IndexReader reader) {
    assertEquals("Reader has wrong refCount value.", refCount, reader.getRefCount());
  }


  private abstract static class TestReopen {
    protected abstract IndexReader openReader() throws IOException;
    protected abstract void modifyIndex(int i) throws IOException;
  }


  @Override
  public void setUp() throws Exception {
    super.setUp();
    indexDir = _TestUtil.getTempDir("IndexReaderReopen");
  }
  
  public void testCloseOrig() throws Throwable {
    Directory dir = newDirectory();
    createIndex(random, dir, false);
    IndexReader r1 = IndexReader.open(dir, false);
    IndexReader r2 = IndexReader.open(dir, false);
    r2.deleteDocument(0);
    r2.close();

    IndexReader r3 = r1.reopen();
    assertTrue(r1 != r3);
    r1.close();
    try {
      r1.document(2);
      fail("did not hit exception");
    } catch (AlreadyClosedException ace) {
      // expected
    }
    r3.close();
    dir.close();
  }

  public void testDeletes() throws Throwable {
    Directory dir = newDirectory();
    createIndex(random, dir, false); // Create an index with a bunch of docs (1 segment)

    modifyIndex(0, dir); // Get delete bitVector on 1st segment
    modifyIndex(5, dir); // Add a doc (2 segments)

    IndexReader r1 = IndexReader.open(dir, false); // MSR

    modifyIndex(5, dir); // Add another doc (3 segments)

    IndexReader r2 = r1.reopen(); // MSR
    assertTrue(r1 != r2);

    SegmentReader sr1 = (SegmentReader) r1.getSequentialSubReaders()[0]; // Get SRs for the first segment from original
    SegmentReader sr2 = (SegmentReader) r2.getSequentialSubReaders()[0]; // and reopened IRs

    // At this point they share the same BitVector
    assertTrue(sr1.deletedDocs==sr2.deletedDocs);

    r2.deleteDocument(0);

    // r1 should not see the delete
    final Bits r1DelDocs = MultiFields.getDeletedDocs(r1);
    assertFalse(r1DelDocs != null && r1DelDocs.get(0));

    // Now r2 should have made a private copy of deleted docs:
    assertTrue(sr1.deletedDocs!=sr2.deletedDocs);

    r1.close();
    r2.close();
    dir.close();
  }

  public void testDeletes2() throws Throwable {
    Directory dir = newDirectory();
    createIndex(random, dir, false);
    // Get delete bitVector
    modifyIndex(0, dir);
    IndexReader r1 = IndexReader.open(dir, false);

    // Add doc:
    modifyIndex(5, dir);

    IndexReader r2 = r1.reopen();
    assertTrue(r1 != r2);

    IndexReader[] rs2 = r2.getSequentialSubReaders();

    SegmentReader sr1 = getOnlySegmentReader(r1);
    SegmentReader sr2 = (SegmentReader) rs2[0];

    // At this point they share the same BitVector
    assertTrue(sr1.deletedDocs==sr2.deletedDocs);
    final BitVector delDocs = sr1.deletedDocs;
    r1.close();

    r2.deleteDocument(0);
    assertTrue(delDocs==sr2.deletedDocs);
    r2.close();
    dir.close();
  }

  private static class KeepAllCommits implements IndexDeletionPolicy {
    public void onInit(List<? extends IndexCommit> commits) {
    }
    public void onCommit(List<? extends IndexCommit> commits) {
    }
  }

  public void testReopenOnCommit() throws Throwable {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(
        dir,
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)).
            setIndexDeletionPolicy(new KeepAllCommits()).
            setMaxBufferedDocs(-1).
            setMergePolicy(newLogMergePolicy(10))
    );
    for(int i=0;i<4;i++) {
      Document doc = new Document();
      doc.add(newField("id", ""+i, Field.Store.NO, Field.Index.NOT_ANALYZED));
      writer.addDocument(doc);
      Map<String,String> data = new HashMap<String,String>();
      data.put("index", i+"");
      writer.commit(data);
    }
    for(int i=0;i<4;i++) {
      writer.deleteDocuments(new Term("id", ""+i));
      Map<String,String> data = new HashMap<String,String>();
      data.put("index", (4+i)+"");
      writer.commit(data);
    }
    writer.close();

    IndexReader r = IndexReader.open(dir, false);
    assertEquals(0, r.numDocs());

    Collection<IndexCommit> commits = IndexReader.listCommits(dir);
    for (final IndexCommit commit : commits) {
      IndexReader r2 = r.reopen(commit);
      assertTrue(r2 != r);

      // Reader should be readOnly
      try {
        r2.deleteDocument(0);
        fail("no exception hit");
      } catch (UnsupportedOperationException uoe) {
        // expected
      }

      final Map<String,String> s = commit.getUserData();
      final int v;
      if (s.size() == 0) {
        // First commit created by IW
        v = -1;
      } else {
        v = Integer.parseInt(s.get("index"));
      }
      if (v < 4) {
        assertEquals(1+v, r2.numDocs());
      } else {
        assertEquals(7-v, r2.numDocs());
      }
      r.close();
      r = r2;
    }
    r.close();
    dir.close();
  }
}
