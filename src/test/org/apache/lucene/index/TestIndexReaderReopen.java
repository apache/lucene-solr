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
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.LuceneTestCase;

public class TestIndexReaderReopen extends LuceneTestCase {
    
  private File indexDir;

  public void testReopen() throws Exception {
    final Directory dir1 = new RAMDirectory();
    
    createIndex(dir1, false);
    performDefaultTests(new TestReopen() {

      protected void modifyIndex(int i) throws IOException {
        TestIndexReaderReopen.modifyIndex(i, dir1);
      }

      protected IndexReader openReader() throws IOException {
        return IndexReader.open(dir1);
      }
      
    });
    
    final Directory dir2 = new RAMDirectory();
    
    createIndex(dir2, true);
    performDefaultTests(new TestReopen() {

      protected void modifyIndex(int i) throws IOException {
        TestIndexReaderReopen.modifyIndex(i, dir2);
      }

      protected IndexReader openReader() throws IOException {
        return IndexReader.open(dir2);
      }
      
    });
  }
  
  public void testParallelReaderReopen() throws Exception {
    final Directory dir1 = new RAMDirectory();
    createIndex(dir1, true);
    final Directory dir2 = new RAMDirectory();
    createIndex(dir2, true);
    
    performDefaultTests(new TestReopen() {

      protected void modifyIndex(int i) throws IOException {
        TestIndexReaderReopen.modifyIndex(i, dir1);
        TestIndexReaderReopen.modifyIndex(i, dir2);
      }

      protected IndexReader openReader() throws IOException {
        ParallelReader pr = new ParallelReader();
        pr.add(IndexReader.open(dir1));
        pr.add(IndexReader.open(dir2));
        return pr;
      }
      
    });
    
    final Directory dir3 = new RAMDirectory();
    createIndex(dir3, true);
    final Directory dir4 = new RAMDirectory();
    createIndex(dir4, true);

    performTestsWithExceptionInReopen(new TestReopen() {

      protected void modifyIndex(int i) throws IOException {
        TestIndexReaderReopen.modifyIndex(i, dir3);
        TestIndexReaderReopen.modifyIndex(i, dir4);
      }

      protected IndexReader openReader() throws IOException {
        ParallelReader pr = new ParallelReader();
        pr.add(IndexReader.open(dir3));
        pr.add(IndexReader.open(dir4));
        pr.add(new FilterIndexReader(IndexReader.open(dir3)));
        return pr;
      }
      
    });
  }

  // LUCENE-1228: IndexWriter.commit() does not update the index version
  // populate an index in iterations.
  // at the end of every iteration, commit the index and reopen/recreate the reader.
  // in each iteration verify the work of previous iteration. 
  // try this once with reopen once recreate, on both RAMDir and FSDir.
  public void testCommitReopenFS () throws IOException {
    Directory dir = FSDirectory.getDirectory(indexDir);
    doTestReopenWithCommit(dir, true);
  }
  public void testCommitRecreateFS () throws IOException {
    Directory dir = FSDirectory.getDirectory(indexDir);
    doTestReopenWithCommit(dir, false);
  }
  public void testCommitReopenRAM () throws IOException {
    Directory dir = new RAMDirectory();
    doTestReopenWithCommit(dir, true);
  }
  public void testCommitRecreateRAM () throws IOException {
    Directory dir = new RAMDirectory();
    doTestReopenWithCommit(dir, false);
  }

  private void doTestReopenWithCommit (Directory dir, boolean withReopen) throws IOException {
    IndexWriter iwriter = new IndexWriter(dir, new KeywordAnalyzer(), true, MaxFieldLength.LIMITED);
    iwriter.setMergeScheduler(new SerialMergeScheduler());
    IndexReader reader = IndexReader.open(dir);
    try {
      int M = 3;
      for (int i=0; i<4; i++) {
        for (int j=0; j<M; j++) {
          Document doc = new Document();
          doc.add(new Field("id", i+"_"+j, Store.YES, Index.NOT_ANALYZED));
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
          reader = IndexReader.open(dir);
        }
      }
    } finally {
      iwriter.close();
      reader.close();
    }
  }
  
  public void testMultiReaderReopen() throws Exception {
    final Directory dir1 = new RAMDirectory();
    createIndex(dir1, true);
    final Directory dir2 = new RAMDirectory();
    createIndex(dir2, true);

    performDefaultTests(new TestReopen() {

      protected void modifyIndex(int i) throws IOException {
        TestIndexReaderReopen.modifyIndex(i, dir1);
        TestIndexReaderReopen.modifyIndex(i, dir2);
      }

      protected IndexReader openReader() throws IOException {
        return new MultiReader(new IndexReader[] 
                        {IndexReader.open(dir1), 
                         IndexReader.open(dir2)});
      }
      
    });
    
    final Directory dir3 = new RAMDirectory();
    createIndex(dir3, true);
    final Directory dir4 = new RAMDirectory();
    createIndex(dir4, true);

    performTestsWithExceptionInReopen(new TestReopen() {

      protected void modifyIndex(int i) throws IOException {
        TestIndexReaderReopen.modifyIndex(i, dir3);
        TestIndexReaderReopen.modifyIndex(i, dir4);
      }

      protected IndexReader openReader() throws IOException {
        return new MultiReader(new IndexReader[] 
                        {IndexReader.open(dir3), 
                         IndexReader.open(dir4),
                         new FilterIndexReader(IndexReader.open(dir3))});
      }
      
    });

  }

  public void testMixedReaders() throws Exception {
    final Directory dir1 = new RAMDirectory();
    createIndex(dir1, true);
    final Directory dir2 = new RAMDirectory();
    createIndex(dir2, true);
    final Directory dir3 = new RAMDirectory();
    createIndex(dir3, false);
    final Directory dir4 = new RAMDirectory();
    createIndex(dir4, true);
    final Directory dir5 = new RAMDirectory();
    createIndex(dir5, false);
    
    performDefaultTests(new TestReopen() {

      protected void modifyIndex(int i) throws IOException {
        // only change norms in this index to maintain the same number of docs for each of ParallelReader's subreaders
        if (i == 1) TestIndexReaderReopen.modifyIndex(i, dir1);  
        
        TestIndexReaderReopen.modifyIndex(i, dir4);
        TestIndexReaderReopen.modifyIndex(i, dir5);
      }

      protected IndexReader openReader() throws IOException {
        ParallelReader pr = new ParallelReader();
        pr.add(IndexReader.open(dir1));
        pr.add(IndexReader.open(dir2));
        MultiReader mr = new MultiReader(new IndexReader[] {
            IndexReader.open(dir3), IndexReader.open(dir4)});
        return new MultiReader(new IndexReader[] {
           pr, mr, IndexReader.open(dir5)});
      }
    });
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
    index1 = couple.newReader;
    IndexReader index2_refreshed = couple.refreshedReader;
    index2.close();
    
    // test if refreshed reader and newly opened reader return equal results
    TestIndexReader.assertIndexEquals(index1, index2_refreshed);
    
    index1.close();
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
      Directory dir1 = new RAMDirectory();
      createIndex(dir1, true);
     
      IndexReader reader0 = IndexReader.open(dir1);
      assertRefCountEquals(1, reader0);

      assertTrue(reader0 instanceof MultiSegmentReader);
      SegmentReader[] subReaders0 = ((MultiSegmentReader) reader0).getSubReaders();
      for (int i = 0; i < subReaders0.length; i++) {
        assertRefCountEquals(1, subReaders0[i]);
      }
      
      // delete first document, so that only one of the subReaders have to be re-opened
      IndexReader modifier = IndexReader.open(dir1);
      modifier.deleteDocument(0);
      modifier.close();
      
      IndexReader reader1 = refreshReader(reader0, true).refreshedReader;
      assertTrue(reader1 instanceof MultiSegmentReader);
      SegmentReader[] subReaders1 = ((MultiSegmentReader) reader1).getSubReaders();
      assertEquals(subReaders0.length, subReaders1.length);
      
      for (int i = 0; i < subReaders0.length; i++) {
        assertRefCountEquals(2, subReaders0[i]);
        if (subReaders0[i] != subReaders1[i]) {
          assertRefCountEquals(1, subReaders1[i]);
        }
      }

      // delete first document, so that only one of the subReaders have to be re-opened
      modifier = IndexReader.open(dir1);
      modifier.deleteDocument(1);
      modifier.close();

      IndexReader reader2 = refreshReader(reader1, true).refreshedReader;
      assertTrue(reader2 instanceof MultiSegmentReader);
      SegmentReader[] subReaders2 = ((MultiSegmentReader) reader2).getSubReaders();
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
            assertRefCountEquals(3, subReaders2[i]);
            assertRefCountEquals(2, subReaders0[i]);
          } else {
            assertRefCountEquals(3, subReaders0[i]);
            assertRefCountEquals(1, subReaders1[i]);
          }
        }
      }
      
      IndexReader reader3 = refreshReader(reader0, true).refreshedReader;
      assertTrue(reader3 instanceof MultiSegmentReader);
      SegmentReader[] subReaders3 = ((MultiSegmentReader) reader3).getSubReaders();
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
    }
  }


  public void testReferenceCountingMultiReader() throws IOException {
    for (int mode = 0; mode <=1; mode++) {
      Directory dir1 = new RAMDirectory();
      createIndex(dir1, false);
      Directory dir2 = new RAMDirectory();
      createIndex(dir2, true);
      
      IndexReader reader1 = IndexReader.open(dir1);
      assertRefCountEquals(1, reader1);
      
      IndexReader multiReader1 = new MultiReader(new IndexReader[] {reader1, IndexReader.open(dir2)}, (mode == 0));
      modifyIndex(0, dir2);
      assertRefCountEquals(1 + mode, reader1);
      
      IndexReader multiReader2 = multiReader1.reopen();
      // index1 hasn't changed, so multiReader2 should share reader1 now with multiReader1
      assertRefCountEquals(2 + mode, reader1);
      
      modifyIndex(0, dir1);
      IndexReader reader2 = reader1.reopen();
      assertRefCountEquals(3 + mode, reader1);
      
      modifyIndex(1, dir1);
      IndexReader reader3 = reader2.reopen();
      assertRefCountEquals(4 + mode, reader1);
      assertRefCountEquals(1, reader2);
      
      multiReader1.close();
      assertRefCountEquals(3 + mode, reader1);
      
      multiReader1.close();
      assertRefCountEquals(3 + mode, reader1);
      
      reader1.close();
      assertRefCountEquals(3, reader1);
      
      multiReader2.close();
      assertRefCountEquals(2, reader1);
      
      multiReader2.close();
      assertRefCountEquals(2, reader1);
      
      reader3.close();
      assertRefCountEquals(1, reader1);
      assertReaderOpen(reader1);
      
      reader2.close();
      assertRefCountEquals(0, reader1);
      assertReaderClosed(reader1, true, false);
      
      reader2.close();
      assertRefCountEquals(0, reader1);
      
      reader3.close();
      assertRefCountEquals(0, reader1);
      assertReaderClosed(reader1, true, true);
    }

  }

  public void testReferenceCountingParallelReader() throws IOException {
    for (int mode = 0; mode <=1; mode++) {
      Directory dir1 = new RAMDirectory();
      createIndex(dir1, false);
      Directory dir2 = new RAMDirectory();
      createIndex(dir2, true);
      
      IndexReader reader1 = IndexReader.open(dir1);
      assertRefCountEquals(1, reader1);
      
      ParallelReader parallelReader1 = new ParallelReader(mode == 0);
      parallelReader1.add(reader1);
      parallelReader1.add(IndexReader.open(dir2));
      modifyIndex(1, dir2);
      assertRefCountEquals(1 + mode, reader1);
      
      IndexReader parallelReader2 = parallelReader1.reopen();
      // index1 hasn't changed, so parallelReader2 should share reader1 now with multiReader1
      assertRefCountEquals(2 + mode, reader1);
      
      modifyIndex(0, dir1);
      modifyIndex(0, dir2);
      IndexReader reader2 = reader1.reopen();
      assertRefCountEquals(3 + mode, reader1);
      
      modifyIndex(4, dir1);
      IndexReader reader3 = reader2.reopen();
      assertRefCountEquals(4 + mode, reader1);
      assertRefCountEquals(1, reader2);
      
      parallelReader1.close();
      assertRefCountEquals(3 + mode, reader1);
      
      parallelReader1.close();
      assertRefCountEquals(3 + mode, reader1);
      
      reader1.close();
      assertRefCountEquals(3, reader1);
      
      parallelReader2.close();
      assertRefCountEquals(2, reader1);
      
      parallelReader2.close();
      assertRefCountEquals(2, reader1);
      
      reader3.close();
      assertRefCountEquals(1, reader1);
      assertReaderOpen(reader1);
      
      reader2.close();
      assertRefCountEquals(0, reader1);
      assertReaderClosed(reader1, true, false);
      
      reader2.close();
      assertRefCountEquals(0, reader1);
      
      reader3.close();
      assertRefCountEquals(0, reader1);
      assertReaderClosed(reader1, true, true);
    }

  }
  
  public void testNormsRefCounting() throws IOException {
    Directory dir1 = new RAMDirectory();
    createIndex(dir1, false);
    
    SegmentReader reader1 = (SegmentReader) IndexReader.open(dir1);
    IndexReader modifier = IndexReader.open(dir1);
    modifier.deleteDocument(0);
    modifier.close();
    
    SegmentReader reader2 = (SegmentReader) reader1.reopen();
    modifier = IndexReader.open(dir1);
    modifier.setNorm(1, "field1", 50);
    modifier.setNorm(1, "field2", 50);
    modifier.close();
    
    SegmentReader reader3 = (SegmentReader) reader2.reopen();
    modifier = IndexReader.open(dir1);
    modifier.deleteDocument(2);
    modifier.close();
    SegmentReader reader4 = (SegmentReader) reader3.reopen();

    modifier = IndexReader.open(dir1);
    modifier.deleteDocument(3);
    modifier.close();
    SegmentReader reader5 = (SegmentReader) reader3.reopen();
    
    // Now reader2-reader5 references reader1. reader1 and reader2
    // share the same norms. reader3, reader4, reader5 also share norms.
    assertRefCountEquals(5, reader1);
    assertFalse(reader1.normsClosed());
    reader1.close();
    assertRefCountEquals(4, reader1);
    assertFalse(reader1.normsClosed());
    reader2.close();
    assertRefCountEquals(3, reader1);
    // now the norms for field1 and field2 should be closed
    assertTrue(reader1.normsClosed("field1"));
    assertTrue(reader1.normsClosed("field2"));
    // but the norms for field3 and field4 should still be open
    assertFalse(reader1.normsClosed("field3"));
    assertFalse(reader1.normsClosed("field4"));
    
    reader3.close();
    assertRefCountEquals(2, reader1);
    assertFalse(reader3.normsClosed());
    reader5.close();
    assertRefCountEquals(1, reader1);
    assertFalse(reader3.normsClosed());
    reader4.close();
    assertRefCountEquals(0, reader1);
    
    // and now all norms that reader1 used should be closed
    assertTrue(reader1.normsClosed());
    
    // now that reader3, reader4 and reader5 are closed,
    // the norms that those three readers shared should be
    // closed as well
    assertTrue(reader3.normsClosed());
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
  }
  
  public void testThreadSafety() throws Exception {
    final Directory dir = new RAMDirectory();
    final int n = 150;

    IndexWriter writer = new IndexWriter(dir, new StandardAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
    for (int i = 0; i < n; i++) {
      writer.addDocument(createDocument(i, 3));
    }
    writer.optimize();
    writer.close();

    final TestReopen test = new TestReopen() {      
      protected void modifyIndex(int i) throws IOException {
        if (i % 3 == 0) {
          IndexReader modifier = IndexReader.open(dir);
          modifier.setNorm(i, "field1", 50);
          modifier.close();
        } else if (i % 3 == 1) {
          IndexReader modifier = IndexReader.open(dir);
          modifier.deleteDocument(i);
          modifier.close();
        } else {
          IndexWriter modifier = new IndexWriter(dir, new StandardAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
          modifier.addDocument(createDocument(n + i, 6));
          modifier.close();
        }
      }

      protected IndexReader openReader() throws IOException {
        return IndexReader.open(dir);
      }      
    };
    
    final List readers = Collections.synchronizedList(new ArrayList());
    IndexReader firstReader = IndexReader.open(dir);
    IndexReader reader = firstReader;
    final Random rnd = new Random();
    
    ReaderThread[] threads = new ReaderThread[n];
    final Set readersToClose = Collections.synchronizedSet(new HashSet());
    
    for (int i = 0; i < n; i++) {
      if (i % 10 == 0) {
        IndexReader refreshed = reader.reopen();
        if (refreshed != reader) {
          readersToClose.add(reader);
        }
        reader = refreshed;
      }
      final IndexReader r = reader;
      
      final int index = i;    
      
      ReaderThreadTask task;
      
      if (i < 20 ||( i >=50 && i < 70) || i > 90) {
        task = new ReaderThreadTask() {
          
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
                
                
                IndexSearcher searcher = new IndexSearcher(refreshed);
                ScoreDoc[] hits = searcher.search(
                    new TermQuery(new Term("field1", "a" + rnd.nextInt(refreshed.maxDoc()))),
                    null, 1000).scoreDocs;
                if (hits.length > 0) {
                  searcher.doc(hits[0].doc);
                }
                
                // r might have changed because this is not a 
                // synchronized method. However we don't want
                // to make it synchronized to test 
                // thread-safety of IndexReader.close().
                // That's why we add refreshed also to 
                // readersToClose, because double closing is fine
                if (refreshed != r) {
                  refreshed.close();
                }
                readersToClose.add(refreshed);
              }
              try {
                synchronized(this) {
                  wait(1000);
                }
              } catch (InterruptedException e) {}
            }
          }
          
        };
      } else {
        task = new ReaderThreadTask() {
          public void run() throws Exception {
            while (!stopped) {
              int numReaders = readers.size();
              if (numReaders > 0) {
                ReaderCouple c = (ReaderCouple) readers.get(rnd.nextInt(numReaders));
                TestIndexReader.assertIndexEquals(c.newReader, c.refreshedReader);
              }
              
              try {
                synchronized(this) {
                  wait(100);
                }
              } catch (InterruptedException e) {}
            }
                        
          }
          
        };
      }
      
      threads[i] = new ReaderThread(task);
      threads[i].start();
    }
    
    synchronized(this) {
      try {
        wait(15000);
      } catch(InterruptedException e) {}
    }
    
    for (int i = 0; i < n; i++) {
      if (threads[i] != null) {
        threads[i].stopThread();
      }
    }
    
    for (int i = 0; i < n; i++) {
      if (threads[i] != null) {
        try {
          threads[i].join();
          if (threads[i].error != null) {
            String msg = "Error occurred in thread " + threads[i].getName() + ":\n" + threads[i].error.getMessage();
            fail(msg);
          }
        } catch (InterruptedException e) {}
      }
      
    }
    
    Iterator it = readersToClose.iterator();
    while (it.hasNext()) {
      ((IndexReader) it.next()).close();
    }
    
    firstReader.close();
    reader.close();
    
    it = readersToClose.iterator();
    while (it.hasNext()) {
      assertReaderClosed((IndexReader) it.next(), true, true);
    }

    assertReaderClosed(reader, true, true);
    assertReaderClosed(firstReader, true, true);
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
    protected boolean stopped;
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
  
  private ReaderCouple refreshReader(IndexReader reader, TestReopen test, int modify, boolean hasChanges) throws IOException {
    synchronized (createReaderMutex) {
      IndexReader r = null;
      if (test != null) {
        test.modifyIndex(modify);
        r = test.openReader();
      }
      
      IndexReader refreshed = reader.reopen();
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
  
  private static void createIndex(Directory dir, boolean multiSegment) throws IOException {
    IndexWriter w = new IndexWriter(dir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
    
    w.setMergePolicy(new LogDocMergePolicy());
    
    for (int i = 0; i < 100; i++) {
      w.addDocument(createDocument(i, 4));
      if (multiSegment && (i % 10) == 0) {
        w.flush();
      }
    }
    
    if (!multiSegment) {
      w.optimize();
    }
    
    w.close();
    
    IndexReader r = IndexReader.open(dir);
    if (multiSegment) {
      assertTrue(r instanceof MultiSegmentReader);
    } else {
      assertTrue(r instanceof SegmentReader);
    }
    r.close();
  }

  private static Document createDocument(int n, int numFields) {
    StringBuffer sb = new StringBuffer();
    Document doc = new Document();
    sb.append("a");
    sb.append(n);
    doc.add(new Field("field1", sb.toString(), Store.YES, Index.ANALYZED));
    sb.append(" b");
    sb.append(n);
    for (int i = 1; i < numFields; i++) {
      doc.add(new Field("field" + (i+1), sb.toString(), Store.YES, Index.ANALYZED));
    }
    return doc;
  }

  private static void modifyIndex(int i, Directory dir) throws IOException {
    switch (i) {
      case 0: {
        IndexWriter w = new IndexWriter(dir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
        w.deleteDocuments(new Term("field2", "a11"));
        w.deleteDocuments(new Term("field2", "b30"));
        w.close();
        break;
      }
      case 1: {
        IndexReader reader = IndexReader.open(dir);
        reader.setNorm(4, "field1", 123);
        reader.setNorm(44, "field2", 222);
        reader.setNorm(44, "field4", 22);
        reader.close();
        break;
      }
      case 2: {
        IndexWriter w = new IndexWriter(dir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
        w.optimize();
        w.close();
        break;
      }
      case 3: {
        IndexWriter w = new IndexWriter(dir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
        w.addDocument(createDocument(101, 4));
        w.optimize();
        w.addDocument(createDocument(102, 4));
        w.addDocument(createDocument(103, 4));
        w.close();
        break;
      }
      case 4: {
        IndexReader reader = IndexReader.open(dir);
        reader.setNorm(5, "field1", 123);
        reader.setNorm(55, "field2", 222);
        reader.close();
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
      if (reader instanceof MultiSegmentReader) {
        SegmentReader[] subReaders = ((MultiSegmentReader) reader).getSubReaders();
        for (int i = 0; i < subReaders.length; i++) {
          assertReaderClosed(subReaders[i], checkSubReaders, checkNormsClosed);
        }
      }
      
      if (reader instanceof MultiReader) {
        IndexReader[] subReaders = ((MultiReader) reader).getSubReaders();
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

  private void assertReaderOpen(IndexReader reader) {
    reader.ensureOpen();
    
    if (reader instanceof MultiSegmentReader) {
      SegmentReader[] subReaders = ((MultiSegmentReader) reader).getSubReaders();
      for (int i = 0; i < subReaders.length; i++) {
        assertReaderOpen(subReaders[i]);
      }
    }
  }

  private void assertRefCountEquals(int refCount, IndexReader reader) {
    assertEquals("Reader has wrong refCount value.", refCount, reader.getRefCount());
  }


  private abstract static class TestReopen {
    protected abstract IndexReader openReader() throws IOException;
    protected abstract void modifyIndex(int i) throws IOException;
  }


  protected void setUp() throws Exception {
    // TODO Auto-generated method stub
    super.setUp();
    String tempDir = System.getProperty("java.io.tmpdir");
    if (tempDir == null)
      throw new IOException("java.io.tmpdir undefined, cannot run test");
    indexDir = new File(tempDir, "IndexReaderReopen");
  }
  
  // LUCENE-1453
  public void testFSDirectoryReopen() throws CorruptIndexException, IOException {
    Directory dir1 = FSDirectory.getDirectory(indexDir);
    createIndex(dir1, false);
    dir1.close();

    IndexReader ir = IndexReader.open(indexDir);
    modifyIndex(3, ir.directory());
    IndexReader newIr = ir.reopen();
    modifyIndex(3, newIr.directory());
    IndexReader newIr2 = newIr.reopen();
    modifyIndex(3, newIr2.directory());
    IndexReader newIr3 = newIr2.reopen();
    
    ir.close();
    newIr.close();
    newIr2.close();
    
    // shouldn't throw Directory AlreadyClosedException
    modifyIndex(3, newIr3.directory());
    newIr3.close();
  }

  // LUCENE-1453
  public void testFSDirectoryReopen2() throws CorruptIndexException, IOException {

    String tempDir = System.getProperty("java.io.tmpdir");
    if (tempDir == null)
      throw new IOException("java.io.tmpdir undefined, cannot run test");
    File indexDir2 = new File(tempDir, "IndexReaderReopen2");

    Directory dir1 = FSDirectory.getDirectory(indexDir2);
    createIndex(dir1, false);

    IndexReader lastReader = IndexReader.open(indexDir2);
    
    Random r = new Random(42);
    for(int i=0;i<10;i++) {
      int mod = r.nextInt(5);
      modifyIndex(mod, lastReader.directory());
      IndexReader reader = lastReader.reopen();
      if (reader != lastReader) {
        lastReader.close();
        lastReader = reader;
      }
    }
    lastReader.close();

    // Make sure we didn't pick up too many incRef's along
    // the way -- this close should be the final close:
    dir1.close();

    try {
      dir1.list();
      fail("did not hit AlreadyClosedException");
    } catch (AlreadyClosedException ace) {
      // expected
    }
  }
}
