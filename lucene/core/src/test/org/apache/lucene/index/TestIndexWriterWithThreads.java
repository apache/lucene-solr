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

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.ThreadInterruptedException;
import org.apache.lucene.util._TestUtil;

/**
 * MultiThreaded IndexWriter tests
 */
public class TestIndexWriterWithThreads extends LuceneTestCase {

  // Used by test cases below
  private class IndexerThread extends Thread {

    boolean diskFull;
    Throwable error;
    AlreadyClosedException ace;
    IndexWriter writer;
    boolean noErrors;
    volatile int addCount;

    public IndexerThread(IndexWriter writer, boolean noErrors) {
      this.writer = writer;
      this.noErrors = noErrors;
    }

    @Override
    public void run() {

      final Document doc = new Document();
      FieldType customType = new FieldType(TextField.TYPE_STORED);
      customType.setStoreTermVectors(true);
      customType.setStoreTermVectorPositions(true);
      customType.setStoreTermVectorOffsets(true);
      
      doc.add(newField("field", "aaa bbb ccc ddd eee fff ggg hhh iii jjj", customType));

      int idUpto = 0;
      int fullCount = 0;
      final long stopTime = System.currentTimeMillis() + 200;

      do {
        try {
          writer.updateDocument(new Term("id", ""+(idUpto++)), doc);
          addCount++;
        } catch (IOException ioe) {
          if (VERBOSE) {
            System.out.println("TEST: expected exc:");
            ioe.printStackTrace(System.out);
          }
          //System.out.println(Thread.currentThread().getName() + ": hit exc");
          //ioe.printStackTrace(System.out);
          if (ioe.getMessage().startsWith("fake disk full at") ||
              ioe.getMessage().equals("now failing on purpose")) {
            diskFull = true;
            try {
              Thread.sleep(1);
            } catch (InterruptedException ie) {
              throw new ThreadInterruptedException(ie);
            }
            if (fullCount++ >= 5)
              break;
          } else {
            if (noErrors) {
              System.out.println(Thread.currentThread().getName() + ": ERROR: unexpected IOException:");
              ioe.printStackTrace(System.out);
              error = ioe;
            }
            break;
          }
        } catch (Throwable t) {
          //t.printStackTrace(System.out);
          if (noErrors) {
            System.out.println(Thread.currentThread().getName() + ": ERROR: unexpected Throwable:");
            t.printStackTrace(System.out);
            error = t;
          }
          break;
        }
      } while(System.currentTimeMillis() < stopTime);
    }
  }

  // LUCENE-1130: make sure immediate disk full on creating
  // an IndexWriter (hit during DW.ThreadState.init()), with
  // multiple threads, is OK:
  public void testImmediateDiskFullWithThreads() throws Exception {

    int NUM_THREADS = 3;
    final int numIterations = TEST_NIGHTLY ? 10 : 3;
    for(int iter=0;iter<numIterations;iter++) {
      if (VERBOSE) {
        System.out.println("\nTEST: iter=" + iter);
      }
      MockDirectoryWrapper dir = newDirectory();
      IndexWriter writer = new IndexWriter(
          dir,
          newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).
              setMaxBufferedDocs(2).
              setMergeScheduler(new ConcurrentMergeScheduler()).
              setMergePolicy(newLogMergePolicy(4))
      );
      ((ConcurrentMergeScheduler) writer.getConfig().getMergeScheduler()).setSuppressExceptions();
      dir.setMaxSizeInBytes(4*1024+20*iter);

      IndexerThread[] threads = new IndexerThread[NUM_THREADS];

      for(int i=0;i<NUM_THREADS;i++)
        threads[i] = new IndexerThread(writer, true);

      for(int i=0;i<NUM_THREADS;i++)
        threads[i].start();

      for(int i=0;i<NUM_THREADS;i++) {
        // Without fix for LUCENE-1130: one of the
        // threads will hang
        threads[i].join();
        assertTrue("hit unexpected Throwable", threads[i].error == null);
      }

      // Make sure once disk space is avail again, we can
      // cleanly close:
      dir.setMaxSizeInBytes(0);
      writer.close(false);
      dir.close();
    }
  }


  // LUCENE-1130: make sure we can close() even while
  // threads are trying to add documents.  Strictly
  // speaking, this isn't valid us of Lucene's APIs, but we
  // still want to be robust to this case:
  public void testCloseWithThreads() throws Exception {
    int NUM_THREADS = 3;
    int numIterations = TEST_NIGHTLY ? 7 : 3;
    for(int iter=0;iter<numIterations;iter++) {
      Directory dir = newDirectory();

      IndexWriter writer = new IndexWriter(
          dir,
          newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).
              setMaxBufferedDocs(10).
              setMergeScheduler(new ConcurrentMergeScheduler()).
              setMergePolicy(newLogMergePolicy(4))
      );
      ((ConcurrentMergeScheduler) writer.getConfig().getMergeScheduler()).setSuppressExceptions();

      IndexerThread[] threads = new IndexerThread[NUM_THREADS];

      for(int i=0;i<NUM_THREADS;i++)
        threads[i] = new IndexerThread(writer, false);

      for(int i=0;i<NUM_THREADS;i++)
        threads[i].start();

      boolean done = false;
      while(!done) {
        Thread.sleep(100);
        for(int i=0;i<NUM_THREADS;i++)
          // only stop when at least one thread has added a doc
          if (threads[i].addCount > 0) {
            done = true;
            break;
          } else if (!threads[i].isAlive()) {
            fail("thread failed before indexing a single document");
          }
      }

      writer.close(false);

      // Make sure threads that are adding docs are not hung:
      for(int i=0;i<NUM_THREADS;i++) {
        // Without fix for LUCENE-1130: one of the
        // threads will hang
        threads[i].join();
        if (threads[i].isAlive())
          fail("thread seems to be hung");
      }

      // Quick test to make sure index is not corrupt:
      IndexReader reader = IndexReader.open(dir);
      DocsEnum tdocs = _TestUtil.docs(random(), reader,
                                      "field",
                                      new BytesRef("aaa"),
                                      MultiFields.getLiveDocs(reader),
                                      null,
                                      false);
      int count = 0;
      while(tdocs.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
        count++;
      }
      assertTrue(count > 0);
      reader.close();

      dir.close();
    }
  }

  // Runs test, with multiple threads, using the specific
  // failure to trigger an IOException
  public void _testMultipleThreadsFailure(MockDirectoryWrapper.Failure failure) throws Exception {

    int NUM_THREADS = 3;

    for(int iter=0;iter<2;iter++) {
      if (VERBOSE) {
        System.out.println("TEST: iter=" + iter);
      }
      MockDirectoryWrapper dir = newDirectory();

      IndexWriter writer = new IndexWriter(
          dir,
          newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).
              setMaxBufferedDocs(2).
              setMergeScheduler(new ConcurrentMergeScheduler()).
              setMergePolicy(newLogMergePolicy(4))
      );
      ((ConcurrentMergeScheduler) writer.getConfig().getMergeScheduler()).setSuppressExceptions();

      IndexerThread[] threads = new IndexerThread[NUM_THREADS];

      for(int i=0;i<NUM_THREADS;i++)
        threads[i] = new IndexerThread(writer, true);

      for(int i=0;i<NUM_THREADS;i++)
        threads[i].start();

      Thread.sleep(10);

      dir.failOn(failure);
      failure.setDoFail();

      for(int i=0;i<NUM_THREADS;i++) {
        threads[i].join();
        assertTrue("hit unexpected Throwable", threads[i].error == null);
      }

      boolean success = false;
      try {
        writer.close(false);
        success = true;
      } catch (IOException ioe) {
        failure.clearDoFail();
        writer.close(false);
      }

      if (success) {
        IndexReader reader = IndexReader.open(dir);
        final Bits delDocs = MultiFields.getLiveDocs(reader);
        for(int j=0;j<reader.maxDoc();j++) {
          if (delDocs == null || !delDocs.get(j)) {
            reader.document(j);
            reader.getTermVectors(j);
          }
        }
        reader.close();
      }

      dir.close();
    }
  }

  // Runs test, with one thread, using the specific failure
  // to trigger an IOException
  public void _testSingleThreadFailure(MockDirectoryWrapper.Failure failure) throws IOException {
    MockDirectoryWrapper dir = newDirectory();

    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random()))
      .setMaxBufferedDocs(2).setMergeScheduler(new ConcurrentMergeScheduler()));
    final Document doc = new Document();
    FieldType customType = new FieldType(TextField.TYPE_STORED);
    customType.setStoreTermVectors(true);
    customType.setStoreTermVectorPositions(true);
    customType.setStoreTermVectorOffsets(true);
    doc.add(newField("field", "aaa bbb ccc ddd eee fff ggg hhh iii jjj", customType));

    for(int i=0;i<6;i++)
      writer.addDocument(doc);

    dir.failOn(failure);
    failure.setDoFail();
    try {
      writer.addDocument(doc);
      writer.addDocument(doc);
      writer.commit();
      fail("did not hit exception");
    } catch (IOException ioe) {
    }
    failure.clearDoFail();
    writer.addDocument(doc);
    writer.close(false);
    dir.close();
  }

  // Throws IOException during FieldsWriter.flushDocument and during DocumentsWriter.abort
  private static class FailOnlyOnAbortOrFlush extends MockDirectoryWrapper.Failure {
    private boolean onlyOnce;
    public FailOnlyOnAbortOrFlush(boolean onlyOnce) {
      this.onlyOnce = onlyOnce;
    }
    @Override
    public void eval(MockDirectoryWrapper dir)  throws IOException {
      if (doFail) {
        StackTraceElement[] trace = new Exception().getStackTrace();
        boolean sawAbortOrFlushDoc = false;
        boolean sawClose = false;
        for (int i = 0; i < trace.length; i++) {
          if ("abort".equals(trace[i].getMethodName()) ||
              "finishDocument".equals(trace[i].getMethodName())) {
            sawAbortOrFlushDoc = true;
          }
          if ("close".equals(trace[i].getMethodName())) {
            sawClose = true;
          }
        }
        if (sawAbortOrFlushDoc && !sawClose) {
          if (onlyOnce)
            doFail = false;
          //System.out.println(Thread.currentThread().getName() + ": now fail");
          //new Throwable().printStackTrace(System.out);
          throw new IOException("now failing on purpose");
        }
      }
    }
  }



  // LUCENE-1130: make sure initial IOException, and then 2nd
  // IOException during rollback(), is OK:
  public void testIOExceptionDuringAbort() throws IOException {
    _testSingleThreadFailure(new FailOnlyOnAbortOrFlush(false));
  }

  // LUCENE-1130: make sure initial IOException, and then 2nd
  // IOException during rollback(), is OK:
  public void testIOExceptionDuringAbortOnlyOnce() throws IOException {
    _testSingleThreadFailure(new FailOnlyOnAbortOrFlush(true));
  }

  // LUCENE-1130: make sure initial IOException, and then 2nd
  // IOException during rollback(), with multiple threads, is OK:
  public void testIOExceptionDuringAbortWithThreads() throws Exception {
    _testMultipleThreadsFailure(new FailOnlyOnAbortOrFlush(false));
  }

  // LUCENE-1130: make sure initial IOException, and then 2nd
  // IOException during rollback(), with multiple threads, is OK:
  public void testIOExceptionDuringAbortWithThreadsOnlyOnce() throws Exception {
    _testMultipleThreadsFailure(new FailOnlyOnAbortOrFlush(true));
  }

  // Throws IOException during DocumentsWriter.writeSegment
  private static class FailOnlyInWriteSegment extends MockDirectoryWrapper.Failure {
    private boolean onlyOnce;
    public FailOnlyInWriteSegment(boolean onlyOnce) {
      this.onlyOnce = onlyOnce;
    }
    @Override
    public void eval(MockDirectoryWrapper dir)  throws IOException {
      if (doFail) {
        StackTraceElement[] trace = new Exception().getStackTrace();
        for (int i = 0; i < trace.length; i++) {
          if ("flush".equals(trace[i].getMethodName()) && "org.apache.lucene.index.DocFieldProcessor".equals(trace[i].getClassName())) {
            if (onlyOnce)
              doFail = false;
            throw new IOException("now failing on purpose");
          }
        }
      }
    }
  }

  // LUCENE-1130: test IOException in writeSegment
  public void testIOExceptionDuringWriteSegment() throws IOException {
    _testSingleThreadFailure(new FailOnlyInWriteSegment(false));
  }

  // LUCENE-1130: test IOException in writeSegment
  public void testIOExceptionDuringWriteSegmentOnlyOnce() throws IOException {
    _testSingleThreadFailure(new FailOnlyInWriteSegment(true));
  }

  // LUCENE-1130: test IOException in writeSegment, with threads
  public void testIOExceptionDuringWriteSegmentWithThreads() throws Exception {
    _testMultipleThreadsFailure(new FailOnlyInWriteSegment(false));
  }

  // LUCENE-1130: test IOException in writeSegment, with threads
  public void testIOExceptionDuringWriteSegmentWithThreadsOnlyOnce() throws Exception {
    _testMultipleThreadsFailure(new FailOnlyInWriteSegment(true));
  }
  
  //  LUCENE-3365: Test adding two documents with the same field from two different IndexWriters 
  //  that we attempt to open at the same time.  As long as the first IndexWriter completes
  //  and closes before the second IndexWriter time's out trying to get the Lock,
  //  we should see both documents
  public void testOpenTwoIndexWritersOnDifferentThreads() throws IOException, InterruptedException {
     final MockDirectoryWrapper dir = newDirectory();
     CountDownLatch oneIWConstructed = new CountDownLatch(1);
     DelayedIndexAndCloseRunnable thread1 = new DelayedIndexAndCloseRunnable(
         dir, oneIWConstructed);
     DelayedIndexAndCloseRunnable thread2 = new DelayedIndexAndCloseRunnable(
         dir, oneIWConstructed);

     thread1.start();
     thread2.start();
     oneIWConstructed.await();

     thread1.startIndexing();
     thread2.startIndexing();

     thread1.join();
     thread2.join();
     
     assertFalse("Failed due to: " + thread1.failure, thread1.failed);
     assertFalse("Failed due to: " + thread2.failure, thread2.failed);
     // now verify that we have two documents in the index
     IndexReader reader = IndexReader.open(dir);
     assertEquals("IndexReader should have one document per thread running", 2,
         reader.numDocs());
     
     reader.close();
     dir.close();
  }
  
   static class DelayedIndexAndCloseRunnable extends Thread {
     private final Directory dir;
     boolean failed = false;
     Throwable failure = null;
     private final CountDownLatch startIndexing = new CountDownLatch(1);
     private CountDownLatch iwConstructed;

     public DelayedIndexAndCloseRunnable(Directory dir,
         CountDownLatch iwConstructed) {
       this.dir = dir;
       this.iwConstructed = iwConstructed;
     }

     public void startIndexing() {
       this.startIndexing.countDown();
     }

     @Override
     public void run() {
       try {
         Document doc = new Document();
         Field field = newField("field", "testData", TextField.TYPE_STORED);
         doc.add(field);
         IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
             TEST_VERSION_CURRENT, new MockAnalyzer(random())));
         iwConstructed.countDown();
         startIndexing.await();
         writer.addDocument(doc);
         writer.close();
       } catch (Throwable e) {
         failed = true;
         failure = e;
         failure.printStackTrace(System.out);
         return;
       }
     }
   }
}
