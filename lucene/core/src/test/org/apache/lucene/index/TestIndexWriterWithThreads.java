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
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.BaseDirectoryWrapper;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LineFileDocs;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.ThreadInterruptedException;
import org.apache.lucene.util.LuceneTestCase.Slow;

/**
 * MultiThreaded IndexWriter tests
 */
@Slow @LuceneTestCase.SuppressCodecs("SimpleText")
public class TestIndexWriterWithThreads extends LuceneTestCase {

  // Used by test cases below
  private static class IndexerThread extends Thread {

    private final CyclicBarrier syncStart;
    boolean diskFull;
    Throwable error;
    IndexWriter writer;
    boolean noErrors;
    volatile int addCount;

    public IndexerThread(IndexWriter writer, boolean noErrors, CyclicBarrier syncStart) {
      this.writer = writer;
      this.noErrors = noErrors;
      this.syncStart = syncStart;
    }

    @Override
    public void run() {
      try {
        syncStart.await();
      } catch (BrokenBarrierException | InterruptedException e) {
        error = e;
        throw new RuntimeException(e);
      }

      final Document doc = new Document();
      FieldType customType = new FieldType(TextField.TYPE_STORED);
      customType.setStoreTermVectors(true);
      customType.setStoreTermVectorPositions(true);
      customType.setStoreTermVectorOffsets(true);
      
      doc.add(newField("field", "aaa bbb ccc ddd eee fff ggg hhh iii jjj", customType));
      doc.add(new NumericDocValuesField("dv", 5));

      int idUpto = 0;
      int fullCount = 0;

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
        } catch (AlreadyClosedException ace) {
          // OK: abort closes the writer
          break;
        } catch (Throwable t) {
          if (noErrors) {
            System.out.println(Thread.currentThread().getName() + ": ERROR: unexpected Throwable:");
            t.printStackTrace(System.out);
            error = t;
          }
          break;
        }
      } while (true);
    }
  }

  // LUCENE-1130: make sure immediate disk full on creating
  // an IndexWriter (hit during DWPT#updateDocuments()), with
  // multiple threads, is OK:
  public void testImmediateDiskFullWithThreads() throws Exception {

    int NUM_THREADS = 3;
    final int numIterations = TEST_NIGHTLY ? 10 : 1;
    for (int iter=0;iter<numIterations;iter++) {
      if (VERBOSE) {
        System.out.println("\nTEST: iter=" + iter);
      }
      MockDirectoryWrapper dir = newMockDirectory();
      IndexWriter writer = new IndexWriter(
          dir,
          newIndexWriterConfig(new MockAnalyzer(random()))
            .setMaxBufferedDocs(2)
            .setMergeScheduler(new ConcurrentMergeScheduler())
            .setMergePolicy(newLogMergePolicy(4))
            .setCommitOnClose(false)
      );
      ((ConcurrentMergeScheduler) writer.getConfig().getMergeScheduler()).setSuppressExceptions();
      dir.setMaxSizeInBytes(4*1024+20*iter);

      CyclicBarrier syncStart = new CyclicBarrier(NUM_THREADS + 1);
      IndexerThread[] threads = new IndexerThread[NUM_THREADS];
      for (int i = 0; i < NUM_THREADS; i++) {
        threads[i] = new IndexerThread(writer, true, syncStart);
        threads[i].start();
      }
      syncStart.await();

      for (int i = 0; i < NUM_THREADS; i++) {
        // Without fix for LUCENE-1130: one of the
        // threads will hang
        threads[i].join();
        assertTrue("hit unexpected Throwable", threads[i].error == null);
      }

      // Make sure once disk space is avail again, we can
      // cleanly close:
      dir.setMaxSizeInBytes(0);
      try {
        writer.commit();
      } catch (AlreadyClosedException ace) {
        // OK: abort closes the writer
        assertTrue(writer.isDeleterClosed());
      } finally {
        writer.close();
      }
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
      if (VERBOSE) {
        System.out.println("\nTEST: iter=" + iter);
      }
      Directory dir = newDirectory();

      IndexWriter writer = new IndexWriter(
          dir,
          newIndexWriterConfig(new MockAnalyzer(random()))
            .setMaxBufferedDocs(10)
            .setMergeScheduler(new ConcurrentMergeScheduler())
            .setMergePolicy(newLogMergePolicy(4))
            .setCommitOnClose(false)
      );
      ((ConcurrentMergeScheduler) writer.getConfig().getMergeScheduler()).setSuppressExceptions();


      CyclicBarrier syncStart = new CyclicBarrier(NUM_THREADS + 1);
      IndexerThread[] threads = new IndexerThread[NUM_THREADS];
      for (int i = 0; i < NUM_THREADS; i++) {
        threads[i] = new IndexerThread(writer, false, syncStart);
        threads[i].start();
      }
      syncStart.await();

      boolean done = false;
      while (!done) {
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

      if (VERBOSE) {
        System.out.println("\nTEST: now close");
      }
      try {
        writer.commit();
      } finally {
        writer.close();
      }

      // Make sure threads that are adding docs are not hung:
      for(int i=0;i<NUM_THREADS;i++) {
        // Without fix for LUCENE-1130: one of the
        // threads will hang
        threads[i].join();

        // [DW] this is unreachable once join() returns a thread cannot be alive.
        if (threads[i].isAlive())
          fail("thread seems to be hung");
      }

      // Quick test to make sure index is not corrupt:
      IndexReader reader = DirectoryReader.open(dir);
      PostingsEnum tdocs = TestUtil.docs(random(), reader,
          "field",
          new BytesRef("aaa"),
          null,
          0);
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

    for (int iter = 0; iter < 2; iter++) {
      if (VERBOSE) {
        System.out.println("TEST: iter=" + iter);
      }
      MockDirectoryWrapper dir = newMockDirectory();

      IndexWriter writer = new IndexWriter(
          dir,
          newIndexWriterConfig(new MockAnalyzer(random()))
             .setMaxBufferedDocs(2)
             .setMergeScheduler(new ConcurrentMergeScheduler())
             .setMergePolicy(newLogMergePolicy(4))
             .setCommitOnClose(false)
      );
      ((ConcurrentMergeScheduler) writer.getConfig().getMergeScheduler()).setSuppressExceptions();

      CyclicBarrier syncStart = new CyclicBarrier(NUM_THREADS + 1);
      IndexerThread[] threads = new IndexerThread[NUM_THREADS];
      for (int i = 0; i < NUM_THREADS; i++) {
        threads[i] = new IndexerThread(writer, true, syncStart);
        threads[i].start();
      }
      syncStart.await();

      dir.failOn(failure);
      failure.setDoFail();

      for (int i = 0; i < NUM_THREADS; i++) {
        threads[i].join();
        assertTrue("hit unexpected Throwable", threads[i].error == null);
      }

      boolean success = false;
      try {
        writer.commit();
        writer.close();
        success = true;
      } catch (AlreadyClosedException ace) {
        // OK: abort closes the writer
        assertTrue(writer.isDeleterClosed());
      } catch (IOException ioe) {
        writer.rollback();
        failure.clearDoFail();
      }
      if (VERBOSE) {
        System.out.println("TEST: success=" + success);
      }

      if (success) {
        IndexReader reader = DirectoryReader.open(dir);
        final Bits delDocs = MultiBits.getLiveDocs(reader);
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
    MockDirectoryWrapper dir = newMockDirectory();

    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()))
      .setMaxBufferedDocs(2)
      .setMergeScheduler(new ConcurrentMergeScheduler())
      .setCommitOnClose(false);

    if (iwc.getMergeScheduler() instanceof ConcurrentMergeScheduler) {
      iwc.setMergeScheduler(new SuppressingConcurrentMergeScheduler() {
          @Override
          protected boolean isOK(Throwable th) {
            return th instanceof AlreadyClosedException ||
              (th instanceof IllegalStateException && th.getMessage().contains("this writer hit an unrecoverable error"));
          }
        });
    }

    IndexWriter writer = new IndexWriter(dir, iwc);
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
    expectThrows(IOException.class, () -> {
      writer.addDocument(doc);
      writer.addDocument(doc);
      writer.commit();
    });

    failure.clearDoFail();
    expectThrows(AlreadyClosedException.class, () -> {
      writer.addDocument(doc);
      writer.commit();
      writer.close();
    });

    assertTrue(writer.isDeleterClosed());
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

      // Since we throw exc during abort, eg when IW is
      // attempting to delete files, we will leave
      // leftovers: 
      dir.setAssertNoUnrefencedFilesOnClose(false);

      if (doFail) {
        if (callStackContainsAnyOf("abort", "finishDocument") && false == callStackContainsAnyOf("merge", "close")) {
          if (onlyOnce) {
            doFail = false;
          }
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
        if (callStackContains(DefaultIndexingChain.class, "flush")) {
          if (onlyOnce)
            doFail = false;
          //System.out.println(Thread.currentThread().getName() + ": NOW FAIL: onlyOnce=" + onlyOnce);
          //new Throwable().printStackTrace(System.out);
          throw new IOException("now failing on purpose");
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
     try (final Directory dir = newDirectory()) {
       CyclicBarrier syncStart = new CyclicBarrier(2);
       DelayedIndexAndCloseRunnable thread1 = new DelayedIndexAndCloseRunnable(dir, syncStart);
       DelayedIndexAndCloseRunnable thread2 = new DelayedIndexAndCloseRunnable(dir, syncStart);
       thread1.start();
       thread2.start();
       thread1.join();
       thread2.join();

       if (thread1.failure instanceof LockObtainFailedException ||
           thread2.failure instanceof LockObtainFailedException) {
         // We only care about the situation when the two writers succeeded.
         return;
       }

       assertFalse("Failed due to: " + thread1.failure, thread1.failed);
       assertFalse("Failed due to: " + thread2.failure, thread2.failed);

       // now verify that we have two documents in the index
       IndexReader reader = DirectoryReader.open(dir);
       assertEquals("IndexReader should have one document per thread running", 2,
         reader.numDocs());

       reader.close();
     }
  }
  
  static class DelayedIndexAndCloseRunnable extends Thread {
    private final Directory dir;
    boolean failed = false;
    Throwable failure = null;
    private CyclicBarrier syncStart;

    public DelayedIndexAndCloseRunnable(Directory dir,
                                        CyclicBarrier syncStart) {
      this.dir = dir;
      this.syncStart = syncStart;
    }

    @Override
    public void run() {
      try {
        Document doc = new Document();
        Field field = newTextField("field", "testData", Field.Store.YES);
        doc.add(field);

        syncStart.await();
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
        writer.addDocument(doc);
        writer.close();
      } catch (Throwable e) {
        failed = true;
        failure = e;
      }
    }
  }

  // LUCENE-4147
  public void testRollbackAndCommitWithThreads() throws Exception {
    final BaseDirectoryWrapper d = newDirectory();

    final int threadCount = TestUtil.nextInt(random(), 2, 6);

    final AtomicReference<IndexWriter> writerRef = new AtomicReference<>();
    MockAnalyzer analyzer = new MockAnalyzer(random());
    analyzer.setMaxTokenLength(TestUtil.nextInt(random(), 1, IndexWriter.MAX_TERM_LENGTH));

    writerRef.set(new IndexWriter(d, newIndexWriterConfig(analyzer)));
    // Make initial commit so the test doesn't trip "corrupt first commit" when virus checker refuses to delete partial segments_N file:
    writerRef.get().commit();
    final LineFileDocs docs = new LineFileDocs(random());
    final Thread[] threads = new Thread[threadCount];
    final int iters = atLeast(100);
    final AtomicBoolean failed = new AtomicBoolean();
    final Lock rollbackLock = new ReentrantLock();
    final Lock commitLock = new ReentrantLock();
    for(int threadID=0;threadID<threadCount;threadID++) {
      threads[threadID] = new Thread() {
          @Override
          public void run() {
            for(int iter=0;iter<iters && !failed.get();iter++) {
              //final int x = random().nextInt(5);
              final int x = random().nextInt(3);
              try {
                switch(x) {
                case 0:
                  rollbackLock.lock();
                  if (VERBOSE) {
                    System.out.println("\nTEST: " + Thread.currentThread().getName() + ": now rollback");
                  }
                  try {
                    writerRef.get().rollback();
                    if (VERBOSE) {
                      System.out.println("TEST: " + Thread.currentThread().getName() + ": rollback done; now open new writer");
                    }
                    writerRef.set(new IndexWriter(d, newIndexWriterConfig(new MockAnalyzer(random()))));
                  } finally {
                    rollbackLock.unlock();
                  }
                  break;
                case 1:
                  commitLock.lock();
                  if (VERBOSE) {
                    System.out.println("\nTEST: " + Thread.currentThread().getName() + ": now commit");
                  }
                  try {
                    if (random().nextBoolean()) {
                      writerRef.get().prepareCommit();
                    }
                    writerRef.get().commit();
                  } catch (AlreadyClosedException | NullPointerException ace) {
                    // ok
                  } finally {
                    commitLock.unlock();
                  }
                  break;
                case 2:
                  if (VERBOSE) {
                    System.out.println("\nTEST: " + Thread.currentThread().getName() + ": now add");
                  }
                  try {
                    writerRef.get().addDocument(docs.nextDoc());
                  } catch (AlreadyClosedException | NullPointerException | AssertionError ace) {
                    // ok
                  }
                  break;
                }
              } catch (Throwable t) {
                failed.set(true);
                throw new RuntimeException(t);
              }
            }
          }
        };
      threads[threadID].start();
    }

    for(int threadID=0;threadID<threadCount;threadID++) {
      threads[threadID].join();
    }

    assertTrue(!failed.get());
    writerRef.get().close();
    d.close();
  }

  public void testUpdateSingleDocWithThreads() throws Exception {
    stressUpdateSingleDocWithThreads(false, rarely());
  }

  public void testSoftUpdateSingleDocWithThreads() throws Exception {
    stressUpdateSingleDocWithThreads(true, rarely());
  }

  public void stressUpdateSingleDocWithThreads(boolean useSoftDeletes, boolean forceMerge) throws Exception{
    try (Directory dir = newDirectory();
         RandomIndexWriter writer = new RandomIndexWriter(random(), dir,
             newIndexWriterConfig().setMaxBufferedDocs(-1).setRAMBufferSizeMB(0.00001), useSoftDeletes)) {
      int numThreads = TEST_NIGHTLY ? 3 + random().nextInt(3) : 3;
      Thread[] threads = new Thread[numThreads];
      AtomicInteger done = new AtomicInteger(0);
      CyclicBarrier barrier = new CyclicBarrier(threads.length + 1);
      Document doc = new Document();
      doc.add(new StringField("id", "1", Field.Store.NO));
      writer.updateDocument(new Term("id", "1"), doc);
      int itersPerThread = 100 + random().nextInt(2000);
      for (int i = 0; i < threads.length; i++) {
        threads[i] = new Thread(() -> {
          try {
            barrier.await();
            for (int iters = 0; iters < itersPerThread; iters++) {
              Document d = new Document();
              d.add(new StringField("id", "1", Field.Store.NO));
              writer.updateDocument(new Term("id", "1"), d);
            }
          } catch (Exception e) {
            throw new AssertionError(e);
          } finally {
            done.incrementAndGet();
          }
        });
        threads[i].start();
      }
      DirectoryReader open = DirectoryReader.open(writer.w);
      assertEquals(open.numDocs(), 1);
      barrier.await();
      try {
        do {
          if (forceMerge && random().nextBoolean()) {
            writer.forceMerge(1);
          }
          DirectoryReader newReader = DirectoryReader.openIfChanged(open);
          if (newReader != null) {
            open.close();
            open = newReader;
          }
          assertEquals(open.numDocs(), 1);
        } while (done.get() < threads.length);
      } finally {
        open.close();
        for (int i = 0; i < threads.length; i++) {
          threads[i].join();
        }
      }
    }
  }
}
