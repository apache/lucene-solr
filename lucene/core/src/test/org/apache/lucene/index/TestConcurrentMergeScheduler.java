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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestConcurrentMergeScheduler extends LuceneTestCase {
  
  private class FailOnlyOnFlush extends MockDirectoryWrapper.Failure {
    boolean doFail;
    boolean hitExc;

    @Override
    public void setDoFail() {
      this.doFail = true;
      hitExc = false;
    }
    @Override
    public void clearDoFail() {
      this.doFail = false;
    }

    @Override
    public void eval(MockDirectoryWrapper dir)  throws IOException {
      if (doFail && isTestThread()) {
        if (callStackContainsAnyOf("flush") && false == callStackContainsAnyOf("close") && random().nextBoolean()) {
          hitExc = true;
          throw new IOException(Thread.currentThread().getName() + ": now failing during flush");
        }
      }
    }
  }

  // Make sure running BG merges still work fine even when
  // we are hitting exceptions during flushing.
  public void testFlushExceptions() throws IOException {
    MockDirectoryWrapper directory = newMockDirectory();
    FailOnlyOnFlush failure = new FailOnlyOnFlush();
    directory.failOn(failure);
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()))
      .setMaxBufferedDocs(2);
    if (iwc.getMergeScheduler() instanceof ConcurrentMergeScheduler) {
      iwc.setMergeScheduler(new SuppressingConcurrentMergeScheduler() {
          @Override
          protected boolean isOK(Throwable th) {
            return th instanceof AlreadyClosedException ||
              (th instanceof IllegalStateException && th.getMessage().contains("this writer hit an unrecoverable error"));
          }
        });
    }
    IndexWriter writer = new IndexWriter(directory, iwc);
    Document doc = new Document();
    Field idField = newStringField("id", "", Field.Store.YES);
    doc.add(idField);

    outer:
    for(int i=0;i<10;i++) {
      if (VERBOSE) {
        System.out.println("TEST: iter=" + i);
      }

      for(int j=0;j<20;j++) {
        idField.setStringValue(Integer.toString(i*20+j));
        writer.addDocument(doc);
      }

      // must cycle here because sometimes the merge flushes
      // the doc we just added and so there's nothing to
      // flush, and we don't hit the exception
      while(true) {
        writer.addDocument(doc);
        failure.setDoFail();
        try {
          writer.flush(true, true);
          if (failure.hitExc) {
            fail("failed to hit IOException");
          }
        } catch (IOException ioe) {
          if (VERBOSE) {
            ioe.printStackTrace(System.out);
          }
          failure.clearDoFail();
          // make sure we are closed or closing - if we are unlucky a merge does
          // the actual closing for us. this is rare but might happen since the
          // tragicEvent is checked by IFD and that might throw during a merge
          expectThrows(AlreadyClosedException.class, writer::ensureOpen);
          // Abort should have closed the deleter:
          assertTrue(writer.isDeleterClosed());
          writer.close(); // now wait for the close to actually happen if a merge thread did the close.
          break outer;
        }
      }
    }

    assertFalse(DirectoryReader.indexExists(directory));
    directory.close();
  }

  // Test that deletes committed after a merge started and
  // before it finishes, are correctly merged back:
  public void testDeleteMerging() throws IOException {
    Directory directory = newDirectory();

    LogDocMergePolicy mp = new LogDocMergePolicy();
    // Force degenerate merging so we can get a mix of
    // merging of segments with and without deletes at the
    // start:
    mp.setMinMergeDocs(1000);
    IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig(new MockAnalyzer(random()))
                                                      .setMergePolicy(mp));

    Document doc = new Document();
    Field idField = newStringField("id", "", Field.Store.YES);
    doc.add(idField);
    for(int i=0;i<10;i++) {
      if (VERBOSE) {
        System.out.println("\nTEST: cycle");
      }
      for(int j=0;j<100;j++) {
        idField.setStringValue(Integer.toString(i*100+j));
        writer.addDocument(doc);
      }

      int delID = i;
      while(delID < 100*(1+i)) {
        if (VERBOSE) {
          System.out.println("TEST: del " + delID);
        }
        writer.deleteDocuments(new Term("id", ""+delID));
        delID += 10;
      }

      writer.commit();
    }

    writer.close();
    IndexReader reader = DirectoryReader.open(directory);
    // Verify that we did not lose any deletes...
    assertEquals(450, reader.numDocs());
    reader.close();
    directory.close();
  }

  public void testNoExtraFiles() throws IOException {
    Directory directory = newDirectory();
    IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig(new MockAnalyzer(random()))
                                                      .setMaxBufferedDocs(2));

    for(int iter=0;iter<7;iter++) {
      if (VERBOSE) {
        System.out.println("TEST: iter=" + iter);
      }

      for(int j=0;j<21;j++) {
        Document doc = new Document();
        doc.add(newTextField("content", "a b c", Field.Store.NO));
        writer.addDocument(doc);
      }
        
      writer.close();
      TestIndexWriter.assertNoUnreferencedFiles(directory, "testNoExtraFiles");

      // Reopen
      writer = new IndexWriter(directory, newIndexWriterConfig(new MockAnalyzer(random()))
                                            .setOpenMode(OpenMode.APPEND).setMaxBufferedDocs(2));
    }

    writer.close();

    directory.close();
  }

  public void testNoWaitClose() throws IOException {
    Directory directory = newDirectory();
    Document doc = new Document();
    Field idField = newStringField("id", "", Field.Store.YES);
    doc.add(idField);

    IndexWriter writer = new IndexWriter(
        directory,
        newIndexWriterConfig(new MockAnalyzer(random())).
            // Force excessive merging:
            setMaxBufferedDocs(2).
            setMergePolicy(newLogMergePolicy(100)).
            setCommitOnClose(false)
    );

    int numIters = TEST_NIGHTLY ? 10 : 3;
    for(int iter=0;iter<numIters;iter++) {

      for(int j=0;j<201;j++) {
        idField.setStringValue(Integer.toString(iter*201+j));
        writer.addDocument(doc);
      }

      int delID = iter*201;
      for(int j=0;j<20;j++) {
        writer.deleteDocuments(new Term("id", Integer.toString(delID)));
        delID += 5;
      }

      // Force a bunch of merge threads to kick off so we
      // stress out aborting them on close:
      ((LogMergePolicy) writer.getConfig().getMergePolicy()).setMergeFactor(3);
      writer.addDocument(doc);

      try {
        writer.commit();
      } finally {
        writer.close();
      }

      IndexReader reader = DirectoryReader.open(directory);
      assertEquals((1+iter)*182, reader.numDocs());
      reader.close();

      // Reopen
      writer = new IndexWriter(
          directory,
          newIndexWriterConfig(new MockAnalyzer(random())).
              setOpenMode(OpenMode.APPEND).
              setMergePolicy(newLogMergePolicy(100)).
              // Force excessive merging:
              setMaxBufferedDocs(2).
              setCommitOnClose(false)
      );
    }
    writer.close();

    directory.close();
  }

  // LUCENE-4544
  public void testMaxMergeCount() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random())).setCommitOnClose(false);

    final int maxMergeCount = TestUtil.nextInt(random(), 1, 5);
    final int maxMergeThreads = TestUtil.nextInt(random(), 1, maxMergeCount);
    final CountDownLatch enoughMergesWaiting = new CountDownLatch(maxMergeCount);
    final AtomicInteger runningMergeCount = new AtomicInteger(0);
    final AtomicBoolean failed = new AtomicBoolean();

    if (VERBOSE) {
      System.out.println("TEST: maxMergeCount=" + maxMergeCount + " maxMergeThreads=" + maxMergeThreads);
    }

    ConcurrentMergeScheduler cms = new ConcurrentMergeScheduler() {

      @Override
      protected void doMerge(MergeSource mergeSource, MergePolicy.OneMerge merge) throws IOException {
        try {
          // Stall all incoming merges until we see
          // maxMergeCount:
          int count = runningMergeCount.incrementAndGet();
          try {
            assertTrue("count=" + count + " vs maxMergeCount=" + maxMergeCount, count <= maxMergeCount);
            enoughMergesWaiting.countDown();

            // Stall this merge until we see exactly
            // maxMergeCount merges waiting
            while (true) {
              if (enoughMergesWaiting.await(10, TimeUnit.MILLISECONDS) || failed.get()) {
                break;
              }
            }
            // Then sleep a bit to give a chance for the bug
            // (too many pending merges) to appear:
            Thread.sleep(20);
            super.doMerge(mergeSource, merge);
          } finally {
            runningMergeCount.decrementAndGet();
          }
        } catch (Throwable t) {
          failed.set(true);
          mergeSource.onMergeFinished(merge);
          throw new RuntimeException(t);
        }
      }
      };
    cms.setMaxMergesAndThreads(maxMergeCount, maxMergeThreads);
    iwc.setMergeScheduler(cms);
    iwc.setMaxBufferedDocs(2);

    TieredMergePolicy tmp = new TieredMergePolicy();
    iwc.setMergePolicy(tmp);
    tmp.setMaxMergeAtOnce(2);
    tmp.setSegmentsPerTier(2);

    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(newField("field", "field", TextField.TYPE_NOT_STORED));
    while(enoughMergesWaiting.getCount() != 0 && !failed.get()) {
      for(int i=0;i<10;i++) {
        w.addDocument(doc);
      }
    }
    try {
      w.commit();
    } finally {
      w.close();
    }
    dir.close();
  }

  private static class TrackingCMS extends ConcurrentMergeScheduler {
    long totMergedBytes;
    CountDownLatch atLeastOneMerge;

    public TrackingCMS(CountDownLatch atLeastOneMerge) {
      setMaxMergesAndThreads(5, 5);
      this.atLeastOneMerge = atLeastOneMerge;
    }

    @Override
    public void doMerge(MergeSource mergeSource, MergePolicy.OneMerge merge) throws IOException {
      totMergedBytes += merge.totalBytesSize();
      atLeastOneMerge.countDown();
      super.doMerge(mergeSource, merge);
    }
  }

  public void testTotalBytesSize() throws Exception {
    Directory d = newDirectory();
    if (d instanceof MockDirectoryWrapper) {
      ((MockDirectoryWrapper)d).setThrottling(MockDirectoryWrapper.Throttling.NEVER);
    }
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setMaxBufferedDocs(5);
    CountDownLatch atLeastOneMerge = new CountDownLatch(1);
    iwc.setMergeScheduler(new TrackingCMS(atLeastOneMerge));
    if (TestUtil.getPostingsFormat("id").equals("SimpleText")) {
      // no
      iwc.setCodec(TestUtil.alwaysPostingsFormat(TestUtil.getDefaultPostingsFormat()));
    }
    IndexWriter w = new IndexWriter(d, iwc);
    for(int i=0;i<1000;i++) {
      Document doc = new Document();
      doc.add(new StringField("id", ""+i, Field.Store.NO));
      w.addDocument(doc);

      if (random().nextBoolean()) {
        w.deleteDocuments(new Term("id", ""+random().nextInt(i+1)));
      }
    }
    atLeastOneMerge.await();
    assertTrue(((TrackingCMS) w.getConfig().getMergeScheduler()).totMergedBytes != 0);
    w.close();
    d.close();
  }

  public void testInvalidMaxMergeCountAndThreads() throws Exception {
    ConcurrentMergeScheduler cms = new ConcurrentMergeScheduler();
    expectThrows(IllegalArgumentException.class, () -> {
      cms.setMaxMergesAndThreads(ConcurrentMergeScheduler.AUTO_DETECT_MERGES_AND_THREADS, 3);
    });
    expectThrows(IllegalArgumentException.class, () -> {
      cms.setMaxMergesAndThreads(3, ConcurrentMergeScheduler.AUTO_DETECT_MERGES_AND_THREADS);
    });
  }

  public void testLiveMaxMergeCount() throws Exception {
    Directory d = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    TieredMergePolicy tmp = new TieredMergePolicy();
    tmp.setSegmentsPerTier(1000);
    tmp.setMaxMergeAtOnce(1000);
    tmp.setMaxMergeAtOnceExplicit(10);
    iwc.setMergePolicy(tmp);
    iwc.setMaxBufferedDocs(2);
    iwc.setRAMBufferSizeMB(-1);

    final AtomicInteger maxRunningMergeCount = new AtomicInteger();

    ConcurrentMergeScheduler cms = new ConcurrentMergeScheduler() {

        final AtomicInteger runningMergeCount = new AtomicInteger();

        @Override
        public void doMerge(MergeSource mergeSource, MergePolicy.OneMerge merge) throws IOException {
          int count = runningMergeCount.incrementAndGet();
          // evil?
          synchronized (this) {
            if (count > maxRunningMergeCount.get()) {
              maxRunningMergeCount.set(count);
            }
          }
          try {
            super.doMerge(mergeSource, merge);
          } finally {
            runningMergeCount.decrementAndGet();
          }

        }
      };

    assertEquals(ConcurrentMergeScheduler.AUTO_DETECT_MERGES_AND_THREADS, cms.getMaxMergeCount());
    assertEquals(ConcurrentMergeScheduler.AUTO_DETECT_MERGES_AND_THREADS, cms.getMaxThreadCount());

    cms.setMaxMergesAndThreads(5, 3);

    iwc.setMergeScheduler(cms);

    IndexWriter w = new IndexWriter(d, iwc);
    // Makes 100 segments
    for(int i=0;i<200;i++) {
      w.addDocument(new Document());
    }

    // No merges should have run so far, because TMP has high segmentsPerTier:
    assertEquals(0, maxRunningMergeCount.get());
    w.forceMerge(1);

    // At most 5 merge threads should have launched at once:
    assertTrue("maxRunningMergeCount=" + maxRunningMergeCount, maxRunningMergeCount.get() <= 5);
    maxRunningMergeCount.set(0);

    // Makes another 100 segments
    for(int i=0;i<200;i++) {
      w.addDocument(new Document());
    }

    ((ConcurrentMergeScheduler) w.getConfig().getMergeScheduler()).setMaxMergesAndThreads(1, 1);
    w.forceMerge(1);

    // At most 1 merge thread should have launched at once:
    assertEquals(1, maxRunningMergeCount.get());

    w.close();
    d.close();
  }

  // LUCENE-6063
  public void testMaybeStallCalled() throws Exception {
    final AtomicBoolean wasCalled = new AtomicBoolean();
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()))
        .setMergePolicy(new LogByteSizeMergePolicy());
    iwc.setMergeScheduler(new ConcurrentMergeScheduler() {
        @Override
        protected boolean maybeStall(MergeSource mergeSource) {
          wasCalled.set(true);
          return true;
        }
      });
    IndexWriter w = new IndexWriter(dir, iwc);
    w.addDocument(new Document());
    w.flush();
    w.addDocument(new Document());
    w.forceMerge(1);
    assertTrue(wasCalled.get());
    w.close();
    dir.close();
  }

  // LUCENE-6094
  public void testHangDuringRollback() throws Throwable {
    Directory dir = newMockDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setMaxBufferedDocs(2);
    LogDocMergePolicy mp = new LogDocMergePolicy();
    iwc.setMergePolicy(mp);
    mp.setMergeFactor(2);
    final CountDownLatch mergeStart = new CountDownLatch(1);
    final CountDownLatch mergeFinish = new CountDownLatch(1);
    ConcurrentMergeScheduler cms = new ConcurrentMergeScheduler() {
        @Override
        protected void doMerge(MergeSource mergeSource, MergePolicy.OneMerge merge) throws IOException {
          mergeStart.countDown();
          try {
            mergeFinish.await();
          } catch (InterruptedException ie) {
            throw new RuntimeException(ie);
          }
          super.doMerge(mergeSource, merge);
        }
      };
    cms.setMaxMergesAndThreads(1, 1);
    iwc.setMergeScheduler(cms);

    final IndexWriter w = new IndexWriter(dir, iwc);
    
    w.addDocument(new Document());
    w.addDocument(new Document());
    // flush

    w.addDocument(new Document());
    w.addDocument(new Document());
    // flush + merge

    // Wait for merge to kick off
    mergeStart.await();

    new Thread() {
      @Override
      public void run() {
        try {
          w.addDocument(new Document());
          w.addDocument(new Document());
          // flush

          w.addDocument(new Document());
          // W/o the fix for LUCENE-6094 we would hang forever here:
          w.addDocument(new Document());
          // flush + merge
          
          // Now allow first merge to finish:
          mergeFinish.countDown();

        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }.start();

    while (w.getDocStats().numDocs != 8) {
      Thread.sleep(10);
    }

    w.rollback();
    dir.close();
  }

  public void testDynamicDefaults() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    ConcurrentMergeScheduler cms = new ConcurrentMergeScheduler();
    assertEquals(ConcurrentMergeScheduler.AUTO_DETECT_MERGES_AND_THREADS, cms.getMaxMergeCount());
    assertEquals(ConcurrentMergeScheduler.AUTO_DETECT_MERGES_AND_THREADS, cms.getMaxThreadCount());
    iwc.setMergeScheduler(cms);
    iwc.setMaxBufferedDocs(2);
    LogMergePolicy lmp = newLogMergePolicy();
    lmp.setMergeFactor(2);
    iwc.setMergePolicy(lmp);

    IndexWriter w = new IndexWriter(dir, iwc);
    w.addDocument(new Document());
    w.addDocument(new Document());
    // flush

    w.addDocument(new Document());
    w.addDocument(new Document());
    // flush + merge

    // CMS should have now set true values:
    assertTrue(cms.getMaxMergeCount() != ConcurrentMergeScheduler.AUTO_DETECT_MERGES_AND_THREADS);
    assertTrue(cms.getMaxThreadCount() != ConcurrentMergeScheduler.AUTO_DETECT_MERGES_AND_THREADS);
    w.close();
    dir.close();
  }

  public void testResetToAutoDefault() throws Exception {
    ConcurrentMergeScheduler cms = new ConcurrentMergeScheduler();
    assertEquals(ConcurrentMergeScheduler.AUTO_DETECT_MERGES_AND_THREADS, cms.getMaxMergeCount());
    assertEquals(ConcurrentMergeScheduler.AUTO_DETECT_MERGES_AND_THREADS, cms.getMaxThreadCount());
    cms.setMaxMergesAndThreads(4, 3);
    assertEquals(4, cms.getMaxMergeCount());
    assertEquals(3, cms.getMaxThreadCount());

    expectThrows(IllegalArgumentException.class, () -> {
      cms.setMaxMergesAndThreads(ConcurrentMergeScheduler.AUTO_DETECT_MERGES_AND_THREADS, 4);
    });

    expectThrows(IllegalArgumentException.class, () -> {
      cms.setMaxMergesAndThreads(4, ConcurrentMergeScheduler.AUTO_DETECT_MERGES_AND_THREADS);
    });

    cms.setMaxMergesAndThreads(ConcurrentMergeScheduler.AUTO_DETECT_MERGES_AND_THREADS, ConcurrentMergeScheduler.AUTO_DETECT_MERGES_AND_THREADS);
    assertEquals(ConcurrentMergeScheduler.AUTO_DETECT_MERGES_AND_THREADS, cms.getMaxMergeCount());
    assertEquals(ConcurrentMergeScheduler.AUTO_DETECT_MERGES_AND_THREADS, cms.getMaxThreadCount());
  }

  public void testSpinningDefaults() throws Exception {
    ConcurrentMergeScheduler cms = new ConcurrentMergeScheduler();
    cms.setDefaultMaxMergesAndThreads(true);
    assertEquals(1, cms.getMaxThreadCount());
    assertEquals(6, cms.getMaxMergeCount());
  }

  public void testAutoIOThrottleGetter() throws Exception {
    ConcurrentMergeScheduler cms = new ConcurrentMergeScheduler();
    cms.disableAutoIOThrottle();
    assertFalse(cms.getAutoIOThrottle());
    cms.enableAutoIOThrottle();
    assertTrue(cms.getAutoIOThrottle());
  }

  public void testNonSpinningDefaults() throws Exception {
    ConcurrentMergeScheduler cms = new ConcurrentMergeScheduler();
    cms.setDefaultMaxMergesAndThreads(false);
    int threadCount = cms.getMaxThreadCount();
    assertTrue(threadCount >= 1);
    assertTrue(threadCount <= 4);
    assertEquals(5+threadCount, cms.getMaxMergeCount());
  }

  // LUCENE-6197
  public void testNoStallMergeThreads() throws Exception {
    MockDirectoryWrapper dir = newMockDirectory();

    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    iwc.setMergePolicy(NoMergePolicy.INSTANCE);
    iwc.setMaxBufferedDocs(2);
    IndexWriter w = new IndexWriter(dir, iwc);
    int numDocs = TEST_NIGHTLY ? 1000 : 100;
    for(int i=0;i<numDocs;i++) {
      Document doc = new Document();
      doc.add(newStringField("field", ""+i, Field.Store.YES));
      w.addDocument(doc);
    }
    w.close();

    iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    AtomicBoolean failed = new AtomicBoolean();
    ConcurrentMergeScheduler cms = new ConcurrentMergeScheduler() {
        @Override
        protected void doStall() {
          if (Thread.currentThread().getName().startsWith("Lucene Merge Thread")) {
            failed.set(true);
          }
          super.doStall();
        }
      };
    cms.setMaxMergesAndThreads(2, 1);
    iwc.setMergeScheduler(cms);
    iwc.setMaxBufferedDocs(2);

    w = new IndexWriter(dir, iwc);
    w.forceMerge(1);
    w.close();
    dir.close();

    assertFalse(failed.get());
  }

  /*
   * This test tries to produce 2 merges running concurrently with 2 segments per merge. While these
   * merges run we kick off a forceMerge that puts a pending merge in the queue but waits for things to happen.
   * While we do this we reduce maxMergeCount to 1. If concurrency in CMS is not right the forceMerge will wait forever
   * since none of the currently running merges picks up the pending merge. This test fails every time.
   */
  public void testChangeMaxMergeCountyWhileForceMerge() throws IOException, InterruptedException {
    int numIters = TEST_NIGHTLY ? 100 : 10;
    for (int iters = 0; iters < numIters; iters++) {
      LogDocMergePolicy mp = new LogDocMergePolicy();
      mp.setMergeFactor(2);
      CountDownLatch forceMergeWaits = new CountDownLatch(1);
      CountDownLatch mergeThreadsStartAfterWait = new CountDownLatch(1);
      CountDownLatch mergeThreadsArrived = new CountDownLatch(2);
      InfoStream stream = new InfoStream() {
        @Override
        public void message(String component, String message) {
          if ("TP".equals(component) && "mergeMiddleStart".equals(message)) {
            mergeThreadsArrived.countDown();
            try {
              mergeThreadsStartAfterWait.await();
            } catch (InterruptedException e) {
              throw new AssertionError(e);
            }
          } else if ("TP".equals(component) && "forceMergeBeforeWait".equals(message)) {
            forceMergeWaits.countDown();
          }
        }

        @Override
        public boolean isEnabled(String component) {
          return "TP".equals(component);
        }

        @Override
        public void close() {
        }
      };
      try (Directory dir = newDirectory();
           IndexWriter writer = new IndexWriter(dir,
               new IndexWriterConfig().setMergeScheduler(new ConcurrentMergeScheduler())
                   .setMergePolicy(mp).setInfoStream(stream)) {
             @Override
             protected boolean isEnableTestPoints() {
               return true;
             }
           }) {
        Thread t = new Thread(() -> {
          try {
            writer.forceMerge(1);
          } catch (IOException e) {
            throw new AssertionError(e);
          }
        });
        ConcurrentMergeScheduler cms = (ConcurrentMergeScheduler) writer.getConfig().getMergeScheduler();
        cms.setMaxMergesAndThreads(2, 2);
        try {
          for (int i = 0; i < 4; i++) {
            Document document = new Document();
            document.add(new TextField("foo", "the quick brown fox jumps over the lazy dog", Field.Store.YES));
            document.add(new TextField("bar", RandomStrings.randomRealisticUnicodeOfLength(random(), 20), Field.Store.YES));
            writer.addDocument(document);
            writer.flush();
          }
          assertEquals(writer.cloneSegmentInfos().toString(), 4, writer.getSegmentCount());
          mergeThreadsArrived.await();
          t.start();
          forceMergeWaits.await();
          cms.setMaxMergesAndThreads(1, 1);
        } finally {
          mergeThreadsStartAfterWait.countDown();
        }

        while (t.isAlive()) {
          t.join(10);
          if (cms.mergeThreadCount() == 0 && writer.hasPendingMerges()) {
            fail("writer has pending merges but no CMS threads are running");
          }
        }
        assertEquals(1, writer.getSegmentCount());
      }
    }
  }
}
