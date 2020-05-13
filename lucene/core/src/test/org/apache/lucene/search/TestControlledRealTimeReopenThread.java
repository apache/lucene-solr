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
package org.apache.lucene.search;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.ThreadedIndexingAndSearchingTestCase;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NRTCachingDirectory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.ThreadInterruptedException;

@SuppressCodecs({ "SimpleText", "Direct" })
public class TestControlledRealTimeReopenThread extends ThreadedIndexingAndSearchingTestCase {

  // Not guaranteed to reflect deletes:
  private SearcherManager nrtNoDeletes;

  // Is guaranteed to reflect deletes:
  private SearcherManager nrtDeletes;

  private IndexWriter genWriter;

  private ControlledRealTimeReopenThread<IndexSearcher> nrtDeletesThread;
  private ControlledRealTimeReopenThread<IndexSearcher> nrtNoDeletesThread;

  private final ThreadLocal<Long> lastGens = new ThreadLocal<>();
  private boolean warmCalled;

  public void testControlledRealTimeReopenThread() throws Exception {
    runTest("TestControlledRealTimeReopenThread");
  }

  @Override
  protected IndexSearcher getFinalSearcher() throws Exception  {
    if (VERBOSE) {
      System.out.println("TEST: finalSearcher maxGen=" + maxGen);
    }
    nrtDeletesThread.waitForGeneration(maxGen);
    return nrtDeletes.acquire();
  }

  @Override
  protected Directory getDirectory(Directory in) {
    // Randomly swap in NRTCachingDir
    if (random().nextBoolean()) {
      if (VERBOSE) {
        System.out.println("TEST: wrap NRTCachingDir");
      }

      return new NRTCachingDirectory(in, 5.0, 60.0);
    } else {
      return in;
    }
  }

  @Override
  protected void updateDocuments(Term id, List<? extends Iterable<? extends IndexableField>> docs) throws Exception {
    final long gen = genWriter.updateDocuments(id, docs);

    // Randomly verify the update "took":
    if (random().nextInt(20) == 2) {
      if (VERBOSE) {
        System.out.println(Thread.currentThread().getName() + ": nrt: verify updateDocuments " + id + " gen=" + gen);
      }
      nrtDeletesThread.waitForGeneration(gen);
      assertTrue(gen <= nrtDeletesThread.getSearchingGen());
      final IndexSearcher s = nrtDeletes.acquire();
      if (VERBOSE) {
        System.out.println(Thread.currentThread().getName() + ": nrt: got deletes searcher=" + s);
      }
      try {
        assertEquals("generation: " + gen, docs.size(), s.search(new TermQuery(id), 10).totalHits.value);
      } finally {
        nrtDeletes.release(s);
      }
    }
    
    lastGens.set(gen);
  }

  @Override
  protected void addDocuments(Term id, List<? extends Iterable<? extends IndexableField>> docs) throws Exception {
    final long gen = genWriter.addDocuments(docs);
    // Randomly verify the add "took":
    if (random().nextInt(20) == 2) {
      if (VERBOSE) {
        System.out.println(Thread.currentThread().getName() + ": nrt: verify addDocuments " + id + " gen=" + gen);
      }
      nrtNoDeletesThread.waitForGeneration(gen);
      assertTrue(gen <= nrtNoDeletesThread.getSearchingGen());
      final IndexSearcher s = nrtNoDeletes.acquire();
      if (VERBOSE) {
        System.out.println(Thread.currentThread().getName() + ": nrt: got noDeletes searcher=" + s);
      }
      try {
        assertEquals("generation: " + gen, docs.size(), s.search(new TermQuery(id), 10).totalHits.value);
      } finally {
        nrtNoDeletes.release(s);
      }
    }
    lastGens.set(gen);
  }

  @Override
  protected void addDocument(Term id, Iterable<? extends IndexableField> doc) throws Exception {
    final long gen = genWriter.addDocument(doc);

    // Randomly verify the add "took":
    if (random().nextInt(20) == 2) {
      if (VERBOSE) {
        System.out.println(Thread.currentThread().getName() + ": nrt: verify addDocument " + id + " gen=" + gen);
      }
      nrtNoDeletesThread.waitForGeneration(gen);
      assertTrue(gen <= nrtNoDeletesThread.getSearchingGen());
      final IndexSearcher s = nrtNoDeletes.acquire();
      if (VERBOSE) {
        System.out.println(Thread.currentThread().getName() + ": nrt: got noDeletes searcher=" + s);
      }
      try {
        assertEquals("generation: " + gen, 1, s.search(new TermQuery(id), 10).totalHits.value);
      } finally {
        nrtNoDeletes.release(s);
      }
    }
    lastGens.set(gen);
  }

  @Override
  protected void updateDocument(Term id, Iterable<? extends IndexableField> doc) throws Exception {
    final long gen = genWriter.updateDocument(id, doc);
    // Randomly verify the udpate "took":
    if (random().nextInt(20) == 2) {
      if (VERBOSE) {
        System.out.println(Thread.currentThread().getName() + ": nrt: verify updateDocument " + id + " gen=" + gen);
      }
      nrtDeletesThread.waitForGeneration(gen);
      assertTrue(gen <= nrtDeletesThread.getSearchingGen());
      final IndexSearcher s = nrtDeletes.acquire();
      if (VERBOSE) {
        System.out.println(Thread.currentThread().getName() + ": nrt: got deletes searcher=" + s);
      }
      try {
        assertEquals("generation: " + gen, 1, s.search(new TermQuery(id), 10).totalHits.value);
      } finally {
        nrtDeletes.release(s);
      }
    }
    lastGens.set(gen);
  }

  @Override
  protected void deleteDocuments(Term id) throws Exception {
    final long gen = genWriter.deleteDocuments(id);
    // randomly verify the delete "took":
    if (random().nextInt(20) == 7) {
      if (VERBOSE) {
        System.out.println(Thread.currentThread().getName() + ": nrt: verify deleteDocuments " + id + " gen=" + gen);
      }
      nrtDeletesThread.waitForGeneration(gen);
      assertTrue(gen <= nrtDeletesThread.getSearchingGen());
      final IndexSearcher s = nrtDeletes.acquire();
      if (VERBOSE) {
        System.out.println(Thread.currentThread().getName() + ": nrt: got deletes searcher=" + s);
      }
      try {
        assertEquals(0, s.search(new TermQuery(id), 10).totalHits.value);
      } finally {
        nrtDeletes.release(s);
      }
    }
    lastGens.set(gen);
  }

  @Override
  protected void doAfterWriter(final ExecutorService es) throws Exception {
    final double minReopenSec = 0.01 + 0.05 * random().nextDouble();
    final double maxReopenSec = minReopenSec * (1.0 + 10 * random().nextDouble());

    if (VERBOSE) {
      System.out.println("TEST: make SearcherManager maxReopenSec=" + maxReopenSec + " minReopenSec=" + minReopenSec);
    }

    genWriter = writer;

    final SearcherFactory sf = new SearcherFactory() {
        @Override
        public IndexSearcher newSearcher(IndexReader r, IndexReader previous) throws IOException {
          TestControlledRealTimeReopenThread.this.warmCalled = true;
          IndexSearcher s = new IndexSearcher(r, es);
          s.search(new TermQuery(new Term("body", "united")), 10);
          return s;
        }
      };

    nrtNoDeletes = new SearcherManager(writer, false, false, sf);
    nrtDeletes = new SearcherManager(writer, sf);
                         
    nrtDeletesThread = new ControlledRealTimeReopenThread<>(genWriter, nrtDeletes, maxReopenSec, minReopenSec);
    nrtDeletesThread.setName("NRTDeletes Reopen Thread");
    nrtDeletesThread.setPriority(Math.min(Thread.currentThread().getPriority()+2, Thread.MAX_PRIORITY));
    nrtDeletesThread.setDaemon(true);
    nrtDeletesThread.start();

    nrtNoDeletesThread = new ControlledRealTimeReopenThread<>(genWriter, nrtNoDeletes, maxReopenSec, minReopenSec);
    nrtNoDeletesThread.setName("NRTNoDeletes Reopen Thread");
    nrtNoDeletesThread.setPriority(Math.min(Thread.currentThread().getPriority()+2, Thread.MAX_PRIORITY));
    nrtNoDeletesThread.setDaemon(true);
    nrtNoDeletesThread.start();
  }

  @Override
  protected void doAfterIndexingThreadDone() {
    Long gen = lastGens.get();
    if (gen != null) {
      addMaxGen(gen);
    }
  }

  private long maxGen = -1;

  private synchronized void addMaxGen(long gen) {
    maxGen = Math.max(gen, maxGen);
  }

  @Override
  protected void doSearching(ExecutorService es, long stopTime) throws Exception {
    runSearchThreads(stopTime);
  }

  @Override
  protected IndexSearcher getCurrentSearcher() throws Exception {
    // Test doesn't assert deletions until the end, so we
    // can randomize whether dels must be applied
    final SearcherManager nrt;
    if (random().nextBoolean()) {
      nrt = nrtDeletes;
    } else {
      nrt = nrtNoDeletes;
    }

    return nrt.acquire();
  }

  @Override
  protected void releaseSearcher(IndexSearcher s) throws Exception {
    // NOTE: a bit iffy... technically you should release
    // against the same SearcherManager you acquired from... but
    // both impls just decRef the underlying reader so we
    // can get away w/ cheating:
    nrtNoDeletes.release(s);
  }

  @Override
  protected void doClose() throws Exception {
    assertTrue(warmCalled);
    if (VERBOSE) {
      System.out.println("TEST: now close SearcherManagers");
    }
    nrtDeletesThread.close();
    nrtDeletes.close();
    nrtNoDeletesThread.close();
    nrtNoDeletes.close();
  }
  
  /*
   * LUCENE-3528 - NRTManager hangs in certain situations 
   */
  public void testThreadStarvationNoDeleteNRTReader() throws IOException, InterruptedException {
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    conf.setMergePolicy(NoMergePolicy.INSTANCE);
    Directory d = newDirectory();
    final CountDownLatch latch = new CountDownLatch(1);
    final CountDownLatch signal = new CountDownLatch(1);

    LatchedIndexWriter writer = new LatchedIndexWriter(d, conf, latch, signal);
    final SearcherManager manager = new SearcherManager(writer, false, false, null);
    Document doc = new Document();
    doc.add(newTextField("test", "test", Field.Store.YES));
    writer.addDocument(doc);
    manager.maybeRefresh();
    Thread t = new Thread() {
      @Override
      public void run() {
        try {
          signal.await();
          manager.maybeRefresh();
          writer.deleteDocuments(new TermQuery(new Term("foo", "barista")));
          manager.maybeRefresh(); // kick off another reopen so we inc. the internal gen
        } catch (Exception e) {
          e.printStackTrace();
        } finally {
          latch.countDown(); // let the add below finish
        }
      }
    };
    t.start();
    writer.waitAfterUpdate = true; // wait in addDocument to let some reopens go through

    final long lastGen = writer.updateDocument(new Term("foo", "bar"), doc); // once this returns the doc is already reflected in the last reopen

    // We now eagerly resolve deletes so the manager should see it after update:
    assertTrue(manager.isSearcherCurrent());
    
    IndexSearcher searcher = manager.acquire();
    try {
      assertEquals(2, searcher.getIndexReader().numDocs());
    } finally {
      manager.release(searcher);
    }
    final ControlledRealTimeReopenThread<IndexSearcher> thread = new ControlledRealTimeReopenThread<>(writer, manager, 0.01, 0.01);
    thread.start(); // start reopening
    if (VERBOSE) {
      System.out.println("waiting now for generation " + lastGen);
    }
    
    final AtomicBoolean finished = new AtomicBoolean(false);
    Thread waiter = new Thread() {
      @Override
      public void run() {
        try {
          thread.waitForGeneration(lastGen);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(ie);
        }
        finished.set(true);
      }
    };
    waiter.start();
    manager.maybeRefresh();
    waiter.join(1000);
    if (!finished.get()) {
      waiter.interrupt();
      fail("thread deadlocked on waitForGeneration");
    }
    thread.close();
    thread.join();
    writer.close();
    IOUtils.close(manager, d);
  }
  
  public static class LatchedIndexWriter extends IndexWriter {

    private CountDownLatch latch;
    boolean waitAfterUpdate = false;
    private CountDownLatch signal;

    public LatchedIndexWriter(Directory d, IndexWriterConfig conf,
        CountDownLatch latch, CountDownLatch signal)
        throws IOException {
      super(d, conf);
      this.latch = latch;
      this.signal = signal;
    }

    @Override
    public long updateDocument(Term term,
        Iterable<? extends IndexableField> doc)
        throws IOException {
      long result = super.updateDocument(term, doc);
      try {
        if (waitAfterUpdate) {
          signal.countDown();
          latch.await();
        }
      } catch (InterruptedException e) {
        throw new ThreadInterruptedException(e);
      }
      return result;
    }
  }

  public void testEvilSearcherFactory() throws Exception {
    final Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    w.commit();

    final IndexReader other = DirectoryReader.open(dir);

    final SearcherFactory theEvilOne = new SearcherFactory() {
      @Override
      public IndexSearcher newSearcher(IndexReader ignored, IndexReader previous) {
        return LuceneTestCase.newSearcher(other);
      }
      };

    expectThrows(IllegalStateException.class, () -> {
      new SearcherManager(w.w, false, false, theEvilOne);
    });

    w.close();
    other.close();
    dir.close();
  }

  public void testListenerCalled() throws Exception {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig(null));
    final AtomicBoolean afterRefreshCalled = new AtomicBoolean(false);
    SearcherManager sm = new SearcherManager(iw, new SearcherFactory());
    sm.addListener(new ReferenceManager.RefreshListener() {
      @Override
      public void beforeRefresh() {
      }
      @Override
      public void afterRefresh(boolean didRefresh) {
        if (didRefresh) {
          afterRefreshCalled.set(true);
        }
      }
    });
    iw.addDocument(new Document());
    iw.commit();
    assertFalse(afterRefreshCalled.get());
    sm.maybeRefreshBlocking();
    assertTrue(afterRefreshCalled.get());
    sm.close();
    iw.close();
    dir.close();
  }

  // Relies on wall clock time, so it can easily false-fail when the machine is otherwise busy:
  @AwaitsFix(bugUrl = "https://issues.apache.org/jira/browse/LUCENE-5737")
  // LUCENE-5461
  public void testCRTReopen() throws Exception {
    //test behaving badly

    //should be high enough
    int maxStaleSecs = 20;

    //build crap data just to store it.
    String s = "        abcdefghijklmnopqrstuvwxyz     ";
    char[] chars = s.toCharArray();
    StringBuilder builder = new StringBuilder(2048);
    for (int i = 0; i < 2048; i++) {
      builder.append(chars[random().nextInt(chars.length)]);
    }
    String content = builder.toString();

    final SnapshotDeletionPolicy sdp = new SnapshotDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
    final Directory dir = new NRTCachingDirectory(newFSDirectory(createTempDir("nrt")), 5, 128);
    IndexWriterConfig config = new IndexWriterConfig(new MockAnalyzer(random()));
    config.setCommitOnClose(true);
    config.setIndexDeletionPolicy(sdp);
    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
    final IndexWriter iw = new IndexWriter(dir, config);
    SearcherManager sm = new SearcherManager(iw, new SearcherFactory());
    ControlledRealTimeReopenThread<IndexSearcher> controlledRealTimeReopenThread =
      new ControlledRealTimeReopenThread<>(iw, sm, maxStaleSecs, 0);

    controlledRealTimeReopenThread.setDaemon(true);
    controlledRealTimeReopenThread.start();

    List<Thread> commitThreads = new ArrayList<>();

    for (int i = 0; i < 500; i++) {
      if (i > 0 && i % 50 == 0) {
        Thread commitThread =  new Thread(new Runnable() {
            @Override
            public void run() {
              try {
                iw.commit();
                IndexCommit ic = sdp.snapshot();
                for (String name : ic.getFileNames()) {
                  //distribute, and backup
                  //System.out.println(names);
                  assertTrue(slowFileExists(dir, name));
                }
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            }
          });
        commitThread.start();
        commitThreads.add(commitThread);
      }
      Document d = new Document();
      d.add(new TextField("count", i + "", Field.Store.NO));
      d.add(new TextField("content", content, Field.Store.YES));
      long start = System.currentTimeMillis();
      long l = iw.addDocument(d);
      controlledRealTimeReopenThread.waitForGeneration(l);
      long wait = System.currentTimeMillis() - start;
      assertTrue("waited too long for generation " + wait,
                 wait < (maxStaleSecs *1000));
      IndexSearcher searcher = sm.acquire();
      TopDocs td = searcher.search(new TermQuery(new Term("count", i + "")), 10);
      sm.release(searcher);
      assertEquals(1, td.totalHits.value);
    }

    for(Thread commitThread : commitThreads) {
      commitThread.join();
    }

    controlledRealTimeReopenThread.close();
    sm.close();
    iw.close();
    dir.close();
  }

  public void testDeleteAll() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    SearcherManager mgr = new SearcherManager(w, new SearcherFactory());
    nrtDeletesThread = new ControlledRealTimeReopenThread<>(w, mgr, 0.1, 0.01);
    nrtDeletesThread.setName("NRTDeletes Reopen Thread");
    nrtDeletesThread.setDaemon(true);
    nrtDeletesThread.start();

    long gen1 = w.addDocument(new Document());
    long gen2 = w.deleteAll();
    nrtDeletesThread.waitForGeneration(gen2);
    IOUtils.close(nrtDeletesThread, nrtDeletes, w, dir);
  }
}
