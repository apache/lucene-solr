package org.apache.lucene.search;

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
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.ThreadedIndexingAndSearchingTestCase;
import org.apache.lucene.search.NRTManagerReopenThread;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.NRTCachingDirectory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase.UseNoMemoryExpensiveCodec;
import org.apache.lucene.util.ThreadInterruptedException;

@UseNoMemoryExpensiveCodec
public class TestNRTManager extends ThreadedIndexingAndSearchingTestCase {

  private final ThreadLocal<Long> lastGens = new ThreadLocal<Long>();
  private boolean warmCalled;

  public void testNRTManager() throws Exception {
    runTest("TestNRTManager");
  }

  @Override
  protected IndexSearcher getFinalSearcher() throws Exception  {
    if (VERBOSE) {
      System.out.println("TEST: finalSearcher maxGen=" + maxGen);
    }
    final SearcherManager manager = nrt.waitForGeneration(maxGen, true);
    return manager.acquire();
  }

  @Override
  protected Directory getDirectory(Directory in) {
    // Randomly swap in NRTCachingDir
    if (random.nextBoolean()) {
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
    final long gen = nrt.updateDocuments(id, docs);

    // Randomly verify the update "took":
    if (random.nextInt(20) == 2) {
      if (VERBOSE) {
        System.out.println(Thread.currentThread().getName() + ": nrt: verify " + id);
      }
      SearcherManager manager = nrt.waitForGeneration(gen, true);
      final IndexSearcher s = manager.acquire();
      if (VERBOSE) {
        System.out.println(Thread.currentThread().getName() + ": nrt: got searcher=" + s);
      }
      try {
        assertEquals(docs.size(), s.search(new TermQuery(id), 10).totalHits);
      } finally {
        manager.release(s);
      }
    }
    
    lastGens.set(gen);
  }

  @Override
  protected void addDocuments(Term id, List<? extends Iterable<? extends IndexableField>> docs) throws Exception {
    final long gen = nrt.addDocuments(docs);
    // Randomly verify the add "took":
    if (random.nextInt(20) == 2) {
      if (VERBOSE) {
        System.out.println(Thread.currentThread().getName() + ": nrt: verify " + id);
      }
      final SearcherManager manager = nrt.waitForGeneration(gen, false);
      final IndexSearcher s = manager.acquire();
      if (VERBOSE) {
        System.out.println(Thread.currentThread().getName() + ": nrt: got searcher=" + s);
      }
      try {
        assertEquals(docs.size(), s.search(new TermQuery(id), 10).totalHits);
      } finally {
        manager.release(s);
      }
    }
    lastGens.set(gen);
  }

  @Override
  protected void addDocument(Term id, Iterable<? extends IndexableField> doc) throws Exception {
    final long gen = nrt.addDocument(doc);

    // Randomly verify the add "took":
    if (random.nextInt(20) == 2) {
      if (VERBOSE) {
        System.out.println(Thread.currentThread().getName() + ": nrt: verify " + id);
      }
      final SearcherManager manager = nrt.waitForGeneration(gen, false);
      final IndexSearcher s = manager.acquire();
      if (VERBOSE) {
        System.out.println(Thread.currentThread().getName() + ": nrt: got searcher=" + s);
      }
      try {
        assertEquals(1, s.search(new TermQuery(id), 10).totalHits);
      } finally {
        manager.release(s);
      }
    }
    lastGens.set(gen);
  }

  @Override
  protected void updateDocument(Term id, Iterable<? extends IndexableField> doc) throws Exception {
    final long gen = nrt.updateDocument(id, doc);
    // Randomly verify the udpate "took":
    if (random.nextInt(20) == 2) {
      if (VERBOSE) {
        System.out.println(Thread.currentThread().getName() + ": nrt: verify " + id);
      }
      final SearcherManager manager = nrt.waitForGeneration(gen, true);
      final IndexSearcher s = manager.acquire();
      if (VERBOSE) {
        System.out.println(Thread.currentThread().getName() + ": nrt: got searcher=" + s);
      }
      try {
        assertEquals(1, s.search(new TermQuery(id), 10).totalHits);
      } finally {
        manager.release(s);
      }
    }
    lastGens.set(gen);
  }

  @Override
  protected void deleteDocuments(Term id) throws Exception {
    final long gen = nrt.deleteDocuments(id);
    // randomly verify the delete "took":
    if (random.nextInt(20) == 7) {
      if (VERBOSE) {
        System.out.println(Thread.currentThread().getName() + ": nrt: verify del " + id);
      }
      final SearcherManager manager = nrt.waitForGeneration(gen, true);
      final IndexSearcher s = manager.acquire();
      if (VERBOSE) {
        System.out.println(Thread.currentThread().getName() + ": nrt: got searcher=" + s);
      }
      try {
        assertEquals(0, s.search(new TermQuery(id), 10).totalHits);
      } finally {
        manager.release(s);
      }
    }
    lastGens.set(gen);
  }

  private NRTManager nrt;
  private NRTManagerReopenThread nrtThread;
  @Override
  protected void doAfterWriter(final ExecutorService es) throws Exception {
    final double minReopenSec = 0.01 + 0.05 * random.nextDouble();
    final double maxReopenSec = minReopenSec * (1.0 + 10 * random.nextDouble());

    if (VERBOSE) {
      System.out.println("TEST: make NRTManager maxReopenSec=" + maxReopenSec + " minReopenSec=" + minReopenSec);
    }

    nrt = new NRTManager(writer,
                         new SearcherFactory() {
                          @Override
                          public IndexSearcher newSearcher(IndexReader r) throws IOException {
                            TestNRTManager.this.warmCalled = true;
                            IndexSearcher s = new IndexSearcher(r, es);
                            s.search(new TermQuery(new Term("body", "united")), 10);
                            return s;
                          }
                        }, false);
                         
    nrtThread = new NRTManagerReopenThread(nrt, maxReopenSec, minReopenSec);
    nrtThread.setName("NRT Reopen Thread");
    nrtThread.setPriority(Math.min(Thread.currentThread().getPriority()+2, Thread.MAX_PRIORITY));
    nrtThread.setDaemon(true);
    nrtThread.start();
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
    return nrt.getSearcherManager(random.nextBoolean()).acquire();
  }

  @Override
  protected void releaseSearcher(IndexSearcher s) throws Exception {
    // Test doesn't assert deletions until the end, so we
    // can randomize whether dels must be applied
    nrt.getSearcherManager(random.nextBoolean()).release(s);
  }

  @Override
  protected void doClose() throws Exception {
    assertTrue(warmCalled);
    if (VERBOSE) {
      System.out.println("TEST: now close NRTManager");
    }
    nrtThread.close();
    nrt.close();
  }
  
  /*
   * LUCENE-3528 - NRTManager hangs in certain situations 
   */
  public void testThreadStarvationNoDeleteNRTReader() throws IOException, InterruptedException {
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random));
    Directory d = newDirectory();
    final CountDownLatch latch = new CountDownLatch(1);
    final CountDownLatch signal = new CountDownLatch(1);

    LatchedIndexWriter writer = new LatchedIndexWriter(d, conf, latch, signal);
    final NRTManager manager = new NRTManager(writer, null, false);
    Document doc = new Document();
    doc.add(newField("test","test", TextField.TYPE_STORED));
    long gen = manager.addDocument(doc);
    assertTrue(manager.maybeReopen(false));
    assertFalse(gen < manager.getCurrentSearchingGen(false));
    Thread t = new Thread() {
      public void run() {
        try {
          signal.await();
          assertTrue(manager.maybeReopen(false));
          manager.deleteDocuments(new TermQuery(new Term("foo", "barista")));
          manager.maybeReopen(false); // kick off another reopen so we inc. the internal gen
        } catch (Exception e) {
          e.printStackTrace();
        } finally {
          latch.countDown(); // let the add below finish
        }
      }
    };
    t.start();
    writer.waitAfterUpdate = true; // wait in addDocument to let some reopens go through
    final long lastGen = manager.updateDocument(new Term("foo", "bar"), doc); // once this returns the doc is already reflected in the last reopen
    assertFalse(manager.getSearcherManager(false).isSearcherCurrent()); // false since there is a delete in the queue
    
    IndexSearcher acquire = manager.getSearcherManager(false).acquire();
    try {
      assertEquals(2, acquire.getIndexReader().numDocs());
    } finally {
      acquire.getIndexReader().decRef();
    }
    NRTManagerReopenThread thread = new NRTManagerReopenThread(manager, 0.01, 0.01);
    thread.start(); // start reopening
    if (VERBOSE) {
      System.out.println("waiting now for generation " + lastGen);
    }
    
    final AtomicBoolean finished = new AtomicBoolean(false);
    Thread waiter = new Thread() {
      public void run() {
        manager.waitForGeneration(lastGen, false);
        finished.set(true);
      }
    };
    waiter.start();
    manager.maybeReopen(false);
    waiter.join(1000);
    if (!finished.get()) {
      waiter.interrupt();
      fail("thread deadlocked on waitForGeneration");
    }
    thread.close();
    thread.join();
    IOUtils.close(manager, writer, d);
  }
  
  public static class LatchedIndexWriter extends IndexWriter {

    private CountDownLatch latch;
    boolean waitAfterUpdate = false;
    private CountDownLatch signal;

    public LatchedIndexWriter(Directory d, IndexWriterConfig conf,
        CountDownLatch latch, CountDownLatch signal)
        throws CorruptIndexException, LockObtainFailedException, IOException {
      super(d, conf);
      this.latch = latch;
      this.signal = signal;

    }

    public void updateDocument(Term term,
        Iterable<? extends IndexableField> doc, Analyzer analyzer)
        throws CorruptIndexException, IOException {
      super.updateDocument(term, doc, analyzer);
      try {
        if (waitAfterUpdate) {
          signal.countDown();
          latch.await();
        }
      } catch (InterruptedException e) {
        throw new ThreadInterruptedException(e);
      }
    }
  }
}
