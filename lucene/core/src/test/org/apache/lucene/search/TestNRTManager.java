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
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.ThreadedIndexingAndSearchingTestCase;
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
    nrtDeletes.waitForGeneration(maxGen);
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
        System.out.println(Thread.currentThread().getName() + ": nrt: verify " + id);
      }
      nrtDeletes.waitForGeneration(gen);
      final IndexSearcher s = nrtDeletes.acquire();
      if (VERBOSE) {
        System.out.println(Thread.currentThread().getName() + ": nrt: got searcher=" + s);
      }
      try {
        assertEquals(docs.size(), s.search(new TermQuery(id), 10).totalHits);
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
        System.out.println(Thread.currentThread().getName() + ": nrt: verify " + id);
      }
      nrtNoDeletes.waitForGeneration(gen);
      final IndexSearcher s = nrtNoDeletes.acquire();
      if (VERBOSE) {
        System.out.println(Thread.currentThread().getName() + ": nrt: got searcher=" + s);
      }
      try {
        assertEquals(docs.size(), s.search(new TermQuery(id), 10).totalHits);
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
        System.out.println(Thread.currentThread().getName() + ": nrt: verify " + id);
      }
      nrtNoDeletes.waitForGeneration(gen);
      final IndexSearcher s = nrtNoDeletes.acquire();
      if (VERBOSE) {
        System.out.println(Thread.currentThread().getName() + ": nrt: got searcher=" + s);
      }
      try {
        assertEquals(1, s.search(new TermQuery(id), 10).totalHits);
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
        System.out.println(Thread.currentThread().getName() + ": nrt: verify " + id);
      }
      nrtDeletes.waitForGeneration(gen);
      final IndexSearcher s = nrtDeletes.acquire();
      if (VERBOSE) {
        System.out.println(Thread.currentThread().getName() + ": nrt: got searcher=" + s);
      }
      try {
        assertEquals(1, s.search(new TermQuery(id), 10).totalHits);
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
        System.out.println(Thread.currentThread().getName() + ": nrt: verify del " + id);
      }
      nrtDeletes.waitForGeneration(gen);
      final IndexSearcher s = nrtDeletes.acquire();
      if (VERBOSE) {
        System.out.println(Thread.currentThread().getName() + ": nrt: got searcher=" + s);
      }
      try {
        assertEquals(0, s.search(new TermQuery(id), 10).totalHits);
      } finally {
        nrtDeletes.release(s);
      }
    }
    lastGens.set(gen);
  }

  // Not guaranteed to reflect deletes:
  private NRTManager nrtNoDeletes;

  // Is guaranteed to reflect deletes:
  private NRTManager nrtDeletes;

  private NRTManager.TrackingIndexWriter genWriter;

  private NRTManagerReopenThread nrtDeletesThread;
  private NRTManagerReopenThread nrtNoDeletesThread;

  @Override
  protected void doAfterWriter(final ExecutorService es) throws Exception {
    final double minReopenSec = 0.01 + 0.05 * random().nextDouble();
    final double maxReopenSec = minReopenSec * (1.0 + 10 * random().nextDouble());

    if (VERBOSE) {
      System.out.println("TEST: make NRTManager maxReopenSec=" + maxReopenSec + " minReopenSec=" + minReopenSec);
    }

    genWriter = new NRTManager.TrackingIndexWriter(writer);

    final SearcherFactory sf = new SearcherFactory() {
        @Override
        public IndexSearcher newSearcher(IndexReader r) throws IOException {
          TestNRTManager.this.warmCalled = true;
          IndexSearcher s = new IndexSearcher(r, es);
          s.search(new TermQuery(new Term("body", "united")), 10);
          return s;
        }
      };

    nrtNoDeletes = new NRTManager(genWriter, sf, false);
    nrtDeletes = new NRTManager(genWriter, sf, true);
                         
    nrtDeletesThread = new NRTManagerReopenThread(nrtDeletes, maxReopenSec, minReopenSec);
    nrtDeletesThread.setName("NRTDeletes Reopen Thread");
    nrtDeletesThread.setPriority(Math.min(Thread.currentThread().getPriority()+2, Thread.MAX_PRIORITY));
    nrtDeletesThread.setDaemon(true);
    nrtDeletesThread.start();

    nrtNoDeletesThread = new NRTManagerReopenThread(nrtNoDeletes, maxReopenSec, minReopenSec);
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
    final NRTManager nrt;
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
    // against the same NRT mgr you acquired from... but
    // both impls just decRef the underlying reader so we
    // can get away w/ cheating:
    nrtNoDeletes.release(s);
  }

  @Override
  protected void doClose() throws Exception {
    assertTrue(warmCalled);
    if (VERBOSE) {
      System.out.println("TEST: now close NRTManager");
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
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    Directory d = newDirectory();
    final CountDownLatch latch = new CountDownLatch(1);
    final CountDownLatch signal = new CountDownLatch(1);

    LatchedIndexWriter _writer = new LatchedIndexWriter(d, conf, latch, signal);
    final NRTManager.TrackingIndexWriter writer = new NRTManager.TrackingIndexWriter(_writer);
    final NRTManager manager = new NRTManager(writer, null, false);
    Document doc = new Document();
    doc.add(newField("test","test", TextField.TYPE_STORED));
    long gen = writer.addDocument(doc);
    manager.maybeRefresh();
    assertFalse(gen < manager.getCurrentSearchingGen());
    Thread t = new Thread() {
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
    _writer.waitAfterUpdate = true; // wait in addDocument to let some reopens go through
    final long lastGen = writer.updateDocument(new Term("foo", "bar"), doc); // once this returns the doc is already reflected in the last reopen

    assertFalse(manager.isSearcherCurrent()); // false since there is a delete in the queue
    
    IndexSearcher searcher = manager.acquire();
    try {
      assertEquals(2, searcher.getIndexReader().numDocs());
    } finally {
      manager.release(searcher);
    }
    NRTManagerReopenThread thread = new NRTManagerReopenThread(manager, 0.01, 0.01);
    thread.start(); // start reopening
    if (VERBOSE) {
      System.out.println("waiting now for generation " + lastGen);
    }
    
    final AtomicBoolean finished = new AtomicBoolean(false);
    Thread waiter = new Thread() {
      public void run() {
        manager.waitForGeneration(lastGen);
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
    IOUtils.close(manager, _writer, d);
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

  public void testEvilSearcherFactory() throws Exception {
    final Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    w.commit();

    final IndexReader other = DirectoryReader.open(dir);

    final SearcherFactory theEvilOne = new SearcherFactory() {
      @Override
      public IndexSearcher newSearcher(IndexReader ignored) throws IOException {
        return new IndexSearcher(other);
      }
      };

    try {
      new NRTManager(new NRTManager.TrackingIndexWriter(w.w), theEvilOne);
    } catch (IllegalStateException ise) {
      // expected
    }
    w.close();
    other.close();
    dir.close();
  }
}
