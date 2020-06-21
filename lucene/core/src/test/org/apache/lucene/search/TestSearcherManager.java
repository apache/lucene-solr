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
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.ThreadedIndexingAndSearchingTestCase;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NamedThreadFactory;
import org.apache.lucene.util.TestUtil;

@SuppressCodecs({ "SimpleText", "Direct" })
public class TestSearcherManager extends ThreadedIndexingAndSearchingTestCase {

  boolean warmCalled;

  private SearcherLifetimeManager.Pruner pruner;

  public void testSearcherManager() throws Exception {
    pruner = new SearcherLifetimeManager.PruneByAge(TEST_NIGHTLY ? TestUtil.nextInt(random(), 1, 20) : 1);
    runTest("TestSearcherManager");
  }

  @Override
  protected IndexSearcher getFinalSearcher() throws Exception  {
    if (!isNRT) {
      writer.commit();
    }
    assertTrue(mgr.maybeRefresh() || mgr.isSearcherCurrent());
    return mgr.acquire();
  }

  private SearcherManager mgr;
  private SearcherLifetimeManager lifetimeMGR;
  private final List<Long> pastSearchers = new ArrayList<>();
  private boolean isNRT;

  @Override
  protected void doAfterWriter(final ExecutorService es) throws Exception {
    final SearcherFactory factory = new SearcherFactory() {
      @Override
      public IndexSearcher newSearcher(IndexReader r, IndexReader previous) throws IOException {
        IndexSearcher s = new IndexSearcher(r, es);
        TestSearcherManager.this.warmCalled = true;
        s.search(new TermQuery(new Term("body", "united")), 10);
        return s;
      }
    };
    if (random().nextBoolean()) {
      // TODO: can we randomize the applyAllDeletes?  But
      // somehow for final searcher we must apply
      // deletes...
      mgr = new SearcherManager(writer, factory);
      isNRT = true;
    } else {
      // SearcherManager needs to see empty commit:
      writer.commit();
      mgr = new SearcherManager(dir, factory);
      isNRT = false;
      assertMergedSegmentsWarmed = false;
    }

    lifetimeMGR = new SearcherLifetimeManager();
  }

  @Override
  protected void doSearching(ExecutorService es, final long stopTime) throws Exception {

    Thread reopenThread = new Thread() {
      @Override
      public void run() {
        try {
          if (VERBOSE) {
            System.out.println("[" + Thread.currentThread().getName() + "]: launch reopen thread");
          }

          while(System.currentTimeMillis() < stopTime) {
            Thread.sleep(TestUtil.nextInt(random(), 1, 100));
            writer.commit();
            Thread.sleep(TestUtil.nextInt(random(), 1, 5));
            boolean block = random().nextBoolean();
            if (block) {
              mgr.maybeRefreshBlocking();
              lifetimeMGR.prune(pruner);
            } else if (mgr.maybeRefresh()) {
              lifetimeMGR.prune(pruner);
            }
          }
        } catch (Throwable t) {
          if (VERBOSE) {
            System.out.println("TEST: reopen thread hit exc");
            t.printStackTrace(System.out);
          }
          failed.set(true);
          throw new RuntimeException(t);
        }
      }
      };
    reopenThread.setDaemon(true);
    reopenThread.start();

    runSearchThreads(stopTime);

    reopenThread.join();
  }

  @Override
  protected IndexSearcher getCurrentSearcher() throws Exception {
    if (random().nextInt(10) == 7) {
      // NOTE: not best practice to call maybeRefresh
      // synchronous to your search threads, but still we
      // test as apps will presumably do this for
      // simplicity:
      if (mgr.maybeRefresh()) {
        lifetimeMGR.prune(pruner);
      }
    }

    IndexSearcher s = null;

    synchronized(pastSearchers) {
      while (pastSearchers.size() != 0 && random().nextDouble() < 0.25) {
        // 1/4 of the time pull an old searcher, ie, simulate
        // a user doing a follow-on action on a previous
        // search (drilling down/up, clicking next/prev page,
        // etc.)
        final Long token = pastSearchers.get(random().nextInt(pastSearchers.size()));
        s = lifetimeMGR.acquire(token);
        if (s == null) {
          // Searcher was pruned
          pastSearchers.remove(token);
        } else {
          break;
        }
      }
    }

    if (s == null) {
      s = mgr.acquire();
      if (s.getIndexReader().numDocs() != 0) {
        Long token = lifetimeMGR.record(s);
        synchronized(pastSearchers) {
          if (!pastSearchers.contains(token)) {
            pastSearchers.add(token);
          }
        }
      }
    }

    return s;
  }

  @Override
  protected void releaseSearcher(IndexSearcher s) throws Exception {
    s.getIndexReader().decRef();
  }

  @Override
  protected void doClose() throws Exception {
    assertTrue(warmCalled);
    if (VERBOSE) {
      System.out.println("TEST: now close SearcherManager");
    }
    mgr.close();
    lifetimeMGR.close();
  }

  public void testIntermediateClose() throws IOException, InterruptedException {
    Directory dir = newDirectory();
    // Test can deadlock if we use SMS:
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
        new MockAnalyzer(random())).setMergeScheduler(new ConcurrentMergeScheduler()));
    writer.addDocument(new Document());
    writer.commit();
    final CountDownLatch awaitEnterWarm = new CountDownLatch(1);
    final CountDownLatch awaitClose = new CountDownLatch(1);
    final AtomicBoolean triedReopen = new AtomicBoolean(false);
    final ExecutorService es = random().nextBoolean() ? null : Executors.newCachedThreadPool(new NamedThreadFactory("testIntermediateClose"));
    final SearcherFactory factory = new SearcherFactory() {
      @Override
      public IndexSearcher newSearcher(IndexReader r, IndexReader previous) {
        try {
          if (triedReopen.get()) {
            awaitEnterWarm.countDown();
            awaitClose.await();
          }
        } catch (InterruptedException e) {
          //
        }
        return new IndexSearcher(r, es);
      }
    };
    final SearcherManager searcherManager = random().nextBoolean() 
        ? new SearcherManager(dir, factory) 
      : new SearcherManager(writer, random().nextBoolean(), false, factory);
    if (VERBOSE) {
      System.out.println("sm created");
    }
    IndexSearcher searcher = searcherManager.acquire();
    try {
      assertEquals(1, searcher.getIndexReader().numDocs());
    } finally {
      searcherManager.release(searcher);
    }
    writer.addDocument(new Document());
    writer.commit();
    final AtomicBoolean success = new AtomicBoolean(false);
    final Throwable[] exc = new Throwable[1];
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          triedReopen.set(true);
          if (VERBOSE) {
            System.out.println("NOW call maybeRefresh");
          }
          searcherManager.maybeRefresh();
          success.set(true);
        } catch (AlreadyClosedException e) {
          // expected
        } catch (Throwable e) {
          if (VERBOSE) {
            System.out.println("FAIL: unexpected exc");
            e.printStackTrace(System.out);
          }
          exc[0] = e;
          // use success as the barrier here to make sure we see the write
          success.set(false);

        }
      }
    });
    thread.start();
    if (VERBOSE) {
      System.out.println("THREAD started");
    }
    awaitEnterWarm.await();
    if (VERBOSE) {
      System.out.println("NOW call close");
    }
    searcherManager.close();
    awaitClose.countDown();
    thread.join();
    expectThrows(AlreadyClosedException.class, () -> {
      searcherManager.acquire();
    });
    assertFalse(success.get());
    assertTrue(triedReopen.get());
    assertNull("" + exc[0], exc[0]);
    writer.close();
    dir.close();
    if (es != null) {
      es.shutdown();
      es.awaitTermination(1, TimeUnit.SECONDS);
    }
  }
  
  public void testCloseTwice() throws Exception {
    // test that we can close SM twice (per Closeable's contract).
    Directory dir = newDirectory();
    new IndexWriter(dir, new IndexWriterConfig(null)).close();
    SearcherManager sm = new SearcherManager(dir, null);
    sm.close();
    sm.close();
    dir.close();
  }

  public void testReferenceDecrementIllegally() throws Exception {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
        new MockAnalyzer(random())).setMergeScheduler(new ConcurrentMergeScheduler()));
    @SuppressWarnings("resource")
    SearcherManager sm = new SearcherManager(writer, false, false, new SearcherFactory());
    writer.addDocument(new Document());
    writer.commit();
    sm.maybeRefreshBlocking();

    IndexSearcher acquire = sm.acquire();
    IndexSearcher acquire2 = sm.acquire();
    sm.release(acquire);
    sm.release(acquire2);


    acquire = sm.acquire();
    acquire.getIndexReader().decRef();
    sm.release(acquire);
    expectThrows(IllegalStateException.class, () -> {
      sm.acquire();
    });

    // sm.close(); -- already closed
    writer.close();
    dir.close();
  }


  public void testEnsureOpen() throws Exception {
    Directory dir = newDirectory();
    new IndexWriter(dir, new IndexWriterConfig(null)).close();
    SearcherManager sm = new SearcherManager(dir, null);
    IndexSearcher s = sm.acquire();
    sm.close();
    
    // this should succeed;
    sm.release(s);
    
    // this should fail
    expectThrows(AlreadyClosedException.class, () -> {
      sm.acquire();
    });
    
    // this should fail
    expectThrows(AlreadyClosedException.class, () -> {
      sm.maybeRefresh();
    });

    dir.close();
  }

  public void testListenerCalled() throws Exception {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig(null));
    final AtomicBoolean afterRefreshCalled = new AtomicBoolean(false);
    SearcherManager sm = new SearcherManager(iw, false, false, new SearcherFactory());
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

  public void testEvilSearcherFactory() throws Exception {
    final Random random = random();
    final Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random, dir);
    w.commit();

    final IndexReader other = DirectoryReader.open(dir);

    final SearcherFactory theEvilOne = new SearcherFactory() {
      @Override
      public IndexSearcher newSearcher(IndexReader ignored, IndexReader previous) {
        return LuceneTestCase.newSearcher(other);
      }
      };

    expectThrows(IllegalStateException.class, () -> {
      new SearcherManager(dir, theEvilOne);
    });
    expectThrows(IllegalStateException.class, () -> {
      new SearcherManager(w.w, random.nextBoolean(), false, theEvilOne);
    });
    w.close();
    other.close();
    dir.close();
  }
  
  public void testMaybeRefreshBlockingLock() throws Exception {
    // make sure that maybeRefreshBlocking releases the lock, otherwise other
    // threads cannot obtain it.
    final Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    w.close();
    
    final SearcherManager sm = new SearcherManager(dir, null);
    
    Thread t = new Thread() {
      @Override
      public void run() {
        try {
          // this used to not release the lock, preventing other threads from obtaining it.
          sm.maybeRefreshBlocking();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };
    t.start();
    t.join();
    
    // if maybeRefreshBlocking didn't release the lock, this will fail.
    assertTrue("failde to obtain the refreshLock!", sm.maybeRefresh());
    
    sm.close();
    dir.close();
  }

  private static class MyFilterLeafReader extends FilterLeafReader {
    public MyFilterLeafReader(LeafReader in) {
      super(in);
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
      return in.getCoreCacheHelper();
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
      return in.getReaderCacheHelper();
    }
  }

  private static class MyFilterDirectoryReader extends FilterDirectoryReader {
    public MyFilterDirectoryReader(DirectoryReader in) throws IOException {
      super(in, 
            new FilterDirectoryReader.SubReaderWrapper() {
              @Override
              public LeafReader wrap(LeafReader reader) {
                FilterLeafReader wrapped = new MyFilterLeafReader(reader);
                assertEquals(reader, wrapped.getDelegate());
                return wrapped;
              }
            });
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
      return new MyFilterDirectoryReader(in);
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
      return in.getReaderCacheHelper();
    }
  }

  // LUCENE-6087
  public void testCustomDirectoryReader() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    DirectoryReader nrtReader = w.getReader();

    FilterDirectoryReader reader = new MyFilterDirectoryReader(nrtReader);
    assertEquals(nrtReader, reader.getDelegate());
    assertEquals(FilterDirectoryReader.unwrap(nrtReader), FilterDirectoryReader.unwrap(reader));

    SearcherManager mgr = new SearcherManager(reader, null);
    for(int i=0;i<10;i++) {
      w.addDocument(new Document());
      mgr.maybeRefresh();
      IndexSearcher s = mgr.acquire();
      try {
        assertTrue(s.getIndexReader() instanceof MyFilterDirectoryReader);
        for (LeafReaderContext ctx : s.getIndexReader().leaves()) {
          assertTrue(ctx.reader() instanceof MyFilterLeafReader);
        }
      } finally {
        mgr.release(s);
      }
    }
    mgr.close();
    w.close();
    dir.close();
  }

  public void testPreviousReaderIsPassed() throws IOException {
    final Directory dir = newDirectory();
    final IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    w.addDocument(new Document());
    class MySearcherFactory extends SearcherFactory {
      IndexReader lastReader = null;
      IndexReader lastPreviousReader = null;
      int called = 0;
      @Override
      public IndexSearcher newSearcher(IndexReader reader, IndexReader previousReader) throws IOException {
        called++;
        lastReader = reader;
        lastPreviousReader = previousReader;
        return super.newSearcher(reader, previousReader);
      }
    }

    MySearcherFactory factory = new MySearcherFactory();
    final SearcherManager sm = new SearcherManager(w, random().nextBoolean(), false, factory);
    assertEquals(1, factory.called);
    assertNull(factory.lastPreviousReader);
    assertNotNull(factory.lastReader);
    IndexSearcher acquire = sm.acquire();
    assertSame(factory.lastReader, acquire.getIndexReader());
    sm.release(acquire);

    final IndexReader lastReader = factory.lastReader;
    // refresh
    w.addDocument(new Document());
    assertTrue(sm.maybeRefresh());

    acquire = sm.acquire();
    assertSame(factory.lastReader, acquire.getIndexReader());
    sm.release(acquire);
    assertNotNull(factory.lastPreviousReader);
    assertSame(lastReader, factory.lastPreviousReader);
    assertNotSame(factory.lastReader, lastReader);
    assertEquals(2, factory.called);
    w.close();
    sm.close();
    dir.close();
  }

  public void testConcurrentIndexCloseSearchAndRefresh() throws Exception {
    final Directory dir = newFSDirectory(createTempDir());
    AtomicReference<IndexWriter> writerRef = new AtomicReference<>();
    final MockAnalyzer analyzer = new MockAnalyzer(random());
    analyzer.setMaxTokenLength(IndexWriter.MAX_TERM_LENGTH);
    writerRef.set(new IndexWriter(dir, ensureSaneIWCOnNightly(newIndexWriterConfig(analyzer))));

    AtomicReference<SearcherManager> mgrRef = new AtomicReference<>();
    mgrRef.set(new SearcherManager(writerRef.get(), null));
    final AtomicBoolean stop = new AtomicBoolean();

    Thread indexThread = new Thread() {
        @Override
        public void run() {
          try {
            int numDocs = TEST_NIGHTLY ? atLeast(20000) : atLeast(200);
            for (int i = 0; i < numDocs; i++) {
              IndexWriter w = writerRef.get();
              Document doc = new Document();
              doc.add(newTextField("field",
                                   TestUtil.randomAnalysisString(random(), 256, false),
                                   Field.Store.YES));
              w.addDocument(doc);
              if (random().nextInt(1000) == 17) {
                if (random().nextBoolean()) {
                  w.close();
                } else {
                  w.rollback();
                }
                writerRef.set(new IndexWriter(dir, ensureSaneIWCOnNightly(newIndexWriterConfig(analyzer))));
              }
            }
            if (VERBOSE) {
              System.out.println("TEST: index count=" + writerRef.get().getDocStats().maxDoc);
            }
          } catch (IOException ioe) {
            throw new RuntimeException(ioe);
          } finally {
            stop.set(true);
          }
        }
      };

    Thread searchThread = new Thread() {
        @Override
        public void run() {
          try {
            long totCount = 0;
            while (stop.get() == false) {
              SearcherManager mgr = mgrRef.get();
              if (mgr != null) {
                IndexSearcher searcher;
                try {
                  searcher = mgr.acquire();
                } catch (AlreadyClosedException ace) {
                  // ok
                  continue;
                }
                totCount += searcher.getIndexReader().maxDoc();
                mgr.release(searcher);
              }
            }
            if (VERBOSE) {
              System.out.println("TEST: search totCount=" + totCount);
            }
          } catch (IOException ioe) {
            throw new RuntimeException(ioe);
          }
        }
      };

    Thread refreshThread = new Thread() {
        @Override
        public void run() {
          try {
            int refreshCount = 0;
            int aceCount = 0;
            while (stop.get() == false) {
              SearcherManager mgr = mgrRef.get();
              if (mgr != null) {
                refreshCount++;
                try {
                  mgr.maybeRefreshBlocking();
                } catch (AlreadyClosedException ace) {
                  // ok
                  aceCount++;
                  continue;
                }
              }
            }
            if (VERBOSE) {
              System.out.println("TEST: refresh count=" + refreshCount + " aceCount=" + aceCount);
            }
          } catch (IOException ioe) {
            throw new RuntimeException(ioe);
          }
        }
      };

    Thread closeThread = new Thread() {
        @Override
        public void run() {
          try {
            int closeCount = 0;
            int aceCount = 0;
            while (stop.get() == false) {
              SearcherManager mgr = mgrRef.get();
              assert mgr != null;
              mgr.close();
              closeCount++;
              while (stop.get() == false) {
                try {
                  mgrRef.set(new SearcherManager(writerRef.get(), null));
                  break;
                } catch (AlreadyClosedException ace) {
                  // ok
                  aceCount++;
                }
              }
            }
            if (VERBOSE) {
              System.out.println("TEST: close count=" + closeCount + " aceCount=" + aceCount);
            }
          } catch (IOException ioe) {
            throw new RuntimeException(ioe);
          }
        }
      };

    indexThread.start();
    searchThread.start();
    refreshThread.start();
    closeThread.start();

    indexThread.join();
    searchThread.join();
    refreshThread.join();
    closeThread.join();

    mgrRef.get().close();
    writerRef.get().close();
    dir.close();
  }
}
