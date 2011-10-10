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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.ThreadedIndexingAndSearchingTestCase;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.NamedThreadFactory;
import org.apache.lucene.util._TestUtil;

public class TestSearcherManager extends ThreadedIndexingAndSearchingTestCase {

  boolean warmCalled;

  public void testSearcherManager() throws Exception {
    runTest("TestSearcherManager");
  }

  @Override
  protected IndexSearcher getFinalSearcher() throws Exception  {
    if (!isNRT) {
      writer.commit();
    }
    assertTrue(mgr.maybeReopen() || mgr.isSearcherCurrent());
    return mgr.acquire();
  }

  private SearcherManager mgr;
  private boolean isNRT;

  @Override
  protected void doAfterWriter(ExecutorService es) throws Exception {
    // SearcherManager needs to see empty commit:
    final SearcherWarmer warmer = new SearcherWarmer() {
      @Override
      public void warm(IndexSearcher s) throws IOException {
        TestSearcherManager.this.warmCalled = true;
        s.search(new TermQuery(new Term("body", "united")), 10);
      }
    };
    if (random.nextBoolean()) {
      mgr = SearcherManager.open(writer, true, warmer, es);
      isNRT = true;
    } else {
      writer.commit();
      mgr = SearcherManager.open(dir, warmer, es);
      isNRT = false;
    }
    
  }

  @Override
  protected void doSearching(ExecutorService es, final long stopTime) throws Exception {

    Thread reopenThread = new Thread() {
      @Override
      public void run() {
        try {
          while(System.currentTimeMillis() < stopTime) {
            Thread.sleep(_TestUtil.nextInt(random, 1, 100));
            writer.commit();
            Thread.sleep(_TestUtil.nextInt(random, 1, 5));
            mgr.maybeReopen();
          }
        } catch (Throwable t) {
          System.out.println("TEST: reopen thread hit exc");
          t.printStackTrace(System.out);
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
    if (random.nextInt(10) == 7) {
      // NOTE: not best practice to call maybeReopen
      // synchronous to your search threads, but still we
      // test as apps will presumably do this for
      // simplicity:
      mgr.maybeReopen();
    }

    return mgr.acquire();
  }

  @Override
  protected void releaseSearcher(IndexSearcher s) throws Exception {
    mgr.release(s);
  }

  @Override
  protected void doClose() throws Exception {
    assertTrue(warmCalled);
    if (VERBOSE) {
      System.out.println("TEST: now close SearcherManager");
    }
    mgr.close();
  }
  
  public void testIntermediateClose() throws IOException, InterruptedException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random)));
    writer.addDocument(new Document());
    writer.commit();
    final CountDownLatch awaitEnterWarm = new CountDownLatch(1);
    final CountDownLatch awaitClose = new CountDownLatch(1);
    final ExecutorService es = random.nextBoolean() ? null : Executors.newCachedThreadPool(new NamedThreadFactory("testIntermediateClose"));
    final SearcherWarmer warmer = new SearcherWarmer() {
      @Override
      public void warm(IndexSearcher s) throws IOException {
        try {
          awaitEnterWarm.countDown();
          awaitClose.await();
        } catch (InterruptedException e) {
          //
        }
      }
    };
    final SearcherManager searcherManager = random.nextBoolean() ? SearcherManager.open(dir,
        warmer, es) : SearcherManager.open(writer, random.nextBoolean(), warmer, es);
    IndexSearcher searcher = searcherManager.acquire();
    try {
      assertEquals(1, searcher.getIndexReader().numDocs());
    } finally {
      searcherManager.release(searcher);
    }
    writer.addDocument(new Document());
    writer.commit();
    final AtomicBoolean success = new AtomicBoolean(false);
    final AtomicBoolean triedReopen = new AtomicBoolean(false);
    final Throwable[] exc = new Throwable[1];
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          triedReopen.set(true);
          searcherManager.maybeReopen();
          success.set(true);
        } catch (AlreadyClosedException e) {
          // expected
        } catch (Throwable e) {
          exc[0] = e;
          // use success as the barrier here to make sure we see the write
          success.set(false);

        }
      }
    });
    thread.start();
    awaitEnterWarm.await();
    for (int i = 0; i < 2; i++) {
      searcherManager.close();
    }
    awaitClose.countDown();
    thread.join();
    try {
      searcherManager.acquire();
      fail("already closed");
    } catch (AlreadyClosedException ex) {
      // expected
    }
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
}
