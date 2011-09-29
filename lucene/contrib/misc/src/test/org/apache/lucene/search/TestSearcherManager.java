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
import java.util.concurrent.ExecutorService;

import org.apache.lucene.index.Term;
import org.apache.lucene.index.ThreadedIndexingAndSearchingTestCase;
import org.apache.lucene.util._TestUtil;

public class TestSearcherManager extends ThreadedIndexingAndSearchingTestCase {

  boolean warmCalled;

  public void testSearcherManager() throws Exception {
    runTest("TestSearcherManager");
  }

  @Override
  protected IndexSearcher getFinalSearcher() throws Exception  {
    writer.commit();
    mgr.maybeReopen();
    return mgr.acquire();
  }

  private SearcherManager mgr;

  @Override
  protected void doAfterWriter(ExecutorService es) throws Exception {
    // SearcherManager needs to see empty commit:
    writer.commit();
    mgr = new SearcherManager(dir,
                              new SearcherWarmer() {
                                @Override
                                public void warm(IndexSearcher s) throws IOException {
                                  TestSearcherManager.this.warmCalled = true;
                                  s.search(new TermQuery(new Term("body", "united")), 10);
                                }
                              }, es);
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
}
