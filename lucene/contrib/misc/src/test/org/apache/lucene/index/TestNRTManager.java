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
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherWarmer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NRTCachingDirectory;

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
    return nrt.get(maxGen, true);
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
      final IndexSearcher s = nrt.get(gen, true);
      if (VERBOSE) {
        System.out.println(Thread.currentThread().getName() + ": nrt: got searcher=" + s);
      }
      try {
        assertEquals(docs.size(), s.search(new TermQuery(id), 10).totalHits);
      } finally {
        nrt.release(s);
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
      final IndexSearcher s = nrt.get(gen, false);
      if (VERBOSE) {
        System.out.println(Thread.currentThread().getName() + ": nrt: got searcher=" + s);
      }
      try {
        assertEquals(docs.size(), s.search(new TermQuery(id), 10).totalHits);
      } finally {
        nrt.release(s);
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
      final IndexSearcher s = nrt.get(gen, false);
      if (VERBOSE) {
        System.out.println(Thread.currentThread().getName() + ": nrt: got searcher=" + s);
      }
      try {
        assertEquals(1, s.search(new TermQuery(id), 10).totalHits);
      } finally {
        nrt.release(s);
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
      final IndexSearcher s = nrt.get(gen, true);
      if (VERBOSE) {
        System.out.println(Thread.currentThread().getName() + ": nrt: got searcher=" + s);
      }
      try {
        assertEquals(1, s.search(new TermQuery(id), 10).totalHits);
      } finally {
        nrt.release(s);
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
      final IndexSearcher s = nrt.get(gen, true);
      if (VERBOSE) {
        System.out.println(Thread.currentThread().getName() + ": nrt: got searcher=" + s);
      }
      try {
        assertEquals(0, s.search(new TermQuery(id), 10).totalHits);
      } finally {
        nrt.release(s);
      }
    }
    lastGens.set(gen);
  }

  private NRTManager nrt;
  private NRTManagerReopenThread nrtThread;

  @Override
  protected void doAfterWriter(ExecutorService es) throws Exception {
    final double minReopenSec = 0.01 + 0.05 * random.nextDouble();
    final double maxReopenSec = minReopenSec * (1.0 + 10 * random.nextDouble());

    if (VERBOSE) {
      System.out.println("TEST: make NRTManager maxReopenSec=" + maxReopenSec + " minReopenSec=" + minReopenSec);
    }

    nrt = new NRTManager(writer, es,
                         new SearcherWarmer() {
                           @Override
                           public void warm(IndexSearcher s) throws IOException {
                             TestNRTManager.this.warmCalled = true;
                             s.search(new TermQuery(new Term("body", "united")), 10);
                           }
                         });
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
    return nrt.get(random.nextBoolean());
  }

  @Override
  protected void releaseSearcher(IndexSearcher s) throws Exception {
    nrt.release(s);
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
}
