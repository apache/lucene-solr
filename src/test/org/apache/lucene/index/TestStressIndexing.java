package org.apache.lucene.index;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.util.*;
import org.apache.lucene.store.*;
import org.apache.lucene.document.*;
import org.apache.lucene.analysis.*;
import org.apache.lucene.search.*;
import org.apache.lucene.queryParser.*;

import java.util.Random;
import java.io.File;

public class TestStressIndexing extends LuceneTestCase {
  private static final Analyzer ANALYZER = new SimpleAnalyzer();
  private static final Random RANDOM = new Random();

  private static abstract class TimedThread extends Thread {
    boolean failed;
    int count;
    private static int RUN_TIME_SEC = 6;
    private TimedThread[] allThreads;

    abstract public void doWork() throws Throwable;

    TimedThread(TimedThread[] threads) {
      this.allThreads = threads;
    }

    public void run() {
      final long stopTime = System.currentTimeMillis() + 1000*RUN_TIME_SEC;

      count = 0;

      try {
        while(System.currentTimeMillis() < stopTime && !anyErrors()) {
          doWork();
          count++;
        }
      } catch (Throwable e) {
        System.out.println(Thread.currentThread() + ": exc");
        e.printStackTrace(System.out);
        failed = true;
      }
    }

    private boolean anyErrors() {
      for(int i=0;i<allThreads.length;i++)
        if (allThreads[i] != null && allThreads[i].failed)
          return true;
      return false;
    }
  }

  private static class IndexerThread extends TimedThread {
    IndexWriter writer;
    public int count;
    int nextID;

    public IndexerThread(IndexWriter writer, TimedThread[] threads) {
      super(threads);
      this.writer = writer;
    }

    public void doWork() throws Exception {
      // Add 10 docs:
      for(int j=0; j<10; j++) {
        Document d = new Document();
        int n = RANDOM.nextInt();
        d.add(new Field("id", Integer.toString(nextID++), Field.Store.YES, Field.Index.NOT_ANALYZED));
        d.add(new Field("contents", English.intToEnglish(n), Field.Store.NO, Field.Index.ANALYZED));
        writer.addDocument(d);
      }

      // Delete 5 docs:
      int deleteID = nextID-1;
      for(int j=0; j<5; j++) {
        writer.deleteDocuments(new Term("id", ""+deleteID));
        deleteID -= 2;
      }
    }
  }

  private static class SearcherThread extends TimedThread {
    private Directory directory;

    public SearcherThread(Directory directory, TimedThread[] threads) {
      super(threads);
      this.directory = directory;
    }

    public void doWork() throws Throwable {
      for (int i=0; i<100; i++)
        (new IndexSearcher(directory)).close();
      count += 100;
    }
  }

  /*
    Run one indexer and 2 searchers against single index as
    stress test.
  */
  public void runStressTest(Directory directory, boolean autoCommit, MergeScheduler mergeScheduler) throws Exception {
    IndexWriter modifier = new IndexWriter(directory, autoCommit, ANALYZER, true);

    modifier.setMaxBufferedDocs(10);

    TimedThread[] threads = new TimedThread[4];
    int numThread = 0;

    if (mergeScheduler != null)
      modifier.setMergeScheduler(mergeScheduler);

    // One modifier that writes 10 docs then removes 5, over
    // and over:
    IndexerThread indexerThread = new IndexerThread(modifier, threads);
    threads[numThread++] = indexerThread;
    indexerThread.start();
    
    IndexerThread indexerThread2 = new IndexerThread(modifier, threads);
    threads[numThread++] = indexerThread2;
    indexerThread2.start();
      
    // Two searchers that constantly just re-instantiate the
    // searcher:
    SearcherThread searcherThread1 = new SearcherThread(directory, threads);
    threads[numThread++] = searcherThread1;
    searcherThread1.start();

    SearcherThread searcherThread2 = new SearcherThread(directory, threads);
    threads[numThread++] = searcherThread2;
    searcherThread2.start();

    for(int i=0;i<numThread;i++)
      threads[i].join();

    modifier.close();

    for(int i=0;i<numThread;i++)
      assertTrue(!((TimedThread) threads[i]).failed);

    //System.out.println("    Writer: " + indexerThread.count + " iterations");
    //System.out.println("Searcher 1: " + searcherThread1.count + " searchers created");
    //System.out.println("Searcher 2: " + searcherThread2.count + " searchers created");
  }

  /*
    Run above stress test against RAMDirectory and then
    FSDirectory.
  */
  public void testStressIndexAndSearching() throws Exception {

    // RAMDir
    Directory directory = new MockRAMDirectory();
    runStressTest(directory, true, null);
    directory.close();

    // FSDir
    String tempDir = System.getProperty("java.io.tmpdir");
    File dirPath = new File(tempDir, "lucene.test.stress");
    directory = FSDirectory.getDirectory(dirPath);
    runStressTest(directory, true, null);
    directory.close();

    // With ConcurrentMergeScheduler, in RAMDir
    directory = new MockRAMDirectory();
    runStressTest(directory, true, new ConcurrentMergeScheduler());
    directory.close();

    // With ConcurrentMergeScheduler, in FSDir
    directory = FSDirectory.getDirectory(dirPath);
    runStressTest(directory, true, new ConcurrentMergeScheduler());
    directory.close();

    // With ConcurrentMergeScheduler and autoCommit=false, in RAMDir
    directory = new MockRAMDirectory();
    runStressTest(directory, false, new ConcurrentMergeScheduler());
    directory.close();

    // With ConcurrentMergeScheduler and autoCommit=false, in FSDir
    directory = FSDirectory.getDirectory(dirPath);
    runStressTest(directory, false, new ConcurrentMergeScheduler());
    directory.close();

    _TestUtil.rmDir(dirPath);
  }
}
