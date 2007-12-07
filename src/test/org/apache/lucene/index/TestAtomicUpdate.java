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
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.queryParser.*;
import org.apache.lucene.util._TestUtil;

import org.apache.lucene.util.LuceneTestCase;

import java.util.Random;
import java.io.File;

public class TestAtomicUpdate extends LuceneTestCase {
  private static final Analyzer ANALYZER = new SimpleAnalyzer();
  private static final Random RANDOM = new Random();

  private static abstract class TimedThread extends Thread {
    boolean failed;
    int count;
    private static int RUN_TIME_SEC = 3;
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

    public IndexerThread(IndexWriter writer, TimedThread[] threads) {
      super(threads);
      this.writer = writer;
    }

    public void doWork() throws Exception {
      // Update all 100 docs...
      for(int i=0; i<100; i++) {
        Document d = new Document();
        int n = RANDOM.nextInt();
        d.add(new Field("id", Integer.toString(i), Field.Store.YES, Field.Index.UN_TOKENIZED));
        d.add(new Field("contents", English.intToEnglish(i+10*count), Field.Store.NO, Field.Index.TOKENIZED));
        writer.updateDocument(new Term("id", Integer.toString(i)), d);
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
      IndexReader r = IndexReader.open(directory);
      try {
        assertEquals(100, r.numDocs());
      } catch (Throwable t) {
        throw t;
      }
      r.close();
    }
  }

  /*
    Run one indexer and 2 searchers against single index as
    stress test.
  */
  public void runTest(Directory directory) throws Exception {

    TimedThread[] threads = new TimedThread[4];

    IndexWriter writer = new IndexWriter(directory, ANALYZER, true);

    // Establish a base index of 100 docs:
    for(int i=0;i<100;i++) {
      Document d = new Document();
      d.add(new Field("id", Integer.toString(i), Field.Store.YES, Field.Index.UN_TOKENIZED));
      d.add(new Field("contents", English.intToEnglish(i), Field.Store.NO, Field.Index.TOKENIZED));
      writer.addDocument(d);
    }
    writer.flush();

    IndexerThread indexerThread = new IndexerThread(writer, threads);
    threads[0] = indexerThread;
    indexerThread.start();
    
    IndexerThread indexerThread2 = new IndexerThread(writer, threads);
    threads[1] = indexerThread2;
    indexerThread2.start();
      
    SearcherThread searcherThread1 = new SearcherThread(directory, threads);
    threads[2] = searcherThread1;
    searcherThread1.start();

    SearcherThread searcherThread2 = new SearcherThread(directory, threads);
    threads[3] = searcherThread2;
    searcherThread2.start();

    indexerThread.join();
    indexerThread2.join();
    searcherThread1.join();
    searcherThread2.join();

    writer.close();

    assertTrue("hit unexpected exception in indexer", !indexerThread.failed);
    assertTrue("hit unexpected exception in indexer2", !indexerThread2.failed);
    assertTrue("hit unexpected exception in search1", !searcherThread1.failed);
    assertTrue("hit unexpected exception in search2", !searcherThread2.failed);
    //System.out.println("    Writer: " + indexerThread.count + " iterations");
    //System.out.println("Searcher 1: " + searcherThread1.count + " searchers created");
    //System.out.println("Searcher 2: " + searcherThread2.count + " searchers created");
  }

  /*
    Run above stress test against RAMDirectory and then
    FSDirectory.
  */
  public void testAtomicUpdates() throws Exception {

    Directory directory;

    // First in a RAM directory:
    directory = new MockRAMDirectory();
    runTest(directory);
    directory.close();

    // Second in an FSDirectory:
    String tempDir = System.getProperty("java.io.tmpdir");
    File dirPath = new File(tempDir, "lucene.test.atomic");
    directory = FSDirectory.getDirectory(dirPath);
    runTest(directory);
    directory.close();
    _TestUtil.rmDir(dirPath);
  }
}
