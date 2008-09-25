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
import java.util.Random;
import org.apache.lucene.store.*;
import org.apache.lucene.util.*;
import org.apache.lucene.analysis.*;
import org.apache.lucene.document.*;

public class TestTransactions extends LuceneTestCase
{
  private static final Random RANDOM = new Random();
  private static volatile boolean doFail;

  private class RandomFailure extends MockRAMDirectory.Failure {
    public void eval(MockRAMDirectory dir) throws IOException {
      if (TestTransactions.doFail && RANDOM.nextInt() % 10 <= 3)
        throw new IOException("now failing randomly but on purpose");
    }
  }

  private static abstract class TimedThread extends Thread {
    boolean failed;
    private static int RUN_TIME_SEC = 6;
    private TimedThread[] allThreads;

    abstract public void doWork() throws Throwable;

    TimedThread(TimedThread[] threads) {
      this.allThreads = threads;
    }

    public void run() {
      final long stopTime = System.currentTimeMillis() + 1000*RUN_TIME_SEC;

      try {
        while(System.currentTimeMillis() < stopTime && !anyErrors())
          doWork();
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
    Directory dir1;
    Directory dir2;
    Object lock;
    int nextID;

    public IndexerThread(Object lock, Directory dir1, Directory dir2, TimedThread[] threads) {
      super(threads);
      this.lock = lock;
      this.dir1 = dir1;
      this.dir2 = dir2;
    }

    public void doWork() throws Throwable {

      IndexWriter writer1 = new IndexWriter(dir1, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
      writer1.setMaxBufferedDocs(3);
      writer1.setMergeFactor(2);
      ((ConcurrentMergeScheduler) writer1.getMergeScheduler()).setSuppressExceptions();

      IndexWriter writer2 = new IndexWriter(dir2, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
      // Intentionally use different params so flush/merge
      // happen @ different times
      writer2.setMaxBufferedDocs(2);
      writer2.setMergeFactor(3);
      ((ConcurrentMergeScheduler) writer2.getMergeScheduler()).setSuppressExceptions();

      update(writer1);
      update(writer2);

      TestTransactions.doFail = true;
      try {
        synchronized(lock) {
          try {
            writer1.prepareCommit();
          } catch (Throwable t) {
            writer1.rollback();
            writer2.rollback();
            return;
          }
          try {
            writer2.prepareCommit();
          } catch (Throwable t) { 	
            writer1.rollback();
            writer2.rollback();
            return;
          }

          writer1.commit();
          writer2.commit();
        }
      } finally {
        TestTransactions.doFail = false;
      }  

      writer1.close();
      writer2.close();
    }

    public void update(IndexWriter writer) throws IOException {
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
    Directory dir1;
    Directory dir2;
    Object lock;

    public SearcherThread(Object lock, Directory dir1, Directory dir2, TimedThread[] threads) {
      super(threads);
      this.lock = lock;
      this.dir1 = dir1;
      this.dir2 = dir2;
    }

    public void doWork() throws Throwable {
      IndexReader r1, r2;
      synchronized(lock) {
        r1 = IndexReader.open(dir1);
        r2 = IndexReader.open(dir2);
      }
      if (r1.numDocs() != r2.numDocs())
        throw new RuntimeException("doc counts differ: r1=" + r1.numDocs() + " r2=" + r2.numDocs());
      r1.close();
      r2.close();
    }
  }

  public void initIndex(Directory dir) throws Throwable {
    IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
    for(int j=0; j<7; j++) {
      Document d = new Document();
      int n = RANDOM.nextInt();
      d.add(new Field("contents", English.intToEnglish(n), Field.Store.NO, Field.Index.ANALYZED));
      writer.addDocument(d);
    }
    writer.close();
  }

  public void testTransactions() throws Throwable {
    MockRAMDirectory dir1 = new MockRAMDirectory();
    MockRAMDirectory dir2 = new MockRAMDirectory();
    dir1.setPreventDoubleWrite(false);
    dir2.setPreventDoubleWrite(false);
    dir1.failOn(new RandomFailure());
    dir2.failOn(new RandomFailure());

    initIndex(dir1);
    initIndex(dir2);

    TimedThread[] threads = new TimedThread[3];
    int numThread = 0;

    IndexerThread indexerThread = new IndexerThread(this, dir1, dir2, threads);
    threads[numThread++] = indexerThread;
    indexerThread.start();

    SearcherThread searcherThread1 = new SearcherThread(this, dir1, dir2, threads);
    threads[numThread++] = searcherThread1;
    searcherThread1.start();

    SearcherThread searcherThread2 = new SearcherThread(this, dir1, dir2, threads);
    threads[numThread++] = searcherThread2;
    searcherThread2.start();

    for(int i=0;i<numThread;i++)
      threads[i].join();

    for(int i=0;i<numThread;i++)
      assertTrue(!((TimedThread) threads[i]).failed);
  }
}
