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
package org.apache.lucene.index;


import java.io.IOException;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.English;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;

public class TestTransactions extends LuceneTestCase {
  
  private static volatile boolean doFail;

  private class RandomFailure extends MockDirectoryWrapper.Failure {
    @Override
    public void eval(MockDirectoryWrapper dir) throws IOException {
      if (TestTransactions.doFail && random().nextInt() % 10 <= 3) {
        if (VERBOSE) {
          System.out.println(Thread.currentThread().getName() + " TEST: now fail on purpose");
          new Throwable().printStackTrace(System.out);
        }
        throw new IOException("now failing randomly but on purpose");
      }
    }
  }

  private static abstract class TimedThread extends Thread {
    volatile boolean failed;
    private static float RUN_TIME_MSEC = atLeast(500);
    private TimedThread[] allThreads;

    abstract public void doWork() throws Throwable;

    TimedThread(TimedThread[] threads) {
      this.allThreads = threads;
    }

    @Override
    public void run() {
      final long stopTime = System.currentTimeMillis() + (long) (RUN_TIME_MSEC);

      try {
        do {
          if (anyErrors()) break;
          doWork();
        } while (System.currentTimeMillis() < stopTime);
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

  private class IndexerThread extends TimedThread {
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

    @Override
    public void doWork() throws Throwable {

      IndexWriter writer1 = new IndexWriter(
          dir1,
          newIndexWriterConfig(new MockAnalyzer(random())).
              setMaxBufferedDocs(3).
              setMergeScheduler(new ConcurrentMergeScheduler()).
              setMergePolicy(newLogMergePolicy(2))
      );
      ((ConcurrentMergeScheduler) writer1.getConfig().getMergeScheduler()).setSuppressExceptions();

      // Intentionally use different params so flush/merge
      // happen @ different times
      IndexWriter writer2 = new IndexWriter(
          dir2,
          newIndexWriterConfig(new MockAnalyzer(random())).
              setMaxBufferedDocs(2).
              setMergeScheduler(new ConcurrentMergeScheduler()).
              setMergePolicy(newLogMergePolicy(3))
      );
      ((ConcurrentMergeScheduler) writer2.getConfig().getMergeScheduler()).setSuppressExceptions();

      update(writer1);
      update(writer2);

      TestTransactions.doFail = true;
      try {
        synchronized(lock) {
          try {
            writer1.prepareCommit();
          } catch (Throwable t) {
            // release resources
            try {
              writer1.rollback();
            } catch (Throwable ignore) {}
            try {
              writer2.rollback();
            } catch (Throwable ignore) {}
            return;
          }
          try {
            writer2.prepareCommit();
          } catch (Throwable t) {
            // release resources
            try {
              writer1.rollback();
            } catch (Throwable ignore) {}
            try {
              writer2.rollback();
            } catch (Throwable ignore) {}
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
      FieldType customType = new FieldType(StringField.TYPE_NOT_STORED);
      customType.setStoreTermVectors(true);
      for(int j=0; j<10; j++) {
        Document d = new Document();
        int n = random().nextInt();
        d.add(newField("id", Integer.toString(nextID++), customType));
        d.add(newTextField("contents", English.intToEnglish(n), Field.Store.NO));
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

    @Override
    public void doWork() throws Throwable {
      IndexReader r1=null, r2=null;
      synchronized(lock) {
        try {
          r1 = DirectoryReader.open(dir1);
          r2 = DirectoryReader.open(dir2);
        } catch (Exception e) {
          // can be rethrown as RuntimeException if it happens during a close listener
          if (!e.getMessage().contains("on purpose")) {
            throw e;
          }
          // release resources
          IOUtils.closeWhileHandlingException(r1, r2);
          return;
        }
      }
      if (r1.numDocs() != r2.numDocs()) {
        throw new RuntimeException("doc counts differ: r1=" + r1.numDocs() + " r2=" + r2.numDocs());
      }
      IOUtils.closeWhileHandlingException(r1, r2);
    }
  }

  public void initIndex(Directory dir) throws Throwable {
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    for(int j=0; j<7; j++) {
      Document d = new Document();
      int n = random().nextInt();
      d.add(newTextField("contents", English.intToEnglish(n), Field.Store.NO));
      writer.addDocument(d);
    }
    writer.close();
  }

  public void testTransactions() throws Throwable {
    // we cant use non-ramdir on windows, because this test needs to double-write.
    MockDirectoryWrapper dir1 = new MockDirectoryWrapper(random(), new RAMDirectory());
    MockDirectoryWrapper dir2 = new MockDirectoryWrapper(random(), new RAMDirectory());
    dir1.setPreventDoubleWrite(false);
    dir2.setPreventDoubleWrite(false);
    dir1.failOn(new RandomFailure());
    dir2.failOn(new RandomFailure());
    dir1.setFailOnOpenInput(false);
    dir2.setFailOnOpenInput(false);

    // We throw exceptions in deleteFile, which creates
    // leftover files:
    dir1.setAssertNoUnrefencedFilesOnClose(false);
    dir2.setAssertNoUnrefencedFilesOnClose(false);

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
      assertTrue(!threads[i].failed);
    dir1.close();
    dir2.close();
  }
}
