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

import java.nio.file.Path;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.store.*;
import org.apache.lucene.util.*;

public class TestAtomicUpdate extends LuceneTestCase {
  

  private static abstract class TimedThread extends Thread {
    volatile boolean failed;
    int count;
    private static float RUN_TIME_MSEC = atLeast(500);
    private TimedThread[] allThreads;

    abstract public void doWork() throws Throwable;

    TimedThread(TimedThread[] threads) {
      this.allThreads = threads;
    }

    @Override
    public void run() {
      final long stopTime = System.currentTimeMillis() + (long) RUN_TIME_MSEC;

      count = 0;

      try {
        do {
          if (anyErrors()) break;
          doWork();
          count++;
        } while(System.currentTimeMillis() < stopTime);
      } catch (Throwable e) {
        System.out.println(Thread.currentThread().getName() + ": exc");
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
    public IndexerThread(IndexWriter writer, TimedThread[] threads) {
      super(threads);
      this.writer = writer;
    }

    @Override
    public void doWork() throws Exception {
      // Update all 100 docs...
      for(int i=0; i<100; i++) {
        Document d = new Document();
        d.add(new StringField("id", Integer.toString(i), Field.Store.YES));
        d.add(new TextField("contents", English.intToEnglish(i+10*count), Field.Store.NO));
        d.add(new IntPoint("doc", i));
        d.add(new IntPoint("doc2d", i, i));
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

    @Override
    public void doWork() throws Throwable {
      IndexReader r = DirectoryReader.open(directory);
      assertEquals(100, r.numDocs());
      r.close();
    }
  }

  /*
    Run one indexer and 2 searchers against single index as
    stress test.
  */
  public void runTest(Directory directory) throws Exception {

    TimedThread[] threads = new TimedThread[4];

    IndexWriterConfig conf = new IndexWriterConfig(new MockAnalyzer(random()))
        .setMaxBufferedDocs(7);
    ((TieredMergePolicy) conf.getMergePolicy()).setMaxMergeAtOnce(3);
    IndexWriter writer = RandomIndexWriter.mockIndexWriter(directory, conf, random());

    // Establish a base index of 100 docs:
    for(int i=0;i<100;i++) {
      Document d = new Document();
      d.add(newStringField("id", Integer.toString(i), Field.Store.YES));
      d.add(newTextField("contents", English.intToEnglish(i), Field.Store.NO));
      if ((i-1)%7 == 0) {
        writer.commit();
      }
      writer.addDocument(d);
    }
    writer.commit();

    IndexReader r = DirectoryReader.open(directory);
    assertEquals(100, r.numDocs());
    r.close();

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
    directory = new MockDirectoryWrapper(random(), new RAMDirectory());
    runTest(directory);
    directory.close();

    // Second in an FSDirectory:
    Path dirPath = createTempDir("lucene.test.atomic");
    directory = newFSDirectory(dirPath);
    runTest(directory);
    directory.close();
  }
}
