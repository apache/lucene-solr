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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.English;
import org.apache.lucene.util.LuceneTestCase;

public class TestAtomicUpdate extends LuceneTestCase {
  
  private static abstract class TimedThread extends Thread {
    int numIterations;
    volatile Throwable failure;

    abstract public void doWork(int currentIteration) throws IOException;

    TimedThread(int numIterations) {
      this.numIterations = numIterations;
    }

    @Override
    public void run() {
      try {
        for (int count = 0; count < numIterations; count++) {
          doWork(count);
        }
      } catch (Throwable e) {
        failure = e;
        e.printStackTrace(System.out);
        throw new RuntimeException(e);
      }
    }
  }

  private static class IndexerThread extends TimedThread {
    IndexWriter writer;
    public IndexerThread(IndexWriter writer, int numIterations) {
      super(numIterations);
      this.writer = writer;
    }

    @Override
    public void doWork(int currentIteration) throws IOException {
      // Update all 100 docs...
      for(int i=0; i<100; i++) {
        Document d = new Document();
        d.add(new StringField("id", Integer.toString(i), Field.Store.YES));
        d.add(new TextField("contents", English.intToEnglish(i+10*currentIteration), Field.Store.NO));
        d.add(new IntPoint("doc", i));
        d.add(new IntPoint("doc2d", i, i));
        writer.updateDocument(new Term("id", Integer.toString(i)), d);
      }
    }
  }

  private static class SearcherThread extends TimedThread {
    private Directory directory;

    public SearcherThread(Directory directory, int numIterations) {
      super(numIterations);
      this.directory = directory;
    }

    @Override
    public void doWork(int currentIteration) throws IOException {
      IndexReader r = DirectoryReader.open(directory);
      assertEquals(100, r.numDocs());
      r.close();
    }
  }

  /*
   * Run N indexer and N searchers against single index as
   * stress test.
   */
  public void runTest(Directory directory) throws Exception {
    int indexThreads = TEST_NIGHTLY ? 2 : 1;
    int searchThreads = TEST_NIGHTLY ? 2 : 1;
    int indexIterations = TEST_NIGHTLY ? 10 : 1;
    int searchIterations = TEST_NIGHTLY ? 10 : 1;

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

    List<TimedThread> threads = new ArrayList<>();
    for (int i = 0; i < indexThreads; i++) {
      threads.add(new IndexerThread(writer, indexIterations));
    }
    for (int i = 0; i < searchThreads; i++) {
      threads.add(new SearcherThread(directory, searchIterations));
    }
    for (TimedThread thread : threads) {
      thread.start();
    }
    for (TimedThread thread : threads) {
      thread.join();
    }

    writer.close();
    
    for (TimedThread thread : threads) {
      if (thread.failure != null) {
        throw new RuntimeException("hit exception from " + thread, thread.failure);
      }
    }
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
