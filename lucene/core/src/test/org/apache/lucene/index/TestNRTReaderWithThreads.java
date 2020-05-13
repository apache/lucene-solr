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


import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.LuceneTestCase;

public class TestNRTReaderWithThreads extends LuceneTestCase {
  AtomicInteger seq = new AtomicInteger(1);

  public void testIndexing() throws Exception {
    Directory mainDir = newDirectory();
    if (mainDir instanceof MockDirectoryWrapper) {
      ((MockDirectoryWrapper)mainDir).setAssertNoDeleteOpenFile(true);
    }
    IndexWriter writer = new IndexWriter(
        mainDir,
        ensureSaneIWCOnNightly(newIndexWriterConfig(new MockAnalyzer(random()))
           .setMaxBufferedDocs(10)
           .setMergePolicy(newLogMergePolicy(false,2)))
    );
    IndexReader reader = writer.getReader(); // start pooling readers
    reader.close();
    int numThreads = TEST_NIGHTLY ? 4 : 2;
    int numIterations = TEST_NIGHTLY ? 2000 : 50;
    RunThread[] indexThreads = new RunThread[numThreads];
    for (int x=0; x < indexThreads.length; x++) {
      indexThreads[x] = new RunThread(x % 2, writer, numIterations);
      indexThreads[x].setName("Thread " + x);
      indexThreads[x].start();
    }    
   
    for (RunThread thread : indexThreads) {
      thread.join();
    }

    writer.close();
    mainDir.close();
    
    for (RunThread thread : indexThreads) {
      if (thread.failure != null) {
        throw new RuntimeException("hit exception from " + thread, thread.failure);
      }
    }
  }

  public class RunThread extends Thread {
    int type;
    IndexWriter writer;
    int numIterations;

    volatile Throwable failure;
    int delCount = 0;
    int addCount = 0;
    final Random r = new Random(random().nextLong());
    
    public RunThread(int type, IndexWriter writer, int numIterations) {
      this.type = type;
      this.writer = writer;
      this.numIterations = numIterations;
    }

    @Override
    public void run() {
      try {
        for (int iter = 0; iter < numIterations; iter++) {
          //int n = random.nextInt(2);
          if (type == 0) {
            int i = seq.addAndGet(1);
            Document doc = DocHelper.createDocument(i, "index1", 10);
            writer.addDocument(doc);
            addCount++;
          } else if (type == 1) {
            // we may or may not delete because the term may not exist,
            // however we're opening and closing the reader rapidly
            IndexReader reader = writer.getReader();
            int id = r.nextInt(seq.intValue());
            Term term = new Term("id", Integer.toString(id));
            int count = TestIndexWriterReader.count(term, reader);
            writer.deleteDocuments(term);
            reader.close();
            delCount += count;
          }
        }
      } catch (Throwable ex) {
        ex.printStackTrace(System.out);
        this.failure = failure;
        throw new RuntimeException(ex);
      }
    }
  }
}
