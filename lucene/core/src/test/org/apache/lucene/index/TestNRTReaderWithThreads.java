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

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

public class TestNRTReaderWithThreads extends LuceneTestCase {
  AtomicInteger seq = new AtomicInteger(1);

  public void testIndexing() throws Exception {
    Directory mainDir = newDirectory();
    IndexWriter writer = new IndexWriter(
        mainDir,
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).
            setMaxBufferedDocs(10).
            setMergePolicy(newLogMergePolicy(false,2))
    );
    IndexReader reader = writer.getReader(); // start pooling readers
    reader.close();
    RunThread[] indexThreads = new RunThread[4];
    for (int x=0; x < indexThreads.length; x++) {
      indexThreads[x] = new RunThread(x % 2, writer);
      indexThreads[x].setName("Thread " + x);
      indexThreads[x].start();
    }    
    long startTime = System.currentTimeMillis();
    long duration = 1000;
    while ((System.currentTimeMillis() - startTime) < duration) {
      Thread.sleep(100);
    }
    int delCount = 0;
    int addCount = 0;
    for (int x=0; x < indexThreads.length; x++) {
      indexThreads[x].run = false;
      assertNull("Exception thrown: "+indexThreads[x].ex, indexThreads[x].ex);
      addCount += indexThreads[x].addCount;
      delCount += indexThreads[x].delCount;
    }
    for (int x=0; x < indexThreads.length; x++) {
      indexThreads[x].join();
    }
    for (int x=0; x < indexThreads.length; x++) {
      assertNull("Exception thrown: "+indexThreads[x].ex, indexThreads[x].ex);
    }
    //System.out.println("addCount:"+addCount);
    //System.out.println("delCount:"+delCount);
    writer.close();
    mainDir.close();
  }

  public class RunThread extends Thread {
    IndexWriter writer;
    volatile boolean run = true;
    volatile Throwable ex;
    int delCount = 0;
    int addCount = 0;
    int type;
    final Random r = new Random(random().nextLong());
    
    public RunThread(int type, IndexWriter writer) {
      this.type = type;
      this.writer = writer;
    }

    @Override
    public void run() {
      try {
        while (run) {
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
        this.ex = ex;
        run = false;
      }
    }
  }
}
