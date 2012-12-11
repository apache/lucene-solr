package org.apache.lucene.index;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

public class TestIndexWriterNRTIsCurrent extends LuceneTestCase {

  public static class ReaderHolder {
    volatile DirectoryReader reader;
    volatile boolean stop = false;
  }

  public void testIsCurrentWithThreads() throws
      IOException, InterruptedException {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT,
        new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir, conf);
    ReaderHolder holder = new ReaderHolder();
    ReaderThread[] threads = new ReaderThread[atLeast(3)];
    final CountDownLatch latch = new CountDownLatch(1);
    WriterThread writerThread = new WriterThread(holder, writer,
        atLeast(500), random(), latch);
    for (int i = 0; i < threads.length; i++) {
      threads[i] = new ReaderThread(holder, latch);
      threads[i].start();
    }
    writerThread.start();

    writerThread.join();
    boolean failed = writerThread.failed != null;
    if (failed)
      writerThread.failed.printStackTrace();
    for (int i = 0; i < threads.length; i++) {
      threads[i].join();
      if (threads[i].failed != null) {
        threads[i].failed.printStackTrace();
        failed = true;
      }
    }
    assertFalse(failed);
    writer.close();
    dir.close();

  }

  public static class WriterThread extends Thread {
    private final ReaderHolder holder;
    private final IndexWriter writer;
    private final int numOps;
    private boolean countdown = true;
    private final CountDownLatch latch;
    Throwable failed;

    WriterThread(ReaderHolder holder, IndexWriter writer, int numOps,
        Random random, CountDownLatch latch) {
      super();
      this.holder = holder;
      this.writer = writer;
      this.numOps = numOps;
      this.latch = latch;
    }

    @Override
    public void run() {
      DirectoryReader currentReader = null;
      Random random = LuceneTestCase.random();
      try {
        Document doc = new Document();
        doc.add(new TextField("id", "1", Field.Store.NO));
        writer.addDocument(doc);
        holder.reader = currentReader = writer.getReader(true);
        Term term = new Term("id");
        for (int i = 0; i < numOps && !holder.stop; i++) {
          float nextOp = random.nextFloat();
          if (nextOp < 0.3) {
            term.set("id", new BytesRef("1"));
            writer.updateDocument(term, doc);
          } else if (nextOp < 0.5) {
            writer.addDocument(doc);
          } else {
            term.set("id", new BytesRef("1"));
            writer.deleteDocuments(term);
          }
          if (holder.reader != currentReader) {
            holder.reader = currentReader;
            if (countdown) {
              countdown = false;
              latch.countDown();
            }
          }
          if (random.nextBoolean()) {
            writer.commit();
            final DirectoryReader newReader = DirectoryReader
                .openIfChanged(currentReader);
            if (newReader != null) { 
              currentReader.decRef();
              currentReader = newReader;
            }
            if (currentReader.numDocs() == 0) {
              writer.addDocument(doc);
            }
          }
        }
      } catch (Throwable e) {
        failed = e;
      } finally {
        holder.reader = null;
        if (countdown) {
          latch.countDown();
        }
        if (currentReader != null) {
          try {
            currentReader.decRef();
          } catch (IOException e) {
          }
        }
      }
      if (VERBOSE) {
        System.out.println("writer stopped - forced by reader: " + holder.stop);
      }
    }
    
  }

  public static final class ReaderThread extends Thread {
    private final ReaderHolder holder;
    private final CountDownLatch latch;
    Throwable failed;

    ReaderThread(ReaderHolder holder, CountDownLatch latch) {
      super();
      this.holder = holder;
      this.latch = latch;
    }

    @Override
    public void run() {
      try {
        latch.await();
      } catch (InterruptedException e) {
        failed = e;
        return;
      }
      DirectoryReader reader;
      while ((reader = holder.reader) != null) {
        if (reader.tryIncRef()) {
          try {
            boolean current = reader.isCurrent();
            if (VERBOSE) {
              System.out.println("Thread: " + Thread.currentThread() + " Reader: " + reader + " isCurrent:" + current);
            }

            assertFalse(current);
          } catch (Throwable e) {
            if (VERBOSE) {
              System.out.println("FAILED Thread: " + Thread.currentThread() + " Reader: " + reader + " isCurrent: false");
            }
            failed = e;
            holder.stop = true;
            return;
          } finally {
            try {
              reader.decRef();
            } catch (IOException e) {
              if (failed == null) {
                failed = e;
              }
              return;
            }
          }
        }
      }
    }
  }
}
