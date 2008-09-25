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
import java.io.IOException;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;
import org.apache.lucene.store.MockRAMDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

public class TestIndexWriterExceptions extends LuceneTestCase {

  final private static boolean DEBUG = false;

  private class IndexerThread extends Thread {

    IndexWriter writer;

    final Random r = new java.util.Random(47);
    Throwable failure;

    public IndexerThread(int i, IndexWriter writer) {
      setName("Indexer " + i);
      this.writer = writer;
    }

    public void run() {

      final Document doc = new Document();

      doc.add(new Field("content1", "aaa bbb ccc ddd", Field.Store.YES, Field.Index.ANALYZED));
      doc.add(new Field("content6", "aaa bbb ccc ddd", Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
      doc.add(new Field("content2", "aaa bbb ccc ddd", Field.Store.YES, Field.Index.NOT_ANALYZED));
      doc.add(new Field("content3", "aaa bbb ccc ddd", Field.Store.YES, Field.Index.NO));

      doc.add(new Field("content4", "aaa bbb ccc ddd", Field.Store.NO, Field.Index.ANALYZED));
      doc.add(new Field("content5", "aaa bbb ccc ddd", Field.Store.NO, Field.Index.NOT_ANALYZED));

      doc.add(new Field("content7", "aaa bbb ccc ddd", Field.Store.NO, Field.Index.NOT_ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));

      final Field idField = new Field("id", "", Field.Store.YES, Field.Index.NOT_ANALYZED);
      doc.add(idField);

      final long stopTime = System.currentTimeMillis() + 3000;

      while(System.currentTimeMillis() < stopTime) {
        doFail.set(this);
        final String id = ""+r.nextInt(50);
        idField.setValue(id);
        Term idTerm = new Term("id", id);
        try {
          writer.updateDocument(idTerm, doc);
        } catch (RuntimeException re) {
          if (DEBUG) {
            System.out.println("EXC: ");
            re.printStackTrace(System.out);
          }
          try {
            _TestUtil.checkIndex(writer.getDirectory());
          } catch (IOException ioe) {
            System.out.println(Thread.currentThread().getName() + ": unexpected exception1");
            ioe.printStackTrace(System.out);
            failure = ioe;
            break;
          }
        } catch (Throwable t) {
          System.out.println(Thread.currentThread().getName() + ": unexpected exception2");
          t.printStackTrace(System.out);
          failure = t;
          break;
        }

        doFail.set(null);

        // After a possible exception (above) I should be able
        // to add a new document without hitting an
        // exception:
        try {
          writer.updateDocument(idTerm, doc);
        } catch (Throwable t) {
          System.out.println(Thread.currentThread().getName() + ": unexpected exception3");
          t.printStackTrace(System.out);
          failure = t;
          break;
        }
      }
    }
  }

  ThreadLocal doFail = new ThreadLocal();

  public class MockIndexWriter extends IndexWriter {
    Random r = new java.util.Random(17);

    public MockIndexWriter(Directory dir, Analyzer a, boolean create, MaxFieldLength mfl) throws IOException {
      super(dir, a, create, mfl);
    }

    boolean testPoint(String name) {
      if (doFail.get() != null && !name.equals("startDoFlush") && r.nextInt(20) == 17) {
        if (DEBUG) {
          System.out.println(Thread.currentThread().getName() + ": NOW FAIL: " + name);
          //new Throwable().printStackTrace(System.out);
        }
        throw new RuntimeException(Thread.currentThread().getName() + ": intentionally failing at " + name);
      }
      return true;
    }
  }

  public void testRandomExceptions() throws Throwable {
    MockRAMDirectory dir = new MockRAMDirectory();

    MockIndexWriter writer  = new MockIndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
    ((ConcurrentMergeScheduler) writer.getMergeScheduler()).setSuppressExceptions();
    //writer.setMaxBufferedDocs(10);
    writer.setRAMBufferSizeMB(0.1);

    if (DEBUG)
      writer.setInfoStream(System.out);

    IndexerThread thread = new IndexerThread(0, writer);
    thread.run();
    if (thread.failure != null) {
      thread.failure.printStackTrace(System.out);
      fail("thread " + thread.getName() + ": hit unexpected failure");
    }

    writer.commit();

    try {
      writer.close();
    } catch (Throwable t) {
      System.out.println("exception during close:");
      t.printStackTrace(System.out);
      writer.rollback();
    }

    // Confirm that when doc hits exception partway through tokenization, it's deleted:
    IndexReader r2 = IndexReader.open(dir);
    final int count = r2.docFreq(new Term("content4", "aaa"));
    final int count2 = r2.docFreq(new Term("content4", "ddd"));
    assertEquals(count, count2);
    r2.close();

    _TestUtil.checkIndex(dir);
  }

  public void testRandomExceptionsThreads() throws Throwable {

    MockRAMDirectory dir = new MockRAMDirectory();
    MockIndexWriter writer  = new MockIndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
    ((ConcurrentMergeScheduler) writer.getMergeScheduler()).setSuppressExceptions();
    //writer.setMaxBufferedDocs(10);
    writer.setRAMBufferSizeMB(0.2);

    if (DEBUG)
      writer.setInfoStream(System.out);

    final int NUM_THREADS = 4;

    final IndexerThread[] threads = new IndexerThread[NUM_THREADS];
    for(int i=0;i<NUM_THREADS;i++) {
      threads[i] = new IndexerThread(i, writer);
      threads[i].start();
    }

    for(int i=0;i<NUM_THREADS;i++)
      threads[i].join();

    for(int i=0;i<NUM_THREADS;i++)
      if (threads[i].failure != null)
        fail("thread " + threads[i].getName() + ": hit unexpected failure");

    writer.commit();

    try {
      writer.close();
    } catch (Throwable t) {
      System.out.println("exception during close:");
      t.printStackTrace(System.out);
      writer.rollback();
    }

    // Confirm that when doc hits exception partway through tokenization, it's deleted:
    IndexReader r2 = IndexReader.open(dir);
    final int count = r2.docFreq(new Term("content4", "aaa"));
    final int count2 = r2.docFreq(new Term("content4", "ddd"));
    assertEquals(count, count2);
    r2.close();

    _TestUtil.checkIndex(dir);
  }
}
