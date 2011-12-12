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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BitVector;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

public class TestIndexReaderReopen extends LuceneTestCase {
  
  public void testReopen() throws Exception {
    final Directory dir1 = newDirectory();
    
    createIndex(random, dir1, false);
    performDefaultTests(new TestReopen() {

      @Override
      protected void modifyIndex(int i) throws IOException {
        TestIndexReaderReopen.modifyIndex(i, dir1);
      }

      @Override
      protected IndexReader openReader() throws IOException {
        return IndexReader.open(dir1);
      }
      
    });
    dir1.close();
    
    final Directory dir2 = newDirectory();
    
    createIndex(random, dir2, true);
    performDefaultTests(new TestReopen() {

      @Override
      protected void modifyIndex(int i) throws IOException {
        TestIndexReaderReopen.modifyIndex(i, dir2);
      }

      @Override
      protected IndexReader openReader() throws IOException {
        return IndexReader.open(dir2);
      }
      
    });
    dir2.close();
  }
  
  public void testParallelReaderReopen() throws Exception {
    final Directory dir1 = newDirectory();
    createIndex(random, dir1, true);
    final Directory dir2 = newDirectory();
    createIndex(random, dir2, true);
    
    performDefaultTests(new TestReopen() {

      @Override
      protected void modifyIndex(int i) throws IOException {
        TestIndexReaderReopen.modifyIndex(i, dir1);
        TestIndexReaderReopen.modifyIndex(i, dir2);
      }

      @Override
      protected IndexReader openReader() throws IOException {
        ParallelReader pr = new ParallelReader();
        pr.add(IndexReader.open(dir1));
        pr.add(IndexReader.open(dir2));
        return pr;
      }
      
    });
    dir1.close();
    dir2.close();
    
    final Directory dir3 = newDirectory();
    createIndex(random, dir3, true);
    final Directory dir4 = newDirectory();
    createIndex(random, dir4, true);

    performTestsWithExceptionInReopen(new TestReopen() {

      @Override
      protected void modifyIndex(int i) throws IOException {
        TestIndexReaderReopen.modifyIndex(i, dir3);
        TestIndexReaderReopen.modifyIndex(i, dir4);
      }

      @Override
      protected IndexReader openReader() throws IOException {
        ParallelReader pr = new ParallelReader();
        pr.add(IndexReader.open(dir3));
        pr.add(IndexReader.open(dir4));
        // Does not implement reopen, so
        // hits exception:
        pr.add(new FilterIndexReader(IndexReader.open(dir3)));
        return pr;
      }
      
    });
    dir3.close();
    dir4.close();
  }

  // LUCENE-1228: IndexWriter.commit() does not update the index version
  // populate an index in iterations.
  // at the end of every iteration, commit the index and reopen/recreate the reader.
  // in each iteration verify the work of previous iteration. 
  // try this once with reopen once recreate, on both RAMDir and FSDir.
  public void testCommitReopen () throws IOException {
    Directory dir = newDirectory();
    doTestReopenWithCommit(random, dir, true);
    dir.close();
  }
  public void testCommitRecreate () throws IOException {
    Directory dir = newDirectory();
    doTestReopenWithCommit(random, dir, false);
    dir.close();
  }

  private void doTestReopenWithCommit (Random random, Directory dir, boolean withReopen) throws IOException {
    IndexWriter iwriter = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random)).setOpenMode(
                                                              OpenMode.CREATE).setMergeScheduler(new SerialMergeScheduler()).setMergePolicy(newLogMergePolicy()));
    iwriter.commit();
    IndexReader reader = IndexReader.open(dir);
    try {
      int M = 3;
      FieldType customType = new FieldType(TextField.TYPE_STORED);
      customType.setTokenized(false);
      FieldType customType2 = new FieldType(TextField.TYPE_STORED);
      customType2.setTokenized(false);
      customType2.setOmitNorms(true);
      FieldType customType3 = new FieldType();
      customType3.setStored(true);
      for (int i=0; i<4; i++) {
        for (int j=0; j<M; j++) {
          Document doc = new Document();
          doc.add(newField("id", i+"_"+j, customType));
          doc.add(newField("id2", i+"_"+j, customType2));
          doc.add(newField("id3", i+"_"+j, customType3));
          iwriter.addDocument(doc);
          if (i>0) {
            int k = i-1;
            int n = j + k*M;
            Document prevItereationDoc = reader.document(n);
            assertNotNull(prevItereationDoc);
            String id = prevItereationDoc.get("id");
            assertEquals(k+"_"+j, id);
          }
        }
        iwriter.commit();
        if (withReopen) {
          // reopen
          IndexReader r2 = IndexReader.openIfChanged(reader);
          if (r2 != null) {
            reader.close();
            reader = r2;
          }
        } else {
          // recreate
          reader.close();
          reader = IndexReader.open(dir);
        }
      }
    } finally {
      iwriter.close();
      reader.close();
    }
  }
  
  public void testMultiReaderReopen() throws Exception {
    final Directory dir1 = newDirectory();
    createIndex(random, dir1, true);

    final Directory dir2 = newDirectory();
    createIndex(random, dir2, true);

    performDefaultTests(new TestReopen() {

      @Override
      protected void modifyIndex(int i) throws IOException {
        TestIndexReaderReopen.modifyIndex(i, dir1);
        TestIndexReaderReopen.modifyIndex(i, dir2);
      }

      @Override
      protected IndexReader openReader() throws IOException {
        return new MultiReader(IndexReader.open(dir1),
            IndexReader.open(dir2));
      }
      
    });

    dir1.close();
    dir2.close();
    
    final Directory dir3 = newDirectory();
    createIndex(random, dir3, true);

    final Directory dir4 = newDirectory();
    createIndex(random, dir4, true);

    performTestsWithExceptionInReopen(new TestReopen() {

      @Override
      protected void modifyIndex(int i) throws IOException {
        TestIndexReaderReopen.modifyIndex(i, dir3);
        TestIndexReaderReopen.modifyIndex(i, dir4);
      }

      @Override
      protected IndexReader openReader() throws IOException {
        return new MultiReader(IndexReader.open(dir3),
            IndexReader.open(dir4),
            // Does not implement reopen, so
            // hits exception:
            new FilterIndexReader(IndexReader.open(dir3)));
      }
      
    });
    dir3.close();
    dir4.close();
  }

  public void testMixedReaders() throws Exception {
    final Directory dir1 = newDirectory();
    createIndex(random, dir1, true);
    final Directory dir2 = newDirectory();
    createIndex(random, dir2, true);
    final Directory dir3 = newDirectory();
    createIndex(random, dir3, false);
    final Directory dir4 = newDirectory();
    createIndex(random, dir4, true);
    final Directory dir5 = newDirectory();
    createIndex(random, dir5, false);
    
    performDefaultTests(new TestReopen() {

      @Override
      protected void modifyIndex(int i) throws IOException {
        TestIndexReaderReopen.modifyIndex(i, dir4);
        TestIndexReaderReopen.modifyIndex(i, dir5);
      }

      @Override
      protected IndexReader openReader() throws IOException {
        MultiReader mr1 = new MultiReader(IndexReader.open(dir1), IndexReader.open(dir2));
        MultiReader mr2 = new MultiReader(IndexReader.open(dir3), IndexReader.open(dir4));
        return new MultiReader(mr1, mr2, IndexReader.open(dir5));
      }
    });
    dir1.close();
    dir2.close();
    dir3.close();
    dir4.close();
    dir5.close();
  }  
  
  private void performDefaultTests(TestReopen test) throws Exception {

    IndexReader index1 = test.openReader();
    IndexReader index2 = test.openReader();
        
    TestIndexReader.assertIndexEquals(index1, index2);

    // verify that reopen() does not return a new reader instance
    // in case the index has no changes
    ReaderCouple couple = refreshReader(index2, false);
    assertTrue(couple.refreshedReader == index2);
    
    couple = refreshReader(index2, test, 0, true);
    index1.close();
    index1 = couple.newReader;

    IndexReader index2_refreshed = couple.refreshedReader;
    index2.close();
    
    // test if refreshed reader and newly opened reader return equal results
    TestIndexReader.assertIndexEquals(index1, index2_refreshed);

    index2_refreshed.close();
    assertReaderClosed(index2, true, true);
    assertReaderClosed(index2_refreshed, true, true);

    index2 = test.openReader();
    
    for (int i = 1; i < 4; i++) {
      
      index1.close();
      couple = refreshReader(index2, test, i, true);
      // refresh IndexReader
      index2.close();
      
      index2 = couple.refreshedReader;
      index1 = couple.newReader;
      TestIndexReader.assertIndexEquals(index1, index2);
    }
    
    index1.close();
    index2.close();
    assertReaderClosed(index1, true, true);
    assertReaderClosed(index2, true, true);
  }

  public void testReferenceCountingMultiReader() throws IOException {
    for (int mode = 0; mode <=1; mode++) {
      Directory dir1 = newDirectory();
      createIndex(random, dir1, false);
      Directory dir2 = newDirectory();
      createIndex(random, dir2, true);
      
      IndexReader reader1 = IndexReader.open(dir1);
      assertRefCountEquals(1, reader1);

      IndexReader initReader2 = IndexReader.open(dir2);
      IndexReader multiReader1 = new MultiReader(new IndexReader[] {reader1, initReader2}, (mode == 0));
      modifyIndex(0, dir2);
      assertRefCountEquals(1 + mode, reader1);
      
      IndexReader multiReader2 = IndexReader.openIfChanged(multiReader1);
      assertNotNull(multiReader2);
      // index1 hasn't changed, so multiReader2 should share reader1 now with multiReader1
      assertRefCountEquals(2 + mode, reader1);
      
      modifyIndex(0, dir1);
      IndexReader reader2 = IndexReader.openIfChanged(reader1);
      assertNotNull(reader2);
      assertNull(IndexReader.openIfChanged(reader2));
      assertRefCountEquals(2 + mode, reader1);

      if (mode == 1) {
        initReader2.close();
      }
      
      modifyIndex(1, dir1);
      IndexReader reader3 = IndexReader.openIfChanged(reader2);
      assertNotNull(reader3);
      assertRefCountEquals(2 + mode, reader1);
      assertRefCountEquals(1, reader2);
      
      multiReader1.close();
      assertRefCountEquals(1 + mode, reader1);
      
      multiReader1.close();
      assertRefCountEquals(1 + mode, reader1);

      if (mode == 1) {
        initReader2.close();
      }
      
      reader1.close();
      assertRefCountEquals(1, reader1);
      
      multiReader2.close();
      assertRefCountEquals(0, reader1);
      
      multiReader2.close();
      assertRefCountEquals(0, reader1);
      
      reader3.close();
      assertRefCountEquals(0, reader1);
      assertReaderClosed(reader1, true, false);
      
      reader2.close();
      assertRefCountEquals(0, reader1);
      assertReaderClosed(reader1, true, false);
      
      reader2.close();
      assertRefCountEquals(0, reader1);
      
      reader3.close();
      assertRefCountEquals(0, reader1);
      assertReaderClosed(reader1, true, true);
      dir1.close();
      dir2.close();
    }

  }
  
  private void performTestsWithExceptionInReopen(TestReopen test) throws Exception {
    IndexReader index1 = test.openReader();
    IndexReader index2 = test.openReader();

    TestIndexReader.assertIndexEquals(index1, index2);
    
    try {
      refreshReader(index1, test, 0, true);
      fail("Expected exception not thrown.");
    } catch (Exception e) {
      // expected exception
    }
    
    // index2 should still be usable and unaffected by the failed reopen() call
    TestIndexReader.assertIndexEquals(index1, index2);

    index1.close();
    index2.close();
  }
  
  public void testThreadSafety() throws Exception {
    final Directory dir = newDirectory();
    // NOTE: this also controls the number of threads!
    final int n = _TestUtil.nextInt(random, 20, 40);
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random)));
    for (int i = 0; i < n; i++) {
      writer.addDocument(createDocument(i, 3));
    }
    writer.forceMerge(1);
    writer.close();

    final TestReopen test = new TestReopen() {      
      @Override
      protected void modifyIndex(int i) throws IOException {
       IndexWriter modifier = new IndexWriter(dir, new IndexWriterConfig(
         TEST_VERSION_CURRENT, new MockAnalyzer(random)));
       modifier.addDocument(createDocument(n + i, 6));
       modifier.close();
      }

      @Override
      protected IndexReader openReader() throws IOException {
        return IndexReader.open(dir);
      }      
    };
    
    final List<ReaderCouple> readers = Collections.synchronizedList(new ArrayList<ReaderCouple>());
    IndexReader firstReader = IndexReader.open(dir);
    IndexReader reader = firstReader;
    final Random rnd = random;
    
    ReaderThread[] threads = new ReaderThread[n];
    final Set<IndexReader> readersToClose = Collections.synchronizedSet(new HashSet<IndexReader>());
    
    for (int i = 0; i < n; i++) {
      if (i % 2 == 0) {
        IndexReader refreshed = IndexReader.openIfChanged(reader);
        if (refreshed != null) {
          readersToClose.add(reader);
          reader = refreshed;
        }
      }
      final IndexReader r = reader;
      
      final int index = i;    
      
      ReaderThreadTask task;
      
      if (i < 4 || (i >=10 && i < 14) || i > 18) {
        task = new ReaderThreadTask() {
          
          @Override
          public void run() throws Exception {
            while (!stopped) {
              if (index % 2 == 0) {
                // refresh reader synchronized
                ReaderCouple c = (refreshReader(r, test, index, true));
                readersToClose.add(c.newReader);
                readersToClose.add(c.refreshedReader);
                readers.add(c);
                // prevent too many readers
                break;
              } else {
                // not synchronized
                IndexReader refreshed = IndexReader.openIfChanged(r);
                if (refreshed == null) {
                  refreshed = r;
                }
                
                IndexSearcher searcher = newSearcher(refreshed);
                ScoreDoc[] hits = searcher.search(
                    new TermQuery(new Term("field1", "a" + rnd.nextInt(refreshed.maxDoc()))),
                    null, 1000).scoreDocs;
                if (hits.length > 0) {
                  searcher.doc(hits[0].doc);
                }
                if (refreshed != r) {
                  refreshed.close();
                }
              }
              synchronized(this) {
                wait(_TestUtil.nextInt(random, 1, 100));
              }
            }
          }
          
        };
      } else {
        task = new ReaderThreadTask() {
          @Override
          public void run() throws Exception {
            while (!stopped) {
              int numReaders = readers.size();
              if (numReaders > 0) {
                ReaderCouple c =  readers.get(rnd.nextInt(numReaders));
                TestIndexReader.assertIndexEquals(c.newReader, c.refreshedReader);
              }
              
              synchronized(this) {
                wait(_TestUtil.nextInt(random, 1, 100));
              }
            }
          }
        };
      }
      
      threads[i] = new ReaderThread(task);
      threads[i].start();
    }
    
    synchronized(this) {
      wait(1000);
    }
    
    for (int i = 0; i < n; i++) {
      if (threads[i] != null) {
        threads[i].stopThread();
      }
    }
    
    for (int i = 0; i < n; i++) {
      if (threads[i] != null) {
        threads[i].join();
        if (threads[i].error != null) {
          String msg = "Error occurred in thread " + threads[i].getName() + ":\n" + threads[i].error.getMessage();
          fail(msg);
        }
      }
      
    }
    
    for (final IndexReader readerToClose : readersToClose) {
      readerToClose.close();
    }
    
    firstReader.close();
    reader.close();
    
    for (final IndexReader readerToClose : readersToClose) {
      assertReaderClosed(readerToClose, true, true);
    }

    assertReaderClosed(reader, true, true);
    assertReaderClosed(firstReader, true, true);

    dir.close();
  }
  
  private static class ReaderCouple {
    ReaderCouple(IndexReader r1, IndexReader r2) {
      newReader = r1;
      refreshedReader = r2;
    }
    
    IndexReader newReader;
    IndexReader refreshedReader;
  }
  
  private abstract static class ReaderThreadTask {
    protected volatile boolean stopped;
    public void stop() {
      this.stopped = true;
    }
    
    public abstract void run() throws Exception;
  }
  
  private static class ReaderThread extends Thread {
    private ReaderThreadTask task;
    private Throwable error;
    
    
    ReaderThread(ReaderThreadTask task) {
      this.task = task;
    }
    
    public void stopThread() {
      this.task.stop();
    }
    
    @Override
    public void run() {
      try {
        this.task.run();
      } catch (Throwable r) {
        r.printStackTrace(System.out);
        this.error = r;
      }
    }
  }
  
  private Object createReaderMutex = new Object();
  
  private ReaderCouple refreshReader(IndexReader reader, boolean hasChanges) throws IOException {
    return refreshReader(reader, null, -1, hasChanges);
  }
  
  ReaderCouple refreshReader(IndexReader reader, TestReopen test, int modify, boolean hasChanges) throws IOException {
    synchronized (createReaderMutex) {
      IndexReader r = null;
      if (test != null) {
        test.modifyIndex(modify);
        r = test.openReader();
      }
      
      IndexReader refreshed = null;
      try {
        refreshed = IndexReader.openIfChanged(reader);
        if (refreshed == null) {
          refreshed = reader;
        }
      } finally {
        if (refreshed == null && r != null) {
          // Hit exception -- close opened reader
          r.close();
        }
      }
      
      if (hasChanges) {
        if (refreshed == reader) {
          fail("No new IndexReader instance created during refresh.");
        }
      } else {
        if (refreshed != reader) {
          fail("New IndexReader instance created during refresh even though index had no changes.");
        }
      }
      
      return new ReaderCouple(r, refreshed);
    }
  }
  
  public static void createIndex(Random random, Directory dir, boolean multiSegment) throws IOException {
    IndexWriter.unlock(dir);
    IndexWriter w = new IndexWriter(dir, LuceneTestCase.newIndexWriterConfig(random,
        TEST_VERSION_CURRENT, new MockAnalyzer(random))
        .setMergePolicy(new LogDocMergePolicy()));
    
    for (int i = 0; i < 100; i++) {
      w.addDocument(createDocument(i, 4));
      if (multiSegment && (i % 10) == 0) {
        w.commit();
      }
    }
    
    if (!multiSegment) {
      w.forceMerge(1);
    }
    
    w.close();

    IndexReader r = IndexReader.open(dir);
    if (multiSegment) {
      assertTrue(r.getSequentialSubReaders().length > 1);
    } else {
      assertTrue(r.getSequentialSubReaders().length == 1);
    }
    r.close();
  }

  public static Document createDocument(int n, int numFields) {
    StringBuilder sb = new StringBuilder();
    Document doc = new Document();
    sb.append("a");
    sb.append(n);
    FieldType customType2 = new FieldType(TextField.TYPE_STORED);
    customType2.setTokenized(false);
    customType2.setOmitNorms(true);
    FieldType customType3 = new FieldType();
    customType3.setStored(true);
    doc.add(new Field("field1", sb.toString(), TextField.TYPE_STORED));
    doc.add(new Field("fielda", sb.toString(), customType2));
    doc.add(new Field("fieldb", sb.toString(), customType3));
    sb.append(" b");
    sb.append(n);
    for (int i = 1; i < numFields; i++) {
      doc.add(new Field("field" + (i+1), sb.toString(), TextField.TYPE_STORED));
    }
    return doc;
  }

  static void modifyIndex(int i, Directory dir) throws IOException {
    switch (i) {
      case 0: {
        if (VERBOSE) {
          System.out.println("TEST: modify index");
        }
        IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)));
        w.deleteDocuments(new Term("field2", "a11"));
        w.deleteDocuments(new Term("field2", "b30"));
        w.close();
        break;
      }
      case 1: {
        IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)));
        w.forceMerge(1);
        w.close();
        break;
      }
      case 2: {
        IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)));
        w.addDocument(createDocument(101, 4));
        w.forceMerge(1);
        w.addDocument(createDocument(102, 4));
        w.addDocument(createDocument(103, 4));
        w.close();
        break;
      }
      case 3: {
        IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)));
        w.addDocument(createDocument(101, 4));
        w.close();
        break;
      }
    }
  }  
  
  static void assertReaderClosed(IndexReader reader, boolean checkSubReaders, boolean checkNormsClosed) {
    assertEquals(0, reader.getRefCount());
    
    if (checkNormsClosed && reader instanceof SegmentReader) {
      // TODO: should we really assert something here? we check for open files and this is obselete...
      // assertTrue(((SegmentReader) reader).normsClosed());
    }
    
    if (checkSubReaders) {
      if (reader instanceof DirectoryReader) {
        IndexReader[] subReaders = reader.getSequentialSubReaders();
        for (int i = 0; i < subReaders.length; i++) {
          assertReaderClosed(subReaders[i], checkSubReaders, checkNormsClosed);
        }
      }
      
      if (reader instanceof MultiReader) {
        IndexReader[] subReaders = reader.getSequentialSubReaders();
        for (int i = 0; i < subReaders.length; i++) {
          assertReaderClosed(subReaders[i], checkSubReaders, checkNormsClosed);
        }
      }
      
      if (reader instanceof ParallelReader) {
        IndexReader[] subReaders = ((ParallelReader) reader).getSubReaders();
        for (int i = 0; i < subReaders.length; i++) {
          assertReaderClosed(subReaders[i], checkSubReaders, checkNormsClosed);
        }
      }
    }
  }

  /*
  private void assertReaderOpen(IndexReader reader) {
    reader.ensureOpen();
    
    if (reader instanceof DirectoryReader) {
      IndexReader[] subReaders = reader.getSequentialSubReaders();
      for (int i = 0; i < subReaders.length; i++) {
        assertReaderOpen(subReaders[i]);
      }
    }
  }
  */

  private void assertRefCountEquals(int refCount, IndexReader reader) {
    assertEquals("Reader has wrong refCount value.", refCount, reader.getRefCount());
  }


  private abstract static class TestReopen {
    protected abstract IndexReader openReader() throws IOException;
    protected abstract void modifyIndex(int i) throws IOException;
  }
  
  private static class KeepAllCommits implements IndexDeletionPolicy {
    public void onInit(List<? extends IndexCommit> commits) {
    }
    public void onCommit(List<? extends IndexCommit> commits) {
    }
  }

  public void testReopenOnCommit() throws Throwable {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(
        dir,
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)).
            setIndexDeletionPolicy(new KeepAllCommits()).
            setMaxBufferedDocs(-1).
            setMergePolicy(newLogMergePolicy(10))
    );
    for(int i=0;i<4;i++) {
      Document doc = new Document();
      doc.add(newField("id", ""+i, StringField.TYPE_UNSTORED));
      writer.addDocument(doc);
      Map<String,String> data = new HashMap<String,String>();
      data.put("index", i+"");
      writer.commit(data);
    }
    for(int i=0;i<4;i++) {
      writer.deleteDocuments(new Term("id", ""+i));
      Map<String,String> data = new HashMap<String,String>();
      data.put("index", (4+i)+"");
      writer.commit(data);
    }
    writer.close();

    IndexReader r = IndexReader.open(dir);
    assertEquals(0, r.numDocs());

    Collection<IndexCommit> commits = IndexReader.listCommits(dir);
    for (final IndexCommit commit : commits) {
      IndexReader r2 = IndexReader.openIfChanged(r, commit);
      assertNotNull(r2);
      assertTrue(r2 != r);

      final Map<String,String> s = commit.getUserData();
      final int v;
      if (s.size() == 0) {
        // First commit created by IW
        v = -1;
      } else {
        v = Integer.parseInt(s.get("index"));
      }
      if (v < 4) {
        assertEquals(1+v, r2.numDocs());
      } else {
        assertEquals(7-v, r2.numDocs());
      }
      r.close();
      r = r2;
    }
    r.close();
    dir.close();
  }
}
