package org.apache.lucene.index;

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
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

public class TestDirectoryReaderReopen extends LuceneTestCase {
  
  public void testReopen() throws Exception {
    final Directory dir1 = newDirectory();
    
    createIndex(random(), dir1, false);
    performDefaultTests(new TestReopen() {

      @Override
      protected void modifyIndex(int i) throws IOException {
        TestDirectoryReaderReopen.modifyIndex(i, dir1);
      }

      @Override
      protected DirectoryReader openReader() throws IOException {
        return DirectoryReader.open(dir1);
      }
      
    });
    dir1.close();
    
    final Directory dir2 = newDirectory();
    
    createIndex(random(), dir2, true);
    performDefaultTests(new TestReopen() {

      @Override
      protected void modifyIndex(int i) throws IOException {
        TestDirectoryReaderReopen.modifyIndex(i, dir2);
      }

      @Override
      protected DirectoryReader openReader() throws IOException {
        return DirectoryReader.open(dir2);
      }
      
    });
    dir2.close();
  }
  
  // LUCENE-1228: IndexWriter.commit() does not update the index version
  // populate an index in iterations.
  // at the end of every iteration, commit the index and reopen/recreate the reader.
  // in each iteration verify the work of previous iteration. 
  // try this once with reopen once recreate, on both RAMDir and FSDir.
  public void testCommitReopen () throws IOException {
    Directory dir = newDirectory();
    doTestReopenWithCommit(random(), dir, true);
    dir.close();
  }
  public void testCommitRecreate () throws IOException {
    Directory dir = newDirectory();
    doTestReopenWithCommit(random(), dir, false);
    dir.close();
  }

  private void doTestReopenWithCommit (Random random, Directory dir, boolean withReopen) throws IOException {
    IndexWriter iwriter = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random)).setOpenMode(
                                                              OpenMode.CREATE).setMergeScheduler(new SerialMergeScheduler()).setMergePolicy(newLogMergePolicy()));
    iwriter.commit();
    DirectoryReader reader = DirectoryReader.open(dir);
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
          DirectoryReader r2 = DirectoryReader.openIfChanged(reader);
          if (r2 != null) {
            reader.close();
            reader = r2;
          }
        } else {
          // recreate
          reader.close();
          reader = DirectoryReader.open(dir);
        }
      }
    } finally {
      iwriter.close();
      reader.close();
    }
  }
    
  private void performDefaultTests(TestReopen test) throws Exception {

    DirectoryReader index1 = test.openReader();
    DirectoryReader index2 = test.openReader();
        
    TestDirectoryReader.assertIndexEquals(index1, index2);

    // verify that reopen() does not return a new reader instance
    // in case the index has no changes
    ReaderCouple couple = refreshReader(index2, false);
    assertTrue(couple.refreshedReader == index2);
    
    couple = refreshReader(index2, test, 0, true);
    index1.close();
    index1 = couple.newReader;

    DirectoryReader index2_refreshed = couple.refreshedReader;
    index2.close();
    
    // test if refreshed reader and newly opened reader return equal results
    TestDirectoryReader.assertIndexEquals(index1, index2_refreshed);

    index2_refreshed.close();
    assertReaderClosed(index2, true);
    assertReaderClosed(index2_refreshed, true);

    index2 = test.openReader();
    
    for (int i = 1; i < 4; i++) {
      
      index1.close();
      couple = refreshReader(index2, test, i, true);
      // refresh DirectoryReader
      index2.close();
      
      index2 = couple.refreshedReader;
      index1 = couple.newReader;
      TestDirectoryReader.assertIndexEquals(index1, index2);
    }
    
    index1.close();
    index2.close();
    assertReaderClosed(index1, true);
    assertReaderClosed(index2, true);
  }
  
  public void testThreadSafety() throws Exception {
    final Directory dir = newDirectory();
    // NOTE: this also controls the number of threads!
    final int n = _TestUtil.nextInt(random(), 20, 40);
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    for (int i = 0; i < n; i++) {
      writer.addDocument(createDocument(i, 3));
    }
    writer.forceMerge(1);
    writer.close();

    final TestReopen test = new TestReopen() {      
      @Override
      protected void modifyIndex(int i) throws IOException {
       IndexWriter modifier = new IndexWriter(dir, new IndexWriterConfig(
         TEST_VERSION_CURRENT, new MockAnalyzer(random())));
       modifier.addDocument(createDocument(n + i, 6));
       modifier.close();
      }

      @Override
      protected DirectoryReader openReader() throws IOException {
        return DirectoryReader.open(dir);
      }      
    };
    
    final List<ReaderCouple> readers = Collections.synchronizedList(new ArrayList<ReaderCouple>());
    DirectoryReader firstReader = DirectoryReader.open(dir);
    DirectoryReader reader = firstReader;
    
    ReaderThread[] threads = new ReaderThread[n];
    final Set<DirectoryReader> readersToClose = Collections.synchronizedSet(new HashSet<DirectoryReader>());
    
    for (int i = 0; i < n; i++) {
      if (i % 2 == 0) {
        DirectoryReader refreshed = DirectoryReader.openIfChanged(reader);
        if (refreshed != null) {
          readersToClose.add(reader);
          reader = refreshed;
        }
      }
      final DirectoryReader r = reader;
      
      final int index = i;    
      
      ReaderThreadTask task;
      
      if (i < 4 || (i >=10 && i < 14) || i > 18) {
        task = new ReaderThreadTask() {
          
          @Override
          public void run() throws Exception {
            Random rnd = LuceneTestCase.random();
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
                DirectoryReader refreshed = DirectoryReader.openIfChanged(r);
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
                wait(_TestUtil.nextInt(random(), 1, 100));
              }
            }
          }
          
        };
      } else {
        task = new ReaderThreadTask() {
          @Override
          public void run() throws Exception {
            Random rnd = LuceneTestCase.random();
            while (!stopped) {
              int numReaders = readers.size();
              if (numReaders > 0) {
                ReaderCouple c =  readers.get(rnd.nextInt(numReaders));
                TestDirectoryReader.assertIndexEquals(c.newReader, c.refreshedReader);
              }
              
              synchronized(this) {
                wait(_TestUtil.nextInt(random(), 1, 100));
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
    
    for (final DirectoryReader readerToClose : readersToClose) {
      readerToClose.close();
    }
    
    firstReader.close();
    reader.close();
    
    for (final DirectoryReader readerToClose : readersToClose) {
      assertReaderClosed(readerToClose, true);
    }

    assertReaderClosed(reader, true);
    assertReaderClosed(firstReader, true);

    dir.close();
  }
  
  private static class ReaderCouple {
    ReaderCouple(DirectoryReader r1, DirectoryReader r2) {
      newReader = r1;
      refreshedReader = r2;
    }
    
    DirectoryReader newReader;
    DirectoryReader refreshedReader;
  }
  
  abstract static class ReaderThreadTask {
    protected volatile boolean stopped;
    public void stop() {
      this.stopped = true;
    }
    
    public abstract void run() throws Exception;
  }
  
  private static class ReaderThread extends Thread {
    ReaderThreadTask task;
    Throwable error;
    
    
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
  
  private ReaderCouple refreshReader(DirectoryReader reader, boolean hasChanges) throws IOException {
    return refreshReader(reader, null, -1, hasChanges);
  }
  
  ReaderCouple refreshReader(DirectoryReader reader, TestReopen test, int modify, boolean hasChanges) throws IOException {
    synchronized (createReaderMutex) {
      DirectoryReader r = null;
      if (test != null) {
        test.modifyIndex(modify);
        r = test.openReader();
      }
      
      DirectoryReader refreshed = null;
      try {
        refreshed = DirectoryReader.openIfChanged(reader);
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
          fail("No new DirectoryReader instance created during refresh.");
        }
      } else {
        if (refreshed != reader) {
          fail("New DirectoryReader instance created during refresh even though index had no changes.");
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

    DirectoryReader r = DirectoryReader.open(dir);
    if (multiSegment) {
      assertTrue(r.leaves().size() > 1);
    } else {
      assertTrue(r.leaves().size() == 1);
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
    doc.add(new TextField("field1", sb.toString(), Field.Store.YES));
    doc.add(new Field("fielda", sb.toString(), customType2));
    doc.add(new Field("fieldb", sb.toString(), customType3));
    sb.append(" b");
    sb.append(n);
    for (int i = 1; i < numFields; i++) {
      doc.add(new TextField("field" + (i+1), sb.toString(), Field.Store.YES));
    }
    return doc;
  }

  static void modifyIndex(int i, Directory dir) throws IOException {
    switch (i) {
      case 0: {
        if (VERBOSE) {
          System.out.println("TEST: modify index");
        }
        IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));
        w.deleteDocuments(new Term("field2", "a11"));
        w.deleteDocuments(new Term("field2", "b30"));
        w.close();
        break;
      }
      case 1: {
        IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));
        w.forceMerge(1);
        w.close();
        break;
      }
      case 2: {
        IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));
        w.addDocument(createDocument(101, 4));
        w.forceMerge(1);
        w.addDocument(createDocument(102, 4));
        w.addDocument(createDocument(103, 4));
        w.close();
        break;
      }
      case 3: {
        IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));
        w.addDocument(createDocument(101, 4));
        w.close();
        break;
      }
    }
  }  
  
  static void assertReaderClosed(IndexReader reader, boolean checkSubReaders) {
    assertEquals(0, reader.getRefCount());
    
    if (checkSubReaders && reader instanceof CompositeReader) {
      // we cannot use reader context here, as reader is
      // already closed and calling getTopReaderContext() throws AlreadyClosed!
      List<? extends IndexReader> subReaders = ((CompositeReader) reader).getSequentialSubReaders();
      for (final IndexReader r : subReaders) {
        assertReaderClosed(r, checkSubReaders);
      }
    }
  }

  abstract static class TestReopen {
    protected abstract DirectoryReader openReader() throws IOException;
    protected abstract void modifyIndex(int i) throws IOException;
  }
  
  static class KeepAllCommits extends IndexDeletionPolicy {
    @Override
    public void onInit(List<? extends IndexCommit> commits) {
    }
    @Override
    public void onCommit(List<? extends IndexCommit> commits) {
    }
  }

  public void testReopenOnCommit() throws Throwable {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(
        dir,
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).
            setIndexDeletionPolicy(new KeepAllCommits()).
            setMaxBufferedDocs(-1).
            setMergePolicy(newLogMergePolicy(10))
    );
    for(int i=0;i<4;i++) {
      Document doc = new Document();
      doc.add(newStringField("id", ""+i, Field.Store.NO));
      writer.addDocument(doc);
      Map<String,String> data = new HashMap<String,String>();
      data.put("index", i+"");
      writer.setCommitData(data);
      writer.commit();
    }
    for(int i=0;i<4;i++) {
      writer.deleteDocuments(new Term("id", ""+i));
      Map<String,String> data = new HashMap<String,String>();
      data.put("index", (4+i)+"");
      writer.setCommitData(data);
      writer.commit();
    }
    writer.close();

    DirectoryReader r = DirectoryReader.open(dir);
    assertEquals(0, r.numDocs());

    Collection<IndexCommit> commits = DirectoryReader.listCommits(dir);
    for (final IndexCommit commit : commits) {
      DirectoryReader r2 = DirectoryReader.openIfChanged(r, commit);
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

  public void testOpenIfChangedNRTToCommit() throws Exception {
    Directory dir = newDirectory();

    // Can't use RIW because it randomly commits:
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    Document doc = new Document();
    doc.add(newStringField("field", "value", Field.Store.NO));
    w.addDocument(doc);
    w.commit();
    List<IndexCommit> commits = DirectoryReader.listCommits(dir);
    assertEquals(1, commits.size());
    w.addDocument(doc);
    DirectoryReader r = DirectoryReader.open(w, true);

    assertEquals(2, r.numDocs());
    IndexReader r2 = DirectoryReader.openIfChanged(r, commits.get(0));
    assertNotNull(r2);
    r.close();
    assertEquals(1, r2.numDocs());
    w.close();
    r2.close();
    dir.close();
  }
}
