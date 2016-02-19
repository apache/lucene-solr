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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockDirectoryWrapper.FakeIOException;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.ThreadInterruptedException;
import org.junit.Test;

@SuppressCodecs("SimpleText") // too slow here
public class TestIndexWriterReader extends LuceneTestCase {
  
  private final int numThreads = TEST_NIGHTLY ? 5 : 3;
  
  public static int count(Term t, IndexReader r) throws IOException {
    int count = 0;
    PostingsEnum td = TestUtil.docs(random(), r,
        t.field(), new BytesRef(t.text()),
        null,
        0);

    if (td != null) {
      final Bits liveDocs = MultiFields.getLiveDocs(r);
      while (td.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
        td.docID();
        if (liveDocs == null || liveDocs.get(td.docID())) {
          count++;
        }
      }
    }
    return count;
  }
  
  public void testAddCloseOpen() throws IOException {
    // Can't use assertNoDeletes: this test pulls a non-NRT
    // reader in the end:
    Directory dir1 = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    
    IndexWriter writer = new IndexWriter(dir1, iwc);
    for (int i = 0; i < 97 ; i++) {
      DirectoryReader reader = writer.getReader();
      if (i == 0) {
        writer.addDocument(DocHelper.createDocument(i, "x", 1 + random().nextInt(5)));
      } else {
        int previous = random().nextInt(i);
        // a check if the reader is current here could fail since there might be
        // merges going on.
        switch (random().nextInt(5)) {
        case 0:
        case 1:
        case 2:
          writer.addDocument(DocHelper.createDocument(i, "x", 1 + random().nextInt(5)));
          break;
        case 3:
          writer.updateDocument(new Term("id", "" + previous), DocHelper.createDocument(
              previous, "x", 1 + random().nextInt(5)));
          break;
        case 4:
          writer.deleteDocuments(new Term("id", "" + previous));
        }
      }
      assertFalse(reader.isCurrent());
      reader.close();
    }
    writer.forceMerge(1); // make sure all merging is done etc.
    DirectoryReader reader = writer.getReader();
    writer.commit(); // no changes that are not visible to the reader

    // A commit is now seen as a change to an NRT reader:
    assertFalse(reader.isCurrent());
    reader.close();
    reader = writer.getReader();
    assertTrue(reader.isCurrent());
    writer.close();

    assertTrue(reader.isCurrent());
    iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    writer = new IndexWriter(dir1, iwc);
    assertTrue(reader.isCurrent());
    writer.addDocument(DocHelper.createDocument(1, "x", 1+random().nextInt(5)));
    assertTrue(reader.isCurrent()); // segments in ram but IW is different to the readers one
    writer.close();
    assertFalse(reader.isCurrent()); // segments written
    reader.close();
    dir1.close();
  }
  
  public void testUpdateDocument() throws Exception {
    boolean doFullMerge = true;

    Directory dir1 = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    if (iwc.getMaxBufferedDocs() < 20) {
      iwc.setMaxBufferedDocs(20);
    }
    // no merging
    iwc.setMergePolicy(NoMergePolicy.INSTANCE);
    if (VERBOSE) {
      System.out.println("TEST: make index");
    }
    IndexWriter writer = new IndexWriter(dir1, iwc);

    // create the index
    createIndexNoClose(!doFullMerge, "index1", writer);

    // writer.flush(false, true, true);

    // get a reader
    DirectoryReader r1 = writer.getReader();
    assertTrue(r1.isCurrent());

    String id10 = r1.document(10).getField("id").stringValue();
    
    Document newDoc = r1.document(10);
    newDoc.removeField("id");
    newDoc.add(newStringField("id", Integer.toString(8000), Field.Store.YES));
    writer.updateDocument(new Term("id", id10), newDoc);
    assertFalse(r1.isCurrent());

    DirectoryReader r2 = writer.getReader();
    assertTrue(r2.isCurrent());
    assertEquals(0, count(new Term("id", id10), r2));
    if (VERBOSE) {
      System.out.println("TEST: verify id");
    }
    assertEquals(1, count(new Term("id", Integer.toString(8000)), r2));
    
    r1.close();
    assertTrue(r2.isCurrent());
    writer.close();
    // writer.close wrote a new commit
    assertFalse(r2.isCurrent());
    
    DirectoryReader r3 = DirectoryReader.open(dir1);
    assertTrue(r3.isCurrent());
    assertFalse(r2.isCurrent());
    assertEquals(0, count(new Term("id", id10), r3));
    assertEquals(1, count(new Term("id", Integer.toString(8000)), r3));

    writer = new IndexWriter(dir1, newIndexWriterConfig(new MockAnalyzer(random())));
    Document doc = new Document();
    doc.add(newTextField("field", "a b c", Field.Store.NO));
    writer.addDocument(doc);
    assertFalse(r2.isCurrent());
    assertTrue(r3.isCurrent());

    writer.close();

    assertFalse(r2.isCurrent());
    assertTrue(!r3.isCurrent());

    r2.close();
    r3.close();
    
    dir1.close();
  }
  
  public void testIsCurrent() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    
    IndexWriter writer = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(newTextField("field", "a b c", Field.Store.NO));
    writer.addDocument(doc);
    writer.close();
    
    iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    writer = new IndexWriter(dir, iwc);
    doc = new Document();
    doc.add(newTextField("field", "a b c", Field.Store.NO));
    DirectoryReader nrtReader = writer.getReader();
    assertTrue(nrtReader.isCurrent());
    writer.addDocument(doc);
    assertFalse(nrtReader.isCurrent()); // should see the changes
    writer.forceMerge(1); // make sure we don't have a merge going on
    assertFalse(nrtReader.isCurrent());
    nrtReader.close();
    
    DirectoryReader dirReader = DirectoryReader.open(dir);
    nrtReader = writer.getReader();
    
    assertTrue(dirReader.isCurrent());
    assertTrue(nrtReader.isCurrent()); // nothing was committed yet so we are still current
    assertEquals(2, nrtReader.maxDoc()); // sees the actual document added
    assertEquals(1, dirReader.maxDoc());
    writer.close(); // close is actually a commit both should see the changes
    assertFalse(nrtReader.isCurrent()); 
    assertFalse(dirReader.isCurrent()); // this reader has been opened before the writer was closed / committed
    
    dirReader.close();
    nrtReader.close();
    dir.close();
  }
  
  /**
   * Test using IW.addIndexes
   */
  public void testAddIndexes() throws Exception {
    boolean doFullMerge = false;

    Directory dir1 = getAssertNoDeletesDirectory(newDirectory());
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    if (iwc.getMaxBufferedDocs() < 20) {
      iwc.setMaxBufferedDocs(20);
    }
    // no merging
    iwc.setMergePolicy(NoMergePolicy.INSTANCE);
    IndexWriter writer = new IndexWriter(dir1, iwc);

    // create the index
    createIndexNoClose(!doFullMerge, "index1", writer);
    writer.flush(false, true);

    // create a 2nd index
    Directory dir2 = newDirectory();
    IndexWriter writer2 = new IndexWriter(dir2, newIndexWriterConfig(new MockAnalyzer(random())));
    createIndexNoClose(!doFullMerge, "index2", writer2);
    writer2.close();

    DirectoryReader r0 = writer.getReader();
    assertTrue(r0.isCurrent());
    writer.addIndexes(dir2);
    assertFalse(r0.isCurrent());
    r0.close();

    DirectoryReader r1 = writer.getReader();
    assertTrue(r1.isCurrent());

    writer.commit();

    // A commit is seen as a change to NRT reader:
    assertFalse(r1.isCurrent());

    assertEquals(200, r1.maxDoc());

    int index2df = r1.docFreq(new Term("indexname", "index2"));

    assertEquals(100, index2df);

    // verify the docs are from different indexes
    Document doc5 = r1.document(5);
    assertEquals("index1", doc5.get("indexname"));
    Document doc150 = r1.document(150);
    assertEquals("index2", doc150.get("indexname"));
    r1.close();
    writer.close();
    dir1.close();
    dir2.close();
  }
  
  public void testAddIndexes2() throws Exception {
    boolean doFullMerge = false;

    Directory dir1 = getAssertNoDeletesDirectory(newDirectory());
    IndexWriter writer = new IndexWriter(dir1, newIndexWriterConfig(new MockAnalyzer(random())));

    // create a 2nd index
    Directory dir2 = newDirectory();
    IndexWriter writer2 = new IndexWriter(dir2, newIndexWriterConfig(new MockAnalyzer(random())));
    createIndexNoClose(!doFullMerge, "index2", writer2);
    writer2.close();

    writer.addIndexes(dir2);
    writer.addIndexes(dir2);
    writer.addIndexes(dir2);
    writer.addIndexes(dir2);
    writer.addIndexes(dir2);

    IndexReader r1 = writer.getReader();
    assertEquals(500, r1.maxDoc());
    
    r1.close();
    writer.close();
    dir1.close();
    dir2.close();
  }

  /**
   * Deletes using IW.deleteDocuments
   */
  public void testDeleteFromIndexWriter() throws Exception {
    boolean doFullMerge = true;

    Directory dir1 = getAssertNoDeletesDirectory(newDirectory());
    IndexWriter writer = new IndexWriter(dir1, newIndexWriterConfig(new MockAnalyzer(random())));
    // create the index
    createIndexNoClose(!doFullMerge, "index1", writer);
    writer.flush(false, true);
    // get a reader
    IndexReader r1 = writer.getReader();

    String id10 = r1.document(10).getField("id").stringValue();

    // deleted IW docs should not show up in the next getReader
    writer.deleteDocuments(new Term("id", id10));
    IndexReader r2 = writer.getReader();
    assertEquals(1, count(new Term("id", id10), r1));
    assertEquals(0, count(new Term("id", id10), r2));
    
    String id50 = r1.document(50).getField("id").stringValue();
    assertEquals(1, count(new Term("id", id50), r1));
    
    writer.deleteDocuments(new Term("id", id50));
    
    IndexReader r3 = writer.getReader();
    assertEquals(0, count(new Term("id", id10), r3));
    assertEquals(0, count(new Term("id", id50), r3));
    
    String id75 = r1.document(75).getField("id").stringValue();
    writer.deleteDocuments(new TermQuery(new Term("id", id75)));
    IndexReader r4 = writer.getReader();
    assertEquals(1, count(new Term("id", id75), r3));
    assertEquals(0, count(new Term("id", id75), r4));
    
    r1.close();
    r2.close();
    r3.close();
    r4.close();
    writer.close();
        
    // reopen the writer to verify the delete made it to the directory
    writer = new IndexWriter(dir1, newIndexWriterConfig(new MockAnalyzer(random())));
    IndexReader w2r1 = writer.getReader();
    assertEquals(0, count(new Term("id", id10), w2r1));
    w2r1.close();
    writer.close();
    dir1.close();
  }

  @Slow
  public void testAddIndexesAndDoDeletesThreads() throws Throwable {
    final int numIter = 2;
    int numDirs = 3;
    
    Directory mainDir = getAssertNoDeletesDirectory(newDirectory());

    IndexWriter mainWriter = new IndexWriter(mainDir, newIndexWriterConfig(new MockAnalyzer(random()))
                                                        .setMergePolicy(newLogMergePolicy()));
    TestUtil.reduceOpenFiles(mainWriter);

    AddDirectoriesThreads addDirThreads = new AddDirectoriesThreads(numIter, mainWriter);
    addDirThreads.launchThreads(numDirs);
    addDirThreads.joinThreads();
    
    //assertEquals(100 + numDirs * (3 * numIter / 4) * addDirThreads.numThreads
    //    * addDirThreads.NUM_INIT_DOCS, addDirThreads.mainWriter.numDocs());
    assertEquals(addDirThreads.count.intValue(), addDirThreads.mainWriter.numDocs());

    addDirThreads.close(true);
    
    assertTrue(addDirThreads.failures.size() == 0);

    TestUtil.checkIndex(mainDir);

    IndexReader reader = DirectoryReader.open(mainDir);
    assertEquals(addDirThreads.count.intValue(), reader.numDocs());
    //assertEquals(100 + numDirs * (3 * numIter / 4) * addDirThreads.numThreads
    //    * addDirThreads.NUM_INIT_DOCS, reader.numDocs());
    reader.close();

    addDirThreads.closeDir();
    mainDir.close();
  }
  
  private class AddDirectoriesThreads {
    Directory addDir;
    final static int NUM_INIT_DOCS = 100;
    int numDirs;
    final Thread[] threads = new Thread[numThreads];
    IndexWriter mainWriter;
    final List<Throwable> failures = new ArrayList<>();
    DirectoryReader[] readers;
    boolean didClose = false;
    AtomicInteger count = new AtomicInteger(0);
    AtomicInteger numaddIndexes = new AtomicInteger(0);
    
    public AddDirectoriesThreads(int numDirs, IndexWriter mainWriter) throws Throwable {
      this.numDirs = numDirs;
      this.mainWriter = mainWriter;
      addDir = newDirectory();
      IndexWriter writer = new IndexWriter(addDir, newIndexWriterConfig(new MockAnalyzer(random()))
                                                     .setMaxBufferedDocs(2));
      TestUtil.reduceOpenFiles(writer);
      for (int i = 0; i < NUM_INIT_DOCS; i++) {
        Document doc = DocHelper.createDocument(i, "addindex", 4);
        writer.addDocument(doc);
      }
        
      writer.close();
      
      readers = new DirectoryReader[numDirs];
      for (int i = 0; i < numDirs; i++)
        readers[i] = DirectoryReader.open(addDir);
    }
    
    void joinThreads() {
      for (int i = 0; i < numThreads; i++)
        try {
          threads[i].join();
        } catch (InterruptedException ie) {
          throw new ThreadInterruptedException(ie);
        }
    }

    void close(boolean doWait) throws Throwable {
      didClose = true;
      if (doWait) {
        mainWriter.close();
      } else {
        mainWriter.rollback();
      }
    }

    void closeDir() throws Throwable {
      for (int i = 0; i < numDirs; i++) {
        readers[i].close();
      }
      addDir.close();
    }
    
    void handle(Throwable t) {
      t.printStackTrace(System.out);
      synchronized (failures) {
        failures.add(t);
      }
    }
    
    void launchThreads(final int numIter) {
      for (int i = 0; i < numThreads; i++) {
        threads[i] = new Thread() {
          @Override
          public void run() {
            try {
              final Directory[] dirs = new Directory[numDirs];
              for (int k = 0; k < numDirs; k++)
                dirs[k] = new MockDirectoryWrapper(random(), TestUtil.ramCopyOf(addDir));
              //int j = 0;
              //while (true) {
                // System.out.println(Thread.currentThread().getName() + ": iter
                // j=" + j);
                for (int x=0; x < numIter; x++) {
                  // only do addIndexes
                  doBody(x, dirs);
                }
                //if (numIter > 0 && j == numIter)
                //  break;
                //doBody(j++, dirs);
                //doBody(5, dirs);
              //}
            } catch (Throwable t) {
              handle(t);
            }
          }
        };
      }
      for (int i = 0; i < numThreads; i++)
        threads[i].start();
    }
    
    void doBody(int j, Directory[] dirs) throws Throwable {
      switch (j % 4) {
        case 0:
          mainWriter.addIndexes(dirs);
          mainWriter.forceMerge(1);
          break;
        case 1:
          mainWriter.addIndexes(dirs);
          numaddIndexes.incrementAndGet();
          break;
        case 2:
          TestUtil.addIndexesSlowly(mainWriter, readers);
          break;
        case 3:
          mainWriter.commit();
      }
      count.addAndGet(dirs.length*NUM_INIT_DOCS);
    }
  }

  public void testIndexWriterReopenSegmentFullMerge() throws Exception {
    doTestIndexWriterReopenSegment(true);
  }

  public void testIndexWriterReopenSegment() throws Exception {
    doTestIndexWriterReopenSegment(false);
  }

  /**
   * Tests creating a segment, then check to insure the segment can be seen via
   * IW.getReader
   */
  public void doTestIndexWriterReopenSegment(boolean doFullMerge) throws Exception {
    Directory dir1 = getAssertNoDeletesDirectory(newDirectory());
    IndexWriter writer = new IndexWriter(dir1, newIndexWriterConfig(new MockAnalyzer(random())));
    IndexReader r1 = writer.getReader();
    assertEquals(0, r1.maxDoc());
    createIndexNoClose(false, "index1", writer);
    writer.flush(!doFullMerge, true);

    IndexReader iwr1 = writer.getReader();
    assertEquals(100, iwr1.maxDoc());

    IndexReader r2 = writer.getReader();
    assertEquals(r2.maxDoc(), 100);
    // add 100 documents
    for (int x = 10000; x < 10000 + 100; x++) {
      Document d = DocHelper.createDocument(x, "index1", 5);
      writer.addDocument(d);
    }
    writer.flush(false, true);
    // verify the reader was reopened internally
    IndexReader iwr2 = writer.getReader();
    assertTrue(iwr2 != r1);
    assertEquals(200, iwr2.maxDoc());
    // should have flushed out a segment
    IndexReader r3 = writer.getReader();
    assertTrue(r2 != r3);
    assertEquals(200, r3.maxDoc());

    // dec ref the readers rather than close them because
    // closing flushes changes to the writer
    r1.close();
    iwr1.close();
    r2.close();
    r3.close();
    iwr2.close();
    writer.close();

    // test whether the changes made it to the directory
    writer = new IndexWriter(dir1, newIndexWriterConfig(new MockAnalyzer(random())));
    IndexReader w2r1 = writer.getReader();
    // insure the deletes were actually flushed to the directory
    assertEquals(200, w2r1.maxDoc());
    w2r1.close();
    writer.close();

    dir1.close();
  }
 
  /*
   * Delete a document by term and return the doc id
   * 
   * public static int deleteDocument(Term term, IndexWriter writer) throws
   * IOException { IndexReader reader = writer.getReader(); TermDocs td =
   * reader.termDocs(term); int doc = -1; //if (td.next()) { // doc = td.doc();
   * //} //writer.deleteDocuments(term); td.close(); return doc; }
   */
  
  public static void createIndex(Random random, Directory dir1, String indexName,
      boolean multiSegment) throws IOException {
    IndexWriter w = new IndexWriter(dir1, LuceneTestCase.newIndexWriterConfig(random, new MockAnalyzer(random))
        .setMergePolicy(new LogDocMergePolicy()));
    for (int i = 0; i < 100; i++) {
      w.addDocument(DocHelper.createDocument(i, indexName, 4));
    }
    if (!multiSegment) {
      w.forceMerge(1);
    }
    w.close();
  }

  public static void createIndexNoClose(boolean multiSegment, String indexName,
      IndexWriter w) throws IOException {
    for (int i = 0; i < 100; i++) {
      w.addDocument(DocHelper.createDocument(i, indexName, 4));
    }
    if (!multiSegment) {
      w.forceMerge(1);
    }
  }

  private static class MyWarmer extends IndexWriter.IndexReaderWarmer {
    int warmCount;
    @Override
    public void warm(LeafReader reader) throws IOException {
      warmCount++;
    }
  }

  public void testMergeWarmer() throws Exception {

    Directory dir1 = getAssertNoDeletesDirectory(newDirectory());
    // Enroll warmer
    MyWarmer warmer = new MyWarmer();
    IndexWriter writer = new IndexWriter(
        dir1,
        newIndexWriterConfig(new MockAnalyzer(random()))
          .setMaxBufferedDocs(2)
          .setMergedSegmentWarmer(warmer)
          .setMergeScheduler(new ConcurrentMergeScheduler())
          .setMergePolicy(newLogMergePolicy())
    );

    // create the index
    createIndexNoClose(false, "test", writer);

    // get a reader to put writer into near real-time mode
    IndexReader r1 = writer.getReader();
    
    ((LogMergePolicy) writer.getConfig().getMergePolicy()).setMergeFactor(2);

    int num = TEST_NIGHTLY ? atLeast(100) : atLeast(10);
    for (int i = 0; i < num; i++) {
      writer.addDocument(DocHelper.createDocument(i, "test", 4));
    }
    ((ConcurrentMergeScheduler) writer.getConfig().getMergeScheduler()).sync();

    assertTrue(warmer.warmCount > 0);
    final int count = warmer.warmCount;

    writer.addDocument(DocHelper.createDocument(17, "test", 4));
    writer.forceMerge(1);
    assertTrue(warmer.warmCount > count);
    
    writer.close();
    r1.close();
    dir1.close();
  }

  public void testAfterCommit() throws Exception {
    Directory dir1 = getAssertNoDeletesDirectory(newDirectory());
    IndexWriter writer = new IndexWriter(dir1, newIndexWriterConfig(new MockAnalyzer(random()))
                                                 .setMergeScheduler(new ConcurrentMergeScheduler()));
    writer.commit();

    // create the index
    createIndexNoClose(false, "test", writer);

    // get a reader to put writer into near real-time mode
    DirectoryReader r1 = writer.getReader();
    TestUtil.checkIndex(dir1);
    writer.commit();
    TestUtil.checkIndex(dir1);
    assertEquals(100, r1.numDocs());

    for (int i = 0; i < 10; i++) {
      writer.addDocument(DocHelper.createDocument(i, "test", 4));
    }
    ((ConcurrentMergeScheduler) writer.getConfig().getMergeScheduler()).sync();

    DirectoryReader r2 = DirectoryReader.openIfChanged(r1);
    if (r2 != null) {
      r1.close();
      r1 = r2;
    }
    assertEquals(110, r1.numDocs());
    writer.close();
    r1.close();
    dir1.close();
  }

  // Make sure reader remains usable even if IndexWriter closes
  public void testAfterClose() throws Exception {
    Directory dir1 = getAssertNoDeletesDirectory(newDirectory());
    IndexWriter writer = new IndexWriter(dir1, newIndexWriterConfig(new MockAnalyzer(random())));

    // create the index
    createIndexNoClose(false, "test", writer);

    DirectoryReader r = writer.getReader();
    writer.close();

    TestUtil.checkIndex(dir1);

    // reader should remain usable even after IndexWriter is closed:
    assertEquals(100, r.numDocs());
    Query q = new TermQuery(new Term("indexname", "test"));
    IndexSearcher searcher = newSearcher(r);
    assertEquals(100, searcher.search(q, 10).totalHits);

    expectThrows(AlreadyClosedException.class, () -> {
      DirectoryReader.openIfChanged(r);
    });

    r.close();
    dir1.close();
  }

  // Stress test reopen during addIndexes
  @Nightly
  public void testDuringAddIndexes() throws Exception {
    Directory dir1 = getAssertNoDeletesDirectory(newDirectory());
    final IndexWriter writer = new IndexWriter(
        dir1,
        newIndexWriterConfig(new MockAnalyzer(random()))
          .setMergePolicy(newLogMergePolicy(2))
    );

    // create the index
    createIndexNoClose(false, "test", writer);
    writer.commit();

    final Directory[] dirs = new Directory[10];
    for (int i=0;i<10;i++) {
      dirs[i] = new MockDirectoryWrapper(random(), TestUtil.ramCopyOf(dir1));
    }

    DirectoryReader r = writer.getReader();

    final int numIterations = 10;
    final List<Throwable> excs = Collections.synchronizedList(new ArrayList<Throwable>());

    // Only one thread can addIndexes at a time, because
    // IndexWriter acquires a write lock in each directory:
    final Thread[] threads = new Thread[1];
    final AtomicBoolean threadDone = new AtomicBoolean(false);
    for(int i=0;i<threads.length;i++) {
      threads[i] = new Thread() {
          @Override
          public void run() {
            int count = 0;
            do {
              count++;
              try {
                writer.addIndexes(dirs);
                writer.maybeMerge();
              } catch (Throwable t) {
                excs.add(t);
                throw new RuntimeException(t);
              }
            } while(count < numIterations);
            threadDone.set(true);
          }
        };
      threads[i].setDaemon(true);
      threads[i].start();
    }

    int lastCount = 0;
    while(threadDone.get() == false) {
      DirectoryReader r2 = DirectoryReader.openIfChanged(r);
      if (r2 != null) {
        r.close();
        r = r2;
        Query q = new TermQuery(new Term("indexname", "test"));
        IndexSearcher searcher = newSearcher(r);
        final int count = searcher.search(q, 10).totalHits;
        assertTrue(count >= lastCount);
        lastCount = count;
      }
    }

    for(int i=0;i<threads.length;i++) {
      threads[i].join();
    }
    // final check
    DirectoryReader r2 = DirectoryReader.openIfChanged(r);
    if (r2 != null) {
      r.close();
      r = r2;
    }
    Query q = new TermQuery(new Term("indexname", "test"));
    IndexSearcher searcher = newSearcher(r);
    final int count = searcher.search(q, 10).totalHits;
    assertTrue(count >= lastCount);

    assertEquals(0, excs.size());
    r.close();
    if (dir1 instanceof MockDirectoryWrapper) {
      final Collection<String> openDeletedFiles = ((MockDirectoryWrapper)dir1).getOpenDeletedFiles();
      assertEquals("openDeleted=" + openDeletedFiles, 0, openDeletedFiles.size());
    }

    writer.close();

    dir1.close();
  }

  private Directory getAssertNoDeletesDirectory(Directory directory) {
    if (directory instanceof MockDirectoryWrapper) {
      ((MockDirectoryWrapper)directory).setAssertNoDeleteOpenFile(true);
    }
    return directory;
  }

  // Stress test reopen during add/delete
  public void testDuringAddDelete() throws Exception {
    Directory dir1 = newDirectory();
    final IndexWriter writer = new IndexWriter(
        dir1,
        newIndexWriterConfig(new MockAnalyzer(random()))
          .setMergePolicy(newLogMergePolicy(2))
    );

    // create the index
    createIndexNoClose(false, "test", writer);
    writer.commit();

    DirectoryReader r = writer.getReader();

    final int iters = TEST_NIGHTLY ? 1000 : 10;
    final List<Throwable> excs = Collections.synchronizedList(new ArrayList<Throwable>());

    final Thread[] threads = new Thread[numThreads];
    final AtomicInteger remainingThreads = new AtomicInteger(numThreads);
    for(int i=0;i<numThreads;i++) {
      threads[i] = new Thread() {
          final Random r = new Random(random().nextLong());

          @Override
          public void run() {
            int count = 0;
            do {
              try {
                for(int docUpto=0;docUpto<10;docUpto++) {
                  writer.addDocument(DocHelper.createDocument(10*count+docUpto, "test", 4));
                }
                count++;
                final int limit = count*10;
                for(int delUpto=0;delUpto<5;delUpto++) {
                  int x = r.nextInt(limit);
                  writer.deleteDocuments(new Term("field3", "b"+x));
                }
              } catch (Throwable t) {
                excs.add(t);
                throw new RuntimeException(t);
              }
            } while(count < iters);
            remainingThreads.decrementAndGet();
          }
        };
      threads[i].setDaemon(true);
      threads[i].start();
    }

    int sum = 0;
    while(remainingThreads.get() > 0) {
      DirectoryReader r2 = DirectoryReader.openIfChanged(r);
      if (r2 != null) {
        r.close();
        r = r2;
        Query q = new TermQuery(new Term("indexname", "test"));
        IndexSearcher searcher = newSearcher(r);
        sum += searcher.search(q, 10).totalHits;
      }
    }

    for(int i=0;i<numThreads;i++) {
      threads[i].join();
    }
    // at least search once
    DirectoryReader r2 = DirectoryReader.openIfChanged(r);
    if (r2 != null) {
      r.close();
      r = r2;
    }
    Query q = new TermQuery(new Term("indexname", "test"));
    IndexSearcher searcher = newSearcher(r);
    sum += searcher.search(q, 10).totalHits;
    assertTrue("no documents found at all", sum > 0);

    assertEquals(0, excs.size());
    writer.close();

    r.close();
    dir1.close();
  }

  public void testForceMergeDeletes() throws Throwable {
    Directory dir = newDirectory();
    final IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
                                                 .setMergePolicy(newLogMergePolicy()));
    Document doc = new Document();
    doc.add(newTextField("field", "a b c", Field.Store.NO));
    Field id = newStringField("id", "", Field.Store.NO);
    doc.add(id);
    id.setStringValue("0");
    w.addDocument(doc);
    id.setStringValue("1");
    w.addDocument(doc);
    w.deleteDocuments(new Term("id", "0"));

    IndexReader r = w.getReader();
    w.forceMergeDeletes();
    w.close();
    r.close();
    r = DirectoryReader.open(dir);
    assertEquals(1, r.numDocs());
    assertFalse(r.hasDeletions());
    r.close();
    dir.close();
  }

  public void testDeletesNumDocs() throws Throwable {
    Directory dir = newDirectory();
    final IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    Document doc = new Document();
    doc.add(newTextField("field", "a b c", Field.Store.NO));
    Field id = newStringField("id", "", Field.Store.NO);
    doc.add(id);
    id.setStringValue("0");
    w.addDocument(doc);
    id.setStringValue("1");
    w.addDocument(doc);
    IndexReader r = w.getReader();
    assertEquals(2, r.numDocs());
    r.close();

    w.deleteDocuments(new Term("id", "0"));
    r = w.getReader();
    assertEquals(1, r.numDocs());
    r.close();

    w.deleteDocuments(new Term("id", "1"));
    r = w.getReader();
    assertEquals(0, r.numDocs());
    r.close();

    w.close();
    dir.close();
  }
  
  public void testEmptyIndex() throws Exception {
    // Ensures that getReader works on an empty index, which hasn't been committed yet.
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    IndexReader r = w.getReader();
    assertEquals(0, r.numDocs());
    r.close();
    w.close();
    dir.close();
  }

  public void testSegmentWarmer() throws Exception {
    Directory dir = newDirectory();
    final AtomicBoolean didWarm = new AtomicBoolean();
    IndexWriter w = new IndexWriter(
        dir,
        newIndexWriterConfig(new MockAnalyzer(random())).
            setMaxBufferedDocs(2).
            setReaderPooling(true).
            setMergedSegmentWarmer(new IndexWriter.IndexReaderWarmer() {
              @Override
              public void warm(LeafReader r) throws IOException {
                IndexSearcher s = newSearcher(r);
                TopDocs hits = s.search(new TermQuery(new Term("foo", "bar")), 10);
                assertEquals(20, hits.totalHits);
                didWarm.set(true);
              }
            }).
            setMergePolicy(newLogMergePolicy(10))
    );

    Document doc = new Document();
    doc.add(newStringField("foo", "bar", Field.Store.NO));
    for(int i=0;i<20;i++) {
      w.addDocument(doc);
    }
    w.waitForMerges();
    w.close();
    dir.close();
    assertTrue(didWarm.get());
  }
  
  public void testSimpleMergedSegmentWarmer() throws Exception {
    Directory dir = newDirectory();
    final AtomicBoolean didWarm = new AtomicBoolean();
    InfoStream infoStream = new InfoStream() {
      @Override
      public void close() throws IOException {}

      @Override
      public void message(String component, String message) {
        if ("SMSW".equals(component)) {
          didWarm.set(true);
        }
      }

      @Override
      public boolean isEnabled(String component) {
        return true;
      }
    };
    IndexWriter w = new IndexWriter(
        dir,
        newIndexWriterConfig(new MockAnalyzer(random()))
           .setMaxBufferedDocs(2)
           .setReaderPooling(true)
           .setInfoStream(infoStream)
           .setMergedSegmentWarmer(new SimpleMergedSegmentWarmer(infoStream))
           .setMergePolicy(newLogMergePolicy(10))
    );

    Document doc = new Document();
    doc.add(newStringField("foo", "bar", Field.Store.NO));
    for(int i=0;i<20;i++) {
      w.addDocument(doc);
    }
    w.waitForMerges();
    w.close();
    dir.close();
    assertTrue(didWarm.get());
  }
  
  public void testReopenAfterNoRealChange() throws Exception {
    Directory d = getAssertNoDeletesDirectory(newDirectory());
    IndexWriter w = new IndexWriter(
        d,
        newIndexWriterConfig(new MockAnalyzer(random())));

    DirectoryReader r = w.getReader(); // start pooling readers

    DirectoryReader r2 = DirectoryReader.openIfChanged(r);
    assertNull(r2);
    
    w.addDocument(new Document());
    DirectoryReader r3 = DirectoryReader.openIfChanged(r);
    assertNotNull(r3);
    assertTrue(r3.getVersion() != r.getVersion());
    assertTrue(r3.isCurrent());

    // Deletes nothing in reality...:
    w.deleteDocuments(new Term("foo", "bar"));

    // ... but IW marks this as not current:
    assertFalse(r3.isCurrent());
    DirectoryReader r4 = DirectoryReader.openIfChanged(r3);
    assertNull(r4);

    // Deletes nothing in reality...:
    w.deleteDocuments(new Term("foo", "bar"));
    DirectoryReader r5 = DirectoryReader.openIfChanged(r3, w);
    assertNull(r5);

    r3.close();

    w.close();
    d.close();
  }
  
  @Test
  public void testNRTOpenExceptions() throws Exception {
    // LUCENE-5262: test that several failed attempts to obtain an NRT reader
    // don't leak file handles.
    MockDirectoryWrapper dir = (MockDirectoryWrapper) getAssertNoDeletesDirectory(newMockDirectory());
    final AtomicBoolean shouldFail = new AtomicBoolean();
    dir.failOn(new MockDirectoryWrapper.Failure() {
      @Override
      public void eval(MockDirectoryWrapper dir) throws IOException {
        StackTraceElement[] trace = new Exception().getStackTrace();
        if (shouldFail.get()) {
          for (int i = 0; i < trace.length; i++) {
            if ("getReadOnlyClone".equals(trace[i].getMethodName())) {
              if (VERBOSE) {
                System.out.println("TEST: now fail; exc:");
                new Throwable().printStackTrace(System.out);
              }
              shouldFail.set(false);
              throw new FakeIOException();
            }
          }
        }
      }
    });
    
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    conf.setMergePolicy(NoMergePolicy.INSTANCE); // prevent merges from getting in the way
    IndexWriter writer = new IndexWriter(dir, conf);
    
    // create a segment and open an NRT reader
    writer.addDocument(new Document());
    writer.getReader().close();
    
    // add a new document so a new NRT reader is required
    writer.addDocument(new Document());

    // try to obtain an NRT reader twice: first time it fails and closes all the
    // other NRT readers. second time it fails, but also fails to close the
    // other NRT reader, since it is already marked closed!
    for (int i = 0; i < 2; i++) {
      shouldFail.set(true);
      expectThrows(FakeIOException.class, () -> {
        writer.getReader().close();
      });
    }
    
    writer.close();
    dir.close();
  }

  /** Make sure if all we do is open NRT reader against
   *  writer, we don't see merge starvation. */
  public void testTooManySegments() throws Exception {
    Directory dir = getAssertNoDeletesDirectory(new RAMDirectory());
    // Don't use newIndexWriterConfig, because we need a
    // "sane" mergePolicy:
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    // Create 500 segments:
    for(int i=0;i<500;i++) {
      Document doc = new Document();
      doc.add(newStringField("id", ""+i, Field.Store.NO));
      w.addDocument(doc);
      IndexReader r = DirectoryReader.open(w);
      // Make sure segment count never exceeds 100:
      assertTrue(r.leaves().size() < 100);
      r.close();
    }
    w.close();
    dir.close();
  }

  // LUCENE-5912: make sure when you reopen an NRT reader using a commit point, the SegmentReaders are in fact shared:
  public void testReopenNRTReaderOnCommit() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    w.addDocument(new Document());

    // Pull NRT reader; it has 1 segment:
    DirectoryReader r1 = DirectoryReader.open(w);
    assertEquals(1, r1.leaves().size());
    w.addDocument(new Document());
    w.commit();

    List<IndexCommit> commits = DirectoryReader.listCommits(dir);
    assertEquals(1, commits.size());
    DirectoryReader r2 = DirectoryReader.openIfChanged(r1, commits.get(0));
    assertEquals(2, r2.leaves().size());

    // Make sure we shared same instance of SegmentReader w/ first reader:
    assertTrue(r1.leaves().get(0).reader() == r2.leaves().get(0).reader());
    r1.close();
    r2.close();
    w.close();
    dir.close();
  }
}
