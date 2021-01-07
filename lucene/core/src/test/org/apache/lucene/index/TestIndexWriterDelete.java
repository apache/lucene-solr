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


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.junit.Ignore;

@SuppressCodecs("SimpleText") // too slow here
public class TestIndexWriterDelete extends LuceneTestCase {

  // test the simple case
  public void testSimpleCase() throws IOException {
    String[] keywords = { "1", "2" };
    String[] unindexed = { "Netherlands", "Italy" };
    String[] unstored = { "Amsterdam has lots of bridges",
        "Venice has lots of canals" };
    String[] text = { "Amsterdam", "Venice" };

    Directory dir = newDirectory();
    IndexWriter modifier = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false)));

    FieldType custom1 = new FieldType();
    custom1.setStored(true);
    for (int i = 0; i < keywords.length; i++) {
      Document doc = new Document();
      doc.add(newStringField("id", keywords[i], Field.Store.YES));
      doc.add(newField("country", unindexed[i], custom1));
      doc.add(newTextField("contents", unstored[i], Field.Store.NO));
      doc.add(newTextField("city", text[i], Field.Store.YES));
      modifier.addDocument(doc);
    }
    modifier.forceMerge(1);
    modifier.commit();

    Term term = new Term("city", "Amsterdam");
    long hitCount = getHitCount(dir, term);
    assertEquals(1, hitCount);
    if (VERBOSE) {
      System.out.println("\nTEST: now delete by term=" + term);
    }
    modifier.deleteDocuments(term);
    modifier.commit();

    if (VERBOSE) {
      System.out.println("\nTEST: now getHitCount");
    }
    hitCount = getHitCount(dir, term);
    assertEquals(0, hitCount);

    modifier.close();
    dir.close();
  }

  // test when delete terms only apply to disk segments
  public void testNonRAMDelete() throws IOException {

    Directory dir = newDirectory();
    IndexWriter modifier = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false))
                                           .setMaxBufferedDocs(2));
    int id = 0;
    int value = 100;

    for (int i = 0; i < 7; i++) {
      addDoc(modifier, ++id, value);
    }
    modifier.commit();

    assertEquals(0, modifier.getNumBufferedDocuments());
    assertTrue(0 < modifier.getSegmentCount());

    modifier.commit();

    IndexReader reader = DirectoryReader.open(dir);
    assertEquals(7, reader.numDocs());
    reader.close();

    modifier.deleteDocuments(new Term("value", String.valueOf(value)));

    modifier.commit();

    reader = DirectoryReader.open(dir);
    assertEquals(0, reader.numDocs());
    reader.close();
    modifier.close();
    dir.close();
  }

  // test when delete terms only apply to ram segments
  public void testRAMDeletes() throws IOException {
    for(int t=0;t<2;t++) {
      if (VERBOSE) {
        System.out.println("TEST: t=" + t);
      }
      Directory dir = newDirectory();
      IndexWriter modifier = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false))
                                             .setMaxBufferedDocs(4));
      int id = 0;
      int value = 100;

      addDoc(modifier, ++id, value);
      if (0 == t)
        modifier.deleteDocuments(new Term("value", String.valueOf(value)));
      else
        modifier.deleteDocuments(new TermQuery(new Term("value", String.valueOf(value))));
      addDoc(modifier, ++id, value);
      if (0 == t) {
        modifier.deleteDocuments(new Term("value", String.valueOf(value)));
        assertEquals(2, modifier.getNumBufferedDeleteTerms());
        assertEquals(1, modifier.getBufferedDeleteTermsSize());
      }
      else
        modifier.deleteDocuments(new TermQuery(new Term("value", String.valueOf(value))));

      addDoc(modifier, ++id, value);
      assertEquals(0, modifier.getSegmentCount());
      modifier.commit();

      IndexReader reader = DirectoryReader.open(dir);
      assertEquals(1, reader.numDocs());

      long hitCount = getHitCount(dir, new Term("id", String.valueOf(id)));
      assertEquals(1, hitCount);
      reader.close();
      modifier.close();
      dir.close();
    }
  }

  // test when delete terms apply to both disk and ram segments
  public void testBothDeletes() throws IOException {
    Directory dir = newDirectory();
    IndexWriter modifier = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false))
                                           .setMaxBufferedDocs(100));

    int id = 0;
    int value = 100;

    for (int i = 0; i < 5; i++) {
      addDoc(modifier, ++id, value);
    }

    value = 200;
    for (int i = 0; i < 5; i++) {
      addDoc(modifier, ++id, value);
    }
    modifier.commit();

    for (int i = 0; i < 5; i++) {
      addDoc(modifier, ++id, value);
    }
    modifier.deleteDocuments(new Term("value", String.valueOf(value)));

    modifier.commit();

    IndexReader reader = DirectoryReader.open(dir);
    assertEquals(5, reader.numDocs());
    modifier.close();
    reader.close();
    dir.close();
  }

  // test that batched delete terms are flushed together
  public void testBatchDeletes() throws IOException {
    Directory dir = newDirectory();
    IndexWriter modifier = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false))
                                           .setMaxBufferedDocs(2));

    int id = 0;
    int value = 100;

    for (int i = 0; i < 7; i++) {
      addDoc(modifier, ++id, value);
    }
    modifier.commit();

    IndexReader reader = DirectoryReader.open(dir);
    assertEquals(7, reader.numDocs());
    reader.close();

    id = 0;
    modifier.deleteDocuments(new Term("id", String.valueOf(++id)));
    modifier.deleteDocuments(new Term("id", String.valueOf(++id)));

    modifier.commit();

    reader = DirectoryReader.open(dir);
    assertEquals(5, reader.numDocs());
    reader.close();

    Term[] terms = new Term[3];
    for (int i = 0; i < terms.length; i++) {
      terms[i] = new Term("id", String.valueOf(++id));
    }
    modifier.deleteDocuments(terms);
    modifier.commit();
    reader = DirectoryReader.open(dir);
    assertEquals(2, reader.numDocs());
    reader.close();

    modifier.close();
    dir.close();
  }

  // test deleteAll()
  public void testDeleteAllSimple() throws IOException {
    if (VERBOSE) {
      System.out.println("TEST: now start");
    }
    Directory dir = newDirectory();
    IndexWriter modifier = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false))
                                           .setMaxBufferedDocs(2));

    int id = 0;
    int value = 100;

    for (int i = 0; i < 7; i++) {
      addDoc(modifier, ++id, value);
    }
    if (VERBOSE) {
      System.out.println("TEST: now commit");
    }
    modifier.commit();

    IndexReader reader = DirectoryReader.open(dir);
    assertEquals(7, reader.numDocs());
    reader.close();

    // Add 1 doc (so we will have something buffered)
    addDoc(modifier, 99, value);

    // Delete all
    if (VERBOSE) {
      System.out.println("TEST: now delete all");
    }
    modifier.deleteAll();

    // Delete all shouldn't be on disk yet
    reader = DirectoryReader.open(dir);
    assertEquals(7, reader.numDocs());
    reader.close();

    // Add a doc and update a doc (after the deleteAll, before the commit)
    addDoc(modifier, 101, value);
    updateDoc(modifier, 102, value);
    if (VERBOSE) {
      System.out.println("TEST: now 2nd commit");
    }

    // commit the delete all
    modifier.commit();

    // Validate there are no docs left
    reader = DirectoryReader.open(dir);
    assertEquals(2, reader.numDocs());
    reader.close();

    modifier.close();
    dir.close();
  }

  public void testDeleteAllNoDeadLock() throws IOException, InterruptedException {
    Directory dir = newDirectory();
    final RandomIndexWriter modifier = new RandomIndexWriter(random(), dir,
        newIndexWriterConfig().setMergePolicy(new MockRandomMergePolicy(random())));
    int numThreads = atLeast(2);
    Thread[] threads = new Thread[numThreads];
    final CountDownLatch latch = new CountDownLatch(1);
    final CountDownLatch doneLatch = new CountDownLatch(numThreads);
    for (int i = 0; i < numThreads; i++) {
      final int offset = i;
      threads[i] = new Thread() {
        @Override
        public void run() {
          int id = offset * 1000;
          int value = 100;
          try {
            latch.await();
            for (int j = 0; j < 1000; j++) {
              Document doc = new Document();
              doc.add(newTextField("content", "aaa", Field.Store.NO));
              doc.add(newStringField("id", String.valueOf(id++), Field.Store.YES));
              doc.add(newStringField("value", String.valueOf(value), Field.Store.NO));
              doc.add(new NumericDocValuesField("dv", value));
              modifier.addDocument(doc);
              if (VERBOSE) {
                System.out.println("\tThread["+offset+"]: add doc: " + id);
              }
            }
          } catch (Exception e) {
            throw new RuntimeException(e);
          } finally {
            doneLatch.countDown();
            if (VERBOSE) {
              System.out.println("\tThread["+offset+"]: done indexing" );
            }
          }
        }
      };
      threads[i].start();
    }
    latch.countDown();
    while (!doneLatch.await(1, TimeUnit.MILLISECONDS)) {
      if (VERBOSE) {
        System.out.println("\nTEST: now deleteAll");
      }
      modifier.deleteAll();
      if (VERBOSE) {
        System.out.println("del all");
      }
    }

    if (VERBOSE) {
      System.out.println("\nTEST: now final deleteAll");
    }

    modifier.deleteAll();
    for (Thread thread : threads) {
      thread.join();
    }

    if (VERBOSE) {
      System.out.println("\nTEST: now close");
    }
    modifier.close();

    DirectoryReader reader = DirectoryReader.open(dir);
    if (VERBOSE) {
      System.out.println("\nTEST: got reader=" + reader);
    }
    assertEquals(0, reader.maxDoc());
    assertEquals(0, reader.numDocs());
    assertEquals(0, reader.numDeletedDocs(), 0);
    reader.close();

    dir.close();
  }

  // test rollback of deleteAll()
  public void testDeleteAllRollback() throws IOException {
    Directory dir = newDirectory();
    IndexWriter modifier = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false))
                                           .setMaxBufferedDocs(2));

    int id = 0;
    int value = 100;

    for (int i = 0; i < 7; i++) {
      addDoc(modifier, ++id, value);
    }
    modifier.commit();

    addDoc(modifier, ++id, value);

    IndexReader reader = DirectoryReader.open(dir);
    assertEquals(7, reader.numDocs());
    reader.close();

    // Delete all
    modifier.deleteAll();

    // Roll it back
    modifier.rollback();

    // Validate that the docs are still there
    reader = DirectoryReader.open(dir);
    assertEquals(7, reader.numDocs());
    reader.close();

    dir.close();
  }


  // test deleteAll() w/ near real-time reader
  public void testDeleteAllNRT() throws IOException {
    Directory dir = newDirectory();
    IndexWriter modifier = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false))
                                           .setMaxBufferedDocs(2));

    int id = 0;
    int value = 100;

    for (int i = 0; i < 7; i++) {
      addDoc(modifier, ++id, value);
    }
    modifier.commit();

    IndexReader reader = modifier.getReader();
    assertEquals(7, reader.numDocs());
    reader.close();

    addDoc(modifier, ++id, value);
    addDoc(modifier, ++id, value);

    // Delete all
    modifier.deleteAll();

    reader = modifier.getReader();
    assertEquals(0, reader.numDocs());
    reader.close();


    // Roll it back
    modifier.rollback();

    // Validate that the docs are still there
    reader = DirectoryReader.open(dir);
    assertEquals(7, reader.numDocs());
    reader.close();

    dir.close();
  }

  // Verify that we can call deleteAll repeatedly without leaking field numbers such that we trigger OOME
  // on creation of FieldInfos. See https://issues.apache.org/jira/browse/LUCENE-9617
  @Nightly // Takes 1-2 minutes to run on a 16-core machine
  public void testDeleteAllRepeated() throws IOException, InterruptedException {
    final int breakingFieldCount = 50_000_000;
    try  (Directory dir = newDirectory()) {
      // Avoid flushing until the end of the test to save time.
      IndexWriterConfig conf = newIndexWriterConfig()
              .setMaxBufferedDocs(1000)
              .setRAMBufferSizeMB(1000)
              .setRAMPerThreadHardLimitMB(1000)
              .setCheckPendingFlushUpdate(false);
      try (IndexWriter modifier = new IndexWriter(dir, conf)) {
        Document document = new Document();
        int fieldsPerDoc = 1_000;
        for (int i = 0; i < fieldsPerDoc; i++) {
          document.add(new StoredField("field" + i, ""));
        }
        AtomicLong numFields = new AtomicLong(0);
        List<Thread> threads = new ArrayList<>();
        int nThreads = atLeast(8);
        for (int i = 0; i < nThreads; i++) {
          Thread t = new Thread(() -> {
            try {
              while (numFields.getAndAdd(fieldsPerDoc) < breakingFieldCount) {
                modifier.addDocument(document);
                modifier.deleteAll();
              }
            } catch (IOException e) {
              throw new UncheckedIOException(e);
            }
          });
          t.start();
          threads.add(t);
        }
        for (Thread t : threads) {
          t.join();
        }
        // Add one last document and flush to build FieldInfos.
        modifier.addDocument(document);
        modifier.flush();
      }
    }
  }

  private void updateDoc(IndexWriter modifier, int id, int value)
      throws IOException {
    Document doc = new Document();
    doc.add(newTextField("content", "aaa", Field.Store.NO));
    doc.add(newStringField("id", String.valueOf(id), Field.Store.YES));
    doc.add(newStringField("value", String.valueOf(value), Field.Store.NO));
    doc.add(new NumericDocValuesField("dv", value));
    modifier.updateDocument(new Term("id", String.valueOf(id)), doc);
  }


  private void addDoc(IndexWriter modifier, int id, int value)
      throws IOException {
    Document doc = new Document();
    doc.add(newTextField("content", "aaa", Field.Store.NO));
    doc.add(newStringField("id", String.valueOf(id), Field.Store.YES));
    doc.add(newStringField("value", String.valueOf(value), Field.Store.NO));
    doc.add(new NumericDocValuesField("dv", value));
    modifier.addDocument(doc);
  }

  private long getHitCount(Directory dir, Term term) throws IOException {
    IndexReader reader = DirectoryReader.open(dir);
    IndexSearcher searcher = newSearcher(reader);
    long hitCount = searcher.search(new TermQuery(term), 1000).totalHits.value;
    reader.close();
    return hitCount;
  }

  // TODO: can we fix MockDirectoryWrapper disk full checking to be more efficient (not recompute on every write)?
  @Nightly
  public void testDeletesOnDiskFull() throws IOException {
    doTestOperationsOnDiskFull(false);
  }

  // TODO: can we fix MockDirectoryWrapper disk full checking to be more efficient (not recompute on every write)?
  @Nightly
  public void testUpdatesOnDiskFull() throws IOException {
    doTestOperationsOnDiskFull(true);
  }

  /**
   * Make sure if modifier tries to commit but hits disk full that modifier
   * remains consistent and usable. Similar to TestIndexReader.testDiskFull().
   */
  private void doTestOperationsOnDiskFull(boolean updates) throws IOException {

    Term searchTerm = new Term("content", "aaa");
    int START_COUNT = 157;
    int END_COUNT = 144;

    // First build up a starting index:
    MockDirectoryWrapper startDir = newMockDirectory();

    IndexWriter writer = new IndexWriter(startDir, newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false)));
    for (int i = 0; i < 157; i++) {
      Document d = new Document();
      d.add(newStringField("id", Integer.toString(i), Field.Store.YES));
      d.add(newTextField("content", "aaa " + i, Field.Store.NO));
      d.add(new NumericDocValuesField("dv", i));
      writer.addDocument(d);
    }
    writer.close();

    long diskUsage = startDir.sizeInBytes();
    long diskFree = diskUsage + 10;

    IOException err = null;

    boolean done = false;

    // Iterate w/ ever increasing free disk space:
    while (!done) {
      if (VERBOSE) {
        System.out.println("TEST: cycle");
      }
      MockDirectoryWrapper dir = new MockDirectoryWrapper(random(), TestUtil.ramCopyOf(startDir));
      dir.setAllowRandomFileNotFoundException(false);
      IndexWriter modifier = new IndexWriter(dir,
                                             newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false))
                                             .setMaxBufferedDocs(1000)
                                             .setMergeScheduler(new ConcurrentMergeScheduler()));
      ((ConcurrentMergeScheduler) modifier.getConfig().getMergeScheduler()).setSuppressExceptions();

      // For each disk size, first try to commit against
      // dir that will hit random IOExceptions & disk
      // full; after, give it infinite disk space & turn
      // off random IOExceptions & retry w/ same reader:
      boolean success = false;

      for (int x = 0; x < 2; x++) {
        if (VERBOSE) {
          System.out.println("TEST: x=" + x);
        }

        double rate = 0.1;
        double diskRatio = ((double)diskFree) / diskUsage;
        long thisDiskFree;
        String testName;

        if (0 == x) {
          thisDiskFree = diskFree;
          if (diskRatio >= 2.0) {
            rate /= 2;
          }
          if (diskRatio >= 4.0) {
            rate /= 2;
          }
          if (diskRatio >= 6.0) {
            rate = 0.0;
          }
          if (VERBOSE) {
            System.out.println("\ncycle: " + diskFree + " bytes");
          }
          testName = "disk full during reader.close() @ " + thisDiskFree
            + " bytes";
          dir.setRandomIOExceptionRateOnOpen(random().nextDouble()*0.01);
        } else {
          thisDiskFree = 0;
          rate = 0.0;
          if (VERBOSE) {
            System.out.println("\ncycle: same writer: unlimited disk space");
          }
          testName = "reader re-use after disk full";
          dir.setRandomIOExceptionRateOnOpen(0.0);
        }

        dir.setMaxSizeInBytes(thisDiskFree);
        dir.setRandomIOExceptionRate(rate);

        try {
          if (0 == x) {
            int docId = 12;
            for (int i = 0; i < 13; i++) {
              if (updates) {
                Document d = new Document();
                d.add(newStringField("id", Integer.toString(i), Field.Store.YES));
                d.add(newTextField("content", "bbb " + i, Field.Store.NO));
                d.add(new NumericDocValuesField("dv", i));
                modifier.updateDocument(new Term("id", Integer.toString(docId)), d);
              } else { // deletes
                modifier.deleteDocuments(new Term("id", Integer.toString(docId)));
                // modifier.setNorm(docId, "contents", (float)2.0);
              }
              docId += 12;
            }
            try {
              modifier.close();
            } catch (IllegalStateException ise) {
              // ok
              throw (IOException) ise.getCause();
            }
          }
          success = true;
          if (0 == x) {
            done = true;
          }
        }
        catch (IOException e) {
          if (VERBOSE) {
            System.out.println("  hit IOException: " + e);
            e.printStackTrace(System.out);
          }
          err = e;
          if (1 == x) {
            e.printStackTrace();
            fail(testName + " hit IOException after disk space was freed up");
          }
        }
        // prevent throwing a random exception here!!
        final double randomIOExceptionRate = dir.getRandomIOExceptionRate();
        final long maxSizeInBytes = dir.getMaxSizeInBytes();
        dir.setRandomIOExceptionRate(0.0);
        dir.setRandomIOExceptionRateOnOpen(0.0);
        dir.setMaxSizeInBytes(0);
        if (!success) {
          // Must force the close else the writer can have
          // open files which cause exc in MockRAMDir.close
          if (VERBOSE) {
            System.out.println("TEST: now rollback");
          }
          modifier.rollback();
        }

        // If the close() succeeded, make sure index is OK:
        if (success) {
          TestUtil.checkIndex(dir);
        }
        dir.setRandomIOExceptionRate(randomIOExceptionRate);
        dir.setMaxSizeInBytes(maxSizeInBytes);

        // Finally, verify index is not corrupt, and, if
        // we succeeded, we see all docs changed, and if
        // we failed, we see either all docs or no docs
        // changed (transactional semantics):
        IndexReader newReader = null;
        try {
          newReader = DirectoryReader.open(dir);
        }
        catch (IOException e) {
          e.printStackTrace();
          fail(testName
               + ":exception when creating IndexReader after disk full during close: "
               + e);
        }

        IndexSearcher searcher = newSearcher(newReader);
        ScoreDoc[] hits = null;
        try {
          hits = searcher.search(new TermQuery(searchTerm), 1000).scoreDocs;
        }
        catch (IOException e) {
          e.printStackTrace();
          fail(testName + ": exception when searching: " + e);
        }
        int result2 = hits.length;
        if (success) {
          if (x == 0 && result2 != END_COUNT) {
            fail(testName
                 + ": method did not throw exception but hits.length for search on term 'aaa' is "
                 + result2 + " instead of expected " + END_COUNT);
          } else if (x == 1 && result2 != START_COUNT && result2 != END_COUNT) {
            // It's possible that the first exception was
            // "recoverable" wrt pending deletes, in which
            // case the pending deletes are retained and
            // then re-flushing (with plenty of disk
            // space) will succeed in flushing the
            // deletes:
            fail(testName
                 + ": method did not throw exception but hits.length for search on term 'aaa' is "
                 + result2 + " instead of expected " + START_COUNT + " or " + END_COUNT);
          }
        } else {
          // On hitting exception we still may have added
          // all docs:
          if (result2 != START_COUNT && result2 != END_COUNT) {
            err.printStackTrace();
            fail(testName
                 + ": method did throw exception but hits.length for search on term 'aaa' is "
                 + result2 + " instead of expected " + START_COUNT + " or " + END_COUNT);
          }
        }
        newReader.close();
        if (result2 == END_COUNT) {
          break;
        }
      }
      dir.close();

      // Try again with more bytes of free space:
      diskFree += Math.max(10, diskFree >>> 3);
    }
    startDir.close();
  }

  @Ignore
  // This test tests that buffered deletes are cleared when
  // an Exception is hit during flush.
  public void testErrorAfterApplyDeletes() throws IOException {

    MockDirectoryWrapper.Failure failure = new MockDirectoryWrapper.Failure() {
        boolean sawMaybe = false;
        boolean failed = false;
        Thread thread;
        @Override
        public MockDirectoryWrapper.Failure reset() {
          thread = Thread.currentThread();
          sawMaybe = false;
          failed = false;
          return this;
        }
        @Override
        public void eval(MockDirectoryWrapper dir)  throws IOException {
          if (Thread.currentThread() != thread) {
            // don't fail during merging
            return;
          }
          if (VERBOSE) {
            System.out.println("FAIL EVAL:");
          }
          new Throwable().printStackTrace(System.out);
          if (sawMaybe && !failed) {
            boolean seen = callStackContainsAnyOf("applyDeletesAndUpdates", "slowFileExists");
            if (!seen) {
              // Only fail once we are no longer in applyDeletes
              failed = true;
              if (VERBOSE) {
                System.out.println("TEST: mock failure: now fail");
                new Throwable().printStackTrace(System.out);
              }
              throw new RuntimeException("fail after applyDeletes");
            }
          }
          if (!failed) {
            if (callStackContainsAnyOf("applyDeletesAndUpdates")) {
              if (VERBOSE) {
                System.out.println("TEST: mock failure: saw applyDeletes");
                new Throwable().printStackTrace(System.out);
              }
              sawMaybe = true;
            }
          }
        }
      };

    // create a couple of files

    String[] keywords = { "1", "2" };
    String[] unindexed = { "Netherlands", "Italy" };
    String[] unstored = { "Amsterdam has lots of bridges",
        "Venice has lots of canals" };
    String[] text = { "Amsterdam", "Venice" };

    MockDirectoryWrapper dir = newMockDirectory();
    IndexWriter modifier = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false))
                                                  .setReaderPooling(false)
                                                  .setMergePolicy(newLogMergePolicy()));

    MergePolicy lmp = modifier.getConfig().getMergePolicy();
    lmp.setNoCFSRatio(1.0);

    dir.failOn(failure.reset());

    FieldType custom1 = new FieldType();
    custom1.setStored(true);
    for (int i = 0; i < keywords.length; i++) {
      Document doc = new Document();
      doc.add(newStringField("id", keywords[i], Field.Store.YES));
      doc.add(newField("country", unindexed[i], custom1));
      doc.add(newTextField("contents", unstored[i], Field.Store.NO));
      doc.add(newTextField("city", text[i], Field.Store.YES));
      modifier.addDocument(doc);
    }
    // flush

    if (VERBOSE) {
      System.out.println("TEST: now full merge");
    }

    modifier.forceMerge(1);
    if (VERBOSE) {
      System.out.println("TEST: now commit");
    }
    modifier.commit();

    // one of the two files hits

    Term term = new Term("city", "Amsterdam");
    long hitCount = getHitCount(dir, term);
    assertEquals(1, hitCount);

    // open the writer again (closed above)

    // delete the doc
    // max buf del terms is two, so this is buffered

    if (VERBOSE) {
      System.out.println("TEST: delete term=" + term);
    }

    modifier.deleteDocuments(term);

    // add a doc
    // doc remains buffered

    if (VERBOSE) {
      System.out.println("TEST: add empty doc");
    }
    Document doc = new Document();
    modifier.addDocument(doc);

    // commit the changes, the buffered deletes, and the new doc

    // The failure object will fail on the first write after the del
    // file gets created when processing the buffered delete

    // in the ac case, this will be when writing the new segments
    // files so we really don't need the new doc, but it's harmless

    // a new segments file won't be created but in this
    // case, creation of the cfs file happens next so we
    // need the doc (to test that it's okay that we don't
    // lose deletes if failing while creating the cfs file)

    if (VERBOSE) {
      System.out.println("TEST: now commit for failure");
    }
    RuntimeException expected = expectThrows(RuntimeException.class, () -> {
      modifier.commit();
    });
    if (VERBOSE) {
      System.out.println("TEST: hit exc:");
      expected.printStackTrace(System.out);
    }

    // The commit above failed, so we need to retry it (which will
    // succeed, because the failure is a one-shot)

    boolean writerClosed;
    try {
      modifier.commit();
      writerClosed = false;
    } catch (IllegalStateException ise) {
      // The above exc struck during merge, and closed the writer
      writerClosed = true;
    }

    if (writerClosed == false) {
      hitCount = getHitCount(dir, term);

      // Make sure the delete was successfully flushed:
      assertEquals(0, hitCount);

      modifier.close();
    }
    dir.close();
  }

  // This test tests that the files created by the docs writer before
  // a segment is written are cleaned up if there's an i/o error

  public void testErrorInDocsWriterAdd() throws IOException {

    MockDirectoryWrapper.Failure failure = new MockDirectoryWrapper.Failure() {
        boolean failed = false;
        @Override
        public MockDirectoryWrapper.Failure reset() {
          failed = false;
          return this;
        }
        @Override
        public void eval(MockDirectoryWrapper dir)  throws IOException {
          if (!failed) {
            failed = true;
            throw new IOException("fail in add doc");
          }
        }
      };

    // create a couple of files

    String[] keywords = { "1", "2" };
    String[] unindexed = { "Netherlands", "Italy" };
    String[] unstored = { "Amsterdam has lots of bridges",
        "Venice has lots of canals" };
    String[] text = { "Amsterdam", "Venice" };

    MockDirectoryWrapper dir = newMockDirectory();
    IndexWriter modifier = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false)));
    modifier.commit();
    dir.failOn(failure.reset());

    FieldType custom1 = new FieldType();
    custom1.setStored(true);
    for (int i = 0; i < keywords.length; i++) {
      Document doc = new Document();
      doc.add(newStringField("id", keywords[i], Field.Store.YES));
      doc.add(newField("country", unindexed[i], custom1));
      doc.add(newTextField("contents", unstored[i], Field.Store.NO));
      doc.add(newTextField("city", text[i], Field.Store.YES));
      try {
        modifier.addDocument(doc);
      } catch (IOException io) {
        if (VERBOSE) {
          System.out.println("TEST: got expected exc:");
          io.printStackTrace(System.out);
        }
        break;
      }
    }
    assertTrue(modifier.isDeleterClosed());

    TestIndexWriter.assertNoUnreferencedFiles(dir, "docsWriter.abort() failed to delete unreferenced files");
    dir.close();
  }

  public void testDeleteNullQuery() throws IOException {
    Directory dir = newDirectory();
    IndexWriter modifier = new IndexWriter(dir, new IndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false)));

    for (int i = 0; i < 5; i++) {
      addDoc(modifier, i, 2*i);
    }

    modifier.deleteDocuments(new TermQuery(new Term("nada", "nada")));
    modifier.commit();
    assertEquals(5, modifier.getDocStats().numDocs);
    modifier.close();
    dir.close();
  }

  public void testDeleteAllSlowly() throws Exception {
    final Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    final int NUM_DOCS = atLeast(1000);
    final List<Integer> ids = new ArrayList<>(NUM_DOCS);
    for(int id=0;id<NUM_DOCS;id++) {
      ids.add(id);
    }
    Collections.shuffle(ids, random());
    for(int id : ids) {
      Document doc = new Document();
      doc.add(newStringField("id", ""+id, Field.Store.NO));
      w.addDocument(doc);
    }
    Collections.shuffle(ids, random());
    int upto = 0;
    while(upto < ids.size()) {
      final int left = ids.size() - upto;
      final int inc = Math.min(left, TestUtil.nextInt(random(), 1, 20));
      final int limit = upto + inc;
      while(upto < limit) {
        if (VERBOSE) {
          System.out.println("TEST: delete id=" + ids.get(upto));
        }
        w.deleteDocuments(new Term("id", ""+ids.get(upto++)));
      }
      if (VERBOSE) {
        System.out.println("\nTEST: now open reader");
      }
      final IndexReader r = w.getReader();
      assertEquals(NUM_DOCS - upto, r.numDocs());
      r.close();
    }

    w.close();
    dir.close();
  }

  // TODO: this test can hit pathological cases (IW settings?) where it runs for far too long
  @Nightly
  public void testIndexingThenDeleting() throws Exception {
    // TODO: move this test to its own class and just @SuppressCodecs?
    // TODO: is it enough to just use newFSDirectory?
    final String fieldFormat = TestUtil.getPostingsFormat("field");
    assumeFalse("This test cannot run with SimpleText codec", fieldFormat.equals("SimpleText"));
    assumeFalse("This test cannot run with Direct codec", fieldFormat.equals("Direct"));
    final Random r = random();
    Directory dir = newDirectory();
    // note this test explicitly disables payloads
    final Analyzer analyzer = new Analyzer() {
      @Override
      public TokenStreamComponents createComponents(String fieldName) {
        return new TokenStreamComponents(new MockTokenizer(MockTokenizer.WHITESPACE, true));
      }
    };
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(analyzer)
                                           .setRAMBufferSizeMB(4.0)
                                    .setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH));
    Document doc = new Document();
    doc.add(newTextField("field", "go 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20", Field.Store.NO));
    int num = atLeast(1);
    for (int iter = 0; iter < num; iter++) {
      int count = 0;

      final boolean doIndexing = r.nextBoolean();
      if (VERBOSE) {
        System.out.println("TEST: iter doIndexing=" + doIndexing);
      }
      if (doIndexing) {
        // Add docs until a flush is triggered
        final int startFlushCount = w.getFlushCount();
        while(w.getFlushCount() == startFlushCount) {
          w.addDocument(doc);
          count++;
        }
      } else {
        // Delete docs until a flush is triggered
        final int startFlushCount = w.getFlushCount();
        while(w.getFlushCount() == startFlushCount) {
          w.deleteDocuments(new Term("foo", ""+count));
          count++;
        }
      }
      assertTrue("flush happened too quickly during " + (doIndexing ? "indexing" : "deleting") + " count=" + count, count > 2500);
    }
    w.close();
    dir.close();
  }

  // LUCENE-3340: make sure deletes that we don't apply
  // during flush (ie are just pushed into the stream) are
  // in fact later flushed due to their RAM usage:
  public void testFlushPushedDeletesByRAM() throws Exception {
    Directory dir = newDirectory();
    // Cannot use RandomIndexWriter because we don't want to
    // ever call commit() for this test:
    // note: tiny rambuffer used, as with a 1MB buffer the test is too slow (flush @ 128,999)
    IndexWriter w = new IndexWriter(dir,
                                    newIndexWriterConfig(new MockAnalyzer(random()))
                                      .setRAMBufferSizeMB(0.8f)
                                      .setMaxBufferedDocs(1000)
                                      .setMergePolicy(NoMergePolicy.INSTANCE)
                                      .setReaderPooling(false));
    int count = 0;
    while(true) {
      Document doc = new Document();
      doc.add(new StringField("id", count+"", Field.Store.NO));
      final Term delTerm;
      if (count == 1010) {
        // This is the only delete that applies
        delTerm = new Term("id", ""+0);
      } else {
        // These get buffered, taking up RAM, but delete
        // nothing when applied:
        delTerm = new Term("id", "x" + count);
      }
      w.updateDocument(delTerm, doc);
      // Eventually segment 0 should get a del docs:
      // TODO: fix this test
      if (slowFileExists(dir, "_0_1.del") || slowFileExists(dir, "_0_1.liv") ) {
        if (VERBOSE) {
          System.out.println("TEST: deletes created @ count=" + count);
        }
        break;
      }
      count++;

      // Today we applyDeletes @ count=21553; even if we make
      // sizable improvements to RAM efficiency of buffered
      // del term we're unlikely to go over 100K:
      if (count > 100000) {
        fail("delete's were not applied");
      }
    }
    w.close();
    dir.close();
  }

  // Make sure buffered (pushed) deletes don't use up so
  // much RAM that it forces long tail of tiny segments:
  @Nightly
  public void testApplyDeletesOnFlush() throws Exception {
    Directory dir = newDirectory();
    // Cannot use RandomIndexWriter because we don't want to
    // ever call commit() for this test:
    final AtomicInteger docsInSegment = new AtomicInteger();
    final AtomicBoolean closing = new AtomicBoolean();
    final AtomicBoolean sawAfterFlush = new AtomicBoolean();
    IndexWriter w = new IndexWriter(dir,
                                    newIndexWriterConfig(new MockAnalyzer(random()))
                                       .setRAMBufferSizeMB(1.2)
                                       .setMaxBufferedDocs(-1)
                                       .setMergePolicy(NoMergePolicy.INSTANCE)
                                       .setReaderPooling(false)
                                       // always use CFS so we don't use tons of file handles in the test
                                       .setUseCompoundFile(true)) {
        @Override
        public void doAfterFlush() {
          assertTrue("only " + docsInSegment.get() + " docs in segment", closing.get() || docsInSegment.get() >= 7);
          docsInSegment.set(0);
          sawAfterFlush.set(true);
        }
      };
    int id = 0;
    while(true) {
      StringBuilder sb = new StringBuilder();
      for(int termIDX=0;termIDX<100;termIDX++) {
        sb.append(' ').append(TestUtil.randomRealisticUnicodeString(random()));
      }
      if (id == 500) {
        w.deleteDocuments(new Term("id", "0"));
      }
      Document doc = new Document();
      doc.add(newStringField("id", ""+id, Field.Store.NO));
      doc.add(newTextField("body", sb.toString(), Field.Store.NO));
      w.updateDocument(new Term("id", ""+id), doc);
      docsInSegment.incrementAndGet();
      // TODO: fix this test
      if (slowFileExists(dir, "_0_1.del") || slowFileExists(dir, "_0_1.liv")) {
        if (VERBOSE) {
          System.out.println("TEST: deletes created @ id=" + id);
        }
        break;
      }
      id++;
    }
    closing.set(true);
    assertTrue(sawAfterFlush.get());
    w.close();
    dir.close();
  }

  // LUCENE-4455
  public void testDeletesCheckIndexOutput() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setMergePolicy(NoMergePolicy.INSTANCE);
    iwc.setMaxBufferedDocs(2);
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(newField("field", "0", StringField.TYPE_NOT_STORED));
    w.addDocument(doc);

    doc = new Document();
    doc.add(newField("field", "1", StringField.TYPE_NOT_STORED));
    w.addDocument(doc);
    w.commit();
    assertEquals(1, w.getSegmentCount());

    w.deleteDocuments(new Term("field", "0"));
    w.commit();
    assertEquals(1, w.getSegmentCount());
    w.close();

    ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
    CheckIndex checker = new CheckIndex(dir);
    checker.setInfoStream(new PrintStream(bos, false, IOUtils.UTF_8), false);
    CheckIndex.Status indexStatus = checker.checkIndex(null);
    assertTrue(indexStatus.clean);
    checker.close();
    String s = bos.toString(IOUtils.UTF_8);

    // Segment should have deletions:
    assertTrue(s.contains("has deletions"));
    iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    w = new IndexWriter(dir, iwc);
    w.forceMerge(1);
    w.close();

    bos = new ByteArrayOutputStream(1024);
    checker = new CheckIndex(dir);
    checker.setInfoStream(new PrintStream(bos, false, IOUtils.UTF_8), false);
    indexStatus = checker.checkIndex(null);
    assertTrue(indexStatus.clean);
    checker.close();
    s = bos.toString(IOUtils.UTF_8);
    assertFalse(s.contains("has deletions"));
    dir.close();
  }

  public void testTryDeleteDocument() throws Exception {

    Directory d = newDirectory();

    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(d, iwc);
    Document doc = new Document();
    w.addDocument(doc);
    w.addDocument(doc);
    w.addDocument(doc);
    w.close();

    iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setOpenMode(IndexWriterConfig.OpenMode.APPEND);
    w = new IndexWriter(d, iwc);
    IndexReader r = DirectoryReader.open(w, false, false);
    assertTrue(w.tryDeleteDocument(r, 1) != -1);
    assertFalse(((StandardDirectoryReader)r).isCurrent());
    assertTrue(w.tryDeleteDocument(r.leaves().get(0).reader(), 0) != -1);
    assertFalse(((StandardDirectoryReader)r).isCurrent());
    r.close();
    w.close();

    r = DirectoryReader.open(d);
    assertEquals(2, r.numDeletedDocs());
    assertNotNull(MultiBits.getLiveDocs(r));
    r.close();
    d.close();
  }

  public void testNRTIsCurrentAfterDelete() throws Exception {
    Directory d = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(d, iwc);
    Document doc = new Document();
    w.addDocument(doc);
    w.addDocument(doc);
    w.addDocument(doc);
    doc.add(new StringField("id", "1", Field.Store.YES));
    w.addDocument(doc);
    w.close();
    iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setOpenMode(IndexWriterConfig.OpenMode.APPEND);
    w = new IndexWriter(d, iwc);
    IndexReader r = DirectoryReader.open(w, false, false);
    w.deleteDocuments(new Term("id", "1"));
    IndexReader r2 = DirectoryReader.open(w, true, true);
    assertFalse(((StandardDirectoryReader)r).isCurrent());
    assertTrue(((StandardDirectoryReader)r2).isCurrent());
    IOUtils.close(r, r2, w, d);
  }

  public void testOnlyDeletesTriggersMergeOnClose() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setMaxBufferedDocs(2);
    LogDocMergePolicy mp = new LogDocMergePolicy();
    mp.setMinMergeDocs(1);
    iwc.setMergePolicy(mp);
    iwc.setMergeScheduler(new SerialMergeScheduler());
    IndexWriter w = new IndexWriter(dir, iwc);
    for(int i=0;i<38;i++) {
      Document doc = new Document();
      doc.add(newStringField("id", ""+i, Field.Store.NO));
      w.addDocument(doc);
    }
    w.commit();

    for(int i=0;i<18;i++) {
      w.deleteDocuments(new Term("id", ""+i));
    }

    w.close();
    DirectoryReader r = DirectoryReader.open(dir);
    assertEquals(1, r.leaves().size());
    r.close();

    dir.close();
  }

  public void testOnlyDeletesTriggersMergeOnGetReader() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setMaxBufferedDocs(2);
    LogDocMergePolicy mp = new LogDocMergePolicy();
    mp.setMinMergeDocs(1);
    iwc.setMergePolicy(mp);
    iwc.setMergeScheduler(new SerialMergeScheduler());
    IndexWriter w = new IndexWriter(dir, iwc);
    for(int i=0;i<38;i++) {
      Document doc = new Document();
      doc.add(newStringField("id", ""+i, Field.Store.NO));
      w.addDocument(doc);
    }
    w.commit();

    for(int i=0;i<18;i++) {
      w.deleteDocuments(new Term("id", ""+i));
    }

    // First one triggers, but does not reflect, the merge:
    if (VERBOSE) {
      System.out.println("TEST: now get reader");
    }
    DirectoryReader.open(w).close();
    IndexReader r = DirectoryReader.open(w);
    assertEquals(1, r.leaves().size());
    r.close();

    w.close();
    dir.close();
  }

  public void testOnlyDeletesTriggersMergeOnFlush() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setMaxBufferedDocs(2);
    LogDocMergePolicy mp = new LogDocMergePolicy();
    mp.setMinMergeDocs(1);
    iwc.setMergePolicy(mp);
    iwc.setMergeScheduler(new SerialMergeScheduler());
    IndexWriter w = new IndexWriter(dir, iwc);
    for(int i=0;i<38;i++) {
      if (VERBOSE) {
        System.out.println("TEST: add doc " + i);
      }
      Document doc = new Document();
      doc.add(newStringField("id", ""+i, Field.Store.NO));
      w.addDocument(doc);
    }
    if (VERBOSE) {
      System.out.println("TEST: commit1");
    }
    w.commit();

    // Deleting 18 out of the 20 docs in the first segment make it the same "level" as the other 9 which should cause a merge to kick off:
    for(int i=0;i<18;i++) {
      w.deleteDocuments(new Term("id", ""+i));
    }
    if (VERBOSE) {
      System.out.println("TEST: commit2");
    }
    w.close();

    DirectoryReader r = DirectoryReader.open(dir);
    assertEquals(1, r.leaves().size());
    r.close();

    dir.close();
  }

  public void testOnlyDeletesDeleteAllDocs() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setMaxBufferedDocs(2);
    LogDocMergePolicy mp = new LogDocMergePolicy();
    mp.setMinMergeDocs(1);
    iwc.setMergePolicy(mp);
    iwc.setMergeScheduler(new SerialMergeScheduler());
    IndexWriter w = new IndexWriter(dir, iwc);
    for(int i=0;i<38;i++) {
      Document doc = new Document();
      doc.add(newStringField("id", ""+i, Field.Store.NO));
      w.addDocument(doc);
    }
    w.commit();

    for(int i=0;i<38;i++) {
      w.deleteDocuments(new Term("id", ""+i));
    }

    DirectoryReader r = DirectoryReader.open(w);
    assertEquals(0, r.leaves().size());
    assertEquals(0, r.maxDoc());
    r.close();

    w.close();
    dir.close();
  }

  // Make sure merges still kick off after IW.deleteAll!
  public void testMergingAfterDeleteAll() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setMaxBufferedDocs(2);
    LogDocMergePolicy mp = new LogDocMergePolicy();
    mp.setMinMergeDocs(1);
    iwc.setMergePolicy(mp);
    iwc.setMergeScheduler(new SerialMergeScheduler());
    IndexWriter w = new IndexWriter(dir, iwc);
    for(int i=0;i<10;i++) {
      Document doc = new Document();
      doc.add(newStringField("id", ""+i, Field.Store.NO));
      w.addDocument(doc);
    }
    w.commit();
    w.deleteAll();

    for(int i=0;i<100;i++) {
      Document doc = new Document();
      doc.add(newStringField("id", ""+i, Field.Store.NO));
      w.addDocument(doc);
    }

    w.forceMerge(1);

    DirectoryReader r = DirectoryReader.open(w);
    assertEquals(1, r.leaves().size());
    r.close();

    w.close();
    dir.close();
  }
}
