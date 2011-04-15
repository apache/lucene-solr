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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

public class TestIndexWriterDelete extends LuceneTestCase {

  // test the simple case
  public void testSimpleCase() throws IOException {
    String[] keywords = { "1", "2" };
    String[] unindexed = { "Netherlands", "Italy" };
    String[] unstored = { "Amsterdam has lots of bridges",
        "Venice has lots of canals" };
    String[] text = { "Amsterdam", "Venice" };

    Directory dir = newDirectory();
    IndexWriter modifier = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random, MockTokenizer.WHITESPACE, false)).setMaxBufferedDeleteTerms(1));

    for (int i = 0; i < keywords.length; i++) {
      Document doc = new Document();
      doc.add(newField("id", keywords[i], Field.Store.YES,
                        Field.Index.NOT_ANALYZED));
      doc.add(newField("country", unindexed[i], Field.Store.YES,
                        Field.Index.NO));
      doc.add(newField("contents", unstored[i], Field.Store.NO,
                        Field.Index.ANALYZED));
      doc
        .add(newField("city", text[i], Field.Store.YES,
                       Field.Index.ANALYZED));
      modifier.addDocument(doc);
    }
    modifier.optimize();
    modifier.commit();

    Term term = new Term("city", "Amsterdam");
    int hitCount = getHitCount(dir, term);
    assertEquals(1, hitCount);
    modifier.deleteDocuments(term);
    modifier.commit();
    hitCount = getHitCount(dir, term);
    assertEquals(0, hitCount);

    modifier.close();
    dir.close();
  }

  // test when delete terms only apply to disk segments
  public void testNonRAMDelete() throws IOException {

    Directory dir = newDirectory();
    IndexWriter modifier = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random, MockTokenizer.WHITESPACE, false)).setMaxBufferedDocs(2)
        .setMaxBufferedDeleteTerms(2));
    modifier.setInfoStream(VERBOSE ? System.out : null);
    int id = 0;
    int value = 100;

    for (int i = 0; i < 7; i++) {
      addDoc(modifier, ++id, value);
    }
    modifier.commit();

    assertEquals(0, modifier.getNumBufferedDocuments());
    assertTrue(0 < modifier.getSegmentCount());

    modifier.commit();

    IndexReader reader = IndexReader.open(dir, true);
    assertEquals(7, reader.numDocs());
    reader.close();

    modifier.deleteDocuments(new Term("value", String.valueOf(value)));

    modifier.commit();

    reader = IndexReader.open(dir, true);
    assertEquals(0, reader.numDocs());
    reader.close();
    modifier.close();
    dir.close();
  }

  public void testMaxBufferedDeletes() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random, MockTokenizer.WHITESPACE, false)).setMaxBufferedDeleteTerms(1));

    writer.setInfoStream(VERBOSE ? System.out : null);
    writer.addDocument(new Document());
    writer.deleteDocuments(new Term("foobar", "1"));
    writer.deleteDocuments(new Term("foobar", "1"));
    writer.deleteDocuments(new Term("foobar", "1"));
    assertEquals(3, writer.getFlushDeletesCount());
    writer.close();
    dir.close();
  }
  
  // test when delete terms only apply to ram segments
  public void testRAMDeletes() throws IOException {
    for(int t=0;t<2;t++) {
      if (VERBOSE) {
        System.out.println("TEST: t=" + t);
      }
      Directory dir = newDirectory();
      IndexWriter modifier = new IndexWriter(dir, newIndexWriterConfig(
          TEST_VERSION_CURRENT, new MockAnalyzer(random, MockTokenizer.WHITESPACE, false)).setMaxBufferedDocs(4)
          .setMaxBufferedDeleteTerms(4));
      modifier.setInfoStream(VERBOSE ? System.out : null);
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

      IndexReader reader = IndexReader.open(dir, true);
      assertEquals(1, reader.numDocs());

      int hitCount = getHitCount(dir, new Term("id", String.valueOf(id)));
      assertEquals(1, hitCount);
      reader.close();
      modifier.close();
      dir.close();
    }
  }

  // test when delete terms apply to both disk and ram segments
  public void testBothDeletes() throws IOException {
    Directory dir = newDirectory();
    IndexWriter modifier = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random, MockTokenizer.WHITESPACE, false)).setMaxBufferedDocs(100)
        .setMaxBufferedDeleteTerms(100));

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

    IndexReader reader = IndexReader.open(dir, true);
    assertEquals(5, reader.numDocs());
    modifier.close();
    reader.close();
    dir.close();
  }

  // test that batched delete terms are flushed together
  public void testBatchDeletes() throws IOException {
    Directory dir = newDirectory();
    IndexWriter modifier = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random, MockTokenizer.WHITESPACE, false)).setMaxBufferedDocs(2)
        .setMaxBufferedDeleteTerms(2));

    int id = 0;
    int value = 100;

    for (int i = 0; i < 7; i++) {
      addDoc(modifier, ++id, value);
    }
    modifier.commit();

    IndexReader reader = IndexReader.open(dir, true);
    assertEquals(7, reader.numDocs());
    reader.close();

    id = 0;
    modifier.deleteDocuments(new Term("id", String.valueOf(++id)));
    modifier.deleteDocuments(new Term("id", String.valueOf(++id)));

    modifier.commit();

    reader = IndexReader.open(dir, true);
    assertEquals(5, reader.numDocs());
    reader.close();

    Term[] terms = new Term[3];
    for (int i = 0; i < terms.length; i++) {
      terms[i] = new Term("id", String.valueOf(++id));
    }
    modifier.deleteDocuments(terms);
    modifier.commit();
    reader = IndexReader.open(dir, true);
    assertEquals(2, reader.numDocs());
    reader.close();

    modifier.close();
    dir.close();
  }

  // test deleteAll()
  public void testDeleteAll() throws IOException {
    Directory dir = newDirectory();
    IndexWriter modifier = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random, MockTokenizer.WHITESPACE, false)).setMaxBufferedDocs(2)
        .setMaxBufferedDeleteTerms(2));

    int id = 0;
    int value = 100;

    for (int i = 0; i < 7; i++) {
      addDoc(modifier, ++id, value);
    }
    modifier.commit();

    IndexReader reader = IndexReader.open(dir, true);
    assertEquals(7, reader.numDocs());
    reader.close();

    // Add 1 doc (so we will have something buffered)
    addDoc(modifier, 99, value);

    // Delete all
    modifier.deleteAll();

    // Delete all shouldn't be on disk yet
    reader = IndexReader.open(dir, true);
    assertEquals(7, reader.numDocs());
    reader.close();

    // Add a doc and update a doc (after the deleteAll, before the commit)
    addDoc(modifier, 101, value);
    updateDoc(modifier, 102, value);

    // commit the delete all
    modifier.commit();

    // Validate there are no docs left
    reader = IndexReader.open(dir, true);
    assertEquals(2, reader.numDocs());
    reader.close();

    modifier.close();
    dir.close();
  }

  // test rollback of deleteAll()
  public void testDeleteAllRollback() throws IOException {
    Directory dir = newDirectory();
    IndexWriter modifier = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random, MockTokenizer.WHITESPACE, false)).setMaxBufferedDocs(2)
        .setMaxBufferedDeleteTerms(2));

    int id = 0;
    int value = 100;

    for (int i = 0; i < 7; i++) {
      addDoc(modifier, ++id, value);
    }
    modifier.commit();

    addDoc(modifier, ++id, value);

    IndexReader reader = IndexReader.open(dir, true);
    assertEquals(7, reader.numDocs());
    reader.close();

    // Delete all
    modifier.deleteAll();

    // Roll it back
    modifier.rollback();
    modifier.close();

    // Validate that the docs are still there
    reader = IndexReader.open(dir, true);
    assertEquals(7, reader.numDocs());
    reader.close();

    dir.close();
  }


  // test deleteAll() w/ near real-time reader
  public void testDeleteAllNRT() throws IOException {
    Directory dir = newDirectory();
    IndexWriter modifier = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random, MockTokenizer.WHITESPACE, false)).setMaxBufferedDocs(2)
        .setMaxBufferedDeleteTerms(2));

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
    modifier.close();

    // Validate that the docs are still there
    reader = IndexReader.open(dir, true);
    assertEquals(7, reader.numDocs());
    reader.close();

    dir.close();
  }


  private void updateDoc(IndexWriter modifier, int id, int value)
      throws IOException {
    Document doc = new Document();
    doc.add(newField("content", "aaa", Field.Store.NO, Field.Index.ANALYZED));
    doc.add(newField("id", String.valueOf(id), Field.Store.YES,
        Field.Index.NOT_ANALYZED));
    doc.add(newField("value", String.valueOf(value), Field.Store.NO,
        Field.Index.NOT_ANALYZED));
    modifier.updateDocument(new Term("id", String.valueOf(id)), doc);
  }


  private void addDoc(IndexWriter modifier, int id, int value)
      throws IOException {
    Document doc = new Document();
    doc.add(newField("content", "aaa", Field.Store.NO, Field.Index.ANALYZED));
    doc.add(newField("id", String.valueOf(id), Field.Store.YES,
        Field.Index.NOT_ANALYZED));
    doc.add(newField("value", String.valueOf(value), Field.Store.NO,
        Field.Index.NOT_ANALYZED));
    modifier.addDocument(doc);
  }

  private int getHitCount(Directory dir, Term term) throws IOException {
    IndexSearcher searcher = new IndexSearcher(dir, true);
    int hitCount = searcher.search(new TermQuery(term), null, 1000).totalHits;
    searcher.close();
    return hitCount;
  }

  public void testDeletesOnDiskFull() throws IOException {
    doTestOperationsOnDiskFull(false);
  }

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
    MockDirectoryWrapper startDir = newDirectory();
    // TODO: find the resource leak that only occurs sometimes here.
    startDir.setNoDeleteOpenFile(false);
    IndexWriter writer = new IndexWriter(startDir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random, MockTokenizer.WHITESPACE, false)));
    for (int i = 0; i < 157; i++) {
      Document d = new Document();
      d.add(newField("id", Integer.toString(i), Field.Store.YES,
                      Field.Index.NOT_ANALYZED));
      d.add(newField("content", "aaa " + i, Field.Store.NO,
                      Field.Index.ANALYZED));
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
      MockDirectoryWrapper dir = new MockDirectoryWrapper(random, new RAMDirectory(startDir));
      dir.setPreventDoubleWrite(false);
      IndexWriter modifier = new IndexWriter(dir,
                                             newIndexWriterConfig(
                                                                  TEST_VERSION_CURRENT, new MockAnalyzer(random, MockTokenizer.WHITESPACE, false))
                                             .setMaxBufferedDocs(1000)
                                             .setMaxBufferedDeleteTerms(1000)
                                             .setMergeScheduler(new ConcurrentMergeScheduler()));
      ((ConcurrentMergeScheduler) modifier.getConfig().getMergeScheduler()).setSuppressExceptions();
      modifier.setInfoStream(VERBOSE ? System.out : null);

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
        } else {
          thisDiskFree = 0;
          rate = 0.0;
          if (VERBOSE) {
            System.out.println("\ncycle: same writer: unlimited disk space");
          }
          testName = "reader re-use after disk full";
        }

        dir.setMaxSizeInBytes(thisDiskFree);
        dir.setRandomIOExceptionRate(rate);

        try {
          if (0 == x) {
            int docId = 12;
            for (int i = 0; i < 13; i++) {
              if (updates) {
                Document d = new Document();
                d.add(newField("id", Integer.toString(i), Field.Store.YES,
                                Field.Index.NOT_ANALYZED));
                d.add(newField("content", "bbb " + i, Field.Store.NO,
                                Field.Index.ANALYZED));
                modifier.updateDocument(new Term("id", Integer.toString(docId)), d);
              } else { // deletes
                modifier.deleteDocuments(new Term("id", Integer.toString(docId)));
                // modifier.setNorm(docId, "contents", (float)2.0);
              }
              docId += 12;
            }
          }
          modifier.close();
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
        dir.setMaxSizeInBytes(0);
        if (!success) {
          // Must force the close else the writer can have
          // open files which cause exc in MockRAMDir.close
         
          modifier.rollback();
        }

        // If the close() succeeded, make sure there are
        // no unreferenced files.
        if (success) {
          _TestUtil.checkIndex(dir);
          TestIndexWriter.assertNoUnreferencedFiles(dir, "after writer.close");
        }
        dir.setRandomIOExceptionRate(randomIOExceptionRate);
        dir.setMaxSizeInBytes(maxSizeInBytes);

        // Finally, verify index is not corrupt, and, if
        // we succeeded, we see all docs changed, and if
        // we failed, we see either all docs or no docs
        // changed (transactional semantics):
        IndexReader newReader = null;
        try {
          newReader = IndexReader.open(dir, true);
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
          hits = searcher.search(new TermQuery(searchTerm), null, 1000).scoreDocs;
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
        searcher.close();
        newReader.close();
        if (result2 == END_COUNT) {
          break;
        }
      }
      dir.close();
      modifier.close();

      // Try again with 10 more bytes of free space:
      diskFree += 10;
    }
    startDir.close();
  }

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
          if (sawMaybe && !failed) {
            boolean seen = false;
            StackTraceElement[] trace = new Exception().getStackTrace();
            for (int i = 0; i < trace.length; i++) {
              if ("applyDeletes".equals(trace[i].getMethodName())) {
                seen = true;
                break;
              }
            }
            if (!seen) {
              // Only fail once we are no longer in applyDeletes
              failed = true;
              if (VERBOSE) {
                System.out.println("TEST: mock failure: now fail");
                new Throwable().printStackTrace(System.out);
              }
              throw new IOException("fail after applyDeletes");
            }
          }
          if (!failed) {
            StackTraceElement[] trace = new Exception().getStackTrace();
            for (int i = 0; i < trace.length; i++) {
              if ("applyDeletes".equals(trace[i].getMethodName())) {
                if (VERBOSE) {
                  System.out.println("TEST: mock failure: saw applyDeletes");
                  new Throwable().printStackTrace(System.out);
                }
                sawMaybe = true;
                break;
              }
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

    MockDirectoryWrapper dir = newDirectory();
    IndexWriter modifier = new IndexWriter(dir, newIndexWriterConfig(
                                                                     TEST_VERSION_CURRENT, new MockAnalyzer(random, MockTokenizer.WHITESPACE, false)).setMaxBufferedDeleteTerms(2).setReaderPooling(false).setMergePolicy(newLogMergePolicy()));
    modifier.setInfoStream(VERBOSE ? System.out : null);

    LogMergePolicy lmp = (LogMergePolicy) modifier.getConfig().getMergePolicy();
    lmp.setUseCompoundFile(true);

    dir.failOn(failure.reset());

    for (int i = 0; i < keywords.length; i++) {
      Document doc = new Document();
      doc.add(newField("id", keywords[i], Field.Store.YES,
                        Field.Index.NOT_ANALYZED));
      doc.add(newField("country", unindexed[i], Field.Store.YES,
                        Field.Index.NO));
      doc.add(newField("contents", unstored[i], Field.Store.NO,
                        Field.Index.ANALYZED));
      doc.add(newField("city", text[i], Field.Store.YES,
                        Field.Index.ANALYZED));
      modifier.addDocument(doc);
    }
    // flush (and commit if ac)

    if (VERBOSE) {
      System.out.println("TEST: now optimize");
    }

    modifier.optimize();
    if (VERBOSE) {
      System.out.println("TEST: now commit");
    }
    modifier.commit();

    // one of the two files hits

    Term term = new Term("city", "Amsterdam");
    int hitCount = getHitCount(dir, term);
    assertEquals(1, hitCount);

    // open the writer again (closed above)

    // delete the doc
    // max buf del terms is two, so this is buffered

    if (VERBOSE) {
      System.out.println("TEST: delete term=" + term);
    }

    modifier.deleteDocuments(term);

    // add a doc (needed for the !ac case; see below)
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
    boolean failed = false;
    try {
      if (VERBOSE) {
        System.out.println("TEST: now commit for failure");
      }
      modifier.commit();
    } catch (IOException ioe) {
      // expected
      failed = true;
    }

    assertTrue(failed);

    // The commit above failed, so we need to retry it (which will
    // succeed, because the failure is a one-shot)

    modifier.commit();

    hitCount = getHitCount(dir, term);

    // Make sure the delete was successfully flushed:
    assertEquals(0, hitCount);

    modifier.close();
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

    MockDirectoryWrapper dir = newDirectory();
    IndexWriter modifier = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random, MockTokenizer.WHITESPACE, false)));
    modifier.commit();
    dir.failOn(failure.reset());

    for (int i = 0; i < keywords.length; i++) {
      Document doc = new Document();
      doc.add(newField("id", keywords[i], Field.Store.YES,
                        Field.Index.NOT_ANALYZED));
      doc.add(newField("country", unindexed[i], Field.Store.YES,
                        Field.Index.NO));
      doc.add(newField("contents", unstored[i], Field.Store.NO,
                        Field.Index.ANALYZED));
      doc.add(newField("city", text[i], Field.Store.YES,
                        Field.Index.ANALYZED));
      try {
        modifier.addDocument(doc);
      } catch (IOException io) {
        break;
      }
    }

    modifier.close();
    TestIndexWriter.assertNoUnreferencedFiles(dir, "docsWriter.abort() failed to delete unreferenced files");
    dir.close();
  }

  public void testDeleteNullQuery() throws IOException {
    Directory dir = newDirectory();
    IndexWriter modifier = new IndexWriter(dir, new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random, MockTokenizer.WHITESPACE, false)));

    for (int i = 0; i < 5; i++) {
      addDoc(modifier, i, 2*i);
    }

    modifier.deleteDocuments(new TermQuery(new Term("nada", "nada")));
    modifier.commit();
    assertEquals(5, modifier.numDocs());
    modifier.close();
    dir.close();
  }
}
