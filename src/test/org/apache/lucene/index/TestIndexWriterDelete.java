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
import java.util.Arrays;

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockRAMDirectory;
import org.apache.lucene.util.LuceneTestCase;

public class TestIndexWriterDelete extends LuceneTestCase {

  // test the simple case
  public void testSimpleCase() throws IOException {
    String[] keywords = { "1", "2" };
    String[] unindexed = { "Netherlands", "Italy" };
    String[] unstored = { "Amsterdam has lots of bridges",
        "Venice has lots of canals" };
    String[] text = { "Amsterdam", "Venice" };

    for(int pass=0;pass<2;pass++) {
      boolean autoCommit = (0==pass);

      Directory dir = new MockRAMDirectory();
      IndexWriter modifier = new IndexWriter(dir, autoCommit,
                                             new WhitespaceAnalyzer(), true);
      modifier.setUseCompoundFile(true);
      modifier.setMaxBufferedDeleteTerms(1);

      for (int i = 0; i < keywords.length; i++) {
        Document doc = new Document();
        doc.add(new Field("id", keywords[i], Field.Store.YES,
                          Field.Index.NOT_ANALYZED));
        doc.add(new Field("country", unindexed[i], Field.Store.YES,
                          Field.Index.NO));
        doc.add(new Field("contents", unstored[i], Field.Store.NO,
                          Field.Index.ANALYZED));
        doc
          .add(new Field("city", text[i], Field.Store.YES,
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
  }

  // test when delete terms only apply to disk segments
  public void testNonRAMDelete() throws IOException {
    for(int pass=0;pass<2;pass++) {
      boolean autoCommit = (0==pass);

      Directory dir = new MockRAMDirectory();
      IndexWriter modifier = new IndexWriter(dir, autoCommit,
                                             new WhitespaceAnalyzer(), true);
      modifier.setMaxBufferedDocs(2);
      modifier.setMaxBufferedDeleteTerms(2);

      int id = 0;
      int value = 100;

      for (int i = 0; i < 7; i++) {
        addDoc(modifier, ++id, value);
      }
      modifier.commit();

      assertEquals(0, modifier.getNumBufferedDocuments());
      assertTrue(0 < modifier.getSegmentCount());

      modifier.commit();

      IndexReader reader = IndexReader.open(dir);
      assertEquals(7, reader.numDocs());
      reader.close();

      modifier.deleteDocuments(new Term("value", String.valueOf(value)));

      modifier.commit();

      reader = IndexReader.open(dir);
      assertEquals(0, reader.numDocs());
      reader.close();
      modifier.close();
      dir.close();
    }
  }

  public void testMaxBufferedDeletes() throws IOException {
    for(int pass=0;pass<2;pass++) {
      boolean autoCommit = (0==pass);
      Directory dir = new MockRAMDirectory();
      IndexWriter writer = new IndexWriter(dir, autoCommit,
                                           new WhitespaceAnalyzer(), true);
      writer.setMaxBufferedDeleteTerms(1);
      writer.deleteDocuments(new Term("foobar", "1"));
      writer.deleteDocuments(new Term("foobar", "1"));
      writer.deleteDocuments(new Term("foobar", "1"));
      assertEquals(3, writer.getFlushDeletesCount());
      writer.close();
      dir.close();
    }
  }

  // test when delete terms only apply to ram segments
  public void testRAMDeletes() throws IOException {
    for(int pass=0;pass<2;pass++) {
      for(int t=0;t<2;t++) {
        boolean autoCommit = (0==pass);
        Directory dir = new MockRAMDirectory();
        IndexWriter modifier = new IndexWriter(dir, autoCommit,
                                               new WhitespaceAnalyzer(), true);
        modifier.setMaxBufferedDocs(4);
        modifier.setMaxBufferedDeleteTerms(4);

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
        modifier.flush();

        modifier.commit();

        IndexReader reader = IndexReader.open(dir);
        assertEquals(1, reader.numDocs());

        int hitCount = getHitCount(dir, new Term("id", String.valueOf(id)));
        assertEquals(1, hitCount);
        reader.close();
        modifier.close();
        dir.close();
      }
    }
  }

  // test when delete terms apply to both disk and ram segments
  public void testBothDeletes() throws IOException {
    for(int pass=0;pass<2;pass++) {
      boolean autoCommit = (0==pass);

      Directory dir = new MockRAMDirectory();
      IndexWriter modifier = new IndexWriter(dir, autoCommit,
                                             new WhitespaceAnalyzer(), true);
      modifier.setMaxBufferedDocs(100);
      modifier.setMaxBufferedDeleteTerms(100);

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

      IndexReader reader = IndexReader.open(dir);
      assertEquals(5, reader.numDocs());
      modifier.close();
    }
  }

  // test that batched delete terms are flushed together
  public void testBatchDeletes() throws IOException {
    for(int pass=0;pass<2;pass++) {
      boolean autoCommit = (0==pass);
      Directory dir = new MockRAMDirectory();
      IndexWriter modifier = new IndexWriter(dir, autoCommit,
                                             new WhitespaceAnalyzer(), true);
      modifier.setMaxBufferedDocs(2);
      modifier.setMaxBufferedDeleteTerms(2);

      int id = 0;
      int value = 100;

      for (int i = 0; i < 7; i++) {
        addDoc(modifier, ++id, value);
      }
      modifier.commit();

      IndexReader reader = IndexReader.open(dir);
      assertEquals(7, reader.numDocs());
      reader.close();
      
      id = 0;
      modifier.deleteDocuments(new Term("id", String.valueOf(++id)));
      modifier.deleteDocuments(new Term("id", String.valueOf(++id)));

      modifier.commit();

      reader = IndexReader.open(dir);
      assertEquals(5, reader.numDocs());
      reader.close();

      Term[] terms = new Term[3];
      for (int i = 0; i < terms.length; i++) {
        terms[i] = new Term("id", String.valueOf(++id));
      }
      modifier.deleteDocuments(terms);
      modifier.commit();
      reader = IndexReader.open(dir);
      assertEquals(2, reader.numDocs());
      reader.close();

      modifier.close();
      dir.close();
    }
  }

  private void addDoc(IndexWriter modifier, int id, int value)
      throws IOException {
    Document doc = new Document();
    doc.add(new Field("content", "aaa", Field.Store.NO, Field.Index.ANALYZED));
    doc.add(new Field("id", String.valueOf(id), Field.Store.YES,
        Field.Index.NOT_ANALYZED));
    doc.add(new Field("value", String.valueOf(value), Field.Store.NO,
        Field.Index.NOT_ANALYZED));
    modifier.addDocument(doc);
  }

  private int getHitCount(Directory dir, Term term) throws IOException {
    IndexSearcher searcher = new IndexSearcher(dir);
    int hitCount = searcher.search(new TermQuery(term), null, 1000).totalHits;
    searcher.close();
    return hitCount;
  }

  public void testDeletesOnDiskFull() throws IOException {
    testOperationsOnDiskFull(false);
  }

  public void testUpdatesOnDiskFull() throws IOException {
    testOperationsOnDiskFull(true);
  }

  /**
   * Make sure if modifier tries to commit but hits disk full that modifier
   * remains consistent and usable. Similar to TestIndexReader.testDiskFull().
   */
  private void testOperationsOnDiskFull(boolean updates) throws IOException {

    boolean debug = false;
    Term searchTerm = new Term("content", "aaa");
    int START_COUNT = 157;
    int END_COUNT = 144;

    for(int pass=0;pass<2;pass++) {
      boolean autoCommit = (0==pass);

      // First build up a starting index:
      MockRAMDirectory startDir = new MockRAMDirectory();
      IndexWriter writer = new IndexWriter(startDir, autoCommit,
                                           new WhitespaceAnalyzer(), true);
      for (int i = 0; i < 157; i++) {
        Document d = new Document();
        d.add(new Field("id", Integer.toString(i), Field.Store.YES,
                        Field.Index.NOT_ANALYZED));
        d.add(new Field("content", "aaa " + i, Field.Store.NO,
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
        MockRAMDirectory dir = new MockRAMDirectory(startDir);
        dir.setPreventDoubleWrite(false);
        IndexWriter modifier = new IndexWriter(dir, autoCommit,
                                               new WhitespaceAnalyzer());

        modifier.setMaxBufferedDocs(1000); // use flush or close
        modifier.setMaxBufferedDeleteTerms(1000); // use flush or close

        // For each disk size, first try to commit against
        // dir that will hit random IOExceptions & disk
        // full; after, give it infinite disk space & turn
        // off random IOExceptions & retry w/ same reader:
        boolean success = false;

        for (int x = 0; x < 2; x++) {

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
            if (debug) {
              System.out.println("\ncycle: " + diskFree + " bytes");
            }
            testName = "disk full during reader.close() @ " + thisDiskFree
              + " bytes";
          } else {
            thisDiskFree = 0;
            rate = 0.0;
            if (debug) {
              System.out.println("\ncycle: same writer: unlimited disk space");
            }
            testName = "reader re-use after disk full";
          }

          dir.setMaxSizeInBytes(thisDiskFree);
          dir.setRandomIOExceptionRate(rate, diskFree);

          try {
            if (0 == x) {
              int docId = 12;
              for (int i = 0; i < 13; i++) {
                if (updates) {
                  Document d = new Document();
                  d.add(new Field("id", Integer.toString(i), Field.Store.YES,
                                  Field.Index.NOT_ANALYZED));
                  d.add(new Field("content", "bbb " + i, Field.Store.NO,
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
            if (debug) {
              System.out.println("  hit IOException: " + e);
              e.printStackTrace(System.out);
            }
            err = e;
            if (1 == x) {
              e.printStackTrace();
              fail(testName + " hit IOException after disk space was freed up");
            }
          }

          // If the close() succeeded, make sure there are
          // no unreferenced files.
          if (success)
            TestIndexWriter.assertNoUnreferencedFiles(dir, "after writer.close");

          // Finally, verify index is not corrupt, and, if
          // we succeeded, we see all docs changed, and if
          // we failed, we see either all docs or no docs
          // changed (transactional semantics):
          IndexReader newReader = null;
          try {
            newReader = IndexReader.open(dir);
          }
          catch (IOException e) {
            e.printStackTrace();
            fail(testName
                 + ":exception when creating IndexReader after disk full during close: "
                 + e);
          }

          IndexSearcher searcher = new IndexSearcher(newReader);
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

        // Try again with 10 more bytes of free space:
        diskFree += 10;
      }
    }
  }

  // This test tests that buffered deletes are cleared when
  // an Exception is hit during flush.
  public void testErrorAfterApplyDeletes() throws IOException {
    
    MockRAMDirectory.Failure failure = new MockRAMDirectory.Failure() {
        boolean sawMaybe = false;
        boolean failed = false;
        public MockRAMDirectory.Failure reset() {
          sawMaybe = false;
          failed = false;
          return this;
        }
        public void eval(MockRAMDirectory dir)  throws IOException {
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
              throw new IOException("fail after applyDeletes");
            }
          }
          if (!failed) {
            StackTraceElement[] trace = new Exception().getStackTrace();
            for (int i = 0; i < trace.length; i++) {
              if ("applyDeletes".equals(trace[i].getMethodName())) {
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

    for(int pass=0;pass<2;pass++) {
      boolean autoCommit = (0==pass);
      MockRAMDirectory dir = new MockRAMDirectory();
      IndexWriter modifier = new IndexWriter(dir, autoCommit,
                                             new WhitespaceAnalyzer(), true);
      modifier.setUseCompoundFile(true);
      modifier.setMaxBufferedDeleteTerms(2);

      dir.failOn(failure.reset());

      for (int i = 0; i < keywords.length; i++) {
        Document doc = new Document();
        doc.add(new Field("id", keywords[i], Field.Store.YES,
                          Field.Index.NOT_ANALYZED));
        doc.add(new Field("country", unindexed[i], Field.Store.YES,
                          Field.Index.NO));
        doc.add(new Field("contents", unstored[i], Field.Store.NO,
                          Field.Index.ANALYZED));
        doc.add(new Field("city", text[i], Field.Store.YES,
                          Field.Index.ANALYZED));
        modifier.addDocument(doc);
      }
      // flush (and commit if ac)

      modifier.optimize();
      modifier.commit();

      // one of the two files hits

      Term term = new Term("city", "Amsterdam");
      int hitCount = getHitCount(dir, term);
      assertEquals(1, hitCount);

      // open the writer again (closed above)

      // delete the doc
      // max buf del terms is two, so this is buffered

      modifier.deleteDocuments(term);

      // add a doc (needed for the !ac case; see below)
      // doc remains buffered

      Document doc = new Document();
      modifier.addDocument(doc);

      // commit the changes, the buffered deletes, and the new doc

      // The failure object will fail on the first write after the del
      // file gets created when processing the buffered delete

      // in the ac case, this will be when writing the new segments
      // files so we really don't need the new doc, but it's harmless

      // in the !ac case, a new segments file won't be created but in
      // this case, creation of the cfs file happens next so we need
      // the doc (to test that it's okay that we don't lose deletes if
      // failing while creating the cfs file)

      boolean failed = false;
      try {
        modifier.commit();
      } catch (IOException ioe) {
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
  }

  // This test tests that the files created by the docs writer before
  // a segment is written are cleaned up if there's an i/o error

  public void testErrorInDocsWriterAdd() throws IOException {
    
    MockRAMDirectory.Failure failure = new MockRAMDirectory.Failure() {
        boolean failed = false;
        public MockRAMDirectory.Failure reset() {
          failed = false;
          return this;
        }
        public void eval(MockRAMDirectory dir)  throws IOException {
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

    for(int pass=0;pass<2;pass++) {
      boolean autoCommit = (0==pass);
      MockRAMDirectory dir = new MockRAMDirectory();
      IndexWriter modifier = new IndexWriter(dir, autoCommit,
                                             new WhitespaceAnalyzer(), true);

      dir.failOn(failure.reset());

      for (int i = 0; i < keywords.length; i++) {
        Document doc = new Document();
        doc.add(new Field("id", keywords[i], Field.Store.YES,
                          Field.Index.NOT_ANALYZED));
        doc.add(new Field("country", unindexed[i], Field.Store.YES,
                          Field.Index.NO));
        doc.add(new Field("contents", unstored[i], Field.Store.NO,
                          Field.Index.ANALYZED));
        doc.add(new Field("city", text[i], Field.Store.YES,
                          Field.Index.ANALYZED));
        try {
          modifier.addDocument(doc);
        } catch (IOException io) {
          break;
        }
      }

      String[] startFiles = dir.list();
      SegmentInfos infos = new SegmentInfos();
      infos.read(dir);
      new IndexFileDeleter(dir, new KeepOnlyLastCommitDeletionPolicy(), infos, null, null);
      String[] endFiles = dir.list();

      if (!Arrays.equals(startFiles, endFiles)) {
        fail("docswriter abort() failed to delete unreferenced files:\n  before delete:\n    "
             + arrayToString(startFiles) + "\n  after delete:\n    "
             + arrayToString(endFiles));
      }

      modifier.close();

    }

  }

  private String arrayToString(String[] l) {
    String s = "";
    for (int i = 0; i < l.length; i++) {
      if (i > 0) {
        s += "\n    ";
      }
      s += l[i];
    }
    return s;
  }
}
