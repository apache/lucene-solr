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
import java.io.Reader;
import java.io.File;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Random;

import org.apache.lucene.util.LuceneTestCase;

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.analysis.WhitespaceTokenizer;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util._TestUtil;

import org.apache.lucene.store.MockRAMDirectory;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.SingleInstanceLockFactory;

/**
 *
 * @version $Id$
 */
public class TestIndexWriter extends LuceneTestCase
{
    public void testDocCount() throws IOException
    {
        Directory dir = new RAMDirectory();

        IndexWriter writer = null;
        IndexReader reader = null;
        int i;

        IndexWriter.setDefaultWriteLockTimeout(2000);
        assertEquals(2000, IndexWriter.getDefaultWriteLockTimeout());

        writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);

        IndexWriter.setDefaultWriteLockTimeout(1000);

        // add 100 documents
        for (i = 0; i < 100; i++) {
            addDoc(writer);
        }
        assertEquals(100, writer.docCount());
        writer.close();

        // delete 40 documents
        reader = IndexReader.open(dir);
        for (i = 0; i < 40; i++) {
            reader.deleteDocument(i);
        }
        reader.close();

        // test doc count before segments are merged/index is optimized
        writer = new IndexWriter(dir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
        assertEquals(100, writer.docCount());
        writer.close();

        reader = IndexReader.open(dir);
        assertEquals(100, reader.maxDoc());
        assertEquals(60, reader.numDocs());
        reader.close();

        // optimize the index and check that the new doc count is correct
        writer = new IndexWriter(dir, true, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
        writer.optimize();
        assertEquals(60, writer.docCount());
        writer.close();

        // check that the index reader gives the same numbers.
        reader = IndexReader.open(dir);
        assertEquals(60, reader.maxDoc());
        assertEquals(60, reader.numDocs());
        reader.close();

        // make sure opening a new index for create over
        // this existing one works correctly:
        writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
        assertEquals(0, writer.docCount());
        writer.close();
    }

    private void addDoc(IndexWriter writer) throws IOException
    {
        Document doc = new Document();
        doc.add(new Field("content", "aaa", Field.Store.NO, Field.Index.TOKENIZED));
        writer.addDocument(doc);
    }

    private void addDocWithIndex(IndexWriter writer, int index) throws IOException
    {
        Document doc = new Document();
        doc.add(new Field("content", "aaa " + index, Field.Store.YES, Field.Index.TOKENIZED));
        doc.add(new Field("id", "" + index, Field.Store.YES, Field.Index.TOKENIZED));
        writer.addDocument(doc);
    }

    /*
      Test: make sure when we run out of disk space or hit
      random IOExceptions in any of the addIndexes(*) calls
      that 1) index is not corrupt (searcher can open/search
      it) and 2) transactional semantics are followed:
      either all or none of the incoming documents were in
      fact added.
    */
    public void testAddIndexOnDiskFull() throws IOException
    {
      int START_COUNT = 57;
      int NUM_DIR = 50;
      int END_COUNT = START_COUNT + NUM_DIR*25;

      boolean debug = false;

      // Build up a bunch of dirs that have indexes which we
      // will then merge together by calling addIndexes(*):
      Directory[] dirs = new Directory[NUM_DIR];
      long inputDiskUsage = 0;
      for(int i=0;i<NUM_DIR;i++) {
        dirs[i] = new RAMDirectory();
        IndexWriter writer  = new IndexWriter(dirs[i], new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
        for(int j=0;j<25;j++) {
          addDocWithIndex(writer, 25*i+j);
        }
        writer.close();
        String[] files = dirs[i].list();
        for(int j=0;j<files.length;j++) {
          inputDiskUsage += dirs[i].fileLength(files[j]);
        }
      }

      // Now, build a starting index that has START_COUNT docs.  We
      // will then try to addIndexes into a copy of this:
      RAMDirectory startDir = new RAMDirectory();
      IndexWriter writer = new IndexWriter(startDir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
      for(int j=0;j<START_COUNT;j++) {
        addDocWithIndex(writer, j);
      }
      writer.close();

      // Make sure starting index seems to be working properly:
      Term searchTerm = new Term("content", "aaa");        
      IndexReader reader = IndexReader.open(startDir);
      assertEquals("first docFreq", 57, reader.docFreq(searchTerm));

      IndexSearcher searcher = new IndexSearcher(reader);
      Hits hits = searcher.search(new TermQuery(searchTerm));
      assertEquals("first number of hits", 57, hits.length());
      searcher.close();
      reader.close();

      // Iterate with larger and larger amounts of free
      // disk space.  With little free disk space,
      // addIndexes will certainly run out of space &
      // fail.  Verify that when this happens, index is
      // not corrupt and index in fact has added no
      // documents.  Then, we increase disk space by 2000
      // bytes each iteration.  At some point there is
      // enough free disk space and addIndexes should
      // succeed and index should show all documents were
      // added.

      // String[] files = startDir.list();
      long diskUsage = startDir.sizeInBytes();

      long startDiskUsage = 0;
      String[] files = startDir.list();
      for(int i=0;i<files.length;i++) {
        startDiskUsage += startDir.fileLength(files[i]);
      }

      for(int iter=0;iter<6;iter++) {

        if (debug)
          System.out.println("TEST: iter=" + iter);

        // Start with 100 bytes more than we are currently using:
        long diskFree = diskUsage+100;

        boolean autoCommit = iter % 2 == 0;
        int method = iter/2;

        boolean success = false;
        boolean done = false;

        String methodName;
        if (0 == method) {
          methodName = "addIndexes(Directory[])";
        } else if (1 == method) {
          methodName = "addIndexes(IndexReader[])";
        } else {
          methodName = "addIndexesNoOptimize(Directory[])";
        }

        while(!done) {

          // Make a new dir that will enforce disk usage:
          MockRAMDirectory dir = new MockRAMDirectory(startDir);
          writer = new IndexWriter(dir, autoCommit, new WhitespaceAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
          IOException err = null;

          MergeScheduler ms = writer.getMergeScheduler();
          for(int x=0;x<2;x++) {
            if (ms instanceof ConcurrentMergeScheduler)
              // This test intentionally produces exceptions
              // in the threads that CMS launches; we don't
              // want to pollute test output with these.
              if (0 == x)
                ((ConcurrentMergeScheduler) ms).setSuppressExceptions();
              else
                ((ConcurrentMergeScheduler) ms).clearSuppressExceptions();

            // Two loops: first time, limit disk space &
            // throw random IOExceptions; second time, no
            // disk space limit:

            double rate = 0.05;
            double diskRatio = ((double) diskFree)/diskUsage;
            long thisDiskFree;

            String testName = null;

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
              if (debug)
                testName = "disk full test " + methodName + " with disk full at " + diskFree + " bytes autoCommit=" + autoCommit;
            } else {
              thisDiskFree = 0;
              rate = 0.0;
              if (debug)
                testName = "disk full test " + methodName + " with unlimited disk space autoCommit=" + autoCommit;
            }

            if (debug)
              System.out.println("\ncycle: " + testName);

            dir.setMaxSizeInBytes(thisDiskFree);
            dir.setRandomIOExceptionRate(rate, diskFree);

            try {

              if (0 == method) {
                writer.addIndexes(dirs);
              } else if (1 == method) {
                IndexReader readers[] = new IndexReader[dirs.length];
                for(int i=0;i<dirs.length;i++) {
                  readers[i] = IndexReader.open(dirs[i]);
                }
                try {
                  writer.addIndexes(readers);
                } finally {
                  for(int i=0;i<dirs.length;i++) {
                    readers[i].close();
                  }
                }
              } else {
                writer.addIndexesNoOptimize(dirs);
              }

              success = true;
              if (debug) {
                System.out.println("  success!");
              }

              if (0 == x) {
                done = true;
              }

            } catch (IOException e) {
              success = false;
              err = e;
              if (debug) {
                System.out.println("  hit IOException: " + e);
                e.printStackTrace(System.out);
              }

              if (1 == x) {
                e.printStackTrace(System.out);
                fail(methodName + " hit IOException after disk space was freed up");
              }
            }

            // Make sure all threads from
            // ConcurrentMergeScheduler are done
            _TestUtil.syncConcurrentMerges(writer);

            if (autoCommit) {

              // Whether we succeeded or failed, check that
              // all un-referenced files were in fact
              // deleted (ie, we did not create garbage).
              // Only check this when autoCommit is true:
              // when it's false, it's expected that there
              // are unreferenced files (ie they won't be
              // referenced until the "commit on close").
              // Just create a new IndexFileDeleter, have it
              // delete unreferenced files, then verify that
              // in fact no files were deleted:

              String successStr;
              if (success) {
                successStr = "success";
              } else {
                successStr = "IOException";
              }
              String message = methodName + " failed to delete unreferenced files after " + successStr + " (" + diskFree + " bytes)";
              assertNoUnreferencedFiles(dir, message);
            }

            if (debug) {
              System.out.println("  now test readers");
            }

            // Finally, verify index is not corrupt, and, if
            // we succeeded, we see all docs added, and if we
            // failed, we see either all docs or no docs added
            // (transactional semantics):
            try {
              reader = IndexReader.open(dir);
            } catch (IOException e) {
              e.printStackTrace(System.out);
              fail(testName + ": exception when creating IndexReader: " + e);
            }
            int result = reader.docFreq(searchTerm);
            if (success) {
              if (autoCommit && result != END_COUNT) {
                fail(testName + ": method did not throw exception but docFreq('aaa') is " + result + " instead of expected " + END_COUNT);
              } else if (!autoCommit && result != START_COUNT) {
                fail(testName + ": method did not throw exception but docFreq('aaa') is " + result + " instead of expected " + START_COUNT + " [autoCommit = false]");
              }
            } else {
              // On hitting exception we still may have added
              // all docs:
              if (result != START_COUNT && result != END_COUNT) {
                err.printStackTrace(System.out);
                fail(testName + ": method did throw exception but docFreq('aaa') is " + result + " instead of expected " + START_COUNT + " or " + END_COUNT);
              }
            }

            searcher = new IndexSearcher(reader);
            try {
              hits = searcher.search(new TermQuery(searchTerm));
            } catch (IOException e) {
              e.printStackTrace(System.out);
              fail(testName + ": exception when searching: " + e);
            }
            int result2 = hits.length();
            if (success) {
              if (result2 != result) {
                fail(testName + ": method did not throw exception but hits.length for search on term 'aaa' is " + result2 + " instead of expected " + result);
              }
            } else {
              // On hitting exception we still may have added
              // all docs:
              if (result2 != result) {
                err.printStackTrace(System.out);
                fail(testName + ": method did throw exception but hits.length for search on term 'aaa' is " + result2 + " instead of expected " + result);
              }
            }

            searcher.close();
            reader.close();
            if (debug) {
              System.out.println("  count is " + result);
            }

            if (done || result == END_COUNT) {
              break;
            }
          }

          if (debug) {
            System.out.println("  start disk = " + startDiskUsage + "; input disk = " + inputDiskUsage + "; max used = " + dir.getMaxUsedSizeInBytes());
          }

          if (done) {
            // Javadocs state that temp free Directory space
            // required is at most 2X total input size of
            // indices so let's make sure:
            assertTrue("max free Directory space required exceeded 1X the total input index sizes during " + methodName +
                       ": max temp usage = " + (dir.getMaxUsedSizeInBytes()-startDiskUsage) + " bytes; " +
                       "starting disk usage = " + startDiskUsage + " bytes; " +
                       "input index disk usage = " + inputDiskUsage + " bytes",
                       (dir.getMaxUsedSizeInBytes()-startDiskUsage) < 2*(startDiskUsage + inputDiskUsage));
          }

          writer.close();

          // Wait for all BG threads to finish else
          // dir.close() will throw IOException because
          // there are still open files
          _TestUtil.syncConcurrentMerges(ms);

          dir.close();

          // Try again with 2000 more bytes of free space:
          diskFree += 2000;
        }
      }

      startDir.close();
    }

    /*
     * Make sure IndexWriter cleans up on hitting a disk
     * full exception in addDocument.
     */
    public void testAddDocumentOnDiskFull() throws IOException {

      boolean debug = false;

      for(int pass=0;pass<3;pass++) {
        if (debug)
          System.out.println("TEST: pass=" + pass);
        boolean autoCommit = pass == 0;
        boolean doAbort = pass == 2;
        long diskFree = 200;
        while(true) {
          if (debug)
            System.out.println("TEST: cycle: diskFree=" + diskFree);
          MockRAMDirectory dir = new MockRAMDirectory();
          dir.setMaxSizeInBytes(diskFree);
          IndexWriter writer = new IndexWriter(dir, autoCommit, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);

          MergeScheduler ms = writer.getMergeScheduler();
          if (ms instanceof ConcurrentMergeScheduler)
            // This test intentionally produces exceptions
            // in the threads that CMS launches; we don't
            // want to pollute test output with these.
            ((ConcurrentMergeScheduler) ms).setSuppressExceptions();

          boolean hitError = false;
          try {
            for(int i=0;i<200;i++) {
              addDoc(writer);
            }
          } catch (IOException e) {
            if (debug) {
              System.out.println("TEST: exception on addDoc");
              e.printStackTrace(System.out);
            }
            hitError = true;
          }

          if (hitError) {
            if (doAbort) {
              writer.abort();
            } else {
              try {
                writer.close();
              } catch (IOException e) {
                if (debug) {
                  System.out.println("TEST: exception on close");
                  e.printStackTrace(System.out);
                }
                dir.setMaxSizeInBytes(0);
                writer.close();
              }
            }

            _TestUtil.syncConcurrentMerges(ms);

            assertNoUnreferencedFiles(dir, "after disk full during addDocument with autoCommit=" + autoCommit);

            // Make sure reader can open the index:
            IndexReader.open(dir).close();

            dir.close();

            // Now try again w/ more space:
            diskFree += 500;
          } else {
            _TestUtil.syncConcurrentMerges(writer);
            dir.close();
            break;
          }
        }
      }
    }                                               

    public static void assertNoUnreferencedFiles(Directory dir, String message) throws IOException {
      String[] startFiles = dir.list();
      SegmentInfos infos = new SegmentInfos();
      infos.read(dir);
      new IndexFileDeleter(dir, new KeepOnlyLastCommitDeletionPolicy(), infos, null, null);
      String[] endFiles = dir.list();

      Arrays.sort(startFiles);
      Arrays.sort(endFiles);

      if (!Arrays.equals(startFiles, endFiles)) {
        fail(message + ": before delete:\n    " + arrayToString(startFiles) + "\n  after delete:\n    " + arrayToString(endFiles));
      }
    }

    /**
     * Make sure we skip wicked long terms.
    */
    public void testWickedLongTerm() throws IOException {
      RAMDirectory dir = new RAMDirectory();
      IndexWriter writer  = new IndexWriter(dir, new StandardAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);

      char[] chars = new char[16383];
      Arrays.fill(chars, 'x');
      Document doc = new Document();
      final String bigTerm = new String(chars);

      // Max length term is 16383, so this contents produces
      // a too-long term:
      String contents = "abc xyz x" + bigTerm + " another term";
      doc.add(new Field("content", contents, Field.Store.NO, Field.Index.TOKENIZED));
      writer.addDocument(doc);

      // Make sure we can add another normal document
      doc = new Document();
      doc.add(new Field("content", "abc bbb ccc", Field.Store.NO, Field.Index.TOKENIZED));
      writer.addDocument(doc);
      writer.close();

      IndexReader reader = IndexReader.open(dir);

      // Make sure all terms < max size were indexed
      assertEquals(2, reader.docFreq(new Term("content", "abc")));
      assertEquals(1, reader.docFreq(new Term("content", "bbb")));
      assertEquals(1, reader.docFreq(new Term("content", "term")));
      assertEquals(1, reader.docFreq(new Term("content", "another")));

      // Make sure position is still incremented when
      // massive term is skipped:
      TermPositions tps = reader.termPositions(new Term("content", "another"));
      assertTrue(tps.next());
      assertEquals(1, tps.freq());
      assertEquals(3, tps.nextPosition());

      // Make sure the doc that has the massive term is in
      // the index:
      assertEquals("document with wicked long term should is not in the index!", 2, reader.numDocs());

      reader.close();

      // Make sure we can add a document with exactly the
      // maximum length term, and search on that term:
      doc = new Document();
      doc.add(new Field("content", bigTerm, Field.Store.NO, Field.Index.TOKENIZED));
      StandardAnalyzer sa = new StandardAnalyzer();
      sa.setMaxTokenLength(100000);
      writer  = new IndexWriter(dir, sa, IndexWriter.MaxFieldLength.LIMITED);
      writer.addDocument(doc);
      writer.close();
      reader = IndexReader.open(dir);
      assertEquals(1, reader.docFreq(new Term("content", bigTerm)));
      reader.close();

      dir.close();
    }

    public void testOptimizeMaxNumSegments() throws IOException {

      MockRAMDirectory dir = new MockRAMDirectory();

      final Document doc = new Document();
      doc.add(new Field("content", "aaa", Field.Store.YES, Field.Index.TOKENIZED));

      for(int numDocs=38;numDocs<500;numDocs += 38) {
        IndexWriter writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
        LogDocMergePolicy ldmp = new LogDocMergePolicy();
        ldmp.setMinMergeDocs(1);
        writer.setMergePolicy(ldmp);
        writer.setMergeFactor(5);
        writer.setMaxBufferedDocs(2);
        for(int j=0;j<numDocs;j++)
          writer.addDocument(doc);
        writer.close();

        SegmentInfos sis = new SegmentInfos();
        sis.read(dir);
        final int segCount = sis.size();

        writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
        writer.setMergePolicy(ldmp);
        writer.setMergeFactor(5);
        writer.optimize(3);
        writer.close();

        sis = new SegmentInfos();
        sis.read(dir);
        final int optSegCount = sis.size();

        if (segCount < 3)
          assertEquals(segCount, optSegCount);
        else
          assertEquals(3, optSegCount);
      }
    }

    public void testOptimizeMaxNumSegments2() throws IOException {
      MockRAMDirectory dir = new MockRAMDirectory();

      final Document doc = new Document();
      doc.add(new Field("content", "aaa", Field.Store.YES, Field.Index.TOKENIZED));

      IndexWriter writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
      LogDocMergePolicy ldmp = new LogDocMergePolicy();
      ldmp.setMinMergeDocs(1);
      writer.setMergePolicy(ldmp);
      writer.setMergeFactor(4);
      writer.setMaxBufferedDocs(2);

      for(int iter=0;iter<10;iter++) {
        for(int i=0;i<19;i++)
          writer.addDocument(doc);

        ((ConcurrentMergeScheduler) writer.getMergeScheduler()).sync();
        writer.commit();

        SegmentInfos sis = new SegmentInfos();
        sis.read(dir);

        final int segCount = sis.size();

        writer.optimize(7);
        writer.commit();

        sis = new SegmentInfos();
        ((ConcurrentMergeScheduler) writer.getMergeScheduler()).sync();
        sis.read(dir);
        final int optSegCount = sis.size();

        if (segCount < 7)
          assertEquals(segCount, optSegCount);
        else
          assertEquals(7, optSegCount);
      }
    }

    /**
     * Make sure optimize doesn't use any more than 1X
     * starting index size as its temporary free space
     * required.
     */
    public void testOptimizeTempSpaceUsage() throws IOException {
    
      MockRAMDirectory dir = new MockRAMDirectory();
      IndexWriter writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
      for(int j=0;j<500;j++) {
        addDocWithIndex(writer, j);
      }
      writer.close();

      long startDiskUsage = 0;
      String[] files = dir.list();
      for(int i=0;i<files.length;i++) {
        startDiskUsage += dir.fileLength(files[i]);
      }

      dir.resetMaxUsedSizeInBytes();
      writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
      writer.optimize();
      writer.close();
      long maxDiskUsage = dir.getMaxUsedSizeInBytes();

      assertTrue("optimized used too much temporary space: starting usage was " + startDiskUsage + " bytes; max temp usage was " + maxDiskUsage + " but should have been " + (2*startDiskUsage) + " (= 2X starting usage)",
                 maxDiskUsage <= 2*startDiskUsage);
      dir.close();
    }

    static String arrayToString(String[] l) {
      String s = "";
      for(int i=0;i<l.length;i++) {
        if (i > 0) {
          s += "\n    ";
        }
        s += l[i];
      }
      return s;
    }

    // Make sure we can open an index for create even when a
    // reader holds it open (this fails pre lock-less
    // commits on windows):
    public void testCreateWithReader() throws IOException {
        String tempDir = System.getProperty("java.io.tmpdir");
        if (tempDir == null)
            throw new IOException("java.io.tmpdir undefined, cannot run test");
        File indexDir = new File(tempDir, "lucenetestindexwriter");

        try {
          Directory dir = FSDirectory.getDirectory(indexDir);

          // add one document & close writer
          IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
          addDoc(writer);
          writer.close();

          // now open reader:
          IndexReader reader = IndexReader.open(dir);
          assertEquals("should be one document", reader.numDocs(), 1);

          // now open index for create:
          writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
          assertEquals("should be zero documents", writer.docCount(), 0);
          addDoc(writer);
          writer.close();

          assertEquals("should be one document", reader.numDocs(), 1);
          IndexReader reader2 = IndexReader.open(dir);
          assertEquals("should be one document", reader2.numDocs(), 1);
          reader.close();
          reader2.close();
        } finally {
          rmDir(indexDir);
        }
    }


    // Same test as above, but use IndexWriter constructor
    // that takes File:
    public void testCreateWithReader2() throws IOException {
        String tempDir = System.getProperty("java.io.tmpdir");
        if (tempDir == null)
            throw new IOException("java.io.tmpdir undefined, cannot run test");
        File indexDir = new File(tempDir, "lucenetestindexwriter");
        try {
          // add one document & close writer
          IndexWriter writer = new IndexWriter(indexDir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
          addDoc(writer);
          writer.close();

          // now open reader:
          IndexReader reader = IndexReader.open(indexDir);
          assertEquals("should be one document", reader.numDocs(), 1);

          // now open index for create:
          writer = new IndexWriter(indexDir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
          assertEquals("should be zero documents", writer.docCount(), 0);
          addDoc(writer);
          writer.close();

          assertEquals("should be one document", reader.numDocs(), 1);
          IndexReader reader2 = IndexReader.open(indexDir);
          assertEquals("should be one document", reader2.numDocs(), 1);
          reader.close();
          reader2.close();
        } finally {
          rmDir(indexDir);
        }
    }

    // Same test as above, but use IndexWriter constructor
    // that takes String:
    public void testCreateWithReader3() throws IOException {
        String tempDir = System.getProperty("tempDir");
        if (tempDir == null)
            throw new IOException("java.io.tmpdir undefined, cannot run test");

        String dirName = tempDir + "/lucenetestindexwriter";
        try {

          // add one document & close writer
          IndexWriter writer = new IndexWriter(dirName, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
          addDoc(writer);
          writer.close();

          // now open reader:
          IndexReader reader = IndexReader.open(dirName);
          assertEquals("should be one document", reader.numDocs(), 1);

          // now open index for create:
          writer = new IndexWriter(dirName, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
          assertEquals("should be zero documents", writer.docCount(), 0);
          addDoc(writer);
          writer.close();

          assertEquals("should be one document", reader.numDocs(), 1);
          IndexReader reader2 = IndexReader.open(dirName);
          assertEquals("should be one document", reader2.numDocs(), 1);
          reader.close();
          reader2.close();
        } finally {
          rmDir(new File(dirName));
        }
    }

    // Simulate a writer that crashed while writing segments
    // file: make sure we can still open the index (ie,
    // gracefully fallback to the previous segments file),
    // and that we can add to the index:
    public void testSimulatedCrashedWriter() throws IOException {
        Directory dir = new RAMDirectory();

        IndexWriter writer = null;

        writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);

        // add 100 documents
        for (int i = 0; i < 100; i++) {
            addDoc(writer);
        }

        // close
        writer.close();

        long gen = SegmentInfos.getCurrentSegmentGeneration(dir);
        assertTrue("segment generation should be > 1 but got " + gen, gen > 1);

        // Make the next segments file, with last byte
        // missing, to simulate a writer that crashed while
        // writing segments file:
        String fileNameIn = SegmentInfos.getCurrentSegmentFileName(dir);
        String fileNameOut = IndexFileNames.fileNameFromGeneration(IndexFileNames.SEGMENTS,
                                                                   "",
                                                                   1+gen);
        IndexInput in = dir.openInput(fileNameIn);
        IndexOutput out = dir.createOutput(fileNameOut);
        long length = in.length();
        for(int i=0;i<length-1;i++) {
          out.writeByte(in.readByte());
        }
        in.close();
        out.close();

        IndexReader reader = null;
        try {
          reader = IndexReader.open(dir);
        } catch (Exception e) {
          fail("reader failed to open on a crashed index");
        }
        reader.close();

        try {
          writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
        } catch (Exception e) {
          fail("writer failed to open on a crashed index");
        }

        // add 100 documents
        for (int i = 0; i < 100; i++) {
            addDoc(writer);
        }

        // close
        writer.close();
    }

    // Simulate a corrupt index by removing last byte of
    // latest segments file and make sure we get an
    // IOException trying to open the index:
    public void testSimulatedCorruptIndex1() throws IOException {
        Directory dir = new RAMDirectory();

        IndexWriter writer = null;

        writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);

        // add 100 documents
        for (int i = 0; i < 100; i++) {
            addDoc(writer);
        }

        // close
        writer.close();

        long gen = SegmentInfos.getCurrentSegmentGeneration(dir);
        assertTrue("segment generation should be > 1 but got " + gen, gen > 1);

        String fileNameIn = SegmentInfos.getCurrentSegmentFileName(dir);
        String fileNameOut = IndexFileNames.fileNameFromGeneration(IndexFileNames.SEGMENTS,
                                                                   "",
                                                                   1+gen);
        IndexInput in = dir.openInput(fileNameIn);
        IndexOutput out = dir.createOutput(fileNameOut);
        long length = in.length();
        for(int i=0;i<length-1;i++) {
          out.writeByte(in.readByte());
        }
        in.close();
        out.close();
        dir.deleteFile(fileNameIn);

        IndexReader reader = null;
        try {
          reader = IndexReader.open(dir);
          fail("reader did not hit IOException on opening a corrupt index");
        } catch (Exception e) {
        }
        if (reader != null) {
          reader.close();
        }
    }

    public void testChangesAfterClose() throws IOException {
        Directory dir = new RAMDirectory();

        IndexWriter writer = null;

        writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
        addDoc(writer);

        // close
        writer.close();
        try {
          addDoc(writer);
          fail("did not hit AlreadyClosedException");
        } catch (AlreadyClosedException e) {
          // expected
        }
    }
  

    // Simulate a corrupt index by removing one of the cfs
    // files and make sure we get an IOException trying to
    // open the index:
    public void testSimulatedCorruptIndex2() throws IOException {
        Directory dir = new RAMDirectory();

        IndexWriter writer = null;

        writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);

        // add 100 documents
        for (int i = 0; i < 100; i++) {
            addDoc(writer);
        }

        // close
        writer.close();

        long gen = SegmentInfos.getCurrentSegmentGeneration(dir);
        assertTrue("segment generation should be > 1 but got " + gen, gen > 1);

        String[] files = dir.list();
        for(int i=0;i<files.length;i++) {
          if (files[i].endsWith(".cfs")) {
            dir.deleteFile(files[i]);
            break;
          }
        }

        IndexReader reader = null;
        try {
          reader = IndexReader.open(dir);
          fail("reader did not hit IOException on opening a corrupt index");
        } catch (Exception e) {
        }
        if (reader != null) {
          reader.close();
        }
    }

    /*
     * Simple test for "commit on close": open writer with
     * autoCommit=false, so it will only commit on close,
     * then add a bunch of docs, making sure reader does not
     * see these docs until writer is closed.
     */
    public void testCommitOnClose() throws IOException {
        Directory dir = new RAMDirectory();      
        IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
        for (int i = 0; i < 14; i++) {
          addDoc(writer);
        }
        writer.close();

        Term searchTerm = new Term("content", "aaa");        
        IndexSearcher searcher = new IndexSearcher(dir);
        Hits hits = searcher.search(new TermQuery(searchTerm));
        assertEquals("first number of hits", 14, hits.length());
        searcher.close();

        IndexReader reader = IndexReader.open(dir);

        writer = new IndexWriter(dir, false, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
        for(int i=0;i<3;i++) {
          for(int j=0;j<11;j++) {
            addDoc(writer);
          }
          searcher = new IndexSearcher(dir);
          hits = searcher.search(new TermQuery(searchTerm));
          assertEquals("reader incorrectly sees changes from writer with autoCommit disabled", 14, hits.length());
          searcher.close();
          assertTrue("reader should have still been current", reader.isCurrent());
        }

        // Now, close the writer:
        writer.close();
        assertFalse("reader should not be current now", reader.isCurrent());

        searcher = new IndexSearcher(dir);
        hits = searcher.search(new TermQuery(searchTerm));
        assertEquals("reader did not see changes after writer was closed", 47, hits.length());
        searcher.close();
    }

    /*
     * Simple test for "commit on close": open writer with
     * autoCommit=false, so it will only commit on close,
     * then add a bunch of docs, making sure reader does not
     * see them until writer has closed.  Then instead of
     * closing the writer, call abort and verify reader sees
     * nothing was added.  Then verify we can open the index
     * and add docs to it.
     */
    public void testCommitOnCloseAbort() throws IOException {
      MockRAMDirectory dir = new MockRAMDirectory();      
      IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
      writer.setMaxBufferedDocs(10);
      for (int i = 0; i < 14; i++) {
        addDoc(writer);
      }
      writer.close();

      Term searchTerm = new Term("content", "aaa");        
      IndexSearcher searcher = new IndexSearcher(dir);
      Hits hits = searcher.search(new TermQuery(searchTerm));
      assertEquals("first number of hits", 14, hits.length());
      searcher.close();

      writer = new IndexWriter(dir, false, new WhitespaceAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
      writer.setMaxBufferedDocs(10);
      for(int j=0;j<17;j++) {
        addDoc(writer);
      }
      // Delete all docs:
      writer.deleteDocuments(searchTerm);

      searcher = new IndexSearcher(dir);
      hits = searcher.search(new TermQuery(searchTerm));
      assertEquals("reader incorrectly sees changes from writer with autoCommit disabled", 14, hits.length());
      searcher.close();

      // Now, close the writer:
      writer.abort();

      assertNoUnreferencedFiles(dir, "unreferenced files remain after abort()");

      searcher = new IndexSearcher(dir);
      hits = searcher.search(new TermQuery(searchTerm));
      assertEquals("saw changes after writer.abort", 14, hits.length());
      searcher.close();
          
      // Now make sure we can re-open the index, add docs,
      // and all is good:
      writer = new IndexWriter(dir, false, new WhitespaceAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
      writer.setMaxBufferedDocs(10);

      // On abort, writer in fact may write to the same
      // segments_N file:
      dir.setPreventDoubleWrite(false);

      for(int i=0;i<12;i++) {
        for(int j=0;j<17;j++) {
          addDoc(writer);
        }
        searcher = new IndexSearcher(dir);
        hits = searcher.search(new TermQuery(searchTerm));
        assertEquals("reader incorrectly sees changes from writer with autoCommit disabled", 14, hits.length());
        searcher.close();
      }

      writer.close();
      searcher = new IndexSearcher(dir);
      hits = searcher.search(new TermQuery(searchTerm));
      assertEquals("didn't see changes after close", 218, hits.length());
      searcher.close();

      dir.close();
    }

    /*
     * Verify that a writer with "commit on close" indeed
     * cleans up the temp segments created after opening
     * that are not referenced by the starting segments
     * file.  We check this by using MockRAMDirectory to
     * measure max temp disk space used.
     */
    public void testCommitOnCloseDiskUsage() throws IOException {
      MockRAMDirectory dir = new MockRAMDirectory();      
      IndexWriter writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
      for(int j=0;j<30;j++) {
        addDocWithIndex(writer, j);
      }
      writer.close();
      dir.resetMaxUsedSizeInBytes();

      long startDiskUsage = dir.getMaxUsedSizeInBytes();
      writer  = new IndexWriter(dir, false, new WhitespaceAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
      for(int j=0;j<1470;j++) {
        addDocWithIndex(writer, j);
      }
      long midDiskUsage = dir.getMaxUsedSizeInBytes();
      dir.resetMaxUsedSizeInBytes();
      writer.optimize();
      writer.close();
      long endDiskUsage = dir.getMaxUsedSizeInBytes();

      // Ending index is 50X as large as starting index; due
      // to 2X disk usage normally we allow 100X max
      // transient usage.  If something is wrong w/ deleter
      // and it doesn't delete intermediate segments then it
      // will exceed this 100X:
      // System.out.println("start " + startDiskUsage + "; mid " + midDiskUsage + ";end " + endDiskUsage);
      assertTrue("writer used to much space while adding documents when autoCommit=false",     
                 midDiskUsage < 100*startDiskUsage);
      assertTrue("writer used to much space after close when autoCommit=false",     
                 endDiskUsage < 100*startDiskUsage);
    }


    /*
     * Verify that calling optimize when writer is open for
     * "commit on close" works correctly both for abort()
     * and close().
     */
    public void testCommitOnCloseOptimize() throws IOException {
      RAMDirectory dir = new RAMDirectory();      
      IndexWriter writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
      writer.setMaxBufferedDocs(10);
      for(int j=0;j<17;j++) {
        addDocWithIndex(writer, j);
      }
      writer.close();

      writer  = new IndexWriter(dir, false, new WhitespaceAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
      writer.optimize();

      // Open a reader before closing (commiting) the writer:
      IndexReader reader = IndexReader.open(dir);

      // Reader should see index as unoptimized at this
      // point:
      assertFalse("Reader incorrectly sees that the index is optimized", reader.isOptimized());
      reader.close();

      // Abort the writer:
      writer.abort();
      assertNoUnreferencedFiles(dir, "aborted writer after optimize");

      // Open a reader after aborting writer:
      reader = IndexReader.open(dir);

      // Reader should still see index as unoptimized:
      assertFalse("Reader incorrectly sees that the index is optimized", reader.isOptimized());
      reader.close();

      writer  = new IndexWriter(dir, false, new WhitespaceAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
      writer.optimize();
      writer.close();
      assertNoUnreferencedFiles(dir, "aborted writer after optimize");

      // Open a reader after aborting writer:
      reader = IndexReader.open(dir);

      // Reader should still see index as unoptimized:
      assertTrue("Reader incorrectly sees that the index is unoptimized", reader.isOptimized());
      reader.close();
    }

    public void testIndexNoDocuments() throws IOException {
      RAMDirectory dir = new RAMDirectory();      
      IndexWriter writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
      writer.flush();
      writer.close();

      IndexReader reader = IndexReader.open(dir);
      assertEquals(0, reader.maxDoc());
      assertEquals(0, reader.numDocs());
      reader.close();

      writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
      writer.flush();
      writer.close();

      reader = IndexReader.open(dir);
      assertEquals(0, reader.maxDoc());
      assertEquals(0, reader.numDocs());
      reader.close();
    }

    public void testManyFields() throws IOException {
      RAMDirectory dir = new RAMDirectory();      
      IndexWriter writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
      writer.setMaxBufferedDocs(10);
      for(int j=0;j<100;j++) {
        Document doc = new Document();
        doc.add(new Field("a"+j, "aaa" + j, Field.Store.YES, Field.Index.TOKENIZED));
        doc.add(new Field("b"+j, "aaa" + j, Field.Store.YES, Field.Index.TOKENIZED));
        doc.add(new Field("c"+j, "aaa" + j, Field.Store.YES, Field.Index.TOKENIZED));
        doc.add(new Field("d"+j, "aaa", Field.Store.YES, Field.Index.TOKENIZED));
        doc.add(new Field("e"+j, "aaa", Field.Store.YES, Field.Index.TOKENIZED));
        doc.add(new Field("f"+j, "aaa", Field.Store.YES, Field.Index.TOKENIZED));
        writer.addDocument(doc);
      }
      writer.close();

      IndexReader reader = IndexReader.open(dir);
      assertEquals(100, reader.maxDoc());
      assertEquals(100, reader.numDocs());
      for(int j=0;j<100;j++) {
        assertEquals(1, reader.docFreq(new Term("a"+j, "aaa"+j)));
        assertEquals(1, reader.docFreq(new Term("b"+j, "aaa"+j)));
        assertEquals(1, reader.docFreq(new Term("c"+j, "aaa"+j)));
        assertEquals(1, reader.docFreq(new Term("d"+j, "aaa")));
        assertEquals(1, reader.docFreq(new Term("e"+j, "aaa")));
        assertEquals(1, reader.docFreq(new Term("f"+j, "aaa")));
      }
      reader.close();
      dir.close();
    }

    public void testSmallRAMBuffer() throws IOException {
      RAMDirectory dir = new RAMDirectory();      
      IndexWriter writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
      writer.setRAMBufferSizeMB(0.000001);
      int lastNumFile = dir.list().length;
      for(int j=0;j<9;j++) {
        Document doc = new Document();
        doc.add(new Field("field", "aaa" + j, Field.Store.YES, Field.Index.TOKENIZED));
        writer.addDocument(doc);
        int numFile = dir.list().length;
        // Verify that with a tiny RAM buffer we see new
        // segment after every doc
        assertTrue(numFile > lastNumFile);
        lastNumFile = numFile;
      }
      writer.close();
      dir.close();
    }

    // Make sure it's OK to change RAM buffer size and
    // maxBufferedDocs in a write session
    public void testChangingRAMBuffer() throws IOException {
      RAMDirectory dir = new RAMDirectory();      
      IndexWriter writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
      writer.setMaxBufferedDocs(10);
      writer.setRAMBufferSizeMB(IndexWriter.DISABLE_AUTO_FLUSH);

      int lastFlushCount = -1;
      for(int j=1;j<52;j++) {
        Document doc = new Document();
        doc.add(new Field("field", "aaa" + j, Field.Store.YES, Field.Index.TOKENIZED));
        writer.addDocument(doc);
        _TestUtil.syncConcurrentMerges(writer);
        int flushCount = writer.getFlushCount();
        if (j == 1)
          lastFlushCount = flushCount;
        else if (j < 10)
          // No new files should be created
          assertEquals(flushCount, lastFlushCount);
        else if (10 == j) {
          assertTrue(flushCount > lastFlushCount);
          lastFlushCount = flushCount;
          writer.setRAMBufferSizeMB(0.000001);
          writer.setMaxBufferedDocs(IndexWriter.DISABLE_AUTO_FLUSH);
        } else if (j < 20) {
          assertTrue(flushCount > lastFlushCount);
          lastFlushCount = flushCount;
        } else if (20 == j) {
          writer.setRAMBufferSizeMB(16);
          writer.setMaxBufferedDocs(IndexWriter.DISABLE_AUTO_FLUSH);
          lastFlushCount = flushCount;
        } else if (j < 30) {
          assertEquals(flushCount, lastFlushCount);
        } else if (30 == j) {
          writer.setRAMBufferSizeMB(0.000001);
          writer.setMaxBufferedDocs(IndexWriter.DISABLE_AUTO_FLUSH);
        } else if (j < 40) {
          assertTrue(flushCount> lastFlushCount);
          lastFlushCount = flushCount;
        } else if (40 == j) {
          writer.setMaxBufferedDocs(10);
          writer.setRAMBufferSizeMB(IndexWriter.DISABLE_AUTO_FLUSH);
          lastFlushCount = flushCount;
        } else if (j < 50) {
          assertEquals(flushCount, lastFlushCount);
          writer.setMaxBufferedDocs(10);
          writer.setRAMBufferSizeMB(IndexWriter.DISABLE_AUTO_FLUSH);
        } else if (50 == j) {
          assertTrue(flushCount > lastFlushCount);
        }
      }
      writer.close();
      dir.close();
    }

    public void testChangingRAMBuffer2() throws IOException {
      RAMDirectory dir = new RAMDirectory();      
      IndexWriter writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
      writer.setMaxBufferedDocs(10);
      writer.setMaxBufferedDeleteTerms(10);
      writer.setRAMBufferSizeMB(IndexWriter.DISABLE_AUTO_FLUSH);

      for(int j=1;j<52;j++) {
        Document doc = new Document();
        doc.add(new Field("field", "aaa" + j, Field.Store.YES, Field.Index.TOKENIZED));
        writer.addDocument(doc);
      }
      
      int lastFlushCount = -1;
      for(int j=1;j<52;j++) {
        writer.deleteDocuments(new Term("field", "aaa" + j));
        _TestUtil.syncConcurrentMerges(writer);
        int flushCount = writer.getFlushCount();
        if (j == 1)
          lastFlushCount = flushCount;
        else if (j < 10) {
          // No new files should be created
          assertEquals(flushCount, lastFlushCount);
        } else if (10 == j) {
          assertTrue(flushCount > lastFlushCount);
          lastFlushCount = flushCount;
          writer.setRAMBufferSizeMB(0.000001);
          writer.setMaxBufferedDeleteTerms(IndexWriter.DISABLE_AUTO_FLUSH);
        } else if (j < 20) {
          assertTrue(flushCount > lastFlushCount);
          lastFlushCount = flushCount;
        } else if (20 == j) {
          writer.setRAMBufferSizeMB(16);
          writer.setMaxBufferedDeleteTerms(IndexWriter.DISABLE_AUTO_FLUSH);
          lastFlushCount = flushCount;
        } else if (j < 30) {
          assertEquals(flushCount, lastFlushCount);
        } else if (30 == j) {
          writer.setRAMBufferSizeMB(0.000001);
          writer.setMaxBufferedDeleteTerms(IndexWriter.DISABLE_AUTO_FLUSH);
        } else if (j < 40) {
          assertTrue(flushCount> lastFlushCount);
          lastFlushCount = flushCount;
        } else if (40 == j) {
          writer.setMaxBufferedDeleteTerms(10);
          writer.setRAMBufferSizeMB(IndexWriter.DISABLE_AUTO_FLUSH);
          lastFlushCount = flushCount;
        } else if (j < 50) {
          assertEquals(flushCount, lastFlushCount);
          writer.setMaxBufferedDeleteTerms(10);
          writer.setRAMBufferSizeMB(IndexWriter.DISABLE_AUTO_FLUSH);
        } else if (50 == j) {
          assertTrue(flushCount > lastFlushCount);
        }
      }
      writer.close();
      dir.close();
    }

    public void testDiverseDocs() throws IOException {
      RAMDirectory dir = new RAMDirectory();      
      IndexWriter writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
      writer.setRAMBufferSizeMB(0.5);
      Random rand = new Random(31415);
      for(int i=0;i<3;i++) {
        // First, docs where every term is unique (heavy on
        // Posting instances)
        for(int j=0;j<100;j++) {
          Document doc = new Document();
          for(int k=0;k<100;k++) {
            doc.add(new Field("field", Integer.toString(rand.nextInt()), Field.Store.YES, Field.Index.TOKENIZED));
          }
          writer.addDocument(doc);
        }

        // Next, many single term docs where only one term
        // occurs (heavy on byte blocks)
        for(int j=0;j<100;j++) {
          Document doc = new Document();
          doc.add(new Field("field", "aaa aaa aaa aaa aaa aaa aaa aaa aaa aaa", Field.Store.YES, Field.Index.TOKENIZED));
          writer.addDocument(doc);
        }

        // Next, many single term docs where only one term
        // occurs but the terms are very long (heavy on
        // char[] arrays)
        for(int j=0;j<100;j++) {
          StringBuffer b = new StringBuffer();
          String x = Integer.toString(j) + ".";
          for(int k=0;k<1000;k++)
            b.append(x);
          String longTerm = b.toString();

          Document doc = new Document();
          doc.add(new Field("field", longTerm, Field.Store.YES, Field.Index.TOKENIZED));
          writer.addDocument(doc);
        }
      }
      writer.close();

      IndexSearcher searcher = new IndexSearcher(dir);
      Hits hits = searcher.search(new TermQuery(new Term("field", "aaa")));
      assertEquals(300, hits.length());
      searcher.close();

      dir.close();
    }

    public void testEnablingNorms() throws IOException {
      RAMDirectory dir = new RAMDirectory();      
      IndexWriter writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
      writer.setMaxBufferedDocs(10);
      // Enable norms for only 1 doc, pre flush
      for(int j=0;j<10;j++) {
        Document doc = new Document();
        Field f = new Field("field", "aaa", Field.Store.YES, Field.Index.TOKENIZED); 
        if (j != 8) {
          f.setOmitNorms(true);
        }
        doc.add(f);
        writer.addDocument(doc);
      }
      writer.close();

      Term searchTerm = new Term("field", "aaa");

      IndexSearcher searcher = new IndexSearcher(dir);
      Hits hits = searcher.search(new TermQuery(searchTerm));
      assertEquals(10, hits.length());
      searcher.close();

      writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
      writer.setMaxBufferedDocs(10);
      // Enable norms for only 1 doc, post flush
      for(int j=0;j<27;j++) {
        Document doc = new Document();
        Field f = new Field("field", "aaa", Field.Store.YES, Field.Index.TOKENIZED); 
        if (j != 26) {
          f.setOmitNorms(true);
        }
        doc.add(f);
        writer.addDocument(doc);
      }
      writer.close();
      searcher = new IndexSearcher(dir);
      hits = searcher.search(new TermQuery(searchTerm));
      assertEquals(27, hits.length());
      searcher.close();

      IndexReader reader = IndexReader.open(dir);
      reader.close();

      dir.close();
    }

    public void testHighFreqTerm() throws IOException {
      RAMDirectory dir = new RAMDirectory();      
      IndexWriter writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true, new IndexWriter.MaxFieldLength(100000000));
      writer.setRAMBufferSizeMB(0.01);
      // Massive doc that has 128 K a's
      StringBuffer b = new StringBuffer(1024*1024);
      for(int i=0;i<4096;i++) {
        b.append(" a a a a a a a a");
        b.append(" a a a a a a a a");
        b.append(" a a a a a a a a");
        b.append(" a a a a a a a a");
      }
      Document doc = new Document();
      doc.add(new Field("field", b.toString(), Field.Store.YES, Field.Index.TOKENIZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
      writer.addDocument(doc);
      writer.close();

      IndexReader reader = IndexReader.open(dir);
      assertEquals(1, reader.maxDoc());
      assertEquals(1, reader.numDocs());
      Term t = new Term("field", "a");
      assertEquals(1, reader.docFreq(t));
      TermDocs td = reader.termDocs(t);
      td.next();
      assertEquals(128*1024, td.freq());
      reader.close();
      dir.close();
    }

    // Make sure that a Directory implementation that does
    // not use LockFactory at all (ie overrides makeLock and
    // implements its own private locking) works OK.  This
    // was raised on java-dev as loss of backwards
    // compatibility.
    public void testNullLockFactory() throws IOException {

      final class MyRAMDirectory extends RAMDirectory {
        private LockFactory myLockFactory;
        MyRAMDirectory() {
          lockFactory = null;
          myLockFactory = new SingleInstanceLockFactory();
        }
        public Lock makeLock(String name) {
          return myLockFactory.makeLock(name);
        }
      }
      
      Directory dir = new MyRAMDirectory();
      IndexWriter writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
      for (int i = 0; i < 100; i++) {
        addDoc(writer);
      }
      writer.close();
      Term searchTerm = new Term("content", "aaa");        
      IndexSearcher searcher = new IndexSearcher(dir);
      Hits hits = searcher.search(new TermQuery(searchTerm));
      assertEquals("did not get right number of hits", 100, hits.length());
      writer.close();

      writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
      writer.close();

      dir.close();
    }

    public void testFlushWithNoMerging() throws IOException {
      Directory dir = new RAMDirectory();
      IndexWriter writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
      writer.setMaxBufferedDocs(2);
      Document doc = new Document();
      doc.add(new Field("field", "aaa", Field.Store.YES, Field.Index.TOKENIZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
      for(int i=0;i<19;i++)
        writer.addDocument(doc);
      writer.flush(false, true);
      writer.close();
      SegmentInfos sis = new SegmentInfos();
      sis.read(dir);
      // Since we flushed w/o allowing merging we should now
      // have 10 segments
      assert sis.size() == 10;
    }

    // Make sure we can flush segment w/ norms, then add
    // empty doc (no norms) and flush
    public void testEmptyDocAfterFlushingRealDoc() throws IOException {
      Directory dir = new RAMDirectory();
      IndexWriter writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
      Document doc = new Document();
      doc.add(new Field("field", "aaa", Field.Store.YES, Field.Index.TOKENIZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
      writer.addDocument(doc);
      writer.flush();
      writer.addDocument(new Document());
      writer.close();
      IndexReader reader = IndexReader.open(dir);
      assertEquals(2, reader.numDocs());
    }

    // Test calling optimize(false) whereby optimize is kicked
    // off but we don't wait for it to finish (but
    // writer.close()) does wait
    public void testBackgroundOptimize() throws IOException {

      Directory dir = new MockRAMDirectory();
      for(int pass=0;pass<2;pass++) {
        IndexWriter writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
        writer.setMergeScheduler(new ConcurrentMergeScheduler());
        Document doc = new Document();
        doc.add(new Field("field", "aaa", Field.Store.YES, Field.Index.TOKENIZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
        writer.setMaxBufferedDocs(2);
        writer.setMergeFactor(101);
        for(int i=0;i<200;i++)
          writer.addDocument(doc);
        writer.optimize(false);

        if (0 == pass) {
          writer.close();
          IndexReader reader = IndexReader.open(dir);
          assertTrue(reader.isOptimized());
          reader.close();
        } else {
          // Get another segment to flush so we can verify it is
          // NOT included in the optimization
          writer.addDocument(doc);
          writer.addDocument(doc);
          writer.close();

          IndexReader reader = IndexReader.open(dir);
          assertTrue(!reader.isOptimized());
          reader.close();

          SegmentInfos infos = new SegmentInfos();
          infos.read(dir);
          assertEquals(2, infos.size());
        }
      }      

      dir.close();
    }

    private void rmDir(File dir) {
        File[] files = dir.listFiles();
        if (files != null) {
          for (int i = 0; i < files.length; i++) {
            files[i].delete();
          }
        }
        dir.delete();
    }
  
  /**
   * Test that no NullPointerException will be raised,
   * when adding one document with a single, empty field
   * and term vectors enabled.
   * @throws IOException
   *
   */
  public void testBadSegment() throws IOException {
    MockRAMDirectory dir = new MockRAMDirectory();
    IndexWriter ir = new IndexWriter(dir, new StandardAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
    
    Document document = new Document();
    document.add(new Field("tvtest", "", Field.Store.NO, Field.Index.TOKENIZED,
        Field.TermVector.YES));
    ir.addDocument(document);
    ir.close();
    dir.close();
  }

  // LUCENE-1008
  public void testNoTermVectorAfterTermVector() throws IOException {
    MockRAMDirectory dir = new MockRAMDirectory();
    IndexWriter iw = new IndexWriter(dir, new StandardAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
    Document document = new Document();
    document.add(new Field("tvtest", "a b c", Field.Store.NO, Field.Index.TOKENIZED,
        Field.TermVector.YES));
    iw.addDocument(document);
    document = new Document();
    document.add(new Field("tvtest", "x y z", Field.Store.NO, Field.Index.TOKENIZED,
                           Field.TermVector.NO));
    iw.addDocument(document);
    // Make first segment
    iw.flush();

    document.add(new Field("tvtest", "a b c", Field.Store.NO, Field.Index.TOKENIZED,
        Field.TermVector.YES));
    iw.addDocument(document);
    // Make 2nd segment
    iw.flush();

    iw.optimize();
    iw.close();
    dir.close();
  }

  // LUCENE-1010
  public void testNoTermVectorAfterTermVectorMerge() throws IOException {
    MockRAMDirectory dir = new MockRAMDirectory();
    IndexWriter iw = new IndexWriter(dir, new StandardAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
    Document document = new Document();
    document.add(new Field("tvtest", "a b c", Field.Store.NO, Field.Index.TOKENIZED,
        Field.TermVector.YES));
    iw.addDocument(document);
    iw.flush();

    document = new Document();
    document.add(new Field("tvtest", "x y z", Field.Store.NO, Field.Index.TOKENIZED,
                           Field.TermVector.NO));
    iw.addDocument(document);
    // Make first segment
    iw.flush();

    iw.optimize();

    document.add(new Field("tvtest", "a b c", Field.Store.NO, Field.Index.TOKENIZED,
        Field.TermVector.YES));
    iw.addDocument(document);
    // Make 2nd segment
    iw.flush();
    iw.optimize();

    iw.close();
    dir.close();
  }

  // LUCENE-1036
  public void testMaxThreadPriority() throws IOException {
    int pri = Thread.currentThread().getPriority();
    try {
      MockRAMDirectory dir = new MockRAMDirectory();
      IndexWriter iw = new IndexWriter(dir, new StandardAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
      Document document = new Document();
      document.add(new Field("tvtest", "a b c", Field.Store.NO, Field.Index.TOKENIZED,
                             Field.TermVector.YES));
      iw.setMaxBufferedDocs(2);
      iw.setMergeFactor(2);
      Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
      for(int i=0;i<4;i++)
        iw.addDocument(document);
      iw.close();
      
    } finally {
      Thread.currentThread().setPriority(pri);
    }
  }

  // Just intercepts all merges & verifies that we are never
  // merging a segment with >= 20 (maxMergeDocs) docs
  private class MyMergeScheduler extends MergeScheduler {
    synchronized public void merge(IndexWriter writer)
      throws CorruptIndexException, IOException {

      while(true) {
        MergePolicy.OneMerge merge = writer.getNextMerge();
        if (merge == null)
          break;
        for(int i=0;i<merge.segments.size();i++)
          assert merge.segments.info(i).docCount < 20;
        writer.merge(merge);
      }
    }

    public void close() {}
  }

  // LUCENE-1013
  public void testSetMaxMergeDocs() throws IOException {
    MockRAMDirectory dir = new MockRAMDirectory();
    IndexWriter iw = new IndexWriter(dir, new StandardAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
    iw.setMergeScheduler(new MyMergeScheduler());
    iw.setMaxMergeDocs(20);
    iw.setMaxBufferedDocs(2);
    iw.setMergeFactor(2);
    Document document = new Document();
    document.add(new Field("tvtest", "a b c", Field.Store.NO, Field.Index.TOKENIZED,
                           Field.TermVector.YES));
    for(int i=0;i<177;i++)
      iw.addDocument(document);
    iw.close();
  }

  // LUCENE-1072
  public void testExceptionFromTokenStream() throws IOException {
    RAMDirectory dir = new MockRAMDirectory();
    IndexWriter writer = new IndexWriter(dir, new Analyzer() {

      public TokenStream tokenStream(String fieldName, Reader reader) {
        return new TokenFilter(new StandardTokenizer(reader)) {
          private int count = 0;

          public Token next() throws IOException {
            if (count++ == 5) {
              throw new IOException();
            }
            return input.next();
          }
        };
      }

    }, true, IndexWriter.MaxFieldLength.LIMITED);

    Document doc = new Document();
    String contents = "aa bb cc dd ee ff gg hh ii jj kk";
    doc.add(new Field("content", contents, Field.Store.NO,
        Field.Index.TOKENIZED));
    try {
      writer.addDocument(doc);
      fail("did not hit expected exception");
    } catch (Exception e) {
    }

    // Make sure we can add another normal document
    doc = new Document();
    doc.add(new Field("content", "aa bb cc dd", Field.Store.NO,
        Field.Index.TOKENIZED));
    writer.addDocument(doc);

    // Make sure we can add another normal document
    doc = new Document();
    doc.add(new Field("content", "aa bb cc dd", Field.Store.NO,
        Field.Index.TOKENIZED));
    writer.addDocument(doc);

    writer.close();
    IndexReader reader = IndexReader.open(dir);
    final Term t = new Term("content", "aa");
    assertEquals(reader.docFreq(t), 3);

    // Make sure the doc that hit the exception was marked
    // as deleted:
    TermDocs tdocs = reader.termDocs(t);
    int count = 0;
    while(tdocs.next()) {
      count++;
    }
    assertEquals(2, count);

    assertEquals(reader.docFreq(new Term("content", "gg")), 0);
    reader.close();
    dir.close();
  }

  private static class FailOnlyOnFlush extends MockRAMDirectory.Failure {
    boolean doFail = false;
    int count;

    public void setDoFail() {
      this.doFail = true;
    }
    public void clearDoFail() {
      this.doFail = false;
    }

    public void eval(MockRAMDirectory dir)  throws IOException {
      if (doFail) {
        StackTraceElement[] trace = new Exception().getStackTrace();
        boolean sawAppend = false;
        boolean sawFlush = false;
        for (int i = 0; i < trace.length; i++) {
          if ("org.apache.lucene.index.DocumentsWriter".equals(trace[i].getClassName()) && "appendPostings".equals(trace[i].getMethodName()))
            sawAppend = true;
          if ("doFlush".equals(trace[i].getMethodName()))
            sawFlush = true;
        }

        if (sawAppend && sawFlush && count++ >= 30) {
          doFail = false;
          throw new IOException("now failing during flush");
        }
      }
    }
  }

  // LUCENE-1072: make sure an errant exception on flushing
  // one segment only takes out those docs in that one flush
  public void testDocumentsWriterAbort() throws IOException {
    MockRAMDirectory dir = new MockRAMDirectory();
    FailOnlyOnFlush failure = new FailOnlyOnFlush();
    failure.setDoFail();
    dir.failOn(failure);

    IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
    writer.setMaxBufferedDocs(2);
    Document doc = new Document();
    String contents = "aa bb cc dd ee ff gg hh ii jj kk";
    doc.add(new Field("content", contents, Field.Store.NO,
        Field.Index.TOKENIZED));
    boolean hitError = false;
    for(int i=0;i<200;i++) {
      try {
        writer.addDocument(doc);
      } catch (IOException ioe) {
        // only one flush should fail:
        assertFalse(hitError);
        hitError = true;
      }
    }
    assertTrue(hitError);
    writer.close();
    IndexReader reader = IndexReader.open(dir);
    assertEquals(198, reader.docFreq(new Term("content", "aa")));
    reader.close();
  }

  private class CrashingFilter extends TokenFilter {
    String fieldName;
    int count;

    public CrashingFilter(String fieldName, TokenStream input) {
      super(input);
      this.fieldName = fieldName;
    }

    public Token next(Token result) throws IOException {
      if (this.fieldName.equals("crash") && count++ >= 4)
        throw new IOException("I'm experiencing problems");
      return input.next(result);
    }

    public void reset() throws IOException {
      super.reset();
      count = 0;
    }
  }

  public void testDocumentsWriterExceptions() throws IOException {
    Analyzer analyzer = new Analyzer() {
      public TokenStream tokenStream(String fieldName, Reader reader) {
        return new CrashingFilter(fieldName, new WhitespaceTokenizer(reader));
      }
    };

    for(int i=0;i<2;i++) {
      MockRAMDirectory dir = new MockRAMDirectory();
      IndexWriter writer = new IndexWriter(dir, analyzer, IndexWriter.MaxFieldLength.LIMITED);
      //writer.setInfoStream(System.out);
      Document doc = new Document();
      doc.add(new Field("contents", "here are some contents", Field.Store.YES,
                        Field.Index.TOKENIZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
      writer.addDocument(doc);
      writer.addDocument(doc);
      doc.add(new Field("crash", "this should crash after 4 terms", Field.Store.YES,
                        Field.Index.TOKENIZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
      doc.add(new Field("other", "this will not get indexed", Field.Store.YES,
                        Field.Index.TOKENIZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
      try {
        writer.addDocument(doc);
        fail("did not hit expected exception");
      } catch (IOException ioe) {
      }

      if (0 == i) {
        doc = new Document();
        doc.add(new Field("contents", "here are some contents", Field.Store.YES,
                          Field.Index.TOKENIZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
        writer.addDocument(doc);
        writer.addDocument(doc);
      }
      writer.close();

      IndexReader reader = IndexReader.open(dir);
      int expected = 3+(1-i)*2;
      assertEquals(expected, reader.docFreq(new Term("contents", "here")));
      assertEquals(expected, reader.maxDoc());
      int numDel = 0;
      for(int j=0;j<reader.maxDoc();j++) {
        if (reader.isDeleted(j))
          numDel++;
        else {
          reader.document(j);
          reader.getTermFreqVectors(j);
        }
      }
      reader.close();

      assertEquals(1, numDel);

      writer = new IndexWriter(dir, analyzer, IndexWriter.MaxFieldLength.LIMITED);
      writer.setMaxBufferedDocs(10);
      doc = new Document();
      doc.add(new Field("contents", "here are some contents", Field.Store.YES,
                        Field.Index.TOKENIZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
      for(int j=0;j<17;j++)
        writer.addDocument(doc);
      writer.optimize();
      writer.close();

      reader = IndexReader.open(dir);
      expected = 19+(1-i)*2;
      assertEquals(expected, reader.docFreq(new Term("contents", "here")));
      assertEquals(expected, reader.maxDoc());
      numDel = 0;
      for(int j=0;j<reader.maxDoc();j++) {
        if (reader.isDeleted(j))
          numDel++;
        else {
          reader.document(j);
          reader.getTermFreqVectors(j);
        }
      }
      reader.close();
      assertEquals(0, numDel);

      dir.close();
    }
  }

  public void testDocumentsWriterExceptionThreads() throws IOException {
    Analyzer analyzer = new Analyzer() {
      public TokenStream tokenStream(String fieldName, Reader reader) {
        return new CrashingFilter(fieldName, new WhitespaceTokenizer(reader));
      }
    };

    final int NUM_THREAD = 3;
    final int NUM_ITER = 100;

    for(int i=0;i<2;i++) {
      MockRAMDirectory dir = new MockRAMDirectory();

      {
        final IndexWriter writer = new IndexWriter(dir, analyzer, IndexWriter.MaxFieldLength.LIMITED);

        final int finalI = i;

        Thread[] threads = new Thread[NUM_THREAD];
        for(int t=0;t<NUM_THREAD;t++) {
          threads[t] = new Thread() {
              public void run() {
                try {
                  for(int iter=0;iter<NUM_ITER;iter++) {
                    Document doc = new Document();
                    doc.add(new Field("contents", "here are some contents", Field.Store.YES,
                                      Field.Index.TOKENIZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
                    writer.addDocument(doc);
                    writer.addDocument(doc);
                    doc.add(new Field("crash", "this should crash after 4 terms", Field.Store.YES,
                                      Field.Index.TOKENIZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
                    doc.add(new Field("other", "this will not get indexed", Field.Store.YES,
                                      Field.Index.TOKENIZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
                    try {
                      writer.addDocument(doc);
                      fail("did not hit expected exception");
                    } catch (IOException ioe) {
                    }

                    if (0 == finalI) {
                      doc = new Document();
                      doc.add(new Field("contents", "here are some contents", Field.Store.YES,
                                        Field.Index.TOKENIZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
                      writer.addDocument(doc);
                      writer.addDocument(doc);
                    }
                  }
                } catch (Throwable t) {
                  synchronized(this) {
                    System.out.println(Thread.currentThread().getName() + ": ERROR: hit unexpected exception");
                    t.printStackTrace(System.out);
                  }
                  fail();
                }
              }
            };
          threads[t].start();
        }

        for(int t=0;t<NUM_THREAD;t++)
          while (true)
            try {
              threads[t].join();
              break;
            } catch (InterruptedException ie) {
              Thread.currentThread().interrupt();
            }            
            
        writer.close();
      }

      IndexReader reader = IndexReader.open(dir);
      int expected = (3+(1-i)*2)*NUM_THREAD*NUM_ITER;
      assertEquals(expected, reader.docFreq(new Term("contents", "here")));
      assertEquals(expected, reader.maxDoc());
      int numDel = 0;
      for(int j=0;j<reader.maxDoc();j++) {
        if (reader.isDeleted(j))
          numDel++;
        else {
          reader.document(j);
          reader.getTermFreqVectors(j);
        }
      }
      reader.close();

      assertEquals(NUM_THREAD*NUM_ITER, numDel);

      IndexWriter writer = new IndexWriter(dir, analyzer, IndexWriter.MaxFieldLength.LIMITED);
      writer.setMaxBufferedDocs(10);
      Document doc = new Document();
      doc.add(new Field("contents", "here are some contents", Field.Store.YES,
                        Field.Index.TOKENIZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
      for(int j=0;j<17;j++)
        writer.addDocument(doc);
      writer.optimize();
      writer.close();

      reader = IndexReader.open(dir);
      expected += 17-NUM_THREAD*NUM_ITER;
      assertEquals(expected, reader.docFreq(new Term("contents", "here")));
      assertEquals(expected, reader.maxDoc());
      numDel = 0;
      for(int j=0;j<reader.maxDoc();j++) {
        if (reader.isDeleted(j))
          numDel++;
        else {
          reader.document(j);
          reader.getTermFreqVectors(j);
        }
      }
      reader.close();
      assertEquals(0, numDel);

      dir.close();
    }
  }

  public void testVariableSchema() throws IOException {
    MockRAMDirectory dir = new MockRAMDirectory();
    int delID = 0;
    for(int i=0;i<20;i++) {
      IndexWriter writer = new IndexWriter(dir, false, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
      writer.setMaxBufferedDocs(2);
      writer.setMergeFactor(2);
      writer.setUseCompoundFile(false);
      Document doc = new Document();
      String contents = "aa bb cc dd ee ff gg hh ii jj kk";

      if (i == 7) {
        // Add empty docs here
        doc.add(new Field("content3", "", Field.Store.NO,
                          Field.Index.TOKENIZED));
      } else {
        Field.Store storeVal;
        if (i%2 == 0) {
          doc.add(new Field("content4", contents, Field.Store.YES,
                            Field.Index.TOKENIZED));
          storeVal = Field.Store.YES;
        } else
          storeVal = Field.Store.NO;
        doc.add(new Field("content1", contents, storeVal,
                          Field.Index.TOKENIZED));
        doc.add(new Field("content3", "", Field.Store.YES,
                          Field.Index.TOKENIZED));
        doc.add(new Field("content5", "", storeVal,
                          Field.Index.TOKENIZED));
      }

      for(int j=0;j<4;j++)
        writer.addDocument(doc);

      writer.close();
      IndexReader reader = IndexReader.open(dir);
      reader.deleteDocument(delID++);
      reader.close();

      if (0 == i % 4) {
        writer = new IndexWriter(dir, false, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
        writer.setUseCompoundFile(false);
        writer.optimize();
        writer.close();
      }
    }
  }

  public void testNoWaitClose() throws Throwable {
    RAMDirectory directory = new MockRAMDirectory();

    final Document doc = new Document();
    Field idField = new Field("id", "", Field.Store.YES, Field.Index.UN_TOKENIZED);
    doc.add(idField);

    for(int pass=0;pass<3;pass++) {
      boolean autoCommit = pass%2 == 0;
      IndexWriter writer = new IndexWriter(directory, autoCommit, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);

      //System.out.println("TEST: pass=" + pass + " ac=" + autoCommit + " cms=" + (pass >= 2));
      for(int iter=0;iter<10;iter++) {
        //System.out.println("TEST: iter=" + iter);
        MergeScheduler ms;
        if (pass >= 2)
          ms = new ConcurrentMergeScheduler();
        else
          ms = new SerialMergeScheduler();
        
        writer.setMergeScheduler(ms);
        writer.setMaxBufferedDocs(2);
        writer.setMergeFactor(100);

        for(int j=0;j<199;j++) {
          idField.setValue(Integer.toString(iter*201+j));
          writer.addDocument(doc);
        }

        int delID = iter*199;
        for(int j=0;j<20;j++) {
          writer.deleteDocuments(new Term("id", Integer.toString(delID)));
          delID += 5;
        }

        // Force a bunch of merge threads to kick off so we
        // stress out aborting them on close:
        writer.setMergeFactor(2);

        final IndexWriter finalWriter = writer;
        final ArrayList failure = new ArrayList();
        Thread t1 = new Thread() {
            public void run() {
              boolean done = false;
              while(!done) {
                for(int i=0;i<100;i++) {
                  try {
                    finalWriter.addDocument(doc);
                  } catch (AlreadyClosedException e) {
                    done = true;
                    break;
                  } catch (NullPointerException e) {
                    done = true;
                    break;
                  } catch (Throwable e) {
                    e.printStackTrace(System.out);
                    failure.add(e);
                    done = true;
                    break;
                  }
                }
                Thread.yield();
              }

            }
          };

        if (failure.size() > 0)
          throw (Throwable) failure.get(0);

        t1.start();

        writer.close(false);
        while(true) {
          try {
            t1.join();
            break;
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
          }
        }

        // Make sure reader can read
        IndexReader reader = IndexReader.open(directory);
        reader.close();

        // Reopen
        writer = new IndexWriter(directory, autoCommit, new WhitespaceAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
      }
      writer.close();
    }

    directory.close();
  }

  // Used by test cases below
  private class IndexerThread extends Thread {

    boolean diskFull;
    Throwable error;
    AlreadyClosedException ace;
    IndexWriter writer;
    boolean noErrors;

    public IndexerThread(IndexWriter writer, boolean noErrors) {
      this.writer = writer;
      this.noErrors = noErrors;
    }

    public void run() {

      final Document doc = new Document();
      doc.add(new Field("field", "aaa bbb ccc ddd eee fff ggg hhh iii jjj", Field.Store.YES, Field.Index.TOKENIZED, Field.TermVector.WITH_POSITIONS_OFFSETS));

      int idUpto = 0;
      int fullCount = 0;
      final long stopTime = System.currentTimeMillis() + 500;

      while(System.currentTimeMillis() < stopTime) {
        try {
          writer.updateDocument(new Term("id", ""+(idUpto++)), doc);
        } catch (IOException ioe) {
          //ioe.printStackTrace(System.out);
          if (ioe.getMessage().startsWith("fake disk full at") ||
              ioe.getMessage().equals("now failing on purpose")) {
            diskFull = true;
            try {
              Thread.sleep(1);
            } catch (InterruptedException ie) {
              Thread.currentThread().interrupt();
            }
            if (fullCount++ >= 5)
              break;
          } else {
            if (noErrors) {
              System.out.println(Thread.currentThread().getName() + ": ERROR: unexpected IOException:");
              ioe.printStackTrace(System.out);
              error = ioe;
            }
            break;
          }
        } catch (Throwable t) {
          //t.printStackTrace(System.out);
          if (noErrors) {
            System.out.println(Thread.currentThread().getName() + ": ERROR: unexpected Throwable:");
            t.printStackTrace(System.out);
            error = t;
          }
          break;
        }
      }
    }
  }

  // LUCENE-1130: make sure we can close() even while
  // threads are trying to add documents.  Strictly
  // speaking, this isn't valid us of Lucene's APIs, but we
  // still want to be robust to this case:
  public void testCloseWithThreads() throws IOException {
    int NUM_THREADS = 3;

    for(int iter=0;iter<20;iter++) {
      MockRAMDirectory dir = new MockRAMDirectory();
      IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
      ConcurrentMergeScheduler cms = new ConcurrentMergeScheduler();

      writer.setMergeScheduler(cms);
      writer.setMaxBufferedDocs(10);
      writer.setMergeFactor(4);

      IndexerThread[] threads = new IndexerThread[NUM_THREADS];

      for(int i=0;i<NUM_THREADS;i++)
        threads[i] = new IndexerThread(writer, false);

      for(int i=0;i<NUM_THREADS;i++)
        threads[i].start();

      try {
        Thread.sleep(100);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
      }

      writer.close(false);

      // Make sure threads that are adding docs are not hung:
      for(int i=0;i<NUM_THREADS;i++) {
        while(true) {
          try {
            // Without fix for LUCENE-1130: one of the
            // threads will hang
            threads[i].join();
            break;
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
          }
        }
        if (threads[i].isAlive())
          fail("thread seems to be hung");
      }

      // Quick test to make sure index is not corrupt:
      IndexReader reader = IndexReader.open(dir);
      TermDocs tdocs = reader.termDocs(new Term("field", "aaa"));
      int count = 0;
      while(tdocs.next()) {
        count++;
      }
      assertTrue(count > 0);
      reader.close();
      
      dir.close();
    }
  }

  // LUCENE-1130: make sure immeidate disk full on creating
  // an IndexWriter (hit during DW.ThreadState.init()) is
  // OK:
  public void testImmediateDiskFull() throws IOException {
    MockRAMDirectory dir = new MockRAMDirectory();
    IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
    dir.setMaxSizeInBytes(dir.getRecomputedActualSizeInBytes());
    writer.setMaxBufferedDocs(2);
    final Document doc = new Document();
    doc.add(new Field("field", "aaa bbb ccc ddd eee fff ggg hhh iii jjj", Field.Store.YES, Field.Index.TOKENIZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
    try {
      writer.addDocument(doc);
      fail("did not hit disk full");
    } catch (IOException ioe) {
    }
    // Without fix for LUCENE-1130: this call will hang:
    try {
      writer.addDocument(doc);
      fail("did not hit disk full");
    } catch (IOException ioe) {
    }
    try {
      writer.close(false);
      fail("did not hit disk full");
    } catch (IOException ioe) {
    }
  }

  // LUCENE-1130: make sure immeidate disk full on creating
  // an IndexWriter (hit during DW.ThreadState.init()), with
  // multiple threads, is OK:
  public void testImmediateDiskFullWithThreads() throws IOException {

    int NUM_THREADS = 3;

    for(int iter=0;iter<10;iter++) {
      MockRAMDirectory dir = new MockRAMDirectory();
      IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
      ConcurrentMergeScheduler cms = new ConcurrentMergeScheduler();
      // We expect disk full exceptions in the merge threads
      cms.setSuppressExceptions();
      writer.setMergeScheduler(cms);
      writer.setMaxBufferedDocs(2);
      writer.setMergeFactor(4);
      dir.setMaxSizeInBytes(4*1024+20*iter);

      IndexerThread[] threads = new IndexerThread[NUM_THREADS];

      for(int i=0;i<NUM_THREADS;i++)
        threads[i] = new IndexerThread(writer, true);

      for(int i=0;i<NUM_THREADS;i++)
        threads[i].start();

      for(int i=0;i<NUM_THREADS;i++) {
        while(true) {
          try {
            // Without fix for LUCENE-1130: one of the
            // threads will hang
            threads[i].join();
            break;
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
          }
        }
        if (threads[i].isAlive())
          fail("thread seems to be hung");
        else
          assertTrue("hit unexpected Throwable", threads[i].error == null);
      }

      try {
        writer.close(false);
      } catch (IOException ioe) {
      }

      dir.close();
    }
  }

  // Throws IOException during FieldsWriter.flushDocument and during DocumentsWriter.abort
  private static class FailOnlyOnAbortOrFlush extends MockRAMDirectory.Failure {
    private boolean onlyOnce;
    public FailOnlyOnAbortOrFlush(boolean onlyOnce) {
      this.onlyOnce = onlyOnce;
    }
    public void eval(MockRAMDirectory dir)  throws IOException {
      if (doFail) {
        StackTraceElement[] trace = new Exception().getStackTrace();
        for (int i = 0; i < trace.length; i++) {
          if ("abort".equals(trace[i].getMethodName()) ||
              "flushDocument".equals(trace[i].getMethodName())) {
            if (onlyOnce)
              doFail = false;
            throw new IOException("now failing on purpose");
          }
        }
      }
    }
  }

  // Runs test, with one thread, using the specific failure
  // to trigger an IOException
  public void _testSingleThreadFailure(MockRAMDirectory.Failure failure) throws IOException {
    MockRAMDirectory dir = new MockRAMDirectory();

    IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
    writer.setMaxBufferedDocs(2);
    final Document doc = new Document();
    doc.add(new Field("field", "aaa bbb ccc ddd eee fff ggg hhh iii jjj", Field.Store.YES, Field.Index.TOKENIZED, Field.TermVector.WITH_POSITIONS_OFFSETS));

    for(int i=0;i<6;i++)
      writer.addDocument(doc);

    dir.failOn(failure);
    failure.setDoFail();
    try {
      writer.addDocument(doc);
      writer.addDocument(doc);
      fail("did not hit exception");
    } catch (IOException ioe) {
    }
    failure.clearDoFail();
    writer.addDocument(doc);
    writer.close(false);
  }

  // Runs test, with multiple threads, using the specific
  // failure to trigger an IOException
  public void _testMultipleThreadsFailure(MockRAMDirectory.Failure failure) throws IOException {

    int NUM_THREADS = 3;

    for(int iter=0;iter<5;iter++) {
      MockRAMDirectory dir = new MockRAMDirectory();
      IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
      ConcurrentMergeScheduler cms = new ConcurrentMergeScheduler();
      // We expect disk full exceptions in the merge threads
      cms.setSuppressExceptions();
      writer.setMergeScheduler(cms);
      writer.setMaxBufferedDocs(2);
      writer.setMergeFactor(4);

      IndexerThread[] threads = new IndexerThread[NUM_THREADS];

      for(int i=0;i<NUM_THREADS;i++)
        threads[i] = new IndexerThread(writer, true);

      for(int i=0;i<NUM_THREADS;i++)
        threads[i].start();

      try {
        Thread.sleep(10);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
      }

      dir.failOn(failure);
      failure.setDoFail();

      for(int i=0;i<NUM_THREADS;i++) {
        while(true) {
          try {
            threads[i].join();
            break;
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
          }
        }
        if (threads[i].isAlive())
          fail("thread seems to be hung");
        else
          assertTrue("hit unexpected Throwable", threads[i].error == null);
      }

      boolean success = false;
      try {
        writer.close(false);
        success = true;
      } catch (IOException ioe) {
        failure.clearDoFail();
        writer.close(false);
      }

      if (success) {
        IndexReader reader = IndexReader.open(dir);
        for(int j=0;j<reader.maxDoc();j++) {
          if (!reader.isDeleted(j)) {
            reader.document(j);
            reader.getTermFreqVectors(j);
          }
        }
        reader.close();
      }

      dir.close();
    }
  }

  // LUCENE-1130: make sure initial IOException, and then 2nd
  // IOException during abort(), is OK:
  public void testIOExceptionDuringAbort() throws IOException {
    _testSingleThreadFailure(new FailOnlyOnAbortOrFlush(false));
  }

  // LUCENE-1130: make sure initial IOException, and then 2nd
  // IOException during abort(), is OK:
  public void testIOExceptionDuringAbortOnlyOnce() throws IOException {
    _testSingleThreadFailure(new FailOnlyOnAbortOrFlush(true));
  }

  // LUCENE-1130: make sure initial IOException, and then 2nd
  // IOException during abort(), with multiple threads, is OK:
  public void testIOExceptionDuringAbortWithThreads() throws IOException {
    _testMultipleThreadsFailure(new FailOnlyOnAbortOrFlush(false));
  }

  // LUCENE-1130: make sure initial IOException, and then 2nd
  // IOException during abort(), with multiple threads, is OK:
  public void testIOExceptionDuringAbortWithThreadsOnlyOnce() throws IOException {
    _testMultipleThreadsFailure(new FailOnlyOnAbortOrFlush(true));
  }

  // Throws IOException during DocumentsWriter.closeDocStore
  private static class FailOnlyInCloseDocStore extends MockRAMDirectory.Failure {
    private boolean onlyOnce;
    public FailOnlyInCloseDocStore(boolean onlyOnce) {
      this.onlyOnce = onlyOnce;
    }
    public void eval(MockRAMDirectory dir)  throws IOException {
      if (doFail) {
        StackTraceElement[] trace = new Exception().getStackTrace();
        for (int i = 0; i < trace.length; i++) {
          if ("closeDocStore".equals(trace[i].getMethodName())) {
            if (onlyOnce)
              doFail = false;
            throw new IOException("now failing on purpose");
          }
        }
      }
    }
  }

  // LUCENE-1130: test IOException in closeDocStore
  public void testIOExceptionDuringCloseDocStore() throws IOException {
    _testSingleThreadFailure(new FailOnlyInCloseDocStore(false));
  }

  // LUCENE-1130: test IOException in closeDocStore
  public void testIOExceptionDuringCloseDocStoreOnlyOnce() throws IOException {
    _testSingleThreadFailure(new FailOnlyInCloseDocStore(true));
  }

  // LUCENE-1130: test IOException in closeDocStore, with threads
  public void testIOExceptionDuringCloseDocStoreWithThreads() throws IOException {
    _testMultipleThreadsFailure(new FailOnlyInCloseDocStore(false));
  }

  // LUCENE-1130: test IOException in closeDocStore, with threads
  public void testIOExceptionDuringCloseDocStoreWithThreadsOnlyOnce() throws IOException {
    _testMultipleThreadsFailure(new FailOnlyInCloseDocStore(true));
  }

  // Throws IOException during DocumentsWriter.writeSegment
  private static class FailOnlyInWriteSegment extends MockRAMDirectory.Failure {
    private boolean onlyOnce;
    public FailOnlyInWriteSegment(boolean onlyOnce) {
      this.onlyOnce = onlyOnce;
    }
    public void eval(MockRAMDirectory dir)  throws IOException {
      if (doFail) {
        StackTraceElement[] trace = new Exception().getStackTrace();
        for (int i = 0; i < trace.length; i++) {
          if ("writeSegment".equals(trace[i].getMethodName())) {
            if (onlyOnce)
              doFail = false;
            // new RuntimeException().printStackTrace(System.out);
            throw new IOException("now failing on purpose");
          }
        }
      }
    }
  }

  // LUCENE-1130: test IOException in writeSegment
  public void testIOExceptionDuringWriteSegment() throws IOException {
    _testSingleThreadFailure(new FailOnlyInWriteSegment(false));
  }

  // LUCENE-1130: test IOException in writeSegment
  public void testIOExceptionDuringWriteSegmentOnlyOnce() throws IOException {
    _testSingleThreadFailure(new FailOnlyInWriteSegment(true));
  }

  // LUCENE-1130: test IOException in writeSegment, with threads
  public void testIOExceptionDuringWriteSegmentWithThreads() throws IOException {
    _testMultipleThreadsFailure(new FailOnlyInWriteSegment(false));
  }

  // LUCENE-1130: test IOException in writeSegment, with threads
  public void testIOExceptionDuringWriteSegmentWithThreadsOnlyOnce() throws IOException {
    _testMultipleThreadsFailure(new FailOnlyInWriteSegment(true));
  }

  // LUCENE-1084: test unlimited field length
  public void testUnlimitedMaxFieldLength() throws IOException {
    Directory dir = new MockRAMDirectory();

    IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.UNLIMITED);

    Document doc = new Document();
    StringBuffer b = new StringBuffer();
    for(int i=0;i<10000;i++)
      b.append(" a");
    b.append(" x");
    doc.add(new Field("field", b.toString(), Field.Store.NO, Field.Index.TOKENIZED));
    writer.addDocument(doc);
    writer.close();

    IndexReader reader = IndexReader.open(dir);
    Term t = new Term("field", "x");
    assertEquals(1, reader.docFreq(t));
    reader.close();
    dir.close();
  }

  // LUCENE-1044: Simulate checksum error in segments_N
  public void testSegmentsChecksumError() throws IOException {
    Directory dir = new MockRAMDirectory();

    IndexWriter writer = null;

    writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);

    // add 100 documents
    for (int i = 0; i < 100; i++) {
      addDoc(writer);
    }

    // close
    writer.close();

    long gen = SegmentInfos.getCurrentSegmentGeneration(dir);
    assertTrue("segment generation should be > 1 but got " + gen, gen > 1);

    final String segmentsFileName = SegmentInfos.getCurrentSegmentFileName(dir);
    IndexInput in = dir.openInput(segmentsFileName);
    IndexOutput out = dir.createOutput(IndexFileNames.fileNameFromGeneration(IndexFileNames.SEGMENTS, "", 1+gen));
    out.copyBytes(in, in.length()-1);
    byte b = in.readByte();
    out.writeByte((byte) (1+b));
    out.close();
    in.close();

    IndexReader reader = null;
    try {
      reader = IndexReader.open(dir);
    } catch (IOException e) {
      e.printStackTrace(System.out);
      fail("segmentInfos failed to retry fallback to correct segments_N file");
    }
    reader.close();
  }

  // LUCENE-1044: test writer.commit() when ac=false
  public void testForceCommit() throws IOException {
    Directory dir = new MockRAMDirectory();

    IndexWriter writer  = new IndexWriter(dir, false, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
    writer.setMaxBufferedDocs(2);
    writer.setMergeFactor(5);

    for (int i = 0; i < 23; i++)
      addDoc(writer);

    IndexReader reader = IndexReader.open(dir);
    assertEquals(0, reader.numDocs());
    writer.commit();
    IndexReader reader2 = reader.reopen();
    assertEquals(0, reader.numDocs());
    assertEquals(23, reader2.numDocs());
    reader.close();

    for (int i = 0; i < 17; i++)
      addDoc(writer);
    assertEquals(23, reader2.numDocs());
    reader2.close();
    reader = IndexReader.open(dir);
    assertEquals(23, reader.numDocs());
    reader.close();
    writer.commit();

    reader = IndexReader.open(dir);
    assertEquals(40, reader.numDocs());
    reader.close();
    writer.close();
    dir.close();
  }

  // Throws IOException during MockRAMDirectory.sync
  private static class FailOnlyInSync extends MockRAMDirectory.Failure {
    boolean didFail;
    public void eval(MockRAMDirectory dir)  throws IOException {
      if (doFail) {
        StackTraceElement[] trace = new Exception().getStackTrace();
        for (int i = 0; i < trace.length; i++) {
          if (doFail && "org.apache.lucene.store.MockRAMDirectory".equals(trace[i].getClassName()) && "sync".equals(trace[i].getMethodName())) {
            didFail = true;
            throw new IOException("now failing on purpose during sync");
          }
        }
      }
    }
  }

  // LUCENE-1044: test exception during sync
  public void testExceptionDuringSync() throws IOException {
    MockRAMDirectory dir = new MockRAMDirectory();
    FailOnlyInSync failure = new FailOnlyInSync();
    dir.failOn(failure);

    IndexWriter writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
    failure.setDoFail();

    ConcurrentMergeScheduler cms = new ConcurrentMergeScheduler();
    // We expect sync exceptions in the merge threads
    cms.setSuppressExceptions();
    writer.setMergeScheduler(cms);
    writer.setMaxBufferedDocs(2);
    writer.setMergeFactor(5);

    for (int i = 0; i < 23; i++)
      addDoc(writer);

    cms.sync();
    assertTrue(failure.didFail);
    failure.clearDoFail();
    writer.close();

    IndexReader reader = IndexReader.open(dir);
    assertEquals(23, reader.numDocs());
    reader.close();
    dir.close();
  }

  // LUCENE-1168
  public void testTermVectorCorruption() throws IOException {

    Directory dir = new MockRAMDirectory();
    for(int iter=0;iter<4;iter++) {
      final boolean autoCommit = 1==iter/2;
      IndexWriter writer = new IndexWriter(dir,
                                           autoCommit, new StandardAnalyzer(),
                                           IndexWriter.MaxFieldLength.LIMITED);
      writer.setMaxBufferedDocs(2);
      writer.setRAMBufferSizeMB(IndexWriter.DISABLE_AUTO_FLUSH);
      writer.setMergeScheduler(new SerialMergeScheduler());
      writer.setMergePolicy(new LogDocMergePolicy());

      Document document = new Document();

      Field storedField = new Field("stored", "stored", Field.Store.YES,
                                    Field.Index.NO);
      document.add(storedField);
      writer.addDocument(document);
      writer.addDocument(document);

      document = new Document();
      document.add(storedField);
      Field termVectorField = new Field("termVector", "termVector",
                                        Field.Store.NO, Field.Index.UN_TOKENIZED,
                                        Field.TermVector.WITH_POSITIONS_OFFSETS);

      document.add(termVectorField);
      writer.addDocument(document);
      writer.optimize();
      writer.close();

      IndexReader reader = IndexReader.open(dir);
      for(int i=0;i<reader.numDocs();i++) {
        reader.document(i);
        reader.getTermFreqVectors(i);
      }
      reader.close();

      writer = new IndexWriter(dir,
                               autoCommit, new StandardAnalyzer(),
                               IndexWriter.MaxFieldLength.LIMITED);
      writer.setMaxBufferedDocs(2);
      writer.setRAMBufferSizeMB(IndexWriter.DISABLE_AUTO_FLUSH);
      writer.setMergeScheduler(new SerialMergeScheduler());
      writer.setMergePolicy(new LogDocMergePolicy());

      Directory[] indexDirs = { dir};
      writer.addIndexes(indexDirs);
      writer.close();
    }
    dir.close();
  }

  // LUCENE-1168
  public void testTermVectorCorruption2() throws IOException {
    Directory dir = new MockRAMDirectory();
    for(int iter=0;iter<4;iter++) {
      final boolean autoCommit = 1==iter/2;
      IndexWriter writer = new IndexWriter(dir,
                                           autoCommit, new StandardAnalyzer(),
                                           IndexWriter.MaxFieldLength.LIMITED);
      writer.setMaxBufferedDocs(2);
      writer.setRAMBufferSizeMB(IndexWriter.DISABLE_AUTO_FLUSH);
      writer.setMergeScheduler(new SerialMergeScheduler());
      writer.setMergePolicy(new LogDocMergePolicy());

      Document document = new Document();

      Field storedField = new Field("stored", "stored", Field.Store.YES,
                                    Field.Index.NO);
      document.add(storedField);
      writer.addDocument(document);
      writer.addDocument(document);

      document = new Document();
      document.add(storedField);
      Field termVectorField = new Field("termVector", "termVector",
                                        Field.Store.NO, Field.Index.UN_TOKENIZED,
                                        Field.TermVector.WITH_POSITIONS_OFFSETS);
      document.add(termVectorField);
      writer.addDocument(document);
      writer.optimize();
      writer.close();

      IndexReader reader = IndexReader.open(dir);
      assertTrue(reader.getTermFreqVectors(0)==null);
      assertTrue(reader.getTermFreqVectors(1)==null);
      assertTrue(reader.getTermFreqVectors(2)!=null);
      reader.close();
    }
    dir.close();
  }

  // LUCENE-1168
  public void testTermVectorCorruption3() throws IOException {
    Directory dir = new MockRAMDirectory();
    IndexWriter writer = new IndexWriter(dir,
                                         false, new StandardAnalyzer(),
                                         IndexWriter.MaxFieldLength.LIMITED);
    writer.setMaxBufferedDocs(2);
    writer.setRAMBufferSizeMB(IndexWriter.DISABLE_AUTO_FLUSH);
    writer.setMergeScheduler(new SerialMergeScheduler());
    writer.setMergePolicy(new LogDocMergePolicy());

    Document document = new Document();

    document = new Document();
    Field storedField = new Field("stored", "stored", Field.Store.YES,
                                  Field.Index.NO);
    document.add(storedField);
    Field termVectorField = new Field("termVector", "termVector",
                                      Field.Store.NO, Field.Index.UN_TOKENIZED,
                                      Field.TermVector.WITH_POSITIONS_OFFSETS);
    document.add(termVectorField);
    for(int i=0;i<10;i++)
      writer.addDocument(document);
    writer.close();

    writer = new IndexWriter(dir,
                             false, new StandardAnalyzer(),
                             IndexWriter.MaxFieldLength.LIMITED);
    writer.setMaxBufferedDocs(2);
    writer.setRAMBufferSizeMB(IndexWriter.DISABLE_AUTO_FLUSH);
    writer.setMergeScheduler(new SerialMergeScheduler());
    writer.setMergePolicy(new LogDocMergePolicy());
    for(int i=0;i<6;i++)
      writer.addDocument(document);

    writer.optimize();
    writer.close();

    IndexReader reader = IndexReader.open(dir);
    for(int i=0;i<10;i++) {
      reader.getTermFreqVectors(i);
      reader.document(i);
    }
    reader.close();
    dir.close();
  }

  // LUCENE-1084: test user-specified field length
  public void testUserSpecifiedMaxFieldLength() throws IOException {
    Directory dir = new MockRAMDirectory();

    IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), new IndexWriter.MaxFieldLength(100000));

    Document doc = new Document();
    StringBuffer b = new StringBuffer();
    for(int i=0;i<10000;i++)
      b.append(" a");
    b.append(" x");
    doc.add(new Field("field", b.toString(), Field.Store.NO, Field.Index.TOKENIZED));
    writer.addDocument(doc);
    writer.close();

    IndexReader reader = IndexReader.open(dir);
    Term t = new Term("field", "x");
    assertEquals(1, reader.docFreq(t));
    reader.close();
    dir.close();
  }

  // LUCENE-325: test expungeDeletes, when 2 singular merges
  // are required
  public void testExpungeDeletes() throws IOException {
    Directory dir = new MockRAMDirectory();
    IndexWriter writer = new IndexWriter(dir,
                                         false, new StandardAnalyzer(),
                                         IndexWriter.MaxFieldLength.LIMITED);
    writer.setMaxBufferedDocs(2);
    writer.setRAMBufferSizeMB(IndexWriter.DISABLE_AUTO_FLUSH);

    Document document = new Document();

    document = new Document();
    Field storedField = new Field("stored", "stored", Field.Store.YES,
                                  Field.Index.NO);
    document.add(storedField);
    Field termVectorField = new Field("termVector", "termVector",
                                      Field.Store.NO, Field.Index.UN_TOKENIZED,
                                      Field.TermVector.WITH_POSITIONS_OFFSETS);
    document.add(termVectorField);
    for(int i=0;i<10;i++)
      writer.addDocument(document);
    writer.close();

    IndexReader ir = IndexReader.open(dir);
    assertEquals(10, ir.maxDoc());
    assertEquals(10, ir.numDocs());
    ir.deleteDocument(0);
    ir.deleteDocument(7);
    assertEquals(8, ir.numDocs());
    ir.close();

    writer = new IndexWriter(dir,
                             false, new StandardAnalyzer(),
                             IndexWriter.MaxFieldLength.LIMITED);
    writer.expungeDeletes();
    writer.close();
    ir = IndexReader.open(dir);
    assertEquals(8, ir.maxDoc());
    assertEquals(8, ir.numDocs());
    ir.close();
    dir.close();
  }

  // LUCENE-325: test expungeDeletes, when many adjacent merges are required
  public void testExpungeDeletes2() throws IOException {
    Directory dir = new MockRAMDirectory();
    IndexWriter writer = new IndexWriter(dir,
                                         false, new StandardAnalyzer(),
                                         IndexWriter.MaxFieldLength.LIMITED);
    writer.setMaxBufferedDocs(2);
    writer.setMergeFactor(50);
    writer.setRAMBufferSizeMB(IndexWriter.DISABLE_AUTO_FLUSH);

    Document document = new Document();

    document = new Document();
    Field storedField = new Field("stored", "stored", Field.Store.YES,
                                  Field.Index.NO);
    document.add(storedField);
    Field termVectorField = new Field("termVector", "termVector",
                                      Field.Store.NO, Field.Index.UN_TOKENIZED,
                                      Field.TermVector.WITH_POSITIONS_OFFSETS);
    document.add(termVectorField);
    for(int i=0;i<98;i++)
      writer.addDocument(document);
    writer.close();

    IndexReader ir = IndexReader.open(dir);
    assertEquals(98, ir.maxDoc());
    assertEquals(98, ir.numDocs());
    for(int i=0;i<98;i+=2)
      ir.deleteDocument(i);
    assertEquals(49, ir.numDocs());
    ir.close();

    writer = new IndexWriter(dir,
                             false, new StandardAnalyzer(),
                             IndexWriter.MaxFieldLength.LIMITED);
    writer.setMergeFactor(3);
    writer.expungeDeletes();
    writer.close();
    ir = IndexReader.open(dir);
    assertEquals(49, ir.maxDoc());
    assertEquals(49, ir.numDocs());
    ir.close();
    dir.close();
  }

  // LUCENE-325: test expungeDeletes without waiting, when
  // many adjacent merges are required
  public void testExpungeDeletes3() throws IOException {
    Directory dir = new MockRAMDirectory();
    IndexWriter writer = new IndexWriter(dir,
                                         false, new StandardAnalyzer(),
                                         IndexWriter.MaxFieldLength.LIMITED);
    writer.setMaxBufferedDocs(2);
    writer.setMergeFactor(50);
    writer.setRAMBufferSizeMB(IndexWriter.DISABLE_AUTO_FLUSH);

    Document document = new Document();

    document = new Document();
    Field storedField = new Field("stored", "stored", Field.Store.YES,
                                  Field.Index.NO);
    document.add(storedField);
    Field termVectorField = new Field("termVector", "termVector",
                                      Field.Store.NO, Field.Index.UN_TOKENIZED,
                                      Field.TermVector.WITH_POSITIONS_OFFSETS);
    document.add(termVectorField);
    for(int i=0;i<98;i++)
      writer.addDocument(document);
    writer.close();

    IndexReader ir = IndexReader.open(dir);
    assertEquals(98, ir.maxDoc());
    assertEquals(98, ir.numDocs());
    for(int i=0;i<98;i+=2)
      ir.deleteDocument(i);
    assertEquals(49, ir.numDocs());
    ir.close();

    writer = new IndexWriter(dir,
                             false, new StandardAnalyzer(),
                             IndexWriter.MaxFieldLength.LIMITED);
    // Force many merges to happen
    writer.setMergeFactor(3);
    writer.expungeDeletes(false);
    writer.close();
    ir = IndexReader.open(dir);
    assertEquals(49, ir.maxDoc());
    assertEquals(49, ir.numDocs());
    ir.close();
    dir.close();
  }
}
