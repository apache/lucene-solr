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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Reader;
import java.io.StringReader;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CachingTokenFilter;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.codecs.CodecProvider;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.Field.TermVector;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.MockRAMDirectory;
import org.apache.lucene.store.NoLockFactory;
import org.apache.lucene.store.MockRAMDirectory;
import org.apache.lucene.store.SingleInstanceLockFactory;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util._TestUtil;
import org.apache.lucene.util.ThreadInterruptedException;
import org.apache.lucene.util.BytesRef;

public class TestIndexWriter extends LuceneTestCase {
    Random random;
    
    @Override
    public void setUp() throws Exception {
      super.setUp();
      random = newRandom();
    }
    
    public TestIndexWriter(String name) {
      super(name);
    }

    public void testDocCount() throws IOException {
        Directory dir = newDirectory(random);

        IndexWriter writer = null;
        IndexReader reader = null;
        int i;

        long savedWriteLockTimeout = IndexWriterConfig.getDefaultWriteLockTimeout();
        try {
          IndexWriterConfig.setDefaultWriteLockTimeout(2000);
          assertEquals(2000, IndexWriterConfig.getDefaultWriteLockTimeout());
          writer  = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()));
        } finally {
          IndexWriterConfig.setDefaultWriteLockTimeout(savedWriteLockTimeout);
        }

        // add 100 documents
        for (i = 0; i < 100; i++) {
            addDoc(writer);
        }
        assertEquals(100, writer.maxDoc());
        writer.close();

        // delete 40 documents
        reader = IndexReader.open(dir, false);
        for (i = 0; i < 40; i++) {
            reader.deleteDocument(i);
        }
        reader.close();

        // test doc count before segments are merged/index is optimized
        writer = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()));
        assertEquals(100, writer.maxDoc());
        writer.close();

        reader = IndexReader.open(dir, true);
        assertEquals(100, reader.maxDoc());
        assertEquals(60, reader.numDocs());
        reader.close();

        // optimize the index and check that the new doc count is correct
        writer = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()));
        assertEquals(100, writer.maxDoc());
        assertEquals(60, writer.numDocs());
        writer.optimize();
        assertEquals(60, writer.maxDoc());
        assertEquals(60, writer.numDocs());
        writer.close();

        // check that the index reader gives the same numbers.
        reader = IndexReader.open(dir, true);
        assertEquals(60, reader.maxDoc());
        assertEquals(60, reader.numDocs());
        reader.close();

        // make sure opening a new index for create over
        // this existing one works correctly:
        writer = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()).setOpenMode(OpenMode.CREATE));
        assertEquals(0, writer.maxDoc());
        assertEquals(0, writer.numDocs());
        writer.close();
        dir.close();
    }

    private static void addDoc(IndexWriter writer) throws IOException
    {
        Document doc = new Document();
        doc.add(new Field("content", "aaa", Field.Store.NO, Field.Index.ANALYZED));
        writer.addDocument(doc);
    }

    private void addDocWithIndex(IndexWriter writer, int index) throws IOException
    {
        Document doc = new Document();
        doc.add(new Field("content", "aaa " + index, Field.Store.YES, Field.Index.ANALYZED));
        doc.add(new Field("id", "" + index, Field.Store.YES, Field.Index.ANALYZED));
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

      // Build up a bunch of dirs that have indexes which we
      // will then merge together by calling addIndexes(*):
      Directory[] dirs = new Directory[NUM_DIR];
      long inputDiskUsage = 0;
      for(int i=0;i<NUM_DIR;i++) {
        dirs[i] = newDirectory(random);
        IndexWriter writer  = new IndexWriter(dirs[i], newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()));
        for(int j=0;j<25;j++) {
          addDocWithIndex(writer, 25*i+j);
        }
        writer.close();
        String[] files = dirs[i].listAll();
        for(int j=0;j<files.length;j++) {
          inputDiskUsage += dirs[i].fileLength(files[j]);
        }
      }

      // Now, build a starting index that has START_COUNT docs.  We
      // will then try to addIndexesNoOptimize into a copy of this:
      MockRAMDirectory startDir = newDirectory(random);
      IndexWriter writer = new IndexWriter(startDir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()));
      for(int j=0;j<START_COUNT;j++) {
        addDocWithIndex(writer, j);
      }
      writer.close();

      // Make sure starting index seems to be working properly:
      Term searchTerm = new Term("content", "aaa");        
      IndexReader reader = IndexReader.open(startDir, true);
      assertEquals("first docFreq", 57, reader.docFreq(searchTerm));

      IndexSearcher searcher = new IndexSearcher(reader);
      ScoreDoc[] hits = searcher.search(new TermQuery(searchTerm), null, 1000).scoreDocs;
      assertEquals("first number of hits", 57, hits.length);
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

      // String[] files = startDir.listAll();
      long diskUsage = startDir.sizeInBytes();

      long startDiskUsage = 0;
      String[] files = startDir.listAll();
      for(int i=0;i<files.length;i++) {
        startDiskUsage += startDir.fileLength(files[i]);
      }

      for(int iter=0;iter<3;iter++) {

        if (VERBOSE)
          System.out.println("TEST: iter=" + iter);

        // Start with 100 bytes more than we are currently using:
        long diskFree = diskUsage+100;

        int method = iter;

        boolean success = false;
        boolean done = false;

        String methodName;
        if (0 == method) {
          methodName = "addIndexes(Directory[]) + optimize()";
        } else if (1 == method) {
          methodName = "addIndexes(IndexReader[])";
        } else {
          methodName = "addIndexes(Directory[])";
        }

        while(!done) {

          // Make a new dir that will enforce disk usage:
          MockRAMDirectory dir = new MockRAMDirectory(startDir);
          writer = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()).setOpenMode(OpenMode.APPEND));
          IOException err = null;

          MergeScheduler ms = writer.getConfig().getMergeScheduler();
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
              if (VERBOSE)
                testName = "disk full test " + methodName + " with disk full at " + diskFree + " bytes";
            } else {
              thisDiskFree = 0;
              rate = 0.0;
              if (VERBOSE)
                testName = "disk full test " + methodName + " with unlimited disk space";
            }

            if (VERBOSE)
              System.out.println("\ncycle: " + testName);

            dir.setMaxSizeInBytes(thisDiskFree);
            dir.setRandomIOExceptionRate(rate, diskFree);

            try {

              if (0 == method) {
                writer.addIndexes(dirs);
                writer.optimize();
              } else if (1 == method) {
                IndexReader readers[] = new IndexReader[dirs.length];
                for(int i=0;i<dirs.length;i++) {
                  readers[i] = IndexReader.open(dirs[i], true);
                }
                try {
                  writer.addIndexes(readers);
                } finally {
                  for(int i=0;i<dirs.length;i++) {
                    readers[i].close();
                  }
                }
              } else {
                writer.addIndexes(dirs);
              }

              success = true;
              if (VERBOSE) {
                System.out.println("  success!");
              }

              if (0 == x) {
                done = true;
              }

            } catch (IOException e) {
              success = false;
              err = e;
              if (VERBOSE) {
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

            if (VERBOSE) {
              System.out.println("  now test readers");
            }

            // Finally, verify index is not corrupt, and, if
            // we succeeded, we see all docs added, and if we
            // failed, we see either all docs or no docs added
            // (transactional semantics):
            try {
              reader = IndexReader.open(dir, true);
            } catch (IOException e) {
              e.printStackTrace(System.out);
              fail(testName + ": exception when creating IndexReader: " + e);
            }
            int result = reader.docFreq(searchTerm);
            if (success) {
              if (result != START_COUNT) {
                fail(testName + ": method did not throw exception but docFreq('aaa') is " + result + " instead of expected " + START_COUNT);
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
              hits = searcher.search(new TermQuery(searchTerm), null, END_COUNT).scoreDocs;
            } catch (IOException e) {
              e.printStackTrace(System.out);
              fail(testName + ": exception when searching: " + e);
            }
            int result2 = hits.length;
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
            if (VERBOSE) {
              System.out.println("  count is " + result);
            }

            if (done || result == END_COUNT) {
              break;
            }
          }

          if (VERBOSE) {
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

          // Make sure we don't hit disk full during close below:
          dir.setMaxSizeInBytes(0);
          dir.setRandomIOExceptionRate(0.0, 0);

          writer.close();

          // Wait for all BG threads to finish else
          // dir.close() will throw IOException because
          // there are still open files
          _TestUtil.syncConcurrentMerges(ms);

          dir.close();

          // Try again with 5000 more bytes of free space:
          diskFree += 5000;
        }
      }

      startDir.close();
      for (Directory dir : dirs)
        dir.close();
    }

    /*
     * Make sure IndexWriter cleans up on hitting a disk
     * full exception in addDocument.
     */
    public void testAddDocumentOnDiskFull() throws IOException {

      for(int pass=0;pass<2;pass++) {
        if (VERBOSE)
          System.out.println("TEST: pass=" + pass);
        boolean doAbort = pass == 1;
        long diskFree = 200;
        while(true) {
          if (VERBOSE)
            System.out.println("TEST: cycle: diskFree=" + diskFree);
          MockRAMDirectory dir = newDirectory(random);
          dir.setMaxSizeInBytes(diskFree);
          IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()));
          MergeScheduler ms = writer.getConfig().getMergeScheduler();
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
            writer.commit();
          } catch (IOException e) {
            if (VERBOSE) {
              System.out.println("TEST: exception on addDoc");
              e.printStackTrace(System.out);
            }
            hitError = true;
          }

          if (hitError) {
            if (doAbort) {
              writer.rollback();
            } else {
              try {
                writer.close();
              } catch (IOException e) {
                if (VERBOSE) {
                  System.out.println("TEST: exception on close");
                  e.printStackTrace(System.out);
                }
                dir.setMaxSizeInBytes(0);
                writer.close();
              }
            }

            //_TestUtil.syncConcurrentMerges(ms);

            if (dir.listAll().length > 0) {
              assertNoUnreferencedFiles(dir, "after disk full during addDocument");
              
              // Make sure reader can open the index:
              IndexReader.open(dir, true).close();
            }
              
            dir.close();
            // Now try again w/ more space:

            diskFree += 500;
          } else {
            //_TestUtil.syncConcurrentMerges(writer);
            writer.close();
            dir.close();
            break;
          }
        }
      }
    }                                               

    public static void assertNoUnreferencedFiles(Directory dir, String message) throws IOException {
      String[] startFiles = dir.listAll();
      SegmentInfos infos = new SegmentInfos();
      infos.read(dir);
      new IndexFileDeleter(dir, new KeepOnlyLastCommitDeletionPolicy(), infos, null, null, CodecProvider.getDefault());
      String[] endFiles = dir.listAll();

      Arrays.sort(startFiles);
      Arrays.sort(endFiles);

      if (!Arrays.equals(startFiles, endFiles)) {
        fail(message + ": before delete:\n    " + arrayToString(startFiles) + "\n  after delete:\n    " + arrayToString(endFiles));
      }
    }

    public void testOptimizeMaxNumSegments() throws IOException {

      MockRAMDirectory dir = newDirectory(random);

      final Document doc = new Document();
      doc.add(new Field("content", "aaa", Field.Store.YES, Field.Index.ANALYZED));

      for(int numDocs=38;numDocs<500;numDocs += 38) {
        LogDocMergePolicy ldmp = new LogDocMergePolicy();
        ldmp.setMinMergeDocs(1);
        ldmp.setMergeFactor(5);
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(random,
          TEST_VERSION_CURRENT, new MockAnalyzer())
          .setOpenMode(OpenMode.CREATE).setMaxBufferedDocs(2).setMergePolicy(
              ldmp));
        for(int j=0;j<numDocs;j++)
          writer.addDocument(doc);
        writer.close();

        SegmentInfos sis = new SegmentInfos();
        sis.read(dir);
        final int segCount = sis.size();

        ldmp = new LogDocMergePolicy();
        ldmp.setMergeFactor(5);
        writer = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT,
          new MockAnalyzer()).setMergePolicy(ldmp));
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
      dir.close();
    }

    public void testOptimizeMaxNumSegments2() throws IOException {
      MockRAMDirectory dir = newDirectory(random);

      final Document doc = new Document();
      doc.add(new Field("content", "aaa", Field.Store.YES, Field.Index.ANALYZED));

      LogDocMergePolicy ldmp = new LogDocMergePolicy();
      ldmp.setMinMergeDocs(1);
      ldmp.setMergeFactor(4);
      IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(random,
        TEST_VERSION_CURRENT, new MockAnalyzer())
        .setMaxBufferedDocs(2).setMergePolicy(ldmp).setMergeScheduler(new ConcurrentMergeScheduler()));

      for(int iter=0;iter<10;iter++) {
        for(int i=0;i<19;i++)
          writer.addDocument(doc);

        ((ConcurrentMergeScheduler) writer.getConfig().getMergeScheduler()).sync();
        writer.commit();

        SegmentInfos sis = new SegmentInfos();
        sis.read(dir);

        final int segCount = sis.size();

        writer.optimize(7);
        writer.commit();

        sis = new SegmentInfos();
        ((ConcurrentMergeScheduler) writer.getConfig().getMergeScheduler()).sync();
        sis.read(dir);
        final int optSegCount = sis.size();

        if (segCount < 7)
          assertEquals(segCount, optSegCount);
        else
          assertEquals(7, optSegCount);
      }
      writer.close();
      dir.close();
    }

    /**
     * Make sure optimize doesn't use any more than 1X
     * starting index size as its temporary free space
     * required.
     */
    public void testOptimizeTempSpaceUsage() throws IOException {
    
      MockRAMDirectory dir = newDirectory(random);
      IndexWriter writer  = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()).setMaxBufferedDocs(10));

      for(int j=0;j<500;j++) {
        addDocWithIndex(writer, j);
      }
      writer.close();

      long startDiskUsage = 0;
      String[] files = dir.listAll();
      for(int i=0;i<files.length;i++) {
        startDiskUsage += dir.fileLength(files[i]);
      }

      dir.resetMaxUsedSizeInBytes();
      writer  = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()).setOpenMode(OpenMode.APPEND));
      writer.optimize();
      writer.close();
      long maxDiskUsage = dir.getMaxUsedSizeInBytes();

      assertTrue("optimized used too much temporary space: starting usage was " + startDiskUsage + " bytes; max temp usage was " + maxDiskUsage + " but should have been " + (3*startDiskUsage) + " (= 3X starting usage)",
                 maxDiskUsage <= 3*startDiskUsage);
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
        File indexDir = _TestUtil.getTempDir("lucenetestindexwriter");

        try {
          Directory dir = FSDirectory.open(indexDir);

          // add one document & close writer
          IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()));
          addDoc(writer);
          writer.close();

          // now open reader:
          IndexReader reader = IndexReader.open(dir, true);
          assertEquals("should be one document", reader.numDocs(), 1);

          // now open index for create:
          writer = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()).setOpenMode(OpenMode.CREATE));
          assertEquals("should be zero documents", writer.maxDoc(), 0);
          addDoc(writer);
          writer.close();

          assertEquals("should be one document", reader.numDocs(), 1);
          IndexReader reader2 = IndexReader.open(dir, true);
          assertEquals("should be one document", reader2.numDocs(), 1);
          reader.close();
          reader2.close();
        } finally {
          rmDir(indexDir);
        }
    }

    // Simulate a writer that crashed while writing segments
    // file: make sure we can still open the index (ie,
    // gracefully fallback to the previous segments file),
    // and that we can add to the index:
    public void testSimulatedCrashedWriter() throws IOException {
        MockRAMDirectory dir = newDirectory(random);
        dir.setPreventDoubleWrite(false);

        IndexWriter writer = null;

        writer  = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()));

        // add 100 documents
        for (int i = 0; i < 100; i++) {
            addDoc(writer);
        }

        // close
        writer.close();

        long gen = SegmentInfos.getCurrentSegmentGeneration(dir);
        assertTrue("segment generation should be > 0 but got " + gen, gen > 0);

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
          reader = IndexReader.open(dir, true);
        } catch (Exception e) {
          fail("reader failed to open on a crashed index");
        }
        reader.close();

        try {
          writer  = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()).setOpenMode(OpenMode.CREATE));
        } catch (Exception e) {
          e.printStackTrace(System.out);
          fail("writer failed to open on a crashed index");
        }

        // add 100 documents
        for (int i = 0; i < 100; i++) {
            addDoc(writer);
        }

        // close
        writer.close();
        dir.close();
    }

    // Simulate a corrupt index by removing last byte of
    // latest segments file and make sure we get an
    // IOException trying to open the index:
    public void testSimulatedCorruptIndex1() throws IOException {
        Directory dir = newDirectory(random);

        IndexWriter writer = null;

        writer  = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()));

        // add 100 documents
        for (int i = 0; i < 100; i++) {
            addDoc(writer);
        }

        // close
        writer.close();

        long gen = SegmentInfos.getCurrentSegmentGeneration(dir);
        assertTrue("segment generation should be > 0 but got " + gen, gen > 0);

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
          reader = IndexReader.open(dir, true);
          fail("reader did not hit IOException on opening a corrupt index");
        } catch (Exception e) {
        }
        if (reader != null) {
          reader.close();
        }
        dir.close();
    }

    public void testChangesAfterClose() throws IOException {
        Directory dir = newDirectory(random);

        IndexWriter writer = null;

        writer  = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()));
        addDoc(writer);

        // close
        writer.close();
        try {
          addDoc(writer);
          fail("did not hit AlreadyClosedException");
        } catch (AlreadyClosedException e) {
          // expected
        }
        dir.close();
    }
  

    // Simulate a corrupt index by removing one of the cfs
    // files and make sure we get an IOException trying to
    // open the index:
    public void testSimulatedCorruptIndex2() throws IOException {
        Directory dir = newDirectory(random);

        IndexWriter writer = null;

        writer  = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()));
        ((LogMergePolicy) writer.getMergePolicy()).setUseCompoundFile(true);

        // add 100 documents
        for (int i = 0; i < 100; i++) {
            addDoc(writer);
        }

        // close
        writer.close();

        long gen = SegmentInfos.getCurrentSegmentGeneration(dir);
        assertTrue("segment generation should be > 0 but got " + gen, gen > 0);

        String[] files = dir.listAll();
        boolean corrupted = false;
        for(int i=0;i<files.length;i++) {
          if (files[i].endsWith(".cfs")) {
            dir.deleteFile(files[i]);
            corrupted = true;
            break;
          }
        }
        assertTrue("failed to find cfs file to remove", corrupted);

        IndexReader reader = null;
        try {
          reader = IndexReader.open(dir, true);
          fail("reader did not hit IOException on opening a corrupt index");
        } catch (Exception e) {
        }
        if (reader != null) {
          reader.close();
        }
        dir.close();
    }

    /*
     * Simple test for "commit on close": open writer then
     * add a bunch of docs, making sure reader does not see
     * these docs until writer is closed.
     */
    public void testCommitOnClose() throws IOException {
        Directory dir = newDirectory(random);      
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()));
        for (int i = 0; i < 14; i++) {
          addDoc(writer);
        }
        writer.close();

        Term searchTerm = new Term("content", "aaa");        
        IndexSearcher searcher = new IndexSearcher(dir, false);
        ScoreDoc[] hits = searcher.search(new TermQuery(searchTerm), null, 1000).scoreDocs;
        assertEquals("first number of hits", 14, hits.length);
        searcher.close();

        IndexReader reader = IndexReader.open(dir, true);

        writer = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()));
        for(int i=0;i<3;i++) {
          for(int j=0;j<11;j++) {
            addDoc(writer);
          }
          searcher = new IndexSearcher(dir, false);
          hits = searcher.search(new TermQuery(searchTerm), null, 1000).scoreDocs;
          assertEquals("reader incorrectly sees changes from writer", 14, hits.length);
          searcher.close();
          assertTrue("reader should have still been current", reader.isCurrent());
        }

        // Now, close the writer:
        writer.close();
        assertFalse("reader should not be current now", reader.isCurrent());

        searcher = new IndexSearcher(dir, false);
        hits = searcher.search(new TermQuery(searchTerm), null, 1000).scoreDocs;
        assertEquals("reader did not see changes after writer was closed", 47, hits.length);
        searcher.close();
        reader.close();
        dir.close();
    }

    /*
     * Simple test for "commit on close": open writer, then
     * add a bunch of docs, making sure reader does not see
     * them until writer has closed.  Then instead of
     * closing the writer, call abort and verify reader sees
     * nothing was added.  Then verify we can open the index
     * and add docs to it.
     */
    public void testCommitOnCloseAbort() throws IOException {
      MockRAMDirectory dir = newDirectory(random);      
      IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()).setMaxBufferedDocs(10));
      for (int i = 0; i < 14; i++) {
        addDoc(writer);
      }
      writer.close();

      Term searchTerm = new Term("content", "aaa");        
      IndexSearcher searcher = new IndexSearcher(dir, false);
      ScoreDoc[] hits = searcher.search(new TermQuery(searchTerm), null, 1000).scoreDocs;
      assertEquals("first number of hits", 14, hits.length);
      searcher.close();

      writer = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer())
        .setOpenMode(OpenMode.APPEND).setMaxBufferedDocs(10));
      for(int j=0;j<17;j++) {
        addDoc(writer);
      }
      // Delete all docs:
      writer.deleteDocuments(searchTerm);

      searcher = new IndexSearcher(dir, false);
      hits = searcher.search(new TermQuery(searchTerm), null, 1000).scoreDocs;
      assertEquals("reader incorrectly sees changes from writer", 14, hits.length);
      searcher.close();

      // Now, close the writer:
      writer.rollback();

      assertNoUnreferencedFiles(dir, "unreferenced files remain after rollback()");

      searcher = new IndexSearcher(dir, false);
      hits = searcher.search(new TermQuery(searchTerm), null, 1000).scoreDocs;
      assertEquals("saw changes after writer.abort", 14, hits.length);
      searcher.close();
          
      // Now make sure we can re-open the index, add docs,
      // and all is good:
      writer = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer())
        .setOpenMode(OpenMode.APPEND).setMaxBufferedDocs(10));

      // On abort, writer in fact may write to the same
      // segments_N file:
      dir.setPreventDoubleWrite(false);

      for(int i=0;i<12;i++) {
        for(int j=0;j<17;j++) {
          addDoc(writer);
        }
        searcher = new IndexSearcher(dir, false);
        hits = searcher.search(new TermQuery(searchTerm), null, 1000).scoreDocs;
        assertEquals("reader incorrectly sees changes from writer", 14, hits.length);
        searcher.close();
      }

      writer.close();
      searcher = new IndexSearcher(dir, false);
      hits = searcher.search(new TermQuery(searchTerm), null, 1000).scoreDocs;
      assertEquals("didn't see changes after close", 218, hits.length);
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
      MockRAMDirectory dir = newDirectory(random);      
      IndexWriter writer  = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()).setMaxBufferedDocs(10));
      ((LogMergePolicy) writer.getMergePolicy()).setMergeFactor(10);
      for(int j=0;j<30;j++) {
        addDocWithIndex(writer, j);
      }
      writer.close();
      dir.resetMaxUsedSizeInBytes();

      long startDiskUsage = dir.getMaxUsedSizeInBytes();
      writer = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer())
        .setOpenMode(OpenMode.APPEND).setMaxBufferedDocs(10).setMergeScheduler(
            new SerialMergeScheduler()));
      ((LogMergePolicy) writer.getMergePolicy()).setMergeFactor(10);
      for(int j=0;j<1470;j++) {
        addDocWithIndex(writer, j);
      }
      long midDiskUsage = dir.getMaxUsedSizeInBytes();
      dir.resetMaxUsedSizeInBytes();
      writer.optimize();
      writer.close();

      IndexReader.open(dir, true).close();

      long endDiskUsage = dir.getMaxUsedSizeInBytes();

      // Ending index is 50X as large as starting index; due
      // to 2X disk usage normally we allow 100X max
      // transient usage.  If something is wrong w/ deleter
      // and it doesn't delete intermediate segments then it
      // will exceed this 100X:
      // System.out.println("start " + startDiskUsage + "; mid " + midDiskUsage + ";end " + endDiskUsage);
      assertTrue("writer used too much space while adding documents: mid=" + midDiskUsage + " start=" + startDiskUsage + " end=" + endDiskUsage,
                 midDiskUsage < 100*startDiskUsage);
      assertTrue("writer used too much space after close: endDiskUsage=" + endDiskUsage + " startDiskUsage=" + startDiskUsage,
                 endDiskUsage < 100*startDiskUsage);
      dir.close();
    }


    /*
     * Verify that calling optimize when writer is open for
     * "commit on close" works correctly both for rollback()
     * and close().
     */
    public void testCommitOnCloseOptimize() throws IOException {
      MockRAMDirectory dir = newDirectory(random);  
      // Must disable throwing exc on double-write: this
      // test uses IW.rollback which easily results in
      // writing to same file more than once
      dir.setPreventDoubleWrite(false);
      IndexWriter writer  = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()).setMaxBufferedDocs(10));
      ((LogMergePolicy) writer.getConfig().getMergePolicy()).setMergeFactor(10);
      for(int j=0;j<17;j++) {
        addDocWithIndex(writer, j);
      }
      writer.close();

      writer  = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()).setOpenMode(OpenMode.APPEND));
      writer.optimize();

      // Open a reader before closing (commiting) the writer:
      IndexReader reader = IndexReader.open(dir, true);

      // Reader should see index as unoptimized at this
      // point:
      assertFalse("Reader incorrectly sees that the index is optimized", reader.isOptimized());
      reader.close();

      // Abort the writer:
      writer.rollback();
      assertNoUnreferencedFiles(dir, "aborted writer after optimize");

      // Open a reader after aborting writer:
      reader = IndexReader.open(dir, true);

      // Reader should still see index as unoptimized:
      assertFalse("Reader incorrectly sees that the index is optimized", reader.isOptimized());
      reader.close();

      writer  = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()).setOpenMode(OpenMode.APPEND));
      writer.optimize();
      writer.close();
      assertNoUnreferencedFiles(dir, "aborted writer after optimize");

      // Open a reader after aborting writer:
      reader = IndexReader.open(dir, true);

      // Reader should still see index as unoptimized:
      assertTrue("Reader incorrectly sees that the index is unoptimized", reader.isOptimized());
      reader.close();
      dir.close();
    }

    public void testIndexNoDocuments() throws IOException {
      MockRAMDirectory dir = newDirectory(random);      
      IndexWriter writer  = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()));
      writer.commit();
      writer.close();

      IndexReader reader = IndexReader.open(dir, true);
      assertEquals(0, reader.maxDoc());
      assertEquals(0, reader.numDocs());
      reader.close();

      writer  = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()).setOpenMode(OpenMode.APPEND));
      writer.commit();
      writer.close();

      reader = IndexReader.open(dir, true);
      assertEquals(0, reader.maxDoc());
      assertEquals(0, reader.numDocs());
      reader.close();
      dir.close();
    }

    public void testManyFields() throws IOException {
      MockRAMDirectory dir = newDirectory(random);      
      IndexWriter writer  = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()).setMaxBufferedDocs(10));
      for(int j=0;j<100;j++) {
        Document doc = new Document();
        doc.add(new Field("a"+j, "aaa" + j, Field.Store.YES, Field.Index.ANALYZED));
        doc.add(new Field("b"+j, "aaa" + j, Field.Store.YES, Field.Index.ANALYZED));
        doc.add(new Field("c"+j, "aaa" + j, Field.Store.YES, Field.Index.ANALYZED));
        doc.add(new Field("d"+j, "aaa", Field.Store.YES, Field.Index.ANALYZED));
        doc.add(new Field("e"+j, "aaa", Field.Store.YES, Field.Index.ANALYZED));
        doc.add(new Field("f"+j, "aaa", Field.Store.YES, Field.Index.ANALYZED));
        writer.addDocument(doc);
      }
      writer.close();

      IndexReader reader = IndexReader.open(dir, true);
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
      MockRAMDirectory dir = newDirectory(random);      
      IndexWriter writer  = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()).setRAMBufferSizeMB(0.000001));
      ((LogMergePolicy) writer.getMergePolicy()).setMergeFactor(10);
      int lastNumFile = dir.listAll().length;
      for(int j=0;j<9;j++) {
        Document doc = new Document();
        doc.add(new Field("field", "aaa" + j, Field.Store.YES, Field.Index.ANALYZED));
        writer.addDocument(doc);
        int numFile = dir.listAll().length;
        // Verify that with a tiny RAM buffer we see new
        // segment after every doc
        assertTrue(numFile > lastNumFile);
        lastNumFile = numFile;
      }
      writer.close();
      dir.close();
    }

    /**
     * Make sure it's OK to change RAM buffer size and // maxBufferedDocs in a
     * write session
     * 
     * @deprecated after all the setters on IW go away (4.0), this test can be
     *             removed because changing ram buffer settings during a write
     *             session won't be possible.
     */
    public void testChangingRAMBuffer() throws IOException {
      MockRAMDirectory dir = newDirectory(random);      
      IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(random,
        TEST_VERSION_CURRENT, new MockAnalyzer()).setMaxBufferedDocs(10).setRAMBufferSizeMB(
        IndexWriterConfig.DISABLE_AUTO_FLUSH));

      int lastFlushCount = -1;
      for(int j=1;j<52;j++) {
        Document doc = new Document();
        doc.add(new Field("field", "aaa" + j, Field.Store.YES, Field.Index.ANALYZED));
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
          writer.setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH);
        } else if (j < 20) {
          assertTrue(flushCount > lastFlushCount);
          lastFlushCount = flushCount;
        } else if (20 == j) {
          writer.setRAMBufferSizeMB(16);
          writer.setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH);
          lastFlushCount = flushCount;
        } else if (j < 30) {
          assertEquals(flushCount, lastFlushCount);
        } else if (30 == j) {
          writer.setRAMBufferSizeMB(0.000001);
          writer.setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH);
        } else if (j < 40) {
          assertTrue(flushCount> lastFlushCount);
          lastFlushCount = flushCount;
        } else if (40 == j) {
          writer.setMaxBufferedDocs(10);
          writer.setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
          lastFlushCount = flushCount;
        } else if (j < 50) {
          assertEquals(flushCount, lastFlushCount);
          writer.setMaxBufferedDocs(10);
          writer.setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
        } else if (50 == j) {
          assertTrue(flushCount > lastFlushCount);
        }
      }
      writer.close();
      dir.close();
    }

    /**
     * @deprecated after setters on IW go away, this test can be deleted because
     *             changing those settings on IW won't be possible.
     */
    public void testChangingRAMBuffer2() throws IOException {
      MockRAMDirectory dir = newDirectory(random);      
      IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(random,
        TEST_VERSION_CURRENT, new MockAnalyzer()).setMaxBufferedDocs(10).setMaxBufferedDeleteTerms(
        10).setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH));

      for(int j=1;j<52;j++) {
        Document doc = new Document();
        doc.add(new Field("field", "aaa" + j, Field.Store.YES, Field.Index.ANALYZED));
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
          writer.setMaxBufferedDeleteTerms(1);
        } else if (j < 20) {
          assertTrue(flushCount > lastFlushCount);
          lastFlushCount = flushCount;
        } else if (20 == j) {
          writer.setRAMBufferSizeMB(16);
          writer.setMaxBufferedDeleteTerms(IndexWriterConfig.DISABLE_AUTO_FLUSH);
          lastFlushCount = flushCount;
        } else if (j < 30) {
          assertEquals(flushCount, lastFlushCount);
        } else if (30 == j) {
          writer.setRAMBufferSizeMB(0.000001);
          writer.setMaxBufferedDeleteTerms(IndexWriterConfig.DISABLE_AUTO_FLUSH);
          writer.setMaxBufferedDeleteTerms(1);
        } else if (j < 40) {
          assertTrue(flushCount> lastFlushCount);
          lastFlushCount = flushCount;
        } else if (40 == j) {
          writer.setMaxBufferedDeleteTerms(10);
          writer.setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
          lastFlushCount = flushCount;
        } else if (j < 50) {
          assertEquals(flushCount, lastFlushCount);
          writer.setMaxBufferedDeleteTerms(10);
          writer.setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
        } else if (50 == j) {
          assertTrue(flushCount > lastFlushCount);
        }
      }
      writer.close();
      dir.close();
    }

    public void testDiverseDocs() throws IOException {
      MockRAMDirectory dir = newDirectory(random);      
      IndexWriter writer  = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()).setRAMBufferSizeMB(0.5));
      for(int i=0;i<3;i++) {
        // First, docs where every term is unique (heavy on
        // Posting instances)
        for(int j=0;j<100;j++) {
          Document doc = new Document();
          for(int k=0;k<100;k++) {
            doc.add(new Field("field", Integer.toString(random.nextInt()), Field.Store.YES, Field.Index.ANALYZED));
          }
          writer.addDocument(doc);
        }

        // Next, many single term docs where only one term
        // occurs (heavy on byte blocks)
        for(int j=0;j<100;j++) {
          Document doc = new Document();
          doc.add(new Field("field", "aaa aaa aaa aaa aaa aaa aaa aaa aaa aaa", Field.Store.YES, Field.Index.ANALYZED));
          writer.addDocument(doc);
        }

        // Next, many single term docs where only one term
        // occurs but the terms are very long (heavy on
        // char[] arrays)
        for(int j=0;j<100;j++) {
          StringBuilder b = new StringBuilder();
          String x = Integer.toString(j) + ".";
          for(int k=0;k<1000;k++)
            b.append(x);
          String longTerm = b.toString();

          Document doc = new Document();
          doc.add(new Field("field", longTerm, Field.Store.YES, Field.Index.ANALYZED));
          writer.addDocument(doc);
        }
      }
      writer.close();

      IndexSearcher searcher = new IndexSearcher(dir, false);
      ScoreDoc[] hits = searcher.search(new TermQuery(new Term("field", "aaa")), null, 1000).scoreDocs;
      assertEquals(300, hits.length);
      searcher.close();

      dir.close();
    }

    public void testEnablingNorms() throws IOException {
      MockRAMDirectory dir = newDirectory(random);      
      IndexWriter writer  = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()).setMaxBufferedDocs(10));
      // Enable norms for only 1 doc, pre flush
      for(int j=0;j<10;j++) {
        Document doc = new Document();
        Field f = new Field("field", "aaa", Field.Store.YES, Field.Index.ANALYZED); 
        if (j != 8) {
          f.setOmitNorms(true);
        }
        doc.add(f);
        writer.addDocument(doc);
      }
      writer.close();

      Term searchTerm = new Term("field", "aaa");

      IndexSearcher searcher = new IndexSearcher(dir, false);
      ScoreDoc[] hits = searcher.search(new TermQuery(searchTerm), null, 1000).scoreDocs;
      assertEquals(10, hits.length);
      searcher.close();

      writer = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer())
        .setOpenMode(OpenMode.CREATE).setMaxBufferedDocs(10));
      // Enable norms for only 1 doc, post flush
      for(int j=0;j<27;j++) {
        Document doc = new Document();
        Field f = new Field("field", "aaa", Field.Store.YES, Field.Index.ANALYZED); 
        if (j != 26) {
          f.setOmitNorms(true);
        }
        doc.add(f);
        writer.addDocument(doc);
      }
      writer.close();
      searcher = new IndexSearcher(dir, false);
      hits = searcher.search(new TermQuery(searchTerm), null, 1000).scoreDocs;
      assertEquals(27, hits.length);
      searcher.close();

      IndexReader reader = IndexReader.open(dir, true);
      reader.close();

      dir.close();
    }

    public void testHighFreqTerm() throws IOException {
      MockRAMDirectory dir = newDirectory(random);      
      IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(random,
          TEST_VERSION_CURRENT, new MockAnalyzer()).setMaxFieldLength(100000000).setRAMBufferSizeMB(0.01));
      // Massive doc that has 128 K a's
      StringBuilder b = new StringBuilder(1024*1024);
      for(int i=0;i<4096;i++) {
        b.append(" a a a a a a a a");
        b.append(" a a a a a a a a");
        b.append(" a a a a a a a a");
        b.append(" a a a a a a a a");
      }
      Document doc = new Document();
      doc.add(new Field("field", b.toString(), Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
      writer.addDocument(doc);
      writer.close();

      IndexReader reader = IndexReader.open(dir, true);
      assertEquals(1, reader.maxDoc());
      assertEquals(1, reader.numDocs());
      Term t = new Term("field", "a");
      assertEquals(1, reader.docFreq(t));
      DocsEnum td = MultiFields.getTermDocsEnum(reader,
                                                MultiFields.getDeletedDocs(reader),
                                                "field",
                                                new BytesRef("a"));
      td.nextDoc();
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

      final class MyRAMDirectory extends MockRAMDirectory {
        private LockFactory myLockFactory;
        MyRAMDirectory() {
          lockFactory = null;
          myLockFactory = new SingleInstanceLockFactory();
        }
        @Override
        public Lock makeLock(String name) {
          return myLockFactory.makeLock(name);
        }
      }
      
      Directory dir = new MyRAMDirectory();
      IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(random,
        TEST_VERSION_CURRENT, new MockAnalyzer()));
      for (int i = 0; i < 100; i++) {
        addDoc(writer);
      }
      writer.close();
      Term searchTerm = new Term("content", "aaa");        
      IndexSearcher searcher = new IndexSearcher(dir, false);
      ScoreDoc[] hits = searcher.search(new TermQuery(searchTerm), null, 1000).scoreDocs;
      assertEquals("did not get right number of hits", 100, hits.length);
      searcher.close();

      writer = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer())
        .setOpenMode(OpenMode.CREATE));
      writer.close();
      searcher.close();
      dir.close();
    }

    public void testFlushWithNoMerging() throws IOException {
      Directory dir = newDirectory(random);
      IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(random, 
        TEST_VERSION_CURRENT, new MockAnalyzer()).setMaxBufferedDocs(2));
      ((LogMergePolicy) writer.getMergePolicy()).setMergeFactor(10);
      Document doc = new Document();
      doc.add(new Field("field", "aaa", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
      for(int i=0;i<19;i++)
        writer.addDocument(doc);
      writer.flush(false, true, true);
      writer.close();
      SegmentInfos sis = new SegmentInfos();
      sis.read(dir);
      // Since we flushed w/o allowing merging we should now
      // have 10 segments
      assert sis.size() == 10;
      dir.close();
    }

    // Make sure we can flush segment w/ norms, then add
    // empty doc (no norms) and flush
    public void testEmptyDocAfterFlushingRealDoc() throws IOException {
      Directory dir = newDirectory(random);
      IndexWriter writer  = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()));
      Document doc = new Document();
      doc.add(new Field("field", "aaa", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
      writer.addDocument(doc);
      writer.commit();
      writer.addDocument(new Document());
      writer.close();
      _TestUtil.checkIndex(dir);
      IndexReader reader = IndexReader.open(dir, true);
      assertEquals(2, reader.numDocs());
      reader.close();
      dir.close();
    }

    // Test calling optimize(false) whereby optimize is kicked
    // off but we don't wait for it to finish (but
    // writer.close()) does wait
    public void testBackgroundOptimize() throws IOException {

      Directory dir = newDirectory(random);
      for(int pass=0;pass<2;pass++) {
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(random,
          TEST_VERSION_CURRENT, new MockAnalyzer())
          .setOpenMode(OpenMode.CREATE).setMaxBufferedDocs(2));
        ((LogMergePolicy) writer.getConfig().getMergePolicy()).setMergeFactor(101);
        Document doc = new Document();
        doc.add(new Field("field", "aaa", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
        for(int i=0;i<200;i++)
          writer.addDocument(doc);
        writer.optimize(false);

        if (0 == pass) {
          writer.close();
          IndexReader reader = IndexReader.open(dir, true);
          assertTrue(reader.isOptimized());
          reader.close();
        } else {
          // Get another segment to flush so we can verify it is
          // NOT included in the optimization
          writer.addDocument(doc);
          writer.addDocument(doc);
          writer.close();

          IndexReader reader = IndexReader.open(dir, true);
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
    MockRAMDirectory dir = newDirectory(random);
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(random,
        TEST_VERSION_CURRENT, new MockAnalyzer()));
    
    Document document = new Document();
    document.add(new Field("tvtest", "", Store.NO, Index.ANALYZED, TermVector.YES));
    iw.addDocument(document);
    iw.close();
    dir.close();
  }

  // LUCENE-1008
  public void testNoTermVectorAfterTermVector() throws IOException {
    MockRAMDirectory dir = newDirectory(random);
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(random,
        TEST_VERSION_CURRENT, new MockAnalyzer()));
    Document document = new Document();
    document.add(new Field("tvtest", "a b c", Field.Store.NO, Field.Index.ANALYZED,
        Field.TermVector.YES));
    iw.addDocument(document);
    document = new Document();
    document.add(new Field("tvtest", "x y z", Field.Store.NO, Field.Index.ANALYZED,
                           Field.TermVector.NO));
    iw.addDocument(document);
    // Make first segment
    iw.commit();

    document.add(new Field("tvtest", "a b c", Field.Store.NO, Field.Index.ANALYZED,
        Field.TermVector.YES));
    iw.addDocument(document);
    // Make 2nd segment
    iw.commit();

    iw.optimize();
    iw.close();
    dir.close();
  }

  // LUCENE-1010
  public void testNoTermVectorAfterTermVectorMerge() throws IOException {
    MockRAMDirectory dir = newDirectory(random);
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(random,
        TEST_VERSION_CURRENT, new MockAnalyzer()));
    Document document = new Document();
    document.add(new Field("tvtest", "a b c", Field.Store.NO, Field.Index.ANALYZED,
        Field.TermVector.YES));
    iw.addDocument(document);
    iw.commit();

    document = new Document();
    document.add(new Field("tvtest", "x y z", Field.Store.NO, Field.Index.ANALYZED,
                           Field.TermVector.NO));
    iw.addDocument(document);
    // Make first segment
    iw.commit();

    iw.optimize();

    document.add(new Field("tvtest", "a b c", Field.Store.NO, Field.Index.ANALYZED,
        Field.TermVector.YES));
    iw.addDocument(document);
    // Make 2nd segment
    iw.commit();
    iw.optimize();

    iw.close();
    dir.close();
  }

  // LUCENE-1036
  public void testMaxThreadPriority() throws IOException {
    int pri = Thread.currentThread().getPriority();
    try {
      MockRAMDirectory dir = newDirectory(random);
      IndexWriterConfig conf = newIndexWriterConfig(random,
          TEST_VERSION_CURRENT, new MockAnalyzer())
          .setMaxBufferedDocs(2);
      ((LogMergePolicy) conf.getMergePolicy()).setMergeFactor(2);
      IndexWriter iw = new IndexWriter(dir, conf);
      Document document = new Document();
      document.add(new Field("tvtest", "a b c", Field.Store.NO, Field.Index.ANALYZED,
                             Field.TermVector.YES));
      Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
      for(int i=0;i<4;i++)
        iw.addDocument(document);
      iw.close();
      dir.close();
    } finally {
      Thread.currentThread().setPriority(pri);
    }
  }

  // Just intercepts all merges & verifies that we are never
  // merging a segment with >= 20 (maxMergeDocs) docs
  private class MyMergeScheduler extends MergeScheduler {
    @Override
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

    @Override
    public void close() {}
  }

  // LUCENE-1013
  public void testSetMaxMergeDocs() throws IOException {
    MockRAMDirectory dir = newDirectory(random);
    IndexWriterConfig conf = newIndexWriterConfig(random,
        TEST_VERSION_CURRENT, new MockAnalyzer())
        .setMergeScheduler(new MyMergeScheduler()).setMaxBufferedDocs(2);
    LogMergePolicy lmp = (LogMergePolicy) conf.getMergePolicy();
    lmp.setMaxMergeDocs(20);
    lmp.setMergeFactor(2);
    IndexWriter iw = new IndexWriter(dir, conf);
    Document document = new Document();
    document.add(new Field("tvtest", "a b c", Field.Store.NO, Field.Index.ANALYZED,
                           Field.TermVector.YES));
    for(int i=0;i<177;i++)
      iw.addDocument(document);
    iw.close();
    dir.close();
  }

  // LUCENE-1072
  public void testExceptionFromTokenStream() throws IOException {
    MockRAMDirectory dir = newDirectory(random);
    IndexWriterConfig conf = newIndexWriterConfig(random, TEST_VERSION_CURRENT, new Analyzer() {

      @Override
      public TokenStream tokenStream(String fieldName, Reader reader) {
        return new TokenFilter(new MockTokenizer(reader, MockTokenizer.SIMPLE, true)) {
          private int count = 0;

          @Override
          public boolean incrementToken() throws IOException {
            if (count++ == 5) {
              throw new IOException();
            }
            return input.incrementToken();
          }
        };
      }

    });
    IndexWriter writer = new IndexWriter(dir, conf);

    Document doc = new Document();
    String contents = "aa bb cc dd ee ff gg hh ii jj kk";
    doc.add(new Field("content", contents, Field.Store.NO,
        Field.Index.ANALYZED));
    try {
      writer.addDocument(doc);
      fail("did not hit expected exception");
    } catch (Exception e) {
    }

    // Make sure we can add another normal document
    doc = new Document();
    doc.add(new Field("content", "aa bb cc dd", Field.Store.NO,
        Field.Index.ANALYZED));
    writer.addDocument(doc);

    // Make sure we can add another normal document
    doc = new Document();
    doc.add(new Field("content", "aa bb cc dd", Field.Store.NO,
        Field.Index.ANALYZED));
    writer.addDocument(doc);

    writer.close();
    IndexReader reader = IndexReader.open(dir, true);
    final Term t = new Term("content", "aa");
    assertEquals(reader.docFreq(t), 3);

    // Make sure the doc that hit the exception was marked
    // as deleted:
    DocsEnum tdocs = MultiFields.getTermDocsEnum(reader,
                                              MultiFields.getDeletedDocs(reader),
                                              t.field(),
                                              new BytesRef(t.text()));

    int count = 0;
    while(tdocs.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
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

    @Override
    public void setDoFail() {
      this.doFail = true;
    }
    @Override
    public void clearDoFail() {
      this.doFail = false;
    }

    @Override
    public void eval(MockRAMDirectory dir)  throws IOException {
      if (doFail) {
        StackTraceElement[] trace = new Exception().getStackTrace();
        boolean sawAppend = false;
        boolean sawFlush = false;
        for (int i = 0; i < trace.length; i++) {
          if ("org.apache.lucene.index.FreqProxTermsWriter".equals(trace[i].getClassName()) && "appendPostings".equals(trace[i].getMethodName()))
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
    MockRAMDirectory dir = newDirectory(random);
    FailOnlyOnFlush failure = new FailOnlyOnFlush();
    failure.setDoFail();
    dir.failOn(failure);

    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()).setMaxBufferedDocs(2));
    Document doc = new Document();
    String contents = "aa bb cc dd ee ff gg hh ii jj kk";
    doc.add(new Field("content", contents, Field.Store.NO,
        Field.Index.ANALYZED));
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
    IndexReader reader = IndexReader.open(dir, true);
    assertEquals(198, reader.docFreq(new Term("content", "aa")));
    reader.close();
    dir.close();
  }

  private class CrashingFilter extends TokenFilter {
    String fieldName;
    int count;

    public CrashingFilter(String fieldName, TokenStream input) {
      super(input);
      this.fieldName = fieldName;
    }

    @Override
    public boolean incrementToken() throws IOException {
      if (this.fieldName.equals("crash") && count++ >= 4)
        throw new IOException("I'm experiencing problems");
      return input.incrementToken();
    }

    @Override
    public void reset() throws IOException {
      super.reset();
      count = 0;
    }
  }

  public void testDocumentsWriterExceptions() throws IOException {
    Analyzer analyzer = new Analyzer() {
      @Override
      public TokenStream tokenStream(String fieldName, Reader reader) {
        return new CrashingFilter(fieldName, new MockTokenizer(reader, MockTokenizer.WHITESPACE, false));
      }
    };

    for(int i=0;i<2;i++) {
      MockRAMDirectory dir = newDirectory(random);
      IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, analyzer));
      //writer.setInfoStream(System.out);
      Document doc = new Document();
      doc.add(new Field("contents", "here are some contents", Field.Store.YES,
                        Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
      writer.addDocument(doc);
      writer.addDocument(doc);
      doc.add(new Field("crash", "this should crash after 4 terms", Field.Store.YES,
                        Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
      doc.add(new Field("other", "this will not get indexed", Field.Store.YES,
                        Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
      try {
        writer.addDocument(doc);
        fail("did not hit expected exception");
      } catch (IOException ioe) {
      }

      if (0 == i) {
        doc = new Document();
        doc.add(new Field("contents", "here are some contents", Field.Store.YES,
                          Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
        writer.addDocument(doc);
        writer.addDocument(doc);
      }
      writer.close();

      IndexReader reader = IndexReader.open(dir, true);
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

      writer = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT,
          analyzer).setMaxBufferedDocs(10));
      doc = new Document();
      doc.add(new Field("contents", "here are some contents", Field.Store.YES,
                        Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
      for(int j=0;j<17;j++)
        writer.addDocument(doc);
      writer.optimize();
      writer.close();

      reader = IndexReader.open(dir, true);
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

  public void testDocumentsWriterExceptionThreads() throws Exception {
    Analyzer analyzer = new Analyzer() {
      @Override
      public TokenStream tokenStream(String fieldName, Reader reader) {
        return new CrashingFilter(fieldName, new MockTokenizer(reader, MockTokenizer.WHITESPACE, false));
      }
    };

    final int NUM_THREAD = 3;
    final int NUM_ITER = 100;

    for(int i=0;i<2;i++) {
      MockRAMDirectory dir = newDirectory(random);

      {
        final IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, analyzer).setMaxBufferedDocs(-1));
        ((LogMergePolicy) writer.getMergePolicy()).setMergeFactor(10);
        final int finalI = i;

        Thread[] threads = new Thread[NUM_THREAD];
        for(int t=0;t<NUM_THREAD;t++) {
          threads[t] = new Thread() {
              @Override
              public void run() {
                try {
                  for(int iter=0;iter<NUM_ITER;iter++) {
                    Document doc = new Document();
                    doc.add(new Field("contents", "here are some contents", Field.Store.YES,
                                      Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
                    writer.addDocument(doc);
                    writer.addDocument(doc);
                    doc.add(new Field("crash", "this should crash after 4 terms", Field.Store.YES,
                                      Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
                    doc.add(new Field("other", "this will not get indexed", Field.Store.YES,
                                      Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
                    try {
                      writer.addDocument(doc);
                      fail("did not hit expected exception");
                    } catch (IOException ioe) {
                    }

                    if (0 == finalI) {
                      doc = new Document();
                      doc.add(new Field("contents", "here are some contents", Field.Store.YES,
                                        Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
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
          threads[t].join();
            
        writer.close();
      }

      IndexReader reader = IndexReader.open(dir, true);
      int expected = (3+(1-i)*2)*NUM_THREAD*NUM_ITER;
      assertEquals("i=" + i, expected, reader.docFreq(new Term("contents", "here")));
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

      IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(random,
          TEST_VERSION_CURRENT, analyzer).setMaxBufferedDocs(10));
      Document doc = new Document();
      doc.add(new Field("contents", "here are some contents", Field.Store.YES,
                        Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
      for(int j=0;j<17;j++)
        writer.addDocument(doc);
      writer.optimize();
      writer.close();

      reader = IndexReader.open(dir, true);
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

  public void testVariableSchema() throws Exception {
    MockRAMDirectory dir = newDirectory(random);
    int delID = 0;
    for(int i=0;i<20;i++) {
      IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()).setMaxBufferedDocs(2));
      LogMergePolicy lmp = (LogMergePolicy) writer.getConfig().getMergePolicy();
      lmp.setMergeFactor(2);
      lmp.setUseCompoundFile(false);
      lmp.setUseCompoundDocStore(false);
      Document doc = new Document();
      String contents = "aa bb cc dd ee ff gg hh ii jj kk";

      if (i == 7) {
        // Add empty docs here
        doc.add(new Field("content3", "", Field.Store.NO,
                          Field.Index.ANALYZED));
      } else {
        Field.Store storeVal;
        if (i%2 == 0) {
          doc.add(new Field("content4", contents, Field.Store.YES,
                            Field.Index.ANALYZED));
          storeVal = Field.Store.YES;
        } else
          storeVal = Field.Store.NO;
        doc.add(new Field("content1", contents, storeVal,
                          Field.Index.ANALYZED));
        doc.add(new Field("content3", "", Field.Store.YES,
                          Field.Index.ANALYZED));
        doc.add(new Field("content5", "", storeVal,
                          Field.Index.ANALYZED));
      }

      for(int j=0;j<4;j++)
        writer.addDocument(doc);

      writer.close();
      IndexReader reader = IndexReader.open(dir, false);
      reader.deleteDocument(delID++);
      reader.close();

      if (0 == i % 4) {
        writer = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()));
        LogMergePolicy lmp2 = (LogMergePolicy) writer.getConfig().getMergePolicy();
        lmp2.setUseCompoundFile(false);
        lmp2.setUseCompoundDocStore(false);
        writer.optimize();
        writer.close();
      }
    }
    dir.close();
  }

  public void testNoWaitClose() throws Throwable {
    MockRAMDirectory directory = newDirectory(random);

    final Document doc = new Document();
    Field idField = new Field("id", "", Field.Store.YES, Field.Index.NOT_ANALYZED);
    doc.add(idField);

    for(int pass=0;pass<2;pass++) {

      IndexWriterConfig conf = newIndexWriterConfig(random,
          TEST_VERSION_CURRENT, new MockAnalyzer()).setOpenMode(OpenMode.CREATE)
          .setMaxBufferedDocs(2);
      if (pass == 2) {
        conf.setMergeScheduler(new SerialMergeScheduler());
      }
      IndexWriter writer = new IndexWriter(directory, conf);
      ((LogMergePolicy) writer.getConfig().getMergePolicy()).setMergeFactor(100);
      
      //System.out.println("TEST: pass=" + pass + " cms=" + (pass >= 2));
      for(int iter=0;iter<10;iter++) {
        //System.out.println("TEST: iter=" + iter);
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
        ((LogMergePolicy) writer.getConfig().getMergePolicy()).setMergeFactor(2);

        final IndexWriter finalWriter = writer;
        final ArrayList<Throwable> failure = new ArrayList<Throwable>();
        Thread t1 = new Thread() {
            @Override
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
          throw failure.get(0);

        t1.start();

        writer.close(false);
        t1.join();

        // Make sure reader can read
        IndexReader reader = IndexReader.open(directory, true);
        reader.close();

        // Reopen
        writer = new IndexWriter(directory, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()).setOpenMode(OpenMode.APPEND));
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
    volatile int addCount;

    public IndexerThread(IndexWriter writer, boolean noErrors) {
      this.writer = writer;
      this.noErrors = noErrors;
    }

    @Override
    public void run() {

      final Document doc = new Document();
      doc.add(new Field("field", "aaa bbb ccc ddd eee fff ggg hhh iii jjj", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));

      int idUpto = 0;
      int fullCount = 0;
      final long stopTime = System.currentTimeMillis() + 200;

      do {
        try {
          writer.updateDocument(new Term("id", ""+(idUpto++)), doc);
          addCount++;
        } catch (IOException ioe) {
          //System.out.println(Thread.currentThread().getName() + ": hit exc");
          //ioe.printStackTrace(System.out);
          if (ioe.getMessage().startsWith("fake disk full at") ||
              ioe.getMessage().equals("now failing on purpose")) {
            diskFull = true;
            try {
              Thread.sleep(1);
            } catch (InterruptedException ie) {
              throw new ThreadInterruptedException(ie);
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
      } while(System.currentTimeMillis() < stopTime);
    }
  }

  // LUCENE-1130: make sure we can close() even while
  // threads are trying to add documents.  Strictly
  // speaking, this isn't valid us of Lucene's APIs, but we
  // still want to be robust to this case:
  public void testCloseWithThreads() throws Exception {
    int NUM_THREADS = 3;

    for(int iter=0;iter<7;iter++) {
      MockRAMDirectory dir = newDirectory(random);
      IndexWriterConfig conf = newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer())
        .setMaxBufferedDocs(10).setMergeScheduler(new ConcurrentMergeScheduler());
      // We expect AlreadyClosedException
      ((ConcurrentMergeScheduler) conf.getMergeScheduler()).setSuppressExceptions();
      IndexWriter writer = new IndexWriter(dir, conf);
      ((LogMergePolicy) writer.getConfig().getMergePolicy()).setMergeFactor(4);

      IndexerThread[] threads = new IndexerThread[NUM_THREADS];

      for(int i=0;i<NUM_THREADS;i++)
        threads[i] = new IndexerThread(writer, false);

      for(int i=0;i<NUM_THREADS;i++)
        threads[i].start();

      boolean done = false;
      while(!done) {
        Thread.sleep(100);
        for(int i=0;i<NUM_THREADS;i++)
          // only stop when at least one thread has added a doc
          if (threads[i].addCount > 0) {
            done = true;
            break;
          }
      }

      writer.close(false);

      // Make sure threads that are adding docs are not hung:
      for(int i=0;i<NUM_THREADS;i++) {
        // Without fix for LUCENE-1130: one of the
        // threads will hang
        threads[i].join();
        if (threads[i].isAlive())
          fail("thread seems to be hung");
      }

      // Quick test to make sure index is not corrupt:
      IndexReader reader = IndexReader.open(dir, true);
      DocsEnum tdocs = MultiFields.getTermDocsEnum(reader,
                                                  MultiFields.getDeletedDocs(reader),
                                                  "field",
                                                  new BytesRef("aaa"));
      int count = 0;
      while(tdocs.nextDoc() != DocsEnum.NO_MORE_DOCS) {
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
    MockRAMDirectory dir = newDirectory(random);
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer())
        .setMaxBufferedDocs(2).setMergeScheduler(new ConcurrentMergeScheduler()));
    dir.setMaxSizeInBytes(Math.max(1, dir.getRecomputedActualSizeInBytes()));
    final Document doc = new Document();
    doc.add(new Field("field", "aaa bbb ccc ddd eee fff ggg hhh iii jjj", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
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

    // Make sure once disk space is avail again, we can
    // cleanly close:
    dir.setMaxSizeInBytes(0);
    writer.close(false);
    dir.close();
  }

  // LUCENE-1130: make sure immediate disk full on creating
  // an IndexWriter (hit during DW.ThreadState.init()), with
  // multiple threads, is OK:
  public void testImmediateDiskFullWithThreads() throws Exception {

    int NUM_THREADS = 3;

    for(int iter=0;iter<10;iter++) {
      MockRAMDirectory dir = newDirectory(random);
      IndexWriterConfig conf = newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer())
        .setMaxBufferedDocs(2).setMergeScheduler(new ConcurrentMergeScheduler());
      // We expect disk full exceptions in the merge threads
      ((ConcurrentMergeScheduler) conf.getMergeScheduler()).setSuppressExceptions();
      IndexWriter writer = new IndexWriter(dir, conf);
      ((LogMergePolicy) writer.getConfig().getMergePolicy()).setMergeFactor(4);
      dir.setMaxSizeInBytes(4*1024+20*iter);

      IndexerThread[] threads = new IndexerThread[NUM_THREADS];

      for(int i=0;i<NUM_THREADS;i++)
        threads[i] = new IndexerThread(writer, true);

      for(int i=0;i<NUM_THREADS;i++)
        threads[i].start();

      for(int i=0;i<NUM_THREADS;i++) {
        // Without fix for LUCENE-1130: one of the
        // threads will hang
        threads[i].join();
        assertTrue("hit unexpected Throwable", threads[i].error == null);
      }

      // Make sure once disk space is avail again, we can
      // cleanly close:
      dir.setMaxSizeInBytes(0);
      writer.close(false);
      dir.close();
    }
  }

  // Throws IOException during FieldsWriter.flushDocument and during DocumentsWriter.abort
  private static class FailOnlyOnAbortOrFlush extends MockRAMDirectory.Failure {
    private boolean onlyOnce;
    public FailOnlyOnAbortOrFlush(boolean onlyOnce) {
      this.onlyOnce = onlyOnce;
    }
    @Override
    public void eval(MockRAMDirectory dir)  throws IOException {
      if (doFail) {
        StackTraceElement[] trace = new Exception().getStackTrace();
        for (int i = 0; i < trace.length; i++) {
          if ("abort".equals(trace[i].getMethodName()) ||
              "flushDocument".equals(trace[i].getMethodName())) {
            if (onlyOnce)
              doFail = false;
            //System.out.println(Thread.currentThread().getName() + ": now fail");
            //new Throwable().printStackTrace(System.out);
            throw new IOException("now failing on purpose");
          }
        }
      }
    }
  }

  // Runs test, with one thread, using the specific failure
  // to trigger an IOException
  public void _testSingleThreadFailure(MockRAMDirectory.Failure failure) throws IOException {
    MockRAMDirectory dir = newDirectory(random);

    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer())
      .setMaxBufferedDocs(2).setMergeScheduler(new ConcurrentMergeScheduler()));
    final Document doc = new Document();
    doc.add(new Field("field", "aaa bbb ccc ddd eee fff ggg hhh iii jjj", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));

    for(int i=0;i<6;i++)
      writer.addDocument(doc);

    dir.failOn(failure);
    failure.setDoFail();
    try {
      writer.addDocument(doc);
      writer.addDocument(doc);
      writer.commit();
      fail("did not hit exception");
    } catch (IOException ioe) {
    }
    failure.clearDoFail();
    writer.addDocument(doc);
    writer.close(false);
    dir.close();
  }

  // Runs test, with multiple threads, using the specific
  // failure to trigger an IOException
  public void _testMultipleThreadsFailure(MockRAMDirectory.Failure failure) throws Exception {

    int NUM_THREADS = 3;

    for(int iter=0;iter<2;iter++) {
      MockRAMDirectory dir = newDirectory(random);
      IndexWriterConfig conf = newIndexWriterConfig(random, TEST_VERSION_CURRENT,
          new MockAnalyzer()).setMaxBufferedDocs(2).setMergeScheduler(new ConcurrentMergeScheduler());
      // We expect disk full exceptions in the merge threads
      ((ConcurrentMergeScheduler) conf.getMergeScheduler()).setSuppressExceptions();
      IndexWriter writer = new IndexWriter(dir, conf);
      ((LogMergePolicy) writer.getConfig().getMergePolicy()).setMergeFactor(4);

      IndexerThread[] threads = new IndexerThread[NUM_THREADS];

      for(int i=0;i<NUM_THREADS;i++)
        threads[i] = new IndexerThread(writer, true);

      for(int i=0;i<NUM_THREADS;i++)
        threads[i].start();

      Thread.sleep(10);

      dir.failOn(failure);
      failure.setDoFail();

      for(int i=0;i<NUM_THREADS;i++) {
        threads[i].join();
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
        IndexReader reader = IndexReader.open(dir, true);
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
  // IOException during rollback(), is OK:
  public void testIOExceptionDuringAbort() throws IOException {
    _testSingleThreadFailure(new FailOnlyOnAbortOrFlush(false));
  }

  // LUCENE-1130: make sure initial IOException, and then 2nd
  // IOException during rollback(), is OK:
  public void testIOExceptionDuringAbortOnlyOnce() throws IOException {
    _testSingleThreadFailure(new FailOnlyOnAbortOrFlush(true));
  }

  // LUCENE-1130: make sure initial IOException, and then 2nd
  // IOException during rollback(), with multiple threads, is OK:
  public void testIOExceptionDuringAbortWithThreads() throws Exception {
    _testMultipleThreadsFailure(new FailOnlyOnAbortOrFlush(false));
  }

  // LUCENE-1130: make sure initial IOException, and then 2nd
  // IOException during rollback(), with multiple threads, is OK:
  public void testIOExceptionDuringAbortWithThreadsOnlyOnce() throws Exception {
    _testMultipleThreadsFailure(new FailOnlyOnAbortOrFlush(true));
  }

  // Throws IOException during DocumentsWriter.closeDocStore
  private static class FailOnlyInCloseDocStore extends MockRAMDirectory.Failure {
    private boolean onlyOnce;
    public FailOnlyInCloseDocStore(boolean onlyOnce) {
      this.onlyOnce = onlyOnce;
    }
    @Override
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
  public void testIOExceptionDuringCloseDocStoreWithThreads() throws Exception {
    _testMultipleThreadsFailure(new FailOnlyInCloseDocStore(false));
  }

  // LUCENE-1130: test IOException in closeDocStore, with threads
  public void testIOExceptionDuringCloseDocStoreWithThreadsOnlyOnce() throws Exception {
    _testMultipleThreadsFailure(new FailOnlyInCloseDocStore(true));
  }

  // Throws IOException during DocumentsWriter.writeSegment
  private static class FailOnlyInWriteSegment extends MockRAMDirectory.Failure {
    private boolean onlyOnce;
    public FailOnlyInWriteSegment(boolean onlyOnce) {
      this.onlyOnce = onlyOnce;
    }
    @Override
    public void eval(MockRAMDirectory dir)  throws IOException {
      if (doFail) {
        StackTraceElement[] trace = new Exception().getStackTrace();
        for (int i = 0; i < trace.length; i++) {
          if ("flush".equals(trace[i].getMethodName()) && "org.apache.lucene.index.DocFieldProcessor".equals(trace[i].getClassName())) {
            if (onlyOnce)
              doFail = false;
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
  public void testIOExceptionDuringWriteSegmentWithThreads() throws Exception {
    _testMultipleThreadsFailure(new FailOnlyInWriteSegment(false));
  }

  // LUCENE-1130: test IOException in writeSegment, with threads
  public void testIOExceptionDuringWriteSegmentWithThreadsOnlyOnce() throws Exception {
    _testMultipleThreadsFailure(new FailOnlyInWriteSegment(true));
  }

  // LUCENE-1084: test unlimited field length
  public void testUnlimitedMaxFieldLength() throws IOException {
    Directory dir = newDirectory(random);

    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()));

    Document doc = new Document();
    StringBuilder b = new StringBuilder();
    for(int i=0;i<10000;i++)
      b.append(" a");
    b.append(" x");
    doc.add(new Field("field", b.toString(), Field.Store.NO, Field.Index.ANALYZED));
    writer.addDocument(doc);
    writer.close();

    IndexReader reader = IndexReader.open(dir, true);
    Term t = new Term("field", "x");
    assertEquals(1, reader.docFreq(t));
    reader.close();
    dir.close();
  }

  // LUCENE-1044: Simulate checksum error in segments_N
  public void testSegmentsChecksumError() throws IOException {
    Directory dir = newDirectory(random);

    IndexWriter writer = null;

    writer = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()));

    // add 100 documents
    for (int i = 0; i < 100; i++) {
      addDoc(writer);
    }

    // close
    writer.close();

    long gen = SegmentInfos.getCurrentSegmentGeneration(dir);
    assertTrue("segment generation should be > 0 but got " + gen, gen > 0);

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
      reader = IndexReader.open(dir, true);
    } catch (IOException e) {
      e.printStackTrace(System.out);
      fail("segmentInfos failed to retry fallback to correct segments_N file");
    }
    reader.close();
    dir.close();
  }

  // LUCENE-1044: test writer.commit() when ac=false
  public void testForceCommit() throws IOException {
    Directory dir = newDirectory(random);

    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(random,
        TEST_VERSION_CURRENT, new MockAnalyzer())
        .setMaxBufferedDocs(2));
    ((LogMergePolicy) writer.getConfig().getMergePolicy()).setMergeFactor(5);
    writer.commit();
    
    for (int i = 0; i < 23; i++)
      addDoc(writer);

    IndexReader reader = IndexReader.open(dir, true);
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
    reader = IndexReader.open(dir, true);
    assertEquals(23, reader.numDocs());
    reader.close();
    writer.commit();

    reader = IndexReader.open(dir, true);
    assertEquals(40, reader.numDocs());
    reader.close();
    writer.close();
    dir.close();
  }

  // Throws IOException during MockRAMDirectory.sync
  private static class FailOnlyInSync extends MockRAMDirectory.Failure {
    boolean didFail;
    @Override
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
    MockRAMDirectory dir = newDirectory(random);
    FailOnlyInSync failure = new FailOnlyInSync();
    dir.failOn(failure);

    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer())
        .setMaxBufferedDocs(2).setMergeScheduler(new ConcurrentMergeScheduler()));
    failure.setDoFail();
    ((LogMergePolicy) writer.getConfig().getMergePolicy()).setMergeFactor(5);

    for (int i = 0; i < 23; i++) {
      addDoc(writer);
      if ((i-1)%2 == 0) {
        try {
          writer.commit();
        } catch (IOException ioe) {
          // expected
        }
      }
    }

    ((ConcurrentMergeScheduler) writer.getConfig().getMergeScheduler()).sync();
    assertTrue(failure.didFail);
    failure.clearDoFail();
    writer.close();

    IndexReader reader = IndexReader.open(dir, true);
    assertEquals(23, reader.numDocs());
    reader.close();
    dir.close();
  }

  // LUCENE-1168
  public void testTermVectorCorruption() throws IOException {

    Directory dir = newDirectory(random);
    for(int iter=0;iter<2;iter++) {
      IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(random,
          TEST_VERSION_CURRENT, new MockAnalyzer())
          .setMaxBufferedDocs(2).setRAMBufferSizeMB(
              IndexWriterConfig.DISABLE_AUTO_FLUSH).setMergeScheduler(
              new SerialMergeScheduler()).setMergePolicy(
              new LogDocMergePolicy()));

      Document document = new Document();

      Field storedField = new Field("stored", "stored", Field.Store.YES,
                                    Field.Index.NO);
      document.add(storedField);
      writer.addDocument(document);
      writer.addDocument(document);

      document = new Document();
      document.add(storedField);
      Field termVectorField = new Field("termVector", "termVector",
                                        Field.Store.NO, Field.Index.NOT_ANALYZED,
                                        Field.TermVector.WITH_POSITIONS_OFFSETS);

      document.add(termVectorField);
      writer.addDocument(document);
      writer.optimize();
      writer.close();

      IndexReader reader = IndexReader.open(dir, true);
      for(int i=0;i<reader.numDocs();i++) {
        reader.document(i);
        reader.getTermFreqVectors(i);
      }
      reader.close();

      writer = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT,
          new MockAnalyzer()).setMaxBufferedDocs(2)
          .setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH)
          .setMergeScheduler(new SerialMergeScheduler()).setMergePolicy(
              new LogDocMergePolicy()));

      Directory[] indexDirs = {new MockRAMDirectory(dir)};
      writer.addIndexes(indexDirs);
      writer.optimize();
      writer.close();
    }
    dir.close();
  }

  // LUCENE-1168
  public void testTermVectorCorruption2() throws IOException {
    Directory dir = newDirectory(random);
    for(int iter=0;iter<2;iter++) {
      IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(random,
          TEST_VERSION_CURRENT, new MockAnalyzer())
          .setMaxBufferedDocs(2).setRAMBufferSizeMB(
              IndexWriterConfig.DISABLE_AUTO_FLUSH).setMergeScheduler(
              new SerialMergeScheduler()).setMergePolicy(
              new LogDocMergePolicy()));

      Document document = new Document();

      Field storedField = new Field("stored", "stored", Field.Store.YES,
                                    Field.Index.NO);
      document.add(storedField);
      writer.addDocument(document);
      writer.addDocument(document);

      document = new Document();
      document.add(storedField);
      Field termVectorField = new Field("termVector", "termVector",
                                        Field.Store.NO, Field.Index.NOT_ANALYZED,
                                        Field.TermVector.WITH_POSITIONS_OFFSETS);
      document.add(termVectorField);
      writer.addDocument(document);
      writer.optimize();
      writer.close();

      IndexReader reader = IndexReader.open(dir, true);
      assertTrue(reader.getTermFreqVectors(0)==null);
      assertTrue(reader.getTermFreqVectors(1)==null);
      assertTrue(reader.getTermFreqVectors(2)!=null);
      reader.close();
    }
    dir.close();
  }

  // LUCENE-1168
  public void testTermVectorCorruption3() throws IOException {
    Directory dir = newDirectory(random);
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(random,
        TEST_VERSION_CURRENT, new MockAnalyzer())
        .setMaxBufferedDocs(2).setRAMBufferSizeMB(
            IndexWriterConfig.DISABLE_AUTO_FLUSH).setMergeScheduler(
            new SerialMergeScheduler()).setMergePolicy(new LogDocMergePolicy()));

    Document document = new Document();

    document = new Document();
    Field storedField = new Field("stored", "stored", Field.Store.YES,
                                  Field.Index.NO);
    document.add(storedField);
    Field termVectorField = new Field("termVector", "termVector",
                                      Field.Store.NO, Field.Index.NOT_ANALYZED,
                                      Field.TermVector.WITH_POSITIONS_OFFSETS);
    document.add(termVectorField);
    for(int i=0;i<10;i++)
      writer.addDocument(document);
    writer.close();

    writer = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT,
        new MockAnalyzer()).setMaxBufferedDocs(2)
        .setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH)
        .setMergeScheduler(new SerialMergeScheduler()).setMergePolicy(
            new LogDocMergePolicy()));
    for(int i=0;i<6;i++)
      writer.addDocument(document);

    writer.optimize();
    writer.close();

    IndexReader reader = IndexReader.open(dir, true);
    for(int i=0;i<10;i++) {
      reader.getTermFreqVectors(i);
      reader.document(i);
    }
    reader.close();
    dir.close();
  }

  // LUCENE-1084: test user-specified field length
  public void testUserSpecifiedMaxFieldLength() throws IOException {
    Directory dir = newDirectory(random);

    IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer()).setMaxFieldLength(100000));

    Document doc = new Document();
    StringBuilder b = new StringBuilder();
    for(int i=0;i<10000;i++)
      b.append(" a");
    b.append(" x");
    doc.add(new Field("field", b.toString(), Field.Store.NO, Field.Index.ANALYZED));
    writer.addDocument(doc);
    writer.close();

    IndexReader reader = IndexReader.open(dir, true);
    Term t = new Term("field", "x");
    assertEquals(1, reader.docFreq(t));
    reader.close();
    dir.close();
  }

  // LUCENE-325: test expungeDeletes, when 2 singular merges
  // are required
  public void testExpungeDeletes() throws IOException {
    Directory dir = newDirectory(random);
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(random, 
        TEST_VERSION_CURRENT, new MockAnalyzer())
        .setMaxBufferedDocs(2).setRAMBufferSizeMB(
            IndexWriterConfig.DISABLE_AUTO_FLUSH));

    Document document = new Document();

    document = new Document();
    Field storedField = new Field("stored", "stored", Field.Store.YES,
                                  Field.Index.NO);
    document.add(storedField);
    Field termVectorField = new Field("termVector", "termVector",
                                      Field.Store.NO, Field.Index.NOT_ANALYZED,
                                      Field.TermVector.WITH_POSITIONS_OFFSETS);
    document.add(termVectorField);
    for(int i=0;i<10;i++)
      writer.addDocument(document);
    writer.close();

    IndexReader ir = IndexReader.open(dir, false);
    assertEquals(10, ir.maxDoc());
    assertEquals(10, ir.numDocs());
    ir.deleteDocument(0);
    ir.deleteDocument(7);
    assertEquals(8, ir.numDocs());
    ir.close();

    writer = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()));
    assertEquals(8, writer.numDocs());
    assertEquals(10, writer.maxDoc());
    writer.expungeDeletes();
    assertEquals(8, writer.numDocs());
    writer.close();
    ir = IndexReader.open(dir, true);
    assertEquals(8, ir.maxDoc());
    assertEquals(8, ir.numDocs());
    ir.close();
    dir.close();
  }

  // LUCENE-325: test expungeDeletes, when many adjacent merges are required
  public void testExpungeDeletes2() throws IOException {
    Directory dir = newDirectory(random);
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(random, 
        TEST_VERSION_CURRENT, new MockAnalyzer())
        .setMaxBufferedDocs(2).setRAMBufferSizeMB(
            IndexWriterConfig.DISABLE_AUTO_FLUSH));
    ((LogMergePolicy) writer.getConfig().getMergePolicy()).setMergeFactor(50);

    Document document = new Document();

    document = new Document();
    Field storedField = new Field("stored", "stored", Field.Store.YES,
                                  Field.Index.NO);
    document.add(storedField);
    Field termVectorField = new Field("termVector", "termVector",
                                      Field.Store.NO, Field.Index.NOT_ANALYZED,
                                      Field.TermVector.WITH_POSITIONS_OFFSETS);
    document.add(termVectorField);
    for(int i=0;i<98;i++)
      writer.addDocument(document);
    writer.close();

    IndexReader ir = IndexReader.open(dir, false);
    assertEquals(98, ir.maxDoc());
    assertEquals(98, ir.numDocs());
    for(int i=0;i<98;i+=2)
      ir.deleteDocument(i);
    assertEquals(49, ir.numDocs());
    ir.close();

    writer = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT,
        new MockAnalyzer()));
    ((LogMergePolicy) writer.getConfig().getMergePolicy()).setMergeFactor(3);
    assertEquals(49, writer.numDocs());
    writer.expungeDeletes();
    writer.close();
    ir = IndexReader.open(dir, true);
    assertEquals(49, ir.maxDoc());
    assertEquals(49, ir.numDocs());
    ir.close();
    dir.close();
  }

  // LUCENE-325: test expungeDeletes without waiting, when
  // many adjacent merges are required
  public void testExpungeDeletes3() throws IOException {
    Directory dir = newDirectory(random);
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(random, 
        TEST_VERSION_CURRENT, new MockAnalyzer())
        .setMaxBufferedDocs(2).setRAMBufferSizeMB(
            IndexWriterConfig.DISABLE_AUTO_FLUSH));
    ((LogMergePolicy) writer.getConfig().getMergePolicy()).setMergeFactor(50);

    Document document = new Document();

    document = new Document();
    Field storedField = new Field("stored", "stored", Field.Store.YES,
                                  Field.Index.NO);
    document.add(storedField);
    Field termVectorField = new Field("termVector", "termVector",
                                      Field.Store.NO, Field.Index.NOT_ANALYZED,
                                      Field.TermVector.WITH_POSITIONS_OFFSETS);
    document.add(termVectorField);
    for(int i=0;i<98;i++)
      writer.addDocument(document);
    writer.close();

    IndexReader ir = IndexReader.open(dir, false);
    assertEquals(98, ir.maxDoc());
    assertEquals(98, ir.numDocs());
    for(int i=0;i<98;i+=2)
      ir.deleteDocument(i);
    assertEquals(49, ir.numDocs());
    ir.close();

    writer = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()));
    // Force many merges to happen
    ((LogMergePolicy) writer.getConfig().getMergePolicy()).setMergeFactor(3);
    writer.expungeDeletes(false);
    writer.close();
    ir = IndexReader.open(dir, true);
    assertEquals(49, ir.maxDoc());
    assertEquals(49, ir.numDocs());
    ir.close();
    dir.close();
  }

  // LUCENE-1179
  public void testEmptyFieldName() throws IOException {
    MockRAMDirectory dir = newDirectory(random);
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()));
    Document doc = new Document();
    doc.add(new Field("", "a b c", Field.Store.NO, Field.Index.ANALYZED));
    writer.addDocument(doc);
    writer.close();
    dir.close();
  }

  // LUCENE-1198
  private static final class MockIndexWriter extends IndexWriter {

    public MockIndexWriter(Directory dir, IndexWriterConfig conf) throws IOException {
      super(dir, conf);
    }

    boolean doFail;

    @Override
    boolean testPoint(String name) {
      if (doFail && name.equals("DocumentsWriter.ThreadState.init start"))
        throw new RuntimeException("intentionally failing");
      return true;
    }
  }
  

  public void testExceptionDocumentsWriterInit() throws IOException {
    MockRAMDirectory dir = newDirectory(random);
    MockIndexWriter w = new MockIndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()));
    Document doc = new Document();
    doc.add(new Field("field", "a field", Field.Store.YES,
                      Field.Index.ANALYZED));
    w.addDocument(doc);
    w.doFail = true;
    try {
      w.addDocument(doc);
      fail("did not hit exception");
    } catch (RuntimeException re) {
      // expected
    }
    w.close();
    _TestUtil.checkIndex(dir);
    dir.close();
  }

  // LUCENE-1208
  public void testExceptionJustBeforeFlush() throws IOException {
    MockRAMDirectory dir = newDirectory(random);
    MockIndexWriter w = new MockIndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()).setMaxBufferedDocs(2));
    Document doc = new Document();
    doc.add(new Field("field", "a field", Field.Store.YES,
                      Field.Index.ANALYZED));
    w.addDocument(doc);

    Analyzer analyzer = new Analyzer() {
      @Override
      public TokenStream tokenStream(String fieldName, Reader reader) {
        return new CrashingFilter(fieldName, new MockTokenizer(reader, MockTokenizer.WHITESPACE, false));
      }
    };

    Document crashDoc = new Document();
    crashDoc.add(new Field("crash", "do it on token 4", Field.Store.YES,
                           Field.Index.ANALYZED));
    try {
      w.addDocument(crashDoc, analyzer);
      fail("did not hit expected exception");
    } catch (IOException ioe) {
      // expected
    }
    w.addDocument(doc);
    w.close();
    dir.close();
  }    

  private static final class MockIndexWriter2 extends IndexWriter {

    public MockIndexWriter2(Directory dir, IndexWriterConfig conf) throws IOException {
      super(dir, conf);
    }

    boolean doFail;
    boolean failed;

    @Override
    boolean testPoint(String name) {
      if (doFail && name.equals("startMergeInit")) {
        failed = true;
        throw new RuntimeException("intentionally failing");
      }
      return true;
    }
  }
  

  // LUCENE-1210
  public void testExceptionOnMergeInit() throws IOException {
    MockRAMDirectory dir = newDirectory(random);
    IndexWriterConfig conf = newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer())
      .setMaxBufferedDocs(2).setMergeScheduler(new ConcurrentMergeScheduler());
    ((LogMergePolicy) conf.getMergePolicy()).setMergeFactor(2);
    MockIndexWriter2 w = new MockIndexWriter2(dir, conf);
    w.doFail = true;
    Document doc = new Document();
    doc.add(new Field("field", "a field", Field.Store.YES,
                      Field.Index.ANALYZED));
    for(int i=0;i<10;i++)
      try {
        w.addDocument(doc);
      } catch (RuntimeException re) {
        break;
      }

    ((ConcurrentMergeScheduler) w.getConfig().getMergeScheduler()).sync();
    assertTrue(w.failed);
    w.close();
    dir.close();
  }

  private static final class MockIndexWriter3 extends IndexWriter {

    public MockIndexWriter3(Directory dir, IndexWriterConfig conf) throws IOException {
      super(dir, conf);
    }

    boolean afterWasCalled;
    boolean beforeWasCalled;

    @Override
    public void doAfterFlush() {
      afterWasCalled = true;
    }
    
    @Override
    protected void doBeforeFlush() throws IOException {
      beforeWasCalled = true;
    }
  }
  

  // LUCENE-1222
  public void testDoBeforeAfterFlush() throws IOException {
    MockRAMDirectory dir = newDirectory(random);
    MockIndexWriter3 w = new MockIndexWriter3(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()));
    Document doc = new Document();
    doc.add(new Field("field", "a field", Field.Store.YES,
                      Field.Index.ANALYZED));
    w.addDocument(doc);
    w.commit();
    assertTrue(w.beforeWasCalled);
    assertTrue(w.afterWasCalled);
    w.beforeWasCalled = false;
    w.afterWasCalled = false;
    w.deleteDocuments(new Term("field", "field"));
    w.commit();
    assertTrue(w.beforeWasCalled);
    assertTrue(w.afterWasCalled);
    w.close();

    IndexReader ir = IndexReader.open(dir, true);
    assertEquals(1, ir.maxDoc());
    assertEquals(0, ir.numDocs());
    ir.close();

    dir.close();
  }

  private static class FailOnlyInCommit extends MockRAMDirectory.Failure {

    boolean fail1, fail2;

    @Override
    public void eval(MockRAMDirectory dir)  throws IOException {
      StackTraceElement[] trace = new Exception().getStackTrace();
      boolean isCommit = false;
      boolean isDelete = false;
      for (int i = 0; i < trace.length; i++) {
        if ("org.apache.lucene.index.SegmentInfos".equals(trace[i].getClassName()) && "prepareCommit".equals(trace[i].getMethodName()))
          isCommit = true;
        if ("org.apache.lucene.store.MockRAMDirectory".equals(trace[i].getClassName()) && "deleteFile".equals(trace[i].getMethodName()))
          isDelete = true;
      }

      if (isCommit) {
        if (!isDelete) {
          fail1 = true;
          throw new RuntimeException("now fail first");
        } else {
          fail2 = true;
          throw new IOException("now fail during delete");
        }
      }
    }
  }
  

  // LUCENE-1214
  public void testExceptionsDuringCommit() throws Throwable {
    MockRAMDirectory dir = newDirectory(random);
    FailOnlyInCommit failure = new FailOnlyInCommit();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()));
    Document doc = new Document();
    doc.add(new Field("field", "a field", Field.Store.YES,
                      Field.Index.ANALYZED));
    w.addDocument(doc);
    dir.failOn(failure);
    try {
      w.close();
      fail();
    } catch (IOException ioe) {
      fail("expected only RuntimeException");
    } catch (RuntimeException re) {
      // Expected
    }
    assertTrue(failure.fail1 && failure.fail2);
    w.rollback();
    dir.close();
  }
  
  final String[] utf8Data = new String[] {
    // unpaired low surrogate
    "ab\udc17cd", "ab\ufffdcd",
    "\udc17abcd", "\ufffdabcd",
    "\udc17", "\ufffd",
    "ab\udc17\udc17cd", "ab\ufffd\ufffdcd",
    "\udc17\udc17abcd", "\ufffd\ufffdabcd",
    "\udc17\udc17", "\ufffd\ufffd",

    // unpaired high surrogate
    "ab\ud917cd", "ab\ufffdcd",
    "\ud917abcd", "\ufffdabcd",
    "\ud917", "\ufffd",
    "ab\ud917\ud917cd", "ab\ufffd\ufffdcd",
    "\ud917\ud917abcd", "\ufffd\ufffdabcd",
    "\ud917\ud917", "\ufffd\ufffd",

    // backwards surrogates
    "ab\udc17\ud917cd", "ab\ufffd\ufffdcd",
    "\udc17\ud917abcd", "\ufffd\ufffdabcd",
    "\udc17\ud917", "\ufffd\ufffd",
    "ab\udc17\ud917\udc17\ud917cd", "ab\ufffd\ud917\udc17\ufffdcd",
    "\udc17\ud917\udc17\ud917abcd", "\ufffd\ud917\udc17\ufffdabcd",
    "\udc17\ud917\udc17\ud917", "\ufffd\ud917\udc17\ufffd"
  };

  // LUCENE-510
  public void testInvalidUTF16() throws Throwable {
    MockRAMDirectory dir = newDirectory(random);
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()));
    Document doc = new Document();

    final int count = utf8Data.length/2;
    for(int i=0;i<count;i++)
      doc.add(new Field("f" + i, utf8Data[2*i], Field.Store.YES, Field.Index.ANALYZED));
    w.addDocument(doc);
    w.close();

    IndexReader ir = IndexReader.open(dir, true);
    Document doc2 = ir.document(0);
    for(int i=0;i<count;i++) {
      assertEquals("field " + i + " was not indexed correctly", 1, ir.docFreq(new Term("f"+i, utf8Data[2*i+1])));
      assertEquals("field " + i + " is incorrect", utf8Data[2*i+1], doc2.getField("f"+i).stringValue());
    }
    ir.close();
    dir.close();
  }

  // LUCENE-510
  public void testAllUnicodeChars() throws Throwable {

    BytesRef utf8 = new BytesRef(10);
    UnicodeUtil.UTF16Result utf16 = new UnicodeUtil.UTF16Result();
    char[] chars = new char[2];
    for(int ch=0;ch<0x0010FFFF;ch++) {

      if (ch == 0xd800)
        // Skip invalid code points
        ch = 0xe000;

      int len = 0;
      if (ch <= 0xffff) {
        chars[len++] = (char) ch;
      } else {
        chars[len++] = (char) (((ch-0x0010000) >> 10) + UnicodeUtil.UNI_SUR_HIGH_START);
        chars[len++] = (char) (((ch-0x0010000) & 0x3FFL) + UnicodeUtil.UNI_SUR_LOW_START);
      }

      UnicodeUtil.UTF16toUTF8(chars, 0, len, utf8);
      
      String s1 = new String(chars, 0, len);
      String s2 = new String(utf8.bytes, 0, utf8.length, "UTF-8");
      assertEquals("codepoint " + ch, s1, s2);

      UnicodeUtil.UTF8toUTF16(utf8.bytes, 0, utf8.length, utf16);
      assertEquals("codepoint " + ch, s1, new String(utf16.result, 0, utf16.length));

      byte[] b = s1.getBytes("UTF-8");
      assertEquals(utf8.length, b.length);
      for(int j=0;j<utf8.length;j++)
        assertEquals(utf8.bytes[j], b[j]);
    }
  }

  private int nextInt(int lim) {
    return random.nextInt(lim);
  }

  private int nextInt(int start, int end) {
    return start + nextInt(end-start);
  }

  private boolean fillUnicode(char[] buffer, char[] expected, int offset, int count) {
    final int len = offset + count;
    boolean hasIllegal = false;

    if (offset > 0 && buffer[offset] >= 0xdc00 && buffer[offset] < 0xe000)
      // Don't start in the middle of a valid surrogate pair
      offset--;

    for(int i=offset;i<len;i++) {
      int t = nextInt(6);
      if (0 == t && i < len-1) {
        // Make a surrogate pair
        // High surrogate
        expected[i] = buffer[i++] = (char) nextInt(0xd800, 0xdc00);
        // Low surrogate
        expected[i] = buffer[i] = (char) nextInt(0xdc00, 0xe000);
      } else if (t <= 1)
        expected[i] = buffer[i] = (char) nextInt(0x80);
      else if (2 == t)
        expected[i] = buffer[i] = (char) nextInt(0x80, 0x800);
      else if (3 == t)
        expected[i] = buffer[i] = (char) nextInt(0x800, 0xd800);
      else if (4 == t)
        expected[i] = buffer[i] = (char) nextInt(0xe000, 0xffff);
      else if (5 == t && i < len-1) {
        // Illegal unpaired surrogate
        if (nextInt(10) == 7) {
          if (random.nextBoolean())
            buffer[i] = (char) nextInt(0xd800, 0xdc00);
          else
            buffer[i] = (char) nextInt(0xdc00, 0xe000);
          expected[i++] = 0xfffd;
          expected[i] = buffer[i] = (char) nextInt(0x800, 0xd800);
          hasIllegal = true;
        } else 
          expected[i] = buffer[i] = (char) nextInt(0x800, 0xd800);
      } else {
        expected[i] = buffer[i] = ' ';
      }
    }

    return hasIllegal;
  }

  // LUCENE-510
  public void testRandomUnicodeStrings() throws Throwable {
    char[] buffer = new char[20];
    char[] expected = new char[20];

    BytesRef utf8 = new BytesRef(20);
    UnicodeUtil.UTF16Result utf16 = new UnicodeUtil.UTF16Result();

    int num = 100000 * RANDOM_MULTIPLIER;
    for (int iter = 0; iter < num; iter++) {
      boolean hasIllegal = fillUnicode(buffer, expected, 0, 20);

      UnicodeUtil.UTF16toUTF8(buffer, 0, 20, utf8);
      if (!hasIllegal) {
        byte[] b = new String(buffer, 0, 20).getBytes("UTF-8");
        assertEquals(b.length, utf8.length);
        for(int i=0;i<b.length;i++)
          assertEquals(b[i], utf8.bytes[i]);
      }

      UnicodeUtil.UTF8toUTF16(utf8.bytes, 0, utf8.length, utf16);
      assertEquals(utf16.length, 20);
      for(int i=0;i<20;i++)
        assertEquals(expected[i], utf16.result[i]);
    }
  }

  // LUCENE-510
  public void testIncrementalUnicodeStrings() throws Throwable {
    char[] buffer = new char[20];
    char[] expected = new char[20];

    BytesRef utf8 = new BytesRef(new byte[20]);
    UnicodeUtil.UTF16Result utf16 = new UnicodeUtil.UTF16Result();
    UnicodeUtil.UTF16Result utf16a = new UnicodeUtil.UTF16Result();

    boolean hasIllegal = false;
    byte[] last = new byte[60];

    int num = 100000 * RANDOM_MULTIPLIER;
    for (int iter = 0; iter < num; iter++) {

      final int prefix;

      if (iter == 0 || hasIllegal)
        prefix = 0;
      else
        prefix = nextInt(20);

      hasIllegal = fillUnicode(buffer, expected, prefix, 20-prefix);

      UnicodeUtil.UTF16toUTF8(buffer, 0, 20, utf8);
      if (!hasIllegal) {
        byte[] b = new String(buffer, 0, 20).getBytes("UTF-8");
        assertEquals(b.length, utf8.length);
        for(int i=0;i<b.length;i++)
          assertEquals(b[i], utf8.bytes[i]);
      }

      int bytePrefix = 20;
      if (iter == 0 || hasIllegal)
        bytePrefix = 0;
      else
        for(int i=0;i<20;i++)
          if (last[i] != utf8.bytes[i]) {
            bytePrefix = i;
            break;
          }
      System.arraycopy(utf8.bytes, 0, last, 0, utf8.length);

      UnicodeUtil.UTF8toUTF16(utf8.bytes, bytePrefix, utf8.length-bytePrefix, utf16);
      assertEquals(20, utf16.length);
      for(int i=0;i<20;i++)
        assertEquals(expected[i], utf16.result[i]);

      UnicodeUtil.UTF8toUTF16(utf8.bytes, 0, utf8.length, utf16a);
      assertEquals(20, utf16a.length);
      for(int i=0;i<20;i++)
        assertEquals(expected[i], utf16a.result[i]);
    }
  }

  // LUCENE-1255
  public void testNegativePositions() throws Throwable {
    final TokenStream tokens = new TokenStream() {
      final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
      final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);
      
      final Iterator<String> terms = Arrays.asList("a","b","c").iterator();
      boolean first = true;
      
      @Override
      public boolean incrementToken() {
        if (!terms.hasNext()) return false;
        clearAttributes();
        termAtt.append(terms.next());
        posIncrAtt.setPositionIncrement(first ? 0 : 1);
        first = false;
        return true;
      }
    };

    MockRAMDirectory dir = newDirectory(random);
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()));
    Document doc = new Document();
    doc.add(new Field("field", tokens));
    w.addDocument(doc);
    w.commit();

    IndexSearcher s = new IndexSearcher(dir, false);
    PhraseQuery pq = new PhraseQuery();
    pq.add(new Term("field", "a"));
    pq.add(new Term("field", "b"));
    pq.add(new Term("field", "c"));
    ScoreDoc[] hits = s.search(pq, null, 1000).scoreDocs;
    assertEquals(1, hits.length);

    Query q = new SpanTermQuery(new Term("field", "a"));
    hits = s.search(q, null, 1000).scoreDocs;
    assertEquals(1, hits.length);

    DocsAndPositionsEnum tps = MultiFields.getTermPositionsEnum(s.getIndexReader(),
                                                                MultiFields.getDeletedDocs(s.getIndexReader()),
                                                                "field",
                                                                new BytesRef("a"));

    assertTrue(tps.nextDoc() != DocsEnum.NO_MORE_DOCS);
    assertEquals(1, tps.freq());
    assertEquals(0, tps.nextPosition());
    w.close();

    assertTrue(_TestUtil.checkIndex(dir));
    s.close();
    dir.close();
  }

  // LUCENE-1274: test writer.prepareCommit()
  public void testPrepareCommit() throws IOException {
    Directory dir = newDirectory(random);

    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()).setMaxBufferedDocs(2));
    ((LogMergePolicy) writer.getConfig().getMergePolicy()).setMergeFactor(5);
    writer.commit();
    
    for (int i = 0; i < 23; i++)
      addDoc(writer);

    IndexReader reader = IndexReader.open(dir, true);
    assertEquals(0, reader.numDocs());

    writer.prepareCommit();

    IndexReader reader2 = IndexReader.open(dir, true);
    assertEquals(0, reader2.numDocs());

    writer.commit();

    IndexReader reader3 = reader.reopen();
    assertEquals(0, reader.numDocs());
    assertEquals(0, reader2.numDocs());
    assertEquals(23, reader3.numDocs());
    reader.close();
    reader2.close();

    for (int i = 0; i < 17; i++)
      addDoc(writer);

    assertEquals(23, reader3.numDocs());
    reader3.close();
    reader = IndexReader.open(dir, true);
    assertEquals(23, reader.numDocs());
    reader.close();

    writer.prepareCommit();

    reader = IndexReader.open(dir, true);
    assertEquals(23, reader.numDocs());
    reader.close();

    writer.commit();
    reader = IndexReader.open(dir, true);
    assertEquals(40, reader.numDocs());
    reader.close();
    writer.close();
    dir.close();
  }

  // LUCENE-1274: test writer.prepareCommit()
  public void testPrepareCommitRollback() throws IOException {
    MockRAMDirectory dir = newDirectory(random);
    dir.setPreventDoubleWrite(false);

    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()).setMaxBufferedDocs(2));
    ((LogMergePolicy) writer.getConfig().getMergePolicy()).setMergeFactor(5);
    writer.commit();
    
    for (int i = 0; i < 23; i++)
      addDoc(writer);

    IndexReader reader = IndexReader.open(dir, true);
    assertEquals(0, reader.numDocs());

    writer.prepareCommit();

    IndexReader reader2 = IndexReader.open(dir, true);
    assertEquals(0, reader2.numDocs());

    writer.rollback();

    IndexReader reader3 = reader.reopen();
    assertEquals(0, reader.numDocs());
    assertEquals(0, reader2.numDocs());
    assertEquals(0, reader3.numDocs());
    reader.close();
    reader2.close();

    writer = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()));
    for (int i = 0; i < 17; i++)
      addDoc(writer);

    assertEquals(0, reader3.numDocs());
    reader3.close();
    reader = IndexReader.open(dir, true);
    assertEquals(0, reader.numDocs());
    reader.close();

    writer.prepareCommit();

    reader = IndexReader.open(dir, true);
    assertEquals(0, reader.numDocs());
    reader.close();

    writer.commit();
    reader = IndexReader.open(dir, true);
    assertEquals(17, reader.numDocs());
    reader.close();
    writer.close();
    dir.close();
  }

  // LUCENE-1274
  public void testPrepareCommitNoChanges() throws IOException {
    MockRAMDirectory dir = newDirectory(random);

    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()));
    writer.prepareCommit();
    writer.commit();
    writer.close();

    IndexReader reader = IndexReader.open(dir, true);
    assertEquals(0, reader.numDocs());
    reader.close();
    dir.close();
  }

  private abstract static class RunAddIndexesThreads {

    Directory dir, dir2;
    final static int NUM_INIT_DOCS = 17;
    IndexWriter writer2;
    final List<Throwable> failures = new ArrayList<Throwable>();
    volatile boolean didClose;
    final IndexReader[] readers;
    final int NUM_COPY;
    final static int NUM_THREADS = 5;
    final Thread[] threads = new Thread[NUM_THREADS];

    public RunAddIndexesThreads(int numCopy) throws Throwable {
      NUM_COPY = numCopy;
      dir = new MockRAMDirectory();
      IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(
          TEST_VERSION_CURRENT, new MockAnalyzer())
          .setMaxBufferedDocs(2));
      for (int i = 0; i < NUM_INIT_DOCS; i++)
        addDoc(writer);
      writer.close();

      dir2 = new MockRAMDirectory();
      writer2 = new IndexWriter(dir2, new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer()));
      writer2.commit();

      readers = new IndexReader[NUM_COPY];
      for(int i=0;i<NUM_COPY;i++)
        readers[i] = IndexReader.open(dir, true);
    }

    void launchThreads(final int numIter) {

      for(int i=0;i<NUM_THREADS;i++) {
        threads[i] = new Thread() {
            @Override
            public void run() {
              try {

                final Directory[] dirs = new Directory[NUM_COPY];
                for(int k=0;k<NUM_COPY;k++)
                  dirs[k] = new MockRAMDirectory(dir);

                int j=0;

                while(true) {
                  // System.out.println(Thread.currentThread().getName() + ": iter j=" + j);
                  if (numIter > 0 && j == numIter)
                    break;
                  doBody(j++, dirs);
                }
              } catch (Throwable t) {
                handle(t);
              }
            }
          };
      }

      for(int i=0;i<NUM_THREADS;i++)
        threads[i].start();
    }

    void joinThreads() throws Exception {
      for(int i=0;i<NUM_THREADS;i++)
        threads[i].join();
    }

    void close(boolean doWait) throws Throwable {
      didClose = true;
      writer2.close(doWait);
    }

    void closeDir() throws Throwable {
      for(int i=0;i<NUM_COPY;i++)
        readers[i].close();
      dir2.close();
    }

    abstract void doBody(int j, Directory[] dirs) throws Throwable;
    abstract void handle(Throwable t);
  }

  private class CommitAndAddIndexes extends RunAddIndexesThreads {
    public CommitAndAddIndexes(int numCopy) throws Throwable {
      super(numCopy);
    }

    @Override
    void handle(Throwable t) {
      t.printStackTrace(System.out);
      synchronized(failures) {
        failures.add(t);
      }
    }

    @Override
    void doBody(int j, Directory[] dirs) throws Throwable {
      switch(j%5) {
      case 0:
        writer2.addIndexes(dirs);
        writer2.optimize();
        break;
      case 1:
        writer2.addIndexes(dirs);
        break;
      case 2:
        writer2.addIndexes(readers);
        break;
      case 3:
        writer2.addIndexes(dirs);
        writer2.maybeMerge();
        break;
      case 4:
        writer2.commit();
      }
    }
  }

  // LUCENE-1335: test simultaneous addIndexes & commits
  // from multiple threads
  public void testAddIndexesWithThreads() throws Throwable {

    final int NUM_ITER = 15;
    final int NUM_COPY = 3;
    CommitAndAddIndexes c = new CommitAndAddIndexes(NUM_COPY);
    c.launchThreads(NUM_ITER);

    for(int i=0;i<100;i++)
      addDoc(c.writer2);

    c.joinThreads();

    int expectedNumDocs = 100+NUM_COPY*(4*NUM_ITER/5)*RunAddIndexesThreads.NUM_THREADS*RunAddIndexesThreads.NUM_INIT_DOCS;
    assertEquals(expectedNumDocs, c.writer2.numDocs());

    c.close(true);

    assertTrue(c.failures.size() == 0);

    _TestUtil.checkIndex(c.dir2);

    IndexReader reader = IndexReader.open(c.dir2, true);
    assertEquals(expectedNumDocs, reader.numDocs());
    reader.close();

    c.closeDir();
  }

  private class CommitAndAddIndexes2 extends CommitAndAddIndexes {
    public CommitAndAddIndexes2(int numCopy) throws Throwable {
      super(numCopy);
    }

    @Override
    void handle(Throwable t) {
      if (!(t instanceof AlreadyClosedException) && !(t instanceof NullPointerException)) {
        t.printStackTrace(System.out);
        synchronized(failures) {
          failures.add(t);
        }
      }
    }
  }

  // LUCENE-1335: test simultaneous addIndexes & close
  public void testAddIndexesWithClose() throws Throwable {
    final int NUM_COPY = 3;
    CommitAndAddIndexes2 c = new CommitAndAddIndexes2(NUM_COPY);
    //c.writer2.setInfoStream(System.out);
    c.launchThreads(-1);

    // Close w/o first stopping/joining the threads
    c.close(true);
    //c.writer2.close();

    c.joinThreads();

    _TestUtil.checkIndex(c.dir2);

    c.closeDir();

    assertTrue(c.failures.size() == 0);
  }

  private class CommitAndAddIndexes3 extends RunAddIndexesThreads {
    public CommitAndAddIndexes3(int numCopy) throws Throwable {
      super(numCopy);
    }

    @Override
    void doBody(int j, Directory[] dirs) throws Throwable {
      switch(j%5) {
      case 0:
        writer2.addIndexes(dirs);
        writer2.optimize();
        break;
      case 1:
        writer2.addIndexes(dirs);
        break;
      case 2:
        writer2.addIndexes(readers);
        break;
      case 3:
        writer2.optimize();
      case 4:
        writer2.commit();
      }
    }

    @Override
    void handle(Throwable t) {
      boolean report = true;

      if (t instanceof AlreadyClosedException || t instanceof MergePolicy.MergeAbortedException || t instanceof NullPointerException) {
        report = !didClose;
      } else if (t instanceof IOException)  {
        Throwable t2 = t.getCause();
        if (t2 instanceof MergePolicy.MergeAbortedException) {
          report = !didClose;
        }
      }
      if (report) {
        t.printStackTrace(System.out);
        synchronized(failures) {
          failures.add(t);
        }
      }
    }
  }

  // LUCENE-1335: test simultaneous addIndexes & close
  public void testAddIndexesWithCloseNoWait() throws Throwable {

    final int NUM_COPY = 50;
    CommitAndAddIndexes3 c = new CommitAndAddIndexes3(NUM_COPY);
    c.launchThreads(-1);

    Thread.sleep(500);

    // Close w/o first stopping/joining the threads
    c.close(false);

    c.joinThreads();

    _TestUtil.checkIndex(c.dir2);

    c.closeDir();

    assertTrue(c.failures.size() == 0);
  }

  // LUCENE-1335: test simultaneous addIndexes & close
  public void testAddIndexesWithRollback() throws Throwable {
    
    final int NUM_COPY = 50;
    CommitAndAddIndexes3 c = new CommitAndAddIndexes3(NUM_COPY);
    c.launchThreads(-1);

    Thread.sleep(500);

    // Close w/o first stopping/joining the threads
    c.didClose = true;
    c.writer2.rollback();

    c.joinThreads();

    _TestUtil.checkIndex(c.dir2);

    c.closeDir();

    assertTrue(c.failures.size() == 0);
  }

  // LUCENE-1347
  private static final class MockIndexWriter4 extends IndexWriter {

    public MockIndexWriter4(Directory dir, IndexWriterConfig conf) throws IOException {
      super(dir, conf);
    }

    boolean doFail;

    @Override
    boolean testPoint(String name) {
      if (doFail && name.equals("rollback before checkpoint"))
        throw new RuntimeException("intentionally failing");
      return true;
    }
  }
  

  // LUCENE-1347
  public void testRollbackExceptionHang() throws Throwable {
    MockRAMDirectory dir = newDirectory(random);
    MockIndexWriter4 w = new MockIndexWriter4(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()));

    addDoc(w);
    w.doFail = true;
    try {
      w.rollback();
      fail("did not hit intentional RuntimeException");
    } catch (RuntimeException re) {
      // expected
    }
    
    w.doFail = false;
    w.rollback();
    dir.close();
  }


  // LUCENE-1219
  public void testBinaryFieldOffsetLength() throws IOException {
    MockRAMDirectory dir = newDirectory(random);
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()));
    byte[] b = new byte[50];
    for(int i=0;i<50;i++)
      b[i] = (byte) (i+77);
    
    Document doc = new Document();
    Field f = new Field("binary", b, 10, 17);
    byte[] bx = f.getBinaryValue();
    assertTrue(bx != null);
    assertEquals(50, bx.length);
    assertEquals(10, f.getBinaryOffset());
    assertEquals(17, f.getBinaryLength());
    doc.add(f);
    w.addDocument(doc);
    w.close();

    IndexReader ir = IndexReader.open(dir, true);
    doc = ir.document(0);
    f = doc.getField("binary");
    b = f.getBinaryValue();
    assertTrue(b != null);
    assertEquals(17, b.length, 17);
    assertEquals(87, b[0]);
    ir.close();
    dir.close();
  }

  // LUCENE-1382
  public void testCommitUserData() throws IOException {
    Directory dir = newDirectory(random);
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()).setMaxBufferedDocs(2));
    for(int j=0;j<17;j++)
      addDoc(w);
    w.close();

    assertEquals(0, IndexReader.getCommitUserData(dir).size());

    IndexReader r = IndexReader.open(dir, true);
    // commit(Map) never called for this index
    assertEquals(0, r.getCommitUserData().size());
    r.close();
      
    w = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()).setMaxBufferedDocs(2));
    for(int j=0;j<17;j++)
      addDoc(w);
    Map<String,String> data = new HashMap<String,String>();
    data.put("label", "test1");
    w.commit(data);
    w.close();
      
    assertEquals("test1", IndexReader.getCommitUserData(dir).get("label"));

    r = IndexReader.open(dir, true);
    assertEquals("test1", r.getCommitUserData().get("label"));
    r.close();

    w = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()));
    w.optimize();
    w.close();

    assertEquals("test1", IndexReader.getCommitUserData(dir).get("label"));
      
    dir.close();
  }

  public void testOptimizeExceptions() throws IOException {
    MockRAMDirectory startDir = newDirectory(random);
    IndexWriterConfig conf = newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()).setMaxBufferedDocs(2);
    ((LogMergePolicy) conf.getMergePolicy()).setMergeFactor(100);
    IndexWriter w = new IndexWriter(startDir, conf);
    for(int i=0;i<27;i++)
      addDoc(w);
    w.close();

    for(int i=0;i<200;i++) {
      MockRAMDirectory dir = new MockRAMDirectory(startDir);
      conf = newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()).setMergeScheduler(new ConcurrentMergeScheduler());
      ((ConcurrentMergeScheduler) conf.getMergeScheduler()).setSuppressExceptions();
      w = new IndexWriter(dir, conf);
      dir.setRandomIOExceptionRate(0.5, 100);
      try {
        w.optimize();
      } catch (IOException ioe) {
        if (ioe.getCause() == null)
          fail("optimize threw IOException without root cause");
      }
      dir.setRandomIOExceptionRate(0, 0);
      w.close();
      dir.close();
    }
    startDir.close();
  }

  // LUCENE-1429
  public void testOutOfMemoryErrorCausesCloseToFail() throws Exception {

    final List<Throwable> thrown = new ArrayList<Throwable>();
    final Directory dir = newDirectory(random);
    final IndexWriter writer = new IndexWriter(dir,
        newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer())) {
        @Override
        public void message(final String message) {
          if (message.startsWith("now flush at close") && 0 == thrown.size()) {
            thrown.add(null);
            throw new OutOfMemoryError("fake OOME at " + message);
          }
        }
      };

    // need to set an info stream so message is called
    writer.setInfoStream(new PrintStream(new ByteArrayOutputStream()));
    try {
      writer.close();
      fail("OutOfMemoryError expected");
    }
    catch (final OutOfMemoryError expected) {}

    // throws IllegalStateEx w/o bug fix
    writer.close();
    dir.close();
  }

  // LUCENE-1442
  public void testDoubleOffsetCounting() throws Exception {
    MockRAMDirectory dir = newDirectory(random);
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(random, 
        TEST_VERSION_CURRENT, new MockAnalyzer()));
    Document doc = new Document();
    Field f = new Field("field", "abcd", Field.Store.NO, Field.Index.NOT_ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS);
    doc.add(f);
    doc.add(f);
    Field f2 = new Field("field", "", Field.Store.NO, Field.Index.NOT_ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS);
    doc.add(f2);
    doc.add(f);
    w.addDocument(doc);
    w.close();

    IndexReader r = IndexReader.open(dir, true);
    TermVectorOffsetInfo[] termOffsets = ((TermPositionVector) r.getTermFreqVector(0, "field")).getOffsets(0);

    // Token "" occurred once
    assertEquals(1, termOffsets.length);
    assertEquals(8, termOffsets[0].getStartOffset());
    assertEquals(8, termOffsets[0].getEndOffset());

    // Token "abcd" occurred three times
    termOffsets = ((TermPositionVector) r.getTermFreqVector(0, "field")).getOffsets(1);
    assertEquals(3, termOffsets.length);
    assertEquals(0, termOffsets[0].getStartOffset());
    assertEquals(4, termOffsets[0].getEndOffset());
    assertEquals(4, termOffsets[1].getStartOffset());
    assertEquals(8, termOffsets[1].getEndOffset());
    assertEquals(8, termOffsets[2].getStartOffset());
    assertEquals(12, termOffsets[2].getEndOffset());
    r.close();
    dir.close();
  }

  // LUCENE-1442
  public void testDoubleOffsetCounting2() throws Exception {
    MockRAMDirectory dir = newDirectory(random);
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()));
    Document doc = new Document();
    Field f = new Field("field", "abcd", Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS);
    doc.add(f);
    doc.add(f);
    w.addDocument(doc);
    w.close();

    IndexReader r = IndexReader.open(dir, true);
    TermVectorOffsetInfo[] termOffsets = ((TermPositionVector) r.getTermFreqVector(0, "field")).getOffsets(0);
    assertEquals(2, termOffsets.length);
    assertEquals(0, termOffsets[0].getStartOffset());
    assertEquals(4, termOffsets[0].getEndOffset());
    assertEquals(5, termOffsets[1].getStartOffset());
    assertEquals(9, termOffsets[1].getEndOffset());
    r.close();
    dir.close();
  }

  // LUCENE-1448
  public void testEndOffsetPositionCharAnalyzer() throws Exception {
    MockRAMDirectory dir = newDirectory(random);
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()));
    Document doc = new Document();
    Field f = new Field("field", "abcd   ", Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS);
    doc.add(f);
    doc.add(f);
    w.addDocument(doc);
    w.close();

    IndexReader r = IndexReader.open(dir, true);
    TermVectorOffsetInfo[] termOffsets = ((TermPositionVector) r.getTermFreqVector(0, "field")).getOffsets(0);
    assertEquals(2, termOffsets.length);
    assertEquals(0, termOffsets[0].getStartOffset());
    assertEquals(4, termOffsets[0].getEndOffset());
    assertEquals(8, termOffsets[1].getStartOffset());
    assertEquals(12, termOffsets[1].getEndOffset());
    r.close();
    dir.close();
  }

  // LUCENE-1448
  public void testEndOffsetPositionWithCachingTokenFilter() throws Exception {
    MockRAMDirectory dir = newDirectory(random);
    Analyzer analyzer = new MockAnalyzer();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, analyzer));
    Document doc = new Document();
    TokenStream stream = new CachingTokenFilter(analyzer.tokenStream("field", new StringReader("abcd   ")));
    Field f = new Field("field", stream, Field.TermVector.WITH_POSITIONS_OFFSETS);
    doc.add(f);
    doc.add(f);
    w.addDocument(doc);
    w.close();

    IndexReader r = IndexReader.open(dir, true);
    TermVectorOffsetInfo[] termOffsets = ((TermPositionVector) r.getTermFreqVector(0, "field")).getOffsets(0);
    assertEquals(2, termOffsets.length);
    assertEquals(0, termOffsets[0].getStartOffset());
    assertEquals(4, termOffsets[0].getEndOffset());
    assertEquals(8, termOffsets[1].getStartOffset());
    assertEquals(12, termOffsets[1].getEndOffset());
    r.close();
    dir.close();
  }
  
  // LUCENE-1448
  public void testEndOffsetPositionStopFilter() throws Exception {
    MockRAMDirectory dir = newDirectory(random);
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(random, 
        TEST_VERSION_CURRENT, new MockAnalyzer(MockTokenizer.SIMPLE, true, MockTokenFilter.ENGLISH_STOPSET, true)));
    Document doc = new Document();
    Field f = new Field("field", "abcd the", Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS);
    doc.add(f);
    doc.add(f);
    w.addDocument(doc);
    w.close();

    IndexReader r = IndexReader.open(dir, true);
    TermVectorOffsetInfo[] termOffsets = ((TermPositionVector) r.getTermFreqVector(0, "field")).getOffsets(0);
    assertEquals(2, termOffsets.length);
    assertEquals(0, termOffsets[0].getStartOffset());
    assertEquals(4, termOffsets[0].getEndOffset());
    assertEquals(9, termOffsets[1].getStartOffset());
    assertEquals(13, termOffsets[1].getEndOffset());
    r.close();
    dir.close();
  }

  // LUCENE-1448
  public void testEndOffsetPositionStandard() throws Exception {
    MockRAMDirectory dir = newDirectory(random);
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(random, 
        TEST_VERSION_CURRENT, new MockAnalyzer()));
    Document doc = new Document();
    Field f = new Field("field", "abcd the  ", Field.Store.NO,
        Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS);
    Field f2 = new Field("field", "crunch man", Field.Store.NO,
        Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS);
    doc.add(f);
    doc.add(f2);
    w.addDocument(doc);
    w.close();

    IndexReader r = IndexReader.open(dir, true);
    TermPositionVector tpv = ((TermPositionVector) r.getTermFreqVector(0, "field"));
    TermVectorOffsetInfo[] termOffsets = tpv.getOffsets(0);
    assertEquals(1, termOffsets.length);
    assertEquals(0, termOffsets[0].getStartOffset());
    assertEquals(4, termOffsets[0].getEndOffset());
    termOffsets = tpv.getOffsets(1);
    assertEquals(11, termOffsets[0].getStartOffset());
    assertEquals(17, termOffsets[0].getEndOffset());
    termOffsets = tpv.getOffsets(2);
    assertEquals(18, termOffsets[0].getStartOffset());
    assertEquals(21, termOffsets[0].getEndOffset());
    r.close();
    dir.close();
  }

  // LUCENE-1448
  public void testEndOffsetPositionStandardEmptyField() throws Exception {
    MockRAMDirectory dir = newDirectory(random);
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(random, 
        TEST_VERSION_CURRENT, new MockAnalyzer()));
    Document doc = new Document();
    Field f = new Field("field", "", Field.Store.NO,
                        Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS);
    Field f2 = new Field("field", "crunch man", Field.Store.NO,
        Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS);
    doc.add(f);
    doc.add(f2);
    w.addDocument(doc);
    w.close();

    IndexReader r = IndexReader.open(dir, true);
    TermPositionVector tpv = ((TermPositionVector) r.getTermFreqVector(0, "field"));
    TermVectorOffsetInfo[] termOffsets = tpv.getOffsets(0);
    assertEquals(1, termOffsets.length);
    assertEquals(0, termOffsets[0].getStartOffset());
    assertEquals(6, termOffsets[0].getEndOffset());
    termOffsets = tpv.getOffsets(1);
    assertEquals(7, termOffsets[0].getStartOffset());
    assertEquals(10, termOffsets[0].getEndOffset());
    r.close();
    dir.close();
  }

  // LUCENE-1448
  public void testEndOffsetPositionStandardEmptyField2() throws Exception {
    MockRAMDirectory dir = newDirectory(random);
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(random, 
        TEST_VERSION_CURRENT, new MockAnalyzer()));
    Document doc = new Document();

    Field f = new Field("field", "abcd", Field.Store.NO,
                        Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS);
    doc.add(f);
    doc.add(new Field("field", "", Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));

    Field f2 = new Field("field", "crunch", Field.Store.NO,
        Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS);
    doc.add(f2);

    w.addDocument(doc);
    w.close();

    IndexReader r = IndexReader.open(dir, true);
    TermPositionVector tpv = ((TermPositionVector) r.getTermFreqVector(0, "field"));
    TermVectorOffsetInfo[] termOffsets = tpv.getOffsets(0);
    assertEquals(1, termOffsets.length);
    assertEquals(0, termOffsets[0].getStartOffset());
    assertEquals(4, termOffsets[0].getEndOffset());
    termOffsets = tpv.getOffsets(1);
    assertEquals(5, termOffsets[0].getStartOffset());
    assertEquals(11, termOffsets[0].getEndOffset());
    r.close();
    dir.close();
  }


  // LUCENE-1468 -- make sure opening an IndexWriter with
  // create=true does not remove non-index files
  
  public void testOtherFiles() throws Throwable {
    File indexDir = new File(TEMP_DIR, "otherfiles");
    Directory dir = FSDirectory.open(indexDir);
    try {
      // Create my own random file:

      IndexOutput out = dir.createOutput("myrandomfile");
      out.writeByte((byte) 42);
      out.close();

      new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer())).close();

      assertTrue(dir.fileExists("myrandomfile"));
    } finally {
      dir.close();
      _TestUtil.rmDir(indexDir);
    }
  }

  public void testDeadlock() throws Exception {
    MockRAMDirectory dir = newDirectory(random);
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()).setMaxBufferedDocs(2));
    Document doc = new Document();
    doc.add(new Field("content", "aaa bbb ccc ddd eee fff ggg hhh iii", Field.Store.YES,
                      Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
    writer.addDocument(doc);
    writer.addDocument(doc);
    writer.addDocument(doc);
    writer.commit();
    // index has 2 segments

    MockRAMDirectory dir2 = newDirectory(random);
    IndexWriter writer2 = new IndexWriter(dir2, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()));
    writer2.addDocument(doc);
    writer2.close();

    IndexReader r1 = IndexReader.open(dir2, true);
    IndexReader r2 = (IndexReader) r1.clone();
    writer.addIndexes(new IndexReader[] {r1, r2});
    writer.close();

    IndexReader r3 = IndexReader.open(dir, true);
    assertEquals(5, r3.numDocs());
    r3.close();

    r1.close();
    r2.close();

    dir2.close();
    dir.close();
  }

  private class IndexerThreadInterrupt extends Thread {
    volatile boolean failed;
    volatile boolean finish;

    volatile boolean allowInterrupt = false;

    @Override
    public void run() {
      MockRAMDirectory dir;
      try { 
        dir = newDirectory(random); 
      } catch (IOException e) { throw new RuntimeException(e); }
      IndexWriter w = null;
      boolean first = true;
      while(!finish) {
        try {

          while(true) {
            if (w != null) {
              w.close();
            }
            IndexWriterConfig conf = newIndexWriterConfig(random, 
                TEST_VERSION_CURRENT, new MockAnalyzer()).setMaxBufferedDocs(2);
            ((LogMergePolicy) conf.getMergePolicy()).setMergeFactor(2);
            w = new IndexWriter(dir, conf);

            //((ConcurrentMergeScheduler) w.getMergeScheduler()).setSuppressExceptions();
            if (!first && !allowInterrupt) {
              // tell main thread it can interrupt us any time,
              // starting now
              allowInterrupt = true;
            }

            Document doc = new Document();
            doc.add(new Field("field", "some text contents", Field.Store.YES, Field.Index.ANALYZED));
            for(int i=0;i<100;i++) {
              w.addDocument(doc);
              w.commit();
            }
            w.close();
            _TestUtil.checkIndex(dir);
            IndexReader.open(dir, true).close();

            if (first && !allowInterrupt) {
              // Strangely, if we interrupt a thread before
              // all classes are loaded, the class loader
              // seems to do scary things with the interrupt
              // status.  In java 1.5, it'll throw an
              // incorrect ClassNotFoundException.  In java
              // 1.6, it'll silently clear the interrupt.
              // So, on first iteration through here we
              // don't open ourselves up for interrupts
              // until we've done the above loop.
              allowInterrupt = true;
              first = false;
            }
          }
        } catch (ThreadInterruptedException re) {
          Throwable e = re.getCause();
          assertTrue(e instanceof InterruptedException);
          if (finish) {
            break;
          }
          
          // Make sure IW cleared the interrupted bit
          // TODO: remove that false once test is fixed for real
          if (false && interrupted()) {
            System.out.println("FAILED; InterruptedException hit but thread.interrupted() was true");
            e.printStackTrace(System.out);
            failed = true;
            break;
          }

        } catch (Throwable t) {
          System.out.println("FAILED; unexpected exception");
          t.printStackTrace(System.out);
          failed = true;
          break;
        }
      }

      if (!failed) {
        // clear interrupt state:
        Thread.interrupted();
        try {
          w.rollback();
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }

        try {
          _TestUtil.checkIndex(dir);
        } catch (Exception e) {
          failed = true;
          System.out.println("CheckIndex FAILED: unexpected exception");
          e.printStackTrace(System.out);
        }
        try {
          IndexReader r = IndexReader.open(dir, true);
          //System.out.println("doc count=" + r.numDocs());
          r.close();
        } catch (Exception e) {
          failed = true;
          System.out.println("IndexReader.open FAILED: unexpected exception");
          e.printStackTrace(System.out);
        }
      }
      dir.close();
    }
  }

  public void testThreadInterruptDeadlock() throws Exception {
    IndexerThreadInterrupt t = new IndexerThreadInterrupt();
    t.setDaemon(true);
    t.start();
    
    // issue 100 interrupts to child thread
    int i = 0;
    while(i < 100) {
      Thread.sleep(1);

      if (t.allowInterrupt) {
        i++;
        t.allowInterrupt = false;
        t.interrupt();
      }
      if (!t.isAlive()) {
        break;
      }
    }
    t.allowInterrupt = false;
    t.finish = true;
    t.join();
    assertFalse(t.failed);
  }


  public void testIndexStoreCombos() throws Exception {
    MockRAMDirectory dir = newDirectory(random);
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()));
    byte[] b = new byte[50];
    for(int i=0;i<50;i++)
      b[i] = (byte) (i+77);

    Document doc = new Document();
    Field f = new Field("binary", b, 10, 17);
    f.setTokenStream(new MockTokenizer(new StringReader("doc1field1"), MockTokenizer.WHITESPACE, false));
    Field f2 = new Field("string", "value", Field.Store.YES,Field.Index.ANALYZED);
    f2.setTokenStream(new MockTokenizer(new StringReader("doc1field2"), MockTokenizer.WHITESPACE, false));
    doc.add(f);
    doc.add(f2);
    w.addDocument(doc);
    
    // add 2 docs to test in-memory merging
    f.setTokenStream(new MockTokenizer(new StringReader("doc2field1"), MockTokenizer.WHITESPACE, false));
    f2.setTokenStream(new MockTokenizer(new StringReader("doc2field2"), MockTokenizer.WHITESPACE, false));
    w.addDocument(doc);
  
    // force segment flush so we can force a segment merge with doc3 later.
    w.commit();

    f.setTokenStream(new MockTokenizer(new StringReader("doc3field1"), MockTokenizer.WHITESPACE, false));
    f2.setTokenStream(new MockTokenizer(new StringReader("doc3field2"), MockTokenizer.WHITESPACE, false));

    w.addDocument(doc);
    w.commit();
    w.optimize();   // force segment merge.
    w.close();

    IndexReader ir = IndexReader.open(dir, true);
    doc = ir.document(0);
    f = doc.getField("binary");
    b = f.getBinaryValue();
    assertTrue(b != null);
    assertEquals(17, b.length, 17);
    assertEquals(87, b[0]);

    assertTrue(ir.document(0).getFieldable("binary").isBinary());
    assertTrue(ir.document(1).getFieldable("binary").isBinary());
    assertTrue(ir.document(2).getFieldable("binary").isBinary());
    
    assertEquals("value", ir.document(0).get("string"));
    assertEquals("value", ir.document(1).get("string"));
    assertEquals("value", ir.document(2).get("string"));


    // test that the terms were indexed.
    assertTrue(MultiFields.getTermDocsEnum(ir, null, "binary", new BytesRef("doc1field1")).nextDoc() != DocsEnum.NO_MORE_DOCS);
    assertTrue(MultiFields.getTermDocsEnum(ir, null, "binary", new BytesRef("doc2field1")).nextDoc() != DocsEnum.NO_MORE_DOCS);
    assertTrue(MultiFields.getTermDocsEnum(ir, null, "binary", new BytesRef("doc3field1")).nextDoc() != DocsEnum.NO_MORE_DOCS);
    assertTrue(MultiFields.getTermDocsEnum(ir, null, "string", new BytesRef("doc1field2")).nextDoc() != DocsEnum.NO_MORE_DOCS);
    assertTrue(MultiFields.getTermDocsEnum(ir, null, "string", new BytesRef("doc2field2")).nextDoc() != DocsEnum.NO_MORE_DOCS);
    assertTrue(MultiFields.getTermDocsEnum(ir, null, "string", new BytesRef("doc3field2")).nextDoc() != DocsEnum.NO_MORE_DOCS);

    ir.close();
    dir.close();

  }

  // LUCENE-1727: make sure doc fields are stored in order
  public void testStoredFieldsOrder() throws Throwable {
    Directory d = newDirectory(random);
    IndexWriter w = new IndexWriter(d, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()));
    Document doc = new Document();
    doc.add(new Field("zzz", "a b c", Field.Store.YES, Field.Index.NO));
    doc.add(new Field("aaa", "a b c", Field.Store.YES, Field.Index.NO));
    doc.add(new Field("zzz", "1 2 3", Field.Store.YES, Field.Index.NO));
    w.addDocument(doc);
    IndexReader r = w.getReader();
    doc = r.document(0);
    Iterator<Fieldable> it = doc.getFields().iterator();
    assertTrue(it.hasNext());
    Field f = (Field) it.next();
    assertEquals(f.name(), "zzz");
    assertEquals(f.stringValue(), "a b c");

    assertTrue(it.hasNext());
    f = (Field) it.next();
    assertEquals(f.name(), "aaa");
    assertEquals(f.stringValue(), "a b c");

    assertTrue(it.hasNext());
    f = (Field) it.next();
    assertEquals(f.name(), "zzz");
    assertEquals(f.stringValue(), "1 2 3");
    assertFalse(it.hasNext());
    r.close();
    w.close();
    d.close();
  }

  public void testEmbeddedFFFF() throws Throwable {

    Directory d = newDirectory(random);
    IndexWriter w = new IndexWriter(d, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()));
    Document doc = new Document();
    doc.add(new Field("field", "a a\uffffb", Field.Store.NO, Field.Index.ANALYZED));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new Field("field", "a", Field.Store.NO, Field.Index.ANALYZED));
    w.addDocument(doc);
    IndexReader r = w.getReader();
    assertEquals(1, r.docFreq(new Term("field", "a\uffffb")));
    r.close();
    w.close();
    _TestUtil.checkIndex(d);
    d.close();
  }

  public void testNoDocsIndex() throws Throwable {
    Directory dir = newDirectory(random);
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(random, 
        TEST_VERSION_CURRENT, new MockAnalyzer()));
    LogMergePolicy lmp = (LogMergePolicy) writer.getConfig().getMergePolicy();
    lmp.setUseCompoundFile(false);
    lmp.setUseCompoundDocStore(false);
    ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
    writer.setInfoStream(new PrintStream(bos));
    writer.addDocument(new Document());
    writer.close();

    _TestUtil.checkIndex(dir);
    dir.close();
  }
  
  // LUCENE-2095: make sure with multiple threads commit
  // doesn't return until all changes are in fact in the
  // index
  public void testCommitThreadSafety() throws Throwable {
    final int NUM_THREADS = 5;
    final double RUN_SEC = 0.5;
    final Directory dir = newDirectory(random);
    final IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(random, 
        TEST_VERSION_CURRENT, new MockAnalyzer()));
    w.commit();
    final AtomicBoolean failed = new AtomicBoolean();
    Thread[] threads = new Thread[NUM_THREADS];
    final long endTime = System.currentTimeMillis()+((long) (RUN_SEC*1000));
    for(int i=0;i<NUM_THREADS;i++) {
      final int finalI = i;
      threads[i] = new Thread() {
          @Override
          public void run() {
            try {
              final Document doc = new Document();
              IndexReader r = IndexReader.open(dir);
              Field f = new Field("f", "", Field.Store.NO, Field.Index.NOT_ANALYZED);
              doc.add(f);
              int count = 0;
              do {
                if (failed.get()) break;
                for(int j=0;j<10;j++) {
                  final String s = finalI + "_" + String.valueOf(count++);
                  f.setValue(s);
                  w.addDocument(doc);
                  w.commit();
                  IndexReader r2 = r.reopen();
                  assertTrue(r2 != r);
                  r.close();
                  r = r2;
                  assertEquals("term=f:" + s + "; r=" + r, 1, r.docFreq(new Term("f", s)));
                }
              } while(System.currentTimeMillis() < endTime);
              r.close();
            } catch (Throwable t) {
              failed.set(true);
              throw new RuntimeException(t);
            }
          }
        };
      threads[i].start();
    }
    for(int i=0;i<NUM_THREADS;i++) {
      threads[i].join();
    }
    assertFalse(failed.get());
    w.close();
    dir.close();
  }

  // both start & end are inclusive
  private final int getInt(Random r, int start, int end) {
    return start + r.nextInt(1+end-start);
  }

  private void checkTermsOrder(IndexReader r, Set<String> allTerms, boolean isTop) throws IOException {
    TermsEnum terms = MultiFields.getFields(r).terms("f").iterator();

    BytesRef last = new BytesRef();

    Set<String> seenTerms = new HashSet<String>();

    while(true) {
      final BytesRef term = terms.next();
      if (term == null) {
        break;
      }

      assertTrue(last.compareTo(term) < 0);
      last.copy(term);

      final String s = term.utf8ToString();
      assertTrue("term " + termDesc(s) + " was not added to index (count=" + allTerms.size() + ")", allTerms.contains(s));
      seenTerms.add(s);
    }

    if (isTop) {
      assertTrue(allTerms.equals(seenTerms));
    }

    // Test seeking:
    Iterator<String> it = seenTerms.iterator();
    while(it.hasNext()) {
      BytesRef tr = new BytesRef(it.next());
      assertEquals("seek failed for term=" + termDesc(tr.utf8ToString()),
                   TermsEnum.SeekStatus.FOUND,
                   terms.seek(tr));
    }
  }

  private final String asUnicodeChar(char c) {
    return "U+" + Integer.toHexString(c);
  }

  private final String termDesc(String s) {
    final String s0;
    assertTrue(s.length() <= 2);
    if (s.length() == 1) {
      s0 = asUnicodeChar(s.charAt(0));
    } else {
      s0 = asUnicodeChar(s.charAt(0)) + "," + asUnicodeChar(s.charAt(1));
    }
    return s0;
  }

  // Make sure terms, including ones with surrogate pairs,
  // sort in codepoint sort order by default
  public void testTermUTF16SortOrder() throws Throwable {
    Random rnd = random;
    Directory dir = newDirectory(random);
    RandomIndexWriter writer = new RandomIndexWriter(rnd, dir);
    Document d = new Document();
    // Single segment
    Field f = new Field("f", "", Field.Store.NO, Field.Index.NOT_ANALYZED);
    d.add(f);
    char[] chars = new char[2];
    final Set<String> allTerms = new HashSet<String>();

    int num = 200 * RANDOM_MULTIPLIER;
    for (int i = 0; i < num; i++) {

      final String s;
      if (rnd.nextBoolean()) {
        // Single char
        if (rnd.nextBoolean()) {
          // Above surrogates
          chars[0] = (char) getInt(rnd, 1+UnicodeUtil.UNI_SUR_LOW_END, 0xffff);
        } else {
          // Below surrogates
          chars[0] = (char) getInt(rnd, 0, UnicodeUtil.UNI_SUR_HIGH_START-1);
        }
        s = new String(chars, 0, 1);
      } else {
        // Surrogate pair
        chars[0] = (char) getInt(rnd, UnicodeUtil.UNI_SUR_HIGH_START, UnicodeUtil.UNI_SUR_HIGH_END);
        assertTrue(((int) chars[0]) >= UnicodeUtil.UNI_SUR_HIGH_START && ((int) chars[0]) <= UnicodeUtil.UNI_SUR_HIGH_END);
        chars[1] = (char) getInt(rnd, UnicodeUtil.UNI_SUR_LOW_START, UnicodeUtil.UNI_SUR_LOW_END);
        s = new String(chars, 0, 2);
      }
      allTerms.add(s);
      f.setValue(s);

      writer.addDocument(d);

      if ((1+i) % 42 == 0) {
        writer.commit();
      }
    }

    IndexReader r = writer.getReader();

    // Test each sub-segment
    final IndexReader[] subs = r.getSequentialSubReaders();
    for(int i=0;i<subs.length;i++) {
      checkTermsOrder(subs[i], allTerms, false);
    }
    checkTermsOrder(r, allTerms, true);

    // Test multi segment
    r.close();

    writer.optimize();

    // Test optimized single segment
    r = writer.getReader();
    checkTermsOrder(r, allTerms, true);
    r.close();

    writer.close();
    dir.close();
  }

  public void testIndexDivisor() throws Exception {
    Directory dir = newDirectory(random);
    IndexWriter w = new IndexWriter(dir, new MockAnalyzer(), IndexWriter.MaxFieldLength.UNLIMITED);
    StringBuilder s = new StringBuilder();
    // must be > 256
    for(int i=0;i<300;i++) {
      s.append(' ').append(""+i);
    }
    Document d = new Document();
    Field f = new Field("field", s.toString(), Field.Store.NO, Field.Index.ANALYZED);
    d.add(f);
    w.addDocument(d);
    IndexReader r = w.getReader(2).getSequentialSubReaders()[0];
    TermsEnum t = r.fields().terms("field").iterator();
    int count = 0;
    while(t.next() != null) {
      final DocsEnum docs = t.docs(null, null);
      assertEquals(0, docs.nextDoc());
      assertEquals(DocsEnum.NO_MORE_DOCS, docs.nextDoc());
      count++;
    }
    assertEquals(300, count);
    r.close();
    w.close();
    dir.close();
  }

  public void testDeleteUnusedFiles() throws Exception {

    for(int iter=0;iter<2;iter++) {
      Directory dir = newDirectory(random);
      IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()));
      ((LogMergePolicy) w.getMergePolicy()).setUseCompoundFile(true);
      Document doc = new Document();
      doc.add(new Field("field", "go", Field.Store.NO, Field.Index.ANALYZED));
      w.addDocument(doc);
      IndexReader r;
      if (iter == 0) {
        // use NRT
        r = w.getReader();
      } else {
        // don't use NRT
        w.commit();
        r = IndexReader.open(dir);
      }

      List<String> files = Arrays.asList(dir.listAll());
      assertTrue(files.contains("_0.cfs"));
      w.addDocument(doc);
      w.optimize();
      if (iter == 1) {
        w.commit();
      }
      IndexReader r2 = r.reopen();
      assertTrue(r != r2);
      files = Arrays.asList(dir.listAll());
      assertTrue(files.contains("_0.cfs"));
      // optimize created this
      assertTrue(files.contains("_2.cfs"));
      w.deleteUnusedFiles();

      files = Arrays.asList(dir.listAll());
      // r still holds this file open
      assertTrue(files.contains("_0.cfs"));
      assertTrue(files.contains("_2.cfs"));

      r.close();
      if (iter == 0) {
        // on closing NRT reader, it calls writer.deleteUnusedFiles
        files = Arrays.asList(dir.listAll());
        assertFalse(files.contains("_0.cfs"));
      } else {
        // now writer can remove it
        w.deleteUnusedFiles();
        files = Arrays.asList(dir.listAll());
        assertFalse(files.contains("_0.cfs"));
      }
      assertTrue(files.contains("_2.cfs"));

      w.close();
      r2.close();

      dir.close();
    }
  }

  public void testDeleteUnsedFiles2() throws Exception {
    // Validates that iw.deleteUnusedFiles() also deletes unused index commits
    // in case a deletion policy which holds onto commits is used.
    Directory dir = newDirectory(random);
    SnapshotDeletionPolicy sdp = new SnapshotDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(random, 
        TEST_VERSION_CURRENT, new MockAnalyzer())
        .setIndexDeletionPolicy(sdp));
    
    // First commit
    Document doc = new Document();
    doc.add(new Field("c", "val", Store.YES, Index.ANALYZED, TermVector.WITH_POSITIONS_OFFSETS));
    writer.addDocument(doc);
    writer.commit();
    assertEquals(1, IndexReader.listCommits(dir).size());

    // Keep that commit
    sdp.snapshot("id");
    
    // Second commit - now KeepOnlyLastCommit cannot delete the prev commit.
    doc = new Document();
    doc.add(new Field("c", "val", Store.YES, Index.ANALYZED, TermVector.WITH_POSITIONS_OFFSETS));
    writer.addDocument(doc);
    writer.commit();
    assertEquals(2, IndexReader.listCommits(dir).size());

    // Should delete the unreferenced commit
    sdp.release("id");
    writer.deleteUnusedFiles();
    assertEquals(1, IndexReader.listCommits(dir).size());
    
    writer.close();
    dir.close();
  }
  
  private static class FlushCountingIndexWriter extends IndexWriter {
    int flushCount;
    public FlushCountingIndexWriter(Directory dir, IndexWriterConfig iwc) throws IOException {
      super(dir, iwc);
    }
    public void doAfterFlush() {
      flushCount++;
    }
  }

  public void testIndexingThenDeleting() throws Exception {
    final Random r = random;

    Directory dir = newDirectory(random);
    FlushCountingIndexWriter w = new FlushCountingIndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()).setRAMBufferSizeMB(0.5).setMaxBufferedDocs(-1).setMaxBufferedDeleteTerms(-1));
    //w.setInfoStream(System.out);
    Document doc = new Document();
    doc.add(new Field("field", "go 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20", Field.Store.NO, Field.Index.ANALYZED));
    int num = 6 * RANDOM_MULTIPLIER;
    for (int iter = 0; iter < num; iter++) {
      int count = 0;

      final boolean doIndexing = r.nextBoolean();
      if (doIndexing) {
        // Add docs until a flush is triggered
        final int startFlushCount = w.flushCount;
        while(w.flushCount == startFlushCount) {
          w.addDocument(doc);
          count++;
        }
      } else {
        // Delete docs until a flush is triggered
        final int startFlushCount = w.flushCount;
        while(w.flushCount == startFlushCount) {
          w.deleteDocuments(new Term("foo", ""+count));
          count++;
        }
      }
      assertTrue("flush happened too quickly during " + (doIndexing ? "indexing" : "deleting") + " count=" + count, count > 2500);
    }
    w.close();
    dir.close();
  }
  
  public void testNoCommits() throws Exception {
    // Tests that if we don't call commit(), the directory has 0 commits. This has
    // changed since LUCENE-2386, where before IW would always commit on a fresh
    // new index.
    Directory dir = newDirectory(random);
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()));
    try {
      IndexReader.listCommits(dir);
      fail("listCommits should have thrown an exception over empty index");
    } catch (IndexNotFoundException e) {
      // that's expected !
    }
    // No changes still should generate a commit, because it's a new index.
    writer.close();
    assertEquals("expected 1 commits!", 1, IndexReader.listCommits(dir).size());
    dir.close();
  }

  public void testEmptyFSDirWithNoLock() throws Exception {
    // Tests that if FSDir is opened w/ a NoLockFactory (or SingleInstanceLF),
    // then IndexWriter ctor succeeds. Previously (LUCENE-2386) it failed 
    // when listAll() was called in IndexFileDeleter.
    FSDirectory dir = FSDirectory.open(new File(TEMP_DIR, "emptyFSDirNoLock"), NoLockFactory.getNoLockFactory());
    new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer())).close();
  }

  public void testEmptyDirRollback() throws Exception {
    // Tests that if IW is created over an empty Directory, some documents are
    // indexed, flushed (but not committed) and then IW rolls back, then no 
    // files are left in the Directory.
    Directory dir = newDirectory(random);
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(random, 
        TEST_VERSION_CURRENT, new MockAnalyzer())
        .setMaxBufferedDocs(2));
    // Creating over empty dir should not create any files.
    assertEquals(0, dir.listAll().length);
    Document doc = new Document();
    // create as many files as possible
    doc.add(new Field("c", "val", Store.YES, Index.ANALYZED, TermVector.WITH_POSITIONS_OFFSETS));
    writer.addDocument(doc);
    // Adding just one document does not call flush yet.
    assertEquals("only the stored and term vector files should exist in the directory", 5, dir.listAll().length);
    
    doc = new Document();
    doc.add(new Field("c", "val", Store.YES, Index.ANALYZED, TermVector.WITH_POSITIONS_OFFSETS));
    writer.addDocument(doc);
    // The second document should cause a flush.
    assertTrue("flush should have occurred and files created", dir.listAll().length > 5);
   
    // After rollback, IW should remove all files
    writer.rollback();
    assertEquals("no files should exist in the directory after rollback", 0, dir.listAll().length);

    // Since we rolled-back above, that close should be a no-op
    writer.close();
    assertEquals("expected a no-op close after IW.rollback()", 0, dir.listAll().length);
    dir.close();
  }

  public void testNoSegmentFile() throws IOException {
    File tempDir = _TestUtil.getTempDir("noSegmentFile");
    try {
      Directory dir = FSDirectory.open(tempDir);
      dir.setLockFactory(NoLockFactory.getNoLockFactory());
      IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(random, 
          TEST_VERSION_CURRENT, new MockAnalyzer()).setMaxBufferedDocs(2));

      Document doc = new Document();
      doc.add(new Field("c", "val", Store.YES, Index.ANALYZED, TermVector.WITH_POSITIONS_OFFSETS));
      w.addDocument(doc);
      w.addDocument(doc);
      IndexWriter w2 = new IndexWriter(dir, newIndexWriterConfig(random, 
          TEST_VERSION_CURRENT, new MockAnalyzer()).setMaxBufferedDocs(2)
          .setOpenMode(OpenMode.CREATE));

      w2.close();
      // If we don't do that, the test fails on Windows
      w.rollback();
      dir.close();
    } finally {
      _TestUtil.rmDir(tempDir);
    }
  }

  public void testFutureCommit() throws Exception {
    Directory dir = newDirectory(random);

    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()).setIndexDeletionPolicy(NoDeletionPolicy.INSTANCE));
    Document doc = new Document();
    w.addDocument(doc);

    // commit to "first"
    Map<String,String> commitData = new HashMap<String,String>();
    commitData.put("tag", "first");
    w.commit(commitData);

    // commit to "second"
    w.addDocument(doc);
    commitData.put("tag", "second");
    w.commit(commitData);
    w.close();

    // open "first" with IndexWriter
    IndexCommit commit = null;
    for(IndexCommit c : IndexReader.listCommits(dir)) {
      if (c.getUserData().get("tag").equals("first")) {
        commit = c;
        break;
      }
    }

    assertNotNull(commit);

    w = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()).setIndexDeletionPolicy(NoDeletionPolicy.INSTANCE).setIndexCommit(commit));

    assertEquals(1, w.numDocs());
    
    // commit IndexWriter to "third"
    w.addDocument(doc);
    commitData.put("tag", "third");
    w.commit(commitData);
    w.close();

    // make sure "second" commit is still there
    commit = null;
    for(IndexCommit c : IndexReader.listCommits(dir)) {
      if (c.getUserData().get("tag").equals("second")) {
        commit = c;
        break;
      }
    }

    assertNotNull(commit);

    IndexReader r = IndexReader.open(commit, true);
    assertEquals(2, r.numDocs());
    r.close();

    // open "second", w/ writeable IndexReader & commit
    r = IndexReader.open(commit, NoDeletionPolicy.INSTANCE, false);
    assertEquals(2, r.numDocs());
    r.deleteDocument(0);
    r.deleteDocument(1);
    commitData.put("tag", "fourth");
    r.commit(commitData);
    r.close();

    // make sure "third" commit is still there
    commit = null;
    for(IndexCommit c : IndexReader.listCommits(dir)) {
      if (c.getUserData().get("tag").equals("third")) {
        commit = c;
        break;
      }
    }
    assertNotNull(commit);

    dir.close();
  }

  public void testRandomStoredFields() throws IOException {
    File index = _TestUtil.getTempDir("lucenerandfields");
    Directory dir = FSDirectory.open(index);
    Random rand = random;
    RandomIndexWriter w = new RandomIndexWriter(rand, dir, newIndexWriterConfig(rand, TEST_VERSION_CURRENT, new MockAnalyzer()).setMaxBufferedDocs(_TestUtil.nextInt(rand, 5, 20)));
    //w.w.setInfoStream(System.out);
    //w.w.setUseCompoundFile(false);
    final int docCount = 200*RANDOM_MULTIPLIER;
    final int fieldCount = _TestUtil.nextInt(rand, 1, 5);
      
    final List<Integer> fieldIDs = new ArrayList<Integer>();

    Field idField = new Field("id", "", Field.Store.YES, Field.Index.NOT_ANALYZED);

    for(int i=0;i<fieldCount;i++) {
      fieldIDs.add(i);
    }

    final Map<String,Document> docs = new HashMap<String,Document>();
    
    for(int i=0;i<docCount;i++) {
      Document doc = new Document();
      doc.add(idField);
      final String id = ""+i;
      idField.setValue(id);
      docs.put(id, doc);

      for(int field: fieldIDs) {
        final String s;
        if (rand.nextInt(4) != 3) {
          s = _TestUtil.randomUnicodeString(rand, 1000);
          doc.add(new Field("f"+field, s, Field.Store.YES, Field.Index.NO));
        } else {
          s = null;
        }
      }
      w.addDocument(doc);
      if (rand.nextInt(50) == 17) {
        // mixup binding of field name -> Number every so often
        Collections.shuffle(fieldIDs);
      }
      if (rand.nextInt(5) == 3 && i > 0) {
        final String delID = ""+rand.nextInt(i);
        w.deleteDocuments(new Term("id", delID));
        docs.remove(delID);
      }
    }

    if (docs.size() > 0) {
      String[] idsList = docs.keySet().toArray(new String[docs.size()]);

      for(int x=0;x<2;x++) {
        IndexReader r = w.getReader();
        IndexSearcher s = new IndexSearcher(r);

        for(int iter=0;iter<1000*RANDOM_MULTIPLIER;iter++) {
          String testID = idsList[rand.nextInt(idsList.length)];
          TopDocs hits = s.search(new TermQuery(new Term("id", testID)), 1);
          assertEquals(1, hits.totalHits);
          Document doc = r.document(hits.scoreDocs[0].doc);
          Document docExp = docs.get(testID);
          for(int i=0;i<fieldCount;i++) {
            assertEquals("doc " + testID + ", field f" + fieldCount + " is wrong", docExp.get("f"+i),  doc.get("f"+i));
          }
        }
        r.close();
        w.optimize();
      }
    }
    w.close();
    dir.close();
    _TestUtil.rmDir(index);
  }

  private static class FailTwiceDuringMerge extends MockRAMDirectory.Failure {
    public boolean didFail1;
    public boolean didFail2;

    @Override
    public void eval(MockRAMDirectory dir)  throws IOException {
      if (!doFail) {
        return;
      }
      StackTraceElement[] trace = new Exception().getStackTrace();
      for (int i = 0; i < trace.length; i++) {
        if ("org.apache.lucene.index.SegmentMerger".equals(trace[i].getClassName()) && "mergeTerms".equals(trace[i].getMethodName()) && !didFail1) {
          didFail1 = true;
          throw new IOException("fake disk full during mergeTerms");
        }
        if ("org.apache.lucene.util.BitVector".equals(trace[i].getClassName()) && "write".equals(trace[i].getMethodName()) && !didFail2) {
          didFail2 = true;
          throw new IOException("fake disk full while writing BitVector");
        }
      }
    }
  }
  
  // LUCENE-2593
  public void testCorruptionAfterDiskFullDuringMerge() throws IOException {
    MockRAMDirectory dir = newDirectory(random);
    final Random rand = random;
    //IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(rand, TEST_VERSION_CURRENT, new MockAnalyzer()).setReaderPooling(true));
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(rand, TEST_VERSION_CURRENT, new MockAnalyzer()).setMergeScheduler(new SerialMergeScheduler()).setReaderPooling(true));

    ((LogMergePolicy) w.getMergePolicy()).setMergeFactor(2);

    Document doc = new Document();
    doc.add(new Field("f", "doctor who", Field.Store.YES, Field.Index.ANALYZED));
    w.addDocument(doc);

    w.commit();

    w.deleteDocuments(new Term("f", "who"));
    w.addDocument(doc);
    
    // disk fills up!
    FailTwiceDuringMerge ftdm = new FailTwiceDuringMerge();
    ftdm.setDoFail();
    dir.failOn(ftdm);

    try {
      w.commit();
      fail("fake disk full IOExceptions not hit");
    } catch (IOException ioe) {
      // expected
      assertTrue(ftdm.didFail1);
    }
    _TestUtil.checkIndex(dir);
    ftdm.clearDoFail();
    w.addDocument(doc);
    w.close();

    _TestUtil.checkIndex(dir);
    dir.close();
  }
}
