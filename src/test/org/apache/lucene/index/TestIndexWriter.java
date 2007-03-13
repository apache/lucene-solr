package org.apache.lucene.index;

import java.io.IOException;
import java.io.File;
import java.util.Arrays;

import junit.framework.TestCase;

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import org.apache.lucene.store.MockRAMDirectory;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.SingleInstanceLockFactory;

/**
 * @author goller
 * @version $Id$
 */
public class TestIndexWriter extends TestCase
{
    public void testDocCount() throws IOException
    {
        Directory dir = new RAMDirectory();

        IndexWriter writer = null;
        IndexReader reader = null;
        int i;

        IndexWriter.setDefaultWriteLockTimeout(2000);
        assertEquals(2000, IndexWriter.getDefaultWriteLockTimeout());

        writer  = new IndexWriter(dir, new WhitespaceAnalyzer());

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
        writer = new IndexWriter(dir, new WhitespaceAnalyzer());
        assertEquals(100, writer.docCount());
        writer.close();

        reader = IndexReader.open(dir);
        assertEquals(100, reader.maxDoc());
        assertEquals(60, reader.numDocs());
        reader.close();

        // optimize the index and check that the new doc count is correct
        writer = new IndexWriter(dir, true, new WhitespaceAnalyzer());
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
        writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true);
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
        IndexWriter writer  = new IndexWriter(dirs[i], new WhitespaceAnalyzer(), true);
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
      IndexWriter writer = new IndexWriter(startDir, new WhitespaceAnalyzer(), true);        
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

        String testName = "disk full test for method " + methodName + " with disk full at " + diskFree + " bytes with autoCommit = " + autoCommit;

        int cycleCount = 0;

        while(!done) {

          cycleCount++;

          // Make a new dir that will enforce disk usage:
          MockRAMDirectory dir = new MockRAMDirectory(startDir);
          writer = new IndexWriter(dir, autoCommit, new WhitespaceAnalyzer(), false);
          IOException err = null;

          for(int x=0;x<2;x++) {

            // Two loops: first time, limit disk space &
            // throw random IOExceptions; second time, no
            // disk space limit:

            double rate = 0.05;
            double diskRatio = ((double) diskFree)/diskUsage;
            long thisDiskFree;

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
                System.out.println("\ncycle: " + methodName + ": " + diskFree + " bytes");
              }
            } else {
              thisDiskFree = 0;
              rate = 0.0;
              if (debug) {
                System.out.println("\ncycle: " + methodName + ", same writer: unlimited disk space");
              }
            }

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
              }

              if (1 == x) {
                e.printStackTrace();
                fail(methodName + " hit IOException after disk space was freed up");
              }
            }

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
              e.printStackTrace();
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
                err.printStackTrace();
                fail(testName + ": method did throw exception but docFreq('aaa') is " + result + " instead of expected " + START_COUNT + " or " + END_COUNT);
              }
            }

            searcher = new IndexSearcher(reader);
            try {
              hits = searcher.search(new TermQuery(searchTerm));
            } catch (IOException e) {
              e.printStackTrace();
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
                err.printStackTrace();
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

      for(int pass=0;pass<3;pass++) {
        boolean autoCommit = pass == 0;
        boolean doAbort = pass == 2;
        long diskFree = 200;
        while(true) {
          MockRAMDirectory dir = new MockRAMDirectory();
          dir.setMaxSizeInBytes(diskFree);
          IndexWriter writer = new IndexWriter(dir, autoCommit, new WhitespaceAnalyzer(), true);
          boolean hitError = false;
          try {
            for(int i=0;i<200;i++) {
              addDoc(writer);
            }
          } catch (IOException e) {
            // e.printStackTrace();
            hitError = true;
          }

          if (hitError) {
            if (doAbort) {
              writer.abort();
            } else {
              try {
                writer.close();
              } catch (IOException e) {
                // e.printStackTrace();
                dir.setMaxSizeInBytes(0);
                writer.close();
              }
            }

            assertNoUnreferencedFiles(dir, "after disk full during addDocument with autoCommit=" + autoCommit);

            // Make sure reader can open the index:
            IndexReader.open(dir).close();

            dir.close();

            // Now try again w/ more space:
            diskFree += 500;
          } else {
            dir.close();
            break;
          }
        }
      }
    
    }                                               

    public void assertNoUnreferencedFiles(Directory dir, String message) throws IOException {
      String[] startFiles = dir.list();
      SegmentInfos infos = new SegmentInfos();
      infos.read(dir);
      IndexFileDeleter d = new IndexFileDeleter(dir, new KeepOnlyLastCommitDeletionPolicy(), infos, null);
      String[] endFiles = dir.list();

      Arrays.sort(startFiles);
      Arrays.sort(endFiles);

      if (!Arrays.equals(startFiles, endFiles)) {
        fail(message + ": before delete:\n    " + arrayToString(startFiles) + "\n  after delete:\n    " + arrayToString(endFiles));
      }
    }

    /**
     * Make sure optimize doesn't use any more than 1X
     * starting index size as its temporary free space
     * required.
     */
    public void testOptimizeTempSpaceUsage() throws IOException {
    
      MockRAMDirectory dir = new MockRAMDirectory();
      IndexWriter writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true);
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
      writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), false);
      writer.optimize();
      writer.close();
      long maxDiskUsage = dir.getMaxUsedSizeInBytes();

      assertTrue("optimized used too much temporary space: starting usage was " + startDiskUsage + " bytes; max temp usage was " + maxDiskUsage + " but should have been " + (2*startDiskUsage) + " (= 2X starting usage)",
                 maxDiskUsage <= 2*startDiskUsage);
      dir.close();
    }

    private String arrayToString(String[] l) {
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
          IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true);
          addDoc(writer);
          writer.close();

          // now open reader:
          IndexReader reader = IndexReader.open(dir);
          assertEquals("should be one document", reader.numDocs(), 1);

          // now open index for create:
          writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true);
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
          IndexWriter writer = new IndexWriter(indexDir, new WhitespaceAnalyzer(), true);
          addDoc(writer);
          writer.close();

          // now open reader:
          IndexReader reader = IndexReader.open(indexDir);
          assertEquals("should be one document", reader.numDocs(), 1);

          // now open index for create:
          writer = new IndexWriter(indexDir, new WhitespaceAnalyzer(), true);
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
          IndexWriter writer = new IndexWriter(dirName, new WhitespaceAnalyzer(), true);
          addDoc(writer);
          writer.close();

          // now open reader:
          IndexReader reader = IndexReader.open(dirName);
          assertEquals("should be one document", reader.numDocs(), 1);

          // now open index for create:
          writer = new IndexWriter(dirName, new WhitespaceAnalyzer(), true);
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

        writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true);

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
          writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true);
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

        writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true);

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

    // Simulate a corrupt index by removing one of the cfs
    // files and make sure we get an IOException trying to
    // open the index:
    public void testSimulatedCorruptIndex2() throws IOException {
        Directory dir = new RAMDirectory();

        IndexWriter writer = null;

        writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true);

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
        IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true);
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

        writer = new IndexWriter(dir, false, new WhitespaceAnalyzer());
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
      Directory dir = new RAMDirectory();      
      IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true);
      for (int i = 0; i < 14; i++) {
        addDoc(writer);
      }
      writer.close();

      Term searchTerm = new Term("content", "aaa");        
      IndexSearcher searcher = new IndexSearcher(dir);
      Hits hits = searcher.search(new TermQuery(searchTerm));
      assertEquals("first number of hits", 14, hits.length());
      searcher.close();

      writer = new IndexWriter(dir, false, new WhitespaceAnalyzer(), false);
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
      writer = new IndexWriter(dir, false, new WhitespaceAnalyzer(), false);
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
      IndexWriter writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true);
      for(int j=0;j<30;j++) {
        addDocWithIndex(writer, j);
      }
      writer.close();
      dir.resetMaxUsedSizeInBytes();

      long startDiskUsage = dir.getMaxUsedSizeInBytes();
      writer  = new IndexWriter(dir, false, new WhitespaceAnalyzer(), false);
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
      IndexWriter writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true);
      for(int j=0;j<17;j++) {
        addDocWithIndex(writer, j);
      }
      writer.close();

      writer  = new IndexWriter(dir, false, new WhitespaceAnalyzer(), false);
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

      writer  = new IndexWriter(dir, false, new WhitespaceAnalyzer(), false);
      writer.optimize();
      writer.close();
      assertNoUnreferencedFiles(dir, "aborted writer after optimize");

      // Open a reader after aborting writer:
      reader = IndexReader.open(dir);

      // Reader should still see index as unoptimized:
      assertTrue("Reader incorrectly sees that the index is unoptimized", reader.isOptimized());
      reader.close();
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
      IndexWriter writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true);
      for (int i = 0; i < 100; i++) {
        addDoc(writer);
      }
      writer.close();
      IndexReader reader = IndexReader.open(dir);
      Term searchTerm = new Term("content", "aaa");        
      IndexSearcher searcher = new IndexSearcher(dir);
      Hits hits = searcher.search(new TermQuery(searchTerm));
      assertEquals("did not get right number of hits", 100, hits.length());
      writer.close();

      writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true);
      writer.close();

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
}


