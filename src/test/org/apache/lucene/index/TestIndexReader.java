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


import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util._TestUtil;

import java.util.Collection;
import java.util.Arrays;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.File;

import org.apache.lucene.store.MockRAMDirectory;

public class TestIndexReader extends TestCase
{
    /** Main for running test case by itself. */
    public static void main(String args[]) {
        TestRunner.run (new TestSuite(TestIndexReader.class));
//        TestRunner.run (new TestIndexReader("testBasicDelete"));
//        TestRunner.run (new TestIndexReader("testDeleteReaderWriterConflict"));
//        TestRunner.run (new TestIndexReader("testDeleteReaderReaderConflict"));
//        TestRunner.run (new TestIndexReader("testFilesOpenClose"));
    }

    public TestIndexReader(String name) {
        super(name);
    }

    public void testIsCurrent() throws Exception
    {
      RAMDirectory d = new MockRAMDirectory();
      IndexWriter writer = new IndexWriter(d, new StandardAnalyzer(), true);
      addDocumentWithFields(writer);
      writer.close();
      // set up reader:
      IndexReader reader = IndexReader.open(d);
      assertTrue(reader.isCurrent());
      // modify index by adding another document:
      writer = new IndexWriter(d, new StandardAnalyzer(), false);
      addDocumentWithFields(writer);
      writer.close();
      assertFalse(reader.isCurrent());
      // re-create index:
      writer = new IndexWriter(d, new StandardAnalyzer(), true);
      addDocumentWithFields(writer);
      writer.close();
      assertFalse(reader.isCurrent());
      reader.close();
      d.close();
    }

    /**
     * Tests the IndexReader.getFieldNames implementation
     * @throws Exception on error
     */
    public void testGetFieldNames() throws Exception
    {
        RAMDirectory d = new MockRAMDirectory();
        // set up writer
        IndexWriter writer = new IndexWriter(d, new StandardAnalyzer(), true);
        addDocumentWithFields(writer);
        writer.close();
        // set up reader
        IndexReader reader = IndexReader.open(d);
        Collection fieldNames = reader.getFieldNames(IndexReader.FieldOption.ALL);
        assertTrue(fieldNames.contains("keyword"));
        assertTrue(fieldNames.contains("text"));
        assertTrue(fieldNames.contains("unindexed"));
        assertTrue(fieldNames.contains("unstored"));
        reader.close();
        // add more documents
        writer = new IndexWriter(d, new StandardAnalyzer(), false);
        // want to get some more segments here
        for (int i = 0; i < 5*writer.getMergeFactor(); i++)
        {
            addDocumentWithFields(writer);
        }
        // new fields are in some different segments (we hope)
        for (int i = 0; i < 5*writer.getMergeFactor(); i++)
        {
            addDocumentWithDifferentFields(writer);
        }
        // new termvector fields
        for (int i = 0; i < 5*writer.getMergeFactor(); i++)
        {
          addDocumentWithTermVectorFields(writer);
        }
        
        writer.close();
        // verify fields again
        reader = IndexReader.open(d);
        fieldNames = reader.getFieldNames(IndexReader.FieldOption.ALL);
        assertEquals(13, fieldNames.size());    // the following fields
        assertTrue(fieldNames.contains("keyword"));
        assertTrue(fieldNames.contains("text"));
        assertTrue(fieldNames.contains("unindexed"));
        assertTrue(fieldNames.contains("unstored"));
        assertTrue(fieldNames.contains("keyword2"));
        assertTrue(fieldNames.contains("text2"));
        assertTrue(fieldNames.contains("unindexed2"));
        assertTrue(fieldNames.contains("unstored2"));
        assertTrue(fieldNames.contains("tvnot"));
        assertTrue(fieldNames.contains("termvector"));
        assertTrue(fieldNames.contains("tvposition"));
        assertTrue(fieldNames.contains("tvoffset"));
        assertTrue(fieldNames.contains("tvpositionoffset"));
        
        // verify that only indexed fields were returned
        fieldNames = reader.getFieldNames(IndexReader.FieldOption.INDEXED);
        assertEquals(11, fieldNames.size());    // 6 original + the 5 termvector fields 
        assertTrue(fieldNames.contains("keyword"));
        assertTrue(fieldNames.contains("text"));
        assertTrue(fieldNames.contains("unstored"));
        assertTrue(fieldNames.contains("keyword2"));
        assertTrue(fieldNames.contains("text2"));
        assertTrue(fieldNames.contains("unstored2"));
        assertTrue(fieldNames.contains("tvnot"));
        assertTrue(fieldNames.contains("termvector"));
        assertTrue(fieldNames.contains("tvposition"));
        assertTrue(fieldNames.contains("tvoffset"));
        assertTrue(fieldNames.contains("tvpositionoffset"));
        
        // verify that only unindexed fields were returned
        fieldNames = reader.getFieldNames(IndexReader.FieldOption.UNINDEXED);
        assertEquals(2, fieldNames.size());    // the following fields
        assertTrue(fieldNames.contains("unindexed"));
        assertTrue(fieldNames.contains("unindexed2"));
                
        // verify index term vector fields  
        fieldNames = reader.getFieldNames(IndexReader.FieldOption.TERMVECTOR);
        assertEquals(1, fieldNames.size());    // 1 field has term vector only
        assertTrue(fieldNames.contains("termvector"));
        
        fieldNames = reader.getFieldNames(IndexReader.FieldOption.TERMVECTOR_WITH_POSITION);
        assertEquals(1, fieldNames.size());    // 4 fields are indexed with term vectors
        assertTrue(fieldNames.contains("tvposition"));
        
        fieldNames = reader.getFieldNames(IndexReader.FieldOption.TERMVECTOR_WITH_OFFSET);
        assertEquals(1, fieldNames.size());    // 4 fields are indexed with term vectors
        assertTrue(fieldNames.contains("tvoffset"));
                
        fieldNames = reader.getFieldNames(IndexReader.FieldOption.TERMVECTOR_WITH_POSITION_OFFSET);
        assertEquals(1, fieldNames.size());    // 4 fields are indexed with term vectors
        assertTrue(fieldNames.contains("tvpositionoffset"));
        reader.close();
        d.close();
    }


    private void assertTermDocsCount(String msg,
                                     IndexReader reader,
                                     Term term,
                                     int expected)
    throws IOException
    {
        TermDocs tdocs = null;

        try {
            tdocs = reader.termDocs(term);
            assertNotNull(msg + ", null TermDocs", tdocs);
            int count = 0;
            while(tdocs.next()) {
                count++;
            }
            assertEquals(msg + ", count mismatch", expected, count);

        } finally {
            if (tdocs != null)
                tdocs.close();
        }

    }



    public void testBasicDelete() throws IOException
    {
        Directory dir = new MockRAMDirectory();

        IndexWriter writer = null;
        IndexReader reader = null;
        Term searchTerm = new Term("content", "aaa");

        //  add 100 documents with term : aaa
        writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true);
        for (int i = 0; i < 100; i++)
        {
            addDoc(writer, searchTerm.text());
        }
        writer.close();

        // OPEN READER AT THIS POINT - this should fix the view of the
        // index at the point of having 100 "aaa" documents and 0 "bbb"
        reader = IndexReader.open(dir);
        assertEquals("first docFreq", 100, reader.docFreq(searchTerm));
        assertTermDocsCount("first reader", reader, searchTerm, 100);
        reader.close();

        // DELETE DOCUMENTS CONTAINING TERM: aaa
        int deleted = 0;
        reader = IndexReader.open(dir);
        deleted = reader.deleteDocuments(searchTerm);
        assertEquals("deleted count", 100, deleted);
        assertEquals("deleted docFreq", 100, reader.docFreq(searchTerm));
        assertTermDocsCount("deleted termDocs", reader, searchTerm, 0);

        // open a 2nd reader to make sure first reader can
        // commit its changes (.del) while second reader
        // is open:
        IndexReader reader2 = IndexReader.open(dir);
        reader.close();

        // CREATE A NEW READER and re-test
        reader = IndexReader.open(dir);
        assertEquals("deleted docFreq", 100, reader.docFreq(searchTerm));
        assertTermDocsCount("deleted termDocs", reader, searchTerm, 0);
        reader.close();
        reader2.close();
        dir.close();
    }

    // Make sure attempts to make changes after reader is
    // closed throws IOException:
    public void testChangesAfterClose() throws IOException
    {
        Directory dir = new RAMDirectory();

        IndexWriter writer = null;
        IndexReader reader = null;
        Term searchTerm = new Term("content", "aaa");

        //  add 11 documents with term : aaa
        writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true);
        for (int i = 0; i < 11; i++)
        {
            addDoc(writer, searchTerm.text());
        }
        writer.close();

        reader = IndexReader.open(dir);

        // Close reader:
        reader.close();

        // Then, try to make changes:
        try {
          reader.deleteDocument(4);
          fail("deleteDocument after close failed to throw IOException");
        } catch (IOException e) {
          // expected
        }

        try {
          reader.setNorm(5, "aaa", 2.0f);
          fail("setNorm after close failed to throw IOException");
        } catch (IOException e) {
          // expected
        }

        try {
          reader.undeleteAll();
          fail("undeleteAll after close failed to throw IOException");
        } catch (IOException e) {
          // expected
        }
    }

    // Make sure we get lock obtain failed exception with 2 writers:
    public void testLockObtainFailed() throws IOException
    {
        Directory dir = new RAMDirectory();

        IndexWriter writer = null;
        IndexReader reader = null;
        Term searchTerm = new Term("content", "aaa");

        //  add 11 documents with term : aaa
        writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true);
        for (int i = 0; i < 11; i++)
        {
            addDoc(writer, searchTerm.text());
        }

        // Create reader:
        reader = IndexReader.open(dir);

        // Try to make changes
        try {
          reader.deleteDocument(4);
          fail("deleteDocument should have hit LockObtainFailedException");
        } catch (LockObtainFailedException e) {
          // expected
        }

        try {
          reader.setNorm(5, "aaa", 2.0f);
          fail("setNorm should have hit LockObtainFailedException");
        } catch (LockObtainFailedException e) {
          // expected
        }

        try {
          reader.undeleteAll();
          fail("undeleteAll should have hit LockObtainFailedException");
        } catch (LockObtainFailedException e) {
          // expected
        }
        writer.close();
        reader.close();
    }

    // Make sure you can set norms & commit even if a reader
    // is open against the index:
    public void testWritingNorms() throws IOException
    {
        String tempDir = System.getProperty("tempDir");
        if (tempDir == null)
            throw new IOException("tempDir undefined, cannot run test");

        File indexDir = new File(tempDir, "lucenetestnormwriter");
        Directory dir = FSDirectory.getDirectory(indexDir);
        IndexWriter writer = null;
        IndexReader reader = null;
        Term searchTerm = new Term("content", "aaa");

        //  add 1 documents with term : aaa
        writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true);
        addDoc(writer, searchTerm.text());
        writer.close();

        //  now open reader & set norm for doc 0
        reader = IndexReader.open(dir);
        reader.setNorm(0, "content", (float) 2.0);

        // we should be holding the write lock now:
        assertTrue("locked", IndexReader.isLocked(dir));

        reader.commit();

        // we should not be holding the write lock now:
        assertTrue("not locked", !IndexReader.isLocked(dir));

        // open a 2nd reader:
        IndexReader reader2 = IndexReader.open(dir);

        // set norm again for doc 0
        reader.setNorm(0, "content", (float) 3.0);
        assertTrue("locked", IndexReader.isLocked(dir));

        reader.close();

        // we should not be holding the write lock now:
        assertTrue("not locked", !IndexReader.isLocked(dir));

        reader2.close();
        dir.close();

        rmDir(indexDir);
    }


    // Make sure you can set norms & commit, and there are
    // no extra norms files left:
    public void testWritingNormsNoReader() throws IOException
    {
        Directory dir = new MockRAMDirectory();
        IndexWriter writer = null;
        IndexReader reader = null;
        Term searchTerm = new Term("content", "aaa");

        //  add 1 documents with term : aaa
        writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true);
        writer.setUseCompoundFile(false);
        addDoc(writer, searchTerm.text());
        writer.close();

        //  now open reader & set norm for doc 0 (writes to
        //  _0_1.s0)
        reader = IndexReader.open(dir);
        reader.setNorm(0, "content", (float) 2.0);
        reader.close();
        
        //  now open reader again & set norm for doc 0 (writes to _0_2.s0)
        reader = IndexReader.open(dir);
        reader.setNorm(0, "content", (float) 2.0);
        reader.close();
        assertFalse("failed to remove first generation norms file on writing second generation",
                    dir.fileExists("_0_1.s0"));
        
        dir.close();
    }


    public void testDeleteReaderWriterConflictUnoptimized() throws IOException{
      deleteReaderWriterConflict(false);
    }

    public void testOpenEmptyDirectory() throws IOException{
      String dirName = "test.empty";
      File fileDirName = new File(dirName);
      if (!fileDirName.exists()) {
        fileDirName.mkdir();
      }
      try {
        IndexReader reader = IndexReader.open(fileDirName);
        fail("opening IndexReader on empty directory failed to produce FileNotFoundException");
      } catch (FileNotFoundException e) {
        // GOOD
      }
      rmDir(fileDirName);
    }
    
    public void testDeleteReaderWriterConflictOptimized() throws IOException{
        deleteReaderWriterConflict(true);
    }

    private void deleteReaderWriterConflict(boolean optimize) throws IOException
    {
        //Directory dir = new RAMDirectory();
        Directory dir = getDirectory();

        Term searchTerm = new Term("content", "aaa");
        Term searchTerm2 = new Term("content", "bbb");

        //  add 100 documents with term : aaa
        IndexWriter writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true);
        for (int i = 0; i < 100; i++)
        {
            addDoc(writer, searchTerm.text());
        }
        writer.close();

        // OPEN READER AT THIS POINT - this should fix the view of the
        // index at the point of having 100 "aaa" documents and 0 "bbb"
        IndexReader reader = IndexReader.open(dir);
        assertEquals("first docFreq", 100, reader.docFreq(searchTerm));
        assertEquals("first docFreq", 0, reader.docFreq(searchTerm2));
        assertTermDocsCount("first reader", reader, searchTerm, 100);
        assertTermDocsCount("first reader", reader, searchTerm2, 0);

        // add 100 documents with term : bbb
        writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), false);
        for (int i = 0; i < 100; i++)
        {
            addDoc(writer, searchTerm2.text());
        }

        // REQUEST OPTIMIZATION
        // This causes a new segment to become current for all subsequent
        // searchers. Because of this, deletions made via a previously open
        // reader, which would be applied to that reader's segment, are lost
        // for subsequent searchers/readers
        if(optimize)
          writer.optimize();
        writer.close();

        // The reader should not see the new data
        assertEquals("first docFreq", 100, reader.docFreq(searchTerm));
        assertEquals("first docFreq", 0, reader.docFreq(searchTerm2));
        assertTermDocsCount("first reader", reader, searchTerm, 100);
        assertTermDocsCount("first reader", reader, searchTerm2, 0);


        // DELETE DOCUMENTS CONTAINING TERM: aaa
        // NOTE: the reader was created when only "aaa" documents were in
        int deleted = 0;
        try {
            deleted = reader.deleteDocuments(searchTerm);
            fail("Delete allowed on an index reader with stale segment information");
        } catch (StaleReaderException e) {
            /* success */
        }

        // Re-open index reader and try again. This time it should see
        // the new data.
        reader.close();
        reader = IndexReader.open(dir);
        assertEquals("first docFreq", 100, reader.docFreq(searchTerm));
        assertEquals("first docFreq", 100, reader.docFreq(searchTerm2));
        assertTermDocsCount("first reader", reader, searchTerm, 100);
        assertTermDocsCount("first reader", reader, searchTerm2, 100);

        deleted = reader.deleteDocuments(searchTerm);
        assertEquals("deleted count", 100, deleted);
        assertEquals("deleted docFreq", 100, reader.docFreq(searchTerm));
        assertEquals("deleted docFreq", 100, reader.docFreq(searchTerm2));
        assertTermDocsCount("deleted termDocs", reader, searchTerm, 0);
        assertTermDocsCount("deleted termDocs", reader, searchTerm2, 100);
        reader.close();

        // CREATE A NEW READER and re-test
        reader = IndexReader.open(dir);
        assertEquals("deleted docFreq", 100, reader.docFreq(searchTerm));
        assertEquals("deleted docFreq", 100, reader.docFreq(searchTerm2));
        assertTermDocsCount("deleted termDocs", reader, searchTerm, 0);
        assertTermDocsCount("deleted termDocs", reader, searchTerm2, 100);
        reader.close();
    }

  private Directory getDirectory() throws IOException {
    return FSDirectory.getDirectory(new File(System.getProperty("tempDir"), "testIndex"));
  }

  public void testFilesOpenClose() throws IOException
    {
        // Create initial data set
        File dirFile = new File(System.getProperty("tempDir"), "testIndex");
        Directory dir = getDirectory();
        IndexWriter writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true);
        addDoc(writer, "test");
        writer.close();
        dir.close();

        // Try to erase the data - this ensures that the writer closed all files
        _TestUtil.rmDir(dirFile);
        dir = getDirectory();

        // Now create the data set again, just as before
        writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true);
        addDoc(writer, "test");
        writer.close();
        dir.close();

        // Now open existing directory and test that reader closes all files
        dir = getDirectory();
        IndexReader reader1 = IndexReader.open(dir);
        reader1.close();
        dir.close();

        // The following will fail if reader did not close
        // all files
        _TestUtil.rmDir(dirFile);
    }

    public void testLastModified() throws IOException {
      assertFalse(IndexReader.indexExists("there_is_no_such_index"));
      Directory dir = new MockRAMDirectory();
      assertFalse(IndexReader.indexExists(dir));
      IndexWriter writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true);
      addDocumentWithFields(writer);
      assertTrue(IndexReader.isLocked(dir));		// writer open, so dir is locked
      writer.close();
      assertTrue(IndexReader.indexExists(dir));
      IndexReader reader = IndexReader.open(dir);
      assertFalse(IndexReader.isLocked(dir));		// reader only, no lock
      long version = IndexReader.lastModified(dir);
      reader.close();
      // modify index and check version has been
      // incremented:
      writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true);
      addDocumentWithFields(writer);
      writer.close();
      reader = IndexReader.open(dir);
      assertTrue("old lastModified is " + version + "; new lastModified is " + IndexReader.lastModified(dir), version <= IndexReader.lastModified(dir));
      reader.close();
      dir.close();
    }

    public void testVersion() throws IOException {
      assertFalse(IndexReader.indexExists("there_is_no_such_index"));
      Directory dir = new MockRAMDirectory();
      assertFalse(IndexReader.indexExists(dir));
      IndexWriter writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true);
      addDocumentWithFields(writer);
      assertTrue(IndexReader.isLocked(dir));		// writer open, so dir is locked
      writer.close();
      assertTrue(IndexReader.indexExists(dir));
      IndexReader reader = IndexReader.open(dir);
      assertFalse(IndexReader.isLocked(dir));		// reader only, no lock
      long version = IndexReader.getCurrentVersion(dir);
      reader.close();
      // modify index and check version has been
      // incremented:
      writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true);
      addDocumentWithFields(writer);
      writer.close();
      reader = IndexReader.open(dir);
      assertTrue("old version is " + version + "; new version is " + IndexReader.getCurrentVersion(dir), version < IndexReader.getCurrentVersion(dir));
      reader.close();
      dir.close();
    }

    public void testLock() throws IOException {
      Directory dir = new MockRAMDirectory();
      IndexWriter writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true);
      addDocumentWithFields(writer);
      writer.close();
      writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), false);
      IndexReader reader = IndexReader.open(dir);
      try {
        reader.deleteDocument(0);
        fail("expected lock");
      } catch(IOException e) {
        // expected exception
      }
      IndexReader.unlock(dir);		// this should not be done in the real world! 
      reader.deleteDocument(0);
      reader.close();
      writer.close();
      dir.close();
    }

    public void testUndeleteAll() throws IOException {
      Directory dir = new MockRAMDirectory();
      IndexWriter writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true);
      addDocumentWithFields(writer);
      addDocumentWithFields(writer);
      writer.close();
      IndexReader reader = IndexReader.open(dir);
      reader.deleteDocument(0);
      reader.deleteDocument(1);
      reader.undeleteAll();
      reader.close();
      reader = IndexReader.open(dir);
      assertEquals(2, reader.numDocs());	// nothing has really been deleted thanks to undeleteAll()
      reader.close();
      dir.close();
    }

    public void testUndeleteAllAfterClose() throws IOException {
      Directory dir = new MockRAMDirectory();
      IndexWriter writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true);
      addDocumentWithFields(writer);
      addDocumentWithFields(writer);
      writer.close();
      IndexReader reader = IndexReader.open(dir);
      reader.deleteDocument(0);
      reader.deleteDocument(1);
      reader.close();
      reader = IndexReader.open(dir);
      reader.undeleteAll();
      assertEquals(2, reader.numDocs());	// nothing has really been deleted thanks to undeleteAll()
      reader.close();
      dir.close();
    }

    public void testUndeleteAllAfterCloseThenReopen() throws IOException {
      Directory dir = new MockRAMDirectory();
      IndexWriter writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true);
      addDocumentWithFields(writer);
      addDocumentWithFields(writer);
      writer.close();
      IndexReader reader = IndexReader.open(dir);
      reader.deleteDocument(0);
      reader.deleteDocument(1);
      reader.close();
      reader = IndexReader.open(dir);
      reader.undeleteAll();
      reader.close();
      reader = IndexReader.open(dir);
      assertEquals(2, reader.numDocs());	// nothing has really been deleted thanks to undeleteAll()
      reader.close();
      dir.close();
    }

    public void testDeleteReaderReaderConflictUnoptimized() throws IOException{
      deleteReaderReaderConflict(false);
    }
    
    public void testDeleteReaderReaderConflictOptimized() throws IOException{
      deleteReaderReaderConflict(true);
    }

    /**
     * Make sure if reader tries to commit but hits disk
     * full that reader remains consistent and usable.
     */
    public void testDiskFull() throws IOException {

      boolean debug = false;
      Term searchTerm = new Term("content", "aaa");
      int START_COUNT = 157;
      int END_COUNT = 144;
      
      // First build up a starting index:
      RAMDirectory startDir = new MockRAMDirectory();
      IndexWriter writer = new IndexWriter(startDir, new WhitespaceAnalyzer(), true);
      for(int i=0;i<157;i++) {
        Document d = new Document();
        d.add(new Field("id", Integer.toString(i), Field.Store.YES, Field.Index.UN_TOKENIZED));
        d.add(new Field("content", "aaa " + i, Field.Store.NO, Field.Index.TOKENIZED));
        writer.addDocument(d);
      }
      writer.close();

      long diskUsage = startDir.sizeInBytes();
      long diskFree = diskUsage+100;      

      IOException err = null;

      boolean done = false;

      // Iterate w/ ever increasing free disk space:
      while(!done) {
        MockRAMDirectory dir = new MockRAMDirectory(startDir);
        IndexReader reader = IndexReader.open(dir);

        // For each disk size, first try to commit against
        // dir that will hit random IOExceptions & disk
        // full; after, give it infinite disk space & turn
        // off random IOExceptions & retry w/ same reader:
        boolean success = false;

        for(int x=0;x<2;x++) {

          double rate = 0.05;
          double diskRatio = ((double) diskFree)/diskUsage;
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
            testName = "disk full during reader.close() @ " + thisDiskFree + " bytes";
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
              for(int i=0;i<13;i++) {
                reader.deleteDocument(docId);
                reader.setNorm(docId, "contents", (float) 2.0);
                docId += 12;
              }
            }
            reader.close();
            success = true;
            if (0 == x) {
              done = true;
            }
          } catch (IOException e) {
            if (debug) {
              System.out.println("  hit IOException: " + e);
            }
            err = e;
            if (1 == x) {
              e.printStackTrace();
              fail(testName + " hit IOException after disk space was freed up");
            }
          }

          // Whether we succeeded or failed, check that all
          // un-referenced files were in fact deleted (ie,
          // we did not create garbage).  Just create a
          // new IndexFileDeleter, have it delete
          // unreferenced files, then verify that in fact
          // no files were deleted:
          String[] startFiles = dir.list();
          SegmentInfos infos = new SegmentInfos();
          infos.read(dir);
          IndexFileDeleter d = new IndexFileDeleter(dir, new KeepOnlyLastCommitDeletionPolicy(), infos, null);
          String[] endFiles = dir.list();

          Arrays.sort(startFiles);
          Arrays.sort(endFiles);

          //for(int i=0;i<startFiles.length;i++) {
          //  System.out.println("  startFiles: " + i + ": " + startFiles[i]);
          //}

          if (!Arrays.equals(startFiles, endFiles)) {
            String successStr;
            if (success) {
              successStr = "success";
            } else {
              successStr = "IOException";
              err.printStackTrace();
            }
            fail("reader.close() failed to delete unreferenced files after " + successStr + " (" + diskFree + " bytes): before delete:\n    " + arrayToString(startFiles) + "\n  after delete:\n    " + arrayToString(endFiles));
          }

          // Finally, verify index is not corrupt, and, if
          // we succeeded, we see all docs changed, and if
          // we failed, we see either all docs or no docs
          // changed (transactional semantics):
          IndexReader newReader = null;
          try {
            newReader = IndexReader.open(dir);
          } catch (IOException e) {
            e.printStackTrace();
            fail(testName + ":exception when creating IndexReader after disk full during close: " + e);
          }
          /*
          int result = newReader.docFreq(searchTerm);
          if (success) {
            if (result != END_COUNT) {
              fail(testName + ": method did not throw exception but docFreq('aaa') is " + result + " instead of expected " + END_COUNT);
            }
          } else {
            // On hitting exception we still may have added
            // all docs:
            if (result != START_COUNT && result != END_COUNT) {
              err.printStackTrace();
              fail(testName + ": method did throw exception but docFreq('aaa') is " + result + " instead of expected " + START_COUNT + " or " + END_COUNT);
            }
          }
          */

          IndexSearcher searcher = new IndexSearcher(newReader);
          Hits hits = null;
          try {
            hits = searcher.search(new TermQuery(searchTerm));
          } catch (IOException e) {
            e.printStackTrace();
            fail(testName + ": exception when searching: " + e);
          }
          int result2 = hits.length();
          if (success) {
            if (result2 != END_COUNT) {
              fail(testName + ": method did not throw exception but hits.length for search on term 'aaa' is " + result2 + " instead of expected " + END_COUNT);
            }
          } else {
            // On hitting exception we still may have added
            // all docs:
            if (result2 != START_COUNT && result2 != END_COUNT) {
              err.printStackTrace();
              fail(testName + ": method did throw exception but hits.length for search on term 'aaa' is " + result2 + " instead of expected " + START_COUNT);
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

      startDir.close();
    }

    public void testDocsOutOfOrderJIRA140() throws IOException {
      Directory dir = new MockRAMDirectory();      
      IndexWriter writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true);
      for(int i=0;i<11;i++) {
        addDoc(writer, "aaa");
      }
      writer.close();
      IndexReader reader = IndexReader.open(dir);

      // Try to delete an invalid docId, yet, within range
      // of the final bits of the BitVector:

      boolean gotException = false;
      try {
        reader.deleteDocument(11);
      } catch (ArrayIndexOutOfBoundsException e) {
        gotException = true;
      }
      reader.close();

      writer = new IndexWriter(dir, new WhitespaceAnalyzer(), false);

      // We must add more docs to get a new segment written
      for(int i=0;i<11;i++) {
        addDoc(writer, "aaa");
      }

      // Without the fix for LUCENE-140 this call will
      // [incorrectly] hit a "docs out of order"
      // IllegalStateException because above out-of-bounds
      // deleteDocument corrupted the index:
      writer.optimize();

      if (!gotException) {
        fail("delete of out-of-bounds doc number failed to hit exception");
      }
      dir.close();
    }

    public void testExceptionReleaseWriteLockJIRA768() throws IOException {

      Directory dir = new MockRAMDirectory();      
      IndexWriter writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true);
      addDoc(writer, "aaa");
      writer.close();

      IndexReader reader = IndexReader.open(dir);
      try {
        reader.deleteDocument(1);
        fail("did not hit exception when deleting an invalid doc number");
      } catch (ArrayIndexOutOfBoundsException e) {
        // expected
      }
      reader.close();
      if (IndexReader.isLocked(dir)) {
        fail("write lock is still held after close");
      }

      reader = IndexReader.open(dir);
      try {
        reader.setNorm(1, "content", (float) 2.0);
        fail("did not hit exception when calling setNorm on an invalid doc number");
      } catch (ArrayIndexOutOfBoundsException e) {
        // expected
      }
      reader.close();
      if (IndexReader.isLocked(dir)) {
        fail("write lock is still held after close");
      }
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

    public void testOpenReaderAfterDelete() throws IOException {
      File dirFile = new File(System.getProperty("tempDir"),
                          "deletetest");
      Directory dir = FSDirectory.getDirectory(dirFile);
      try {
        IndexReader reader = IndexReader.open(dir);
        fail("expected FileNotFoundException");
      } catch (FileNotFoundException e) {
        // expected
      }

      dirFile.delete();

      // Make sure we still get a CorruptIndexException (not NPE):
      try {
        IndexReader reader = IndexReader.open(dir);
        fail("expected FileNotFoundException");
      } catch (FileNotFoundException e) {
        // expected
      }
    }

    private void deleteReaderReaderConflict(boolean optimize) throws IOException
    {
        Directory dir = getDirectory();

        Term searchTerm1 = new Term("content", "aaa");
        Term searchTerm2 = new Term("content", "bbb");
        Term searchTerm3 = new Term("content", "ccc");

        //  add 100 documents with term : aaa
        //  add 100 documents with term : bbb
        //  add 100 documents with term : ccc
        IndexWriter writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true);
        for (int i = 0; i < 100; i++)
        {
            addDoc(writer, searchTerm1.text());
            addDoc(writer, searchTerm2.text());
            addDoc(writer, searchTerm3.text());
        }
        if(optimize)
          writer.optimize();
        writer.close();

        // OPEN TWO READERS
        // Both readers get segment info as exists at this time
        IndexReader reader1 = IndexReader.open(dir);
        assertEquals("first opened", 100, reader1.docFreq(searchTerm1));
        assertEquals("first opened", 100, reader1.docFreq(searchTerm2));
        assertEquals("first opened", 100, reader1.docFreq(searchTerm3));
        assertTermDocsCount("first opened", reader1, searchTerm1, 100);
        assertTermDocsCount("first opened", reader1, searchTerm2, 100);
        assertTermDocsCount("first opened", reader1, searchTerm3, 100);

        IndexReader reader2 = IndexReader.open(dir);
        assertEquals("first opened", 100, reader2.docFreq(searchTerm1));
        assertEquals("first opened", 100, reader2.docFreq(searchTerm2));
        assertEquals("first opened", 100, reader2.docFreq(searchTerm3));
        assertTermDocsCount("first opened", reader2, searchTerm1, 100);
        assertTermDocsCount("first opened", reader2, searchTerm2, 100);
        assertTermDocsCount("first opened", reader2, searchTerm3, 100);

        // DELETE DOCS FROM READER 2 and CLOSE IT
        // delete documents containing term: aaa
        // when the reader is closed, the segment info is updated and
        // the first reader is now stale
        reader2.deleteDocuments(searchTerm1);
        assertEquals("after delete 1", 100, reader2.docFreq(searchTerm1));
        assertEquals("after delete 1", 100, reader2.docFreq(searchTerm2));
        assertEquals("after delete 1", 100, reader2.docFreq(searchTerm3));
        assertTermDocsCount("after delete 1", reader2, searchTerm1, 0);
        assertTermDocsCount("after delete 1", reader2, searchTerm2, 100);
        assertTermDocsCount("after delete 1", reader2, searchTerm3, 100);
        reader2.close();

        // Make sure reader 1 is unchanged since it was open earlier
        assertEquals("after delete 1", 100, reader1.docFreq(searchTerm1));
        assertEquals("after delete 1", 100, reader1.docFreq(searchTerm2));
        assertEquals("after delete 1", 100, reader1.docFreq(searchTerm3));
        assertTermDocsCount("after delete 1", reader1, searchTerm1, 100);
        assertTermDocsCount("after delete 1", reader1, searchTerm2, 100);
        assertTermDocsCount("after delete 1", reader1, searchTerm3, 100);


        // ATTEMPT TO DELETE FROM STALE READER
        // delete documents containing term: bbb
        try {
            reader1.deleteDocuments(searchTerm2);
            fail("Delete allowed from a stale index reader");
        } catch (IOException e) {
            /* success */
        }

        // RECREATE READER AND TRY AGAIN
        reader1.close();
        reader1 = IndexReader.open(dir);
        assertEquals("reopened", 100, reader1.docFreq(searchTerm1));
        assertEquals("reopened", 100, reader1.docFreq(searchTerm2));
        assertEquals("reopened", 100, reader1.docFreq(searchTerm3));
        assertTermDocsCount("reopened", reader1, searchTerm1, 0);
        assertTermDocsCount("reopened", reader1, searchTerm2, 100);
        assertTermDocsCount("reopened", reader1, searchTerm3, 100);

        reader1.deleteDocuments(searchTerm2);
        assertEquals("deleted 2", 100, reader1.docFreq(searchTerm1));
        assertEquals("deleted 2", 100, reader1.docFreq(searchTerm2));
        assertEquals("deleted 2", 100, reader1.docFreq(searchTerm3));
        assertTermDocsCount("deleted 2", reader1, searchTerm1, 0);
        assertTermDocsCount("deleted 2", reader1, searchTerm2, 0);
        assertTermDocsCount("deleted 2", reader1, searchTerm3, 100);
        reader1.close();

        // Open another reader to confirm that everything is deleted
        reader2 = IndexReader.open(dir);
        assertEquals("reopened 2", 100, reader2.docFreq(searchTerm1));
        assertEquals("reopened 2", 100, reader2.docFreq(searchTerm2));
        assertEquals("reopened 2", 100, reader2.docFreq(searchTerm3));
        assertTermDocsCount("reopened 2", reader2, searchTerm1, 0);
        assertTermDocsCount("reopened 2", reader2, searchTerm2, 0);
        assertTermDocsCount("reopened 2", reader2, searchTerm3, 100);
        reader2.close();

        dir.close();
    }


    private void addDocumentWithFields(IndexWriter writer) throws IOException
    {
        Document doc = new Document();
        doc.add(new Field("keyword","test1", Field.Store.YES, Field.Index.UN_TOKENIZED));
        doc.add(new Field("text","test1", Field.Store.YES, Field.Index.TOKENIZED));
        doc.add(new Field("unindexed","test1", Field.Store.YES, Field.Index.NO));
        doc.add(new Field("unstored","test1", Field.Store.NO, Field.Index.TOKENIZED));
        writer.addDocument(doc);
    }

    private void addDocumentWithDifferentFields(IndexWriter writer) throws IOException
    {
        Document doc = new Document();
        doc.add(new Field("keyword2","test1", Field.Store.YES, Field.Index.UN_TOKENIZED));
        doc.add(new Field("text2","test1", Field.Store.YES, Field.Index.TOKENIZED));
        doc.add(new Field("unindexed2","test1", Field.Store.YES, Field.Index.NO));
        doc.add(new Field("unstored2","test1", Field.Store.NO, Field.Index.TOKENIZED));
        writer.addDocument(doc);
    }

    private void addDocumentWithTermVectorFields(IndexWriter writer) throws IOException
    {
        Document doc = new Document();
        doc.add(new Field("tvnot","tvnot", Field.Store.YES, Field.Index.TOKENIZED, Field.TermVector.NO));
        doc.add(new Field("termvector","termvector", Field.Store.YES, Field.Index.TOKENIZED, Field.TermVector.YES));
        doc.add(new Field("tvoffset","tvoffset", Field.Store.YES, Field.Index.TOKENIZED, Field.TermVector.WITH_OFFSETS));
        doc.add(new Field("tvposition","tvposition", Field.Store.YES, Field.Index.TOKENIZED, Field.TermVector.WITH_POSITIONS));
        doc.add(new Field("tvpositionoffset","tvpositionoffset", Field.Store.YES, Field.Index.TOKENIZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
        
        writer.addDocument(doc);
    }
    
    private void addDoc(IndexWriter writer, String value) throws IOException
    {
        Document doc = new Document();
        doc.add(new Field("content", value, Field.Store.NO, Field.Index.TOKENIZED));
        writer.addDocument(doc);
    }
    private void rmDir(File dir) {
        File[] files = dir.listFiles();
        for (int i = 0; i < files.length; i++) {
            files[i].delete();
        }
        dir.delete();
    }

    
}
