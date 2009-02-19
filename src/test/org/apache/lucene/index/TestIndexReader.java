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


import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

import junit.framework.TestSuite;
import junit.textui.TestRunner;

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.document.SetBasedFieldSelector;
import org.apache.lucene.index.IndexReader.FieldOption;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.MockRAMDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

public class TestIndexReader extends LuceneTestCase
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
      IndexWriter writer = new IndexWriter(d, new StandardAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
      addDocumentWithFields(writer);
      writer.close();
      // set up reader:
      IndexReader reader = IndexReader.open(d);
      assertTrue(reader.isCurrent());
      // modify index by adding another document:
      writer = new IndexWriter(d, new StandardAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
      addDocumentWithFields(writer);
      writer.close();
      assertFalse(reader.isCurrent());
      // re-create index:
      writer = new IndexWriter(d, new StandardAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
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
        IndexWriter writer = new IndexWriter(d, new StandardAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
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
        writer = new IndexWriter(d, new StandardAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
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

  public void testTermVectors() throws Exception {
    RAMDirectory d = new MockRAMDirectory();
    // set up writer
    IndexWriter writer = new IndexWriter(d, new StandardAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
    // want to get some more segments here
    // new termvector fields
    for (int i = 0; i < 5 * writer.getMergeFactor(); i++) {
      Document doc = new Document();
        doc.add(new Field("tvnot","one two two three three three", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.NO));
        doc.add(new Field("termvector","one two two three three three", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.YES));
        doc.add(new Field("tvoffset","one two two three three three", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_OFFSETS));
        doc.add(new Field("tvposition","one two two three three three", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS));
        doc.add(new Field("tvpositionoffset","one two two three three three", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));

        writer.addDocument(doc);
    }
    writer.close();
    IndexReader reader = IndexReader.open(d);
    FieldSortedTermVectorMapper mapper = new FieldSortedTermVectorMapper(new TermVectorEntryFreqSortedComparator());
    reader.getTermFreqVector(0, mapper);
    Map map = mapper.getFieldToTerms();
    assertTrue("map is null and it shouldn't be", map != null);
    assertTrue("map Size: " + map.size() + " is not: " + 4, map.size() == 4);
    Set set = (Set) map.get("termvector");
    for (Iterator iterator = set.iterator(); iterator.hasNext();) {
      TermVectorEntry entry = (TermVectorEntry) iterator.next();
      assertTrue("entry is null and it shouldn't be", entry != null);
      System.out.println("Entry: " + entry);
    }




    
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
        writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
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
    
    public void testBinaryFields() throws IOException
    {
        Directory dir = new RAMDirectory();
        byte[] bin = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        
        IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.UNLIMITED);
        
        for (int i = 0; i < 10; i++) {
          addDoc(writer, "document number " + (i + 1));
          addDocumentWithFields(writer);
          addDocumentWithDifferentFields(writer);
          addDocumentWithTermVectorFields(writer);
        }
        writer.close();
        writer = new IndexWriter(dir, new WhitespaceAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
        Document doc = new Document();
        doc.add(new Field("bin1", bin, Field.Store.YES));
        doc.add(new Field("bin2", bin, Field.Store.COMPRESS));
        doc.add(new Field("junk", "junk text", Field.Store.NO, Field.Index.ANALYZED));
        writer.addDocument(doc);
        writer.close();
        IndexReader reader = IndexReader.open(dir);
        doc = reader.document(reader.maxDoc() - 1);
        Field[] fields = doc.getFields("bin1");
        assertNotNull(fields);
        assertEquals(1, fields.length);
        Field b1 = fields[0];
        assertTrue(b1.isBinary());
        byte[] data1 = b1.getBinaryValue();
        assertEquals(bin.length, b1.getBinaryLength());
        for (int i = 0; i < bin.length; i++) {
          assertEquals(bin[i], data1[i + b1.getBinaryOffset()]);
        }
        fields = doc.getFields("bin2");
        assertNotNull(fields);
        assertEquals(1, fields.length);
        b1 = fields[0];
        assertTrue(b1.isBinary());
        data1 = b1.getBinaryValue();
        assertEquals(bin.length, b1.getBinaryLength());
        for (int i = 0; i < bin.length; i++) {
          assertEquals(bin[i], data1[i + b1.getBinaryOffset()]);
        }
        Set lazyFields = new HashSet();
        lazyFields.add("bin1");
        FieldSelector sel = new SetBasedFieldSelector(new HashSet(), lazyFields);
        doc = reader.document(reader.maxDoc() - 1, sel);
        Fieldable[] fieldables = doc.getFieldables("bin1");
        assertNotNull(fieldables);
        assertEquals(1, fieldables.length);
        Fieldable fb1 = fieldables[0];
        assertTrue(fb1.isBinary());
        assertEquals(bin.length, fb1.getBinaryLength());
        data1 = fb1.getBinaryValue();
        assertEquals(bin.length, fb1.getBinaryLength());
        for (int i = 0; i < bin.length; i++) {
          assertEquals(bin[i], data1[i + fb1.getBinaryOffset()]);
        }
        reader.close();
        // force optimize


        writer = new IndexWriter(dir, new WhitespaceAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
        writer.optimize();
        writer.close();
        reader = IndexReader.open(dir);
        doc = reader.document(reader.maxDoc() - 1);
        fields = doc.getFields("bin1");
        assertNotNull(fields);
        assertEquals(1, fields.length);
        b1 = fields[0];
        assertTrue(b1.isBinary());
        data1 = b1.getBinaryValue();
        assertEquals(bin.length, b1.getBinaryLength());
        for (int i = 0; i < bin.length; i++) {
          assertEquals(bin[i], data1[i + b1.getBinaryOffset()]);
        }
        fields = doc.getFields("bin2");
        assertNotNull(fields);
        assertEquals(1, fields.length);
        b1 = fields[0];
        assertTrue(b1.isBinary());
        data1 = b1.getBinaryValue();
        assertEquals(bin.length, b1.getBinaryLength());
        for (int i = 0; i < bin.length; i++) {
          assertEquals(bin[i], data1[i + b1.getBinaryOffset()]);
        }
        reader.close();
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
        writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
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
        } catch (AlreadyClosedException e) {
          // expected
        }

        try {
          reader.setNorm(5, "aaa", 2.0f);
          fail("setNorm after close failed to throw IOException");
        } catch (AlreadyClosedException e) {
          // expected
        }

        try {
          reader.undeleteAll();
          fail("undeleteAll after close failed to throw IOException");
        } catch (AlreadyClosedException e) {
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
        writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
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
        IndexWriter writer;
        IndexReader reader;
        Term searchTerm = new Term("content", "aaa");

        //  add 1 documents with term : aaa
        writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
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
        writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
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
        IndexReader.open(fileDirName);
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
        IndexWriter writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
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
        writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
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
        IndexWriter writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
        addDoc(writer, "test");
        writer.close();
        dir.close();

        // Try to erase the data - this ensures that the writer closed all files
        _TestUtil.rmDir(dirFile);
        dir = getDirectory();

        // Now create the data set again, just as before
        writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
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
      final File fileDir = new File(System.getProperty("tempDir"), "testIndex");
      for(int i=0;i<2;i++) {
        try {
          final Directory dir;
          if (0 == i)
            dir = new MockRAMDirectory();
          else
            dir = getDirectory();
          assertFalse(IndexReader.indexExists(dir));
          IndexWriter writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
          addDocumentWithFields(writer);
          assertTrue(IndexReader.isLocked(dir));		// writer open, so dir is locked
          writer.close();
          assertTrue(IndexReader.indexExists(dir));
          IndexReader reader = IndexReader.open(dir);
          assertFalse(IndexReader.isLocked(dir));		// reader only, no lock
          long version = IndexReader.lastModified(dir);
          if (i == 1) {
            long version2 = IndexReader.lastModified(fileDir);
            assertEquals(version, version2);
          }
          reader.close();
          // modify index and check version has been
          // incremented:
          while(true) {
            try {
              Thread.sleep(1000);
              break;
            } catch (InterruptedException ie) {
              Thread.currentThread().interrupt();
            }
          }

          writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
          addDocumentWithFields(writer);
          writer.close();
          reader = IndexReader.open(dir);
          assertTrue("old lastModified is " + version + "; new lastModified is " + IndexReader.lastModified(dir), version <= IndexReader.lastModified(dir));
          reader.close();
          dir.close();
        } finally {
          if (i == 1)
            _TestUtil.rmDir(fileDir);
        }
      }
    }

    public void testVersion() throws IOException {
      assertFalse(IndexReader.indexExists("there_is_no_such_index"));
      Directory dir = new MockRAMDirectory();
      assertFalse(IndexReader.indexExists(dir));
      IndexWriter writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
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
      writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
      addDocumentWithFields(writer);
      writer.close();
      reader = IndexReader.open(dir);
      assertTrue("old version is " + version + "; new version is " + IndexReader.getCurrentVersion(dir), version < IndexReader.getCurrentVersion(dir));
      reader.close();
      dir.close();
    }

    public void testLock() throws IOException {
      Directory dir = new MockRAMDirectory();
      IndexWriter writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
      addDocumentWithFields(writer);
      writer.close();
      writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
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
      IndexWriter writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
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
      IndexWriter writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
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
      IndexWriter writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
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
      IndexWriter writer = new IndexWriter(startDir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
      for(int i=0;i<157;i++) {
        Document d = new Document();
        d.add(new Field("id", Integer.toString(i), Field.Store.YES, Field.Index.NOT_ANALYZED));
        d.add(new Field("content", "aaa " + i, Field.Store.NO, Field.Index.ANALYZED));
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

        // If IndexReader hits disk full, it can write to
        // the same files again.
        dir.setPreventDoubleWrite(false);

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
              e.printStackTrace(System.out);
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
          new IndexFileDeleter(dir, new KeepOnlyLastCommitDeletionPolicy(), infos, null, null);
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
          ScoreDoc[] hits = null;
          try {
            hits = searcher.search(new TermQuery(searchTerm), null, 1000).scoreDocs;
          } catch (IOException e) {
            e.printStackTrace();
            fail(testName + ": exception when searching: " + e);
          }
          int result2 = hits.length;
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
      IndexWriter writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
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

      writer = new IndexWriter(dir, new WhitespaceAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);

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
      IndexWriter writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
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
        IndexReader.open(dir);
        fail("expected FileNotFoundException");
      } catch (FileNotFoundException e) {
        // expected
      }

      dirFile.delete();

      // Make sure we still get a CorruptIndexException (not NPE):
      try {
        IndexReader.open(dir);
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
        IndexWriter writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
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
        doc.add(new Field("keyword","test1", Field.Store.YES, Field.Index.NOT_ANALYZED));
        doc.add(new Field("text","test1", Field.Store.YES, Field.Index.ANALYZED));
        doc.add(new Field("unindexed","test1", Field.Store.YES, Field.Index.NO));
        doc.add(new Field("unstored","test1", Field.Store.NO, Field.Index.ANALYZED));
        writer.addDocument(doc);
    }

    private void addDocumentWithDifferentFields(IndexWriter writer) throws IOException
    {
        Document doc = new Document();
        doc.add(new Field("keyword2","test1", Field.Store.YES, Field.Index.NOT_ANALYZED));
        doc.add(new Field("text2","test1", Field.Store.YES, Field.Index.ANALYZED));
        doc.add(new Field("unindexed2","test1", Field.Store.YES, Field.Index.NO));
        doc.add(new Field("unstored2","test1", Field.Store.NO, Field.Index.ANALYZED));
        writer.addDocument(doc);
    }

    private void addDocumentWithTermVectorFields(IndexWriter writer) throws IOException
    {
        Document doc = new Document();
        doc.add(new Field("tvnot","tvnot", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.NO));
        doc.add(new Field("termvector","termvector", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.YES));
        doc.add(new Field("tvoffset","tvoffset", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_OFFSETS));
        doc.add(new Field("tvposition","tvposition", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS));
        doc.add(new Field("tvpositionoffset","tvpositionoffset", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
        
        writer.addDocument(doc);
    }
    
    private void addDoc(IndexWriter writer, String value) throws IOException
    {
        Document doc = new Document();
        doc.add(new Field("content", value, Field.Store.NO, Field.Index.ANALYZED));
        writer.addDocument(doc);
    }
    private void rmDir(File dir) {
        File[] files = dir.listFiles();
        for (int i = 0; i < files.length; i++) {
            files[i].delete();
        }
        dir.delete();
    }

    public static void assertIndexEquals(IndexReader index1, IndexReader index2) throws IOException {
      assertEquals("IndexReaders have different values for numDocs.", index1.numDocs(), index2.numDocs());
      assertEquals("IndexReaders have different values for maxDoc.", index1.maxDoc(), index2.maxDoc());
      assertEquals("Only one IndexReader has deletions.", index1.hasDeletions(), index2.hasDeletions());
      assertEquals("Only one index is optimized.", index1.isOptimized(), index2.isOptimized());
      
      // check field names
      Collection fields1 = index1.getFieldNames(FieldOption.ALL);
      Collection fields2 = index1.getFieldNames(FieldOption.ALL);
      assertEquals("IndexReaders have different numbers of fields.", fields1.size(), fields2.size());
      Iterator it1 = fields1.iterator();
      Iterator it2 = fields1.iterator();
      while (it1.hasNext()) {
        assertEquals("Different field names.", (String) it1.next(), (String) it2.next());
      }
      
      // check norms
      it1 = fields1.iterator();
      while (it1.hasNext()) {
        String curField = (String) it1.next();
        byte[] norms1 = index1.norms(curField);
        byte[] norms2 = index2.norms(curField);
        assertEquals(norms1.length, norms2.length);
        for (int i = 0; i < norms1.length; i++) {
          assertEquals("Norm different for doc " + i + " and field '" + curField + "'.", norms1[i], norms2[i]);
        }      
      }
      
      // check deletions
      for (int i = 0; i < index1.maxDoc(); i++) {
        assertEquals("Doc " + i + " only deleted in one index.", index1.isDeleted(i), index2.isDeleted(i));
      }
      
      // check stored fields
      for (int i = 0; i < index1.maxDoc(); i++) {
        if (!index1.isDeleted(i)) {
          Document doc1 = index1.document(i);
          Document doc2 = index2.document(i);
          fields1 = doc1.getFields();
          fields2 = doc2.getFields();
          assertEquals("Different numbers of fields for doc " + i + ".", fields1.size(), fields2.size());
          it1 = fields1.iterator();
          it2 = fields2.iterator();
          while (it1.hasNext()) {
            Field curField1 = (Field) it1.next();
            Field curField2 = (Field) it2.next();
            assertEquals("Different fields names for doc " + i + ".", curField1.name(), curField2.name());
            assertEquals("Different field values for doc " + i + ".", curField1.stringValue(), curField2.stringValue());
          }          
        }
      }
      
      // check dictionary and posting lists
      TermEnum enum1 = index1.terms();
      TermEnum enum2 = index2.terms();
      TermPositions tp1 = index1.termPositions();
      TermPositions tp2 = index2.termPositions();
      while(enum1.next()) {
        assertTrue(enum2.next());
        assertEquals("Different term in dictionary.", enum1.term(), enum2.term());
        tp1.seek(enum1.term());
        tp2.seek(enum1.term());
        while(tp1.next()) {
          assertTrue(tp2.next());
          assertEquals("Different doc id in postinglist of term " + enum1.term() + ".", tp1.doc(), tp2.doc());
          assertEquals("Different term frequence in postinglist of term " + enum1.term() + ".", tp1.freq(), tp2.freq());
          for (int i = 0; i < tp1.freq(); i++) {
            assertEquals("Different positions in postinglist of term " + enum1.term() + ".", tp1.nextPosition(), tp2.nextPosition());
          }
        }
      }
    }

    public void testGetIndexCommit() throws IOException {

      RAMDirectory d = new MockRAMDirectory();

      // set up writer
      IndexWriter writer = new IndexWriter(d, new StandardAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
      writer.setMaxBufferedDocs(2);
      for(int i=0;i<27;i++)
        addDocumentWithFields(writer);
      writer.close();

      SegmentInfos sis = new SegmentInfos();
      sis.read(d);
      IndexReader r = IndexReader.open(d);
      IndexCommit c = r.getIndexCommit();

      assertEquals(sis.getCurrentSegmentFileName(), c.getSegmentsFileName());

      assertTrue(c.equals(r.getIndexCommit()));

      // Change the index
      writer = new IndexWriter(d, new StandardAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
      writer.setMaxBufferedDocs(2);
      for(int i=0;i<7;i++)
        addDocumentWithFields(writer);
      writer.close();

      IndexReader r2 = r.reopen();
      assertFalse(c.equals(r2.getIndexCommit()));
      assertFalse(r2.getIndexCommit().isOptimized());
      r2.close();

      writer = new IndexWriter(d, new StandardAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
      writer.optimize();
      writer.close();

      r2 = r.reopen();
      assertTrue(r2.getIndexCommit().isOptimized());

      r.close();
      r2.close();
      d.close();
    }      

    public void testReadOnly() throws Throwable {
      RAMDirectory d = new MockRAMDirectory();
      IndexWriter writer = new IndexWriter(d, new StandardAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
      addDocumentWithFields(writer);
      writer.commit();
      addDocumentWithFields(writer);
      writer.close();

      IndexReader r = IndexReader.open(d, true);
      try {
        r.deleteDocument(0);
        fail();
      } catch (UnsupportedOperationException uoe) {
        // expected
      }
      
      writer = new IndexWriter(d, new StandardAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
      addDocumentWithFields(writer);
      writer.close();

      // Make sure reopen is still readonly:
      IndexReader r2 = r.reopen();
      r.close();

      assertFalse(r == r2);

      try {
        r2.deleteDocument(0);
        fail();
      } catch (UnsupportedOperationException uoe) {
        // expected
      }

      writer = new IndexWriter(d, new StandardAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
      writer.optimize();
      writer.close();

      // Make sure reopen to a single segment is still readonly:
      IndexReader r3 = r2.reopen();
      r2.close();
      
      assertFalse(r == r2);

      try {
        r3.deleteDocument(0);
        fail();
      } catch (UnsupportedOperationException uoe) {
        // expected
      }

      // Make sure write lock isn't held
      writer = new IndexWriter(d, new StandardAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
      writer.close();

      r3.close();
    }

  // LUCENE-1474
  public void testIndexReader() throws Exception {
    Directory dir = new RAMDirectory();
    IndexWriter writer = new IndexWriter(dir, new StandardAnalyzer(),
                                         IndexWriter.MaxFieldLength.UNLIMITED);
    writer.addDocument(createDocument("a"));
    writer.addDocument(createDocument("b"));
    writer.addDocument(createDocument("c"));
    writer.close();
    IndexReader reader = IndexReader.open(dir);
    reader.deleteDocuments(new Term("id", "a"));
    reader.flush();
    reader.deleteDocuments(new Term("id", "b"));
    reader.close();
    IndexReader.open(dir).close();
  }

  private Document createDocument(String id) {
    Document doc = new Document();
    doc.add(new Field("id", id, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS));
    return doc;
  }

  public void testFalseDirectoryAlreadyClosed() throws Throwable {

    String tempDir = System.getProperty("java.io.tmpdir");
    if (tempDir == null)
      throw new RuntimeException("java.io.tmpdir undefined");
    File indexDir = new File(tempDir, "lucenetestdiralreadyclosed");

    try {
      FSDirectory dir = FSDirectory.getDirectory(indexDir);
      IndexWriter w = new IndexWriter(indexDir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
      w.setUseCompoundFile(false);
      Document doc = new Document();
      w.addDocument(doc);
      w.close();
      assertTrue(new File(indexDir, "_0.fnm").delete());

      try {
        IndexReader.open(indexDir);
        fail("did not hit expected exception");
      } catch (AlreadyClosedException ace) {
        fail("should not have hit AlreadyClosedException");
      } catch (FileNotFoundException ioe) {
        // expected
      }

      // Make sure we really did close the dir inside IndexReader.open
      dir.close();

      try {
        dir.fileExists("hi");
        fail("did not hit expected exception");
      } catch (AlreadyClosedException ace) {
        // expected
      }
    } finally {
      _TestUtil.rmDir(indexDir);
    }
  }
}
