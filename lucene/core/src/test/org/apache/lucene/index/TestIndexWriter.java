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
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.simpletext.SimpleTextCodec;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.BaseDirectoryWrapper;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.store.NoLockFactory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.store.SimpleFSLockFactory;
import org.apache.lucene.store.SingleInstanceLockFactory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.ThreadInterruptedException;
import org.apache.lucene.util._TestUtil;
import org.apache.lucene.util.packed.PackedInts;
import org.junit.Test;

public class TestIndexWriter extends LuceneTestCase {

    private static final FieldType storedTextType = new FieldType(TextField.TYPE_NOT_STORED);
    public void testDocCount() throws IOException {
        Directory dir = newDirectory();

        IndexWriter writer = null;
        IndexReader reader = null;
        int i;

        long savedWriteLockTimeout = IndexWriterConfig.getDefaultWriteLockTimeout();
        try {
          IndexWriterConfig.setDefaultWriteLockTimeout(2000);
          assertEquals(2000, IndexWriterConfig.getDefaultWriteLockTimeout());
          writer  = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random())));
        } finally {
          IndexWriterConfig.setDefaultWriteLockTimeout(savedWriteLockTimeout);
        }

        // add 100 documents
        for (i = 0; i < 100; i++) {
            addDocWithIndex(writer,i);
        }
        assertEquals(100, writer.maxDoc());
        writer.close();

        // delete 40 documents
        writer = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).setMergePolicy(NoMergePolicy.NO_COMPOUND_FILES));
        for (i = 0; i < 40; i++) {
            writer.deleteDocuments(new Term("id", ""+i));
        }
        writer.close();

        reader = DirectoryReader.open(dir);
        assertEquals(60, reader.numDocs());
        reader.close();

        // merge the index down and check that the new doc count is correct
        writer = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random())));
        assertEquals(60, writer.numDocs());
        writer.forceMerge(1);
        assertEquals(60, writer.maxDoc());
        assertEquals(60, writer.numDocs());
        writer.close();

        // check that the index reader gives the same numbers.
        reader = DirectoryReader.open(dir);
        assertEquals(60, reader.maxDoc());
        assertEquals(60, reader.numDocs());
        reader.close();

        // make sure opening a new index for create over
        // this existing one works correctly:
        writer = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random())).setOpenMode(OpenMode.CREATE));
        assertEquals(0, writer.maxDoc());
        assertEquals(0, writer.numDocs());
        writer.close();
        dir.close();
    }

    static void addDoc(IndexWriter writer) throws IOException
    {
        Document doc = new Document();
        doc.add(newTextField("content", "aaa", Field.Store.NO));
        writer.addDocument(doc);
    }

    static void addDocWithIndex(IndexWriter writer, int index) throws IOException
    {
        Document doc = new Document();
        doc.add(newField("content", "aaa " + index, storedTextType));
        doc.add(newField("id", "" + index, storedTextType));
        writer.addDocument(doc);
    }



    public static void assertNoUnreferencedFiles(Directory dir, String message) throws IOException {
      String[] startFiles = dir.listAll();
      new IndexWriter(dir, new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()))).rollback();
      String[] endFiles = dir.listAll();

      Arrays.sort(startFiles);
      Arrays.sort(endFiles);

      if (!Arrays.equals(startFiles, endFiles)) {
        fail(message + ": before delete:\n    " + arrayToString(startFiles) + "\n  after delete:\n    " + arrayToString(endFiles));
      }
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
      Directory dir = newDirectory();

      // add one document & close writer
      IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random())));
      addDoc(writer);
      writer.close();

      // now open reader:
      IndexReader reader = DirectoryReader.open(dir);
      assertEquals("should be one document", reader.numDocs(), 1);

      // now open index for create:
      writer = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random())).setOpenMode(OpenMode.CREATE));
      assertEquals("should be zero documents", writer.maxDoc(), 0);
      addDoc(writer);
      writer.close();

      assertEquals("should be one document", reader.numDocs(), 1);
      IndexReader reader2 = DirectoryReader.open(dir);
      assertEquals("should be one document", reader2.numDocs(), 1);
      reader.close();
      reader2.close();

      dir.close();
    }

    public void testChangesAfterClose() throws IOException {
        Directory dir = newDirectory();

        IndexWriter writer = null;

        writer  = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random())));
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



    public void testIndexNoDocuments() throws IOException {
      Directory dir = newDirectory();
      IndexWriter writer  = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random())));
      writer.commit();
      writer.close();

      IndexReader reader = DirectoryReader.open(dir);
      assertEquals(0, reader.maxDoc());
      assertEquals(0, reader.numDocs());
      reader.close();

      writer  = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random())).setOpenMode(OpenMode.APPEND));
      writer.commit();
      writer.close();

      reader = DirectoryReader.open(dir);
      assertEquals(0, reader.maxDoc());
      assertEquals(0, reader.numDocs());
      reader.close();
      dir.close();
    }

    public void testManyFields() throws IOException {
      Directory dir = newDirectory();
      IndexWriter writer  = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random())).setMaxBufferedDocs(10));
      for(int j=0;j<100;j++) {
        Document doc = new Document();
        doc.add(newField("a"+j, "aaa" + j, storedTextType));
        doc.add(newField("b"+j, "aaa" + j, storedTextType));
        doc.add(newField("c"+j, "aaa" + j, storedTextType));
        doc.add(newField("d"+j, "aaa", storedTextType));
        doc.add(newField("e"+j, "aaa", storedTextType));
        doc.add(newField("f"+j, "aaa", storedTextType));
        writer.addDocument(doc);
      }
      writer.close();

      IndexReader reader = DirectoryReader.open(dir);
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
      Directory dir = newDirectory();
      IndexWriter writer  = new IndexWriter(
          dir,
          newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random())).
              setRAMBufferSizeMB(0.000001).
              setMergePolicy(newLogMergePolicy(10))
      );
      int lastNumFile = dir.listAll().length;
      for(int j=0;j<9;j++) {
        Document doc = new Document();
        doc.add(newField("field", "aaa" + j, storedTextType));
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

    // Make sure it's OK to change RAM buffer size and
    // maxBufferedDocs in a write session
    public void testChangingRAMBuffer() throws IOException {
      Directory dir = newDirectory();      
      IndexWriter writer  = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));
      writer.getConfig().setMaxBufferedDocs(10);
      writer.getConfig().setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);

      int lastFlushCount = -1;
      for(int j=1;j<52;j++) {
        Document doc = new Document();
        doc.add(new Field("field", "aaa" + j, storedTextType));
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
          writer.getConfig().setRAMBufferSizeMB(0.000001);
          writer.getConfig().setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH);
        } else if (j < 20) {
          assertTrue(flushCount > lastFlushCount);
          lastFlushCount = flushCount;
        } else if (20 == j) {
          writer.getConfig().setRAMBufferSizeMB(16);
          writer.getConfig().setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH);
          lastFlushCount = flushCount;
        } else if (j < 30) {
          assertEquals(flushCount, lastFlushCount);
        } else if (30 == j) {
          writer.getConfig().setRAMBufferSizeMB(0.000001);
          writer.getConfig().setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH);
        } else if (j < 40) {
          assertTrue(flushCount> lastFlushCount);
          lastFlushCount = flushCount;
        } else if (40 == j) {
          writer.getConfig().setMaxBufferedDocs(10);
          writer.getConfig().setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
          lastFlushCount = flushCount;
        } else if (j < 50) {
          assertEquals(flushCount, lastFlushCount);
          writer.getConfig().setMaxBufferedDocs(10);
          writer.getConfig().setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
        } else if (50 == j) {
          assertTrue(flushCount > lastFlushCount);
        }
      }
      writer.close();
      dir.close();
    }

    public void testChangingRAMBuffer2() throws IOException {
      Directory dir = newDirectory();      
      IndexWriter writer  = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));
      writer.getConfig().setMaxBufferedDocs(10);
      writer.getConfig().setMaxBufferedDeleteTerms(10);
      writer.getConfig().setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);

      for(int j=1;j<52;j++) {
        Document doc = new Document();
        doc.add(new Field("field", "aaa" + j, storedTextType));
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
          assertTrue("" + j, flushCount > lastFlushCount);
          lastFlushCount = flushCount;
          writer.getConfig().setRAMBufferSizeMB(0.000001);
          writer.getConfig().setMaxBufferedDeleteTerms(1);
        } else if (j < 20) {
          assertTrue(flushCount > lastFlushCount);
          lastFlushCount = flushCount;
        } else if (20 == j) {
          writer.getConfig().setRAMBufferSizeMB(16);
          writer.getConfig().setMaxBufferedDeleteTerms(IndexWriterConfig.DISABLE_AUTO_FLUSH);
          lastFlushCount = flushCount;
        } else if (j < 30) {
          assertEquals(flushCount, lastFlushCount);
        } else if (30 == j) {
          writer.getConfig().setRAMBufferSizeMB(0.000001);
          writer.getConfig().setMaxBufferedDeleteTerms(IndexWriterConfig.DISABLE_AUTO_FLUSH);
          writer.getConfig().setMaxBufferedDeleteTerms(1);
        } else if (j < 40) {
          assertTrue(flushCount> lastFlushCount);
          lastFlushCount = flushCount;
        } else if (40 == j) {
          writer.getConfig().setMaxBufferedDeleteTerms(10);
          writer.getConfig().setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
          lastFlushCount = flushCount;
        } else if (j < 50) {
          assertEquals(flushCount, lastFlushCount);
          writer.getConfig().setMaxBufferedDeleteTerms(10);
          writer.getConfig().setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
        } else if (50 == j) {
          assertTrue(flushCount > lastFlushCount);
        }
      }
      writer.close();
      dir.close();
    }

    public void testDiverseDocs() throws IOException {
      Directory dir = newDirectory();
      IndexWriter writer  = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random())).setRAMBufferSizeMB(0.5));
      int n = atLeast(1);
      for(int i=0;i<n;i++) {
        // First, docs where every term is unique (heavy on
        // Posting instances)
        for(int j=0;j<100;j++) {
          Document doc = new Document();
          for(int k=0;k<100;k++) {
            doc.add(newField("field", Integer.toString(random().nextInt()), storedTextType));
          }
          writer.addDocument(doc);
        }

        // Next, many single term docs where only one term
        // occurs (heavy on byte blocks)
        for(int j=0;j<100;j++) {
          Document doc = new Document();
          doc.add(newField("field", "aaa aaa aaa aaa aaa aaa aaa aaa aaa aaa", storedTextType));
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
          doc.add(newField("field", longTerm, storedTextType));
          writer.addDocument(doc);
        }
      }
      writer.close();

      IndexReader reader = DirectoryReader.open(dir);
      IndexSearcher searcher = newSearcher(reader);
      int totalHits = searcher.search(new TermQuery(new Term("field", "aaa")), null, 1).totalHits;
      assertEquals(n*100, totalHits);
      reader.close();

      dir.close();
    }

    public void testEnablingNorms() throws IOException {
      Directory dir = newDirectory();
      IndexWriter writer  = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random())).setMaxBufferedDocs(10));
      // Enable norms for only 1 doc, pre flush
      FieldType customType = new FieldType(TextField.TYPE_STORED);
      customType.setOmitNorms(true);
      for(int j=0;j<10;j++) {
        Document doc = new Document();
        Field f = null;
        if (j != 8) {
          f = newField("field", "aaa", customType);
        }
        else {
          f = newField("field", "aaa", storedTextType);
        }
        doc.add(f);
        writer.addDocument(doc);
      }
      writer.close();

      Term searchTerm = new Term("field", "aaa");

      IndexReader reader = DirectoryReader.open(dir);
      IndexSearcher searcher = newSearcher(reader);
      ScoreDoc[] hits = searcher.search(new TermQuery(searchTerm), null, 1000).scoreDocs;
      assertEquals(10, hits.length);
      reader.close();

      writer = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random()))
        .setOpenMode(OpenMode.CREATE).setMaxBufferedDocs(10));
      // Enable norms for only 1 doc, post flush
      for(int j=0;j<27;j++) {
        Document doc = new Document();
        Field f = null;
        if (j != 26) {
          f = newField("field", "aaa", customType);
        }
        else {
          f = newField("field", "aaa", storedTextType);
        }
        doc.add(f);
        writer.addDocument(doc);
      }
      writer.close();
      reader = DirectoryReader.open(dir);
      searcher = newSearcher(reader);
      hits = searcher.search(new TermQuery(searchTerm), null, 1000).scoreDocs;
      assertEquals(27, hits.length);
      reader.close();

      reader = DirectoryReader.open(dir);
      reader.close();

      dir.close();
    }

    public void testHighFreqTerm() throws IOException {
      Directory dir = newDirectory();
      IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
          TEST_VERSION_CURRENT, new MockAnalyzer(random())).setRAMBufferSizeMB(0.01));
      // Massive doc that has 128 K a's
      StringBuilder b = new StringBuilder(1024*1024);
      for(int i=0;i<4096;i++) {
        b.append(" a a a a a a a a");
        b.append(" a a a a a a a a");
        b.append(" a a a a a a a a");
        b.append(" a a a a a a a a");
      }
      Document doc = new Document();
      FieldType customType = new FieldType(TextField.TYPE_STORED);
      customType.setStoreTermVectors(true);
      customType.setStoreTermVectorPositions(true);
      customType.setStoreTermVectorOffsets(true);
      doc.add(newField("field", b.toString(), customType));
      writer.addDocument(doc);
      writer.close();

      IndexReader reader = DirectoryReader.open(dir);
      assertEquals(1, reader.maxDoc());
      assertEquals(1, reader.numDocs());
      Term t = new Term("field", "a");
      assertEquals(1, reader.docFreq(t));
      DocsEnum td = _TestUtil.docs(random(), reader,
                                   "field",
                                   new BytesRef("a"),
                                   MultiFields.getLiveDocs(reader),
                                   null,
                                   DocsEnum.FLAG_FREQS);
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

      final class MyRAMDirectory extends MockDirectoryWrapper {
        private LockFactory myLockFactory;
        MyRAMDirectory(Directory delegate) {
          super(random(), delegate);
          lockFactory = null;
          myLockFactory = new SingleInstanceLockFactory();
        }
        @Override
        public Lock makeLock(String name) {
          return myLockFactory.makeLock(name);
        }
      }

      Directory dir = new MyRAMDirectory(new RAMDirectory());
      IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random())));
      for (int i = 0; i < 100; i++) {
        addDoc(writer);
      }
      writer.close();
      Term searchTerm = new Term("content", "aaa");
      IndexReader reader = DirectoryReader.open(dir);
      IndexSearcher searcher = newSearcher(reader);
      ScoreDoc[] hits = searcher.search(new TermQuery(searchTerm), null, 1000).scoreDocs;
      assertEquals("did not get right number of hits", 100, hits.length);
      reader.close();

      writer = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random()))
        .setOpenMode(OpenMode.CREATE));
      writer.close();
      dir.close();
    }

    public void testFlushWithNoMerging() throws IOException {
      Directory dir = newDirectory();
      IndexWriter writer = new IndexWriter(
          dir,
          newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).
              setMaxBufferedDocs(2).
              setMergePolicy(newLogMergePolicy(10))
      );
      Document doc = new Document();
      FieldType customType = new FieldType(TextField.TYPE_STORED);
      customType.setStoreTermVectors(true);
      customType.setStoreTermVectorPositions(true);
      customType.setStoreTermVectorOffsets(true);
      doc.add(newField("field", "aaa", customType));
      for(int i=0;i<19;i++)
        writer.addDocument(doc);
      writer.flush(false, true);
      writer.close();
      SegmentInfos sis = new SegmentInfos();
      sis.read(dir);
      // Since we flushed w/o allowing merging we should now
      // have 10 segments
      assertEquals(10, sis.size());
      dir.close();
    }

    // Make sure we can flush segment w/ norms, then add
    // empty doc (no norms) and flush
    public void testEmptyDocAfterFlushingRealDoc() throws IOException {
      Directory dir = newDirectory();
      IndexWriter writer  = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random())));
      Document doc = new Document();
      FieldType customType = new FieldType(TextField.TYPE_STORED);
      customType.setStoreTermVectors(true);
      customType.setStoreTermVectorPositions(true);
      customType.setStoreTermVectorOffsets(true);
      doc.add(newField("field", "aaa", customType));
      writer.addDocument(doc);
      writer.commit();
      if (VERBOSE) {
        System.out.println("\nTEST: now add empty doc");
      }
      writer.addDocument(new Document());
      writer.close();
      IndexReader reader = DirectoryReader.open(dir);
      assertEquals(2, reader.numDocs());
      reader.close();
      dir.close();
    }



  /**
   * Test that no NullPointerException will be raised,
   * when adding one document with a single, empty field
   * and term vectors enabled.
   */
  public void testBadSegment() throws IOException {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random())));

    Document document = new Document();
    FieldType customType = new FieldType(TextField.TYPE_NOT_STORED);
    customType.setStoreTermVectors(true);
    document.add(newField("tvtest", "", customType));
    iw.addDocument(document);
    iw.close();
    dir.close();
  }

  // LUCENE-1036
  public void testMaxThreadPriority() throws IOException {
    int pri = Thread.currentThread().getPriority();
    try {
      Directory dir = newDirectory();
      IndexWriterConfig conf = newIndexWriterConfig(
          TEST_VERSION_CURRENT, new MockAnalyzer(random()))
        .setMaxBufferedDocs(2).setMergePolicy(newLogMergePolicy());
      ((LogMergePolicy) conf.getMergePolicy()).setMergeFactor(2);
      IndexWriter iw = new IndexWriter(dir, conf);
      Document document = new Document();
      FieldType customType = new FieldType(TextField.TYPE_NOT_STORED);
      customType.setStoreTermVectors(true);
      document.add(newField("tvtest", "a b c", customType));
      Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
      for(int i=0;i<4;i++)
        iw.addDocument(document);
      iw.close();
      dir.close();
    } finally {
      Thread.currentThread().setPriority(pri);
    }
  }

  public void testVariableSchema() throws Exception {
    Directory dir = newDirectory();
    for(int i=0;i<20;i++) {
      if (VERBOSE) {
        System.out.println("TEST: iter=" + i);
      }
      IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random())).setMaxBufferedDocs(2).setMergePolicy(newLogMergePolicy()));
      //LogMergePolicy lmp = (LogMergePolicy) writer.getConfig().getMergePolicy();
      //lmp.setMergeFactor(2);
      //lmp.setUseCompoundFile(false);
      Document doc = new Document();
      String contents = "aa bb cc dd ee ff gg hh ii jj kk";

      FieldType customType = new FieldType(TextField.TYPE_STORED);
      FieldType type = null;
      if (i == 7) {
        // Add empty docs here
        doc.add(newTextField("content3", "", Field.Store.NO));
      } else {
        if (i%2 == 0) {
          doc.add(newField("content4", contents, customType));
          type = customType;
        } else
          type = TextField.TYPE_NOT_STORED; 
        doc.add(newTextField("content1", contents, Field.Store.NO));
        doc.add(newField("content3", "", customType));
        doc.add(newField("content5", "", type));
      }

      for(int j=0;j<4;j++)
        writer.addDocument(doc);

      writer.close();

      if (0 == i % 4) {
        writer = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random())));
        //LogMergePolicy lmp2 = (LogMergePolicy) writer.getConfig().getMergePolicy();
        //lmp2.setUseCompoundFile(false);
        writer.forceMerge(1);
        writer.close();
      }
    }
    dir.close();
  }

  // LUCENE-1084: test unlimited field length
  public void testUnlimitedMaxFieldLength() throws IOException {
    Directory dir = newDirectory();

    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random())));

    Document doc = new Document();
    StringBuilder b = new StringBuilder();
    for(int i=0;i<10000;i++)
      b.append(" a");
    b.append(" x");
    doc.add(newTextField("field", b.toString(), Field.Store.NO));
    writer.addDocument(doc);
    writer.close();

    IndexReader reader = DirectoryReader.open(dir);
    Term t = new Term("field", "x");
    assertEquals(1, reader.docFreq(t));
    reader.close();
    dir.close();
  }



  // LUCENE-1179
  public void testEmptyFieldName() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    Document doc = new Document();
    doc.add(newTextField("", "a b c", Field.Store.NO));
    writer.addDocument(doc);
    writer.close();
    dir.close();
  }
  
  public void testEmptyFieldNameTerms() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    Document doc = new Document();
    doc.add(newTextField("", "a b c", Field.Store.NO));
    writer.addDocument(doc);  
    writer.close();
    DirectoryReader reader = DirectoryReader.open(dir);
    AtomicReader subreader = getOnlySegmentReader(reader);
    TermsEnum te = subreader.fields().terms("").iterator(null);
    assertEquals(new BytesRef("a"), te.next());
    assertEquals(new BytesRef("b"), te.next());
    assertEquals(new BytesRef("c"), te.next());
    assertNull(te.next());
    reader.close();
    dir.close();
  }
  
  public void testEmptyFieldNameWithEmptyTerm() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    Document doc = new Document();
    doc.add(newStringField("", "", Field.Store.NO));
    doc.add(newStringField("", "a", Field.Store.NO));
    doc.add(newStringField("", "b", Field.Store.NO));
    doc.add(newStringField("", "c", Field.Store.NO));
    writer.addDocument(doc);  
    writer.close();
    DirectoryReader reader = DirectoryReader.open(dir);
    AtomicReader subreader = getOnlySegmentReader(reader);
    TermsEnum te = subreader.fields().terms("").iterator(null);
    assertEquals(new BytesRef(""), te.next());
    assertEquals(new BytesRef("a"), te.next());
    assertEquals(new BytesRef("b"), te.next());
    assertEquals(new BytesRef("c"), te.next());
    assertNull(te.next());
    reader.close();
    dir.close();
  }



  private static final class MockIndexWriter extends IndexWriter {

    public MockIndexWriter(Directory dir, IndexWriterConfig conf) throws IOException {
      super(dir, conf);
    }

    boolean afterWasCalled;
    boolean beforeWasCalled;

    @Override
    public void doAfterFlush() {
      afterWasCalled = true;
    }

    @Override
    protected void doBeforeFlush() {
      beforeWasCalled = true;
    }
  }


  // LUCENE-1222
  public void testDoBeforeAfterFlush() throws IOException {
    Directory dir = newDirectory();
    MockIndexWriter w = new MockIndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    Document doc = new Document();
    FieldType customType = new FieldType(TextField.TYPE_STORED);
    doc.add(newField("field", "a field", customType));
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

    IndexReader ir = DirectoryReader.open(dir);
    assertEquals(0, ir.numDocs());
    ir.close();

    dir.close();
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

    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    Document doc = new Document();
    doc.add(new TextField("field", tokens));
    try {
      w.addDocument(doc);
      fail("did not hit expected exception");
    } catch (IllegalArgumentException iea) {
      // expected
    }
    w.close();
    dir.close();
  }

  // LUCENE-2529
  public void testPositionIncrementGapEmptyField() throws Exception {
    Directory dir = newDirectory();
    MockAnalyzer analyzer = new MockAnalyzer(random());
    analyzer.setPositionIncrementGap( 100 );
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, analyzer));
    Document doc = new Document();
    FieldType customType = new FieldType(TextField.TYPE_NOT_STORED);
    customType.setStoreTermVectors(true);
    customType.setStoreTermVectorPositions(true);
    Field f = newField("field", "", customType);
    Field f2 = newField("field", "crunch man", customType);
    doc.add(f);
    doc.add(f2);
    w.addDocument(doc);
    w.close();

    IndexReader r = DirectoryReader.open(dir);
    Terms tpv = r.getTermVectors(0).terms("field");
    TermsEnum termsEnum = tpv.iterator(null);
    assertNotNull(termsEnum.next());
    DocsAndPositionsEnum dpEnum = termsEnum.docsAndPositions(null, null);
    assertNotNull(dpEnum);
    assertTrue(dpEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(1, dpEnum.freq());
    assertEquals(100, dpEnum.nextPosition());

    assertNotNull(termsEnum.next());
    dpEnum = termsEnum.docsAndPositions(null, dpEnum);
    assertNotNull(dpEnum);
    assertTrue(dpEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(1, dpEnum.freq());
    assertEquals(101, dpEnum.nextPosition());
    assertNull(termsEnum.next());

    r.close();
    dir.close();
  }

  public void testDeadlock() throws Exception {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random())).setMaxBufferedDocs(2));
    Document doc = new Document();

    FieldType customType = new FieldType(TextField.TYPE_STORED);
    customType.setStoreTermVectors(true);
    customType.setStoreTermVectorPositions(true);
    customType.setStoreTermVectorOffsets(true);
    
    doc.add(newField("content", "aaa bbb ccc ddd eee fff ggg hhh iii", customType));
    writer.addDocument(doc);
    writer.addDocument(doc);
    writer.addDocument(doc);
    writer.commit();
    // index has 2 segments

    Directory dir2 = newDirectory();
    IndexWriter writer2 = new IndexWriter(dir2, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    writer2.addDocument(doc);
    writer2.close();

    IndexReader r1 = DirectoryReader.open(dir2);
    writer.addIndexes(r1, r1);
    writer.close();

    IndexReader r3 = DirectoryReader.open(dir);
    assertEquals(5, r3.numDocs());
    r3.close();

    r1.close();

    dir2.close();
    dir.close();
  }

  private class IndexerThreadInterrupt extends Thread {
    volatile boolean failed;
    volatile boolean finish;

    volatile boolean allowInterrupt = false;
    final Random random;
    final Directory adder;
    
    IndexerThreadInterrupt() throws IOException {
      this.random = new Random(random().nextLong());
      // make a little directory for addIndexes
      // LUCENE-2239: won't work with NIOFS/MMAP
      adder = new MockDirectoryWrapper(random, new RAMDirectory());
      IndexWriterConfig conf = newIndexWriterConfig(random,
          TEST_VERSION_CURRENT, new MockAnalyzer(random));
      IndexWriter w = new IndexWriter(adder, conf);
      Document doc = new Document();
      doc.add(newStringField(random, "id", "500", Field.Store.NO));
      doc.add(newField(random, "field", "some prepackaged text contents", storedTextType));
      if (defaultCodecSupportsDocValues()) {
        doc.add(new BinaryDocValuesField("binarydv", new BytesRef("500")));
        doc.add(new NumericDocValuesField("numericdv", 500));
        doc.add(new SortedDocValuesField("sorteddv", new BytesRef("500")));
      }
      if (defaultCodecSupportsSortedSet()) {
        doc.add(new SortedSetDocValuesField("sortedsetdv", new BytesRef("one")));
        doc.add(new SortedSetDocValuesField("sortedsetdv", new BytesRef("two")));
      }
      w.addDocument(doc);
      doc = new Document();
      doc.add(newStringField(random, "id", "501", Field.Store.NO));
      doc.add(newField(random, "field", "some more contents", storedTextType));
      if (defaultCodecSupportsDocValues()) {
        doc.add(new BinaryDocValuesField("binarydv", new BytesRef("501")));
        doc.add(new NumericDocValuesField("numericdv", 501));
        doc.add(new SortedDocValuesField("sorteddv", new BytesRef("501")));
      }
      if (defaultCodecSupportsSortedSet()) {
        doc.add(new SortedSetDocValuesField("sortedsetdv", new BytesRef("two")));
        doc.add(new SortedSetDocValuesField("sortedsetdv", new BytesRef("three")));
      }
      w.addDocument(doc);
      w.deleteDocuments(new Term("id", "500"));
      w.close();
    }

    @Override
    public void run() {
      // LUCENE-2239: won't work with NIOFS/MMAP
      Directory dir = new MockDirectoryWrapper(random, new RAMDirectory());
      IndexWriter w = null;
      while(!finish) {
        try {

          while(!finish) {
            if (w != null) {
              w.close();
              w = null;
            }
            IndexWriterConfig conf = newIndexWriterConfig(random,
                                                          TEST_VERSION_CURRENT, new MockAnalyzer(random)).setMaxBufferedDocs(2);
            w = new IndexWriter(dir, conf);

            Document doc = new Document();
            Field idField = newStringField(random, "id", "", Field.Store.NO);
            Field binaryDVField = null;
            Field numericDVField = null;
            Field sortedDVField = null;
            Field sortedSetDVField = new SortedSetDocValuesField("sortedsetdv", new BytesRef());
            doc.add(idField);
            doc.add(newField(random, "field", "some text contents", storedTextType));
            if (defaultCodecSupportsDocValues()) {
              binaryDVField = new BinaryDocValuesField("binarydv", new BytesRef());
              numericDVField = new NumericDocValuesField("numericdv", 0);
              sortedDVField = new SortedDocValuesField("sorteddv", new BytesRef());
              doc.add(binaryDVField);
              doc.add(numericDVField);
              doc.add(sortedDVField);
            }
            if (defaultCodecSupportsSortedSet()) {
              doc.add(sortedSetDVField);
            }
            for(int i=0;i<100;i++) {
              idField.setStringValue(Integer.toString(i));
              if (defaultCodecSupportsDocValues()) {
                binaryDVField.setBytesValue(new BytesRef(idField.stringValue()));
                numericDVField.setLongValue(i);
                sortedDVField.setBytesValue(new BytesRef(idField.stringValue()));
              }
              sortedSetDVField.setBytesValue(new BytesRef(idField.stringValue()));
              int action = random.nextInt(100);
              if (action == 17) {
                w.addIndexes(adder);
              } else if (action%30 == 0) {
                w.deleteAll();
              } else if (action%2 == 0) {
                w.updateDocument(new Term("id", idField.stringValue()), doc);
              } else {
                w.addDocument(doc);
              }
              if (random.nextInt(3) == 0) {
                IndexReader r = null;
                try {
                  r = DirectoryReader.open(w, random.nextBoolean());
                  if (random.nextBoolean() && r.maxDoc() > 0) {
                    int docid = random.nextInt(r.maxDoc());
                    w.tryDeleteDocument(r, docid);
                  }
                } finally {
                  IOUtils.closeWhileHandlingException(r);
                }
              }
              if (i%10 == 0) {
                w.commit();
              }
              if (random.nextInt(50) == 0) {
                w.forceMerge(1);
              }
            }
            w.close();
            w = null;
            DirectoryReader.open(dir).close();

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
          }
        } catch (ThreadInterruptedException re) {
          // NOTE: important to leave this verbosity/noise
          // on!!  This test doesn't repro easily so when
          // Jenkins hits a fail we need to study where the
          // interrupts struck!
          System.out.println("TEST: got interrupt");
          re.printStackTrace(System.out);
          Throwable e = re.getCause();
          assertTrue(e instanceof InterruptedException);
          if (finish) {
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
        if (w != null) {
          try {
            w.rollback();
          } catch (IOException ioe) {
            throw new RuntimeException(ioe);
          }
        }

        try {
          _TestUtil.checkIndex(dir);
        } catch (Exception e) {
          failed = true;
          System.out.println("CheckIndex FAILED: unexpected exception");
          e.printStackTrace(System.out);
        }
        try {
          IndexReader r = DirectoryReader.open(dir);
          //System.out.println("doc count=" + r.numDocs());
          r.close();
        } catch (Exception e) {
          failed = true;
          System.out.println("DirectoryReader.open FAILED: unexpected exception");
          e.printStackTrace(System.out);
        }
      }
      try {
        IOUtils.close(dir, adder);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public void testThreadInterruptDeadlock() throws Exception {
    IndexerThreadInterrupt t = new IndexerThreadInterrupt();
    t.setDaemon(true);
    t.start();

    // Force class loader to load ThreadInterruptedException
    // up front... else we can see a false failure if 2nd
    // interrupt arrives while class loader is trying to
    // init this class (in servicing a first interrupt):
    assertTrue(new ThreadInterruptedException(new InterruptedException()).getCause() instanceof InterruptedException);

    // issue 300 interrupts to child thread
    final int numInterrupts = atLeast(300);
    int i = 0;
    while(i < numInterrupts) {
      // TODO: would be nice to also sometimes interrupt the
      // CMS merge threads too ...
      Thread.sleep(10);
      if (t.allowInterrupt) {
        i++;
        t.interrupt();
      }
      if (!t.isAlive()) {
        break;
      }
    }
    t.finish = true;
    t.join();
    assertFalse(t.failed);
  }


  public void testIndexStoreCombos() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    byte[] b = new byte[50];
    for(int i=0;i<50;i++)
      b[i] = (byte) (i+77);

    Document doc = new Document();

    FieldType customType = new FieldType(StoredField.TYPE);
    customType.setTokenized(true);
    
    Field f = new Field("binary", b, 10, 17, customType);
    customType.setIndexed(true);
    f.setTokenStream(new MockTokenizer(new StringReader("doc1field1"), MockTokenizer.WHITESPACE, false));

    FieldType customType2 = new FieldType(TextField.TYPE_STORED);
    
    Field f2 = newField("string", "value", customType2);
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
    w.forceMerge(1);   // force segment merge.
    w.close();

    IndexReader ir = DirectoryReader.open(dir);
    Document doc2 = ir.document(0);
    IndexableField f3 = doc2.getField("binary");
    b = f3.binaryValue().bytes;
    assertTrue(b != null);
    assertEquals(17, b.length, 17);
    assertEquals(87, b[0]);

    assertTrue(ir.document(0).getField("binary").binaryValue()!=null);
    assertTrue(ir.document(1).getField("binary").binaryValue()!=null);
    assertTrue(ir.document(2).getField("binary").binaryValue()!=null);

    assertEquals("value", ir.document(0).get("string"));
    assertEquals("value", ir.document(1).get("string"));
    assertEquals("value", ir.document(2).get("string"));


    // test that the terms were indexed.
    assertTrue(_TestUtil.docs(random(), ir, "binary", new BytesRef("doc1field1"), null, null, DocsEnum.FLAG_NONE).nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    assertTrue(_TestUtil.docs(random(), ir, "binary", new BytesRef("doc2field1"), null, null, DocsEnum.FLAG_NONE).nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    assertTrue(_TestUtil.docs(random(), ir, "binary", new BytesRef("doc3field1"), null, null, DocsEnum.FLAG_NONE).nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    assertTrue(_TestUtil.docs(random(), ir, "string", new BytesRef("doc1field2"), null, null, DocsEnum.FLAG_NONE).nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    assertTrue(_TestUtil.docs(random(), ir, "string", new BytesRef("doc2field2"), null, null, DocsEnum.FLAG_NONE).nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    assertTrue(_TestUtil.docs(random(), ir, "string", new BytesRef("doc3field2"), null, null, DocsEnum.FLAG_NONE).nextDoc() != DocIdSetIterator.NO_MORE_DOCS);

    ir.close();
    dir.close();

  }

  public void testNoDocsIndex() throws Throwable {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    writer.addDocument(new Document());
    writer.close();

    dir.close();
  }

  public void testIndexDivisor() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig config = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    config.setTermIndexInterval(2);
    IndexWriter w = new IndexWriter(dir, config);
    StringBuilder s = new StringBuilder();
    // must be > 256
    for(int i=0;i<300;i++) {
      s.append(' ').append(i);
    }
    Document d = new Document();
    Field f = newTextField("field", s.toString(), Field.Store.NO);
    d.add(f);
    w.addDocument(d);

    AtomicReader r = getOnlySegmentReader(w.getReader());
    TermsEnum t = r.fields().terms("field").iterator(null);
    int count = 0;
    while(t.next() != null) {
      final DocsEnum docs = _TestUtil.docs(random(), t, null, null, DocsEnum.FLAG_NONE);
      assertEquals(0, docs.nextDoc());
      assertEquals(DocIdSetIterator.NO_MORE_DOCS, docs.nextDoc());
      count++;
    }
    assertEquals(300, count);
    r.close();
    w.close();
    dir.close();
  }

  public void testDeleteUnusedFiles() throws Exception {
    for(int iter=0;iter<2;iter++) {
      Directory dir = newMockDirectory(); // relies on windows semantics

      LogMergePolicy mergePolicy = newLogMergePolicy(true);
      
      // This test expects all of its segments to be in CFS
      mergePolicy.setNoCFSRatio(1.0);
      mergePolicy.setMaxCFSSegmentSizeMB(Double.POSITIVE_INFINITY);

      IndexWriter w = new IndexWriter(
          dir,
          newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).
              setMergePolicy(mergePolicy)
      );
      Document doc = new Document();
      doc.add(newTextField("field", "go", Field.Store.NO));
      w.addDocument(doc);
      DirectoryReader r;
      if (iter == 0) {
        // use NRT
        r = w.getReader();
      } else {
        // don't use NRT
        w.commit();
        r = DirectoryReader.open(dir);
      }

      List<String> files = Arrays.asList(dir.listAll());
      assertTrue(files.contains("_0.cfs"));
      w.addDocument(doc);
      w.forceMerge(1);
      if (iter == 1) {
        w.commit();
      }
      IndexReader r2 = DirectoryReader.openIfChanged(r);
      assertNotNull(r2);
      assertTrue(r != r2);
      files = Arrays.asList(dir.listAll());

      // NOTE: here we rely on "Windows" behavior, ie, even
      // though IW wanted to delete _0.cfs since it was
      // merged away, because we have a reader open
      // against this file, it should still be here:
      assertTrue(files.contains("_0.cfs"));
      // forceMerge created this
      //assertTrue(files.contains("_2.cfs"));
      w.deleteUnusedFiles();

      files = Arrays.asList(dir.listAll());
      // r still holds this file open
      assertTrue(files.contains("_0.cfs"));
      //assertTrue(files.contains("_2.cfs"));

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
      //assertTrue(files.contains("_2.cfs"));

      w.close();
      r2.close();

      dir.close();
    }
  }

  public void testDeleteUnsedFiles2() throws Exception {
    // Validates that iw.deleteUnusedFiles() also deletes unused index commits
    // in case a deletion policy which holds onto commits is used.
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random()))
        .setIndexDeletionPolicy(new SnapshotDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy())));
    SnapshotDeletionPolicy sdp = (SnapshotDeletionPolicy) writer.getConfig().getIndexDeletionPolicy();

    // First commit
    Document doc = new Document();

    FieldType customType = new FieldType(TextField.TYPE_STORED);
    customType.setStoreTermVectors(true);
    customType.setStoreTermVectorPositions(true);
    customType.setStoreTermVectorOffsets(true);
    
    doc.add(newField("c", "val", customType));
    writer.addDocument(doc);
    writer.commit();
    assertEquals(1, DirectoryReader.listCommits(dir).size());

    // Keep that commit
    sdp.snapshot("id");

    // Second commit - now KeepOnlyLastCommit cannot delete the prev commit.
    doc = new Document();
    doc.add(newField("c", "val", customType));
    writer.addDocument(doc);
    writer.commit();
    assertEquals(2, DirectoryReader.listCommits(dir).size());

    // Should delete the unreferenced commit
    sdp.release("id");
    writer.deleteUnusedFiles();
    assertEquals(1, DirectoryReader.listCommits(dir).size());

    writer.close();
    dir.close();
  }

  public void testEmptyFSDirWithNoLock() throws Exception {
    // Tests that if FSDir is opened w/ a NoLockFactory (or SingleInstanceLF),
    // then IndexWriter ctor succeeds. Previously (LUCENE-2386) it failed
    // when listAll() was called in IndexFileDeleter.
    Directory dir = newFSDirectory(_TestUtil.getTempDir("emptyFSDirNoLock"), NoLockFactory.getNoLockFactory());
    new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random()))).close();
    dir.close();
  }

  public void testEmptyDirRollback() throws Exception {
    // TODO: generalize this test
    assumeFalse("test makes assumptions about file counts", Codec.getDefault() instanceof SimpleTextCodec);
    // Tests that if IW is created over an empty Directory, some documents are
    // indexed, flushed (but not committed) and then IW rolls back, then no
    // files are left in the Directory.
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random()))
                                         .setMaxBufferedDocs(2).setMergePolicy(newLogMergePolicy()));
    String[] files = dir.listAll();

    // Creating over empty dir should not create any files,
    // or, at most the write.lock file
    final int extraFileCount;
    if (files.length == 1) {
      assertTrue(files[0].endsWith("write.lock"));
      extraFileCount = 1;
    } else {
      assertEquals(0, files.length);
      extraFileCount = 0;
    }

    Document doc = new Document();
    FieldType customType = new FieldType(TextField.TYPE_STORED);
    customType.setStoreTermVectors(true);
    customType.setStoreTermVectorPositions(true);
    customType.setStoreTermVectorOffsets(true);
    // create as many files as possible
    doc.add(newField("c", "val", customType));
    writer.addDocument(doc);
    // Adding just one document does not call flush yet.
    int computedExtraFileCount = 0;
    for (String file : dir.listAll()) {
      if (file.lastIndexOf('.') < 0
          // don't count stored fields and term vectors in
          || !Arrays.asList("fdx", "fdt", "tvx", "tvd", "tvf").contains(file.substring(file.lastIndexOf('.') + 1))) {
        ++computedExtraFileCount;
      }
    }
    assertEquals("only the stored and term vector files should exist in the directory", extraFileCount, computedExtraFileCount);

    doc = new Document();
    doc.add(newField("c", "val", customType));
    writer.addDocument(doc);

    // The second document should cause a flush.
    assertTrue("flush should have occurred and files should have been created", dir.listAll().length > 5 + extraFileCount);

    // After rollback, IW should remove all files
    writer.rollback();
    assertEquals("no files should exist in the directory after rollback", 0, dir.listAll().length);

    // Since we rolled-back above, that close should be a no-op
    writer.close();
    assertEquals("expected a no-op close after IW.rollback()", 0, dir.listAll().length);
    dir.close();
  }

  public void testNoSegmentFile() throws IOException {
    BaseDirectoryWrapper dir = newDirectory();
    dir.setLockFactory(NoLockFactory.getNoLockFactory());
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random())).setMaxBufferedDocs(2));

    Document doc = new Document();
    FieldType customType = new FieldType(TextField.TYPE_STORED);
    customType.setStoreTermVectors(true);
    customType.setStoreTermVectorPositions(true);
    customType.setStoreTermVectorOffsets(true);
    doc.add(newField("c", "val", customType));
    w.addDocument(doc);
    w.addDocument(doc);
    IndexWriter w2 = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random())).setMaxBufferedDocs(2)
        .setOpenMode(OpenMode.CREATE));

    w2.close();
    // If we don't do that, the test fails on Windows
    w.rollback();

    // This test leaves only segments.gen, which causes
    // DirectoryReader.indexExists to return true:
    dir.setCheckIndexOnClose(false);
    dir.close();
  }

  public void testNoUnwantedTVFiles() throws Exception {

    Directory dir = newDirectory();
    IndexWriter indexWriter = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).setRAMBufferSizeMB(0.01).setMergePolicy(newLogMergePolicy()));
    ((LogMergePolicy) indexWriter.getConfig().getMergePolicy()).setUseCompoundFile(false);

    String BIG="alskjhlaksjghlaksjfhalksvjepgjioefgjnsdfjgefgjhelkgjhqewlrkhgwlekgrhwelkgjhwelkgrhwlkejg";
    BIG=BIG+BIG+BIG+BIG;

    FieldType customType = new FieldType(TextField.TYPE_STORED);
    customType.setOmitNorms(true);
    FieldType customType2 = new FieldType(TextField.TYPE_STORED);
    customType2.setTokenized(false);
    FieldType customType3 = new FieldType(TextField.TYPE_STORED);
    customType3.setTokenized(false);
    customType3.setOmitNorms(true);
    
    for (int i=0; i<2; i++) {
      Document doc = new Document();
      doc.add(new Field("id", Integer.toString(i)+BIG, customType3));
      doc.add(new Field("str", Integer.toString(i)+BIG, customType2));
      doc.add(new Field("str2", Integer.toString(i)+BIG, storedTextType));
      doc.add(new Field("str3", Integer.toString(i)+BIG, customType));
      indexWriter.addDocument(doc);
    }

    indexWriter.close();

    _TestUtil.checkIndex(dir);

    assertNoUnreferencedFiles(dir, "no tv files");
    DirectoryReader r0 = DirectoryReader.open(dir);
    for (AtomicReaderContext ctx : r0.leaves()) {
      SegmentReader sr = (SegmentReader) ctx.reader();
      assertFalse(sr.getFieldInfos().hasVectors());
    }
    
    r0.close();
    dir.close();
  }

  static final class StringSplitAnalyzer extends Analyzer {
    @Override
    public TokenStreamComponents createComponents(String fieldName, Reader reader) {
      return new TokenStreamComponents(new StringSplitTokenizer(reader));
    }
  }

  private static class StringSplitTokenizer extends Tokenizer {
    private String[] tokens;
    private int upto;
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

    public StringSplitTokenizer(Reader r) {
      super(r);
      try {
        setReader(r);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public final boolean incrementToken() {
      clearAttributes();      
      if (upto < tokens.length) {
        termAtt.setEmpty();
        termAtt.append(tokens[upto]);
        upto++;
        return true;
      } else {
        return false;
      }
    }

    @Override
    public void reset() throws IOException {
       this.upto = 0;
       final StringBuilder b = new StringBuilder();
       final char[] buffer = new char[1024];
       int n;
       while ((n = input.read(buffer)) != -1) {
         b.append(buffer, 0, n);
       }
       this.tokens = b.toString().split(" ");
    }
  }

  /**
   * Make sure we skip wicked long terms.
   */
  public void testWickedLongTerm() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, new StringSplitAnalyzer());

    char[] chars = new char[DocumentsWriterPerThread.MAX_TERM_LENGTH_UTF8];
    Arrays.fill(chars, 'x');
    Document doc = new Document();
    final String bigTerm = new String(chars);
    final BytesRef bigTermBytesRef = new BytesRef(bigTerm);

    // This contents produces a too-long term:
    String contents = "abc xyz x" + bigTerm + " another term";
    doc.add(new TextField("content", contents, Field.Store.NO));
    w.addDocument(doc);

    // Make sure we can add another normal document
    doc = new Document();
    doc.add(new TextField("content", "abc bbb ccc", Field.Store.NO));
    w.addDocument(doc);

    IndexReader reader = w.getReader();
    w.close();

    // Make sure all terms < max size were indexed
    assertEquals(2, reader.docFreq(new Term("content", "abc")));
    assertEquals(1, reader.docFreq(new Term("content", "bbb")));
    assertEquals(1, reader.docFreq(new Term("content", "term")));
    assertEquals(1, reader.docFreq(new Term("content", "another")));

    // Make sure position is still incremented when
    // massive term is skipped:
    DocsAndPositionsEnum tps = MultiFields.getTermPositionsEnum(reader, null, "content", new BytesRef("another"));
    assertEquals(0, tps.nextDoc());
    assertEquals(1, tps.freq());
    assertEquals(3, tps.nextPosition());

    // Make sure the doc that has the massive term is in
    // the index:
    assertEquals("document with wicked long term should is not in the index!", 2, reader.numDocs());

    reader.close();
    dir.close();
    dir = newDirectory();

    // Make sure we can add a document with exactly the
    // maximum length term, and search on that term:
    doc = new Document();
    FieldType customType = new FieldType(TextField.TYPE_NOT_STORED);
    customType.setTokenized(false);
    Field contentField = new Field("content", "", customType);
    doc.add(contentField);

    w = new RandomIndexWriter(random(), dir);

    contentField.setStringValue("other");
    w.addDocument(doc);

    contentField.setStringValue("term");
    w.addDocument(doc);

    contentField.setStringValue(bigTerm);
    w.addDocument(doc);

    contentField.setStringValue("zzz");
    w.addDocument(doc);

    reader = w.getReader();
    w.close();
    assertEquals(1, reader.docFreq(new Term("content", bigTerm)));

    SortedDocValues dti = FieldCache.DEFAULT.getTermsIndex(SlowCompositeReaderWrapper.wrap(reader), "content", random().nextFloat() * PackedInts.FAST);
    assertEquals(4, dti.getValueCount());
    BytesRef br = new BytesRef();
    dti.lookupOrd(2, br);
    assertEquals(bigTermBytesRef, br);
    reader.close();
    dir.close();
  }

  // LUCENE-3183
  public void testEmptyFieldNameTIIOne() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    iwc.setTermIndexInterval(1);
    iwc.setReaderTermsIndexDivisor(1);
    IndexWriter writer = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(newTextField("", "a b c", Field.Store.NO));
    writer.addDocument(doc);
    writer.close();
    dir.close();
  }

  public void testDeleteAllNRTLeftoverFiles() throws Exception {

    Directory d = new MockDirectoryWrapper(random(), new RAMDirectory());
    IndexWriter w = new IndexWriter(d, new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    Document doc = new Document();
    for(int i = 0; i < 20; i++) {
      for(int j = 0; j < 100; ++j) {
        w.addDocument(doc);
      }
      w.commit();
      DirectoryReader.open(w, true).close();

      w.deleteAll();
      w.commit();

      // Make sure we accumulate no files except for empty
      // segments_N and segments.gen:
      assertTrue(d.listAll().length <= 2);
    }

    w.close();
    d.close();
  }

  public void testNRTReaderVersion() throws Exception {
    Directory d = new MockDirectoryWrapper(random(), new RAMDirectory());
    IndexWriter w = new IndexWriter(d, new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    Document doc = new Document();
    doc.add(newStringField("id", "0", Field.Store.YES));
    w.addDocument(doc);
    DirectoryReader r = w.getReader();
    long version = r.getVersion();
    r.close();

    w.addDocument(doc);
    r = w.getReader();
    long version2 = r.getVersion();
    r.close();
    assert(version2 > version);

    w.deleteDocuments(new Term("id", "0"));
    r = w.getReader();
    w.close();
    long version3 = r.getVersion();
    r.close();
    assert(version3 > version2);
    d.close();
  }

  public void testWhetherDeleteAllDeletesWriteLock() throws Exception {
    Directory d = newFSDirectory(_TestUtil.getTempDir("TestIndexWriter.testWhetherDeleteAllDeletesWriteLock"));
    // Must use SimpleFSLockFactory... NativeFSLockFactory
    // somehow "knows" a lock is held against write.lock
    // even if you remove that file:
    d.setLockFactory(new SimpleFSLockFactory());
    RandomIndexWriter w1 = new RandomIndexWriter(random(), d);
    w1.deleteAll();
    try {
      new RandomIndexWriter(random(), d, newIndexWriterConfig(TEST_VERSION_CURRENT, null).setWriteLockTimeout(100));
      fail("should not be able to create another writer");
    } catch (LockObtainFailedException lofe) {
      // expected
    }
    w1.close();
    d.close();
  }

  public void testChangeIndexOptions() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir,
                                    new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));

    FieldType docsAndFreqs = new FieldType(TextField.TYPE_NOT_STORED);
    docsAndFreqs.setIndexOptions(IndexOptions.DOCS_AND_FREQS);

    FieldType docsOnly = new FieldType(TextField.TYPE_NOT_STORED);
    docsOnly.setIndexOptions(IndexOptions.DOCS_ONLY);

    Document doc = new Document();
    doc.add(new Field("field", "a b c", docsAndFreqs));
    w.addDocument(doc);
    w.addDocument(doc);

    doc = new Document();
    doc.add(new Field("field", "a b c", docsOnly));
    w.addDocument(doc);
    w.close();
    dir.close();
  }

  public void testOnlyUpdateDocuments() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir,
                                    new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));

    final List<Document> docs = new ArrayList<Document>();
    docs.add(new Document());
    w.updateDocuments(new Term("foo", "bar"),
                      docs);
    w.close();
    dir.close();
  }

  // LUCENE-3872
  public void testPrepareCommitThenClose() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir,
                                    new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));

    w.prepareCommit();
    try {
      w.close();
      fail("should have hit exception");
    } catch (IllegalStateException ise) {
      // expected
    }
    w.commit();
    w.close();
    IndexReader r = DirectoryReader.open(dir);
    assertEquals(0, r.maxDoc());
    r.close();
    dir.close();
  }

  // LUCENE-3872
  public void testPrepareCommitThenRollback() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir,
                                    new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));

    w.prepareCommit();
    w.rollback();
    assertFalse(DirectoryReader.indexExists(dir));
    dir.close();
  }

  // LUCENE-3872
  public void testPrepareCommitThenRollback2() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir,
                                    new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));

    w.commit();
    w.addDocument(new Document());
    w.prepareCommit();
    w.rollback();
    assertTrue(DirectoryReader.indexExists(dir));
    IndexReader r = DirectoryReader.open(dir);
    assertEquals(0, r.maxDoc());
    r.close();
    dir.close();
  }
  
  public void testDontInvokeAnalyzerForUnAnalyzedFields() throws Exception {
    Analyzer analyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        throw new IllegalStateException("don't invoke me!");
      }

      @Override
      public int getPositionIncrementGap(String fieldName) {
        throw new IllegalStateException("don't invoke me!");
      }

      @Override
      public int getOffsetGap(String fieldName) {
        throw new IllegalStateException("don't invoke me!");
      }
    };
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig( 
        TEST_VERSION_CURRENT, analyzer));
    Document doc = new Document();
    FieldType customType = new FieldType(StringField.TYPE_NOT_STORED);
    customType.setStoreTermVectors(true);
    customType.setStoreTermVectorPositions(true);
    customType.setStoreTermVectorOffsets(true);
    Field f = newField("field", "abcd", customType);
    doc.add(f);
    doc.add(f);
    Field f2 = newField("field", "", customType);
    doc.add(f2);
    doc.add(f);
    w.addDocument(doc);
    w.close();
    dir.close();
  }
  
  //LUCENE-1468 -- make sure opening an IndexWriter with
  // create=true does not remove non-index files
  
  public void testOtherFiles() throws Throwable {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, 
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    iw.addDocument(new Document());
    iw.close();
    try {
      // Create my own random file:
      IndexOutput out = dir.createOutput("myrandomfile", newIOContext(random()));
      out.writeByte((byte) 42);
      out.close();
      
      new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random()))).close();
      
      assertTrue(dir.fileExists("myrandomfile"));
    } finally {
      dir.close();
    }
  }
  
  // here we do better, there is no current segments file, so we don't delete anything.
  // however, if you actually go and make a commit, the next time you run indexwriter
  // this file will be gone.
  public void testOtherFiles2() throws Throwable {
    Directory dir = newDirectory();
    try {
      // Create my own random file:
      IndexOutput out = dir.createOutput("_a.frq", newIOContext(random()));
      out.writeByte((byte) 42);
      out.close();
      
      new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random()))).close();
      
      assertTrue(dir.fileExists("_a.frq"));
      
      IndexWriter iw = new IndexWriter(dir, 
          newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));
      iw.addDocument(new Document());
      iw.close();
      
      assertFalse(dir.fileExists("_a.frq"));
    } finally {
      dir.close();
    }
  }

  // LUCENE-4398
  public void testRotatingFieldNames() throws Exception {
    Directory dir = newFSDirectory(_TestUtil.getTempDir("TestIndexWriter.testChangingFields"));
    IndexWriterConfig iwc = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    iwc.setRAMBufferSizeMB(0.2);
    iwc.setMaxBufferedDocs(-1);
    IndexWriter w = new IndexWriter(dir, iwc);
    int upto = 0;

    FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
    ft.setOmitNorms(true);

    int firstDocCount = -1;
    for(int iter=0;iter<10;iter++) {
      final int startFlushCount = w.getFlushCount();
      int docCount = 0;
      while(w.getFlushCount() == startFlushCount) {
        Document doc = new Document();
        for(int i=0;i<10;i++) {
          doc.add(new Field("field" + (upto++), "content", ft));
        }
        w.addDocument(doc);
        docCount++;
      }

      if (VERBOSE) {
        System.out.println("TEST: iter=" + iter + " flushed after docCount=" + docCount);
      }

      if (iter == 0) {
        firstDocCount = docCount;
      }

      assertTrue("flushed after too few docs: first segment flushed at docCount=" + firstDocCount + ", but current segment flushed after docCount=" + docCount + "; iter=" + iter, ((float) docCount) / firstDocCount > 0.9);

      if (upto > 5000) {
        // Start re-using field names after a while
        // ... important because otherwise we can OOME due
        // to too many FieldInfo instances.
        upto = 0;
      }
    }
    w.close();
    dir.close();
  }
  
  // LUCENE-4575
  public void testCommitWithUserDataOnly() throws Exception {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, null));
    writer.commit(); // first commit to complete IW create transaction.
    
    // this should store the commit data, even though no other changes were made
    writer.setCommitData(new HashMap<String,String>() {{
      put("key", "value");
    }});
    writer.commit();
    
    DirectoryReader r = DirectoryReader.open(dir);
    assertEquals("value", r.getIndexCommit().getUserData().get("key"));
    r.close();
    
    // now check setCommitData and prepareCommit/commit sequence
    writer.setCommitData(new HashMap<String,String>() {{
      put("key", "value1");
    }});
    writer.prepareCommit();
    writer.setCommitData(new HashMap<String,String>() {{
      put("key", "value2");
    }});
    writer.commit(); // should commit the first commitData only, per protocol

    r = DirectoryReader.open(dir);
    assertEquals("value1", r.getIndexCommit().getUserData().get("key"));
    r.close();
    
    // now should commit the second commitData - there was a bug where 
    // IndexWriter.finishCommit overrode the second commitData
    writer.commit();
    r = DirectoryReader.open(dir);
    assertEquals("IndexWriter.finishCommit may have overridden the second commitData",
        "value2", r.getIndexCommit().getUserData().get("key"));
    r.close();
    
    writer.close();
    dir.close();
  }
  
  @Test
  public void testGetCommitData() throws Exception {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, null));
    writer.setCommitData(new HashMap<String,String>() {{
      put("key", "value");
    }});
    assertEquals("value", writer.getCommitData().get("key"));
    writer.close();
    
    // validate that it's also visible when opening a new IndexWriter
    writer = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, null).setOpenMode(OpenMode.APPEND));
    assertEquals("value", writer.getCommitData().get("key"));
    writer.close();
    
    dir.close();
  }
  
  public void testIterableThrowsException() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    int iters = atLeast(100);
    int docCount = 0;
    int docId = 0;
    Set<String> liveIds = new HashSet<String>();
    for (int i = 0; i < iters; i++) {
      List<Iterable<IndexableField>> docs = new ArrayList<Iterable<IndexableField>>();
      FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
      FieldType idFt = new FieldType(TextField.TYPE_STORED);
      
      int numDocs = atLeast(4);
      for (int j = 0; j < numDocs; j++) {
        Document doc = new Document();
        doc.add(newField("id", ""+ (docId++), idFt));
        doc.add(newField("foo", _TestUtil.randomSimpleString(random()), ft));
        docs.add(doc);
      }
      boolean success = false;
      try {
        w.addDocuments(new RandomFailingFieldIterable(docs, random()));
        success = true;
      } catch (RuntimeException e) {
        assertEquals("boom", e.getMessage());
      } finally {
        if (success) {
          docCount += docs.size();
          for (Iterable<IndexableField> indexDocument : docs) {
            liveIds.add(((Document)indexDocument).get("id"));  
          }
        }
      }
    }
    DirectoryReader reader = w.getReader();
    assertEquals(docCount, reader.numDocs());
    List<AtomicReaderContext> leaves = reader.leaves();
    for (AtomicReaderContext atomicReaderContext : leaves) {
      AtomicReader ar = atomicReaderContext.reader();
      Bits liveDocs = ar.getLiveDocs();
      int maxDoc = ar.maxDoc();
      for (int i = 0; i < maxDoc; i++) {
        if (liveDocs == null || liveDocs.get(i)) {
          assertTrue(liveIds.remove(ar.document(i).get("id")));
        }
      }
    }
    assertTrue(liveIds.isEmpty());
    IOUtils.close(reader, w, dir);
  }

  private static class RandomFailingFieldIterable implements Iterable<Iterable<IndexableField>> {
    private final List<Iterable<IndexableField>> docList;
    private final Random random;

    public RandomFailingFieldIterable(List<Iterable<IndexableField>> docList, Random random) {
      this.docList = docList;
      this.random = random;
    }
    
    @Override
    public Iterator<Iterable<IndexableField>> iterator() {
      final Iterator<Iterable<IndexableField>> docIter = docList.iterator();
      return new Iterator<Iterable<IndexableField>>() {

        @Override
        public boolean hasNext() {
          return docIter.hasNext();
        }

        @Override
        public Iterable<IndexableField> next() {
          if (random.nextInt(5) == 0) {
            throw new RuntimeException("boom");
          }
          return docIter.next();
        }

        @Override
        public void remove() {throw new UnsupportedOperationException();}
        
        
      };
    }
    
  }

  // LUCENE-2727/LUCENE-2812/LUCENE-4738:
  public void testCorruptFirstCommit() throws Exception {
    for(int i=0;i<6;i++) {
      BaseDirectoryWrapper dir = newDirectory();
      dir.createOutput("segments_0", IOContext.DEFAULT).close();
      IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
      int mode = i/2;
      if (mode == 0) {
        iwc.setOpenMode(OpenMode.CREATE);
      } else if (mode == 1) {
        iwc.setOpenMode(OpenMode.APPEND);
      } else if (mode == 2) {
        iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
      }

      if (VERBOSE) {
        System.out.println("\nTEST: i=" + i);
      }

      try {
        if ((i & 1) == 0) {
          new IndexWriter(dir, iwc).close();
        } else {
          new IndexWriter(dir, iwc).rollback();
        }
        if (mode != 0) {
          fail("expected exception");
        }
      } catch (IOException ioe) {
        // OpenMode.APPEND should throw an exception since no
        // index exists:
        if (mode == 0) {
          // Unexpected
          throw ioe;
        }
      }

      if (VERBOSE) {
        System.out.println("  at close: " + Arrays.toString(dir.listAll()));
      }

      if (mode != 0) {
        dir.setCheckIndexOnClose(false);
      }
      dir.close();
    }
  }
}
