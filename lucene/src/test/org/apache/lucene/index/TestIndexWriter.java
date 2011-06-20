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
import java.io.IOException;
import java.io.PrintStream;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockFixedLengthPayloadFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.Field.TermVector;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.store.NoLockFactory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.store.SingleInstanceLockFactory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.ThreadInterruptedException;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util._TestUtil;
import org.apache.lucene.index.codecs.preflexrw.PreFlexRWCodec;

public class TestIndexWriter extends LuceneTestCase {

    public void testDocCount() throws IOException {
        Directory dir = newDirectory();

        IndexWriter writer = null;
        IndexReader reader = null;
        int i;

        long savedWriteLockTimeout = IndexWriterConfig.getDefaultWriteLockTimeout();
        try {
          IndexWriterConfig.setDefaultWriteLockTimeout(2000);
          assertEquals(2000, IndexWriterConfig.getDefaultWriteLockTimeout());
          writer  = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random)));
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

        reader = IndexReader.open(dir, true);
        assertEquals(60, reader.numDocs());
        reader.close();

        // optimize the index and check that the new doc count is correct
        writer = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random)));
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
        writer = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random)).setOpenMode(OpenMode.CREATE));
        assertEquals(0, writer.maxDoc());
        assertEquals(0, writer.numDocs());
        writer.close();
        dir.close();
    }

    static void addDoc(IndexWriter writer) throws IOException
    {
        Document doc = new Document();
        doc.add(newField("content", "aaa", Field.Store.NO, Field.Index.ANALYZED));
        writer.addDocument(doc);
    }

    static void addDocWithIndex(IndexWriter writer, int index) throws IOException
    {
        Document doc = new Document();
        doc.add(newField("content", "aaa " + index, Field.Store.YES, Field.Index.ANALYZED));
        doc.add(newField("id", "" + index, Field.Store.YES, Field.Index.ANALYZED));
        writer.addDocument(doc);
    }



    public static void assertNoUnreferencedFiles(Directory dir, String message) throws IOException {
      String[] startFiles = dir.listAll();
      SegmentInfos infos = new SegmentInfos();
      infos.read(dir);
      new IndexWriter(dir, new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random))).rollback();
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
      IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random)));
      addDoc(writer);
      writer.close();

      // now open reader:
      IndexReader reader = IndexReader.open(dir, true);
      assertEquals("should be one document", reader.numDocs(), 1);

      // now open index for create:
      writer = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random)).setOpenMode(OpenMode.CREATE));
      assertEquals("should be zero documents", writer.maxDoc(), 0);
      addDoc(writer);
      writer.close();

      assertEquals("should be one document", reader.numDocs(), 1);
      IndexReader reader2 = IndexReader.open(dir, true);
      assertEquals("should be one document", reader2.numDocs(), 1);
      reader.close();
      reader2.close();

      dir.close();
    }

    public void testChangesAfterClose() throws IOException {
        Directory dir = newDirectory();

        IndexWriter writer = null;

        writer  = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random)));
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
      MockDirectoryWrapper dir = newDirectory();
      IndexWriter writer  = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random)));
      writer.commit();
      writer.close();

      IndexReader reader = IndexReader.open(dir, true);
      assertEquals(0, reader.maxDoc());
      assertEquals(0, reader.numDocs());
      reader.close();

      writer  = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random)).setOpenMode(OpenMode.APPEND));
      writer.commit();
      writer.close();

      reader = IndexReader.open(dir, true);
      assertEquals(0, reader.maxDoc());
      assertEquals(0, reader.numDocs());
      reader.close();
      dir.close();
    }

    public void testManyFields() throws IOException {
      MockDirectoryWrapper dir = newDirectory();
      IndexWriter writer  = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random)).setMaxBufferedDocs(10));
      for(int j=0;j<100;j++) {
        Document doc = new Document();
        doc.add(newField("a"+j, "aaa" + j, Field.Store.YES, Field.Index.ANALYZED));
        doc.add(newField("b"+j, "aaa" + j, Field.Store.YES, Field.Index.ANALYZED));
        doc.add(newField("c"+j, "aaa" + j, Field.Store.YES, Field.Index.ANALYZED));
        doc.add(newField("d"+j, "aaa", Field.Store.YES, Field.Index.ANALYZED));
        doc.add(newField("e"+j, "aaa", Field.Store.YES, Field.Index.ANALYZED));
        doc.add(newField("f"+j, "aaa", Field.Store.YES, Field.Index.ANALYZED));
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
      MockDirectoryWrapper dir = newDirectory();
      IndexWriter writer  = new IndexWriter(
          dir,
          newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random)).
              setRAMBufferSizeMB(0.000001).
              setMergePolicy(newLogMergePolicy(10))
      );
      int lastNumFile = dir.listAll().length;
      for(int j=0;j<9;j++) {
        Document doc = new Document();
        doc.add(newField("field", "aaa" + j, Field.Store.YES, Field.Index.ANALYZED));
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
      IndexWriter writer  = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)));
      writer.getConfig().setMaxBufferedDocs(10);
      writer.getConfig().setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);

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
      IndexWriter writer  = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)));
      writer.getConfig().setMaxBufferedDocs(10);
      writer.getConfig().setMaxBufferedDeleteTerms(10);
      writer.getConfig().setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);

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
      MockDirectoryWrapper dir = newDirectory();
      IndexWriter writer  = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random)).setRAMBufferSizeMB(0.5));
      for(int i=0;i<3;i++) {
        // First, docs where every term is unique (heavy on
        // Posting instances)
        for(int j=0;j<100;j++) {
          Document doc = new Document();
          for(int k=0;k<100;k++) {
            doc.add(newField("field", Integer.toString(random.nextInt()), Field.Store.YES, Field.Index.ANALYZED));
          }
          writer.addDocument(doc);
        }

        // Next, many single term docs where only one term
        // occurs (heavy on byte blocks)
        for(int j=0;j<100;j++) {
          Document doc = new Document();
          doc.add(newField("field", "aaa aaa aaa aaa aaa aaa aaa aaa aaa aaa", Field.Store.YES, Field.Index.ANALYZED));
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
          doc.add(newField("field", longTerm, Field.Store.YES, Field.Index.ANALYZED));
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
      MockDirectoryWrapper dir = newDirectory();
      IndexWriter writer  = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random)).setMaxBufferedDocs(10));
      // Enable norms for only 1 doc, pre flush
      for(int j=0;j<10;j++) {
        Document doc = new Document();
        Field f = newField("field", "aaa", Field.Store.YES, Field.Index.ANALYZED);
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

      writer = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random))
        .setOpenMode(OpenMode.CREATE).setMaxBufferedDocs(10));
      // Enable norms for only 1 doc, post flush
      for(int j=0;j<27;j++) {
        Document doc = new Document();
        Field f = newField("field", "aaa", Field.Store.YES, Field.Index.ANALYZED);
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
      MockDirectoryWrapper dir = newDirectory();
      IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
          TEST_VERSION_CURRENT, new MockAnalyzer(random)).setRAMBufferSizeMB(0.01));
      // Massive doc that has 128 K a's
      StringBuilder b = new StringBuilder(1024*1024);
      for(int i=0;i<4096;i++) {
        b.append(" a a a a a a a a");
        b.append(" a a a a a a a a");
        b.append(" a a a a a a a a");
        b.append(" a a a a a a a a");
      }
      Document doc = new Document();
      doc.add(newField("field", b.toString(), Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
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

      final class MyRAMDirectory extends MockDirectoryWrapper {
        private LockFactory myLockFactory;
        MyRAMDirectory(Directory delegate) {
          super(random, delegate);
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
        TEST_VERSION_CURRENT, new MockAnalyzer(random)));
      for (int i = 0; i < 100; i++) {
        addDoc(writer);
      }
      writer.close();
      Term searchTerm = new Term("content", "aaa");
      IndexSearcher searcher = new IndexSearcher(dir, false);
      ScoreDoc[] hits = searcher.search(new TermQuery(searchTerm), null, 1000).scoreDocs;
      assertEquals("did not get right number of hits", 100, hits.length);
      searcher.close();

      writer = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random))
        .setOpenMode(OpenMode.CREATE));
      writer.close();
      searcher.close();
      dir.close();
    }

    public void testFlushWithNoMerging() throws IOException {
      Directory dir = newDirectory();
      IndexWriter writer = new IndexWriter(
          dir,
          newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)).
              setMaxBufferedDocs(2).
              setMergePolicy(newLogMergePolicy(10))
      );
      Document doc = new Document();
      doc.add(newField("field", "aaa", Store.YES, Index.ANALYZED, TermVector.WITH_POSITIONS_OFFSETS));
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
      IndexWriter writer  = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random)));
      writer.setInfoStream(VERBOSE ? System.out : null);
      Document doc = new Document();
      doc.add(newField("field", "aaa", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
      writer.addDocument(doc);
      writer.commit();
      if (VERBOSE) {
        System.out.println("\nTEST: now add empty doc");
      }
      writer.addDocument(new Document());
      writer.close();
      IndexReader reader = IndexReader.open(dir, true);
      assertEquals(2, reader.numDocs());
      reader.close();
      dir.close();
    }



  /**
   * Test that no NullPointerException will be raised,
   * when adding one document with a single, empty field
   * and term vectors enabled.
   * @throws IOException
   *
   */
  public void testBadSegment() throws IOException {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random)));

    Document document = new Document();
    document.add(newField("tvtest", "", Store.NO, Index.ANALYZED, TermVector.YES));
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
          TEST_VERSION_CURRENT, new MockAnalyzer(random))
        .setMaxBufferedDocs(2).setMergePolicy(newLogMergePolicy());
      ((LogMergePolicy) conf.getMergePolicy()).setMergeFactor(2);
      IndexWriter iw = new IndexWriter(dir, conf);
      Document document = new Document();
      document.add(newField("tvtest", "a b c", Field.Store.NO, Field.Index.ANALYZED,
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

  public void testVariableSchema() throws Exception {
    Directory dir = newDirectory();
    int delID = 0;
    for(int i=0;i<20;i++) {
      if (VERBOSE) {
        System.out.println("TEST: iter=" + i);
      }
      IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random)).setMaxBufferedDocs(2).setMergePolicy(newLogMergePolicy()));
      writer.setInfoStream(VERBOSE ? System.out : null);
      //LogMergePolicy lmp = (LogMergePolicy) writer.getConfig().getMergePolicy();
      //lmp.setMergeFactor(2);
      //lmp.setUseCompoundFile(false);
      Document doc = new Document();
      String contents = "aa bb cc dd ee ff gg hh ii jj kk";

      if (i == 7) {
        // Add empty docs here
        doc.add(newField("content3", "", Field.Store.NO,
                          Field.Index.ANALYZED));
      } else {
        Field.Store storeVal;
        if (i%2 == 0) {
          doc.add(newField("content4", contents, Field.Store.YES,
                            Field.Index.ANALYZED));
          storeVal = Field.Store.YES;
        } else
          storeVal = Field.Store.NO;
        doc.add(newField("content1", contents, storeVal,
                          Field.Index.ANALYZED));
        doc.add(newField("content3", "", Field.Store.YES,
                          Field.Index.ANALYZED));
        doc.add(newField("content5", "", storeVal,
                          Field.Index.ANALYZED));
      }

      for(int j=0;j<4;j++)
        writer.addDocument(doc);

      writer.close();
      IndexReader reader = IndexReader.open(dir, false);
      reader.deleteDocument(delID++);
      reader.close();

      if (0 == i % 4) {
        writer = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random)));
        //LogMergePolicy lmp2 = (LogMergePolicy) writer.getConfig().getMergePolicy();
        //lmp2.setUseCompoundFile(false);
        writer.optimize();
        writer.close();
      }
    }
    dir.close();
  }

  public void testNoWaitClose() throws Throwable {
    Directory directory = newDirectory();

    final Document doc = new Document();
    Field idField = newField("id", "", Field.Store.YES, Field.Index.NOT_ANALYZED);
    doc.add(idField);

    for(int pass=0;pass<2;pass++) {
      if (VERBOSE) {
        System.out.println("TEST: pass=" + pass);
      }

      IndexWriterConfig conf =  newIndexWriterConfig(
              TEST_VERSION_CURRENT, new MockAnalyzer(random)).
              setOpenMode(OpenMode.CREATE).
              setMaxBufferedDocs(2).
              setMergePolicy(newLogMergePolicy());
      if (pass == 2) {
        conf.setMergeScheduler(new SerialMergeScheduler());
      }

      IndexWriter writer = new IndexWriter(directory, conf);
      ((LogMergePolicy) writer.getConfig().getMergePolicy()).setMergeFactor(100);          
      writer.setInfoStream(VERBOSE ? System.out : null);

      for(int iter=0;iter<10;iter++) {
        if (VERBOSE) {
          System.out.println("TEST: iter=" + iter);
        }
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

        if (failure.size() > 0) {
          throw failure.get(0);
        }

        t1.start();

        writer.close(false);
        t1.join();

        // Make sure reader can read
        IndexReader reader = IndexReader.open(directory, true);
        reader.close();

        // Reopen
        writer = new IndexWriter(directory, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random)).setOpenMode(OpenMode.APPEND).setMergePolicy(newLogMergePolicy()));
        writer.setInfoStream(VERBOSE ? System.out : null);
      }
      writer.close();
    }

    directory.close();
  }


  // LUCENE-1084: test unlimited field length
  public void testUnlimitedMaxFieldLength() throws IOException {
    Directory dir = newDirectory();

    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random)));

    Document doc = new Document();
    StringBuilder b = new StringBuilder();
    for(int i=0;i<10000;i++)
      b.append(" a");
    b.append(" x");
    doc.add(newField("field", b.toString(), Field.Store.NO, Field.Index.ANALYZED));
    writer.addDocument(doc);
    writer.close();

    IndexReader reader = IndexReader.open(dir, true);
    Term t = new Term("field", "x");
    assertEquals(1, reader.docFreq(t));
    reader.close();
    dir.close();
  }



  // LUCENE-1179
  public void testEmptyFieldName() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random)));
    Document doc = new Document();
    doc.add(newField("", "a b c", Field.Store.NO, Field.Index.ANALYZED));
    writer.addDocument(doc);
    writer.close();
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
    protected void doBeforeFlush() throws IOException {
      beforeWasCalled = true;
    }
  }


  // LUCENE-1222
  public void testDoBeforeAfterFlush() throws IOException {
    Directory dir = newDirectory();
    MockIndexWriter w = new MockIndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random)));
    Document doc = new Document();
    doc.add(newField("field", "a field", Field.Store.YES,
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
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random)));
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

    assertTrue(tps.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(1, tps.freq());
    assertEquals(0, tps.nextPosition());
    w.close();

    s.close();
    dir.close();
  }



  // LUCENE-1219
  public void testBinaryFieldOffsetLength() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random)));
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

  // LUCENE-2529
  public void testPositionIncrementGapEmptyField() throws Exception {
    Directory dir = newDirectory();
    MockAnalyzer analyzer = new MockAnalyzer(random);
    analyzer.setPositionIncrementGap( 100 );
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, analyzer));
    Document doc = new Document();
    Field f = newField("field", "", Field.Store.NO,
                        Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS);
    Field f2 = newField("field", "crunch man", Field.Store.NO,
        Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS);
    doc.add(f);
    doc.add(f2);
    w.addDocument(doc);
    w.close();

    IndexReader r = IndexReader.open(dir, true);
    TermPositionVector tpv = ((TermPositionVector) r.getTermFreqVector(0, "field"));
    int[] poss = tpv.getTermPositions(0);
    assertEquals(1, poss.length);
    assertEquals(100, poss[0]);
    poss = tpv.getTermPositions(1);
    assertEquals(1, poss.length);
    assertEquals(101, poss[0]);
    r.close();
    dir.close();
  }


  // LUCENE-1468 -- make sure opening an IndexWriter with
  // create=true does not remove non-index files

  public void testOtherFiles() throws Throwable {
    Directory dir = newDirectory();
    try {
      // Create my own random file:
      IndexOutput out = dir.createOutput("myrandomfile", IOContext.DEFAULT);
      out.writeByte((byte) 42);
      out.close();

      new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random))).close();

      assertTrue(dir.fileExists("myrandomfile"));
    } finally {
      dir.close();
    }
  }

  public void testDeadlock() throws Exception {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random)).setMaxBufferedDocs(2));
    Document doc = new Document();
    doc.add(newField("content", "aaa bbb ccc ddd eee fff ggg hhh iii", Field.Store.YES,
                      Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
    writer.addDocument(doc);
    writer.addDocument(doc);
    writer.addDocument(doc);
    writer.commit();
    // index has 2 segments

    Directory dir2 = newDirectory();
    IndexWriter writer2 = new IndexWriter(dir2, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random)));
    writer2.addDocument(doc);
    writer2.close();

    IndexReader r1 = IndexReader.open(dir2, true);
    IndexReader r2 = (IndexReader) r1.clone();
    writer.addIndexes(r1, r2);
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
            IndexWriterConfig conf = newIndexWriterConfig(
                                                          TEST_VERSION_CURRENT, new MockAnalyzer(random)).setMaxBufferedDocs(2);
            w = new IndexWriter(dir, conf);

            Document doc = new Document();
            doc.add(newField("field", "some text contents", Field.Store.YES, Field.Index.ANALYZED));
            for(int i=0;i<100;i++) {
              w.addDocument(doc);
              if (i%10 == 0) {
                w.commit();
              }
            }
            w.close();
            w = null;
            _TestUtil.checkIndex(dir);
            IndexReader.open(dir, true).close();

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
          if (VERBOSE) {
            System.out.println("TEST: got interrupt");
            re.printStackTrace(System.out);
          }
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
          IndexReader r = IndexReader.open(dir, true);
          //System.out.println("doc count=" + r.numDocs());
          r.close();
        } catch (Exception e) {
          failed = true;
          System.out.println("IndexReader.open FAILED: unexpected exception");
          e.printStackTrace(System.out);
        }
      }
      try {
        dir.close();
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

    // issue 100 interrupts to child thread
    int i = 0;
    while(i < 100) {
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
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random)));
    byte[] b = new byte[50];
    for(int i=0;i<50;i++)
      b[i] = (byte) (i+77);

    Document doc = new Document();
    Field f = new Field("binary", b, 10, 17);
    f.setTokenStream(new MockTokenizer(new StringReader("doc1field1"), MockTokenizer.WHITESPACE, false));
    Field f2 = newField("string", "value", Field.Store.YES,Field.Index.ANALYZED);
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
    assertTrue(MultiFields.getTermDocsEnum(ir, null, "binary", new BytesRef("doc1field1")).nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    assertTrue(MultiFields.getTermDocsEnum(ir, null, "binary", new BytesRef("doc2field1")).nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    assertTrue(MultiFields.getTermDocsEnum(ir, null, "binary", new BytesRef("doc3field1")).nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    assertTrue(MultiFields.getTermDocsEnum(ir, null, "string", new BytesRef("doc1field2")).nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    assertTrue(MultiFields.getTermDocsEnum(ir, null, "string", new BytesRef("doc2field2")).nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    assertTrue(MultiFields.getTermDocsEnum(ir, null, "string", new BytesRef("doc3field2")).nextDoc() != DocIdSetIterator.NO_MORE_DOCS);

    ir.close();
    dir.close();

  }

  // LUCENE-1727: make sure doc fields are stored in order
  public void testStoredFieldsOrder() throws Throwable {
    Directory d = newDirectory();
    IndexWriter w = new IndexWriter(d, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random)));
    Document doc = new Document();
    doc.add(newField("zzz", "a b c", Field.Store.YES, Field.Index.NO));
    doc.add(newField("aaa", "a b c", Field.Store.YES, Field.Index.NO));
    doc.add(newField("zzz", "1 2 3", Field.Store.YES, Field.Index.NO));
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

  public void testNoDocsIndex() throws Throwable {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random)));
    ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
    writer.setInfoStream(new PrintStream(bos));
    writer.addDocument(new Document());
    writer.close();

    dir.close();
  }

  public void testIndexDivisor() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig config = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random));
    config.setTermIndexInterval(2);
    IndexWriter w = new IndexWriter(dir, config);
    StringBuilder s = new StringBuilder();
    // must be > 256
    for(int i=0;i<300;i++) {
      s.append(' ').append(i);
    }
    Document d = new Document();
    Field f = newField("field", s.toString(), Field.Store.NO, Field.Index.ANALYZED);
    d.add(f);
    w.addDocument(d);

    IndexReader r = w.getReader().getSequentialSubReaders()[0];
    TermsEnum t = r.fields().terms("field").iterator();
    int count = 0;
    while(t.next() != null) {
      final DocsEnum docs = t.docs(null, null);
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
      Directory dir = newDirectory();

      LogMergePolicy mergePolicy = newLogMergePolicy(true);
      mergePolicy.setNoCFSRatio(1); // This test expects all of its segments to be in CFS

      IndexWriter w = new IndexWriter(
          dir,
          newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)).
              setMergePolicy(mergePolicy)
      );
      Document doc = new Document();
      doc.add(newField("field", "go", Field.Store.NO, Field.Index.ANALYZED));
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

      // NOTE: here we rely on "Windows" behavior, ie, even
      // though IW wanted to delete _0.cfs since it was
      // optimized away, because we have a reader open
      // against this file, it should still be here:
      assertTrue(files.contains("_0.cfs"));
      // optimize created this
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
    SnapshotDeletionPolicy sdp = new SnapshotDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random))
        .setIndexDeletionPolicy(sdp));

    // First commit
    Document doc = new Document();
    doc.add(newField("c", "val", Store.YES, Index.ANALYZED, TermVector.WITH_POSITIONS_OFFSETS));
    writer.addDocument(doc);
    writer.commit();
    assertEquals(1, IndexReader.listCommits(dir).size());

    // Keep that commit
    sdp.snapshot("id");

    // Second commit - now KeepOnlyLastCommit cannot delete the prev commit.
    doc = new Document();
    doc.add(newField("c", "val", Store.YES, Index.ANALYZED, TermVector.WITH_POSITIONS_OFFSETS));
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

  public void testEmptyFSDirWithNoLock() throws Exception {
    // Tests that if FSDir is opened w/ a NoLockFactory (or SingleInstanceLF),
    // then IndexWriter ctor succeeds. Previously (LUCENE-2386) it failed
    // when listAll() was called in IndexFileDeleter.
    Directory dir = newFSDirectory(_TestUtil.getTempDir("emptyFSDirNoLock"), NoLockFactory.getNoLockFactory());
    new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random))).close();
    dir.close();
  }

  public void testEmptyDirRollback() throws Exception {
    // Tests that if IW is created over an empty Directory, some documents are
    // indexed, flushed (but not committed) and then IW rolls back, then no
    // files are left in the Directory.
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random))
                                         .setMaxBufferedDocs(2).setMergePolicy(newLogMergePolicy()));
    String[] files = dir.listAll();

    writer.setInfoStream(VERBOSE ? System.out : null);

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
    // create as many files as possible
    doc.add(newField("c", "val", Store.YES, Index.ANALYZED, TermVector.WITH_POSITIONS_OFFSETS));
    writer.addDocument(doc);
    // Adding just one document does not call flush yet.
    assertEquals("only the stored and term vector files should exist in the directory", 5 + extraFileCount, dir.listAll().length);

    doc = new Document();
    doc.add(newField("c", "val", Store.YES, Index.ANALYZED, TermVector.WITH_POSITIONS_OFFSETS));
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
    Directory dir = newDirectory();
    dir.setLockFactory(NoLockFactory.getNoLockFactory());
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random)).setMaxBufferedDocs(2));

    Document doc = new Document();
    doc.add(newField("c", "val", Store.YES, Index.ANALYZED, TermVector.WITH_POSITIONS_OFFSETS));
    w.addDocument(doc);
    w.addDocument(doc);
    IndexWriter w2 = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random)).setMaxBufferedDocs(2)
        .setOpenMode(OpenMode.CREATE));

    w2.close();
    // If we don't do that, the test fails on Windows
    w.rollback();
    dir.close();
  }

  public void testRandomStoredFields() throws IOException {
    Directory dir = newDirectory();
    Random rand = random;
    RandomIndexWriter w = new RandomIndexWriter(rand, dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)).setMaxBufferedDocs(_TestUtil.nextInt(rand, 5, 20)));
    //w.w.setInfoStream(System.out);
    //w.w.setUseCompoundFile(false);
    if (VERBOSE) {
      w.w.setInfoStream(System.out);
    }
    final int docCount = atLeast(200);
    final int fieldCount = _TestUtil.nextInt(rand, 1, 5);

    final List<Integer> fieldIDs = new ArrayList<Integer>();

    Field idField = newField("id", "", Field.Store.YES, Field.Index.NOT_ANALYZED);

    for(int i=0;i<fieldCount;i++) {
      fieldIDs.add(i);
    }

    final Map<String,Document> docs = new HashMap<String,Document>();

    if (VERBOSE) {
      System.out.println("TEST: build index docCount=" + docCount);
    }

    for(int i=0;i<docCount;i++) {
      Document doc = new Document();
      doc.add(idField);
      final String id = ""+i;
      idField.setValue(id);
      docs.put(id, doc);
      if (VERBOSE) {
        System.out.println("TEST: add doc id=" + id);
      }

      for(int field: fieldIDs) {
        final String s;
        if (rand.nextInt(4) != 3) {
          s = _TestUtil.randomUnicodeString(rand, 1000);
          doc.add(newField("f"+field, s, Field.Store.YES, Field.Index.NO));
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
        if (VERBOSE) {
          System.out.println("TEST: delete doc id=" + delID);
        }
        w.deleteDocuments(new Term("id", delID));
        docs.remove(delID);
      }
    }

    if (VERBOSE) {
      System.out.println("TEST: " + docs.size() + " docs in index; now load fields");
    }
    if (docs.size() > 0) {
      String[] idsList = docs.keySet().toArray(new String[docs.size()]);

      for(int x=0;x<2;x++) {
        IndexReader r = w.getReader();
        IndexSearcher s = newSearcher(r);

        if (VERBOSE) {
          System.out.println("TEST: cycle x=" + x + " r=" + r);
        }

        int num = atLeast(1000);
        for(int iter=0;iter<num;iter++) {
          String testID = idsList[rand.nextInt(idsList.length)];
          if (VERBOSE) {
            System.out.println("TEST: test id=" + testID);
          }
          TopDocs hits = s.search(new TermQuery(new Term("id", testID)), 1);
          assertEquals(1, hits.totalHits);
          Document doc = r.document(hits.scoreDocs[0].doc);
          Document docExp = docs.get(testID);
          for(int i=0;i<fieldCount;i++) {
            assertEquals("doc " + testID + ", field f" + fieldCount + " is wrong", docExp.get("f"+i),  doc.get("f"+i));
          }
        }
        s.close();
        r.close();
        w.optimize();
      }
    }
    w.close();
    dir.close();
  }

  public void testNoUnwantedTVFiles() throws Exception {

    Directory dir = newDirectory();
    IndexWriter indexWriter = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)).setRAMBufferSizeMB(0.01).setMergePolicy(newLogMergePolicy()));
    ((LogMergePolicy) indexWriter.getConfig().getMergePolicy()).setUseCompoundFile(false);

    String BIG="alskjhlaksjghlaksjfhalksvjepgjioefgjnsdfjgefgjhelkgjhqewlrkhgwlekgrhwelkgjhwelkgrhwlkejg";
    BIG=BIG+BIG+BIG+BIG;

    for (int i=0; i<2; i++) {
      Document doc = new Document();
      doc.add(new Field("id", Integer.toString(i)+BIG, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS));
      doc.add(new Field("str", Integer.toString(i)+BIG, Field.Store.YES, Field.Index.NOT_ANALYZED));
      doc.add(new Field("str2", Integer.toString(i)+BIG, Field.Store.YES, Field.Index.ANALYZED));
      doc.add(new Field("str3", Integer.toString(i)+BIG, Field.Store.YES, Field.Index.ANALYZED_NO_NORMS));
      indexWriter.addDocument(doc);
    }

    indexWriter.close();

    _TestUtil.checkIndex(dir);

    assertNoUnreferencedFiles(dir, "no tv files");
    String[] files = dir.listAll();
    for(String file : files) {
      assertTrue(!file.endsWith(IndexFileNames.VECTORS_FIELDS_EXTENSION));
      assertTrue(!file.endsWith(IndexFileNames.VECTORS_INDEX_EXTENSION));
      assertTrue(!file.endsWith(IndexFileNames.VECTORS_DOCUMENTS_EXTENSION));
    }

    dir.close();
  }

  static final class StringSplitAnalyzer extends Analyzer {
    @Override
    public TokenStream tokenStream(String fieldName, Reader reader) {
      return new StringSplitTokenizer(reader);
    }
  }

  private static class StringSplitTokenizer extends Tokenizer {
    private final String[] tokens;
    private int upto = 0;
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

    public StringSplitTokenizer(Reader r) {
      try {
        final StringBuilder b = new StringBuilder();
        final char[] buffer = new char[1024];
        int n;
        while((n = r.read(buffer)) != -1) {
          b.append(buffer, 0, n);
        }
        tokens = b.toString().split(" ");
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }

    @Override
    public final boolean incrementToken() throws IOException {
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
  }

  /**
   * Make sure we skip wicked long terms.
   */
  public void testWickedLongTerm() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random, dir, new StringSplitAnalyzer());

    char[] chars = new char[DocumentsWriterPerThread.MAX_TERM_LENGTH_UTF8];
    Arrays.fill(chars, 'x');
    Document doc = new Document();
    final String bigTerm = new String(chars);
    final BytesRef bigTermBytesRef = new BytesRef(bigTerm);

    // This contents produces a too-long term:
    String contents = "abc xyz x" + bigTerm + " another term";
    doc.add(new Field("content", contents, Field.Store.NO, Field.Index.ANALYZED));
    w.addDocument(doc);

    // Make sure we can add another normal document
    doc = new Document();
    doc.add(new Field("content", "abc bbb ccc", Field.Store.NO, Field.Index.ANALYZED));
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
    Field contentField = new Field("content", "", Field.Store.NO, Field.Index.NOT_ANALYZED);
    doc.add(contentField);

    w = new RandomIndexWriter(random, dir);

    contentField.setValue("other");
    w.addDocument(doc);

    contentField.setValue("term");
    w.addDocument(doc);

    contentField.setValue(bigTerm);
    w.addDocument(doc);

    contentField.setValue("zzz");
    w.addDocument(doc);

    reader = w.getReader();
    w.close();
    assertEquals(1, reader.docFreq(new Term("content", bigTerm)));

    FieldCache.DocTermsIndex dti = FieldCache.DEFAULT.getTermsIndex(reader, "content", random.nextBoolean());
    assertEquals(5, dti.numOrd());                // +1 for null ord
    assertEquals(4, dti.size());
    assertEquals(bigTermBytesRef, dti.lookup(3, new BytesRef()));
    reader.close();
    dir.close();
  }

  // LUCENE-3183
  public void testEmptyFieldNameTIIOne() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random));
    iwc.setTermIndexInterval(1);
    iwc.setReaderTermsIndexDivisor(1);
    IndexWriter writer = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(newField("", "a b c", Field.Store.NO, Field.Index.ANALYZED));
    writer.addDocument(doc);
    writer.close();
    dir.close();
  }
}
