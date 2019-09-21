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
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.io.StringReader;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
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
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.mockfile.ExtrasFS;
import org.apache.lucene.mockfile.FilterPath;
import org.apache.lucene.mockfile.WindowsFS;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.BaseDirectoryWrapper;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.NoLockFactory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.store.SimpleFSLockFactory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.IOSupplier;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.SetOnce;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.ThreadInterruptedException;
import org.apache.lucene.util.Version;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.junit.Ignore;
import org.junit.Test;

public class TestIndexWriter extends LuceneTestCase {

  private static final FieldType storedTextType = new FieldType(TextField.TYPE_NOT_STORED);
  public void testDocCount() throws IOException {
    Directory dir = newDirectory();

    IndexWriter writer = null;
    IndexReader reader = null;
    int i;

    writer  = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));

    // add 100 documents
    for (i = 0; i < 100; i++) {
      addDocWithIndex(writer,i);
      if (random().nextBoolean()) {
        writer.commit();
      }
    }
    IndexWriter.DocStats docStats = writer.getDocStats();
    assertEquals(100, docStats.maxDoc);
    assertEquals(100, docStats.numDocs);
    writer.close();

    // delete 40 documents
    writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
                             .setMergePolicy(new FilterMergePolicy(NoMergePolicy.INSTANCE) {
                               @Override
                               public boolean keepFullyDeletedSegment(IOSupplier<CodecReader>
                                                                          readerIOSupplier) {
                                 return true;
                               }
                             }));

    for (i = 0; i < 40; i++) {
      writer.deleteDocuments(new Term("id", ""+i));
      if (random().nextBoolean()) {
        writer.commit();
      }
    }
    writer.flush();
    docStats = writer.getDocStats();
    assertEquals(100, docStats.maxDoc);
    assertEquals(60, docStats.numDocs);
    writer.close();

    reader = DirectoryReader.open(dir);
    assertEquals(60, reader.numDocs());
    reader.close();

    // merge the index down and check that the new doc count is correct
    writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    assertEquals(60, writer.getDocStats().numDocs);
    writer.forceMerge(1);
    docStats = writer.getDocStats();
    assertEquals(60, docStats.maxDoc);
    assertEquals(60, docStats.numDocs);
    writer.close();

    // check that the index reader gives the same numbers.
    reader = DirectoryReader.open(dir);
    assertEquals(60, reader.maxDoc());
    assertEquals(60, reader.numDocs());
    reader.close();

    // make sure opening a new index for create over
    // this existing one works correctly:
    writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
                             .setOpenMode(OpenMode.CREATE));
    docStats = writer.getDocStats();
    assertEquals(0, docStats.maxDoc);
    assertEquals(0, docStats.numDocs);
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

  // TODO: we have the logic in MDW to do this check, and it's better, because it knows about files it tried
  // to delete but couldn't: we should replace this!!!!
  public static void assertNoUnreferencedFiles(Directory dir, String message) throws IOException {
    String[] startFiles = dir.listAll();
    new IndexWriter(dir, new IndexWriterConfig(new MockAnalyzer(random()))).rollback();
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
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    addDoc(writer);
    writer.close();

    // now open reader:
    IndexReader reader = DirectoryReader.open(dir);
    assertEquals("should be one document", reader.numDocs(), 1);

    // now open index for create:
    writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
                             .setOpenMode(OpenMode.CREATE));
    assertEquals("should be zero documents", writer.getDocStats().maxDoc, 0);
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
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));

    addDoc(writer);

    // close
    writer.close();
    expectThrows(AlreadyClosedException.class, () -> {
      addDoc(writer);
    });

    dir.close();
  }



  public void testIndexNoDocuments() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer  = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    writer.commit();
    writer.close();

    IndexReader reader = DirectoryReader.open(dir);
    assertEquals(0, reader.maxDoc());
    assertEquals(0, reader.numDocs());
    reader.close();

    writer  = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
                              .setOpenMode(OpenMode.APPEND));
    writer.commit();
    writer.close();

    reader = DirectoryReader.open(dir);
    assertEquals(0, reader.maxDoc());
    assertEquals(0, reader.numDocs());
    reader.close();
    dir.close();
  }

  public void testSmallRAMBuffer() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer  = new IndexWriter(
                                          dir,
                                          newIndexWriterConfig(new MockAnalyzer(random()))
                                          .setRAMBufferSizeMB(0.000001)
                                          .setMergePolicy(newLogMergePolicy(10))
                                          );
    int lastNumSegments = getSegmentCount(dir);
    for(int j=0;j<9;j++) {
      Document doc = new Document();
      doc.add(newField("field", "aaa" + j, storedTextType));
      writer.addDocument(doc);
      // Verify that with a tiny RAM buffer we see new
      // segment after every doc
      int numSegments = getSegmentCount(dir);
      assertTrue(numSegments > lastNumSegments);
      lastNumSegments = numSegments;
    }
    writer.close();
    dir.close();
  }

  /** Returns how many unique segment names are in the directory. */
  private static int getSegmentCount(Directory dir) throws IOException {
    Set<String> segments = new HashSet<>();
    for(String file : dir.listAll()) {
      segments.add(IndexFileNames.parseSegmentName(file));
    }

    return segments.size();
  }

  // Make sure it's OK to change RAM buffer size and
  // maxBufferedDocs in a write session
  public void testChangingRAMBuffer() throws IOException {
    Directory dir = newDirectory();      
    IndexWriter writer  = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    writer.getConfig().setMaxBufferedDocs(10);
    writer.getConfig().setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);

    int lastFlushCount = -1;
    for(int j=1;j<52;j++) {
      Document doc = new Document();
      doc.add(new Field("field", "aaa" + j, storedTextType));
      writer.addDocument(doc);
      TestUtil.syncConcurrentMerges(writer);
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

  public void testEnablingNorms() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer  = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
                                          .setMaxBufferedDocs(10));
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
    ScoreDoc[] hits = searcher.search(new TermQuery(searchTerm), 1000).scoreDocs;
    assertEquals(10, hits.length);
    reader.close();

    writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
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
    hits = searcher.search(new TermQuery(searchTerm), 1000).scoreDocs;
    assertEquals(27, hits.length);
    reader.close();

    reader = DirectoryReader.open(dir);
    reader.close();

    dir.close();
  }

  public void testHighFreqTerm() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
                                         .setRAMBufferSizeMB(0.01));
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
    PostingsEnum td = TestUtil.docs(random(), reader,
                                    "field",
                                    new BytesRef("a"),
                                    null,
                                    PostingsEnum.FREQS);
    td.nextDoc();
    assertEquals(128*1024, td.freq());
    reader.close();
    dir.close();
  }

  public void testFlushWithNoMerging() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(
                                         dir,
                                         newIndexWriterConfig(new MockAnalyzer(random()))
                                         .setMaxBufferedDocs(2)
                                         .setMergePolicy(newLogMergePolicy(10))
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
    SegmentInfos sis = SegmentInfos.readLatestCommit(dir);
    // Since we flushed w/o allowing merging we should now
    // have 10 segments
    assertEquals(10, sis.size());
    dir.close();
  }

  // Make sure we can flush segment w/ norms, then add
  // empty doc (no norms) and flush
  public void testEmptyDocAfterFlushingRealDoc() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer  = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
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
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));

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
      IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()))
                                 .setMaxBufferedDocs(2)
                                 .setMergePolicy(newLogMergePolicy());
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
      IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
                                                  .setMaxBufferedDocs(2)
                                                  .setMergePolicy(newLogMergePolicy()));
      //LogMergePolicy lmp = (LogMergePolicy) writer.getConfig().getMergePolicy();
      //lmp.setMergeFactor(2);
      //lmp.setNoCFSRatio(0.0);
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
        writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
        //LogMergePolicy lmp2 = (LogMergePolicy) writer.getConfig().getMergePolicy();
        //lmp2.setNoCFSRatio(0.0);
        writer.forceMerge(1);
        writer.close();
      }
    }
    dir.close();
  }

  // LUCENE-1084: test unlimited field length
  public void testUnlimitedMaxFieldLength() throws IOException {
    Directory dir = newDirectory();

    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));

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
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    Document doc = new Document();
    doc.add(newTextField("", "a b c", Field.Store.NO));
    writer.addDocument(doc);
    writer.close();
    dir.close();
  }
  
  public void testEmptyFieldNameTerms() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    Document doc = new Document();
    doc.add(newTextField("", "a b c", Field.Store.NO));
    writer.addDocument(doc);  
    writer.close();
    DirectoryReader reader = DirectoryReader.open(dir);
    LeafReader subreader = getOnlyLeafReader(reader);
    TermsEnum te = subreader.terms("").iterator();
    assertEquals(new BytesRef("a"), te.next());
    assertEquals(new BytesRef("b"), te.next());
    assertEquals(new BytesRef("c"), te.next());
    assertNull(te.next());
    reader.close();
    dir.close();
  }
  
  public void testEmptyFieldNameWithEmptyTerm() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    Document doc = new Document();
    doc.add(newStringField("", "", Field.Store.NO));
    doc.add(newStringField("", "a", Field.Store.NO));
    doc.add(newStringField("", "b", Field.Store.NO));
    doc.add(newStringField("", "c", Field.Store.NO));
    writer.addDocument(doc);  
    writer.close();
    DirectoryReader reader = DirectoryReader.open(dir);
    LeafReader subreader = getOnlyLeafReader(reader);
    TermsEnum te = subreader.terms("").iterator();
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
    MockIndexWriter w = new MockIndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
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
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    Document doc = new Document();
    doc.add(new TextField("field", tokens));
    expectThrows(IllegalArgumentException.class, () -> {
      w.addDocument(doc);
    });

    w.close();
    dir.close();
  }

  // LUCENE-2529
  public void testPositionIncrementGapEmptyField() throws Exception {
    Directory dir = newDirectory();
    MockAnalyzer analyzer = new MockAnalyzer(random());
    analyzer.setPositionIncrementGap( 100 );
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(analyzer));
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
    TermsEnum termsEnum = tpv.iterator();
    assertNotNull(termsEnum.next());
    PostingsEnum dpEnum = termsEnum.postings(null, PostingsEnum.ALL);
    assertNotNull(dpEnum);
    assertTrue(dpEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(1, dpEnum.freq());
    assertEquals(100, dpEnum.nextPosition());

    assertNotNull(termsEnum.next());
    dpEnum = termsEnum.postings(dpEnum, PostingsEnum.ALL);
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
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
                                                .setMaxBufferedDocs(2));
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
    IndexWriter writer2 = new IndexWriter(dir2, newIndexWriterConfig(new MockAnalyzer(random())));
    writer2.addDocument(doc);
    writer2.close();

    DirectoryReader r1 = DirectoryReader.open(dir2);
    TestUtil.addIndexesSlowly(writer, r1, r1);
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
    final ByteArrayOutputStream bytesLog = new ByteArrayOutputStream();
    final PrintStream log = new PrintStream(bytesLog, true, IOUtils.UTF_8);
    final int id;

    IndexerThreadInterrupt(int id) throws IOException {
      this.id = id;
      this.random = new Random(random().nextLong());
      // make a little directory for addIndexes
      // LUCENE-2239: won't work with NIOFS/MMAP
      adder = new MockDirectoryWrapper(random, new ByteBuffersDirectory());
      IndexWriterConfig conf = newIndexWriterConfig(random, new MockAnalyzer(random));
      if (conf.getMergeScheduler() instanceof ConcurrentMergeScheduler) {
        conf.setMergeScheduler(new SuppressingConcurrentMergeScheduler() {
            @Override
            protected boolean isOK(Throwable th) {
              return th instanceof AlreadyClosedException ||
                (th instanceof IllegalStateException && th.getMessage().contains("this writer hit an unrecoverable error"));
            }
          });
      }
      IndexWriter w = new IndexWriter(adder, conf);
      Document doc = new Document();
      doc.add(newStringField(random, "id", "500", Field.Store.NO));
      doc.add(newField(random, "field", "some prepackaged text contents", storedTextType));
      doc.add(new BinaryDocValuesField("binarydv", new BytesRef("500")));
      doc.add(new NumericDocValuesField("numericdv", 500));
      doc.add(new SortedDocValuesField("sorteddv", new BytesRef("500")));
      doc.add(new SortedSetDocValuesField("sortedsetdv", new BytesRef("one")));
      doc.add(new SortedSetDocValuesField("sortedsetdv", new BytesRef("two")));
      doc.add(new SortedNumericDocValuesField("sortednumericdv", 4));
      doc.add(new SortedNumericDocValuesField("sortednumericdv", 3));
      w.addDocument(doc);
      doc = new Document();
      doc.add(newStringField(random, "id", "501", Field.Store.NO));
      doc.add(newField(random, "field", "some more contents", storedTextType));
      doc.add(new BinaryDocValuesField("binarydv", new BytesRef("501")));
      doc.add(new NumericDocValuesField("numericdv", 501));
      doc.add(new SortedDocValuesField("sorteddv", new BytesRef("501")));
      doc.add(new SortedSetDocValuesField("sortedsetdv", new BytesRef("two")));
      doc.add(new SortedSetDocValuesField("sortedsetdv", new BytesRef("three")));
      doc.add(new SortedNumericDocValuesField("sortednumericdv", 6));
      doc.add(new SortedNumericDocValuesField("sortednumericdv", 1));
      w.addDocument(doc);
      w.deleteDocuments(new Term("id", "500"));
      w.close();
    }

    @Override
    public void run() {
      // LUCENE-2239: won't work with NIOFS/MMAP
      MockDirectoryWrapper dir = new MockDirectoryWrapper(random, new ByteBuffersDirectory());

      // open/close slowly sometimes
      dir.setUseSlowOpenClosers(true);
      
      // throttle a little
      dir.setThrottling(MockDirectoryWrapper.Throttling.SOMETIMES);

      IndexWriter w = null;
      while(!finish) {
        try {

          while(!finish) {
            if (w != null) {
              // If interrupt arrives inside here, it's
              // fine: we will cycle back and the first
              // thing we do is try to close again,
              // i.e. we'll never try to open a new writer
              // until this one successfully closes:
              // w.rollback();
              try {
                w.close();
              } catch (AlreadyClosedException ace) {
                // OK
              }
              w = null;
            }
            IndexWriterConfig conf = newIndexWriterConfig(random,
                                                          new MockAnalyzer(random)).setMaxBufferedDocs(2);
            if (conf.getMergeScheduler() instanceof ConcurrentMergeScheduler) {
              conf.setMergeScheduler(new SuppressingConcurrentMergeScheduler() {
                  @Override
                  protected boolean isOK(Throwable th) {
                    return th instanceof AlreadyClosedException ||
                      (th instanceof IllegalStateException && th.getMessage().contains("this writer hit an unrecoverable error"));
                  }
                });
            }
            //conf.setInfoStream(log);
            w = new IndexWriter(dir, conf);

            Document doc = new Document();
            Field idField = newStringField(random, "id", "", Field.Store.NO);
            Field binaryDVField = new BinaryDocValuesField("binarydv", new BytesRef());
            Field numericDVField = new NumericDocValuesField("numericdv", 0);
            Field sortedDVField = new SortedDocValuesField("sorteddv", new BytesRef());
            Field sortedSetDVField = new SortedSetDocValuesField("sortedsetdv", new BytesRef());
            doc.add(idField);
            doc.add(newField(random, "field", "some text contents", storedTextType));
            doc.add(binaryDVField);
            doc.add(numericDVField);
            doc.add(sortedDVField);
            doc.add(sortedSetDVField);
            for(int i=0;i<100;i++) {
              //log.println("\nTEST: i=" + i);
              idField.setStringValue(Integer.toString(i));
              binaryDVField.setBytesValue(new BytesRef(idField.stringValue()));
              numericDVField.setLongValue(i);
              sortedDVField.setBytesValue(new BytesRef(idField.stringValue()));
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
                  r = DirectoryReader.open(w, random.nextBoolean(), false);
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
          log.println("TEST thread " + id + ": got interrupt");
          re.printStackTrace(log);
          Throwable e = re.getCause();
          assertTrue(e instanceof InterruptedException);
          if (finish) {
            break;
          }
        } catch (Throwable t) {
          log.println("thread " + id + " FAILED; unexpected exception");
          t.printStackTrace(log);
          listIndexFiles(log, dir);
          failed = true;
          break;
        }
      }

      if (VERBOSE) {
        log.println("TEST: thread " + id + ": now finish failed=" + failed);
      }
      if (!failed) {
        if (VERBOSE) {
          log.println("TEST: thread " + id + ": now rollback");
        }
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
          TestUtil.checkIndex(dir);
        } catch (Exception e) {
          failed = true;
          log.println("thread " + id + ": CheckIndex FAILED: unexpected exception");
          e.printStackTrace(log);
          listIndexFiles(log, dir);
        }
        try {
          IndexReader r = DirectoryReader.open(dir);
          //System.out.println("doc count=" + r.numDocs());
          r.close();
        } catch (Exception e) {
          failed = true;
          log.println("thread " + id + ": DirectoryReader.open FAILED: unexpected exception");
          e.printStackTrace(log);
          listIndexFiles(log, dir);
        }
      }
      try {
        IOUtils.close(dir);
      } catch (IOException e) {
        failed = true;
        throw new RuntimeException("thread " + id, e);
      }
      try {
        IOUtils.close(adder);
      } catch (IOException e) {
        failed = true;
        throw new RuntimeException("thread " + id, e);
      }
    }

    private void listIndexFiles(PrintStream log, Directory dir) {
      try {
        log.println("index files: " + Arrays.toString(dir.listAll()));
      } catch (IOException ioe) {
        // Suppress
        log.println("failed to index files:");
        ioe.printStackTrace(log);
      }
    }
  }

  public void testThreadInterruptDeadlock() throws Exception {
    IndexerThreadInterrupt t = new IndexerThreadInterrupt(1);
    t.setDaemon(true);
    t.start();

    // Force class loader to load ThreadInterruptedException
    // up front... else we can see a false failure if 2nd
    // interrupt arrives while class loader is trying to
    // init this class (in servicing a first interrupt):
    assertTrue(new ThreadInterruptedException(new InterruptedException()).getCause() instanceof InterruptedException);

    // issue 100 interrupts to child thread
    final int numInterrupts = atLeast(100);
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
    if (t.failed) {
      fail(t.bytesLog.toString("UTF-8"));
    }
  }

  public void testIndexStoreCombos() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    byte[] b = new byte[50];
    for(int i=0;i<50;i++)
      b[i] = (byte) (i+77);

    Document doc = new Document();

    FieldType customType = new FieldType(StoredField.TYPE);
    customType.setTokenized(true);
    
    Field f = new Field("binary", b, 10, 17, customType);
    // TODO: this is evil, changing the type after creating the field:
    customType.setIndexOptions(IndexOptions.DOCS);
    final MockTokenizer doc1field1 = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    doc1field1.setReader(new StringReader("doc1field1"));
    f.setTokenStream(doc1field1);

    FieldType customType2 = new FieldType(TextField.TYPE_STORED);
    
    Field f2 = newField("string", "value", customType2);
    final MockTokenizer doc1field2 = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    doc1field2.setReader(new StringReader("doc1field2"));
    f2.setTokenStream(doc1field2);
    doc.add(f);
    doc.add(f2);
    w.addDocument(doc);

    // add 2 docs to test in-memory merging
    final MockTokenizer doc2field1 = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    doc2field1.setReader(new StringReader("doc2field1"));
    f.setTokenStream(doc2field1);
    final MockTokenizer doc2field2 = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    doc2field2.setReader(new StringReader("doc2field2"));
    f2.setTokenStream(doc2field2);
    w.addDocument(doc);

    // force segment flush so we can force a segment merge with doc3 later.
    w.commit();

    final MockTokenizer doc3field1 = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    doc3field1.setReader(new StringReader("doc3field1"));
    f.setTokenStream(doc3field1);
    final MockTokenizer doc3field2 = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    doc3field2.setReader(new StringReader("doc3field2"));
    f2.setTokenStream(doc3field2);

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
    assertTrue(TestUtil.docs(random(), ir, "binary", new BytesRef("doc1field1"), null, PostingsEnum.NONE).nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    assertTrue(TestUtil.docs(random(), ir, "binary", new BytesRef("doc2field1"), null, PostingsEnum.NONE).nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    assertTrue(TestUtil.docs(random(), ir, "binary", new BytesRef("doc3field1"), null, PostingsEnum.NONE).nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    assertTrue(TestUtil.docs(random(), ir, "string", new BytesRef("doc1field2"), null, PostingsEnum.NONE).nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    assertTrue(TestUtil.docs(random(), ir, "string", new BytesRef("doc2field2"), null, PostingsEnum.NONE).nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    assertTrue(TestUtil.docs(random(), ir, "string", new BytesRef("doc3field2"), null, PostingsEnum.NONE).nextDoc() != DocIdSetIterator.NO_MORE_DOCS);

    ir.close();
    dir.close();

  }

  public void testNoDocsIndex() throws Throwable {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    writer.addDocument(new Document());
    writer.close();

    dir.close();
  }


  public void testDeleteUnusedFiles() throws Exception {
    assumeFalse("test relies on exact filenames", Codec.getDefault() instanceof SimpleTextCodec);
    assumeWorkingMMapOnWindows();
    
    for(int iter=0;iter<2;iter++) {
      // relies on windows semantics
      Path path = createTempDir();
      FileSystem fs = new WindowsFS(path.getFileSystem()).getFileSystem(URI.create("file:///"));
      Path indexPath = new FilterPath(path, fs);

      // NOTE: on Unix, we cannot use MMapDir, because WindowsFS doesn't see/think it keeps file handles open.  Yet, on Windows, we MUST use
      // MMapDir because the windows OS will in fact prevent file deletion for us, and fails otherwise:
      FSDirectory dir;
      if (Constants.WINDOWS) {
        dir = new MMapDirectory(indexPath);
      } else {
        dir = new NIOFSDirectory(indexPath);
      }

      MergePolicy mergePolicy = newLogMergePolicy(true);
      
      // This test expects all of its segments to be in CFS
      mergePolicy.setNoCFSRatio(1.0);
      mergePolicy.setMaxCFSSegmentSizeMB(Double.POSITIVE_INFINITY);

      IndexWriter w = new IndexWriter(
          dir,
          newIndexWriterConfig(new MockAnalyzer(random()))
            .setMergePolicy(mergePolicy)
            .setUseCompoundFile(true)
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

      assertTrue(Files.exists(indexPath.resolve("_0.cfs")));
      assertTrue(Files.exists(indexPath.resolve("_0.cfe")));
      assertTrue(Files.exists(indexPath.resolve("_0.si")));
      if (iter == 1) {
        // we run a full commit so there should be a segments file etc.
        assertTrue(Files.exists(indexPath.resolve("segments_1")));
      } else {
        // this is an NRT reopen - no segments files yet
        assertFalse(Files.exists(indexPath.resolve("segments_1")));
      }
      w.addDocument(doc);
      w.forceMerge(1);
      if (iter == 1) {
        w.commit();
      }
      IndexReader r2 = DirectoryReader.openIfChanged(r);
      assertNotNull(r2);
      assertTrue(r != r2);

      // NOTE: here we rely on "Windows" behavior, ie, even
      // though IW wanted to delete _0.cfs since it was
      // merged away, because we have a reader open
      // against this file, it should still be here:
      assertTrue(Files.exists(indexPath.resolve("_0.cfs")));
      // forceMerge created this
      //assertTrue(files.contains("_2.cfs"));
      w.deleteUnusedFiles();

      // r still holds this file open
      assertTrue(Files.exists(indexPath.resolve("_0.cfs")));
      //assertTrue(files.contains("_2.cfs"));

      r.close();
      if (iter == 0) {
        // on closing NRT reader, it calls writer.deleteUnusedFiles
        assertFalse(Files.exists(indexPath.resolve("_0.cfs")));
      } else {
        // now FSDir can remove it
        dir.deletePendingFiles();
        assertFalse(Files.exists(indexPath.resolve("_0.cfs")));
      }

      w.close();
      r2.close();

      dir.close();
    }
  }

  public void testDeleteUnusedFiles2() throws Exception {
    // Validates that iw.deleteUnusedFiles() also deletes unused index commits
    // in case a deletion policy which holds onto commits is used.
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
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
    IndexCommit id = sdp.snapshot();

    // Second commit - now KeepOnlyLastCommit cannot delete the prev commit.
    doc = new Document();
    doc.add(newField("c", "val", customType));
    writer.addDocument(doc);
    writer.commit();
    assertEquals(2, DirectoryReader.listCommits(dir).size());

    // Should delete the unreferenced commit
    sdp.release(id);
    writer.deleteUnusedFiles();
    assertEquals(1, DirectoryReader.listCommits(dir).size());

    writer.close();
    dir.close();
  }

  public void testEmptyFSDirWithNoLock() throws Exception {
    // Tests that if FSDir is opened w/ a NoLockFactory (or SingleInstanceLF),
    // then IndexWriter ctor succeeds. Previously (LUCENE-2386) it failed
    // when listAll() was called in IndexFileDeleter.
    Directory dir = newFSDirectory(createTempDir("emptyFSDirNoLock"), NoLockFactory.INSTANCE);
    new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))).close();
    dir.close();
  }

  public void testEmptyDirRollback() throws Exception {
    // TODO: generalize this test
    assumeFalse("test makes assumptions about file counts", Codec.getDefault() instanceof SimpleTextCodec);
    // Tests that if IW is created over an empty Directory, some documents are
    // indexed, flushed (but not committed) and then IW rolls back, then no
    // files are left in the Directory.
    Directory dir = newDirectory();
    
    String[] origFiles = dir.listAll();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
                                                .setMaxBufferedDocs(2)
                                                .setMergePolicy(newLogMergePolicy())
                                                .setUseCompoundFile(false));
    String[] files = dir.listAll();

    // Creating over empty dir should not create any files,
    // or, at most the write.lock file
    final int extraFileCount = files.length - origFiles.length;
    if (extraFileCount == 1) {
      assertTrue(Arrays.asList(files).contains(IndexWriter.WRITE_LOCK_NAME));
    } else {
      Arrays.sort(origFiles);
      Arrays.sort(files);
      assertArrayEquals(origFiles, files);
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
      if (IndexWriter.WRITE_LOCK_NAME.equals(file) || 
          file.startsWith(IndexFileNames.SEGMENTS) || 
          IndexFileNames.CODEC_FILE_PATTERN.matcher(file).matches()) {
        if (file.lastIndexOf('.') < 0
            // don't count stored fields and term vectors in
            || !Arrays.asList("fdx", "fdt", "tvx", "tvd", "tvf").contains(file.substring(file.lastIndexOf('.') + 1))) {
          ++computedExtraFileCount;
        }
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
    String allFiles[] = dir.listAll();
    assertEquals("no files should exist in the directory after rollback", origFiles.length + extraFileCount, allFiles.length);

    // Since we rolled-back above, that close should be a no-op
    writer.close();
    allFiles = dir.listAll();
    assertEquals("expected a no-op close after IW.rollback()", origFiles.length + extraFileCount, allFiles.length);
    dir.close();
  }

  public void testNoUnwantedTVFiles() throws Exception {

    Directory dir = newDirectory();
    IndexWriter indexWriter = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
                                                     .setRAMBufferSizeMB(0.01)
                                                     .setMergePolicy(newLogMergePolicy()));
    indexWriter.getConfig().getMergePolicy().setNoCFSRatio(0.0);

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

    TestUtil.checkIndex(dir);

    assertNoUnreferencedFiles(dir, "no tv files");
    DirectoryReader r0 = DirectoryReader.open(dir);
    for (LeafReaderContext ctx : r0.leaves()) {
      SegmentReader sr = (SegmentReader) ctx.reader();
      assertFalse(sr.getFieldInfos().hasVectors());
    }
    
    r0.close();
    dir.close();
  }

  static final class StringSplitAnalyzer extends Analyzer {
    @Override
    public TokenStreamComponents createComponents(String fieldName) {
      return new TokenStreamComponents(new StringSplitTokenizer());
    }
  }

  private static class StringSplitTokenizer extends Tokenizer {
    private String[] tokens;
    private int upto;
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

    public StringSplitTokenizer() {
      super();
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
      super.reset();
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
    Document hugeDoc = new Document();
    final String bigTerm = new String(chars);

    // This contents produces a too-long term:
    String contents = "abc xyz x" + bigTerm + " another term";
    hugeDoc.add(new TextField("content", contents, Field.Store.NO));
    expectThrows(IllegalArgumentException.class, () -> {
      w.addDocument(hugeDoc);
    });

    // Make sure we can add another normal document
    Document doc = new Document();
    doc.add(new TextField("content", "abc bbb ccc", Field.Store.NO));
    w.addDocument(doc);

    // So we remove the deleted doc:
    w.forceMerge(1);

    IndexReader reader = w.getReader();
    w.close();

    // Make sure all terms < max size were indexed
    assertEquals(1, reader.docFreq(new Term("content", "abc")));
    assertEquals(1, reader.docFreq(new Term("content", "bbb")));
    assertEquals(0, reader.docFreq(new Term("content", "term")));

    // Make sure the doc that has the massive term is NOT in
    // the index:
    assertEquals("document with wicked long term is in the index!", 1, reader.numDocs());

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

    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setCodec(TestUtil.getDefaultCodec());

    RandomIndexWriter w2 = new RandomIndexWriter(random(), dir, iwc);

    contentField.setStringValue("other");
    w2.addDocument(doc);

    contentField.setStringValue("term");
    w2.addDocument(doc);

    contentField.setStringValue(bigTerm);
    w2.addDocument(doc);

    contentField.setStringValue("zzz");
    w2.addDocument(doc);

    reader = w2.getReader();
    w2.close();
    assertEquals(1, reader.docFreq(new Term("content", bigTerm)));

    reader.close();
    dir.close();
  }

  public void testDeleteAllNRTLeftoverFiles() throws Exception {

    MockDirectoryWrapper d = new MockDirectoryWrapper(random(), new ByteBuffersDirectory());
    IndexWriter w = new IndexWriter(d, new IndexWriterConfig(new MockAnalyzer(random())));
    Document doc = new Document();
    for(int i = 0; i < 20; i++) {
      for(int j = 0; j < 100; ++j) {
        w.addDocument(doc);
      }
      w.commit();
      DirectoryReader.open(w).close();

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
    Directory d = new MockDirectoryWrapper(random(), new ByteBuffersDirectory());
    IndexWriter w = new IndexWriter(d, new IndexWriterConfig(new MockAnalyzer(random())));
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
    // Must use SimpleFSLockFactory... NativeFSLockFactory
    // somehow "knows" a lock is held against write.lock
    // even if you remove that file:
    Directory d = newFSDirectory(createTempDir("TestIndexWriter.testWhetherDeleteAllDeletesWriteLock"), SimpleFSLockFactory.INSTANCE);
    RandomIndexWriter w1 = new RandomIndexWriter(random(), d);
    w1.deleteAll();
    expectThrows(LockObtainFailedException.class, () -> {
      new RandomIndexWriter(random(), d, newIndexWriterConfig(null));
    });

    w1.close();
    d.close();
  }

  public void testOnlyUpdateDocuments() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir,
                                    new IndexWriterConfig(new MockAnalyzer(random())));

    final List<Document> docs = new ArrayList<>();
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
                                    new IndexWriterConfig(new MockAnalyzer(random())));

    w.prepareCommit();
    expectThrows(IllegalStateException.class, () -> {
      w.close();
    });
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
                                    new IndexWriterConfig(new MockAnalyzer(random())));

    w.prepareCommit();
    w.rollback();
    assertFalse(DirectoryReader.indexExists(dir));
    dir.close();
  }

  // LUCENE-3872
  public void testPrepareCommitThenRollback2() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir,
                                    new IndexWriterConfig(new MockAnalyzer(random())));

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
      protected TokenStreamComponents createComponents(String fieldName) {
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
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(analyzer));
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
        newIndexWriterConfig(new MockAnalyzer(random())));
    iw.addDocument(new Document());
    iw.close();
    try {
      // Create my own random file:
      IndexOutput out = dir.createOutput("myrandomfile", newIOContext(random()));
      out.writeByte((byte) 42);
      out.close();
      
      new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))).close();
      
      assertTrue(slowFileExists(dir, "myrandomfile"));
    } finally {
      dir.close();
    }
  }
  
  // LUCENE-3849
  public void testStopwordsPosIncHole() throws Exception {
    Directory dir = newDirectory();
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new MockTokenizer();
        TokenStream stream = new MockTokenFilter(tokenizer, MockTokenFilter.ENGLISH_STOPSET);
        return new TokenStreamComponents(tokenizer, stream);
      }
    };
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, a);
    Document doc = new Document();
    doc.add(new TextField("body", "just a", Field.Store.NO));
    doc.add(new TextField("body", "test of gaps", Field.Store.NO));
    iw.addDocument(doc);
    IndexReader ir = iw.getReader();
    iw.close();
    IndexSearcher is = newSearcher(ir);
    PhraseQuery.Builder builder = new PhraseQuery.Builder();
    builder.add(new Term("body", "just"), 0);
    builder.add(new Term("body", "test"), 2);
    PhraseQuery pq = builder.build();
    // body:"just ? test"
    assertEquals(1, is.search(pq, 5).totalHits.value);
    ir.close();
    dir.close();
  }
  
  // LUCENE-3849
  public void testStopwordsPosIncHole2() throws Exception {
    // use two stopfilters for testing here
    Directory dir = newDirectory();
    final Automaton secondSet = Automata.makeString("foobar");
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new MockTokenizer();
        TokenStream stream = new MockTokenFilter(tokenizer, MockTokenFilter.ENGLISH_STOPSET);
        stream = new MockTokenFilter(stream, new CharacterRunAutomaton(secondSet));
        return new TokenStreamComponents(tokenizer, stream);
      }
    };
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, a);
    Document doc = new Document();
    doc.add(new TextField("body", "just a foobar", Field.Store.NO));
    doc.add(new TextField("body", "test of gaps", Field.Store.NO));
    iw.addDocument(doc);
    IndexReader ir = iw.getReader();
    iw.close();
    IndexSearcher is = newSearcher(ir);
    PhraseQuery.Builder builder = new PhraseQuery.Builder();
    builder.add(new Term("body", "just"), 0);
    builder.add(new Term("body", "test"), 3);
    PhraseQuery pq = builder.build();
    // body:"just ? ? test"
    assertEquals(1, is.search(pq, 5).totalHits.value);
    ir.close();
    dir.close();
  }
  
  // LUCENE-4575
  public void testCommitWithUserDataOnly() throws Exception {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(null));
    writer.commit(); // first commit to complete IW create transaction.
    
    // this should store the commit data, even though no other changes were made
    writer.setLiveCommitData(new HashMap<String,String>() {{
      put("key", "value");
    }}.entrySet());
    writer.commit();
    
    DirectoryReader r = DirectoryReader.open(dir);
    assertEquals("value", r.getIndexCommit().getUserData().get("key"));
    r.close();
    
    // now check setCommitData and prepareCommit/commit sequence
    writer.setLiveCommitData(new HashMap<String,String>() {{
      put("key", "value1");
    }}.entrySet());
    writer.prepareCommit();
    writer.setLiveCommitData(new HashMap<String,String>() {{
      put("key", "value2");
    }}.entrySet());
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

  private Map<String,String> getLiveCommitData(IndexWriter writer) {
    Map<String,String> data = new HashMap<>();
    Iterable<Map.Entry<String,String>> iter = writer.getLiveCommitData();
    if (iter != null) {
      for(Map.Entry<String,String> ent : iter) {
        data.put(ent.getKey(), ent.getValue());
      }
    }
    return data;
  }
  
  @Test
  public void testGetCommitData() throws Exception {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(null));
    writer.setLiveCommitData(new HashMap<String,String>() {{
      put("key", "value");
    }}.entrySet());
    assertEquals("value", getLiveCommitData(writer).get("key"));
    writer.close();
    
    // validate that it's also visible when opening a new IndexWriter
    writer = new IndexWriter(dir, newIndexWriterConfig(null)
                                    .setOpenMode(OpenMode.APPEND));
    assertEquals("value", getLiveCommitData(writer).get("key"));
    writer.close();
    
    dir.close();
  }
  
  public void testNullAnalyzer() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwConf = newIndexWriterConfig(null);
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwConf);

    // add 3 good docs
    for (int i = 0; i < 3; i++) {
      Document doc = new Document();
      doc.add(new StringField("id", Integer.toString(i), Field.Store.NO));
      iw.addDocument(doc);
    }

    // add broken doc
    expectThrows(NullPointerException.class, () -> {
      Document broke = new Document();
      broke.add(newTextField("test", "broken", Field.Store.NO));
      iw.addDocument(broke);
    });

    // ensure good docs are still ok
    IndexReader ir = iw.getReader();
    assertEquals(3, ir.numDocs());
    ir.close();
    iw.close();
    dir.close();
  }
  
  public void testNullDocument() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);

    // add 3 good docs
    for (int i = 0; i < 3; i++) {
      Document doc = new Document();
      doc.add(new StringField("id", Integer.toString(i), Field.Store.NO));
      iw.addDocument(doc);
    }

    // add broken doc
    expectThrows(NullPointerException.class, () -> {
      iw.addDocument(null);
    });

    // ensure good docs are still ok
    IndexReader ir = iw.getReader();
    assertEquals(3, ir.numDocs());

    ir.close();
    iw.close();
    dir.close();
  }
  
  public void testNullDocuments() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);

    // add 3 good docs
    for (int i = 0; i < 3; i++) {
      Document doc = new Document();
      doc.add(new StringField("id", Integer.toString(i), Field.Store.NO));
      iw.addDocument(doc);
    }

    // add broken doc block
    expectThrows(NullPointerException.class, () -> {
      iw.addDocuments(null);
    });

    // ensure good docs are still ok
    IndexReader ir = iw.getReader();
    assertEquals(3, ir.numDocs());

    ir.close();
    iw.close();
    dir.close();
  }
  
  public void testIterableFieldThrowsException() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    int iters = atLeast(100);
    int docCount = 0;
    int docId = 0;
    Set<String> liveIds = new HashSet<>();
    for (int i = 0; i < iters; i++) {      
      int numDocs = atLeast(4);
      for (int j = 0; j < numDocs; j++) {
        String id = Integer.toString(docId++);
        final List<IndexableField> fields = new ArrayList<>();
        fields.add(new StringField("id", id, Field.Store.YES));
        fields.add(new StringField("foo", TestUtil.randomSimpleString(random()), Field.Store.NO));
        docId++;
        
        boolean success = false;
        try {
          w.addDocument(new RandomFailingIterable<IndexableField>(fields, random()));
          success = true;
        } catch (RuntimeException e) {
          assertEquals("boom", e.getMessage());
        } finally {
          if (success) {
            docCount++;
            liveIds.add(id);
          }
        }
      }
    }
    DirectoryReader reader = w.getReader();
    assertEquals(docCount, reader.numDocs());
    List<LeafReaderContext> leaves = reader.leaves();
    for (LeafReaderContext leafReaderContext : leaves) {
      LeafReader ar = leafReaderContext.reader();
      Bits liveDocs = ar.getLiveDocs();
      int maxDoc = ar.maxDoc();
      for (int i = 0; i < maxDoc; i++) {
        if (liveDocs == null || liveDocs.get(i)) {
          assertTrue(liveIds.remove(ar.document(i).get("id")));
        }
      }
    }
    assertTrue(liveIds.isEmpty());
    w.close();
    IOUtils.close(reader, dir);
  }
  
  public void testIterableThrowsException() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    int iters = atLeast(100);
    int docCount = 0;
    int docId = 0;
    Set<String> liveIds = new HashSet<>();
    for (int i = 0; i < iters; i++) {
      int numDocs = atLeast(4);
      for (int j = 0; j < numDocs; j++) {
        String id = Integer.toString(docId++);
        final List<IndexableField> fields = new ArrayList<>();
        fields.add(new StringField("id", id, Field.Store.YES));
        fields.add(new StringField("foo", TestUtil.randomSimpleString(random()), Field.Store.NO));
        docId++;

        boolean success = false;
        try {
          w.addDocument(new RandomFailingIterable<IndexableField>(fields, random()));
          success = true;
        } catch (RuntimeException e) {
          assertEquals("boom", e.getMessage());
        } finally {
          if (success) {
            docCount++;
            liveIds.add(id);
          }
        }
      }
    }
    DirectoryReader reader = w.getReader();
    assertEquals(docCount, reader.numDocs());
    List<LeafReaderContext> leaves = reader.leaves();
    for (LeafReaderContext leafReaderContext : leaves) {
      LeafReader ar = leafReaderContext.reader();
      Bits liveDocs = ar.getLiveDocs();
      int maxDoc = ar.maxDoc();
      for (int i = 0; i < maxDoc; i++) {
        if (liveDocs == null || liveDocs.get(i)) {
          assertTrue(liveIds.remove(ar.document(i).get("id")));
        }
      }
    }
    assertTrue(liveIds.isEmpty());
    w.close();
    IOUtils.close(reader, dir);
  }
  
  public void testIterableThrowsException2() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    Exception expected = expectThrows(Exception.class, () -> {
      w.addDocuments(new Iterable<Document>() {
        @Override
        public Iterator<Document> iterator() {
          return new Iterator<Document>() {

            @Override
            public boolean hasNext() {
              return true;
            }

            @Override
            public Document next() {
              throw new RuntimeException("boom");
            }

            @Override
            public void remove() { assert false; }
          };
        }
      });
    });
    assertEquals("boom", expected.getMessage());

    w.close();
    IOUtils.close(dir);
  }

  private static class RandomFailingIterable<T> implements Iterable<T> {
    private final Iterable<? extends T> list;
    private final int failOn;

    public RandomFailingIterable(Iterable<? extends T> list, Random random) {
      this.list = list;
      this.failOn = random.nextInt(5);
    }
    
    @Override
    public Iterator<T> iterator() {
      final Iterator<? extends T> docIter = list.iterator();
      return new Iterator<T>() {
        int count = 0;

        @Override
        public boolean hasNext() {
          return docIter.hasNext();
        }

        @Override
        public T next() {
          if (count == failOn) {
            throw new RuntimeException("boom");
          }
          count++;
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

      // Create a corrupt first commit:
      dir.createOutput(IndexFileNames.fileNameFromGeneration(IndexFileNames.PENDING_SEGMENTS,
                                                             "",
                                                             0), IOContext.DEFAULT).close();

      IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
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

  public void testHasUncommittedChanges() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    assertTrue(writer.hasUncommittedChanges());  // this will be true because a commit will create an empty index
    Document doc = new Document();
    doc.add(newTextField("myfield", "a b c", Field.Store.NO));
    writer.addDocument(doc);
    assertTrue(writer.hasUncommittedChanges());

    // Must commit, waitForMerges, commit again, to be
    // certain that hasUncommittedChanges returns false:
    writer.commit();
    writer.waitForMerges();
    writer.commit();
    assertFalse(writer.hasUncommittedChanges());
    writer.addDocument(doc);
    assertTrue(writer.hasUncommittedChanges());
    writer.commit();
    doc = new Document();
    doc.add(newStringField("id", "xyz", Field.Store.YES));
    writer.addDocument(doc);
    assertTrue(writer.hasUncommittedChanges());

    // Must commit, waitForMerges, commit again, to be
    // certain that hasUncommittedChanges returns false:
    writer.commit();
    writer.waitForMerges();
    writer.commit();
    assertFalse(writer.hasUncommittedChanges());
    writer.deleteDocuments(new Term("id", "xyz"));
    assertTrue(writer.hasUncommittedChanges());

    // Must commit, waitForMerges, commit again, to be
    // certain that hasUncommittedChanges returns false:
    writer.commit();
    writer.waitForMerges();
    writer.commit();
    assertFalse(writer.hasUncommittedChanges());
    writer.close();

    writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    assertFalse(writer.hasUncommittedChanges());
    writer.addDocument(doc);
    assertTrue(writer.hasUncommittedChanges());

    writer.close();
    dir.close();
  }
  
  public void testMergeAllDeleted() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    AtomicBoolean keepFullyDeletedSegments = new AtomicBoolean();
    iwc.setMergePolicy(new FilterMergePolicy(iwc.getMergePolicy()) {
      @Override
      public boolean keepFullyDeletedSegment(IOSupplier<CodecReader> readerIOSupplier) throws IOException {
        return keepFullyDeletedSegments.get();
      }
    });
    final SetOnce<IndexWriter> iwRef = new SetOnce<>();
    IndexWriter evilWriter = RandomIndexWriter.mockIndexWriter(random(), dir, iwc, new RandomIndexWriter.TestPoint() {
      @Override
      public void apply(String message) {
        if ("startCommitMerge".equals(message)) {
          keepFullyDeletedSegments.set(false);
        } else if ("startMergeInit".equals(message)) {
          keepFullyDeletedSegments.set(true);
        }
      }
    });
    iwRef.set(evilWriter);
    for (int i = 0; i < 1000; i++) {
      addDoc(evilWriter);
      if (random().nextInt(17) == 0) {
        evilWriter.commit();
      }
    }
    evilWriter.deleteDocuments(new MatchAllDocsQuery());
    evilWriter.forceMerge(1);
    evilWriter.close();
    dir.close();
  }

  // LUCENE-5239
  public void testDeleteSameTermAcrossFields() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new TextField("a", "foo", Field.Store.NO));
    w.addDocument(doc);

    // Should not delete the document; with LUCENE-5239 the
    // "foo" from the 2nd delete term would incorrectly
    // match field a's "foo":
    w.deleteDocuments(new Term("a", "xxx"));
    w.deleteDocuments(new Term("b", "foo"));
    IndexReader r = w.getReader();
    w.close();

    // Make sure document was not (incorrectly) deleted:
    assertEquals(1, r.numDocs());
    r.close();
    dir.close();
  }

  public void testHasUncommittedChangesAfterException() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    Directory directory = newDirectory();
    // we don't use RandomIndexWriter because it might add more docvalues than we expect !!!!
    IndexWriterConfig iwc = newIndexWriterConfig(analyzer);
    iwc.setMergePolicy(newLogMergePolicy());
    IndexWriter iwriter = new IndexWriter(directory, iwc);
    Document doc = new Document();
    doc.add(new SortedDocValuesField("dv", new BytesRef("foo!")));
    doc.add(new SortedDocValuesField("dv", new BytesRef("bar!")));
    expectThrows(IllegalArgumentException.class, () -> {
      iwriter.addDocument(doc);
    });

    iwriter.commit();
    assertFalse(iwriter.hasUncommittedChanges());
    iwriter.close();
    directory.close();
  }

  public void testDoubleClose() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    Document doc = new Document();
    doc.add(new SortedDocValuesField("dv", new BytesRef("foo!")));
    w.addDocument(doc);
    w.close();
    // Close again should have no effect
    w.close();
    dir.close();
  }

  public void testRollbackThenClose() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    Document doc = new Document();
    doc.add(new SortedDocValuesField("dv", new BytesRef("foo!")));
    w.addDocument(doc);
    w.rollback();
    // Close after rollback should have no effect
    w.close();
    dir.close();
  }

  public void testCloseThenRollback() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    Document doc = new Document();
    doc.add(new SortedDocValuesField("dv", new BytesRef("foo!")));
    w.addDocument(doc);
    w.close();
    // Rollback after close should have no effect
    w.rollback();
    dir.close();
  }

  public void testCloseWhileMergeIsRunning() throws IOException {
    Directory dir = newDirectory();

    final CountDownLatch mergeStarted = new CountDownLatch(1);
    final CountDownLatch closeStarted = new CountDownLatch(1);

    IndexWriterConfig iwc = newIndexWriterConfig(random(), new MockAnalyzer(random())).setCommitOnClose(false);
    LogDocMergePolicy mp = new LogDocMergePolicy();
    mp.setMergeFactor(2);
    iwc.setMergePolicy(mp);
    iwc.setInfoStream(new InfoStream() {
        @Override
        public boolean isEnabled(String component) {
          return true;
        }

        @Override
        public void message(String component, String message) {
          if (message.equals("rollback")) {
            closeStarted.countDown();
          }
        }

        @Override
        public void close() {
        }
      });

    iwc.setMergeScheduler(new ConcurrentMergeScheduler() {
        @Override
        public void doMerge(IndexWriter writer, MergePolicy.OneMerge merge) throws IOException {
          mergeStarted.countDown();
          try {
            closeStarted.await();
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(ie);
          }
          super.doMerge(writer, merge);
        }

        @Override
        public void close() {
        }
      });
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new SortedDocValuesField("dv", new BytesRef("foo!")));
    w.addDocument(doc);
    w.commit();
    w.addDocument(doc);
    w.commit();
    w.close();
    dir.close();
  }

  /** Make sure that close waits for any still-running commits. */
  public void testCloseDuringCommit() throws Exception {

    final CountDownLatch startCommit = new CountDownLatch(1);
    final CountDownLatch finishCommit = new CountDownLatch(1);

    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(null);
    // use an infostream that "takes a long time" to commit
    final IndexWriter iw = RandomIndexWriter.mockIndexWriter(random(), dir, iwc, new RandomIndexWriter.TestPoint() {
      @Override
      public void apply(String message) {
        if (message.equals("finishStartCommit")) {
          startCommit.countDown();
          try {
            Thread.sleep(10);
          } catch (InterruptedException ie) {
            throw new ThreadInterruptedException(ie);
          }
        }
      }
    });
    new Thread() {
      @Override
      public void run() {
        try {
          iw.commit();
          finishCommit.countDown();
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }
      }
    }.start();
    startCommit.await();
    try {
      iw.close();
    } catch (IllegalStateException ise) {
      // OK, but not required (depends on thread scheduling)
    }
    finishCommit.await();
    iw.close();
    dir.close();
  }

  // LUCENE-5895:

  /** Make sure we see ids per segment and per commit. */
  public void testIds() throws Exception {
    Directory d = newDirectory();
    IndexWriter w = new IndexWriter(d, newIndexWriterConfig(new MockAnalyzer(random())));
    w.addDocument(new Document());
    w.close();
    
    SegmentInfos sis = SegmentInfos.readLatestCommit(d);
    byte[] id1 = sis.getId();
    assertNotNull(id1);
    assertEquals(StringHelper.ID_LENGTH, id1.length);
    
    byte[] id2 = sis.info(0).info.getId();
    assertNotNull(id2);
    assertEquals(StringHelper.ID_LENGTH, id2.length);

    // Make sure CheckIndex includes id output:
    ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
    CheckIndex checker = new CheckIndex(d);
    checker.setDoSlowChecks(false);
    checker.setInfoStream(new PrintStream(bos, false, IOUtils.UTF_8), false);
    CheckIndex.Status indexStatus = checker.checkIndex(null);
    String s = bos.toString(IOUtils.UTF_8);
    checker.close();
    // Make sure CheckIndex didn't fail
    assertTrue(s, indexStatus != null && indexStatus.clean);

    // Commit id is always stored:
    assertTrue("missing id=" + StringHelper.idToString(id1) + " in:\n" + s, s.contains("id=" + StringHelper.idToString(id1)));

    assertTrue("missing id=" + StringHelper.idToString(id1) + " in:\n" + s, s.contains("id=" + StringHelper.idToString(id1)));
    d.close();

    Set<String> ids = new HashSet<>();
    for(int i=0;i<100000;i++) {
      String id = StringHelper.idToString(StringHelper.randomId());
      assertFalse("id=" + id + " i=" + i, ids.contains(id));
      ids.add(id);
    }
  }
  
  public void testEmptyNorm() throws Exception {
    Directory d = newDirectory();
    IndexWriter w = new IndexWriter(d, newIndexWriterConfig(new MockAnalyzer(random())));
    Document doc = new Document();
    doc.add(new TextField("foo", new CannedTokenStream()));
    w.addDocument(doc);
    w.commit();
    w.close();
    DirectoryReader r = DirectoryReader.open(d);
    NumericDocValues norms = getOnlyLeafReader(r).getNormValues("foo");
    assertEquals(0, norms.nextDoc());
    assertEquals(0, norms.longValue());
    r.close();
    d.close();
  }

  public void testManySeparateThreads() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setMaxBufferedDocs(1000);
    final IndexWriter w = new IndexWriter(dir, iwc);
    // Index 100 docs, each from a new thread, but always only 1 thread is in IW at once:
    for(int i=0;i<100;i++) {
      Thread thread = new Thread() {
        @Override
        public void run() {
          Document doc = new Document();
          doc.add(newStringField("foo", "bar", Field.Store.NO));
          try {
            w.addDocument(doc);
          } catch (IOException ioe) {
            throw new RuntimeException(ioe);
          }
        }
        };
      thread.start();
      thread.join();
    }
    w.close();

    IndexReader r = DirectoryReader.open(dir);
    assertEquals(1, r.leaves().size());
    r.close();
    dir.close();
  }

  // LUCENE-6505
  public void testNRTSegmentsFile() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    // creates segments_1
    w.commit();

    // newly opened NRT reader should see gen=1 segments file
    DirectoryReader r = DirectoryReader.open(w);
    assertEquals(1, r.getIndexCommit().getGeneration());
    assertEquals("segments_1", r.getIndexCommit().getSegmentsFileName());

    // newly opened non-NRT reader should see gen=1 segments file
    DirectoryReader r2 = DirectoryReader.open(dir);
    assertEquals(1, r2.getIndexCommit().getGeneration());
    assertEquals("segments_1", r2.getIndexCommit().getSegmentsFileName());
    r2.close();
    
    // make a change and another commit
    w.addDocument(new Document());
    w.commit();
    DirectoryReader r3 = DirectoryReader.openIfChanged(r);
    r.close();
    assertNotNull(r3);

    // reopened NRT reader should see gen=2 segments file
    assertEquals(2, r3.getIndexCommit().getGeneration());
    assertEquals("segments_2", r3.getIndexCommit().getSegmentsFileName());
    r3.close();

    // newly opened non-NRT reader should see gen=2 segments file
    DirectoryReader r4 = DirectoryReader.open(dir);
    assertEquals(2, r4.getIndexCommit().getGeneration());
    assertEquals("segments_2", r4.getIndexCommit().getSegmentsFileName());
    r4.close();

    w.close();
    dir.close();
  }

  // LUCENE-6505
  public void testNRTAfterCommit() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    w.commit();

    w.addDocument(new Document());
    DirectoryReader r = DirectoryReader.open(w);
    w.commit();

    // commit even with no other changes counts as a "change" that NRT reader reopen will see:
    DirectoryReader r2 = DirectoryReader.open(dir);
    assertNotNull(r2);
    assertEquals(2, r2.getIndexCommit().getGeneration());
    assertEquals("segments_2", r2.getIndexCommit().getSegmentsFileName());

    IOUtils.close(r, r2, w, dir);
  }

  // LUCENE-6505
  public void testNRTAfterSetUserDataWithoutCommit() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    w.commit();

    DirectoryReader r = DirectoryReader.open(w);
    Map<String,String> m = new HashMap<>();
    m.put("foo", "bar");
    w.setLiveCommitData(m.entrySet());

    // setLiveCommitData with no other changes should count as an NRT change:
    DirectoryReader r2 = DirectoryReader.openIfChanged(r);
    assertNotNull(r2);

    IOUtils.close(r2, r, w, dir);
  }

  // LUCENE-6505
  public void testNRTAfterSetUserDataWithCommit() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    w.commit();

    DirectoryReader r = DirectoryReader.open(w);
    Map<String,String> m = new HashMap<>();
    m.put("foo", "bar");
    w.setLiveCommitData(m.entrySet());
    w.commit();
    // setLiveCommitData and also commit, with no other changes, should count as an NRT change:
    DirectoryReader r2 = DirectoryReader.openIfChanged(r);
    assertNotNull(r2);
    IOUtils.close(r, r2, w, dir);
  }

  // LUCENE-6523
  public void testCommitImmediatelyAfterNRTReopen() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    w.commit();

    w.addDocument(new Document());

    DirectoryReader r = DirectoryReader.open(w);
    w.commit();

    assertFalse(r.isCurrent());

    DirectoryReader r2 = DirectoryReader.openIfChanged(r);
    assertNotNull(r2);
    // segments_N should have changed:
    assertFalse(r2.getIndexCommit().getSegmentsFileName().equals(r.getIndexCommit().getSegmentsFileName()));
    IOUtils.close(r, r2, w, dir);
  }

  public void testPendingDeleteDVGeneration() throws IOException {
    // irony: currently we don't emulate windows well enough to work on windows!
    assumeFalse("windows is not supported", Constants.WINDOWS);

    Path path = createTempDir();

    // Use WindowsFS to prevent open files from being deleted:
    FileSystem fs = new WindowsFS(path.getFileSystem()).getFileSystem(URI.create("file:///"));
    Path root = new FilterPath(path, fs);

    // MMapDirectory doesn't work because it closes its file handles after mapping!
    List<Closeable> toClose = new ArrayList<>();
    try (FSDirectory dir = new SimpleFSDirectory(root);
         Closeable closeable = () -> IOUtils.close(toClose)) {
      IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()))
          .setUseCompoundFile(false)
          .setMergePolicy(NoMergePolicy.INSTANCE) // avoid merging away the randomFile
          .setMaxBufferedDocs(2)
          .setRAMBufferSizeMB(-1);
      IndexWriter w = new IndexWriter(dir, iwc);
      Document d = new Document();
      d.add(new StringField("id", "1", Field.Store.YES));
      d.add(new NumericDocValuesField("id", 1));
      w.addDocument(d);
      d = new Document();
      d.add(new StringField("id", "2", Field.Store.YES));
      d.add(new NumericDocValuesField("id", 2));
      w.addDocument(d);
      w.flush();
      d = new Document();
      d.add(new StringField("id", "1", Field.Store.YES));
      d.add(new NumericDocValuesField("id", 1));
      w.updateDocument(new Term("id", "1"), d);
      w.commit();
      Set<String> files = new HashSet<>(Arrays.asList(dir.listAll()));
      int numIters = 10 + random().nextInt(50);
      for (int i = 0; i < numIters; i++) {
        if (random().nextBoolean()) {
          d = new Document();
          d.add(new StringField("id", "1", Field.Store.YES));
          d.add(new NumericDocValuesField("id", 1));
          w.updateDocument(new Term("id", "1"), d);
        } else if (random().nextBoolean()) {
          w.deleteDocuments(new Term("id", "2"));
        } else {
          w.updateNumericDocValue(new Term("id", "1"), "id", 2);
        }
        w.prepareCommit();
        List<String> newFiles = new ArrayList<>(Arrays.asList(dir.listAll()));
        newFiles.removeAll(files);
        String randomFile = RandomPicks.randomFrom(random(), newFiles);
        toClose.add(dir.openInput(randomFile, IOContext.DEFAULT));
        w.rollback();
        iwc = new IndexWriterConfig(new MockAnalyzer(random()))
            .setUseCompoundFile(false)
            .setMergePolicy(NoMergePolicy.INSTANCE)
            .setMaxBufferedDocs(2)
            .setRAMBufferSizeMB(-1);
        w = new IndexWriter(dir, iwc);
        expectThrows(NoSuchFileException.class, () -> {
          dir.deleteFile(randomFile);
        });
      }
      w.close();
    }

  }

  public void testPendingDeletionsRollbackWithReader() throws IOException {
    // irony: currently we don't emulate windows well enough to work on windows!
    assumeFalse("windows is not supported", Constants.WINDOWS);

    Path path = createTempDir();

    // Use WindowsFS to prevent open files from being deleted:
    FileSystem fs = new WindowsFS(path.getFileSystem()).getFileSystem(URI.create("file:///"));
    Path root = new FilterPath(path, fs);
    try (FSDirectory _dir = new SimpleFSDirectory(root)) {
      Directory dir = new FilterDirectory(_dir) {};

      IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
      IndexWriter w = new IndexWriter(dir, iwc);
      Document d = new Document();
      d.add(new StringField("id", "1", Field.Store.YES));
      d.add(new NumericDocValuesField("numval", 1));
      w.addDocument(d);
      w.commit();
      w.addDocument(d);
      w.flush();
      DirectoryReader reader = DirectoryReader.open(w);
      w.rollback();

      // try-delete superfluous files (some will fail due to windows-fs)
      IndexWriterConfig iwc2 = new IndexWriterConfig(new MockAnalyzer(random()));
      new IndexWriter(dir, iwc2).close();

      // test that we can index on top of pending deletions
      IndexWriterConfig iwc3 = new IndexWriterConfig(new MockAnalyzer(random()));
      w = new IndexWriter(dir, iwc3);
      w.addDocument(d);
      w.commit();

      reader.close();
      w.close();
    }
  }

  public void testWithPendingDeletions() throws Exception {
    // irony: currently we don't emulate windows well enough to work on windows!
    assumeFalse("windows is not supported", Constants.WINDOWS);

    Path path = createTempDir();

    // Use WindowsFS to prevent open files from being deleted:
    FileSystem fs = new WindowsFS(path.getFileSystem()).getFileSystem(URI.create("file:///"));
    Path root = new FilterPath(path, fs);
    IndexCommit indexCommit;
    DirectoryReader reader;
    // MMapDirectory doesn't work because it closes its file handles after mapping!
    try (FSDirectory dir = new SimpleFSDirectory(root)) {
      IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random())).setIndexDeletionPolicy(NoDeletionPolicy.INSTANCE);
      IndexWriter w = new IndexWriter(dir, iwc);
      w.commit();
      reader = w.getReader();
      // we pull this commit to open it again later to check that we fail if a future file delete is pending
      indexCommit = reader.getIndexCommit();
      w.close();
      w = new IndexWriter(dir, new IndexWriterConfig(new MockAnalyzer(random())).setIndexDeletionPolicy(NoDeletionPolicy.INSTANCE));
      w.addDocument(new Document());
      w.close();
      IndexInput in = dir.openInput("segments_2", IOContext.DEFAULT);
      dir.deleteFile("segments_2");
      assertTrue(dir.getPendingDeletions().size() > 0);

      // make sure we get NoSuchFileException if we try to delete and already-pending-delete file:
      expectThrows(NoSuchFileException.class, () -> {
        dir.deleteFile("segments_2");
      });

      try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(new MockAnalyzer(random())).setIndexCommit(indexCommit))) {
        writer.addDocument(new Document());
        writer.commit();
        assertEquals(1, writer.getDocStats().maxDoc);
        // now check that we moved to 3
        dir.openInput("segments_3", IOContext.READ).close();;
      }
      reader.close();
      in.close();
    }
  }

  public void testPendingDeletesAlreadyWrittenFiles() throws IOException {
    Path path = createTempDir();
    // irony: currently we don't emulate windows well enough to work on windows!
    assumeFalse("windows is not supported", Constants.WINDOWS);

    // Use WindowsFS to prevent open files from being deleted:
    FileSystem fs = new WindowsFS(path.getFileSystem()).getFileSystem(URI.create("file:///"));
    Path root = new FilterPath(path, fs);
    DirectoryReader reader;
    // MMapDirectory doesn't work because it closes its file handles after mapping!
    try (FSDirectory dir = new SimpleFSDirectory(root)) {
      IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
      IndexWriter w = new IndexWriter(dir, iwc);
      w.commit();
      IndexInput in = dir.openInput("segments_1", IOContext.DEFAULT);
      w.addDocument(new Document());
      w.close();

      assertTrue(dir.getPendingDeletions().size() > 0);

      // make sure we get NoSuchFileException if we try to delete and already-pending-delete file:
      expectThrows(NoSuchFileException.class, () -> {
        dir.deleteFile("segments_1");
      });
      new IndexWriter(dir, new IndexWriterConfig(new MockAnalyzer(random()))).close();
      in.close();
    }
  }

  public void testLeftoverTempFiles() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    w.close();
    
    IndexOutput out = dir.createTempOutput("_0", "bkd", IOContext.DEFAULT);
    String tempName = out.getName();
    out.close();
    iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    w = new IndexWriter(dir, iwc);

    // Make sure IW deleted the unref'd file:
    try {
      dir.openInput(tempName, IOContext.DEFAULT);
      fail("did not hit exception");
    } catch (FileNotFoundException | NoSuchFileException e) {
      // expected
    }
    w.close();
    dir.close();
  }

  @Ignore("requires running tests with biggish heap")
  public void testMassiveField() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    final IndexWriter w = new IndexWriter(dir, iwc);

    StringBuilder b = new StringBuilder();
    while (b.length() <= IndexWriter.MAX_STORED_STRING_LENGTH) {
      b.append("x ");
    }

    final Document doc = new Document();
    //doc.add(new TextField("big", b.toString(), Field.Store.YES));
    doc.add(new StoredField("big", b.toString()));
    Exception e = expectThrows(IllegalArgumentException.class, () -> {w.addDocument(doc);});
    assertEquals("stored field \"big\" is too large (" + b.length() + " characters) to store", e.getMessage());

    // make sure writer is still usable:
    Document doc2 = new Document();
    doc2.add(new StringField("id", "foo", Field.Store.YES));
    w.addDocument(doc2);

    DirectoryReader r = DirectoryReader.open(w);
    assertEquals(1, r.numDocs());
    r.close();
    w.close();
    dir.close();
  }

  public void testRecordsIndexCreatedVersion() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    w.commit();
    w.close();
    assertEquals(Version.LATEST.major, SegmentInfos.readLatestCommit(dir).getIndexCreatedVersionMajor());
    dir.close();
  }

  public void testFlushLargestWriter() throws IOException, InterruptedException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, new IndexWriterConfig());
    int numDocs = indexDocsForMultipleThreadStates(w);
    DocumentsWriterPerThreadPool.ThreadState largestNonPendingWriter
        = w.docWriter.flushControl.findLargestNonPendingWriter();
    assertFalse(largestNonPendingWriter.flushPending);
    assertNotNull(largestNonPendingWriter.dwpt);

    int numRamDocs = w.numRamDocs();
    int numDocsInDWPT = largestNonPendingWriter.dwpt.getNumDocsInRAM();
    assertTrue(w.flushNextBuffer());
    assertNull(largestNonPendingWriter.dwpt);
    assertEquals(numRamDocs-numDocsInDWPT, w.numRamDocs());

    // make sure it's not locked
    largestNonPendingWriter.lock();
    largestNonPendingWriter.unlock();
    if (random().nextBoolean()) {
      w.commit();
    }
    DirectoryReader reader = DirectoryReader.open(w, true, true);
    assertEquals(numDocs, reader.numDocs());
    reader.close();
    w.close();
    dir.close();
  }

  private int indexDocsForMultipleThreadStates(IndexWriter w) throws InterruptedException {
    Thread[] threads = new Thread[3];
    CountDownLatch latch = new CountDownLatch(threads.length);
    int numDocsPerThread = 10 + random().nextInt(30);
    // ensure we have more than on thread state
    for (int i = 0; i < threads.length; i++) {
      threads[i] = new Thread(() -> {
        latch.countDown();
        try {
          latch.await();
          for (int j = 0; j < numDocsPerThread; j++) {
            Document doc = new Document();
            doc.add(new StringField("id", "foo", Field.Store.YES));
            w.addDocument(doc);
          }
        } catch (Exception e) {
          throw new AssertionError(e);
        }
      });
      threads[i].start();
    }
    for (Thread t : threads) {
      t.join();
    }
    return numDocsPerThread * threads.length;
  }

  public void testNeverCheckOutOnFullFlush() throws IOException, InterruptedException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, new IndexWriterConfig());
    indexDocsForMultipleThreadStates(w);
    DocumentsWriterPerThreadPool.ThreadState largestNonPendingWriter
        = w.docWriter.flushControl.findLargestNonPendingWriter();
    assertFalse(largestNonPendingWriter.flushPending);
    assertNotNull(largestNonPendingWriter.dwpt);
    int activeThreadStateCount = w.docWriter.perThreadPool.getActiveThreadStateCount();
    w.docWriter.flushControl.markForFullFlush();
    DocumentsWriterPerThread documentsWriterPerThread = w.docWriter.flushControl.checkoutLargestNonPendingWriter();
    assertNull(documentsWriterPerThread);
    assertEquals(activeThreadStateCount, w.docWriter.flushControl.numQueuedFlushes());
    w.docWriter.flushControl.abortFullFlushes();
    assertNull("was aborted", w.docWriter.flushControl.checkoutLargestNonPendingWriter());
    assertEquals(0, w.docWriter.flushControl.numQueuedFlushes());
    w.close();
    dir.close();
  }

  public void testHoldLockOnLargestWriter() throws IOException, InterruptedException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, new IndexWriterConfig());
    int numDocs = indexDocsForMultipleThreadStates(w);
    DocumentsWriterPerThreadPool.ThreadState largestNonPendingWriter
        = w.docWriter.flushControl.findLargestNonPendingWriter();
    assertFalse(largestNonPendingWriter.flushPending);
    assertNotNull(largestNonPendingWriter.dwpt);

    CountDownLatch wait = new CountDownLatch(1);
    CountDownLatch locked = new CountDownLatch(1);
    Thread lockThread = new Thread(() -> {
      try {
        largestNonPendingWriter.lock();
        locked.countDown();
        wait.await();
      } catch (InterruptedException e) {
        throw new AssertionError(e);
      } finally {
        largestNonPendingWriter.unlock();
      }
    });
    lockThread.start();
    Thread flushThread = new Thread(() -> {
      try {
        locked.await();
        assertTrue(w.flushNextBuffer());
      } catch (Exception e) {
        throw new AssertionError(e);
      }
    });
    flushThread.start();

    locked.await();
    // access a synced method to ensure we never lock while we hold the flush control monitor
    w.docWriter.flushControl.activeBytes();
    wait.countDown();
    lockThread.join();
    flushThread.join();

    assertNull("largest DWPT should be flushed", largestNonPendingWriter.dwpt);
    // make sure it's not locked
    largestNonPendingWriter.lock();
    largestNonPendingWriter.unlock();
    if (random().nextBoolean()) {
      w.commit();
    }
    DirectoryReader reader = DirectoryReader.open(w, true, true);
    assertEquals(numDocs, reader.numDocs());
    reader.close();
    w.close();
    dir.close();
  }

  public void testCheckPendingFlushPostUpdate() throws IOException, InterruptedException {
    MockDirectoryWrapper dir = newMockDirectory();
    Set<String> flushingThreads = Collections.synchronizedSet(new HashSet<>());
    dir.failOn(new MockDirectoryWrapper.Failure() {
      @Override
      public void eval(MockDirectoryWrapper dir) throws IOException {
        StackTraceElement[] trace = new Exception().getStackTrace();
        for (int i = 0; i < trace.length; i++) {
          if ("flush".equals(trace[i].getMethodName())
              && "org.apache.lucene.index.DocumentsWriterPerThread".equals(trace[i].getClassName())) {
            flushingThreads.add(Thread.currentThread().getName());
            break;
          }
        }
      }
    });
    IndexWriter w = new IndexWriter(dir, new IndexWriterConfig()
        .setCheckPendingFlushUpdate(false)
        .setMaxBufferedDocs(Integer.MAX_VALUE)
        .setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH));
    AtomicBoolean done = new AtomicBoolean(false);
    int numThreads = 2 + random().nextInt(3);
    CountDownLatch latch = new CountDownLatch(numThreads);
    Set<String> indexingThreads = new HashSet<>();
    Thread[] threads = new Thread[numThreads];
    for (int i = 0; i < numThreads; i++) {
      threads[i] = new Thread(() -> {
        latch.countDown();
        int numDocs = 0;
        while (done.get() == false) {

          Document doc = new Document();
          doc.add(new StringField("id", "foo", Field.Store.YES));
          try {
            w.addDocument(doc);
          } catch (Exception e) {
            throw new AssertionError(e);
          }
          if (numDocs++ % 10 == 0) {
            Thread.yield();
          }
        }
      });
      indexingThreads.add(threads[i].getName());
      threads[i].start();
    }
    latch.await();
    try {
      int numIters = rarely() ? 1 + random().nextInt(5) : 1;
      for (int i = 0; i < numIters; i++) {
        waitForDocsInBuffers(w, Math.min(2, threads.length));
        w.commit();
        assertTrue(flushingThreads.toString(), flushingThreads.contains(Thread.currentThread().getName()));
        flushingThreads.retainAll(indexingThreads);
        assertTrue(flushingThreads.toString(), flushingThreads.isEmpty());
      }
      w.getConfig().setCheckPendingFlushUpdate(true);
      numIters = 0;
      while (true) {
        assertFalse("should finish in less than 100 iterations", numIters++ >= 100);
        waitForDocsInBuffers(w, Math.min(2, threads.length));
        w.flush();
        flushingThreads.retainAll(indexingThreads);
        if (flushingThreads.isEmpty() == false) {
          break;
        }
      }
    } finally {
      done.set(true);
      for (int i = 0; i < numThreads; i++) {
        threads[i].join();
      }
      IOUtils.close(w, dir);
    }
  }

  private static void waitForDocsInBuffers(IndexWriter w, int buffersWithDocs) {
    // wait until at least N threadstates have a doc in order to observe
    // who flushes the segments.
    while(true) {
      int numStatesWithDocs = 0;
      DocumentsWriterPerThreadPool perThreadPool = w.docWriter.perThreadPool;
      for (int i = 0; i < perThreadPool.getActiveThreadStateCount(); i++) {
        DocumentsWriterPerThreadPool.ThreadState threadState = perThreadPool.getThreadState(i);
        threadState.lock();
        try {
          DocumentsWriterPerThread dwpt = threadState.dwpt;
          if (dwpt != null && dwpt.getNumDocsInRAM() > 1) {
            numStatesWithDocs++;
          }
        } finally {
          threadState.unlock();
        }
      }
      if (numStatesWithDocs >= buffersWithDocs) {
        return;
      }
    }
  }

  public void testSoftUpdateDocuments() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig()
        .setMergePolicy(NoMergePolicy.INSTANCE)
        .setSoftDeletesField("soft_delete"));
    expectThrows(IllegalArgumentException.class, () -> {
      writer.softUpdateDocument(null, new Document(), new NumericDocValuesField("soft_delete", 1));
    });
    
    expectThrows(IllegalArgumentException.class, () -> {
      writer.softUpdateDocument(new Term("id", "1"), new Document());
    });

    expectThrows(IllegalArgumentException.class, () -> {
      writer.softUpdateDocuments(null, Arrays.asList(new Document()), new NumericDocValuesField("soft_delete", 1));
    });

    expectThrows(IllegalArgumentException.class, () -> {
      writer.softUpdateDocuments(new Term("id", "1"), Arrays.asList(new Document()));
    });

    Document doc = new Document();
    doc.add(new StringField("id", "1", Field.Store.YES));
    doc.add(new StringField("version", "1", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new StringField("id", "1", Field.Store.YES));
    doc.add(new StringField("version", "2", Field.Store.YES));
    Field field = new NumericDocValuesField("soft_delete", 1);
    writer.softUpdateDocument(new Term("id", "1"), doc, field);
    DirectoryReader reader = DirectoryReader.open(writer);
    assertEquals(2, reader.docFreq(new Term("id", "1")));
    IndexSearcher searcher = new IndexSearcher(reader);
    TopDocs topDocs = searcher.search(new TermQuery(new Term("id", "1")), 10);
    assertEquals(1, topDocs.totalHits.value);
    Document document = reader.document(topDocs.scoreDocs[0].doc);
    assertEquals("2", document.get("version"));

    // update the on-disk version
    doc = new Document();
    doc.add(new StringField("id", "1", Field.Store.YES));
    doc.add(new StringField("version", "3", Field.Store.YES));
    field = new NumericDocValuesField("soft_delete", 1);
    writer.softUpdateDocument(new Term("id", "1"), doc, field);
    DirectoryReader oldReader = reader;
    reader = DirectoryReader.openIfChanged(reader, writer);
    assertNotSame(reader, oldReader);
    oldReader.close();
    searcher = new IndexSearcher(reader);
    topDocs = searcher.search(new TermQuery(new Term("id", "1")), 10);
    assertEquals(1, topDocs.totalHits.value);
    document = reader.document(topDocs.scoreDocs[0].doc);
    assertEquals("3", document.get("version"));

    // now delete it
    writer.updateDocValues(new Term("id", "1"), field);
    oldReader = reader;
    reader = DirectoryReader.openIfChanged(reader, writer);
    assertNotSame(reader, oldReader);
    assertNotNull(reader);
    oldReader.close();
    searcher = new IndexSearcher(reader);
    topDocs = searcher.search(new TermQuery(new Term("id", "1")), 10);
    assertEquals(0, topDocs.totalHits.value);
    int numSoftDeleted = 0;
    for (SegmentCommitInfo info : writer.cloneSegmentInfos()) {
     numSoftDeleted += info.getSoftDelCount();
    }
    IndexWriter.DocStats docStats = writer.getDocStats();
    assertEquals(docStats.maxDoc - docStats.numDocs, numSoftDeleted);
    for (LeafReaderContext context : reader.leaves()) {
      LeafReader leaf = context.reader();
      assertNull(((SegmentReader) leaf).getHardLiveDocs());
    }
    writer.close();
    reader.close();
    dir.close();
  }

  public void testSoftUpdatesConcurrently() throws IOException, InterruptedException {
    softUpdatesConcurrently(false);
  }

  public void testSoftUpdatesConcurrentlyMixedDeletes() throws IOException, InterruptedException {
    softUpdatesConcurrently(true);
  }

  public void softUpdatesConcurrently(boolean mixDeletes) throws IOException, InterruptedException {
    Directory dir = newDirectory();
    IndexWriterConfig indexWriterConfig = newIndexWriterConfig();
    indexWriterConfig.setSoftDeletesField("soft_delete");
    AtomicBoolean mergeAwaySoftDeletes = new AtomicBoolean(random().nextBoolean());
    if (mixDeletes == false) {
      indexWriterConfig.setMergePolicy(new OneMergeWrappingMergePolicy(indexWriterConfig.getMergePolicy(), towrap ->
          new MergePolicy.OneMerge(towrap.segments) {
            @Override
            public CodecReader wrapForMerge(CodecReader reader) throws IOException {
              if (mergeAwaySoftDeletes.get()) {
                return towrap.wrapForMerge(reader);
              } else {
                CodecReader wrapped = towrap.wrapForMerge(reader);
                return new FilterCodecReader(wrapped) {
                  @Override
                  public CacheHelper getCoreCacheHelper() {
                    return in.getCoreCacheHelper();
                  }

                  @Override
                  public CacheHelper getReaderCacheHelper() {
                    return in.getReaderCacheHelper();
                  }

                  @Override
                  public Bits getLiveDocs() {
                    return null; // everything is live
                  }

                  @Override
                  public int numDocs() {
                    return maxDoc();
                  }
                };
              }
            }
          }
      ) {
        @Override
        public int numDeletesToMerge(SegmentCommitInfo info, int delCount, IOSupplier<CodecReader> readerSupplier) throws IOException {
          if (mergeAwaySoftDeletes.get()) {
            return super.numDeletesToMerge(info, delCount, readerSupplier);
          } else {
            return 0;
          }
        }
      });
    }
    IndexWriter writer = new IndexWriter(dir, indexWriterConfig);
    Thread[] threads = new Thread[2 + random().nextInt(3)];
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch started = new CountDownLatch(threads.length);
    boolean updateSeveralDocs = random().nextBoolean();
    Set<String> ids = Collections.synchronizedSet(new HashSet<>());
    for (int i = 0; i < threads.length; i++) {
      threads[i] = new Thread(() -> {
        try {
          started.countDown();
          startLatch.await();
          for (int d = 0;  d < 100; d++) {
            String id = String.valueOf(random().nextInt(10));
            if (updateSeveralDocs) {
              Document doc = new Document();
              doc.add(new StringField("id", id, Field.Store.YES));
              if (mixDeletes && random().nextBoolean()) {
                writer.updateDocuments(new Term("id", id), Arrays.asList(doc, doc));
              } else {
                writer.softUpdateDocuments(new Term("id", id), Arrays.asList(doc, doc),
                    new NumericDocValuesField("soft_delete", 1));
              }
            } else {
              Document doc = new Document();
              doc.add(new StringField("id", id, Field.Store.YES));
              if (mixDeletes && random().nextBoolean()) {
                writer.updateDocument(new Term("id", id), doc);
              } else {
                writer.softUpdateDocument(new Term("id", id), doc,
                    new NumericDocValuesField("soft_delete", 1));
              }
            }
            ids.add(id);
          }
        } catch (IOException | InterruptedException e) {
          throw new AssertionError(e);
        }
      });
      threads[i].start();
    }
    started.await();
    startLatch.countDown();

    for (int i = 0; i < threads.length; i++) {
      threads[i].join();
    }
    DirectoryReader reader = DirectoryReader.open(writer);
    IndexSearcher searcher = new IndexSearcher(reader);
    for (String id : ids) {
      TopDocs topDocs = searcher.search(new TermQuery(new Term("id", id)), 10);
      if (updateSeveralDocs) {
        assertEquals(2, topDocs.totalHits.value);
        assertEquals(Math.abs(topDocs.scoreDocs[0].doc - topDocs.scoreDocs[1].doc), 1);
      } else {
        assertEquals(1, topDocs.totalHits.value);
      }
    }
    if (mixDeletes == false) {
      for (LeafReaderContext context : reader.leaves()) {
        LeafReader leaf = context.reader();
        assertNull(((SegmentReader) leaf).getHardLiveDocs());
      }
    }
    mergeAwaySoftDeletes.set(true);
    writer.addDocument(new Document()); // add a dummy doc to trigger a segment here
    writer.flush();
    writer.forceMerge(1);
    DirectoryReader oldReader = reader;
    reader = DirectoryReader.openIfChanged(reader, writer);
    if (reader != null) {
      oldReader.close();
      assertNotSame(oldReader, reader);
    } else {
      reader = oldReader;
    }
    for (String id : ids) {
      if (updateSeveralDocs) {
        assertEquals(2, reader.docFreq(new Term("id", id)));
      } else {
        assertEquals(1, reader.docFreq(new Term("id", id)));
      }
    }
    int numSoftDeleted = 0;
    for (SegmentCommitInfo info : writer.cloneSegmentInfos()) {
      numSoftDeleted += info.getSoftDelCount() + info.getDelCount();
    }
    IndexWriter.DocStats docStats = writer.getDocStats();
    assertEquals(docStats.maxDoc - docStats.numDocs, numSoftDeleted);
    writer.commit();
    try (DirectoryReader dirReader = DirectoryReader.open(dir)) {
      int delCount = 0;
      for (LeafReaderContext ctx : dirReader.leaves()) {
        SegmentCommitInfo segmentInfo = ((SegmentReader) ctx.reader()).getSegmentInfo();
        delCount += segmentInfo.getSoftDelCount() + segmentInfo.getDelCount();
      }
      assertEquals(numSoftDeleted, delCount);
    }
    IOUtils.close(reader, writer, dir);
  }

  public void testDeleteHappensBeforeWhileFlush() throws IOException, InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    CountDownLatch inFlush = new CountDownLatch(1);
    try (Directory dir = new FilterDirectory(newDirectory()) {
      @Override
      public IndexOutput createOutput(String name, IOContext context) throws IOException {
        StackTraceElement[] trace = new Exception().getStackTrace();
        for (int i = 0; i < trace.length; i++) {
          if ("flush".equals(trace[i].getMethodName()) && DefaultIndexingChain.class.getName().equals(trace[i].getClassName())) {
            try {
              inFlush.countDown();
              latch.await();
            } catch (InterruptedException e) {
              throw new AssertionError(e);
            }
          }
        }
        return super.createOutput(name, context);
      }
    }; IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig())) {
      Document document = new Document();
      document.add(new StringField("id", "1", Field.Store.YES));
      writer.addDocument(document);
      Thread t = new Thread(() -> {
        try {
          inFlush.await();
          writer.docWriter.flushControl.setApplyAllDeletes();
          if (random().nextBoolean()) {
            writer.updateDocument(new Term("id", "1"), document);
          } else {
            writer.deleteDocuments(new Term("id", "1"));
          }

        } catch (Exception e) {
          throw new AssertionError(e);
        } finally {
          latch.countDown();
        }
      });
      t.start();
      try (IndexReader reader = writer.getReader()) {
        assertEquals(1, reader.numDocs());
      };
      t.join();
    }
  }

  private static void assertFiles(IndexWriter writer) throws IOException {
    Predicate<String> filter = file -> file.startsWith("segments") == false && file.equals("write.lock") == false;
    // remove segment files we don't know if we have committed and what is kept around
    Set<String> segFiles = new HashSet<>(writer.cloneSegmentInfos().files(true)).stream()
        .filter(filter).collect(Collectors.toSet());
    Set<String> dirFiles = Arrays.stream(writer.getDirectory().listAll())
        .filter(file -> !ExtrasFS.isExtra(file)) // ExtraFS might add an files, ignore them
        .filter(filter).collect(Collectors.toSet());
    assertEquals(segFiles.size(), dirFiles.size());
  }

  public void testFullyDeletedSegmentsReleaseFiles() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig();
    config.setRAMBufferSizeMB(Integer.MAX_VALUE);
    config.setMaxBufferedDocs(2); // no auto flush
    IndexWriter writer = new IndexWriter(dir, config);
    Document d = new Document();
    d.add(new StringField("id", "doc-0", Field.Store.YES));
    writer.addDocument(d);
    writer.flush();
    d = new Document();
    d.add(new StringField("id", "doc-1", Field.Store.YES));
    writer.addDocument(d);
    writer.deleteDocuments(new Term("id", "doc-1"));
    assertEquals(1, writer.listOfSegmentCommitInfos().size());
    writer.flush();
    assertEquals(1, writer.listOfSegmentCommitInfos().size());
    writer.commit();
    assertFiles(writer);
    assertEquals(1, writer.listOfSegmentCommitInfos().size());
    IOUtils.close(writer, dir);
  }

  public void testSegmentInfoIsSnapshot() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig();
    config.setRAMBufferSizeMB(Integer.MAX_VALUE);
    config.setMaxBufferedDocs(2); // no auto flush
    IndexWriter writer = new IndexWriter(dir, config);
    Document d = new Document();
    d.add(new StringField("id", "doc-0", Field.Store.YES));
    writer.addDocument(d);
    d = new Document();
    d.add(new StringField("id", "doc-1", Field.Store.YES));
    writer.addDocument(d);
    DirectoryReader reader = writer.getReader();
    SegmentCommitInfo segmentInfo = ((SegmentReader) reader.leaves().get(0).reader()).getSegmentInfo();
    SegmentCommitInfo originalInfo = ((SegmentReader) reader.leaves().get(0).reader()).getOriginalSegmentInfo();
    assertEquals(0, originalInfo.getDelCount());
    assertEquals(0, segmentInfo.getDelCount());
    writer.deleteDocuments(new Term("id", "doc-0"));
    writer.commit();
    assertEquals(0, segmentInfo.getDelCount());
    assertEquals(1, originalInfo.getDelCount());
    IOUtils.close(reader, writer, dir);
  }

  public void testPreventChangingSoftDeletesField() throws Exception {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig().setSoftDeletesField("my_deletes"));
    Document v1 = new Document();
    v1.add(new StringField("id", "1", Field.Store.YES));
    v1.add(new StringField("version", "1", Field.Store.YES));
    writer.addDocument(v1);
    Document v2 = new Document();
    v2.add(new StringField("id", "1", Field.Store.YES));
    v2.add(new StringField("version", "2", Field.Store.YES));
    writer.softUpdateDocument(new Term("id", "1"), v2, new NumericDocValuesField("my_deletes", 1));
    writer.commit();
    writer.close();
    for (SegmentCommitInfo si : SegmentInfos.readLatestCommit(dir)) {
      FieldInfos fieldInfos = IndexWriter.readFieldInfos(si);
      assertEquals("my_deletes", fieldInfos.getSoftDeletesField());
      assertTrue(fieldInfos.fieldInfo("my_deletes").isSoftDeletesField());
    }

    IllegalArgumentException illegalError = expectThrows(IllegalArgumentException.class, () -> {
      new IndexWriter(dir, newIndexWriterConfig().setSoftDeletesField("your_deletes"));
    });
    assertEquals("cannot configure [your_deletes] as soft-deletes; " +
        "this index uses [my_deletes] as soft-deletes already", illegalError.getMessage());

    IndexWriterConfig softDeleteConfig = newIndexWriterConfig().setSoftDeletesField("my_deletes")
        .setMergePolicy(new SoftDeletesRetentionMergePolicy("my_deletes", () -> new MatchAllDocsQuery(), newMergePolicy()));
    writer = new IndexWriter(dir, softDeleteConfig);
    Document tombstone = new Document();
    tombstone.add(new StringField("id", "tombstone", Field.Store.YES));
    tombstone.add(new NumericDocValuesField("my_deletes", 1));
    writer.addDocument(tombstone);
    writer.flush();
    for (SegmentCommitInfo si : writer.cloneSegmentInfos()) {
      FieldInfos fieldInfos = IndexWriter.readFieldInfos(si);
      assertEquals("my_deletes", fieldInfos.getSoftDeletesField());
      assertTrue(fieldInfos.fieldInfo("my_deletes").isSoftDeletesField());
    }
    writer.close();
    // reopen writer without soft-deletes field should be prevented
    IllegalArgumentException reopenError = expectThrows(IllegalArgumentException.class, () -> {
      new IndexWriter(dir, newIndexWriterConfig());
    });
    assertEquals("this index has [my_deletes] as soft-deletes already" +
        " but soft-deletes field is not configured in IWC", reopenError.getMessage());
    dir.close();
  }

  public void testPreventAddingIndexesWithDifferentSoftDeletesField() throws Exception {
    Directory dir1 = newDirectory();
    IndexWriter w1 = new IndexWriter(dir1, newIndexWriterConfig().setSoftDeletesField("soft_deletes_1"));
    for (int i = 0; i < 2; i++) {
      Document d = new Document();
      d.add(new StringField("id", "1", Field.Store.YES));
      d.add(new StringField("version", Integer.toString(i), Field.Store.YES));
      w1.softUpdateDocument(new Term("id", "1"), d, new NumericDocValuesField("soft_deletes_1", 1));
    }
    w1.commit();
    w1.close();

    Directory dir2 = newDirectory();
    IndexWriter w2 = new IndexWriter(dir2, newIndexWriterConfig().setSoftDeletesField("soft_deletes_2"));
    IllegalArgumentException error = expectThrows(IllegalArgumentException.class, () -> w2.addIndexes(dir1));
    assertEquals("cannot configure [soft_deletes_2] as soft-deletes; this index uses [soft_deletes_1] as soft-deletes already",
        error.getMessage());
    w2.close();

    Directory dir3 = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig().setSoftDeletesField("soft_deletes_1");
    IndexWriter w3 = new IndexWriter(dir3, config);
    w3.addIndexes(dir1);
    for (SegmentCommitInfo si : w3.cloneSegmentInfos()) {
      FieldInfo softDeleteField = IndexWriter.readFieldInfos(si).fieldInfo("soft_deletes_1");
      assertTrue(softDeleteField.isSoftDeletesField());
    }
    w3.close();
    IOUtils.close(dir1, dir2, dir3);
  }

  public void testNotAllowUsingExistingFieldAsSoftDeletes() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    for (int i = 0; i < 2; i++) {
      Document d = new Document();
      d.add(new StringField("id", "1", Field.Store.YES));
      if (random().nextBoolean()) {
        d.add(new NumericDocValuesField("dv_field", 1));
        w.updateDocument(new Term("id", "1"), d);
      } else {
        w.softUpdateDocument(new Term("id", "1"), d, new NumericDocValuesField("dv_field", 1));
      }
    }
    w.commit();
    w.close();
    String softDeletesField = random().nextBoolean() ? "id" : "dv_field";
    IllegalArgumentException error = expectThrows(IllegalArgumentException.class, () -> {
      IndexWriterConfig config = newIndexWriterConfig().setSoftDeletesField(softDeletesField);
      new IndexWriter(dir, config);
    });
    assertEquals("cannot configure [" + softDeletesField + "] as soft-deletes;" +
        " this index uses [" + softDeletesField + "] as non-soft-deletes already", error.getMessage());
    IndexWriterConfig config = newIndexWriterConfig().setSoftDeletesField("non-existing-field");
    w = new IndexWriter(dir, config);
    w.close();
    dir.close();
  }

  public void testBrokenPayload() throws Exception {
    Directory d = newDirectory();
    IndexWriter w = new IndexWriter(d, newIndexWriterConfig(new MockAnalyzer(random())));
    Document doc = new Document();
    Token token = new Token("bar", 0, 3);
    BytesRef evil = new BytesRef(new byte[1024]);
    evil.offset = 1000; // offset + length is now out of bounds.
    token.setPayload(evil);
    doc.add(new TextField("foo", new CannedTokenStream(token)));
    expectThrows(IndexOutOfBoundsException.class, () -> w.addDocument(doc));
    w.close();
    d.close();
  }

  public void testSoftAndHardLiveDocs() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig indexWriterConfig = newIndexWriterConfig();
    String softDeletesField = "soft_delete";
    indexWriterConfig.setSoftDeletesField(softDeletesField);
    IndexWriter writer = new IndexWriter(dir, indexWriterConfig);
    Set<Integer> uniqueDocs = new HashSet<>();
    for (int i = 0; i < 100; i++) {
      int docId = random().nextInt(5);
      uniqueDocs.add(docId);
      Document doc = new Document();
      doc.add(new StringField("id",  String.valueOf(docId), Field.Store.YES));
      if (docId %  2 == 0) {
        writer.updateDocument(new Term("id", String.valueOf(docId)), doc);
      } else {
        writer.softUpdateDocument(new Term("id", String.valueOf(docId)), doc,
            new NumericDocValuesField(softDeletesField,  0));
      }
      if (random().nextBoolean()) {
        assertHardLiveDocs(writer, uniqueDocs);
      }
    }

    if (random().nextBoolean()) {
      writer.commit();
    }
    assertHardLiveDocs(writer, uniqueDocs);


    IOUtils.close(writer, dir);
  }

  private void assertHardLiveDocs(IndexWriter writer, Set<Integer> uniqueDocs) throws IOException {
    try (DirectoryReader reader = DirectoryReader.open(writer)) {
      assertEquals(uniqueDocs.size(), reader.numDocs());
      List<LeafReaderContext> leaves = reader.leaves();
      for (LeafReaderContext ctx : leaves) {
        LeafReader leaf = ctx.reader();
        assertTrue(leaf instanceof SegmentReader);
        SegmentReader sr = (SegmentReader) leaf;
        if (sr.getHardLiveDocs() != null) {
          Terms id = sr.terms("id");
          TermsEnum iterator = id.iterator();
          Bits hardLiveDocs = sr.getHardLiveDocs();
          Bits liveDocs = sr.getLiveDocs();
          for (Integer dId : uniqueDocs) {
            boolean mustBeHardDeleted = dId % 2 == 0;
            if (iterator.seekExact(new BytesRef(dId.toString()))) {
              PostingsEnum postings = iterator.postings(null);
              while (postings.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                if (liveDocs.get(postings.docID())) {
                  assertTrue(hardLiveDocs.get(postings.docID()));
                } else if (mustBeHardDeleted) {
                  assertFalse(hardLiveDocs.get(postings.docID()));
                } else {
                  assertTrue(hardLiveDocs.get(postings.docID()));
                }
              }
            }
          }
        }
      }
    }
  }

  public void testSetIndexCreatedVersion() throws IOException {
    IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
        () -> new IndexWriterConfig().setIndexCreatedVersionMajor(Version.LATEST.major+1));
    assertEquals("indexCreatedVersionMajor may not be in the future: current major version is " +
        Version.LATEST.major + ", but got: " + (Version.LATEST.major+1), e.getMessage());
    e = expectThrows(IllegalArgumentException.class,
        () -> new IndexWriterConfig().setIndexCreatedVersionMajor(Version.LATEST.major-2));
    assertEquals("indexCreatedVersionMajor may not be less than the minimum supported version: " +
        (Version.LATEST.major-1) + ", but got: " + (Version.LATEST.major-2), e.getMessage());

    for (int previousMajor = Version.LATEST.major - 1; previousMajor <= Version.LATEST.major; previousMajor++) {
      for (int newMajor = Version.LATEST.major - 1; newMajor <= Version.LATEST.major; newMajor++) {
        for (OpenMode openMode : OpenMode.values()) {
          try (Directory dir = newDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig().setIndexCreatedVersionMajor(previousMajor))) {}
            SegmentInfos infos = SegmentInfos.readLatestCommit(dir);
            assertEquals(previousMajor, infos.getIndexCreatedVersionMajor());
            try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig().setOpenMode(openMode).setIndexCreatedVersionMajor(newMajor))) {}
            infos = SegmentInfos.readLatestCommit(dir);
            if (openMode == OpenMode.CREATE) {
              assertEquals(newMajor, infos.getIndexCreatedVersionMajor());
            } else {
              assertEquals(previousMajor, infos.getIndexCreatedVersionMajor());
            }
          }
        }
      }
    }
  }

  // see LUCENE-8639
  public void testFlushWhileStartingNewThreads() throws IOException, InterruptedException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, new IndexWriterConfig());
    w.addDocument(new Document());
    int activeThreadStateCount = w.docWriter.perThreadPool.getActiveThreadStateCount();
    assertEquals(1, activeThreadStateCount);
    CountDownLatch latch = new CountDownLatch(1);
    Thread thread = new Thread(() -> {
      latch.countDown();
      List<Closeable> states = new ArrayList<>();
      try {
        for (int i = 0; i < 100; i++) {
          DocumentsWriterPerThreadPool.ThreadState state = w.docWriter.perThreadPool.getAndLock();
          states.add(state::unlock);
          if (state.isInitialized()) {
            state.dwpt.deleteQueue.getNextSequenceNumber();
          } else {
            w.docWriter.deleteQueue.getNextSequenceNumber();
          }
        }
      } finally {
        IOUtils.closeWhileHandlingException(states);
      }
    });
    thread.start();
    latch.await();
    w.docWriter.flushControl.markForFullFlush();
    thread.join();
    w.docWriter.flushControl.abortFullFlushes();
    w.close();
    dir.close();
  }

  public void testRefreshAndRollbackConcurrently() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    AtomicBoolean stopped = new AtomicBoolean();
    Semaphore indexedDocs = new Semaphore(0);
    Thread indexer = new Thread(() -> {
      while (stopped.get() == false) {
        try {
          String id = Integer.toString(random().nextInt(100));
          Document doc = new Document();
          doc.add(new StringField("id", id, Field.Store.YES));
          w.updateDocument(new Term("id", id), doc);
          indexedDocs.release(1);
        } catch (IOException e) {
          throw new AssertionError(e);
        } catch (AlreadyClosedException ignored) {
          return;
        }
      }
    });

    SearcherManager sm = new SearcherManager(w, new SearcherFactory());
    Thread refresher = new Thread(() -> {
      while (stopped.get() == false) {
        try {
          sm.maybeRefreshBlocking();
        } catch (IOException e) {
          throw new AssertionError(e);
        } catch (AlreadyClosedException ignored) {
          return;
        }
      }
    });

    try {
      indexer.start();
      refresher.start();
      indexedDocs.acquire(1 + random().nextInt(100));
      w.rollback();
    } finally {
      stopped.set(true);
      indexer.join();
      refresher.join();
      IOUtils.close(sm, dir);
    }
  }
}
