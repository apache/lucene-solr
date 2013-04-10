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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.analysis.*;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.store.BaseDirectoryWrapper;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

public class TestIndexWriterExceptions extends LuceneTestCase {

  private static class DocCopyIterator implements Iterable<Document> {
    private final Document doc;
    private final int count;
    
    /* private field types */
    /* private field types */

    private static final FieldType custom1 = new FieldType(TextField.TYPE_NOT_STORED);
    private static final FieldType custom2 = new FieldType();
    private static final FieldType custom3 = new FieldType();
    private static final FieldType custom4 = new FieldType(StringField.TYPE_NOT_STORED);
    private static final FieldType custom5 = new FieldType(TextField.TYPE_STORED);
    
    static {

      custom1.setStoreTermVectors(true);
      custom1.setStoreTermVectorPositions(true);
      custom1.setStoreTermVectorOffsets(true);
      
      custom2.setStored(true);
      custom2.setIndexed(true);
      
      custom3.setStored(true);

      custom4.setStoreTermVectors(true);
      custom4.setStoreTermVectorPositions(true);
      custom4.setStoreTermVectorOffsets(true);
      
      custom5.setStoreTermVectors(true);
      custom5.setStoreTermVectorPositions(true);
      custom5.setStoreTermVectorOffsets(true);
    }

    public DocCopyIterator(Document doc, int count) {
      this.count = count;
      this.doc = doc;
    }

    @Override
    public Iterator<Document> iterator() {
      return new Iterator<Document>() {
        int upto;

        @Override
        public boolean hasNext() {
          return upto < count;
        }

        @Override
        public Document next() {
          upto++;
          return doc;
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    }
  }

  private class IndexerThread extends Thread {

    IndexWriter writer;

    final Random r = new Random(random().nextLong());
    volatile Throwable failure;

    public IndexerThread(int i, IndexWriter writer) {
      setName("Indexer " + i);
      this.writer = writer;
    }

    @Override
    public void run() {

      final Document doc = new Document();

      doc.add(newTextField(r, "content1", "aaa bbb ccc ddd", Field.Store.YES));
      doc.add(newField(r, "content6", "aaa bbb ccc ddd", DocCopyIterator.custom1));
      doc.add(newField(r, "content2", "aaa bbb ccc ddd", DocCopyIterator.custom2));
      doc.add(newField(r, "content3", "aaa bbb ccc ddd", DocCopyIterator.custom3));

      doc.add(newTextField(r, "content4", "aaa bbb ccc ddd", Field.Store.NO));
      doc.add(newStringField(r, "content5", "aaa bbb ccc ddd", Field.Store.NO));
      if (defaultCodecSupportsDocValues()) {
        doc.add(new NumericDocValuesField("numericdv", 5));
        doc.add(new BinaryDocValuesField("binarydv", new BytesRef("hello")));
        doc.add(new SortedDocValuesField("sorteddv", new BytesRef("world")));
      }
      if (defaultCodecSupportsSortedSet()) {
        doc.add(new SortedSetDocValuesField("sortedsetdv", new BytesRef("hellllo")));
        doc.add(new SortedSetDocValuesField("sortedsetdv", new BytesRef("again")));
      }

      doc.add(newField(r, "content7", "aaa bbb ccc ddd", DocCopyIterator.custom4));

      final Field idField = newField(r, "id", "", DocCopyIterator.custom2);
      doc.add(idField);

      final long stopTime = System.currentTimeMillis() + 500;

      do {
        if (VERBOSE) {
          System.out.println(Thread.currentThread().getName() + ": TEST: IndexerThread: cycle");
        }
        doFail.set(this);
        final String id = ""+r.nextInt(50);
        idField.setStringValue(id);
        Term idTerm = new Term("id", id);
        try {
          if (r.nextBoolean()) {
            writer.updateDocuments(idTerm, new DocCopyIterator(doc, _TestUtil.nextInt(r, 1, 20)));
          } else {
            writer.updateDocument(idTerm, doc);
          }
        } catch (RuntimeException re) {
          if (VERBOSE) {
            System.out.println(Thread.currentThread().getName() + ": EXC: ");
            re.printStackTrace(System.out);
          }
          try {
            _TestUtil.checkIndex(writer.getDirectory());
          } catch (IOException ioe) {
            System.out.println(Thread.currentThread().getName() + ": unexpected exception1");
            ioe.printStackTrace(System.out);
            failure = ioe;
            break;
          }
        } catch (Throwable t) {
          System.out.println(Thread.currentThread().getName() + ": unexpected exception2");
          t.printStackTrace(System.out);
          failure = t;
          break;
        }

        doFail.set(null);

        // After a possible exception (above) I should be able
        // to add a new document without hitting an
        // exception:
        try {
          writer.updateDocument(idTerm, doc);
        } catch (Throwable t) {
          System.out.println(Thread.currentThread().getName() + ": unexpected exception3");
          t.printStackTrace(System.out);
          failure = t;
          break;
        }
      } while(System.currentTimeMillis() < stopTime);
    }
  }

  ThreadLocal<Thread> doFail = new ThreadLocal<Thread>();

  private class MockIndexWriter extends IndexWriter {
    Random r = new Random(random().nextLong());

    public MockIndexWriter(Directory dir, IndexWriterConfig conf) throws IOException {
      super(dir, conf);
    }

    @Override
    boolean testPoint(String name) {
      if (doFail.get() != null && !name.equals("startDoFlush") && r.nextInt(40) == 17) {
        if (VERBOSE) {
          System.out.println(Thread.currentThread().getName() + ": NOW FAIL: " + name);
          new Throwable().printStackTrace(System.out);
        }
        throw new RuntimeException(Thread.currentThread().getName() + ": intentionally failing at " + name);
      }
      return true;
    }
  }

  public void testRandomExceptions() throws Throwable {
    if (VERBOSE) {
      System.out.println("\nTEST: start testRandomExceptions");
    }
    Directory dir = newDirectory();

    MockAnalyzer analyzer = new MockAnalyzer(random());
    analyzer.setEnableChecks(false); // disable workflow checking as we forcefully close() in exceptional cases.
    MockIndexWriter writer  = new MockIndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, analyzer)
        .setRAMBufferSizeMB(0.1).setMergeScheduler(new ConcurrentMergeScheduler()));
    ((ConcurrentMergeScheduler) writer.getConfig().getMergeScheduler()).setSuppressExceptions();
    //writer.setMaxBufferedDocs(10);
    if (VERBOSE) {
      System.out.println("TEST: initial commit");
    }
    writer.commit();

    IndexerThread thread = new IndexerThread(0, writer);
    thread.run();
    if (thread.failure != null) {
      thread.failure.printStackTrace(System.out);
      fail("thread " + thread.getName() + ": hit unexpected failure");
    }

    if (VERBOSE) {
      System.out.println("TEST: commit after thread start");
    }
    writer.commit();

    try {
      writer.close();
    } catch (Throwable t) {
      System.out.println("exception during close:");
      t.printStackTrace(System.out);
      writer.rollback();
    }

    // Confirm that when doc hits exception partway through tokenization, it's deleted:
    IndexReader r2 = DirectoryReader.open(dir);
    final int count = r2.docFreq(new Term("content4", "aaa"));
    final int count2 = r2.docFreq(new Term("content4", "ddd"));
    assertEquals(count, count2);
    r2.close();

    dir.close();
  }

  public void testRandomExceptionsThreads() throws Throwable {
    Directory dir = newDirectory();
    MockAnalyzer analyzer = new MockAnalyzer(random());
    analyzer.setEnableChecks(false); // disable workflow checking as we forcefully close() in exceptional cases.
    MockIndexWriter writer  = new MockIndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, analyzer)
        .setRAMBufferSizeMB(0.2).setMergeScheduler(new ConcurrentMergeScheduler()));
    ((ConcurrentMergeScheduler) writer.getConfig().getMergeScheduler()).setSuppressExceptions();
    //writer.setMaxBufferedDocs(10);
    writer.commit();

    final int NUM_THREADS = 4;

    final IndexerThread[] threads = new IndexerThread[NUM_THREADS];
    for(int i=0;i<NUM_THREADS;i++) {
      threads[i] = new IndexerThread(i, writer);
      threads[i].start();
    }

    for(int i=0;i<NUM_THREADS;i++)
      threads[i].join();

    for(int i=0;i<NUM_THREADS;i++)
      if (threads[i].failure != null) {
        fail("thread " + threads[i].getName() + ": hit unexpected failure");
      }

    writer.commit();

    try {
      writer.close();
    } catch (Throwable t) {
      System.out.println("exception during close:");
      t.printStackTrace(System.out);
      writer.rollback();
    }

    // Confirm that when doc hits exception partway through tokenization, it's deleted:
    IndexReader r2 = DirectoryReader.open(dir);
    final int count = r2.docFreq(new Term("content4", "aaa"));
    final int count2 = r2.docFreq(new Term("content4", "ddd"));
    assertEquals(count, count2);
    r2.close();

    dir.close();
  }

  // LUCENE-1198
  private static final class MockIndexWriter2 extends IndexWriter {

    public MockIndexWriter2(Directory dir, IndexWriterConfig conf) throws IOException {
      super(dir, conf);
    }

    boolean doFail;

    @Override
    boolean testPoint(String name) {
      if (doFail && name.equals("DocumentsWriterPerThread addDocument start"))
        throw new RuntimeException("intentionally failing");
      return true;
    }
  }

  private static String CRASH_FAIL_MESSAGE = "I'm experiencing problems";

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
        throw new IOException(CRASH_FAIL_MESSAGE);
      return input.incrementToken();
    }

    @Override
    public void reset() throws IOException {
      super.reset();
      count = 0;
    }
  }

  public void testExceptionDocumentsWriterInit() throws IOException {
    Directory dir = newDirectory();
    MockIndexWriter2 w = new MockIndexWriter2(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    Document doc = new Document();
    doc.add(newTextField("field", "a field", Field.Store.YES));
    w.addDocument(doc);
    w.doFail = true;
    try {
      w.addDocument(doc);
      fail("did not hit exception");
    } catch (RuntimeException re) {
      // expected
    }
    w.close();
    dir.close();
  }

  // LUCENE-1208
  public void testExceptionJustBeforeFlush() throws IOException {
    Directory dir = newDirectory();
    MockIndexWriter w = new MockIndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random())).setMaxBufferedDocs(2));
    Document doc = new Document();
    doc.add(newTextField("field", "a field", Field.Store.YES));
    w.addDocument(doc);

    Analyzer analyzer = new Analyzer(new Analyzer.PerFieldReuseStrategy()) {
      @Override
      public TokenStreamComponents createComponents(String fieldName, Reader reader) {
        MockTokenizer tokenizer = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
        tokenizer.setEnableChecks(false); // disable workflow checking as we forcefully close() in exceptional cases.
        return new TokenStreamComponents(tokenizer, new CrashingFilter(fieldName, tokenizer));
      }
    };

    Document crashDoc = new Document();
    crashDoc.add(newTextField("crash", "do it on token 4", Field.Store.YES));
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

  private static final class MockIndexWriter3 extends IndexWriter {

    public MockIndexWriter3(Directory dir, IndexWriterConfig conf) throws IOException {
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
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random()))
      .setMaxBufferedDocs(2).setMergePolicy(newLogMergePolicy());
    ConcurrentMergeScheduler cms = new ConcurrentMergeScheduler();
    cms.setSuppressExceptions();
    conf.setMergeScheduler(cms);
    ((LogMergePolicy) conf.getMergePolicy()).setMergeFactor(2);
    MockIndexWriter3 w = new MockIndexWriter3(dir, conf);
    w.doFail = true;
    Document doc = new Document();
    doc.add(newTextField("field", "a field", Field.Store.YES));
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

  // LUCENE-1072
  public void testExceptionFromTokenStream() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig( TEST_VERSION_CURRENT, new Analyzer() {

      @Override
      public TokenStreamComponents createComponents(String fieldName, Reader reader) {
        MockTokenizer tokenizer = new MockTokenizer(reader, MockTokenizer.SIMPLE, true);
        tokenizer.setEnableChecks(false); // disable workflow checking as we forcefully close() in exceptional cases.
        return new TokenStreamComponents(tokenizer, new TokenFilter(tokenizer) {
          private int count = 0;

          @Override
          public boolean incrementToken() throws IOException {
            if (count++ == 5) {
              throw new IOException();
            }
            return input.incrementToken();
          }

          @Override
          public void reset() throws IOException {
            super.reset();
            this.count = 0;
          }
        });
      }

    });
    conf.setMaxBufferedDocs(Math.max(3, conf.getMaxBufferedDocs()));

    IndexWriter writer = new IndexWriter(dir, conf);

    Document doc = new Document();
    String contents = "aa bb cc dd ee ff gg hh ii jj kk";
    doc.add(newTextField("content", contents, Field.Store.NO));
    try {
      writer.addDocument(doc);
      fail("did not hit expected exception");
    } catch (Exception e) {
    }

    // Make sure we can add another normal document
    doc = new Document();
    doc.add(newTextField("content", "aa bb cc dd", Field.Store.NO));
    writer.addDocument(doc);

    // Make sure we can add another normal document
    doc = new Document();
    doc.add(newTextField("content", "aa bb cc dd", Field.Store.NO));
    writer.addDocument(doc);

    writer.close();
    IndexReader reader = DirectoryReader.open(dir);
    final Term t = new Term("content", "aa");
    assertEquals(3, reader.docFreq(t));

    // Make sure the doc that hit the exception was marked
    // as deleted:
    DocsEnum tdocs = _TestUtil.docs(random(), reader,
                                    t.field(),
                                    new BytesRef(t.text()),
                                    MultiFields.getLiveDocs(reader),
                                    null,
                                    0);

    int count = 0;
    while(tdocs.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
      count++;
    }
    assertEquals(2, count);

    assertEquals(reader.docFreq(new Term("content", "gg")), 0);
    reader.close();
    dir.close();
  }

  private static class FailOnlyOnFlush extends MockDirectoryWrapper.Failure {
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
    public void eval(MockDirectoryWrapper dir)  throws IOException {
      if (doFail) {
        StackTraceElement[] trace = new Exception().getStackTrace();
        boolean sawAppend = false;
        boolean sawFlush = false;
        for (int i = 0; i < trace.length; i++) {
          if (FreqProxTermsWriterPerField.class.getName().equals(trace[i].getClassName()) && "flush".equals(trace[i].getMethodName()))
            sawAppend = true;
          if ("flush".equals(trace[i].getMethodName()))
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
    MockDirectoryWrapper dir = newMockDirectory();
    FailOnlyOnFlush failure = new FailOnlyOnFlush();
    failure.setDoFail();
    dir.failOn(failure);

    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random())).setMaxBufferedDocs(2));
    Document doc = new Document();
    String contents = "aa bb cc dd ee ff gg hh ii jj kk";
    doc.add(newTextField("content", contents, Field.Store.NO));
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
    IndexReader reader = DirectoryReader.open(dir);
    assertEquals(198, reader.docFreq(new Term("content", "aa")));
    reader.close();
    dir.close();
  }

  public void testDocumentsWriterExceptions() throws IOException {
    Analyzer analyzer = new Analyzer(new Analyzer.PerFieldReuseStrategy()) {
      @Override
      public TokenStreamComponents createComponents(String fieldName, Reader reader) {
        MockTokenizer tokenizer = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
        tokenizer.setEnableChecks(false); // disable workflow checking as we forcefully close() in exceptional cases.
        return new TokenStreamComponents(tokenizer, new CrashingFilter(fieldName, tokenizer));
      }
    };

    for(int i=0;i<2;i++) {
      if (VERBOSE) {
        System.out.println("TEST: cycle i=" + i);
      }
      Directory dir = newDirectory();
      IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, analyzer).setMergePolicy(newLogMergePolicy()));

      // don't allow a sudden merge to clean up the deleted
      // doc below:
      LogMergePolicy lmp = (LogMergePolicy) writer.getConfig().getMergePolicy();
      lmp.setMergeFactor(Math.max(lmp.getMergeFactor(), 5));

      Document doc = new Document();
      doc.add(newField("contents", "here are some contents", DocCopyIterator.custom5));
      writer.addDocument(doc);
      writer.addDocument(doc);
      doc.add(newField("crash", "this should crash after 4 terms", DocCopyIterator.custom5));
      doc.add(newField("other", "this will not get indexed", DocCopyIterator.custom5));
      try {
        writer.addDocument(doc);
        fail("did not hit expected exception");
      } catch (IOException ioe) {
        if (VERBOSE) {
          System.out.println("TEST: hit expected exception");
          ioe.printStackTrace(System.out);
        }
      }

      if (0 == i) {
        doc = new Document();
        doc.add(newField("contents", "here are some contents", DocCopyIterator.custom5));
        writer.addDocument(doc);
        writer.addDocument(doc);
      }
      writer.close();

      if (VERBOSE) {
        System.out.println("TEST: open reader");
      }
      IndexReader reader = DirectoryReader.open(dir);
      if (i == 0) { 
        int expected = 5;
        assertEquals(expected, reader.docFreq(new Term("contents", "here")));
        assertEquals(expected, reader.maxDoc());
        int numDel = 0;
        final Bits liveDocs = MultiFields.getLiveDocs(reader);
        assertNotNull(liveDocs);
        for(int j=0;j<reader.maxDoc();j++) {
          if (!liveDocs.get(j))
            numDel++;
          else {
            reader.document(j);
            reader.getTermVectors(j);
          }
        }
        assertEquals(1, numDel);
      }
      reader.close();

      writer = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT,
          analyzer).setMaxBufferedDocs(10));
      doc = new Document();
      doc.add(newField("contents", "here are some contents", DocCopyIterator.custom5));
      for(int j=0;j<17;j++)
        writer.addDocument(doc);
      writer.forceMerge(1);
      writer.close();

      reader = DirectoryReader.open(dir);
      int expected = 19+(1-i)*2;
      assertEquals(expected, reader.docFreq(new Term("contents", "here")));
      assertEquals(expected, reader.maxDoc());
      int numDel = 0;
      assertNull(MultiFields.getLiveDocs(reader));
      for(int j=0;j<reader.maxDoc();j++) {
        reader.document(j);
        reader.getTermVectors(j);
      }
      reader.close();
      assertEquals(0, numDel);

      dir.close();
    }
  }

  public void testDocumentsWriterExceptionThreads() throws Exception {
    Analyzer analyzer = new Analyzer(new Analyzer.PerFieldReuseStrategy()) {
      @Override
      public TokenStreamComponents createComponents(String fieldName, Reader reader) {
        MockTokenizer tokenizer = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
        tokenizer.setEnableChecks(false); // disable workflow checking as we forcefully close() in exceptional cases.
        return new TokenStreamComponents(tokenizer, new CrashingFilter(fieldName, tokenizer));
      }
    };

    final int NUM_THREAD = 3;
    final int NUM_ITER = 100;

    for(int i=0;i<2;i++) {
      Directory dir = newDirectory();

      {
        final IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
            TEST_VERSION_CURRENT, analyzer).setMaxBufferedDocs(-1)
            .setMergePolicy(
                random().nextBoolean() ? NoMergePolicy.COMPOUND_FILES
                    : NoMergePolicy.NO_COMPOUND_FILES));
        // don't use a merge policy here they depend on the DWPThreadPool and its max thread states etc.
        final int finalI = i;

        Thread[] threads = new Thread[NUM_THREAD];
        for(int t=0;t<NUM_THREAD;t++) {
          threads[t] = new Thread() {
              @Override
              public void run() {
                try {
                  for(int iter=0;iter<NUM_ITER;iter++) {
                    Document doc = new Document();
                    doc.add(newField("contents", "here are some contents", DocCopyIterator.custom5));
                    writer.addDocument(doc);
                    writer.addDocument(doc);
                    doc.add(newField("crash", "this should crash after 4 terms", DocCopyIterator.custom5));
                    doc.add(newField("other", "this will not get indexed", DocCopyIterator.custom5));
                    try {
                      writer.addDocument(doc);
                      fail("did not hit expected exception");
                    } catch (IOException ioe) {
                    }

                    if (0 == finalI) {
                      doc = new Document();
                      doc.add(newField("contents", "here are some contents", DocCopyIterator.custom5));
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

      IndexReader reader = DirectoryReader.open(dir);
      int expected = (3+(1-i)*2)*NUM_THREAD*NUM_ITER;
      assertEquals("i=" + i, expected, reader.docFreq(new Term("contents", "here")));
      assertEquals(expected, reader.maxDoc());
      int numDel = 0;
      final Bits liveDocs = MultiFields.getLiveDocs(reader);
      assertNotNull(liveDocs);
      for(int j=0;j<reader.maxDoc();j++) {
        if (!liveDocs.get(j))
          numDel++;
        else {
          reader.document(j);
          reader.getTermVectors(j);
        }
      }
      reader.close();

      assertEquals(NUM_THREAD*NUM_ITER, numDel);

      IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
          TEST_VERSION_CURRENT, analyzer).setMaxBufferedDocs(10));
      Document doc = new Document();
      doc.add(newField("contents", "here are some contents", DocCopyIterator.custom5));
      for(int j=0;j<17;j++)
        writer.addDocument(doc);
      writer.forceMerge(1);
      writer.close();

      reader = DirectoryReader.open(dir);
      expected += 17-NUM_THREAD*NUM_ITER;
      assertEquals(expected, reader.docFreq(new Term("contents", "here")));
      assertEquals(expected, reader.maxDoc());
      assertNull(MultiFields.getLiveDocs(reader));
      for(int j=0;j<reader.maxDoc();j++) {
        reader.document(j);
        reader.getTermVectors(j);
      }
      reader.close();

      dir.close();
    }
  }

  // Throws IOException during MockDirectoryWrapper.sync
  private static class FailOnlyInSync extends MockDirectoryWrapper.Failure {
    boolean didFail;
    @Override
    public void eval(MockDirectoryWrapper dir)  throws IOException {
      if (doFail) {
        StackTraceElement[] trace = new Exception().getStackTrace();
        for (int i = 0; i < trace.length; i++) {
          if (doFail && MockDirectoryWrapper.class.getName().equals(trace[i].getClassName()) && "sync".equals(trace[i].getMethodName())) {
            didFail = true;
            if (VERBOSE) {
              System.out.println("TEST: now throw exc:");
              new Throwable().printStackTrace(System.out);
            }
            throw new IOException("now failing on purpose during sync");
          }
        }
      }
    }
  }

  // TODO: these are also in TestIndexWriter... add a simple doc-writing method
  // like this to LuceneTestCase?
  private void addDoc(IndexWriter writer) throws IOException
  {
      Document doc = new Document();
      doc.add(newTextField("content", "aaa", Field.Store.NO));
      writer.addDocument(doc);
  }

  // LUCENE-1044: test exception during sync
  public void testExceptionDuringSync() throws IOException {
    MockDirectoryWrapper dir = newMockDirectory();
    FailOnlyInSync failure = new FailOnlyInSync();
    dir.failOn(failure);

    IndexWriter writer = new IndexWriter(
        dir,
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).
            setMaxBufferedDocs(2).
            setMergeScheduler(new ConcurrentMergeScheduler()).
            setMergePolicy(newLogMergePolicy(5))
    );
    failure.setDoFail();

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

    IndexReader reader = DirectoryReader.open(dir);
    assertEquals(23, reader.numDocs());
    reader.close();
    dir.close();
  }

  private static class FailOnlyInCommit extends MockDirectoryWrapper.Failure {

    boolean failOnCommit, failOnDeleteFile;
    private final boolean dontFailDuringGlobalFieldMap;
    private static final String PREPARE_STAGE = "prepareCommit";
    private static final String FINISH_STAGE = "finishCommit";
    private final String stage;
    
    public FailOnlyInCommit(boolean dontFailDuringGlobalFieldMap, String stage) {
      this.dontFailDuringGlobalFieldMap = dontFailDuringGlobalFieldMap;
      this.stage = stage;
    }

    @Override
    public void eval(MockDirectoryWrapper dir)  throws IOException {
      StackTraceElement[] trace = new Exception().getStackTrace();
      boolean isCommit = false;
      boolean isDelete = false;
      boolean isInGlobalFieldMap = false;
      for (int i = 0; i < trace.length; i++) {
        if (SegmentInfos.class.getName().equals(trace[i].getClassName()) && stage.equals(trace[i].getMethodName()))
          isCommit = true;
        if (MockDirectoryWrapper.class.getName().equals(trace[i].getClassName()) && "deleteFile".equals(trace[i].getMethodName()))
          isDelete = true;
        if (SegmentInfos.class.getName().equals(trace[i].getClassName()) && "writeGlobalFieldMap".equals(trace[i].getMethodName()))
          isInGlobalFieldMap = true;
          
      }
      if (isInGlobalFieldMap && dontFailDuringGlobalFieldMap) {
        isCommit = false;
      }
      if (isCommit) {
        if (!isDelete) {
          failOnCommit = true;
          throw new RuntimeException("now fail first");
        } else {
          failOnDeleteFile = true;
          throw new IOException("now fail during delete");
        }
      }
    }
  }

  public void testExceptionsDuringCommit() throws Throwable {
    FailOnlyInCommit[] failures = new FailOnlyInCommit[] {
        // LUCENE-1214
        new FailOnlyInCommit(false, FailOnlyInCommit.PREPARE_STAGE), // fail during global field map is written
        new FailOnlyInCommit(true, FailOnlyInCommit.PREPARE_STAGE), // fail after global field map is written
        new FailOnlyInCommit(false, FailOnlyInCommit.FINISH_STAGE)  // fail while running finishCommit    
    };
    
    for (FailOnlyInCommit failure : failures) {
      MockDirectoryWrapper dir = newMockDirectory();
      dir.setFailOnCreateOutput(false);
      IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(
          TEST_VERSION_CURRENT, new MockAnalyzer(random())));
      Document doc = new Document();
      doc.add(newTextField("field", "a field", Field.Store.YES));
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
      assertTrue(failure.failOnCommit && failure.failOnDeleteFile);
      w.rollback();
      assertEquals(0, dir.listAll().length);
      dir.close();
    }
  }

  public void testForceMergeExceptions() throws IOException {
    Directory startDir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random())).setMaxBufferedDocs(2).setMergePolicy(newLogMergePolicy());
    ((LogMergePolicy) conf.getMergePolicy()).setMergeFactor(100);
    IndexWriter w = new IndexWriter(startDir, conf);
    for(int i=0;i<27;i++)
      addDoc(w);
    w.close();

    int iter = TEST_NIGHTLY ? 200 : 10;
    for(int i=0;i<iter;i++) {
      if (VERBOSE) {
        System.out.println("TEST: iter " + i);
      }
      MockDirectoryWrapper dir = new MockDirectoryWrapper(random(), new RAMDirectory(startDir, newIOContext(random())));
      conf = newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random())).setMergeScheduler(new ConcurrentMergeScheduler());
      ((ConcurrentMergeScheduler) conf.getMergeScheduler()).setSuppressExceptions();
      w = new IndexWriter(dir, conf);
      dir.setRandomIOExceptionRate(0.5);
      try {
        w.forceMerge(1);
      } catch (IOException ioe) {
        if (ioe.getCause() == null)
          fail("forceMerge threw IOException without root cause");
      }
      dir.setRandomIOExceptionRate(0);
      w.close();
      dir.close();
    }
    startDir.close();
  }

  // LUCENE-1429
  public void testOutOfMemoryErrorCausesCloseToFail() throws Exception {

    final AtomicBoolean thrown = new AtomicBoolean(false);
    final Directory dir = newDirectory();
    final IndexWriter writer = new IndexWriter(dir,
        newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random())).setInfoStream(new InfoStream() {
        @Override
        public void message(String component, final String message) {
          if (message.startsWith("now flush at close") && thrown.compareAndSet(false, true)) {
            throw new OutOfMemoryError("fake OOME at " + message);
          }
        }

        @Override
        public boolean isEnabled(String component) {
          return true;
        }
        
        @Override
        public void close() {}
      }));

    try {
      writer.close();
      fail("OutOfMemoryError expected");
    }
    catch (final OutOfMemoryError expected) {}

    // throws IllegalStateEx w/o bug fix
    writer.close();
    dir.close();
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
    Directory dir = newDirectory();
    MockIndexWriter4 w = new MockIndexWriter4(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random())));

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

  // LUCENE-1044: Simulate checksum error in segments_N
  public void testSegmentsChecksumError() throws IOException {
    Directory dir = newDirectory();

    IndexWriter writer = null;

    writer = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random())));

    // add 100 documents
    for (int i = 0; i < 100; i++) {
      addDoc(writer);
    }

    // close
    writer.close();

    long gen = SegmentInfos.getLastCommitGeneration(dir);
    assertTrue("segment generation should be > 0 but got " + gen, gen > 0);

    final String segmentsFileName = SegmentInfos.getLastCommitSegmentsFileName(dir);
    IndexInput in = dir.openInput(segmentsFileName, newIOContext(random()));
    IndexOutput out = dir.createOutput(IndexFileNames.fileNameFromGeneration(IndexFileNames.SEGMENTS, "", 1+gen), newIOContext(random()));
    out.copyBytes(in, in.length()-1);
    byte b = in.readByte();
    out.writeByte((byte) (1+b));
    out.close();
    in.close();

    IndexReader reader = null;
    try {
      reader = DirectoryReader.open(dir);
    } catch (IOException e) {
      e.printStackTrace(System.out);
      fail("segmentInfos failed to retry fallback to correct segments_N file");
    }
    reader.close();
    
    // should remove the corrumpted segments_N
    new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, null)).close();
    dir.close();
  }

  // Simulate a corrupt index by removing last byte of
  // latest segments file and make sure we get an
  // IOException trying to open the index:
  public void testSimulatedCorruptIndex1() throws IOException {
      BaseDirectoryWrapper dir = newDirectory();
      dir.setCheckIndexOnClose(false); // we are corrupting it!

      IndexWriter writer = null;

      writer  = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random())));

      // add 100 documents
      for (int i = 0; i < 100; i++) {
          addDoc(writer);
      }

      // close
      writer.close();

      long gen = SegmentInfos.getLastCommitGeneration(dir);
      assertTrue("segment generation should be > 0 but got " + gen, gen > 0);

      String fileNameIn = SegmentInfos.getLastCommitSegmentsFileName(dir);
      String fileNameOut = IndexFileNames.fileNameFromGeneration(IndexFileNames.SEGMENTS,
                                                                 "",
                                                                 1+gen);
      IndexInput in = dir.openInput(fileNameIn, newIOContext(random()));
      IndexOutput out = dir.createOutput(fileNameOut, newIOContext(random()));
      long length = in.length();
      for(int i=0;i<length-1;i++) {
        out.writeByte(in.readByte());
      }
      in.close();
      out.close();
      dir.deleteFile(fileNameIn);

      IndexReader reader = null;
      try {
        reader = DirectoryReader.open(dir);
        fail("reader did not hit IOException on opening a corrupt index");
      } catch (Exception e) {
      }
      if (reader != null) {
        reader.close();
      }
      dir.close();
  }

  // Simulate a corrupt index by removing one of the cfs
  // files and make sure we get an IOException trying to
  // open the index:
  public void testSimulatedCorruptIndex2() throws IOException {
    BaseDirectoryWrapper dir = newDirectory();
    dir.setCheckIndexOnClose(false); // we are corrupting it!
    IndexWriter writer = null;

    writer  = new IndexWriter(
                              dir,
                              newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).
                              setMergePolicy(newLogMergePolicy(true))
                              );
    LogMergePolicy lmp = (LogMergePolicy) writer.getConfig().getMergePolicy();
    // Force creation of CFS:
    lmp.setNoCFSRatio(1.0);
    lmp.setMaxCFSSegmentSizeMB(Double.POSITIVE_INFINITY);

    // add 100 documents
    for (int i = 0; i < 100; i++) {
      addDoc(writer);
    }

    // close
    writer.close();

    long gen = SegmentInfos.getLastCommitGeneration(dir);
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
      reader = DirectoryReader.open(dir);
      fail("reader did not hit IOException on opening a corrupt index");
    } catch (Exception e) {
    }
    if (reader != null) {
      reader.close();
    }
    dir.close();
  }

  // Simulate a writer that crashed while writing segments
  // file: make sure we can still open the index (ie,
  // gracefully fallback to the previous segments file),
  // and that we can add to the index:
  public void testSimulatedCrashedWriter() throws IOException {
      Directory dir = newDirectory();
      if (dir instanceof MockDirectoryWrapper) {
        ((MockDirectoryWrapper)dir).setPreventDoubleWrite(false);
      }

      IndexWriter writer = null;

      writer  = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random())));

      // add 100 documents
      for (int i = 0; i < 100; i++) {
          addDoc(writer);
      }

      // close
      writer.close();

      long gen = SegmentInfos.getLastCommitGeneration(dir);
      assertTrue("segment generation should be > 0 but got " + gen, gen > 0);

      // Make the next segments file, with last byte
      // missing, to simulate a writer that crashed while
      // writing segments file:
      String fileNameIn = SegmentInfos.getLastCommitSegmentsFileName(dir);
      String fileNameOut = IndexFileNames.fileNameFromGeneration(IndexFileNames.SEGMENTS,
                                                                 "",
                                                                 1+gen);
      IndexInput in = dir.openInput(fileNameIn, newIOContext(random()));
      IndexOutput out = dir.createOutput(fileNameOut, newIOContext(random()));
      long length = in.length();
      for(int i=0;i<length-1;i++) {
        out.writeByte(in.readByte());
      }
      in.close();
      out.close();

      IndexReader reader = null;
      try {
        reader = DirectoryReader.open(dir);
      } catch (Exception e) {
        fail("reader failed to open on a crashed index");
      }
      reader.close();

      try {
        writer  = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random())).setOpenMode(OpenMode.CREATE));
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

  public void testTermVectorExceptions() throws IOException {
    FailOnTermVectors[] failures = new FailOnTermVectors[] {
        new FailOnTermVectors(FailOnTermVectors.AFTER_INIT_STAGE),
        new FailOnTermVectors(FailOnTermVectors.INIT_STAGE), };
    int num = atLeast(1);
    for (int j = 0; j < num; j++) {
      for (FailOnTermVectors failure : failures) {
        MockDirectoryWrapper dir = newMockDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(
            TEST_VERSION_CURRENT, new MockAnalyzer(random())));
        dir.failOn(failure);
        int numDocs = 10 + random().nextInt(30);
        for (int i = 0; i < numDocs; i++) {
          Document doc = new Document();
          Field field = newTextField(random(), "field", "a field", Field.Store.YES);
          doc.add(field);
          // random TV
          try {
            w.addDocument(doc);
            assertFalse(field.fieldType().storeTermVectors());
          } catch (RuntimeException e) {
            assertTrue(e.getMessage().startsWith(FailOnTermVectors.EXC_MSG));
          }
          if (random().nextInt(20) == 0) {
            w.commit();
            _TestUtil.checkIndex(dir);
          }
            
        }
        Document document = new Document();
        document.add(new TextField("field", "a field", Field.Store.YES));
        w.addDocument(document);

        for (int i = 0; i < numDocs; i++) {
          Document doc = new Document();
          Field field = newTextField(random(), "field", "a field", Field.Store.YES);
          doc.add(field);
          // random TV
          try {
            w.addDocument(doc);
            assertFalse(field.fieldType().storeTermVectors());
          } catch (RuntimeException e) {
            assertTrue(e.getMessage().startsWith(FailOnTermVectors.EXC_MSG));
          }
          if (random().nextInt(20) == 0) {
            w.commit();
            _TestUtil.checkIndex(dir);
          }
        }
        document = new Document();
        document.add(new TextField("field", "a field", Field.Store.YES));
        w.addDocument(document);
        w.close();
        IndexReader reader = DirectoryReader.open(dir);
        assertTrue(reader.numDocs() > 0);
        SegmentInfos sis = new SegmentInfos();
        sis.read(dir);
        for(AtomicReaderContext context : reader.leaves()) {
          assertFalse(context.reader().getFieldInfos().hasVectors());
        }
        reader.close();
        dir.close();
      }
    }
  }
  
  private static class FailOnTermVectors extends MockDirectoryWrapper.Failure {

    private static final String INIT_STAGE = "initTermVectorsWriter";
    private static final String AFTER_INIT_STAGE = "finishDocument";
    private static final String EXC_MSG = "FOTV";
    private final String stage;
    
    public FailOnTermVectors(String stage) {
      this.stage = stage;
    }

    @Override
    public void eval(MockDirectoryWrapper dir)  throws IOException {

      StackTraceElement[] trace = new Exception().getStackTrace();
      boolean fail = false;
      for (int i = 0; i < trace.length; i++) {
        if (TermVectorsConsumer.class.getName().equals(trace[i].getClassName()) && stage.equals(trace[i].getMethodName())) {
          fail = true;
        }
      }
      
      if (fail) {
        throw new RuntimeException(EXC_MSG);
      }
    }
  }

  public void testAddDocsNonAbortingException() throws Exception {
    final Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    final int numDocs1 = random().nextInt(25);
    for(int docCount=0;docCount<numDocs1;docCount++) {
      Document doc = new Document();
      doc.add(newTextField("content", "good content", Field.Store.NO));
      w.addDocument(doc);
    }
    
    final List<Document> docs = new ArrayList<Document>();
    for(int docCount=0;docCount<7;docCount++) {
      Document doc = new Document();
      docs.add(doc);
      doc.add(newStringField("id", docCount+"", Field.Store.NO));
      doc.add(newTextField("content", "silly content " + docCount, Field.Store.NO));
      if (docCount == 4) {
        Field f = newTextField("crash", "", Field.Store.NO);
        doc.add(f);
        MockTokenizer tokenizer = new MockTokenizer(new StringReader("crash me on the 4th token"), MockTokenizer.WHITESPACE, false);
        tokenizer.setEnableChecks(false); // disable workflow checking as we forcefully close() in exceptional cases.
        f.setTokenStream(new CrashingFilter("crash", tokenizer));
      }
    }
    try {
      w.addDocuments(docs);
      // BUG: CrashingFilter didn't
      fail("did not hit expected exception");
    } catch (IOException ioe) {
      // expected
      assertEquals(CRASH_FAIL_MESSAGE, ioe.getMessage());
    }

    final int numDocs2 = random().nextInt(25);
    for(int docCount=0;docCount<numDocs2;docCount++) {
      Document doc = new Document();
      doc.add(newTextField("content", "good content", Field.Store.NO));
      w.addDocument(doc);
    }

    final IndexReader r = w.getReader();
    w.close();

    final IndexSearcher s = newSearcher(r);
    PhraseQuery pq = new PhraseQuery();
    pq.add(new Term("content", "silly"));
    pq.add(new Term("content", "content"));
    assertEquals(0, s.search(pq, 1).totalHits);

    pq = new PhraseQuery();
    pq.add(new Term("content", "good"));
    pq.add(new Term("content", "content"));
    assertEquals(numDocs1+numDocs2, s.search(pq, 1).totalHits);
    r.close();
    dir.close();
  }


  public void testUpdateDocsNonAbortingException() throws Exception {
    final Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    final int numDocs1 = random().nextInt(25);
    for(int docCount=0;docCount<numDocs1;docCount++) {
      Document doc = new Document();
      doc.add(newTextField("content", "good content", Field.Store.NO));
      w.addDocument(doc);
    }

    // Use addDocs (no exception) to get docs in the index:
    final List<Document> docs = new ArrayList<Document>();
    final int numDocs2 = random().nextInt(25);
    for(int docCount=0;docCount<numDocs2;docCount++) {
      Document doc = new Document();
      docs.add(doc);
      doc.add(newStringField("subid", "subs", Field.Store.NO));
      doc.add(newStringField("id", docCount+"", Field.Store.NO));
      doc.add(newTextField("content", "silly content " + docCount, Field.Store.NO));
    }
    w.addDocuments(docs);

    final int numDocs3 = random().nextInt(25);
    for(int docCount=0;docCount<numDocs3;docCount++) {
      Document doc = new Document();
      doc.add(newTextField("content", "good content", Field.Store.NO));
      w.addDocument(doc);
    }

    docs.clear();
    final int limit = _TestUtil.nextInt(random(), 2, 25);
    final int crashAt = random().nextInt(limit);
    for(int docCount=0;docCount<limit;docCount++) {
      Document doc = new Document();
      docs.add(doc);
      doc.add(newStringField("id", docCount+"", Field.Store.NO));
      doc.add(newTextField("content", "silly content " + docCount, Field.Store.NO));
      if (docCount == crashAt) {
        Field f = newTextField("crash", "", Field.Store.NO);
        doc.add(f);
        MockTokenizer tokenizer = new MockTokenizer(new StringReader("crash me on the 4th token"), MockTokenizer.WHITESPACE, false);
        tokenizer.setEnableChecks(false); // disable workflow checking as we forcefully close() in exceptional cases.
        f.setTokenStream(new CrashingFilter("crash", tokenizer));
      }
    }

    try {
      w.updateDocuments(new Term("subid", "subs"), docs);
      // BUG: CrashingFilter didn't
      fail("did not hit expected exception");
    } catch (IOException ioe) {
      // expected
      assertEquals(CRASH_FAIL_MESSAGE, ioe.getMessage());
    }

    final int numDocs4 = random().nextInt(25);
    for(int docCount=0;docCount<numDocs4;docCount++) {
      Document doc = new Document();
      doc.add(newTextField("content", "good content", Field.Store.NO));
      w.addDocument(doc);
    }

    final IndexReader r = w.getReader();
    w.close();

    final IndexSearcher s = newSearcher(r);
    PhraseQuery pq = new PhraseQuery();
    pq.add(new Term("content", "silly"));
    pq.add(new Term("content", "content"));
    assertEquals(numDocs2, s.search(pq, 1).totalHits);

    pq = new PhraseQuery();
    pq.add(new Term("content", "good"));
    pq.add(new Term("content", "content"));
    assertEquals(numDocs1+numDocs3+numDocs4, s.search(pq, 1).totalHits);
    r.close();
    dir.close();
  }
  
  static class UOEDirectory extends RAMDirectory {
    boolean doFail = false;

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
      if (doFail && name.startsWith("segments_")) {
        StackTraceElement[] trace = new Exception().getStackTrace();
        for (int i = 0; i < trace.length; i++) {
          if ("read".equals(trace[i].getMethodName())) {
            throw new UnsupportedOperationException("expected UOE");
          }
        }
      }
      return super.openInput(name, context);
    }
  }
  
  public void testExceptionOnCtor() throws Exception {
    UOEDirectory uoe = new UOEDirectory();
    Directory d = new MockDirectoryWrapper(random(), uoe);
    IndexWriter iw = new IndexWriter(d, newIndexWriterConfig(TEST_VERSION_CURRENT, null));
    iw.addDocument(new Document());
    iw.close();
    uoe.doFail = true;
    try {
      new IndexWriter(d, newIndexWriterConfig(TEST_VERSION_CURRENT, null));
      fail("should have gotten a UOE");
    } catch (UnsupportedOperationException expected) {
    }

    uoe.doFail = false;
    d.close();
  }
  
  public void testIllegalPositions() throws Exception {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, null));
    Document doc = new Document();
    Token t1 = new Token("foo", 0, 3);
    t1.setPositionIncrement(Integer.MAX_VALUE);
    Token t2 = new Token("bar", 4, 7);
    t2.setPositionIncrement(200);
    TokenStream overflowingTokenStream = new CannedTokenStream(
        new Token[] { t1, t2 }
    );
    Field field = new TextField("foo", overflowingTokenStream);
    doc.add(field);
    try {
      iw.addDocument(doc);
      fail();
    } catch (IllegalArgumentException expected) {
      // expected exception
    }
    iw.close();
    dir.close();
  }
  
  public void testLegalbutVeryLargePositions() throws Exception {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, null));
    Document doc = new Document();
    Token t1 = new Token("foo", 0, 3);
    t1.setPositionIncrement(Integer.MAX_VALUE-500);
    if (random().nextBoolean()) {
      t1.setPayload(new BytesRef(new byte[] { 0x1 } ));
    }
    TokenStream overflowingTokenStream = new CannedTokenStream(
        new Token[] { t1 }
    );
    Field field = new TextField("foo", overflowingTokenStream);
    doc.add(field);
    iw.addDocument(doc);
    iw.close();
    dir.close();
  }
  
  public void testBoostOmitNorms() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    iwc.setMergePolicy(newLogMergePolicy());
    IndexWriter iw = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new StringField("field1", "sometext", Field.Store.YES));
    doc.add(new TextField("field2", "sometext", Field.Store.NO));
    doc.add(new StringField("foo", "bar", Field.Store.NO));
    iw.addDocument(doc); // add an 'ok' document
    try {
      doc = new Document();
      // try to boost with norms omitted
      List<IndexableField> list = new ArrayList<IndexableField>();
      list.add(new IndexableField() {

        @Override
        public String name() {
          return "foo";
        }

        @Override
        public IndexableFieldType fieldType() {
          return StringField.TYPE_NOT_STORED;
        }

        @Override
        public float boost() {
          return 5f;
        }

        @Override
        public BytesRef binaryValue() {
          return null;
        }

        @Override
        public String stringValue() {
          return "baz";
        }

        @Override
        public Reader readerValue() {
          return null;
        }

        @Override
        public Number numericValue() {
          return null;
        }

        @Override
        public TokenStream tokenStream(Analyzer analyzer) throws IOException {
          return null;
        }
      });
      iw.addDocument(list);
      fail("didn't get any exception, boost silently discarded");
    } catch (UnsupportedOperationException expected) {
      // expected
    }
    DirectoryReader ir = DirectoryReader.open(iw, false);
    assertEquals(1, ir.numDocs());
    assertEquals("sometext", ir.document(0).get("field1"));
    ir.close();
    iw.close();
    dir.close();
  }
  
  // See LUCENE-4870 TooManyOpenFiles errors are thrown as
  // FNFExceptions which can trigger data loss.
  public void testTooManyFileException() throws Exception {

    // Create failure that throws Too many open files exception randomly
    MockDirectoryWrapper.Failure failure = new MockDirectoryWrapper.Failure() {

      @Override
      public MockDirectoryWrapper.Failure reset() {
        doFail = false;
        return this;
      }

      @Override
      public void eval(MockDirectoryWrapper dir) throws IOException {
        if (doFail) {
          if (random().nextBoolean()) {
            throw new FileNotFoundException("some/file/name.ext (Too many open files)");
          }
        }
      }
    };

    MockDirectoryWrapper dir = newMockDirectory();
    // The exception is only thrown on open input
    dir.setFailOnOpenInput(true);
    dir.failOn(failure);

    // Create an index with one document
    IndexWriterConfig iwc = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    IndexWriter iw = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new StringField("foo", "bar", Field.Store.NO));
    iw.addDocument(doc); // add a document
    iw.commit();
    DirectoryReader ir = DirectoryReader.open(dir);
    assertEquals(1, ir.numDocs());
    ir.close();
    iw.close();

    // Open and close the index a few times
    for (int i = 0; i < 10; i++) {
      failure.setDoFail();
      iwc = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
      try {
        iw = new IndexWriter(dir, iwc);
      } catch (CorruptIndexException ex) {
        // Exceptions are fine - we are running out of file handlers here
        continue;
      } catch (FileNotFoundException ex) {
        continue;
      }
      failure.clearDoFail();
      iw.close();
      ir = DirectoryReader.open(dir);
      assertEquals("lost document after iteration: " + i, 1, ir.numDocs());
      ir.close();
    }

    // Check if document is still there
    failure.clearDoFail();
    ir = DirectoryReader.open(dir);
    assertEquals(1, ir.numDocs());
    ir.close();

    dir.close();
  }
  
  

}
