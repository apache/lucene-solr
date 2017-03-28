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


import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
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
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.BaseDirectoryWrapper;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.store.MockDirectoryWrapper.FakeIOException;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.lucene.util.TestUtil;

@SuppressCodecs("SimpleText") // too slow here
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
      custom2.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
      
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
      doc.add(new NumericDocValuesField("numericdv", 5));
      doc.add(new BinaryDocValuesField("binarydv", new BytesRef("hello")));
      doc.add(new SortedDocValuesField("sorteddv", new BytesRef("world")));
      doc.add(new SortedSetDocValuesField("sortedsetdv", new BytesRef("hellllo")));
      doc.add(new SortedSetDocValuesField("sortedsetdv", new BytesRef("again")));
      doc.add(new SortedNumericDocValuesField("sortednumericdv", 10));
      doc.add(new SortedNumericDocValuesField("sortednumericdv", 5));

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
            writer.updateDocuments(idTerm, new DocCopyIterator(doc, TestUtil.nextInt(r, 1, 20)));
          } else {
            writer.updateDocument(idTerm, doc);
          }
        } catch (RuntimeException re) {
          if (VERBOSE) {
            System.out.println(Thread.currentThread().getName() + ": EXC: ");
            re.printStackTrace(System.out);
          }
          try {
            TestUtil.checkIndex(writer.getDirectory());
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

  ThreadLocal<Thread> doFail = new ThreadLocal<>();

  private class TestPoint1 implements RandomIndexWriter.TestPoint {
    Random r = new Random(random().nextLong());
    @Override
    public void apply(String name) {
      if (doFail.get() != null && !name.equals("startDoFlush") && r.nextInt(40) == 17) {
        if (VERBOSE) {
          System.out.println(Thread.currentThread().getName() + ": NOW FAIL: " + name);
          new Throwable().printStackTrace(System.out);
        }
        throw new RuntimeException(Thread.currentThread().getName() + ": intentionally failing at " + name);
      }
    }
  }

  public void testRandomExceptions() throws Throwable {
    if (VERBOSE) {
      System.out.println("\nTEST: start testRandomExceptions");
    }
    Directory dir = newDirectory();

    MockAnalyzer analyzer = new MockAnalyzer(random());
    analyzer.setEnableChecks(false); // disable workflow checking as we forcefully close() in exceptional cases.
    
    IndexWriter writer  = RandomIndexWriter.mockIndexWriter(random(), dir, newIndexWriterConfig(analyzer)
                                                                   .setRAMBufferSizeMB(0.1)
                                                                   .setMergeScheduler(new ConcurrentMergeScheduler()), new TestPoint1());
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
    IndexWriter writer  = RandomIndexWriter.mockIndexWriter(random(), dir, newIndexWriterConfig(analyzer)
                                                 .setRAMBufferSizeMB(0.2)
                                                 .setMergeScheduler(new ConcurrentMergeScheduler()), new TestPoint1());
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
  private static final class TestPoint2 implements RandomIndexWriter.TestPoint {
    boolean doFail;

    @Override
    public void apply(String name) {
      if (doFail && name.equals("DocumentsWriterPerThread addDocument start"))
        throw new RuntimeException("intentionally failing");
    }
  }

  private static String CRASH_FAIL_MESSAGE = "I'm experiencing problems";

  private static class CrashingFilter extends TokenFilter {
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
    TestPoint2 testPoint = new TestPoint2();
    IndexWriter w = RandomIndexWriter.mockIndexWriter(random(), dir, newIndexWriterConfig(new MockAnalyzer(random())), testPoint);
    Document doc = new Document();
    doc.add(newTextField("field", "a field", Field.Store.YES));
    w.addDocument(doc);

    testPoint.doFail = true;
    expectThrows(RuntimeException.class, () -> {
      w.addDocument(doc);
    });

    w.close();
    dir.close();
  }

  // LUCENE-1208
  public void testExceptionJustBeforeFlush() throws IOException {
    Directory dir = newDirectory();

    final AtomicBoolean doCrash = new AtomicBoolean();

    Analyzer analyzer = new Analyzer(Analyzer.PER_FIELD_REUSE_STRATEGY) {
      @Override
      public TokenStreamComponents createComponents(String fieldName) {
        MockTokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
        tokenizer.setEnableChecks(false); // disable workflow checking as we forcefully close() in exceptional cases.
        TokenStream stream = tokenizer;
        if (doCrash.get()) {
          stream = new CrashingFilter(fieldName, stream);
        }
        return new TokenStreamComponents(tokenizer, stream);
      }
    };

    IndexWriter w = RandomIndexWriter.mockIndexWriter(random(), dir, 
                                                      newIndexWriterConfig(analyzer)
                                                        .setMaxBufferedDocs(2), 
                                                      new TestPoint1());
    Document doc = new Document();
    doc.add(newTextField("field", "a field", Field.Store.YES));
    w.addDocument(doc);

    Document crashDoc = new Document();
    crashDoc.add(newTextField("crash", "do it on token 4", Field.Store.YES));
    doCrash.set(true);
    expectThrows(IOException.class, () -> {
      w.addDocument(crashDoc);
    });

    w.addDocument(doc);
    w.close();
    dir.close();
  }

  private static final class TestPoint3 implements RandomIndexWriter.TestPoint {
    boolean doFail;
    boolean failed;
    @Override
    public void apply(String name) {
      if (doFail && name.equals("startMergeInit")) {
        failed = true;
        throw new RuntimeException("intentionally failing");
      }
    }
  }


  // LUCENE-1210
  public void testExceptionOnMergeInit() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()))
      .setMaxBufferedDocs(2)
      .setMergePolicy(newLogMergePolicy());
    ConcurrentMergeScheduler cms = new ConcurrentMergeScheduler();
    cms.setSuppressExceptions();
    conf.setMergeScheduler(cms);
    ((LogMergePolicy) conf.getMergePolicy()).setMergeFactor(2);
    TestPoint3 testPoint = new TestPoint3();
    IndexWriter w = RandomIndexWriter.mockIndexWriter(random(), dir, conf, testPoint);
    testPoint.doFail = true;
    Document doc = new Document();
    doc.add(newTextField("field", "a field", Field.Store.YES));
    for(int i=0;i<10;i++) {
      try {
        w.addDocument(doc);
      } catch (RuntimeException re) {
        break;
      }
    }

    try {
      ((ConcurrentMergeScheduler) w.getConfig().getMergeScheduler()).sync();
    } catch (IllegalStateException ise) {
      // OK: merge exc causes tragedy
    }
    assertTrue(testPoint.failed);
    w.close();
    dir.close();
  }

  // LUCENE-1072
  public void testExceptionFromTokenStream() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new Analyzer() {

      @Override
      public TokenStreamComponents createComponents(String fieldName) {
        MockTokenizer tokenizer = new MockTokenizer(MockTokenizer.SIMPLE, true);
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

    Document brokenDoc = new Document();
    String contents = "aa bb cc dd ee ff gg hh ii jj kk";
    brokenDoc.add(newTextField("content", contents, Field.Store.NO));
    expectThrows(Exception.class, () -> {
      writer.addDocument(brokenDoc);
    });

    // Make sure we can add another normal document
    Document doc = new Document();
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
    PostingsEnum tdocs = TestUtil.docs(random(), reader,
        t.field(),
        new BytesRef(t.text()),
        null,
        0);

    final Bits liveDocs = MultiFields.getLiveDocs(reader);
    int count = 0;
    while(tdocs.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
      if (liveDocs == null || liveDocs.get(tdocs.docID())) {
        count++;
      }
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
        boolean sawFlush = false;
        boolean sawFinishDocument = false;
        for (int i = 0; i < trace.length; i++) {
          if ("flush".equals(trace[i].getMethodName())) {
            sawFlush = true;
          }
          if ("finishDocument".equals(trace[i].getMethodName())) {
            sawFinishDocument = true;
          }
        }

        if (sawFlush && sawFinishDocument == false && count++ >= 30) {
          doFail = false;
          throw new IOException("now failing during flush");
        }
      }
    }
  }

  // make sure an aborting exception closes the writer:
  public void testDocumentsWriterAbort() throws IOException {
    MockDirectoryWrapper dir = newMockDirectory();
    FailOnlyOnFlush failure = new FailOnlyOnFlush();
    failure.setDoFail();
    dir.failOn(failure);

    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
                                                .setMaxBufferedDocs(2));
    Document doc = new Document();
    String contents = "aa bb cc dd ee ff gg hh ii jj kk";
    doc.add(newTextField("content", contents, Field.Store.NO));
    boolean hitError = false;
    writer.addDocument(doc);

    expectThrows(IOException.class, () -> {
      writer.addDocument(doc);
    });

    // only one flush should fail:
    assertFalse(hitError);
    hitError = true;
    assertTrue(writer.deleter.isClosed());
    assertTrue(writer.isClosed());
    assertFalse(DirectoryReader.indexExists(dir));

    dir.close();
  }

  public void testDocumentsWriterExceptions() throws IOException {
    Analyzer analyzer = new Analyzer(Analyzer.PER_FIELD_REUSE_STRATEGY) {
      @Override
      public TokenStreamComponents createComponents(String fieldName) {
        MockTokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
        tokenizer.setEnableChecks(false); // disable workflow checking as we forcefully close() in exceptional cases.
        return new TokenStreamComponents(tokenizer, new CrashingFilter(fieldName, tokenizer));
      }
    };

    for(int i=0;i<2;i++) {
      if (VERBOSE) {
        System.out.println("TEST: cycle i=" + i);
      }
      Directory dir = newDirectory();
      IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(analyzer)
                                                  .setMergePolicy(newLogMergePolicy()));

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

      writer = new IndexWriter(dir, newIndexWriterConfig(analyzer)
                                      .setMaxBufferedDocs(10));
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
    Analyzer analyzer = new Analyzer(Analyzer.PER_FIELD_REUSE_STRATEGY) {
      @Override
      public TokenStreamComponents createComponents(String fieldName) {
        MockTokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
        tokenizer.setEnableChecks(false); // disable workflow checking as we forcefully close() in exceptional cases.
        return new TokenStreamComponents(tokenizer, new CrashingFilter(fieldName, tokenizer));
      }
    };

    final int NUM_THREAD = 3;
    final int NUM_ITER = 100;

    for(int i=0;i<2;i++) {
      Directory dir = newDirectory();

      {
        final IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(analyzer)
            .setMaxBufferedDocs(-1)
            .setMergePolicy(NoMergePolicy.INSTANCE));
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
                    expectThrows(IOException.class, () -> {
                      writer.addDocument(doc);
                    });

                    if (0 == finalI) {
                      Document extraDoc = new Document();
                      extraDoc.add(newField("contents", "here are some contents", DocCopyIterator.custom5));
                      writer.addDocument(extraDoc);
                      writer.addDocument(extraDoc);
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

      IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(analyzer)
                                                  .setMaxBufferedDocs(10));
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
        newIndexWriterConfig(new MockAnalyzer(random()))
            .setMaxBufferedDocs(2)
            .setMergeScheduler(new ConcurrentMergeScheduler())
            .setMergePolicy(newLogMergePolicy(5))
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
        if (isCommit && isDelete && isInGlobalFieldMap) {
          break;
        }
        if (SegmentInfos.class.getName().equals(trace[i].getClassName()) && stage.equals(trace[i].getMethodName())) {
          isCommit = true;
        }
        if (MockDirectoryWrapper.class.getName().equals(trace[i].getClassName()) && "deleteFile".equals(trace[i].getMethodName())) {
          isDelete = true;
        }
        if (SegmentInfos.class.getName().equals(trace[i].getClassName()) && "writeGlobalFieldMap".equals(trace[i].getMethodName())) {
          isInGlobalFieldMap = true;
        }
          
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
      int fileCount = dir.listAll().length;
      IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
      Document doc = new Document();
      doc.add(newTextField("field", "a field", Field.Store.YES));
      w.addDocument(doc);
      dir.failOn(failure);
      expectThrows(RuntimeException.class, () -> {
        w.close();
      });

      assertTrue("failOnCommit=" + failure.failOnCommit + " failOnDeleteFile=" + failure.failOnDeleteFile, failure.failOnCommit && failure.failOnDeleteFile);
      w.rollback();
      String files[] = dir.listAll();
      assertTrue(files.length == fileCount || (files.length == fileCount+1 && Arrays.asList(files).contains(IndexWriter.WRITE_LOCK_NAME)));
      dir.close();
    }
  }

  public void testForceMergeExceptions() throws IOException {
    Directory startDir = newDirectory();
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()))
                               .setMaxBufferedDocs(2)
                               .setMergePolicy(newLogMergePolicy());
    ((LogMergePolicy) conf.getMergePolicy()).setMergeFactor(100);
    IndexWriter w = new IndexWriter(startDir, conf);
    for(int i=0;i<27;i++) {
      addDoc(w);
    }
    w.close();

    int iter = TEST_NIGHTLY ? 200 : 10;
    for(int i=0;i<iter;i++) {
      if (VERBOSE) {
        System.out.println("\nTEST: iter " + i);
      }
      MockDirectoryWrapper dir = new MockDirectoryWrapper(random(), TestUtil.ramCopyOf(startDir));
      conf = newIndexWriterConfig(new MockAnalyzer(random()))
               .setMergeScheduler(new ConcurrentMergeScheduler());
      ((ConcurrentMergeScheduler) conf.getMergeScheduler()).setSuppressExceptions();
      w = new IndexWriter(dir, conf);
      dir.setRandomIOExceptionRate(0.5);
      try {
        w.forceMerge(1);
      } catch (IllegalStateException ise) {
        // expected
      } catch (IOException ioe) {
        if (ioe.getCause() == null) {
          fail("forceMerge threw IOException without root cause");
        }
      }
      dir.setRandomIOExceptionRate(0);
      //System.out.println("TEST: now close IW");
      try {
        w.close();
      } catch (IllegalStateException ise) {
        // ok
      }
      dir.close();
    }
    startDir.close();
  }

  // LUCENE-1429
  public void testOutOfMemoryErrorCausesCloseToFail() throws Exception {

    final AtomicBoolean thrown = new AtomicBoolean(false);
    final Directory dir = newDirectory();
    final IndexWriter writer = new IndexWriter(dir,
        newIndexWriterConfig(new MockAnalyzer(random()))
          .setInfoStream(new InfoStream() {
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

    expectThrows(OutOfMemoryError.class, () -> {
      writer.close();
    });

    // throws IllegalStateEx w/o bug fix
    writer.close();
    dir.close();
  }

  /** If IW hits OOME during indexing, it should refuse to commit any further changes. */
  public void testOutOfMemoryErrorRollback() throws Exception {

    final AtomicBoolean thrown = new AtomicBoolean(false);
    final Directory dir = newDirectory();
    final IndexWriter writer = new IndexWriter(dir,
        newIndexWriterConfig(new MockAnalyzer(random()))
          .setInfoStream(new InfoStream() {
        @Override
        public void message(String component, final String message) {
          if (message.contains("startFullFlush") && thrown.compareAndSet(false, true)) {
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
    writer.addDocument(new Document());

    expectThrows(OutOfMemoryError.class, () -> {
      writer.commit();
    });

    try {
      writer.close();
    } catch (IllegalArgumentException ok) {
      // ok
    }

    expectThrows(AlreadyClosedException.class, () -> {
      writer.addDocument(new Document());
    });

    // IW should have done rollback() during close, since it hit OOME, and so no index should exist:
    assertFalse(DirectoryReader.indexExists(dir));

    dir.close();
  }

  // LUCENE-1347
  private static final class TestPoint4 implements RandomIndexWriter.TestPoint {

    boolean doFail;

    @Override
    public void apply(String name) {
      if (doFail && name.equals("rollback before checkpoint"))
        throw new RuntimeException("intentionally failing");
    }
  }

  // LUCENE-1347
  public void testRollbackExceptionHang() throws Throwable {
    Directory dir = newDirectory();
    TestPoint4 testPoint = new TestPoint4();
    IndexWriter w = RandomIndexWriter.mockIndexWriter(random(), dir, newIndexWriterConfig(new MockAnalyzer(random())), testPoint);
    

    addDoc(w);
    testPoint.doFail = true;
    expectThrows(RuntimeException.class, () -> {
      w.rollback();
    });

    testPoint.doFail = false;
    w.rollback();
    dir.close();
  }

  // LUCENE-1044: Simulate checksum error in segments_N
  public void testSegmentsChecksumError() throws IOException {
    BaseDirectoryWrapper dir = newDirectory();
    dir.setCheckIndexOnClose(false); // we corrupt the index

    IndexWriter writer = null;

    writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));

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

    expectThrows(CorruptIndexException.class, () -> {
      DirectoryReader.open(dir);
    });

    dir.close();
  }

  // Simulate a corrupt index by removing last byte of
  // latest segments file and make sure we get an
  // IOException trying to open the index:
  public void testSimulatedCorruptIndex1() throws IOException {
      BaseDirectoryWrapper dir = newDirectory();
      dir.setCheckIndexOnClose(false); // we are corrupting it!

      IndexWriter writer = null;

      writer  = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));

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

      expectThrows(Exception.class, () -> {
        DirectoryReader.open(dir);
      });

      dir.close();
  }

  // Simulate a corrupt index by removing one of the
  // files and make sure we get an IOException trying to
  // open the index:
  public void testSimulatedCorruptIndex2() throws IOException {
    BaseDirectoryWrapper dir = newDirectory();
    dir.setCheckIndexOnClose(false); // we are corrupting it!
    IndexWriter writer = null;

    writer  = new IndexWriter(
                              dir,
                              newIndexWriterConfig(new MockAnalyzer(random()))
                                .setMergePolicy(newLogMergePolicy(true))
                                .setUseCompoundFile(true)
                              );
    MergePolicy lmp = writer.getConfig().getMergePolicy();
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
    
    boolean corrupted = false;
    SegmentInfos sis = SegmentInfos.readLatestCommit(dir);
    for (SegmentCommitInfo si : sis) {
      assertTrue(si.info.getUseCompoundFile());
      List<String> victims = new ArrayList<String>(si.info.files());
      Collections.shuffle(victims, random());
      dir.deleteFile(victims.get(0));
      corrupted = true;
      break;
    }

    assertTrue("failed to find cfs file to remove: ", corrupted);

    expectThrows(Exception.class, () -> {
      DirectoryReader.open(dir);
    });

    dir.close();
  }

  public void testTermVectorExceptions() throws IOException {
    FailOnTermVectors[] failures = new FailOnTermVectors[] {
        new FailOnTermVectors(FailOnTermVectors.AFTER_INIT_STAGE),
        new FailOnTermVectors(FailOnTermVectors.INIT_STAGE), };
    int num = atLeast(1);
    iters:
    for (int j = 0; j < num; j++) {
      for (FailOnTermVectors failure : failures) {
        MockDirectoryWrapper dir = newMockDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
        dir.failOn(failure);
        int numDocs = 10 + random().nextInt(30);
        for (int i = 0; i < numDocs; i++) {
          Document doc = new Document();
          // random TV
          Field field = newTextField(random(), "field", "a field", Field.Store.YES);
          doc.add(field);
          try {
            w.addDocument(doc);
            assertFalse(field.fieldType().storeTermVectors());
          } catch (RuntimeException e) {
            assertTrue(e.getMessage().startsWith(FailOnTermVectors.EXC_MSG));
            // This is an aborting exception, so writer is closed:
            assertTrue(w.deleter.isClosed());
            assertTrue(w.isClosed());
            dir.close();
            continue iters;
          }
          if (random().nextInt(20) == 0) {
            w.commit();
            TestUtil.checkIndex(dir);
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
            TestUtil.checkIndex(dir);
          }
        }
        document = new Document();
        document.add(new TextField("field", "a field", Field.Store.YES));
        w.addDocument(document);
        w.close();
        IndexReader reader = DirectoryReader.open(dir);
        assertTrue(reader.numDocs() > 0);
        SegmentInfos sis = SegmentInfos.readLatestCommit(dir);
        for(LeafReaderContext context : reader.leaves()) {
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
          break;
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
    
    final List<Document> docs = new ArrayList<>();
    for(int docCount=0;docCount<7;docCount++) {
      Document doc = new Document();
      docs.add(doc);
      doc.add(newStringField("id", docCount+"", Field.Store.NO));
      doc.add(newTextField("content", "silly content " + docCount, Field.Store.NO));
      if (docCount == 4) {
        Field f = newTextField("crash", "", Field.Store.NO);
        doc.add(f);
        MockTokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
        tokenizer.setReader(new StringReader("crash me on the 4th token"));
        tokenizer.setEnableChecks(false); // disable workflow checking as we forcefully close() in exceptional cases.
        f.setTokenStream(new CrashingFilter("crash", tokenizer));
      }
    }

    IOException expected = expectThrows(IOException.class, () -> {
      w.addDocuments(docs);
    });
    assertEquals(CRASH_FAIL_MESSAGE, expected.getMessage());

    final int numDocs2 = random().nextInt(25);
    for(int docCount=0;docCount<numDocs2;docCount++) {
      Document doc = new Document();
      doc.add(newTextField("content", "good content", Field.Store.NO));
      w.addDocument(doc);
    }

    final IndexReader r = w.getReader();
    w.close();

    final IndexSearcher s = newSearcher(r);
    PhraseQuery pq = new PhraseQuery("content", "silly", "good");
    assertEquals(0, s.search(pq, 1).totalHits);

    pq = new PhraseQuery("content", "good", "content");
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
    final List<Document> docs = new ArrayList<>();
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
    final int limit = TestUtil.nextInt(random(), 2, 25);
    final int crashAt = random().nextInt(limit);
    for(int docCount=0;docCount<limit;docCount++) {
      Document doc = new Document();
      docs.add(doc);
      doc.add(newStringField("id", docCount+"", Field.Store.NO));
      doc.add(newTextField("content", "silly content " + docCount, Field.Store.NO));
      if (docCount == crashAt) {
        Field f = newTextField("crash", "", Field.Store.NO);
        doc.add(f);
        MockTokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
        tokenizer.setReader(new StringReader("crash me on the 4th token"));
        tokenizer.setEnableChecks(false); // disable workflow checking as we forcefully close() in exceptional cases.
        f.setTokenStream(new CrashingFilter("crash", tokenizer));
      }
    }

    IOException expected = expectThrows(IOException.class, () -> {
      w.updateDocuments(new Term("subid", "subs"), docs);
    });
    assertEquals(CRASH_FAIL_MESSAGE, expected.getMessage());

    final int numDocs4 = random().nextInt(25);
    for(int docCount=0;docCount<numDocs4;docCount++) {
      Document doc = new Document();
      doc.add(newTextField("content", "good content", Field.Store.NO));
      w.addDocument(doc);
    }

    final IndexReader r = w.getReader();
    w.close();

    final IndexSearcher s = newSearcher(r);
    PhraseQuery pq = new PhraseQuery("content", "silly", "content");
    assertEquals(numDocs2, s.search(pq, 1).totalHits);

    pq = new PhraseQuery("content", "good", "content");
    assertEquals(numDocs1+numDocs3+numDocs4, s.search(pq, 1).totalHits);
    r.close();
    dir.close();
  }
  
  /** test a null string value doesn't abort the entire segment */
  public void testNullStoredField() throws Exception {
    Directory dir = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig(analyzer));
    // add good document
    Document doc = new Document();
    iw.addDocument(doc);
    expectThrows(IllegalArgumentException.class, () -> {
      // set to null value
      String value = null;
      doc.add(new StoredField("foo", value));
      iw.addDocument(doc);
    });

    assertNull(iw.getTragicException());
    iw.close();
    // make sure we see our good doc
    DirectoryReader r = DirectoryReader.open(dir);
    assertEquals(1, r.numDocs());
    r.close();
    dir.close();
  }
  
  /** test a null string value doesn't abort the entire segment */
  public void testNullStoredFieldReuse() throws Exception {
    Directory dir = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig(analyzer));
    // add good document
    Document doc = new Document();
    Field theField = new StoredField("foo", "hello", StoredField.TYPE);
    doc.add(theField);
    iw.addDocument(doc);
    expectThrows(IllegalArgumentException.class, () -> {
      // set to null value
      theField.setStringValue(null);
      iw.addDocument(doc);
    });

    assertNull(iw.getTragicException());
    iw.close();
    // make sure we see our good doc
    DirectoryReader r = DirectoryReader.open(dir);
    assertEquals(1, r.numDocs());
    r.close();
    dir.close();
  }
  
  /** test a null byte[] value doesn't abort the entire segment */
  public void testNullStoredBytesField() throws Exception {
    Directory dir = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig(analyzer));
    // add good document
    Document doc = new Document();
    iw.addDocument(doc);

    expectThrows(NullPointerException.class, () -> {
      // set to null value
      byte v[] = null;
      Field theField = new StoredField("foo", v);
      doc.add(theField);
      iw.addDocument(doc);
    });

    assertNull(iw.getTragicException());
    iw.close();
    // make sure we see our good doc
    DirectoryReader r = DirectoryReader.open(dir);
    assertEquals(1, r.numDocs());
    r.close();
    dir.close();
  }
  
  /** test a null byte[] value doesn't abort the entire segment */
  public void testNullStoredBytesFieldReuse() throws Exception {
    Directory dir = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig(analyzer));
    // add good document
    Document doc = new Document();
    Field theField = new StoredField("foo", new BytesRef("hello").bytes);
    doc.add(theField);
    iw.addDocument(doc);
    expectThrows(NullPointerException.class, () -> {
      // set to null value
      byte v[] = null;
      theField.setBytesValue(v);
      iw.addDocument(doc);
    });

    assertNull(iw.getTragicException());
    iw.close();
    // make sure we see our good doc
    DirectoryReader r = DirectoryReader.open(dir);
    assertEquals(1, r.numDocs());
    r.close();
    dir.close();
  }
  
  /** test a null bytesref value doesn't abort the entire segment */
  public void testNullStoredBytesRefField() throws Exception {
    Directory dir = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig(analyzer));
    // add good document
    Document doc = new Document();
    iw.addDocument(doc);

    expectThrows(IllegalArgumentException.class, () -> {
      // set to null value
      BytesRef v = null;
      Field theField = new StoredField("foo", v);
      doc.add(theField);
      iw.addDocument(doc);
      fail("didn't get expected exception");
    });

    assertNull(iw.getTragicException());
    iw.close();
    // make sure we see our good doc
    DirectoryReader r = DirectoryReader.open(dir);
    assertEquals(1, r.numDocs());
    r.close();
    dir.close();
  }
  
  /** test a null bytesref value doesn't abort the entire segment */
  public void testNullStoredBytesRefFieldReuse() throws Exception {
    Directory dir = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig(analyzer));
    // add good document
    Document doc = new Document();
    Field theField = new StoredField("foo", new BytesRef("hello"));
    doc.add(theField);
    iw.addDocument(doc);
    expectThrows(IllegalArgumentException.class, () -> {
      // set to null value
      BytesRef v = null;
      theField.setBytesValue(v);
      iw.addDocument(doc);
      fail("didn't get expected exception");
    });

    assertNull(iw.getTragicException());
    iw.close();
    // make sure we see our good doc
    DirectoryReader r = DirectoryReader.open(dir);
    assertEquals(1, r.numDocs());
    r.close();
    dir.close();
  }
  
  public void testCrazyPositionIncrementGap() throws Exception {
    Directory dir = newDirectory();
    Analyzer analyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        return new TokenStreamComponents(new MockTokenizer(MockTokenizer.KEYWORD, false));
      }

      @Override
      public int getPositionIncrementGap(String fieldName) {
        return -2;
      }
    };
    IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig(analyzer));
    // add good document
    Document doc = new Document();
    iw.addDocument(doc);
    expectThrows(IllegalArgumentException.class, () -> {
      doc.add(newTextField("foo", "bar", Field.Store.NO));
      doc.add(newTextField("foo", "bar", Field.Store.NO));
      iw.addDocument(doc);
    });

    assertNull(iw.getTragicException());
    iw.close();

    // make sure we see our good doc
    DirectoryReader r = DirectoryReader.open(dir);   
    assertEquals(1, r.numDocs());
    r.close();
    dir.close();
  }
  
  // TODO: we could also check isValid, to catch "broken" bytesref values, might be too much?
  
  static class UOEDirectory extends RAMDirectory {
    boolean doFail = false;

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
      if (doFail && name.startsWith("segments_")) {
        StackTraceElement[] trace = new Exception().getStackTrace();
        for (int i = 0; i < trace.length; i++) {
          if ("readCommit".equals(trace[i].getMethodName()) || "readLatestCommit".equals(trace[i].getMethodName())) {
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
    IndexWriter iw = new IndexWriter(d, newIndexWriterConfig(null));
    iw.addDocument(new Document());
    iw.close();
    uoe.doFail = true;
    expectThrows(UnsupportedOperationException.class, () -> {
      new IndexWriter(d, newIndexWriterConfig(null));
    });

    uoe.doFail = false;
    d.close();
  }

  public void testBoostOmitNorms() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setMergePolicy(newLogMergePolicy());
    IndexWriter iw = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new StringField("field1", "sometext", Field.Store.YES));
    doc.add(new TextField("field2", "sometext", Field.Store.NO));
    doc.add(new StringField("foo", "bar", Field.Store.NO));
    iw.addDocument(doc); // add an 'ok' document
    expectThrows(UnsupportedOperationException.class, () -> {
      // try to boost with norms omitted
      List<IndexableField> list = new ArrayList<>();
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
        public TokenStream tokenStream(Analyzer analyzer, TokenStream reuse) {
          return null;
        }
      });
      iw.addDocument(list);
    });

    DirectoryReader ir = DirectoryReader.open(iw);
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
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
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
      iwc = new IndexWriterConfig(new MockAnalyzer(random()));
      try {
        iw = new IndexWriter(dir, iwc);
      } catch (AssertionError ex) {
        // This is fine: we tripped IW's assert that all files it's about to fsync do exist:
        assertTrue(ex.getMessage().matches("file .* does not exist; files=\\[.*\\]"));
      } catch (CorruptIndexException ex) {
        // Exceptions are fine - we are running out of file handlers here
        continue;
      } catch (FileNotFoundException | NoSuchFileException ex) {
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

  // Make sure if we hit a transient IOException (e.g., disk
  // full), and then the exception stops (e.g., disk frees
  // up), so we successfully close IW or open an NRT
  // reader, we don't lose any deletes or updates:
  public void testNoLostDeletesOrUpdates() throws Throwable {
    int deleteCount = 0;
    int docBase = 0;
    int docCount = 0;

    MockDirectoryWrapper dir = newMockDirectory();
    final AtomicBoolean shouldFail = new AtomicBoolean();
    dir.failOn(new MockDirectoryWrapper.Failure() {
      
      @Override
      public void eval(MockDirectoryWrapper dir) throws IOException {
        if (shouldFail.get() == false) {
          // Only sometimes throw the exc, so we get
          // it sometimes on creating the file, on
          // flushing buffer, on closing the file:
          return;
        }
        
        if (random().nextInt(3) != 2) {
          return;
        }

        StackTraceElement[] trace = Thread.currentThread().getStackTrace();

        boolean sawSeal = false;
        boolean sawWrite = false;
        for (int i = 0; i < trace.length; i++) {
          if ("sealFlushedSegment".equals(trace[i].getMethodName())) {
            sawSeal = true;
            break;
          }
          if ("writeLiveDocs".equals(trace[i].getMethodName()) || "writeFieldUpdates".equals(trace[i].getMethodName())) {
            sawWrite = true;
          }
        }
        
        // Don't throw exc if we are "flushing", else
        // the segment is aborted and docs are lost:
        if (sawWrite && sawSeal == false) {
          if (VERBOSE) {
            System.out.println("TEST: now fail; thread=" + Thread.currentThread().getName() + " exc:");
            new Throwable().printStackTrace(System.out);
          }
          shouldFail.set(false);
          throw new FakeIOException();
        }
      }
    });
    
    RandomIndexWriter w = null;

    boolean tragic = false;

    for(int iter=0;iter<10*RANDOM_MULTIPLIER;iter++) {
      int numDocs = atLeast(100);
      if (VERBOSE) {
        System.out.println("\nTEST: iter=" + iter + " numDocs=" + numDocs + " docBase=" + docBase + " delCount=" + deleteCount);
      }
      if (w == null) {
        IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
        w = new RandomIndexWriter(random(), dir, iwc);
        // Since we hit exc during merging, a partial
        // forceMerge can easily return when there are still
        // too many segments in the index:
        w.setDoRandomForceMergeAssert(false);
      }
      for(int i=0;i<numDocs;i++) {
        Document doc = new Document();
        doc.add(new StringField("id", ""+(docBase+i), Field.Store.NO));
        doc.add(new NumericDocValuesField("f", 1L));
        doc.add(new NumericDocValuesField("cf", 2L));
        doc.add(new BinaryDocValuesField("bf", TestBinaryDocValuesUpdates.toBytes(1L)));
        doc.add(new BinaryDocValuesField("bcf", TestBinaryDocValuesUpdates.toBytes(2L)));
        w.addDocument(doc);
      }
      docCount += numDocs;

      // TODO: we could make the test more evil, by letting
      // it throw more than one exc, randomly, before "recovering"

      // TODO: we could also install an infoStream and try
      // to fail in "more evil" places inside BDS

      shouldFail.set(true);
      boolean doClose = false;
      try {
        for(int i=0;i<numDocs;i++) {
          if (random().nextInt(10) == 7) {
            boolean fieldUpdate = random().nextBoolean();
            int docid = docBase + i;
            if (fieldUpdate) {
              long value = iter;
              if (VERBOSE) {
                System.out.println("  update id=" + docid + " to value " + value);
              }
              Term idTerm = new Term("id", Integer.toString(docid));
              if (random().nextBoolean()) { // update only numeric field
                w.updateDocValues(idTerm, new NumericDocValuesField("f", value), new NumericDocValuesField("cf", value*2));
              } else if (random().nextBoolean()) {
                w.updateDocValues(idTerm, new BinaryDocValuesField("bf", TestBinaryDocValuesUpdates.toBytes(value)),
                    new BinaryDocValuesField("bcf", TestBinaryDocValuesUpdates.toBytes(value*2)));
              } else {
                w.updateDocValues(idTerm, 
                    new NumericDocValuesField("f", value), 
                    new NumericDocValuesField("cf", value*2),
                    new BinaryDocValuesField("bf", TestBinaryDocValuesUpdates.toBytes(value)),
                    new BinaryDocValuesField("bcf", TestBinaryDocValuesUpdates.toBytes(value*2)));
              }
            }
            
            // sometimes do both deletes and updates
            if (!fieldUpdate || random().nextBoolean()) {
              if (VERBOSE) {
                System.out.println("  delete id=" + docid);
              }
              deleteCount++;
              w.deleteDocuments(new Term("id", ""+docid));
            }
          }
        }

        // Trigger writeLiveDocs + writeFieldUpdates so we hit fake exc:
        IndexReader r = w.getReader();

        // Sometimes we will make it here (we only randomly
        // throw the exc):
        assertEquals(docCount-deleteCount, r.numDocs());
        r.close();
        
        // Sometimes close, so the disk full happens on close:
        if (random().nextBoolean()) {
          if (VERBOSE) {
            System.out.println("  now close writer");
          }
          doClose = true;
          w.commit();
          w.close();
          w = null;
        }

      } catch (Throwable t) {
        // FakeIOException can be thrown from mergeMiddle, in which case IW
        // registers it before our CMS gets to suppress it. IW.forceMerge later
        // throws it as a wrapped IOE, so don't fail in this case.
        if (t instanceof FakeIOException || (t.getCause() instanceof FakeIOException)) {
          // expected
          if (VERBOSE) {
            System.out.println("TEST: hit expected IOE");
          }
          if (t instanceof AlreadyClosedException) {
            // FakeIOExc struck during merge and writer is now closed:
            w = null;
            tragic = true;
          }
        } else {
          throw t;
        }
      }
      shouldFail.set(false);

      if (w != null) {
        MergeScheduler ms = w.w.getConfig().getMergeScheduler();
        if (ms instanceof ConcurrentMergeScheduler) {
          ((ConcurrentMergeScheduler) ms).sync();
        }

        if (w.w.getTragicException() != null) {
          // Tragic exc in CMS closed the writer
          w = null;
        }
      }

      IndexReader r;

      if (doClose && w != null) {
        if (VERBOSE) {
          System.out.println("  now 2nd close writer");
        }
        w.close();
        w = null;
      }

      if (w == null || random().nextBoolean()) {
        // Open non-NRT reader, to make sure the "on
        // disk" bits are good:
        if (VERBOSE) {
          System.out.println("TEST: verify against non-NRT reader");
        }
        if (w != null) {
          w.commit();
        }
        r = DirectoryReader.open(dir);
      } else {
        if (VERBOSE) {
          System.out.println("TEST: verify against NRT reader");
        }
        r = w.getReader();
      }
      if (tragic == false) {
        assertEquals(docCount-deleteCount, r.numDocs());
      }
      BytesRef scratch = new BytesRef();
      for (LeafReaderContext context : r.leaves()) {
        LeafReader reader = context.reader();
        Bits liveDocs = reader.getLiveDocs();
        NumericDocValues f = reader.getNumericDocValues("f");
        NumericDocValues cf = reader.getNumericDocValues("cf");
        BinaryDocValues bf = reader.getBinaryDocValues("bf");
        BinaryDocValues bcf = reader.getBinaryDocValues("bcf");
        for (int i = 0; i < reader.maxDoc(); i++) {
          if (liveDocs == null || liveDocs.get(i)) {
            assertEquals("doc=" + (docBase + i), cf.get(i), f.get(i) * 2);
            assertEquals("doc=" + (docBase + i), TestBinaryDocValuesUpdates.getValue(bcf, i), TestBinaryDocValuesUpdates.getValue(bf, i) * 2);
          }
        }
      }

      r.close();

      // Sometimes re-use RIW, other times open new one:
      if (w != null && random().nextBoolean()) {
        if (VERBOSE) {
          System.out.println("TEST: close writer");
        }
        w.close();
        w = null;
      }

      docBase += numDocs;
    }

    if (w != null) {
      w.close();
    }

    // Final verify:
    if (tragic == false) {
      IndexReader r = DirectoryReader.open(dir);
      assertEquals(docCount-deleteCount, r.numDocs());
      r.close();
    }

    dir.close();
  }
  
  // kind of slow, but omits positions, so just CPU
  @Nightly
  public void testTooManyTokens() throws Exception {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));
    Document doc = new Document();
    FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
    ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
    doc.add(new Field("foo", new TokenStream() {
      CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
      PositionIncrementAttribute posIncAtt = addAttribute(PositionIncrementAttribute.class);
      long num = 0;
      
      @Override
      public boolean incrementToken() throws IOException {
        if (num == Integer.MAX_VALUE + 1) {
          return false;
        }
        clearAttributes();
        if (num == 0) {
          posIncAtt.setPositionIncrement(1);
        } else {
          posIncAtt.setPositionIncrement(0);
        }
        termAtt.append("a");
        num++;
        if (VERBOSE && num % 1000000 == 0) {
          System.out.println("indexed: " + num);
        }
        return true;
      }
    }, ft));

    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      iw.addDocument(doc);
    });
    assertTrue(expected.getMessage().contains("too many tokens"));

    iw.close();
    dir.close();
  }
  
  public void testExceptionDuringRollback() throws Exception {
    // currently: fail in two different places
    final String messageToFailOn = random().nextBoolean() ? 
        "rollback: done finish merges" : "rollback before checkpoint";
    
    // infostream that throws exception during rollback
    InfoStream evilInfoStream = new InfoStream() {
      @Override
      public void message(String component, String message) {
        if (messageToFailOn.equals(message)) {
          throw new RuntimeException("BOOM!");
        }
      }

      @Override
      public boolean isEnabled(String component) {
        return true;
      }
      
      @Override
      public void close() throws IOException {}
    };
    
    Directory dir = newMockDirectory(); // we want to ensure we don't leak any locks or file handles
    IndexWriterConfig iwc = new IndexWriterConfig(null);
    iwc.setInfoStream(evilInfoStream);
    IndexWriter iw = new IndexWriter(dir, iwc);
    // TODO: cutover to RandomIndexWriter.mockIndexWriter?
    iw.enableTestPoints = true;
    Document doc = new Document();
    for (int i = 0; i < 10; i++) {
      iw.addDocument(doc);
    }
    iw.commit();

    iw.addDocument(doc);
    
    // pool readers
    DirectoryReader r = DirectoryReader.open(iw);

    // sometimes sneak in a pending commit: we don't want to leak a file handle to that segments_N
    if (random().nextBoolean()) {
      iw.prepareCommit();
    }
    
    RuntimeException expected = expectThrows(RuntimeException.class, () -> {
      iw.rollback();
    });
    assertEquals("BOOM!", expected.getMessage());
    
    r.close();
    
    // even though we hit exception: we are closed, no locks or files held, index in good state
    assertTrue(iw.isClosed());
    dir.obtainLock(IndexWriter.WRITE_LOCK_NAME).close();
    
    r = DirectoryReader.open(dir);
    assertEquals(10, r.maxDoc());
    r.close();
    
    // no leaks
    dir.close();
  }
  
  public void testRandomExceptionDuringRollback() throws Exception {
    // fail in random places on i/o
    final int numIters = RANDOM_MULTIPLIER * 75;
    for (int iter = 0; iter < numIters; iter++) {
      MockDirectoryWrapper dir = newMockDirectory();
      dir.failOn(new MockDirectoryWrapper.Failure() {
        
        @Override
        public void eval(MockDirectoryWrapper dir) throws IOException {
          if (random().nextInt(10) != 0) {
            return;
          }
          boolean maybeFail = false;
          StackTraceElement[] trace = Thread.currentThread().getStackTrace();
          
          for (int i = 0; i < trace.length; i++) {
            if ("rollbackInternal".equals(trace[i].getMethodName())) {
              maybeFail = true;
              break;
            }
          }
          
          if (maybeFail) {
            if (VERBOSE) {
              System.out.println("TEST: now fail; thread=" + Thread.currentThread().getName() + " exc:");
              new Throwable().printStackTrace(System.out);
            }
            throw new FakeIOException();
          }
        }
      });
      
      IndexWriterConfig iwc = new IndexWriterConfig(null);
      IndexWriter iw = new IndexWriter(dir, iwc);
      Document doc = new Document();
      for (int i = 0; i < 10; i++) {
        iw.addDocument(doc);
      }
      iw.commit();
      
      iw.addDocument(doc);
      
      // pool readers
      DirectoryReader r = DirectoryReader.open(iw);
      
      // sometimes sneak in a pending commit: we don't want to leak a file handle to that segments_N
      if (random().nextBoolean()) {
        iw.prepareCommit();
      }
      
      try {
        iw.rollback();
      } catch (FakeIOException expected) {
        // ok, we randomly hit exc here
      }
      
      r.close();
      
      // even though we hit exception: we are closed, no locks or files held, index in good state
      assertTrue(iw.isClosed());
      dir.obtainLock(IndexWriter.WRITE_LOCK_NAME).close();
      
      r = DirectoryReader.open(dir);
      assertEquals(10, r.maxDoc());
      r.close();
      
      // no leaks
      dir.close();
    }
  }

  public void testMergeExceptionIsTragic() throws Exception {
    MockDirectoryWrapper dir = newMockDirectory();
    final AtomicBoolean didFail = new AtomicBoolean();
    dir.failOn(new MockDirectoryWrapper.Failure() {
        
        @Override
        public void eval(MockDirectoryWrapper dir) throws IOException {
          if (random().nextInt(10) != 0) {
            return;
          }
          if (didFail.get()) {
            // Already failed
            return;
          }
          StackTraceElement[] trace = Thread.currentThread().getStackTrace();
          
          for (int i = 0; i < trace.length; i++) {
            if ("merge".equals(trace[i].getMethodName())) {
              if (VERBOSE) {
                System.out.println("TEST: now fail; thread=" + Thread.currentThread().getName() + " exc:");
                new Throwable().printStackTrace(System.out);
              }
              didFail.set(true);
              throw new FakeIOException();
            }
          }
        }
      });

    IndexWriterConfig iwc = newIndexWriterConfig();
    MergePolicy mp = iwc.getMergePolicy();
    if (mp instanceof TieredMergePolicy) {
      TieredMergePolicy tmp = (TieredMergePolicy) mp;
      if (tmp.getMaxMergedSegmentMB() < 0.2) {
        tmp.setMaxMergedSegmentMB(0.2);
      }
    }
    MergeScheduler ms = iwc.getMergeScheduler();
    if (ms instanceof ConcurrentMergeScheduler) {
      ((ConcurrentMergeScheduler) ms).setSuppressExceptions();
    }
    IndexWriter w = new IndexWriter(dir, iwc);

    while (true) {
      try {
        Document doc = new Document();
        doc.add(newStringField("field", "string", Field.Store.NO));
        w.addDocument(doc);
        if (random().nextInt(10) == 7) {
          // Flush new segment:
          DirectoryReader.open(w).close();
        }
      } catch (AlreadyClosedException ace) {
        // OK: e.g. CMS hit the exc in BG thread and closed the writer
        break;
      } catch (FakeIOException fioe) {
        // OK: e.g. SMS hit the exception
        break;
      }
    }

    assertNotNull(w.getTragicException());
    assertFalse(w.isOpen());
    assertTrue(didFail.get());

    if (ms instanceof ConcurrentMergeScheduler) {
      // Sneaky: CMS's merge thread will be concurrently rolling back IW due
      // to the tragedy, with this main thread, so we have to wait here
      // to ensure the rollback has finished, else MDW still sees open files:
      ((ConcurrentMergeScheduler) ms).sync();
    }

    dir.close();
  }
}
