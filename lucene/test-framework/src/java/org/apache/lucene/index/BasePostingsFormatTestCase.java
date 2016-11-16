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

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.TermsEnum.SeekStatus;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LineFileDocs;
import org.apache.lucene.util.RamUsageTester;
import org.apache.lucene.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import static org.apache.lucene.index.PostingsEnum.ALL;
import static org.apache.lucene.index.PostingsEnum.FREQS;
import static org.apache.lucene.index.PostingsEnum.NONE;
import static org.apache.lucene.index.PostingsEnum.OFFSETS;
import static org.apache.lucene.index.PostingsEnum.PAYLOADS;
import static org.apache.lucene.index.PostingsEnum.POSITIONS;

/**
 * Abstract class to do basic tests for a postings format.
 * NOTE: This test focuses on the postings
 * (docs/freqs/positions/payloads/offsets) impl, not the
 * terms dict.  The [stretch] goal is for this test to be
 * so thorough in testing a new PostingsFormat that if this
 * test passes, then all Lucene/Solr tests should also pass.  Ie,
 * if there is some bug in a given PostingsFormat that this
 * test fails to catch then this test needs to be improved! */

// TODO can we make it easy for testing to pair up a "random terms dict impl" with your postings base format...

// TODO test when you reuse after skipping a term or two, eg the block reuse case

/* TODO
  - threads
  - assert doc=-1 before any nextDoc
  - if a PF passes this test but fails other tests then this
    test has a bug!!
  - test tricky reuse cases, eg across fields
  - verify you get null if you pass needFreq/needOffset but
    they weren't indexed
*/

public abstract class BasePostingsFormatTestCase extends BaseIndexFileFormatTestCase {

  static RandomPostingsTester postingsTester;

  // TODO maybe instead of @BeforeClass just make a single test run: build postings & index & test it?

  @BeforeClass
  public static void createPostings() throws IOException {
    postingsTester = new RandomPostingsTester(random());
  }
  
  @AfterClass
  public static void afterClass() throws Exception {
    postingsTester = null;
  }

  public void testDocsOnly() throws Exception {
    postingsTester.testFull(getCodec(), createTempDir("testPostingsFormat.testExact"), IndexOptions.DOCS, false);
  }

  public void testDocsAndFreqs() throws Exception {
    postingsTester.testFull(getCodec(), createTempDir("testPostingsFormat.testExact"), IndexOptions.DOCS_AND_FREQS, false);
  }

  public void testDocsAndFreqsAndPositions() throws Exception {
    postingsTester.testFull(getCodec(), createTempDir("testPostingsFormat.testExact"), IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, false);
  }

  public void testDocsAndFreqsAndPositionsAndPayloads() throws Exception {
    postingsTester.testFull(getCodec(), createTempDir("testPostingsFormat.testExact"), IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, true);
  }

  public void testDocsAndFreqsAndPositionsAndOffsets() throws Exception {
    postingsTester.testFull(getCodec(), createTempDir("testPostingsFormat.testExact"), IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS, false);
  }

  public void testDocsAndFreqsAndPositionsAndOffsetsAndPayloads() throws Exception {
    postingsTester.testFull(getCodec(), createTempDir("testPostingsFormat.testExact"), IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS, true);
  }

  public void testRandom() throws Exception {

    int iters = 5;

    for(int iter=0;iter<iters;iter++) {
      Path path = createTempDir("testPostingsFormat");
      Directory dir = newFSDirectory(path);

      boolean indexPayloads = random().nextBoolean();
      // TODO test thread safety of buildIndex too
      FieldsProducer fieldsProducer = postingsTester.buildIndex(getCodec(), dir, IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS, indexPayloads, false);

      postingsTester.testFields(fieldsProducer);

      // NOTE: you can also test "weaker" index options than
      // you indexed with:
      postingsTester.testTerms(fieldsProducer, EnumSet.allOf(RandomPostingsTester.Option.class), IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS, IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS, false);

      fieldsProducer.close();
      fieldsProducer = null;

      dir.close();
    }
  }

  protected boolean isPostingsEnumReuseImplemented() {
    return true;
  }

  public void testPostingsEnumReuse() throws Exception {

    Path path = createTempDir("testPostingsEnumReuse");
    Directory dir = newFSDirectory(path);

    FieldsProducer fieldsProducer = postingsTester.buildIndex(getCodec(), dir, IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS, random().nextBoolean(), true);
    Collections.shuffle(postingsTester.allTerms, random());
    RandomPostingsTester.FieldAndTerm fieldAndTerm = postingsTester.allTerms.get(0);

    Terms terms = fieldsProducer.terms(fieldAndTerm.field);
    TermsEnum te = terms.iterator();

    te.seekExact(fieldAndTerm.term);
    checkReuse(te, PostingsEnum.FREQS, PostingsEnum.ALL, false);
    if (isPostingsEnumReuseImplemented()) {
      checkReuse(te, PostingsEnum.ALL, PostingsEnum.ALL, true);
    }

    fieldsProducer.close();
    dir.close();
  }

  protected static void checkReuse(TermsEnum termsEnum, int firstFlags, int secondFlags, boolean shouldReuse) throws IOException {
    PostingsEnum postings1 = termsEnum.postings(null, firstFlags);
    PostingsEnum postings2 = termsEnum.postings(postings1, secondFlags);
    if (shouldReuse) {
      assertSame("Expected PostingsEnum " + postings1.getClass().getName() + " to be reused", postings1, postings2);
    } else {
      assertNotSame("Expected PostingsEnum " + postings1.getClass().getName() + " to not be reused", postings1, postings2);
    }
  }
  
  public void testJustEmptyField() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(null);
    iwc.setCodec(getCodec());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);
    Document doc = new Document();
    doc.add(newStringField("", "something", Field.Store.NO));
    iw.addDocument(doc);
    DirectoryReader ir = iw.getReader();
    LeafReader ar = getOnlyLeafReader(ir);
    Fields fields = ar.fields();
    int fieldCount = fields.size();
    // -1 is allowed, if the codec doesn't implement fields.size():
    assertTrue(fieldCount == 1 || fieldCount == -1);
    Terms terms = ar.terms("");
    assertNotNull(terms);
    TermsEnum termsEnum = terms.iterator();
    assertNotNull(termsEnum.next());
    assertEquals(termsEnum.term(), new BytesRef("something"));
    assertNull(termsEnum.next());
    ir.close();
    iw.close();
    dir.close();
  }
  
  public void testEmptyFieldAndEmptyTerm() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(null);
    iwc.setCodec(getCodec());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);
    Document doc = new Document();
    doc.add(newStringField("", "", Field.Store.NO));
    iw.addDocument(doc);
    DirectoryReader ir = iw.getReader();
    LeafReader ar = getOnlyLeafReader(ir);
    Fields fields = ar.fields();
    int fieldCount = fields.size();
    // -1 is allowed, if the codec doesn't implement fields.size():
    assertTrue(fieldCount == 1 || fieldCount == -1);
    Terms terms = ar.terms("");
    assertNotNull(terms);
    TermsEnum termsEnum = terms.iterator();
    assertNotNull(termsEnum.next());
    assertEquals(termsEnum.term(), new BytesRef(""));
    assertNull(termsEnum.next());
    ir.close();
    iw.close();
    dir.close();
  }
  
  public void testDidntWantFreqsButAskedAnyway() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(getCodec());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);
    Document doc = new Document();
    doc.add(newTextField("field", "value", Field.Store.NO));
    iw.addDocument(doc);
    iw.addDocument(doc);
    DirectoryReader ir = iw.getReader();
    LeafReader ar = getOnlyLeafReader(ir);
    TermsEnum termsEnum = ar.terms("field").iterator();
    assertTrue(termsEnum.seekExact(new BytesRef("value")));
    PostingsEnum docsEnum = termsEnum.postings(null, PostingsEnum.NONE);
    assertEquals(0, docsEnum.nextDoc());
    assertEquals(1, docsEnum.freq());
    assertEquals(1, docsEnum.nextDoc());
    assertEquals(1, docsEnum.freq());
    ir.close();
    iw.close();
    dir.close();
  }
  
  public void testAskForPositionsWhenNotThere() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(getCodec());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);
    Document doc = new Document();
    doc.add(newStringField("field", "value", Field.Store.NO));
    iw.addDocument(doc);
    iw.addDocument(doc);
    DirectoryReader ir = iw.getReader();
    LeafReader ar = getOnlyLeafReader(ir);
    TermsEnum termsEnum = ar.terms("field").iterator();
    assertTrue(termsEnum.seekExact(new BytesRef("value")));
    PostingsEnum docsEnum = termsEnum.postings(null, PostingsEnum.POSITIONS);
    assertEquals(0, docsEnum.nextDoc());
    assertEquals(1, docsEnum.freq());
    assertEquals(1, docsEnum.nextDoc());
    assertEquals(1, docsEnum.freq());
    ir.close();
    iw.close();
    dir.close();
  }
  
  // tests that ghost fields still work
  // TODO: can this be improved?
  public void testGhosts() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(null);
    iwc.setCodec(getCodec());
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);
    Document doc = new Document();
    iw.addDocument(doc);
    doc.add(newStringField("ghostField", "something", Field.Store.NO));
    iw.addDocument(doc);
    iw.forceMerge(1);
    iw.deleteDocuments(new Term("ghostField", "something")); // delete the only term for the field
    iw.forceMerge(1);
    DirectoryReader ir = iw.getReader();
    LeafReader ar = getOnlyLeafReader(ir);
    Fields fields = ar.fields();
    // Ghost busting terms dict impls will have
    // fields.size() == 0; all others must be == 1:
    assertTrue(fields.size() <= 1);
    Terms terms = fields.terms("ghostField");
    if (terms != null) {
      TermsEnum termsEnum = terms.iterator();
      BytesRef term = termsEnum.next();
      if (term != null) {
        PostingsEnum postingsEnum = termsEnum.postings(null);
        assertTrue(postingsEnum.nextDoc() == PostingsEnum.NO_MORE_DOCS);
      }
    }
    ir.close();
    iw.close();
    dir.close();
  }

  // tests that level 2 ghost fields still work
  public void testLevel2Ghosts() throws Exception {
    Directory dir = newDirectory();

    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriterConfig iwc = newIndexWriterConfig(null);
    iwc.setCodec(getCodec());
    iwc.setMergePolicy(newLogMergePolicy());
    IndexWriter iw = new IndexWriter(dir, iwc);

    Document document = new Document();
    document.add(new StringField("id", "0", Field.Store.NO));
    document.add(new StringField("suggest_field", "apples", Field.Store.NO));
    iw.addDocument(document);
    // need another document so whole segment isn't deleted
    iw.addDocument(new Document());
    iw.commit();

    document = new Document();
    document.add(new StringField("id", "1", Field.Store.NO));
    document.add(new StringField("suggest_field2", "apples", Field.Store.NO));
    iw.addDocument(document);
    iw.commit();

    iw.deleteDocuments(new Term("id", "0"));
    // first force merge creates a level 1 ghost field
    iw.forceMerge(1);
    
    // second force merge creates a level 2 ghost field, causing MultiFields to include "suggest_field" in its iteration, yet a null Terms is returned (no documents have
    // this field anymore)
    iw.addDocument(new Document());
    iw.forceMerge(1);

    DirectoryReader reader = DirectoryReader.open(iw);
    IndexSearcher indexSearcher = new IndexSearcher(reader);

    assertEquals(1, indexSearcher.count(new TermQuery(new Term("id", "1"))));

    reader.close();
    iw.close();
    dir.close();
  }

  private static class TermFreqs {
    long totalTermFreq;
    int docFreq;
  };

  // LUCENE-5123: make sure we can visit postings twice
  // during flush/merge
  public void testInvertedWrite() throws Exception {
    Directory dir = newDirectory();
    MockAnalyzer analyzer = new MockAnalyzer(random());
    analyzer.setMaxTokenLength(TestUtil.nextInt(random(), 1, IndexWriter.MAX_TERM_LENGTH));
    IndexWriterConfig iwc = newIndexWriterConfig(analyzer);

    // Must be concurrent because thread(s) can be merging
    // while up to one thread flushes, and each of those
    // threads iterates over the map while the flushing
    // thread might be adding to it:
    final Map<String,TermFreqs> termFreqs = new ConcurrentHashMap<>();

    final AtomicLong sumDocFreq = new AtomicLong();
    final AtomicLong sumTotalTermFreq = new AtomicLong();

    // TODO: would be better to use / delegate to the current
    // Codec returned by getCodec()

    iwc.setCodec(new FilterCodec(getCodec().getName(), getCodec()) {
        @Override
        public PostingsFormat postingsFormat() {

          final PostingsFormat defaultPostingsFormat = delegate.postingsFormat();

          final Thread mainThread = Thread.currentThread();

          // A PF that counts up some stats and then in
          // the end we verify the stats match what the
          // final IndexReader says, just to exercise the
          // new freedom of iterating the postings more
          // than once at flush/merge:

          return new PostingsFormat(defaultPostingsFormat.getName()) {

            @Override
            public FieldsConsumer fieldsConsumer(final SegmentWriteState state) throws IOException {

              final FieldsConsumer fieldsConsumer = defaultPostingsFormat.fieldsConsumer(state);

              return new FieldsConsumer() {
                @Override
                public void write(Fields fields) throws IOException {
                  fieldsConsumer.write(fields);

                  boolean isMerge = state.context.context == IOContext.Context.MERGE;

                  // We only use one thread for flushing
                  // in this test:
                  assert isMerge || Thread.currentThread() == mainThread;

                  // We iterate the provided TermsEnum
                  // twice, so we excercise this new freedom
                  // with the inverted API; if
                  // addOnSecondPass is true, we add up
                  // term stats on the 2nd iteration:
                  boolean addOnSecondPass = random().nextBoolean();

                  //System.out.println("write isMerge=" + isMerge + " 2ndPass=" + addOnSecondPass);

                  // Gather our own stats:
                  Terms terms = fields.terms("body");
                  assert terms != null;

                  TermsEnum termsEnum = terms.iterator();
                  PostingsEnum docs = null;
                  while(termsEnum.next() != null) {
                    BytesRef term = termsEnum.term();
                    // TODO: also sometimes ask for payloads/offsets?
                    boolean noPositions = random().nextBoolean();
                    if (noPositions) {
                      docs = termsEnum.postings(docs, PostingsEnum.FREQS);
                    } else {
                      docs = termsEnum.postings(null, PostingsEnum.POSITIONS);
                    }
                    int docFreq = 0;
                    long totalTermFreq = 0;
                    while (docs.nextDoc() != PostingsEnum.NO_MORE_DOCS) {
                      docFreq++;
                      totalTermFreq += docs.freq();
                      int limit = TestUtil.nextInt(random(), 1, docs.freq());
                      if (!noPositions) {
                        for (int i = 0; i < limit; i++) {
                          docs.nextPosition();
                        }
                      }
                    }

                    String termString = term.utf8ToString();

                    // During merge we should only see terms
                    // we had already seen during a
                    // previous flush:
                    assertTrue(isMerge==false || termFreqs.containsKey(termString));

                    if (isMerge == false) {
                      if (addOnSecondPass == false) {
                        TermFreqs tf = termFreqs.get(termString);
                        if (tf == null) {
                          tf = new TermFreqs();
                          termFreqs.put(termString, tf);
                        }
                        tf.docFreq += docFreq;
                        tf.totalTermFreq += totalTermFreq;
                        sumDocFreq.addAndGet(docFreq);
                        sumTotalTermFreq.addAndGet(totalTermFreq);
                      } else if (termFreqs.containsKey(termString) == false) {
                        // Add placeholder (2nd pass will
                        // set its counts):
                        termFreqs.put(termString, new TermFreqs());
                      }
                    }
                  }

                  // Also test seeking the TermsEnum:
                  for(String term : termFreqs.keySet()) {
                    if (termsEnum.seekExact(new BytesRef(term))) {
                      // TODO: also sometimes ask for payloads/offsets?
                      boolean noPositions = random().nextBoolean();
                      if (noPositions) {
                        docs = termsEnum.postings(docs, PostingsEnum.FREQS);
                      } else {
                        docs = termsEnum.postings(null, PostingsEnum.POSITIONS);
                      }

                      int docFreq = 0;
                      long totalTermFreq = 0;
                      while (docs.nextDoc() != PostingsEnum.NO_MORE_DOCS) {
                        docFreq++;
                        totalTermFreq += docs.freq();
                        int limit = TestUtil.nextInt(random(), 1, docs.freq());
                        if (!noPositions) {
                          for (int i = 0; i < limit; i++) {
                            docs.nextPosition();
                          }
                        }
                      }

                      if (isMerge == false && addOnSecondPass) {
                        TermFreqs tf = termFreqs.get(term);
                        assert tf != null;
                        tf.docFreq += docFreq;
                        tf.totalTermFreq += totalTermFreq;
                        sumDocFreq.addAndGet(docFreq);
                        sumTotalTermFreq.addAndGet(totalTermFreq);
                      }

                      //System.out.println("  term=" + term + " docFreq=" + docFreq + " ttDF=" + termToDocFreq.get(term));
                      assertTrue(docFreq <= termFreqs.get(term).docFreq);
                      assertTrue(totalTermFreq <= termFreqs.get(term).totalTermFreq);
                    }
                  }

                  // Also test seekCeil
                  for(int iter=0;iter<10;iter++) {
                    BytesRef term = new BytesRef(TestUtil.randomRealisticUnicodeString(random()));
                    SeekStatus status = termsEnum.seekCeil(term);
                    if (status == SeekStatus.NOT_FOUND) {
                      assertTrue(term.compareTo(termsEnum.term()) < 0);
                    }
                  }
                }

                @Override
                public void close() throws IOException {
                  fieldsConsumer.close();
                }
              };
            }

            @Override
            public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
              return defaultPostingsFormat.fieldsProducer(state);
            }
          };
        }
      });

    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);

    LineFileDocs docs = new LineFileDocs(random());
    int bytesToIndex = atLeast(100) * 1024;
    int bytesIndexed = 0;
    while (bytesIndexed < bytesToIndex) {
      Document doc = docs.nextDoc();
      Document justBodyDoc = new Document();
      justBodyDoc.add(doc.getField("body"));
      w.addDocument(justBodyDoc);
      bytesIndexed += RamUsageTester.sizeOf(justBodyDoc);
    }

    IndexReader r = w.getReader();
    w.close();

    Terms terms = MultiFields.getTerms(r, "body");
    assertEquals(sumDocFreq.get(), terms.getSumDocFreq());
    assertEquals(sumTotalTermFreq.get(), terms.getSumTotalTermFreq());

    TermsEnum termsEnum = terms.iterator();
    long termCount = 0;
    boolean supportsOrds = true;
    while(termsEnum.next() != null) {
      BytesRef term = termsEnum.term();
      assertEquals(termFreqs.get(term.utf8ToString()).docFreq, termsEnum.docFreq());
      assertEquals(termFreqs.get(term.utf8ToString()).totalTermFreq, termsEnum.totalTermFreq());
      if (supportsOrds) {
        long ord;
        try {
          ord = termsEnum.ord();
        } catch (UnsupportedOperationException uoe) {
          supportsOrds = false;
          ord = -1;
        }
        if (ord != -1) {
          assertEquals(termCount, ord);
        }
      }
      termCount++;
    }
    assertEquals(termFreqs.size(), termCount);

    r.close();
    dir.close();
  }
  
  protected void assertReused(String field, PostingsEnum p1, PostingsEnum p2) {
    // if its not DirectPF, we should always reuse. This one has trouble.
    if (!"Direct".equals(TestUtil.getPostingsFormat(field))) {
      assertSame(p1, p2);
    }
  }
  
  public void testPostingsEnumDocsOnly() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(null);
    IndexWriter iw = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new StringField("foo", "bar", Field.Store.NO));
    iw.addDocument(doc);
    DirectoryReader reader = DirectoryReader.open(iw);
    
    // sugar method (FREQS)
    PostingsEnum postings = getOnlyLeafReader(reader).postings(new Term("foo", "bar"));
    assertEquals(-1, postings.docID());
    assertEquals(0, postings.nextDoc());
    assertEquals(1, postings.freq());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, postings.nextDoc());
    
    // termsenum reuse (FREQS)
    TermsEnum termsEnum = getOnlyLeafReader(reader).terms("foo").iterator();
    termsEnum.seekExact(new BytesRef("bar"));
    PostingsEnum postings2 = termsEnum.postings(postings);
    assertNotNull(postings2);
    assertReused("foo", postings, postings2);
    // and it had better work
    assertEquals(-1, postings.docID());
    assertEquals(0, postings.nextDoc());
    assertEquals(1, postings.freq());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, postings.nextDoc());
    
    // asking for any flags: ok
    for (int flag : new int[] { NONE, FREQS, POSITIONS, PAYLOADS, OFFSETS, ALL }) {
      postings = termsEnum.postings(null, flag);
      assertEquals(-1, postings.docID());
      assertEquals(0, postings.nextDoc());
      assertEquals(1, postings.freq());
      assertEquals(DocIdSetIterator.NO_MORE_DOCS, postings.nextDoc());
      // reuse that too
      postings2 = termsEnum.postings(postings, flag);
      assertNotNull(postings2);
      assertReused("foo", postings, postings2);
      // and it had better work
      assertEquals(-1, postings2.docID());
      assertEquals(0, postings2.nextDoc());
      assertEquals(1, postings2.freq());
      assertEquals(DocIdSetIterator.NO_MORE_DOCS, postings2.nextDoc());
    }
    
    iw.close();
    reader.close();
    dir.close();
  }
  
  public void testPostingsEnumFreqs() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        return new TokenStreamComponents(new MockTokenizer());
      }
    });
    IndexWriter iw = new IndexWriter(dir, iwc);
    Document doc = new Document();
    FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
    ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
    doc.add(new Field("foo", "bar bar", ft));
    iw.addDocument(doc);
    DirectoryReader reader = DirectoryReader.open(iw);
    
    // sugar method (FREQS)
    PostingsEnum postings = getOnlyLeafReader(reader).postings(new Term("foo", "bar"));
    assertEquals(-1, postings.docID());
    assertEquals(0, postings.nextDoc());
    assertEquals(2, postings.freq());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, postings.nextDoc());
    
    // termsenum reuse (FREQS)
    TermsEnum termsEnum = getOnlyLeafReader(reader).terms("foo").iterator();
    termsEnum.seekExact(new BytesRef("bar"));
    PostingsEnum postings2 = termsEnum.postings(postings);
    assertNotNull(postings2);
    assertReused("foo", postings, postings2);
    // and it had better work
    assertEquals(-1, postings2.docID());
    assertEquals(0, postings2.nextDoc());
    assertEquals(2, postings2.freq());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, postings2.nextDoc());
    
    // asking for docs only: ok
    PostingsEnum docsOnly = termsEnum.postings(null, PostingsEnum.NONE);
    assertEquals(-1, docsOnly.docID());
    assertEquals(0, docsOnly.nextDoc());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsOnly.freq() == 1 || docsOnly.freq() == 2);
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsOnly.nextDoc());
    // reuse that too
    PostingsEnum docsOnly2 = termsEnum.postings(docsOnly, PostingsEnum.NONE);
    assertNotNull(docsOnly2);
    assertReused("foo", docsOnly, docsOnly2);
    // and it had better work
    assertEquals(-1, docsOnly2.docID());
    assertEquals(0, docsOnly2.nextDoc());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsOnly.freq() == 1 || docsOnly.freq() == 2);
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsOnly2.nextDoc());
    
    // asking for any flags: ok
    for (int flag : new int[] { NONE, FREQS, POSITIONS, PAYLOADS, OFFSETS, ALL }) {
      postings = termsEnum.postings(null, flag);
      assertEquals(-1, postings.docID());
      assertEquals(0, postings.nextDoc());
      if (flag != NONE) {
        assertEquals(2, postings.freq());
      }
      assertEquals(DocIdSetIterator.NO_MORE_DOCS, postings.nextDoc());
      // reuse that too
      postings2 = termsEnum.postings(postings, flag);
      assertNotNull(postings2);
      assertReused("foo", postings, postings2);
      // and it had better work
      assertEquals(-1, postings2.docID());
      assertEquals(0, postings2.nextDoc());
      if (flag != NONE) {
        assertEquals(2, postings2.freq());
      }
      assertEquals(DocIdSetIterator.NO_MORE_DOCS, postings2.nextDoc());
    }
    
    iw.close();
    reader.close();
    dir.close();
  }
  
  public void testPostingsEnumPositions() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        return new TokenStreamComponents(new MockTokenizer());
      }
    });
    IndexWriter iw = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new TextField("foo", "bar bar", Field.Store.NO));
    iw.addDocument(doc);
    DirectoryReader reader = DirectoryReader.open(iw);
    
    // sugar method (FREQS)
    PostingsEnum postings = getOnlyLeafReader(reader).postings(new Term("foo", "bar"));
    assertEquals(-1, postings.docID());
    assertEquals(0, postings.nextDoc());
    assertEquals(2, postings.freq());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, postings.nextDoc());
    
    // termsenum reuse (FREQS)
    TermsEnum termsEnum = getOnlyLeafReader(reader).terms("foo").iterator();
    termsEnum.seekExact(new BytesRef("bar"));
    PostingsEnum postings2 = termsEnum.postings(postings);
    assertNotNull(postings2);
    assertReused("foo", postings, postings2);
    // and it had better work
    assertEquals(-1, postings2.docID());
    assertEquals(0, postings2.nextDoc());
    assertEquals(2, postings2.freq());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, postings2.nextDoc());
    
    // asking for docs only: ok
    PostingsEnum docsOnly = termsEnum.postings(null, PostingsEnum.NONE);
    assertEquals(-1, docsOnly.docID());
    assertEquals(0, docsOnly.nextDoc());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsOnly.freq() == 1 || docsOnly.freq() == 2);
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsOnly.nextDoc());
    // reuse that too
    PostingsEnum docsOnly2 = termsEnum.postings(docsOnly, PostingsEnum.NONE);
    assertNotNull(docsOnly2);
    assertReused("foo", docsOnly, docsOnly2);
    // and it had better work
    assertEquals(-1, docsOnly2.docID());
    assertEquals(0, docsOnly2.nextDoc());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsOnly2.freq() == 1 || docsOnly2.freq() == 2);
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsOnly2.nextDoc());
    
    // asking for positions, ok
    PostingsEnum docsAndPositionsEnum = getOnlyLeafReader(reader).postings(new Term("foo", "bar"), PostingsEnum.POSITIONS);
    assertEquals(-1, docsAndPositionsEnum.docID());
    assertEquals(0, docsAndPositionsEnum.nextDoc());
    assertEquals(2, docsAndPositionsEnum.freq());
    assertEquals(0, docsAndPositionsEnum.nextPosition());
    assertEquals(-1, docsAndPositionsEnum.startOffset());
    assertEquals(-1, docsAndPositionsEnum.endOffset());
    assertNull(docsAndPositionsEnum.getPayload());
    assertEquals(1, docsAndPositionsEnum.nextPosition());
    assertEquals(-1, docsAndPositionsEnum.startOffset());
    assertEquals(-1, docsAndPositionsEnum.endOffset());
    assertNull(docsAndPositionsEnum.getPayload());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum.nextDoc());
    
    // now reuse the positions
    PostingsEnum docsAndPositionsEnum2 = termsEnum.postings(docsAndPositionsEnum, PostingsEnum.POSITIONS);
    assertReused("foo", docsAndPositionsEnum, docsAndPositionsEnum2);
    assertEquals(-1, docsAndPositionsEnum2.docID());
    assertEquals(0, docsAndPositionsEnum2.nextDoc());
    assertEquals(2, docsAndPositionsEnum2.freq());
    assertEquals(0, docsAndPositionsEnum2.nextPosition());
    assertEquals(-1, docsAndPositionsEnum2.startOffset());
    assertEquals(-1, docsAndPositionsEnum2.endOffset());
    assertNull(docsAndPositionsEnum2.getPayload());
    assertEquals(1, docsAndPositionsEnum2.nextPosition());
    assertEquals(-1, docsAndPositionsEnum2.startOffset());
    assertEquals(-1, docsAndPositionsEnum2.endOffset());
    assertNull(docsAndPositionsEnum2.getPayload());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum2.nextDoc());
    
    // payloads, offsets, etc don't cause an error if they aren't there
    docsAndPositionsEnum = getOnlyLeafReader(reader).postings(new Term("foo", "bar"), PostingsEnum.PAYLOADS);
    assertNotNull(docsAndPositionsEnum);
    // but make sure they work
    assertEquals(-1, docsAndPositionsEnum.docID());
    assertEquals(0, docsAndPositionsEnum.nextDoc());
    assertEquals(2, docsAndPositionsEnum.freq());
    assertEquals(0, docsAndPositionsEnum.nextPosition());
    assertEquals(-1, docsAndPositionsEnum.startOffset());
    assertEquals(-1, docsAndPositionsEnum.endOffset());
    assertNull(docsAndPositionsEnum.getPayload());
    assertEquals(1, docsAndPositionsEnum.nextPosition());
    assertEquals(-1, docsAndPositionsEnum.startOffset());
    assertEquals(-1, docsAndPositionsEnum.endOffset());
    assertNull(docsAndPositionsEnum.getPayload());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum.nextDoc());
    // reuse
    docsAndPositionsEnum2 = termsEnum.postings(docsAndPositionsEnum, PostingsEnum.PAYLOADS);
    assertReused("foo", docsAndPositionsEnum, docsAndPositionsEnum2);
    assertEquals(-1, docsAndPositionsEnum2.docID());
    assertEquals(0, docsAndPositionsEnum2.nextDoc());
    assertEquals(2, docsAndPositionsEnum2.freq());
    assertEquals(0, docsAndPositionsEnum2.nextPosition());
    assertEquals(-1, docsAndPositionsEnum2.startOffset());
    assertEquals(-1, docsAndPositionsEnum2.endOffset());
    assertNull(docsAndPositionsEnum2.getPayload());
    assertEquals(1, docsAndPositionsEnum2.nextPosition());
    assertEquals(-1, docsAndPositionsEnum2.startOffset());
    assertEquals(-1, docsAndPositionsEnum2.endOffset());
    assertNull(docsAndPositionsEnum2.getPayload());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum2.nextDoc());
    
    docsAndPositionsEnum = getOnlyLeafReader(reader).postings(new Term("foo", "bar"), PostingsEnum.OFFSETS);
    assertNotNull(docsAndPositionsEnum);
    assertEquals(-1, docsAndPositionsEnum.docID());
    assertEquals(0, docsAndPositionsEnum.nextDoc());
    assertEquals(2, docsAndPositionsEnum.freq());
    assertEquals(0, docsAndPositionsEnum.nextPosition());
    assertEquals(-1, docsAndPositionsEnum.startOffset());
    assertEquals(-1, docsAndPositionsEnum.endOffset());
    assertNull(docsAndPositionsEnum.getPayload());
    assertEquals(1, docsAndPositionsEnum.nextPosition());
    assertEquals(-1, docsAndPositionsEnum.startOffset());
    assertEquals(-1, docsAndPositionsEnum.endOffset());
    assertNull(docsAndPositionsEnum.getPayload());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum.nextDoc());
    // reuse
    docsAndPositionsEnum2 = termsEnum.postings(docsAndPositionsEnum, PostingsEnum.OFFSETS);
    assertReused("foo", docsAndPositionsEnum, docsAndPositionsEnum2);
    assertEquals(-1, docsAndPositionsEnum2.docID());
    assertEquals(0, docsAndPositionsEnum2.nextDoc());
    assertEquals(2, docsAndPositionsEnum2.freq());
    assertEquals(0, docsAndPositionsEnum2.nextPosition());
    assertEquals(-1, docsAndPositionsEnum2.startOffset());
    assertEquals(-1, docsAndPositionsEnum2.endOffset());
    assertNull(docsAndPositionsEnum2.getPayload());
    assertEquals(1, docsAndPositionsEnum2.nextPosition());
    assertEquals(-1, docsAndPositionsEnum2.startOffset());
    assertEquals(-1, docsAndPositionsEnum2.endOffset());
    assertNull(docsAndPositionsEnum2.getPayload());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum2.nextDoc());
    
    docsAndPositionsEnum = getOnlyLeafReader(reader).postings(new Term("foo", "bar"), PostingsEnum.ALL);
    assertNotNull(docsAndPositionsEnum);
    assertEquals(-1, docsAndPositionsEnum.docID());
    assertEquals(0, docsAndPositionsEnum.nextDoc());
    assertEquals(2, docsAndPositionsEnum.freq());
    assertEquals(0, docsAndPositionsEnum.nextPosition());
    assertEquals(-1, docsAndPositionsEnum.startOffset());
    assertEquals(-1, docsAndPositionsEnum.endOffset());
    assertNull(docsAndPositionsEnum.getPayload());
    assertEquals(1, docsAndPositionsEnum.nextPosition());
    assertEquals(-1, docsAndPositionsEnum.startOffset());
    assertEquals(-1, docsAndPositionsEnum.endOffset());
    assertNull(docsAndPositionsEnum.getPayload());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum.nextDoc());
    docsAndPositionsEnum2 = termsEnum.postings(docsAndPositionsEnum, PostingsEnum.ALL);
    assertReused("foo", docsAndPositionsEnum, docsAndPositionsEnum2);
    assertEquals(-1, docsAndPositionsEnum2.docID());
    assertEquals(0, docsAndPositionsEnum2.nextDoc());
    assertEquals(2, docsAndPositionsEnum2.freq());
    assertEquals(0, docsAndPositionsEnum2.nextPosition());
    assertEquals(-1, docsAndPositionsEnum2.startOffset());
    assertEquals(-1, docsAndPositionsEnum2.endOffset());
    assertNull(docsAndPositionsEnum2.getPayload());
    assertEquals(1, docsAndPositionsEnum2.nextPosition());
    assertEquals(-1, docsAndPositionsEnum2.startOffset());
    assertEquals(-1, docsAndPositionsEnum2.endOffset());
    assertNull(docsAndPositionsEnum2.getPayload());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum2.nextDoc());
    
    iw.close();
    reader.close();
    dir.close();
  }
  
  public void testPostingsEnumOffsets() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        return new TokenStreamComponents(new MockTokenizer());
      }
    });
    IndexWriter iw = new IndexWriter(dir, iwc);
    Document doc = new Document();
    FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
    ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    doc.add(new Field("foo", "bar bar", ft));
    iw.addDocument(doc);
    DirectoryReader reader = DirectoryReader.open(iw);
    
    // sugar method (FREQS)
    PostingsEnum postings = getOnlyLeafReader(reader).postings(new Term("foo", "bar"));
    assertEquals(-1, postings.docID());
    assertEquals(0, postings.nextDoc());
    assertEquals(2, postings.freq());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, postings.nextDoc());
    
    // termsenum reuse (FREQS)
    TermsEnum termsEnum = getOnlyLeafReader(reader).terms("foo").iterator();
    termsEnum.seekExact(new BytesRef("bar"));
    PostingsEnum postings2 = termsEnum.postings(postings);
    assertNotNull(postings2);
    assertReused("foo", postings, postings2);
    // and it had better work
    assertEquals(-1, postings2.docID());
    assertEquals(0, postings2.nextDoc());
    assertEquals(2, postings2.freq());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, postings2.nextDoc());
    
    // asking for docs only: ok
    PostingsEnum docsOnly = termsEnum.postings(null, PostingsEnum.NONE);
    assertEquals(-1, docsOnly.docID());
    assertEquals(0, docsOnly.nextDoc());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsOnly.freq() == 1 || docsOnly.freq() == 2);
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsOnly.nextDoc());
    // reuse that too
    PostingsEnum docsOnly2 = termsEnum.postings(docsOnly, PostingsEnum.NONE);
    assertNotNull(docsOnly2);
    assertReused("foo", docsOnly, docsOnly2);
    // and it had better work
    assertEquals(-1, docsOnly2.docID());
    assertEquals(0, docsOnly2.nextDoc());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsOnly2.freq() == 1 || docsOnly2.freq() == 2);
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsOnly2.nextDoc());
    
    // asking for positions, ok
    PostingsEnum docsAndPositionsEnum = getOnlyLeafReader(reader).postings(new Term("foo", "bar"), PostingsEnum.POSITIONS);
    assertEquals(-1, docsAndPositionsEnum.docID());
    assertEquals(0, docsAndPositionsEnum.nextDoc());
    assertEquals(2, docsAndPositionsEnum.freq());
    assertEquals(0, docsAndPositionsEnum.nextPosition());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum.startOffset() == -1 || docsAndPositionsEnum.startOffset() == 0);
    assertTrue(docsAndPositionsEnum.endOffset() == -1 || docsAndPositionsEnum.endOffset() == 3);
    assertNull(docsAndPositionsEnum.getPayload());
    assertEquals(1, docsAndPositionsEnum.nextPosition());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum.startOffset() == -1 || docsAndPositionsEnum.startOffset() == 4);
    assertTrue(docsAndPositionsEnum.endOffset() == -1 || docsAndPositionsEnum.endOffset() == 7);
    assertNull(docsAndPositionsEnum.getPayload());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum.nextDoc());
    
    // now reuse the positions
    PostingsEnum docsAndPositionsEnum2 = termsEnum.postings(docsAndPositionsEnum, PostingsEnum.POSITIONS);
    assertReused("foo", docsAndPositionsEnum, docsAndPositionsEnum2);
    assertEquals(-1, docsAndPositionsEnum2.docID());
    assertEquals(0, docsAndPositionsEnum2.nextDoc());
    assertEquals(2, docsAndPositionsEnum2.freq());
    assertEquals(0, docsAndPositionsEnum2.nextPosition());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum2.startOffset() == -1 || docsAndPositionsEnum2.startOffset() == 0);
    assertTrue(docsAndPositionsEnum2.endOffset() == -1 || docsAndPositionsEnum2.endOffset() == 3);
    assertNull(docsAndPositionsEnum2.getPayload());
    assertEquals(1, docsAndPositionsEnum2.nextPosition());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum2.startOffset() == -1 || docsAndPositionsEnum2.startOffset() == 4);
    assertTrue(docsAndPositionsEnum2.endOffset() == -1 || docsAndPositionsEnum2.endOffset() == 7);
    assertNull(docsAndPositionsEnum2.getPayload());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum2.nextDoc());
    
    // payloads don't cause an error if they aren't there
    docsAndPositionsEnum = getOnlyLeafReader(reader).postings(new Term("foo", "bar"), PostingsEnum.PAYLOADS);
    assertNotNull(docsAndPositionsEnum);
    // but make sure they work
    assertEquals(-1, docsAndPositionsEnum.docID());
    assertEquals(0, docsAndPositionsEnum.nextDoc());
    assertEquals(2, docsAndPositionsEnum.freq());
    assertEquals(0, docsAndPositionsEnum.nextPosition());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum.startOffset() == -1 || docsAndPositionsEnum.startOffset() == 0);
    assertTrue(docsAndPositionsEnum.endOffset() == -1 || docsAndPositionsEnum.endOffset() == 3);
    assertNull(docsAndPositionsEnum.getPayload());
    assertEquals(1, docsAndPositionsEnum.nextPosition());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum.startOffset() == -1 || docsAndPositionsEnum.startOffset() == 4);
    assertTrue(docsAndPositionsEnum.endOffset() == -1 || docsAndPositionsEnum.endOffset() == 7);
    assertNull(docsAndPositionsEnum.getPayload());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum.nextDoc());
    // reuse
    docsAndPositionsEnum2 = termsEnum.postings(docsAndPositionsEnum, PostingsEnum.PAYLOADS);
    assertReused("foo", docsAndPositionsEnum, docsAndPositionsEnum2);
    assertEquals(-1, docsAndPositionsEnum2.docID());
    assertEquals(0, docsAndPositionsEnum2.nextDoc());
    assertEquals(2, docsAndPositionsEnum2.freq());
    assertEquals(0, docsAndPositionsEnum2.nextPosition());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum2.startOffset() == -1 || docsAndPositionsEnum2.startOffset() == 0);
    assertTrue(docsAndPositionsEnum2.endOffset() == -1 || docsAndPositionsEnum2.endOffset() == 3);
    assertNull(docsAndPositionsEnum2.getPayload());
    assertEquals(1, docsAndPositionsEnum2.nextPosition());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum2.startOffset() == -1 || docsAndPositionsEnum2.startOffset() == 4);
    assertTrue(docsAndPositionsEnum2.endOffset() == -1 || docsAndPositionsEnum2.endOffset() == 7);
    assertNull(docsAndPositionsEnum2.getPayload());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum2.nextDoc());
    
    docsAndPositionsEnum = getOnlyLeafReader(reader).postings(new Term("foo", "bar"), PostingsEnum.OFFSETS);
    assertNotNull(docsAndPositionsEnum);
    assertEquals(-1, docsAndPositionsEnum.docID());
    assertEquals(0, docsAndPositionsEnum.nextDoc());
    assertEquals(2, docsAndPositionsEnum.freq());
    assertEquals(0, docsAndPositionsEnum.nextPosition());
    assertEquals(0, docsAndPositionsEnum.startOffset());
    assertEquals(3, docsAndPositionsEnum.endOffset());
    assertNull(docsAndPositionsEnum.getPayload());
    assertEquals(1, docsAndPositionsEnum.nextPosition());
    assertEquals(4, docsAndPositionsEnum.startOffset());
    assertEquals(7, docsAndPositionsEnum.endOffset());
    assertNull(docsAndPositionsEnum.getPayload());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum.nextDoc());
    // reuse
    docsAndPositionsEnum2 = termsEnum.postings(docsAndPositionsEnum, PostingsEnum.OFFSETS);
    assertReused("foo", docsAndPositionsEnum, docsAndPositionsEnum2);
    assertEquals(-1, docsAndPositionsEnum2.docID());
    assertEquals(0, docsAndPositionsEnum2.nextDoc());
    assertEquals(2, docsAndPositionsEnum2.freq());
    assertEquals(0, docsAndPositionsEnum2.nextPosition());
    assertEquals(0, docsAndPositionsEnum2.startOffset());
    assertEquals(3, docsAndPositionsEnum2.endOffset());
    assertNull(docsAndPositionsEnum2.getPayload());
    assertEquals(1, docsAndPositionsEnum2.nextPosition());
    assertEquals(4, docsAndPositionsEnum2.startOffset());
    assertEquals(7, docsAndPositionsEnum2.endOffset());
    assertNull(docsAndPositionsEnum2.getPayload());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum2.nextDoc());
    
    docsAndPositionsEnum = getOnlyLeafReader(reader).postings(new Term("foo", "bar"), PostingsEnum.ALL);
    assertNotNull(docsAndPositionsEnum);
    assertEquals(-1, docsAndPositionsEnum.docID());
    assertEquals(0, docsAndPositionsEnum.nextDoc());
    assertEquals(2, docsAndPositionsEnum.freq());
    assertEquals(0, docsAndPositionsEnum.nextPosition());
    assertEquals(0, docsAndPositionsEnum.startOffset());
    assertEquals(3, docsAndPositionsEnum.endOffset());
    assertNull(docsAndPositionsEnum.getPayload());
    assertEquals(1, docsAndPositionsEnum.nextPosition());
    assertEquals(4, docsAndPositionsEnum.startOffset());
    assertEquals(7, docsAndPositionsEnum.endOffset());
    assertNull(docsAndPositionsEnum.getPayload());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum.nextDoc());
    docsAndPositionsEnum2 = termsEnum.postings(docsAndPositionsEnum, PostingsEnum.ALL);
    assertReused("foo", docsAndPositionsEnum, docsAndPositionsEnum2);
    assertEquals(-1, docsAndPositionsEnum2.docID());
    assertEquals(0, docsAndPositionsEnum2.nextDoc());
    assertEquals(2, docsAndPositionsEnum2.freq());
    assertEquals(0, docsAndPositionsEnum2.nextPosition());
    assertEquals(0, docsAndPositionsEnum2.startOffset());
    assertEquals(3, docsAndPositionsEnum2.endOffset());
    assertNull(docsAndPositionsEnum2.getPayload());
    assertEquals(1, docsAndPositionsEnum2.nextPosition());
    assertEquals(4, docsAndPositionsEnum2.startOffset());
    assertEquals(7, docsAndPositionsEnum2.endOffset());
    assertNull(docsAndPositionsEnum2.getPayload());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum2.nextDoc());
    
    iw.close();
    reader.close();
    dir.close();
  }
  
  public void testPostingsEnumPayloads() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(null);
    IndexWriter iw = new IndexWriter(dir, iwc);
    Document doc = new Document();
    Token token1 = new Token("bar", 0, 3);
    token1.setPayload(new BytesRef("pay1"));
    Token token2 = new Token("bar", 4, 7);
    token2.setPayload(new BytesRef("pay2"));
    doc.add(new TextField("foo", new CannedTokenStream(token1, token2)));
    iw.addDocument(doc);
    DirectoryReader reader = DirectoryReader.open(iw);
    
    // sugar method (FREQS)
    PostingsEnum postings = getOnlyLeafReader(reader).postings(new Term("foo", "bar"));
    assertEquals(-1, postings.docID());
    assertEquals(0, postings.nextDoc());
    assertEquals(2, postings.freq());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, postings.nextDoc());
    
    // termsenum reuse (FREQS)
    TermsEnum termsEnum = getOnlyLeafReader(reader).terms("foo").iterator();
    termsEnum.seekExact(new BytesRef("bar"));
    PostingsEnum postings2 = termsEnum.postings(postings);
    assertNotNull(postings2);
    assertReused("foo", postings, postings2);
    // and it had better work
    assertEquals(-1, postings2.docID());
    assertEquals(0, postings2.nextDoc());
    assertEquals(2, postings2.freq());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, postings2.nextDoc());
    
    // asking for docs only: ok
    PostingsEnum docsOnly = termsEnum.postings(null, PostingsEnum.NONE);
    assertEquals(-1, docsOnly.docID());
    assertEquals(0, docsOnly.nextDoc());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsOnly.freq() == 1 || docsOnly.freq() == 2);
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsOnly.nextDoc());
    // reuse that too
    PostingsEnum docsOnly2 = termsEnum.postings(docsOnly, PostingsEnum.NONE);
    assertNotNull(docsOnly2);
    assertReused("foo", docsOnly, docsOnly2);
    // and it had better work
    assertEquals(-1, docsOnly2.docID());
    assertEquals(0, docsOnly2.nextDoc());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsOnly2.freq() == 1 || docsOnly2.freq() == 2);
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsOnly2.nextDoc());
    
    // asking for positions, ok
    PostingsEnum docsAndPositionsEnum = getOnlyLeafReader(reader).postings(new Term("foo", "bar"), PostingsEnum.POSITIONS);
    assertEquals(-1, docsAndPositionsEnum.docID());
    assertEquals(0, docsAndPositionsEnum.nextDoc());
    assertEquals(2, docsAndPositionsEnum.freq());
    assertEquals(0, docsAndPositionsEnum.nextPosition());
    assertEquals(-1, docsAndPositionsEnum.startOffset());
    assertEquals(-1, docsAndPositionsEnum.endOffset());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum.getPayload() == null || new BytesRef("pay1").equals(docsAndPositionsEnum.getPayload()));
    assertEquals(1, docsAndPositionsEnum.nextPosition());
    assertEquals(-1, docsAndPositionsEnum.startOffset());
    assertEquals(-1, docsAndPositionsEnum.endOffset());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum.getPayload() == null || new BytesRef("pay2").equals(docsAndPositionsEnum.getPayload()));
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum.nextDoc());
    
    // now reuse the positions
    PostingsEnum docsAndPositionsEnum2 = termsEnum.postings(docsAndPositionsEnum, PostingsEnum.POSITIONS);
    assertReused("foo", docsAndPositionsEnum, docsAndPositionsEnum2);
    assertEquals(-1, docsAndPositionsEnum2.docID());
    assertEquals(0, docsAndPositionsEnum2.nextDoc());
    assertEquals(2, docsAndPositionsEnum2.freq());
    assertEquals(0, docsAndPositionsEnum2.nextPosition());
    assertEquals(-1, docsAndPositionsEnum2.startOffset());
    assertEquals(-1, docsAndPositionsEnum2.endOffset());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum2.getPayload() == null || new BytesRef("pay1").equals(docsAndPositionsEnum2.getPayload()));
    assertEquals(1, docsAndPositionsEnum2.nextPosition());
    assertEquals(-1, docsAndPositionsEnum2.startOffset());
    assertEquals(-1, docsAndPositionsEnum2.endOffset());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum2.getPayload() == null || new BytesRef("pay2").equals(docsAndPositionsEnum2.getPayload()));
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum2.nextDoc());
    
    // payloads
    docsAndPositionsEnum = getOnlyLeafReader(reader).postings(new Term("foo", "bar"), PostingsEnum.PAYLOADS);
    assertNotNull(docsAndPositionsEnum);
    assertEquals(-1, docsAndPositionsEnum.docID());
    assertEquals(0, docsAndPositionsEnum.nextDoc());
    assertEquals(2, docsAndPositionsEnum.freq());
    assertEquals(0, docsAndPositionsEnum.nextPosition());
    assertEquals(-1, docsAndPositionsEnum.startOffset());
    assertEquals(-1, docsAndPositionsEnum.endOffset());
    assertEquals(new BytesRef("pay1"), docsAndPositionsEnum.getPayload());
    assertEquals(1, docsAndPositionsEnum.nextPosition());
    assertEquals(-1, docsAndPositionsEnum.startOffset());
    assertEquals(-1, docsAndPositionsEnum.endOffset());
    assertEquals(new BytesRef("pay2"), docsAndPositionsEnum.getPayload());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum.nextDoc());
    // reuse
    docsAndPositionsEnum2 = termsEnum.postings(docsAndPositionsEnum, PostingsEnum.PAYLOADS);
    assertReused("foo", docsAndPositionsEnum, docsAndPositionsEnum2);
    assertEquals(-1, docsAndPositionsEnum2.docID());
    assertEquals(0, docsAndPositionsEnum2.nextDoc());
    assertEquals(2, docsAndPositionsEnum2.freq());
    assertEquals(0, docsAndPositionsEnum2.nextPosition());
    assertEquals(-1, docsAndPositionsEnum2.startOffset());
    assertEquals(-1, docsAndPositionsEnum2.endOffset());
    assertEquals(new BytesRef("pay1"), docsAndPositionsEnum2.getPayload());
    assertEquals(1, docsAndPositionsEnum2.nextPosition());
    assertEquals(-1, docsAndPositionsEnum2.startOffset());
    assertEquals(-1, docsAndPositionsEnum2.endOffset());
    assertEquals(new BytesRef("pay2"), docsAndPositionsEnum2.getPayload());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum2.nextDoc());
    
    docsAndPositionsEnum = getOnlyLeafReader(reader).postings(new Term("foo", "bar"), PostingsEnum.OFFSETS);
    assertNotNull(docsAndPositionsEnum);
    assertEquals(-1, docsAndPositionsEnum.docID());
    assertEquals(0, docsAndPositionsEnum.nextDoc());
    assertEquals(2, docsAndPositionsEnum.freq());
    assertEquals(0, docsAndPositionsEnum.nextPosition());
    assertEquals(-1, docsAndPositionsEnum.startOffset());
    assertEquals(-1, docsAndPositionsEnum.endOffset());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum.getPayload() == null || new BytesRef("pay1").equals(docsAndPositionsEnum.getPayload()));
    assertEquals(1, docsAndPositionsEnum.nextPosition());
    assertEquals(-1, docsAndPositionsEnum.startOffset());
    assertEquals(-1, docsAndPositionsEnum.endOffset());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum.getPayload() == null || new BytesRef("pay2").equals(docsAndPositionsEnum.getPayload()));
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum.nextDoc());
    // reuse
    docsAndPositionsEnum2 = termsEnum.postings(docsAndPositionsEnum, PostingsEnum.OFFSETS);
    assertReused("foo", docsAndPositionsEnum, docsAndPositionsEnum2);
    assertEquals(-1, docsAndPositionsEnum2.docID());
    assertEquals(0, docsAndPositionsEnum2.nextDoc());
    assertEquals(2, docsAndPositionsEnum2.freq());
    assertEquals(0, docsAndPositionsEnum2.nextPosition());
    assertEquals(-1, docsAndPositionsEnum2.startOffset());
    assertEquals(-1, docsAndPositionsEnum2.endOffset());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum2.getPayload() == null || new BytesRef("pay1").equals(docsAndPositionsEnum2.getPayload()));
    assertEquals(1, docsAndPositionsEnum2.nextPosition());
    assertEquals(-1, docsAndPositionsEnum2.startOffset());
    assertEquals(-1, docsAndPositionsEnum2.endOffset());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum2.getPayload() == null || new BytesRef("pay2").equals(docsAndPositionsEnum2.getPayload()));
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum2.nextDoc());
    
    docsAndPositionsEnum = getOnlyLeafReader(reader).postings(new Term("foo", "bar"), PostingsEnum.ALL);
    assertNotNull(docsAndPositionsEnum);
    assertEquals(-1, docsAndPositionsEnum.docID());
    assertEquals(0, docsAndPositionsEnum.nextDoc());
    assertEquals(2, docsAndPositionsEnum.freq());
    assertEquals(0, docsAndPositionsEnum.nextPosition());
    assertEquals(-1, docsAndPositionsEnum.startOffset());
    assertEquals(-1, docsAndPositionsEnum.endOffset());
    assertEquals(new BytesRef("pay1"), docsAndPositionsEnum.getPayload());
    assertEquals(1, docsAndPositionsEnum.nextPosition());
    assertEquals(-1, docsAndPositionsEnum.startOffset());
    assertEquals(-1, docsAndPositionsEnum.endOffset());
    assertEquals(new BytesRef("pay2"), docsAndPositionsEnum.getPayload());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum.nextDoc());
    docsAndPositionsEnum2 = termsEnum.postings(docsAndPositionsEnum, PostingsEnum.ALL);
    assertReused("foo", docsAndPositionsEnum, docsAndPositionsEnum2);
    assertEquals(-1, docsAndPositionsEnum2.docID());
    assertEquals(0, docsAndPositionsEnum2.nextDoc());
    assertEquals(2, docsAndPositionsEnum2.freq());
    assertEquals(0, docsAndPositionsEnum2.nextPosition());
    assertEquals(-1, docsAndPositionsEnum2.startOffset());
    assertEquals(-1, docsAndPositionsEnum2.endOffset());
    assertEquals(new BytesRef("pay1"), docsAndPositionsEnum2.getPayload());
    assertEquals(1, docsAndPositionsEnum2.nextPosition());
    assertEquals(-1, docsAndPositionsEnum2.startOffset());
    assertEquals(-1, docsAndPositionsEnum2.endOffset());
    assertEquals(new BytesRef("pay2"), docsAndPositionsEnum2.getPayload());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum2.nextDoc());
    
    iw.close();
    reader.close();
    dir.close();
  }
  
  public void testPostingsEnumAll() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(null);
    IndexWriter iw = new IndexWriter(dir, iwc);
    Document doc = new Document();
    Token token1 = new Token("bar", 0, 3);
    token1.setPayload(new BytesRef("pay1"));
    Token token2 = new Token("bar", 4, 7);
    token2.setPayload(new BytesRef("pay2"));
    FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
    ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    doc.add(new Field("foo", new CannedTokenStream(token1, token2), ft));
    iw.addDocument(doc);
    DirectoryReader reader = DirectoryReader.open(iw);
    
    // sugar method (FREQS)
    PostingsEnum postings = getOnlyLeafReader(reader).postings(new Term("foo", "bar"));
    assertEquals(-1, postings.docID());
    assertEquals(0, postings.nextDoc());
    assertEquals(2, postings.freq());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, postings.nextDoc());
    
    // termsenum reuse (FREQS)
    TermsEnum termsEnum = getOnlyLeafReader(reader).terms("foo").iterator();
    termsEnum.seekExact(new BytesRef("bar"));
    PostingsEnum postings2 = termsEnum.postings(postings);
    assertNotNull(postings2);
    assertReused("foo", postings, postings2);
    // and it had better work
    assertEquals(-1, postings2.docID());
    assertEquals(0, postings2.nextDoc());
    assertEquals(2, postings2.freq());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, postings2.nextDoc());
    
    // asking for docs only: ok
    PostingsEnum docsOnly = termsEnum.postings(null, PostingsEnum.NONE);
    assertEquals(-1, docsOnly.docID());
    assertEquals(0, docsOnly.nextDoc());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsOnly.freq() == 1 || docsOnly.freq() == 2);
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsOnly.nextDoc());
    // reuse that too
    PostingsEnum docsOnly2 = termsEnum.postings(docsOnly, PostingsEnum.NONE);
    assertNotNull(docsOnly2);
    assertReused("foo", docsOnly, docsOnly2);
    // and it had better work
    assertEquals(-1, docsOnly2.docID());
    assertEquals(0, docsOnly2.nextDoc());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsOnly2.freq() == 1 || docsOnly2.freq() == 2);
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsOnly2.nextDoc());
    
    // asking for positions, ok
    PostingsEnum docsAndPositionsEnum = getOnlyLeafReader(reader).postings(new Term("foo", "bar"), PostingsEnum.POSITIONS);
    assertEquals(-1, docsAndPositionsEnum.docID());
    assertEquals(0, docsAndPositionsEnum.nextDoc());
    assertEquals(2, docsAndPositionsEnum.freq());
    assertEquals(0, docsAndPositionsEnum.nextPosition());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum.startOffset() == -1 || docsAndPositionsEnum.startOffset() == 0);
    assertTrue(docsAndPositionsEnum.endOffset() == -1 || docsAndPositionsEnum.endOffset() == 3);
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum.getPayload() == null || new BytesRef("pay1").equals(docsAndPositionsEnum.getPayload()));
    assertEquals(1, docsAndPositionsEnum.nextPosition());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum.startOffset() == -1 || docsAndPositionsEnum.startOffset() == 4);
    assertTrue(docsAndPositionsEnum.endOffset() == -1 || docsAndPositionsEnum.endOffset() == 7);
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum.getPayload() == null || new BytesRef("pay2").equals(docsAndPositionsEnum.getPayload()));
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum.nextDoc());
    
    // now reuse the positions
    PostingsEnum docsAndPositionsEnum2 = termsEnum.postings(docsAndPositionsEnum, PostingsEnum.POSITIONS);
    assertReused("foo", docsAndPositionsEnum, docsAndPositionsEnum2);
    assertEquals(-1, docsAndPositionsEnum2.docID());
    assertEquals(0, docsAndPositionsEnum2.nextDoc());
    assertEquals(2, docsAndPositionsEnum2.freq());
    assertEquals(0, docsAndPositionsEnum2.nextPosition());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum2.startOffset() == -1 || docsAndPositionsEnum2.startOffset() == 0);
    assertTrue(docsAndPositionsEnum2.endOffset() == -1 || docsAndPositionsEnum2.endOffset() == 3);
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum2.getPayload() == null || new BytesRef("pay1").equals(docsAndPositionsEnum2.getPayload()));
    assertEquals(1, docsAndPositionsEnum2.nextPosition());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum2.startOffset() == -1 || docsAndPositionsEnum2.startOffset() == 4);
    assertTrue(docsAndPositionsEnum2.endOffset() == -1 || docsAndPositionsEnum2.endOffset() == 7);
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum2.getPayload() == null || new BytesRef("pay2").equals(docsAndPositionsEnum2.getPayload()));
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum2.nextDoc());
    
    // payloads
    docsAndPositionsEnum = getOnlyLeafReader(reader).postings(new Term("foo", "bar"), PostingsEnum.PAYLOADS);
    assertNotNull(docsAndPositionsEnum);
    assertEquals(-1, docsAndPositionsEnum.docID());
    assertEquals(0, docsAndPositionsEnum.nextDoc());
    assertEquals(2, docsAndPositionsEnum.freq());
    assertEquals(0, docsAndPositionsEnum.nextPosition());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum.startOffset() == -1 || docsAndPositionsEnum.startOffset() == 0);
    assertTrue(docsAndPositionsEnum.endOffset() == -1 || docsAndPositionsEnum.endOffset() == 3);
    assertEquals(new BytesRef("pay1"), docsAndPositionsEnum.getPayload());
    assertEquals(1, docsAndPositionsEnum.nextPosition());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum.startOffset() == -1 || docsAndPositionsEnum.startOffset() == 4);
    assertTrue(docsAndPositionsEnum.endOffset() == -1 || docsAndPositionsEnum.endOffset() == 7);
    assertEquals(new BytesRef("pay2"), docsAndPositionsEnum.getPayload());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum.nextDoc());
    // reuse
    docsAndPositionsEnum2 = termsEnum.postings(docsAndPositionsEnum, PostingsEnum.PAYLOADS);
    assertReused("foo", docsAndPositionsEnum, docsAndPositionsEnum2);
    assertEquals(-1, docsAndPositionsEnum2.docID());
    assertEquals(0, docsAndPositionsEnum2.nextDoc());
    assertEquals(2, docsAndPositionsEnum2.freq());
    assertEquals(0, docsAndPositionsEnum2.nextPosition());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum2.startOffset() == -1 || docsAndPositionsEnum2.startOffset() == 0);
    assertTrue(docsAndPositionsEnum2.endOffset() == -1 || docsAndPositionsEnum2.endOffset() == 3);
    assertEquals(new BytesRef("pay1"), docsAndPositionsEnum2.getPayload());
    assertEquals(1, docsAndPositionsEnum2.nextPosition());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum2.startOffset() == -1 || docsAndPositionsEnum2.startOffset() == 4);
    assertTrue(docsAndPositionsEnum2.endOffset() == -1 || docsAndPositionsEnum2.endOffset() == 7);
    assertEquals(new BytesRef("pay2"), docsAndPositionsEnum2.getPayload());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum2.nextDoc());
    
    docsAndPositionsEnum = getOnlyLeafReader(reader).postings(new Term("foo", "bar"), PostingsEnum.OFFSETS);
    assertNotNull(docsAndPositionsEnum);
    assertEquals(-1, docsAndPositionsEnum.docID());
    assertEquals(0, docsAndPositionsEnum.nextDoc());
    assertEquals(2, docsAndPositionsEnum.freq());
    assertEquals(0, docsAndPositionsEnum.nextPosition());
    assertEquals(0, docsAndPositionsEnum.startOffset());
    assertEquals(3, docsAndPositionsEnum.endOffset());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum.getPayload() == null || new BytesRef("pay1").equals(docsAndPositionsEnum.getPayload()));
    assertEquals(1, docsAndPositionsEnum.nextPosition());
    assertEquals(4, docsAndPositionsEnum.startOffset());
    assertEquals(7, docsAndPositionsEnum.endOffset());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum.getPayload() == null || new BytesRef("pay2").equals(docsAndPositionsEnum.getPayload()));
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum.nextDoc());
    // reuse
    docsAndPositionsEnum2 = termsEnum.postings(docsAndPositionsEnum, PostingsEnum.OFFSETS);
    assertReused("foo", docsAndPositionsEnum, docsAndPositionsEnum2);
    assertEquals(-1, docsAndPositionsEnum2.docID());
    assertEquals(0, docsAndPositionsEnum2.nextDoc());
    assertEquals(2, docsAndPositionsEnum2.freq());
    assertEquals(0, docsAndPositionsEnum2.nextPosition());
    assertEquals(0, docsAndPositionsEnum2.startOffset());
    assertEquals(3, docsAndPositionsEnum2.endOffset());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum2.getPayload() == null || new BytesRef("pay1").equals(docsAndPositionsEnum2.getPayload()));
    assertEquals(1, docsAndPositionsEnum2.nextPosition());
    assertEquals(4, docsAndPositionsEnum2.startOffset());
    assertEquals(7, docsAndPositionsEnum2.endOffset());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum2.getPayload() == null || new BytesRef("pay2").equals(docsAndPositionsEnum2.getPayload()));
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum2.nextDoc());
    
    docsAndPositionsEnum = getOnlyLeafReader(reader).postings(new Term("foo", "bar"), PostingsEnum.ALL);
    assertNotNull(docsAndPositionsEnum);
    assertEquals(-1, docsAndPositionsEnum.docID());
    assertEquals(0, docsAndPositionsEnum.nextDoc());
    assertEquals(2, docsAndPositionsEnum.freq());
    assertEquals(0, docsAndPositionsEnum.nextPosition());
    assertEquals(0, docsAndPositionsEnum.startOffset());
    assertEquals(3, docsAndPositionsEnum.endOffset());
    assertEquals(new BytesRef("pay1"), docsAndPositionsEnum.getPayload());
    assertEquals(1, docsAndPositionsEnum.nextPosition());
    assertEquals(4, docsAndPositionsEnum.startOffset());
    assertEquals(7, docsAndPositionsEnum.endOffset());
    assertEquals(new BytesRef("pay2"), docsAndPositionsEnum.getPayload());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum.nextDoc());
    docsAndPositionsEnum2 = termsEnum.postings(docsAndPositionsEnum, PostingsEnum.ALL);
    assertReused("foo", docsAndPositionsEnum, docsAndPositionsEnum2);
    assertEquals(-1, docsAndPositionsEnum2.docID());
    assertEquals(0, docsAndPositionsEnum2.nextDoc());
    assertEquals(2, docsAndPositionsEnum2.freq());
    assertEquals(0, docsAndPositionsEnum2.nextPosition());
    assertEquals(0, docsAndPositionsEnum2.startOffset());
    assertEquals(3, docsAndPositionsEnum2.endOffset());
    assertEquals(new BytesRef("pay1"), docsAndPositionsEnum2.getPayload());
    assertEquals(1, docsAndPositionsEnum2.nextPosition());
    assertEquals(4, docsAndPositionsEnum2.startOffset());
    assertEquals(7, docsAndPositionsEnum2.endOffset());
    assertEquals(new BytesRef("pay2"), docsAndPositionsEnum2.getPayload());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum2.nextDoc());
    
    iw.close();
    reader.close();
    dir.close();
  }

  @Override
  protected void addRandomFields(Document doc) {
    for (IndexOptions opts : IndexOptions.values()) {
      if (opts == IndexOptions.NONE) {
        continue;
      }
      FieldType ft = new FieldType();
      ft.setIndexOptions(opts);
      ft.freeze();
      final int numFields = random().nextInt(5);
      for (int j = 0; j < numFields; ++j) {
        doc.add(new Field("f_" + opts, TestUtil.randomSimpleString(random(), 2), ft));
      }
    }
  }
}
