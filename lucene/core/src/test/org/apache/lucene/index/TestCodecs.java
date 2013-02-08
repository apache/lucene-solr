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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsConsumer;
import org.apache.lucene.codecs.TermStats;
import org.apache.lucene.codecs.TermsConsumer;
import org.apache.lucene.codecs.lucene3x.Lucene3xCodec;
import org.apache.lucene.codecs.mocksep.MockSepPostingsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.FieldInfo.DocValuesType;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.OpenBitSet;
import org.apache.lucene.util._TestUtil;
import org.junit.BeforeClass;

// TODO: test multiple codecs here?

// TODO
//   - test across fields
//   - fix this test to run once for all codecs
//   - make more docs per term, to test > 1 level skipping
//   - test all combinations of payloads/not and omitTF/not
//   - test w/ different indexDivisor
//   - test field where payload length rarely changes
//   - 0-term fields
//   - seek/skip to same term/doc i'm already on
//   - mix in deleted docs
//   - seek, skip beyond end -- assert returns false
//   - seek, skip to things that don't exist -- ensure it
//     goes to 1 before next one known to exist
//   - skipTo(term)
//   - skipTo(doc)

public class TestCodecs extends LuceneTestCase {
  private static String[] fieldNames = new String[] {"one", "two", "three", "four"};

  private static int NUM_TEST_ITER;
  private final static int NUM_TEST_THREADS = 3;
  private final static int NUM_FIELDS = 4;
  private final static int NUM_TERMS_RAND = 50; // must be > 16 to test skipping
  private final static int DOC_FREQ_RAND = 500; // must be > 16 to test skipping
  private final static int TERM_DOC_FREQ_RAND = 20;

  @BeforeClass
  public static void beforeClass() {
    NUM_TEST_ITER = atLeast(20);
  }

  class FieldData implements Comparable<FieldData> {
    final FieldInfo fieldInfo;
    final TermData[] terms;
    final boolean omitTF;
    final boolean storePayloads;

    public FieldData(final String name, final FieldInfos.Builder fieldInfos, final TermData[] terms, final boolean omitTF, final boolean storePayloads) {
      this.omitTF = omitTF;
      this.storePayloads = storePayloads;
      // TODO: change this test to use all three
      fieldInfo = fieldInfos.addOrUpdate(name, new IndexableFieldType() {

        @Override
        public boolean indexed() { return true; }

        @Override
        public boolean stored() { return false; }

        @Override
        public boolean tokenized() { return false; }

        @Override
        public boolean storeTermVectors() { return false; }

        @Override
        public boolean storeTermVectorOffsets() { return false; }

        @Override
        public boolean storeTermVectorPositions() { return false; }

        @Override
        public boolean storeTermVectorPayloads() { return false; }

        @Override
        public boolean omitNorms() { return false; }

        @Override
        public IndexOptions indexOptions() { return omitTF ? IndexOptions.DOCS_ONLY : IndexOptions.DOCS_AND_FREQS_AND_POSITIONS; }

        @Override
        public DocValuesType docValueType() { return null; }
      });
      if (storePayloads) {
        fieldInfo.setStorePayloads();
      }
      this.terms = terms;
      for(int i=0;i<terms.length;i++)
        terms[i].field = this;

      Arrays.sort(terms);
    }

    @Override
    public int compareTo(final FieldData other) {
      return fieldInfo.name.compareTo(other.fieldInfo.name);
    }

    public void write(final FieldsConsumer consumer) throws Throwable {
      Arrays.sort(terms);
      final TermsConsumer termsConsumer = consumer.addField(fieldInfo);
      long sumTotalTermCount = 0;
      long sumDF = 0;
      OpenBitSet visitedDocs = new OpenBitSet();
      for (final TermData term : terms) {
        for (int i = 0; i < term.docs.length; i++) {
          visitedDocs.set(term.docs[i]);
        }
        sumDF += term.docs.length;
        sumTotalTermCount += term.write(termsConsumer);
      }
      termsConsumer.finish(omitTF ? -1 : sumTotalTermCount, sumDF, (int) visitedDocs.cardinality());
    }
  }

  class PositionData {
    int pos;
    BytesRef payload;

    PositionData(final int pos, final BytesRef payload) {
      this.pos = pos;
      this.payload = payload;
    }
  }

  class TermData implements Comparable<TermData> {
    String text2;
    final BytesRef text;
    int[] docs;
    PositionData[][] positions;
    FieldData field;

    public TermData(final String text, final int[] docs, final PositionData[][] positions) {
      this.text = new BytesRef(text);
      this.text2 = text;
      this.docs = docs;
      this.positions = positions;
    }

    @Override
    public int compareTo(final TermData o) {
      return text.compareTo(o.text);
    }

    public long write(final TermsConsumer termsConsumer) throws Throwable {
      final PostingsConsumer postingsConsumer = termsConsumer.startTerm(text);
      long totTF = 0;
      for(int i=0;i<docs.length;i++) {
        final int termDocFreq;
        if (field.omitTF) {
          termDocFreq = -1;
        } else {
          termDocFreq = positions[i].length;
        }
        postingsConsumer.startDoc(docs[i], termDocFreq);
        if (!field.omitTF) {
          totTF += positions[i].length;
          for(int j=0;j<positions[i].length;j++) {
            final PositionData pos = positions[i][j];
            postingsConsumer.addPosition(pos.pos, pos.payload, -1, -1);
          }
        }
        postingsConsumer.finishDoc();
      }
      termsConsumer.finishTerm(text, new TermStats(docs.length, field.omitTF ? -1 : totTF));
      return totTF;
    }
  }

  final private static String SEGMENT = "0";

  TermData[] makeRandomTerms(final boolean omitTF, final boolean storePayloads) {
    final int numTerms = 1+random().nextInt(NUM_TERMS_RAND);
    //final int numTerms = 2;
    final TermData[] terms = new TermData[numTerms];

    final HashSet<String> termsSeen = new HashSet<String>();

    for(int i=0;i<numTerms;i++) {

      // Make term text
      String text2;
      while(true) {
        text2 = _TestUtil.randomUnicodeString(random());
        if (!termsSeen.contains(text2) && !text2.endsWith(".")) {
          termsSeen.add(text2);
          break;
        }
      }

      final int docFreq = 1+random().nextInt(DOC_FREQ_RAND);
      final int[] docs = new int[docFreq];
      PositionData[][] positions;

      if (!omitTF)
        positions = new PositionData[docFreq][];
      else
        positions = null;

      int docID = 0;
      for(int j=0;j<docFreq;j++) {
        docID += _TestUtil.nextInt(random(), 1, 10);
        docs[j] = docID;

        if (!omitTF) {
          final int termFreq = 1+random().nextInt(TERM_DOC_FREQ_RAND);
          positions[j] = new PositionData[termFreq];
          int position = 0;
          for(int k=0;k<termFreq;k++) {
            position += _TestUtil.nextInt(random(), 1, 10);

            final BytesRef payload;
            if (storePayloads && random().nextInt(4) == 0) {
              final byte[] bytes = new byte[1+random().nextInt(5)];
              for(int l=0;l<bytes.length;l++) {
                bytes[l] = (byte) random().nextInt(255);
              }
              payload = new BytesRef(bytes);
            } else {
              payload = null;
            }

            positions[j][k] = new PositionData(position, payload);
          }
        }
      }

      terms[i] = new TermData(text2, docs, positions);
    }

    return terms;
  }

  public void testFixedPostings() throws Throwable {
    final int NUM_TERMS = 100;
    final TermData[] terms = new TermData[NUM_TERMS];
    for(int i=0;i<NUM_TERMS;i++) {
      final int[] docs = new int[] {i};
      final String text = Integer.toString(i, Character.MAX_RADIX);
      terms[i] = new TermData(text, docs, null);
    }

    final FieldInfos.Builder builder = new FieldInfos.Builder();

    final FieldData field = new FieldData("field", builder, terms, true, false);
    final FieldData[] fields = new FieldData[] {field};
    final FieldInfos fieldInfos = builder.finish();
    final Directory dir = newDirectory();
    this.write(fieldInfos, dir, fields, true);
    Codec codec = Codec.getDefault();
    final SegmentInfo si = new SegmentInfo(dir, Constants.LUCENE_MAIN_VERSION, SEGMENT, 10000, false, codec, null, null);

    final FieldsProducer reader = codec.postingsFormat().fieldsProducer(new SegmentReadState(dir, si, fieldInfos, newIOContext(random()), DirectoryReader.DEFAULT_TERMS_INDEX_DIVISOR));

    final Iterator<String> fieldsEnum = reader.iterator();
    String fieldName = fieldsEnum.next();
    assertNotNull(fieldName);
    final Terms terms2 = reader.terms(fieldName);
    assertNotNull(terms2);

    final TermsEnum termsEnum = terms2.iterator(null);

    DocsEnum docsEnum = null;
    for(int i=0;i<NUM_TERMS;i++) {
      final BytesRef term = termsEnum.next();
      assertNotNull(term);
      assertEquals(terms[i].text2, term.utf8ToString());

      // do this twice to stress test the codec's reuse, ie,
      // make sure it properly fully resets (rewinds) its
      // internal state:
      for(int iter=0;iter<2;iter++) {
        docsEnum = _TestUtil.docs(random(), termsEnum, null,  docsEnum, DocsEnum.FLAG_NONE);
        assertEquals(terms[i].docs[0], docsEnum.nextDoc());
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsEnum.nextDoc());
      }
    }
    assertNull(termsEnum.next());

    for(int i=0;i<NUM_TERMS;i++) {
      assertEquals(termsEnum.seekCeil(new BytesRef(terms[i].text2)), TermsEnum.SeekStatus.FOUND);
    }

    assertFalse(fieldsEnum.hasNext());
    reader.close();
    dir.close();
  }

  public void testRandomPostings() throws Throwable {
    final FieldInfos.Builder builder = new FieldInfos.Builder();

    final FieldData[] fields = new FieldData[NUM_FIELDS];
    for(int i=0;i<NUM_FIELDS;i++) {
      final boolean omitTF = 0==(i%3);
      final boolean storePayloads = 1==(i%3);
      fields[i] = new FieldData(fieldNames[i], builder, this.makeRandomTerms(omitTF, storePayloads), omitTF, storePayloads);
    }

    final Directory dir = newDirectory();
    final FieldInfos fieldInfos = builder.finish();

    if (VERBOSE) {
      System.out.println("TEST: now write postings");
    }

    this.write(fieldInfos, dir, fields, false);
    Codec codec = Codec.getDefault();
    final SegmentInfo si = new SegmentInfo(dir, Constants.LUCENE_MAIN_VERSION, SEGMENT, 10000,
                                           false, codec, null, null);

    if (VERBOSE) {
      System.out.println("TEST: now read postings");
    }
    final FieldsProducer terms = codec.postingsFormat().fieldsProducer(new SegmentReadState(dir, si, fieldInfos, newIOContext(random()), DirectoryReader.DEFAULT_TERMS_INDEX_DIVISOR));

    final Verify[] threads = new Verify[NUM_TEST_THREADS-1];
    for(int i=0;i<NUM_TEST_THREADS-1;i++) {
      threads[i] = new Verify(si, fields, terms);
      threads[i].setDaemon(true);
      threads[i].start();
    }

    new Verify(si, fields, terms).run();

    for(int i=0;i<NUM_TEST_THREADS-1;i++) {
      threads[i].join();
      assert !threads[i].failed;
    }

    terms.close();
    dir.close();
  }

  public void testSepPositionAfterMerge() throws IOException {
    final Directory dir = newDirectory();
    final IndexWriterConfig config = newIndexWriterConfig(TEST_VERSION_CURRENT,
      new MockAnalyzer(random()));
    config.setMergePolicy(newLogMergePolicy());
    config.setCodec(_TestUtil.alwaysPostingsFormat(new MockSepPostingsFormat()));
    final IndexWriter writer = new IndexWriter(dir, config);

    try {
      final PhraseQuery pq = new PhraseQuery();
      pq.add(new Term("content", "bbb"));
      pq.add(new Term("content", "ccc"));

      final Document doc = new Document();
      FieldType customType = new FieldType(TextField.TYPE_NOT_STORED);
      customType.setOmitNorms(true);
      doc.add(newField("content", "aaa bbb ccc ddd", customType));

      // add document and force commit for creating a first segment
      writer.addDocument(doc);
      writer.commit();

      ScoreDoc[] results = this.search(writer, pq, 5);
      assertEquals(1, results.length);
      assertEquals(0, results[0].doc);

      // add document and force commit for creating a second segment
      writer.addDocument(doc);
      writer.commit();

      // at this point, there should be at least two segments
      results = this.search(writer, pq, 5);
      assertEquals(2, results.length);
      assertEquals(0, results[0].doc);

      writer.forceMerge(1);

      // optimise to merge the segments.
      results = this.search(writer, pq, 5);
      assertEquals(2, results.length);
      assertEquals(0, results[0].doc);
    }
    finally {
      writer.close();
      dir.close();
    }
  }

  private ScoreDoc[] search(final IndexWriter writer, final Query q, final int n) throws IOException {
    final IndexReader reader = writer.getReader();
    final IndexSearcher searcher = newSearcher(reader);
    try {
      return searcher.search(q, null, n).scoreDocs;
    }
    finally {
      reader.close();
    }
  }

  private class Verify extends Thread {
    final Fields termsDict;
    final FieldData[] fields;
    final SegmentInfo si;
    volatile boolean failed;

    Verify(final SegmentInfo si, final FieldData[] fields, final Fields termsDict) {
      this.fields = fields;
      this.termsDict = termsDict;
      this.si = si;
    }

    @Override
    public void run() {
      try {
        this._run();
      } catch (final Throwable t) {
        failed = true;
        throw new RuntimeException(t);
      }
    }

    private void verifyDocs(final int[] docs, final PositionData[][] positions, final DocsEnum docsEnum, final boolean doPos) throws Throwable {
      for(int i=0;i<docs.length;i++) {
        final int doc = docsEnum.nextDoc();
        assertTrue(doc != DocIdSetIterator.NO_MORE_DOCS);
        assertEquals(docs[i], doc);
        if (doPos) {
          this.verifyPositions(positions[i], ((DocsAndPositionsEnum) docsEnum));
        }
      }
      assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsEnum.nextDoc());
    }

    byte[] data = new byte[10];

    private void verifyPositions(final PositionData[] positions, final DocsAndPositionsEnum posEnum) throws Throwable {
      for(int i=0;i<positions.length;i++) {
        final int pos = posEnum.nextPosition();
        assertEquals(positions[i].pos, pos);
        if (positions[i].payload != null) {
          assertNotNull(posEnum.getPayload());
          if (random().nextInt(3) < 2) {
            // Verify the payload bytes
            final BytesRef otherPayload = posEnum.getPayload();
            assertTrue("expected=" + positions[i].payload.toString() + " got=" + otherPayload.toString(), positions[i].payload.equals(otherPayload));
          }
        } else {
          assertNull(posEnum.getPayload());
        }
      }
    }

    public void _run() throws Throwable {

      for(int iter=0;iter<NUM_TEST_ITER;iter++) {
        final FieldData field = fields[random().nextInt(fields.length)];
        final TermsEnum termsEnum = termsDict.terms(field.fieldInfo.name).iterator(null);
        if (si.getCodec() instanceof Lucene3xCodec) {
          // code below expects unicode sort order
          continue;
        }

        int upto = 0;
        // Test straight enum of the terms:
        while(true) {
          final BytesRef term = termsEnum.next();
          if (term == null) {
            break;
          }
          final BytesRef expected = new BytesRef(field.terms[upto++].text2);
          assertTrue("expected=" + expected + " vs actual " + term, expected.bytesEquals(term));
        }
        assertEquals(upto, field.terms.length);

        // Test random seek:
        TermData term = field.terms[random().nextInt(field.terms.length)];
        TermsEnum.SeekStatus status = termsEnum.seekCeil(new BytesRef(term.text2));
        assertEquals(status, TermsEnum.SeekStatus.FOUND);
        assertEquals(term.docs.length, termsEnum.docFreq());
        if (field.omitTF) {
          this.verifyDocs(term.docs, term.positions, _TestUtil.docs(random(), termsEnum, null, null, DocsEnum.FLAG_NONE), false);
        } else {
          this.verifyDocs(term.docs, term.positions, termsEnum.docsAndPositions(null, null), true);
        }

        // Test random seek by ord:
        final int idx = random().nextInt(field.terms.length);
        term = field.terms[idx];
        boolean success = false;
        try {
          termsEnum.seekExact(idx);
          success = true;
        } catch (UnsupportedOperationException uoe) {
          // ok -- skip it
        }
        if (success) {
          assertEquals(status, TermsEnum.SeekStatus.FOUND);
          assertTrue(termsEnum.term().bytesEquals(new BytesRef(term.text2)));
          assertEquals(term.docs.length, termsEnum.docFreq());
          if (field.omitTF) {
            this.verifyDocs(term.docs, term.positions, _TestUtil.docs(random(), termsEnum, null, null, DocsEnum.FLAG_NONE), false);
          } else {
            this.verifyDocs(term.docs, term.positions, termsEnum.docsAndPositions(null, null), true);
          }
        }

        // Test seek to non-existent terms:
        if (VERBOSE) {
          System.out.println("TEST: seek non-exist terms");
        }
        for(int i=0;i<100;i++) {
          final String text2 = _TestUtil.randomUnicodeString(random()) + ".";
          status = termsEnum.seekCeil(new BytesRef(text2));
          assertTrue(status == TermsEnum.SeekStatus.NOT_FOUND ||
                     status == TermsEnum.SeekStatus.END);
        }

        // Seek to each term, backwards:
        if (VERBOSE) {
          System.out.println("TEST: seek terms backwards");
        }
        for(int i=field.terms.length-1;i>=0;i--) {
          assertEquals(Thread.currentThread().getName() + ": field=" + field.fieldInfo.name + " term=" + field.terms[i].text2, TermsEnum.SeekStatus.FOUND, termsEnum.seekCeil(new BytesRef(field.terms[i].text2)));
          assertEquals(field.terms[i].docs.length, termsEnum.docFreq());
        }

        // Seek to each term by ord, backwards
        for(int i=field.terms.length-1;i>=0;i--) {
          try {
            termsEnum.seekExact(i);
            assertEquals(field.terms[i].docs.length, termsEnum.docFreq());
            assertTrue(termsEnum.term().bytesEquals(new BytesRef(field.terms[i].text2)));
          } catch (UnsupportedOperationException uoe) {
          }
        }

        // Seek to non-existent empty-string term
        status = termsEnum.seekCeil(new BytesRef(""));
        assertNotNull(status);
        //assertEquals(TermsEnum.SeekStatus.NOT_FOUND, status);

        // Make sure we're now pointing to first term
        assertTrue(termsEnum.term().bytesEquals(new BytesRef(field.terms[0].text2)));

        // Test docs enum
        termsEnum.seekCeil(new BytesRef(""));
        upto = 0;
        do {
          term = field.terms[upto];
          if (random().nextInt(3) == 1) {
            final DocsEnum docs;
            final DocsEnum docsAndFreqs;
            final DocsAndPositionsEnum postings;
            if (!field.omitTF) {
              postings = termsEnum.docsAndPositions(null, null);
              if (postings != null) {
                docs = docsAndFreqs = postings;
              } else {
                docs = docsAndFreqs = _TestUtil.docs(random(), termsEnum, null, null, DocsEnum.FLAG_FREQS);
              }
            } else {
              postings = null;
              docsAndFreqs = null;
              docs = _TestUtil.docs(random(), termsEnum, null, null, DocsEnum.FLAG_NONE);
            }
            assertNotNull(docs);
            int upto2 = -1;
            boolean ended = false;
            while(upto2 < term.docs.length-1) {
              // Maybe skip:
              final int left = term.docs.length-upto2;
              int doc;
              if (random().nextInt(3) == 1 && left >= 1) {
                final int inc = 1+random().nextInt(left-1);
                upto2 += inc;
                if (random().nextInt(2) == 1) {
                  doc = docs.advance(term.docs[upto2]);
                  assertEquals(term.docs[upto2], doc);
                } else {
                  doc = docs.advance(1+term.docs[upto2]);
                  if (doc == DocIdSetIterator.NO_MORE_DOCS) {
                    // skipped past last doc
                    assert upto2 == term.docs.length-1;
                    ended = true;
                    break;
                  } else {
                    // skipped to next doc
                    assert upto2 < term.docs.length-1;
                    if (doc >= term.docs[1+upto2]) {
                      upto2++;
                    }
                  }
                }
              } else {
                doc = docs.nextDoc();
                assertTrue(doc != -1);
                upto2++;
              }
              assertEquals(term.docs[upto2], doc);
              if (!field.omitTF) {
                assertEquals(term.positions[upto2].length, postings.freq());
                if (random().nextInt(2) == 1) {
                  this.verifyPositions(term.positions[upto2], postings);
                }
              }
            }

            if (!ended) {
              assertEquals(DocIdSetIterator.NO_MORE_DOCS, docs.nextDoc());
            }
          }
          upto++;

        } while (termsEnum.next() != null);

        assertEquals(upto, field.terms.length);
      }
    }
  }

  private void write(final FieldInfos fieldInfos, final Directory dir, final FieldData[] fields, boolean allowPreFlex) throws Throwable {

    final int termIndexInterval = _TestUtil.nextInt(random(), 13, 27);
    final Codec codec = Codec.getDefault();
    final SegmentInfo si = new SegmentInfo(dir, Constants.LUCENE_MAIN_VERSION, SEGMENT, 10000, false, codec, null, null);
    final SegmentWriteState state = new SegmentWriteState(InfoStream.getDefault(), dir, si, fieldInfos, termIndexInterval, null, newIOContext(random()));

    final FieldsConsumer consumer = codec.postingsFormat().fieldsConsumer(state);
    Arrays.sort(fields);
    for (final FieldData field : fields) {
      if (!allowPreFlex && codec instanceof Lucene3xCodec) {
        // code below expects unicode sort order
        continue;
      }
      field.write(consumer);
    }
    consumer.close();
  }
  
  public void testDocsOnlyFreq() throws Exception {
    // tests that when fields are indexed with DOCS_ONLY, the Codec
    // returns 1 in docsEnum.freq()
    Directory dir = newDirectory();
    Random random = random();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random)));
    // we don't need many documents to assert this, but don't use one document either
    int numDocs = atLeast(random, 50);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      doc.add(new StringField("f", "doc", Store.NO));
      writer.addDocument(doc);
    }
    writer.close();
    
    Term term = new Term("f", new BytesRef("doc"));
    DirectoryReader reader = DirectoryReader.open(dir);
    for (AtomicReaderContext ctx : reader.leaves()) {
      DocsEnum de = ctx.reader().termDocsEnum(term);
      while (de.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
        assertEquals("wrong freq for doc " + de.docID(), 1, de.freq());
      }
    }
    reader.close();
    
    dir.close();
  }
}
