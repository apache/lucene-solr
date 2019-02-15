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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.Version;
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

  static class FieldData implements Comparable<FieldData> {
    final FieldInfo fieldInfo;
    final TermData[] terms;
    final boolean omitTF;
    final boolean storePayloads;

    public FieldData(final String name, final FieldInfos.Builder fieldInfos, final TermData[] terms, final boolean omitTF, final boolean storePayloads) {
      this.omitTF = omitTF;
      this.storePayloads = storePayloads;
      // TODO: change this test to use all three
      fieldInfo = fieldInfos.getOrAdd(name);
      if (omitTF) {
        fieldInfo.setIndexOptions(IndexOptions.DOCS);
      } else {
        fieldInfo.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
      }
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
  }

  static class PositionData {
    int pos;
    BytesRef payload;

    PositionData(final int pos, final BytesRef payload) {
      this.pos = pos;
      this.payload = payload;
    }
  }

  static class TermData implements Comparable<TermData> {
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
  }

  final private static String SEGMENT = "0";

  TermData[] makeRandomTerms(final boolean omitTF, final boolean storePayloads) {
    final int numTerms = 1+random().nextInt(NUM_TERMS_RAND);
    //final int numTerms = 2;
    final TermData[] terms = new TermData[numTerms];

    final HashSet<String> termsSeen = new HashSet<>();

    for(int i=0;i<numTerms;i++) {

      // Make term text
      String text2;
      while(true) {
        text2 = TestUtil.randomUnicodeString(random());
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
        docID += TestUtil.nextInt(random(), 1, 10);
        docs[j] = docID;

        if (!omitTF) {
          final int termFreq = 1+random().nextInt(TERM_DOC_FREQ_RAND);
          positions[j] = new PositionData[termFreq];
          int position = 0;
          for(int k=0;k<termFreq;k++) {
            position += TestUtil.nextInt(random(), 1, 10);

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

    final FieldInfos.Builder builder = new FieldInfos.Builder(new FieldInfos.FieldNumbers(null));

    final FieldData field = new FieldData("field", builder, terms, true, false);
    final FieldData[] fields = new FieldData[] {field};
    final FieldInfos fieldInfos = builder.finish();
    final Directory dir = newDirectory();
    Codec codec = Codec.getDefault();
    final SegmentInfo si = new SegmentInfo(dir, Version.LATEST, Version.LATEST, SEGMENT, 10000, false, codec, Collections.emptyMap(), StringHelper.randomId(), new HashMap<>(), null);
    
    this.write(si, fieldInfos, dir, fields);
    final FieldsProducer reader = codec.postingsFormat().fieldsProducer(new SegmentReadState(dir, si, fieldInfos, newIOContext(random())));

    final Iterator<String> fieldsEnum = reader.iterator();
    String fieldName = fieldsEnum.next();
    assertNotNull(fieldName);
    final Terms terms2 = reader.terms(fieldName);
    assertNotNull(terms2);

    final TermsEnum termsEnum = terms2.iterator();

    PostingsEnum postingsEnum = null;
    for(int i=0;i<NUM_TERMS;i++) {
      final BytesRef term = termsEnum.next();
      assertNotNull(term);
      assertEquals(terms[i].text2, term.utf8ToString());

      // do this twice to stress test the codec's reuse, ie,
      // make sure it properly fully resets (rewinds) its
      // internal state:
      for(int iter=0;iter<2;iter++) {
        postingsEnum = TestUtil.docs(random(), termsEnum, postingsEnum, PostingsEnum.NONE);
        assertEquals(terms[i].docs[0], postingsEnum.nextDoc());
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, postingsEnum.nextDoc());
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
    final FieldInfos.Builder builder = new FieldInfos.Builder(new FieldInfos.FieldNumbers(null));

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

    Codec codec = Codec.getDefault();
    final SegmentInfo si = new SegmentInfo(dir, Version.LATEST, Version.LATEST, SEGMENT, 10000, false, codec, Collections.emptyMap(), StringHelper.randomId(), new HashMap<>(), null);
    this.write(si, fieldInfos, dir, fields);

    if (VERBOSE) {
      System.out.println("TEST: now read postings");
    }
    final FieldsProducer terms = codec.postingsFormat().fieldsProducer(new SegmentReadState(dir, si, fieldInfos, newIOContext(random())));

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

  private static class Verify extends Thread {
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

    private void verifyDocs(final int[] docs, final PositionData[][] positions, final PostingsEnum postingsEnum, final boolean doPos) throws Throwable {
      for(int i=0;i<docs.length;i++) {
        final int doc = postingsEnum.nextDoc();
        assertTrue(doc != DocIdSetIterator.NO_MORE_DOCS);
        assertEquals(docs[i], doc);
        if (doPos) {
          this.verifyPositions(positions[i], postingsEnum);
        }
      }
      assertEquals(DocIdSetIterator.NO_MORE_DOCS, postingsEnum.nextDoc());
    }

    byte[] data = new byte[10];

    private void verifyPositions(final PositionData[] positions, final PostingsEnum posEnum) throws Throwable {
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
        final TermsEnum termsEnum = termsDict.terms(field.fieldInfo.name).iterator();

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
          this.verifyDocs(term.docs, term.positions, TestUtil.docs(random(), termsEnum, null, PostingsEnum.NONE), false);
        } else {
          this.verifyDocs(term.docs, term.positions, termsEnum.postings(null, PostingsEnum.ALL), true);
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
            this.verifyDocs(term.docs, term.positions, TestUtil.docs(random(), termsEnum, null, PostingsEnum.NONE), false);
          } else {
            this.verifyDocs(term.docs, term.positions, termsEnum.postings(null, PostingsEnum.ALL), true);
          }
        }

        // Test seek to non-existent terms:
        if (VERBOSE) {
          System.out.println("TEST: seek non-exist terms");
        }
        for(int i=0;i<100;i++) {
          final String text2 = TestUtil.randomUnicodeString(random()) + ".";
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
            final PostingsEnum postings;
            if (!field.omitTF) {
              // TODO: we should randomize which postings features are available, but
              // need to coordinate this with the checks below that rely on such features
              postings = termsEnum.postings(null, PostingsEnum.ALL);
            } else {
              postings = TestUtil.docs(random(), termsEnum, null, PostingsEnum.FREQS);
            }
            assertNotNull(postings);
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
                  doc = postings.advance(term.docs[upto2]);
                  assertEquals(term.docs[upto2], doc);
                } else {
                  doc = postings.advance(1+term.docs[upto2]);
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
                doc = postings.nextDoc();
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
              assertEquals(DocIdSetIterator.NO_MORE_DOCS, postings.nextDoc());
            }
          }
          upto++;

        } while (termsEnum.next() != null);

        assertEquals(upto, field.terms.length);
      }
    }
  }

  private static class DataFields extends Fields {
    private final FieldData[] fields;

    public DataFields(FieldData[] fields) {
      // already sorted:
      this.fields = fields;
    }

    @Override
    public Iterator<String> iterator() {
      return new Iterator<String>() {
        int upto = -1;

        @Override
        public boolean hasNext() {
          return upto+1 < fields.length;
        }

        @Override
        public String next() {
          upto++;
          return fields[upto].fieldInfo.name;
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    }

    @Override
    public Terms terms(String field) {
      // Slow linear search:
      for(FieldData fieldData : fields) {
        if (fieldData.fieldInfo.name.equals(field)) {
          return new DataTerms(fieldData);
        }
      }
      return null;
    }

    @Override
    public int size() {
      return fields.length;
    }
  }

  private static class DataTerms extends Terms {
    final FieldData fieldData;

    public DataTerms(FieldData fieldData) {
      this.fieldData = fieldData;
    }

    @Override
    public TermsEnum iterator() {
      return new DataTermsEnum(fieldData);
    }

    @Override
    public long size() {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getSumTotalTermFreq() {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getSumDocFreq() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getDocCount() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasFreqs() {
      return fieldData.fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
    }

    @Override
    public boolean hasOffsets() {
      return fieldData.fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
    }

    @Override
    public boolean hasPositions() {
      return fieldData.fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
    }

    @Override
    public boolean hasPayloads() {
      return fieldData.fieldInfo.hasPayloads();
    }
  }

  private static class DataTermsEnum extends BaseTermsEnum {
    final FieldData fieldData;
    private int upto = -1;

    public DataTermsEnum(FieldData fieldData) {
      this.fieldData = fieldData;
    }

    @Override
    public BytesRef next() {
      upto++;
      if (upto == fieldData.terms.length) {
        return null;
      }

      return term();
    }

    @Override
    public BytesRef term() {
      return fieldData.terms[upto].text;
    }

    @Override
    public SeekStatus seekCeil(BytesRef text) {
      // Stupid linear impl:
      for(int i=0;i<fieldData.terms.length;i++) {
        int cmp = fieldData.terms[i].text.compareTo(text);
        if (cmp == 0) {
          upto = i;
          return SeekStatus.FOUND;
        } else if (cmp > 0) {
          upto = i;
          return SeekStatus.NOT_FOUND;
        }
      }

      return SeekStatus.END;
    }

    @Override
    public void seekExact(long ord) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long ord() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int docFreq() {
      throw new UnsupportedOperationException();
    }
  
    @Override
    public long totalTermFreq() {
      throw new UnsupportedOperationException();
    }

    @Override
    public PostingsEnum postings(PostingsEnum reuse, int flags) {
      return new DataPostingsEnum(fieldData.terms[upto]);
    }

    @Override
    public ImpactsEnum impacts(int flags) throws IOException {
      throw new UnsupportedOperationException();
    }
  }

  private static class DataPostingsEnum extends PostingsEnum {
    final TermData termData;
    int docUpto = -1;
    int posUpto;

    public DataPostingsEnum(TermData termData) {
      this.termData = termData;
    }

    @Override
    public long cost() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int nextDoc() {
      docUpto++;
      if (docUpto == termData.docs.length) {
        return NO_MORE_DOCS;
      }
      posUpto = -1;
      return docID();
    }

    @Override
    public int docID() {
      return termData.docs[docUpto];
    }

    @Override
    public int advance(int target) {
      // Slow linear impl:
      nextDoc();
      while (docID() < target) {
        nextDoc();
      }

      return docID();
    }

    @Override
    public int freq() {
      return termData.positions[docUpto].length;
    }

    @Override
    public int nextPosition() {
      posUpto++;
      return termData.positions[docUpto][posUpto].pos;
    }

    @Override
    public BytesRef getPayload() {
      return termData.positions[docUpto][posUpto].payload;
    }
    
    @Override
    public int startOffset() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int endOffset() {
      throw new UnsupportedOperationException();
    }
  }

  private void write(SegmentInfo si, final FieldInfos fieldInfos, final Directory dir, final FieldData[] fields) throws Throwable {

    final Codec codec = si.getCodec();
    final SegmentWriteState state = new SegmentWriteState(InfoStream.getDefault(), dir, si, fieldInfos, null, newIOContext(random()));

    Arrays.sort(fields);
    FieldsConsumer consumer = codec.postingsFormat().fieldsConsumer(state);
    NormsProducer fakeNorms = new NormsProducer() {
      
      @Override
      public long ramBytesUsed() {
        return 0;
      }
      
      @Override
      public void close() throws IOException {}
      
      @Override
      public NumericDocValues getNorms(FieldInfo field) throws IOException {
        return new NumericDocValues() {
          
          int doc = -1;
          
          @Override
          public int nextDoc() throws IOException {
            return advance(doc + 1);
          }
          
          @Override
          public int docID() {
            return doc;
          }
          
          @Override
          public long cost() {
            return si.maxDoc();
          }
          
          @Override
          public int advance(int target) throws IOException {
            if (target >= si.maxDoc()) {
              return doc = NO_MORE_DOCS;
            } else {
              return doc = target;
            }
          }
          
          @Override
          public boolean advanceExact(int target) throws IOException {
            doc = target;
            return true;
          }
          
          @Override
          public long longValue() throws IOException {
            return 1;
          }
        };
      }
      
      @Override
      public void checkIntegrity() throws IOException {}
    };
    boolean success = false;
    try {
      consumer.write(new DataFields(fields), fakeNorms);
      success = true;
    } finally {
      if (success) {
        IOUtils.close(consumer);
      } else {
        IOUtils.closeWhileHandlingException(consumer);
      }
    }
  }
  
  public void testDocsOnlyFreq() throws Exception {
    // tests that when fields are indexed with DOCS_ONLY, the Codec
    // returns 1 in docsEnum.freq()
    Directory dir = newDirectory();
    Random random = random();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random)));
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
    for (LeafReaderContext ctx : reader.leaves()) {
      PostingsEnum de = ctx.reader().postings(term);
      while (de.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
        assertEquals("wrong freq for doc " + de.docID(), 1, de.freq());
      }
    }
    reader.close();
    
    dir.close();
  }
  
}
