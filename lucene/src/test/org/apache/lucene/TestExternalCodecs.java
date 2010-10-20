package org.apache.lucene;

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

import org.apache.lucene.util.*;
import org.apache.lucene.index.*;
import org.apache.lucene.document.*;
import org.apache.lucene.search.*;
import org.apache.lucene.analysis.*;
import org.apache.lucene.index.codecs.*;
import org.apache.lucene.index.codecs.standard.*;
import org.apache.lucene.index.codecs.pulsing.*;
import org.apache.lucene.store.*;
import java.util.*;
import java.io.*;

/* Intentionally outside of oal.index to verify fully
   external codecs work fine */

public class TestExternalCodecs extends LuceneTestCase {

  // For fun, test that we can override how terms are
  // sorted, and basic things still work -- this comparator
  // sorts in reversed unicode code point order:
  private static final Comparator<BytesRef> reverseUnicodeComparator = new Comparator<BytesRef>() {
      public int compare(BytesRef t1, BytesRef t2) {
        byte[] b1 = t1.bytes;
        byte[] b2 = t2.bytes;
        int b1Stop;
        int b1Upto = t1.offset;
        int b2Upto = t2.offset;
        if (t1.length < t2.length) {
          b1Stop = t1.offset + t1.length;
        } else {
          b1Stop = t1.offset + t2.length;
        }
        while(b1Upto < b1Stop) {
          final int bb1 = b1[b1Upto++] & 0xff;
          final int bb2 = b2[b2Upto++] & 0xff;
          if (bb1 != bb2) {
            //System.out.println("cmp 1=" + t1 + " 2=" + t2 + " return " + (bb2-bb1));
            return bb2 - bb1;
          }
        }

        // One is prefix of another, or they are equal
        return t2.length-t1.length;
      }

      public boolean equals(Object other) {
        return this == other;
      }
    };

  // TODO
  //   - good improvement would be to write through to disk,
  //     and then load into ram from disk
  public static class RAMOnlyCodec extends Codec {

    // Postings state:
    static class RAMPostings extends FieldsProducer {
      final Map<String,RAMField> fieldToTerms = new TreeMap<String,RAMField>();

      @Override
      public Terms terms(String field) {
        return fieldToTerms.get(field);
      }

      @Override
      public FieldsEnum iterator() {
        return new RAMFieldsEnum(this);
      }

      @Override
      public void close() {
      }

      @Override
      public void loadTermsIndex(int indexDivisor) {
      }
    } 

    static class RAMField extends Terms {
      final String field;
      final SortedMap<String,RAMTerm> termToDocs = new TreeMap<String,RAMTerm>();
      RAMField(String field) {
        this.field = field;
      }

      @Override
      public long getUniqueTermCount() {
        return termToDocs.size();
      }

      @Override
      public TermsEnum iterator() {
        return new RAMTermsEnum(RAMOnlyCodec.RAMField.this);
      }

      @Override
      public Comparator<BytesRef> getComparator() {
        return reverseUnicodeComparator;
      }
    }

    static class RAMTerm {
      final String term;
      final List<RAMDoc> docs = new ArrayList<RAMDoc>();
      public RAMTerm(String term) {
        this.term = term;
      }
    }

    static class RAMDoc {
      final int docID;
      final int[] positions;
      byte[][] payloads;

      public RAMDoc(int docID, int freq) {
        this.docID = docID;
        positions = new int[freq];
      }
    }

    // Classes for writing to the postings state
    private static class RAMFieldsConsumer extends FieldsConsumer {

      private final RAMPostings postings;
      private final RAMTermsConsumer termsConsumer = new RAMTermsConsumer();

      public RAMFieldsConsumer(RAMPostings postings) {
        this.postings = postings;
      }

      @Override
      public TermsConsumer addField(FieldInfo field) {
        RAMField ramField = new RAMField(field.name);
        postings.fieldToTerms.put(field.name, ramField);
        termsConsumer.reset(ramField);
        return termsConsumer;
      }

      @Override
      public void close() {
        // TODO: finalize stuff
      }
    }

    private static class RAMTermsConsumer extends TermsConsumer {
      private RAMField field;
      private final RAMPostingsWriterImpl postingsWriter = new RAMPostingsWriterImpl();
      RAMTerm current;
      
      void reset(RAMField field) {
        this.field = field;
      }
      
      @Override
      public PostingsConsumer startTerm(BytesRef text) {
        final String term = text.utf8ToString();
        current = new RAMTerm(term);
        postingsWriter.reset(current);
        return postingsWriter;
      }

      
      @Override
      public Comparator<BytesRef> getComparator() {
        return BytesRef.getUTF8SortedAsUnicodeComparator();
      }

      @Override
      public void finishTerm(BytesRef text, int numDocs) {
        assert numDocs > 0;
        assert numDocs == current.docs.size();
        field.termToDocs.put(current.term, current);
      }

      @Override
      public void finish() {
      }
    }

    public static class RAMPostingsWriterImpl extends PostingsConsumer {
      private RAMTerm term;
      private RAMDoc current;
      private int posUpto = 0;

      public void reset(RAMTerm term) {
        this.term = term;
      }

      @Override
      public void startDoc(int docID, int freq) {
        current = new RAMDoc(docID, freq);
        term.docs.add(current);
        posUpto = 0;
      }

      @Override
      public void addPosition(int position, BytesRef payload) {
        current.positions[posUpto] = position;
        if (payload != null && payload.length > 0) {
          if (current.payloads == null) {
            current.payloads = new byte[current.positions.length][];
          }
          byte[] bytes = current.payloads[posUpto] = new byte[payload.length];
          System.arraycopy(payload.bytes, payload.offset, bytes, 0, payload.length);
        }
        posUpto++;
      }

      @Override
      public void finishDoc() {
        assert posUpto == current.positions.length;
      }
    }


    // Classes for reading from the postings state
    static class RAMFieldsEnum extends FieldsEnum {
      private final RAMPostings postings;
      private final Iterator<String> it;
      private String current;

      public RAMFieldsEnum(RAMPostings postings) {
        this.postings = postings;
        this.it = postings.fieldToTerms.keySet().iterator();
      }

      @Override
      public String next() {
        if (it.hasNext()) {
          current = it.next();
        } else {
          current = null;
        }
        return current;
      }

      @Override
      public TermsEnum terms() {
        return new RAMTermsEnum(postings.fieldToTerms.get(current));
      }
    }

    static class RAMTermsEnum extends TermsEnum {
      Iterator<String> it;
      String current;
      private final RAMField ramField;

      public RAMTermsEnum(RAMField field) {
        this.ramField = field;
      }
      
      @Override
      public Comparator<BytesRef> getComparator() {
        return BytesRef.getUTF8SortedAsUnicodeComparator();
      }

      @Override
      public BytesRef next() {
        if (it == null) {
          if (current == null) {
            it = ramField.termToDocs.keySet().iterator();
          } else {
            it = ramField.termToDocs.tailMap(current).keySet().iterator();
          }
        }
        if (it.hasNext()) {
          current = it.next();
          return new BytesRef(current);
        } else {
          return null;
        }
      }

      @Override
      public SeekStatus seek(BytesRef term, boolean useCache) {
        current = term.utf8ToString();
        it = null;
        if (ramField.termToDocs.containsKey(current)) {
          return SeekStatus.FOUND;
        } else {
          if (current.compareTo(ramField.termToDocs.lastKey()) > 0) {
            return SeekStatus.END;
          } else {
            return SeekStatus.NOT_FOUND;
          }
        }
      }

      @Override
      public SeekStatus seek(long ord) {
        throw new UnsupportedOperationException();
      }

      @Override
      public long ord() {
        throw new UnsupportedOperationException();
      }

      @Override
      public BytesRef term() {
        // TODO: reuse BytesRef
        return new BytesRef(current);
      }

      @Override
      public int docFreq() {
        return ramField.termToDocs.get(current).docs.size();
      }

      @Override
      public void cacheCurrentTerm() {
      }

      @Override
      public DocsEnum docs(Bits skipDocs, DocsEnum reuse) {
        return new RAMDocsEnum(ramField.termToDocs.get(current), skipDocs);
      }

      @Override
      public DocsAndPositionsEnum docsAndPositions(Bits skipDocs, DocsAndPositionsEnum reuse) {
        return new RAMDocsAndPositionsEnum(ramField.termToDocs.get(current), skipDocs);
      }
    }

    private static class RAMDocsEnum extends DocsEnum {
      private final RAMTerm ramTerm;
      private final Bits skipDocs;
      private RAMDoc current;
      int upto = -1;
      int posUpto = 0;

      public RAMDocsEnum(RAMTerm ramTerm, Bits skipDocs) {
        this.ramTerm = ramTerm;
        this.skipDocs = skipDocs;
      }

      @Override
      public int advance(int targetDocID) {
        do {
          nextDoc();
        } while (upto < ramTerm.docs.size() && current.docID < targetDocID);
        return NO_MORE_DOCS;
      }

      // TODO: override bulk read, for better perf
      @Override
      public int nextDoc() {
        while(true) {
          upto++;
          if (upto < ramTerm.docs.size()) {
            current = ramTerm.docs.get(upto);
            if (skipDocs == null || !skipDocs.get(current.docID)) {
              posUpto = 0;
              return current.docID;
            }
          } else {
            return NO_MORE_DOCS;
          }
        }
      }

      @Override
      public int freq() {
        return current.positions.length;
      }

      @Override
      public int docID() {
        return current.docID;
      }
    }

    private static class RAMDocsAndPositionsEnum extends DocsAndPositionsEnum {
      private final RAMTerm ramTerm;
      private final Bits skipDocs;
      private RAMDoc current;
      int upto = -1;
      int posUpto = 0;

      public RAMDocsAndPositionsEnum(RAMTerm ramTerm, Bits skipDocs) {
        this.ramTerm = ramTerm;
        this.skipDocs = skipDocs;
      }

      @Override
      public int advance(int targetDocID) {
        do {
          nextDoc();
        } while (upto < ramTerm.docs.size() && current.docID < targetDocID);
        return NO_MORE_DOCS;
      }

      // TODO: override bulk read, for better perf
      @Override
      public int nextDoc() {
        while(true) {
          upto++;
          if (upto < ramTerm.docs.size()) {
            current = ramTerm.docs.get(upto);
            if (skipDocs == null || !skipDocs.get(current.docID)) {
              posUpto = 0;
              return current.docID;
            }
          } else {
            return NO_MORE_DOCS;
          }
        }
      }

      @Override
      public int freq() {
        return current.positions.length;
      }

      @Override
      public int docID() {
        return current.docID;
      }

      @Override
      public int nextPosition() {
        return current.positions[posUpto++];
      }

      @Override
      public boolean hasPayload() {
        return current.payloads != null && current.payloads[posUpto-1] != null;
      }

      @Override
      public BytesRef getPayload() {
        return new BytesRef(current.payloads[posUpto-1]);
      }
    }

    // Holds all indexes created
    private final Map<String,RAMPostings> state = new HashMap<String,RAMPostings>();

    @Override
    public FieldsConsumer fieldsConsumer(SegmentWriteState writeState) {
      RAMPostings postings = new RAMPostings();
      RAMFieldsConsumer consumer = new RAMFieldsConsumer(postings);
      synchronized(state) {
        state.put(writeState.segmentName, postings);
      }
      return consumer;
    }

    @Override
    public FieldsProducer fieldsProducer(SegmentReadState readState)
      throws IOException {
      return state.get(readState.segmentInfo.name);
    }

    @Override
    public void getExtensions(Set<String> extensions) {
    }

    @Override
    public void files(Directory dir, SegmentInfo segmentInfo, Set<String> files) {
    }
  }

  public static class MyCodecs extends CodecProvider {
    PerFieldCodecWrapper perField;

    MyCodecs() {
      Codec ram = new RAMOnlyCodec();
      Codec pulsing = new PulsingReverseTermsCodec();
      perField = new PerFieldCodecWrapper(ram);
      perField.add("field2", pulsing);
      perField.add("id", pulsing);
      register(perField);
    }
    
    @Override
    public Codec getWriter(SegmentWriteState state) {
      return perField;
    }
  }

  // copied from PulsingCodec, just changing the terms
  // comparator
  private static class PulsingReverseTermsCodec extends Codec {

    public PulsingReverseTermsCodec() {
      name = "PulsingReverseTerms";
    }

    @Override
    public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
      PostingsWriterBase docsWriter = new StandardPostingsWriter(state);

      // Terms that have <= freqCutoff number of docs are
      // "pulsed" (inlined):
      final int freqCutoff = 1;
      PostingsWriterBase pulsingWriter = new PulsingPostingsWriterImpl(freqCutoff, docsWriter);

      // Terms dict index
      TermsIndexWriterBase indexWriter;
      boolean success = false;
      try {
        indexWriter = new FixedGapTermsIndexWriter(state) {
            // We sort in reverse unicode order, so, we must
            // disable the suffix-stripping opto that
            // FixedGapTermsIndexWriter does by default!
            @Override
            protected int indexedTermPrefixLength(BytesRef priorTerm, BytesRef indexedTerm) {
              return indexedTerm.length;
            }
          };
        success = true;
      } finally {
        if (!success) {
          pulsingWriter.close();
        }
      }

      // Terms dict
      success = false;
      try {
        FieldsConsumer ret = new PrefixCodedTermsWriter(indexWriter, state, pulsingWriter, reverseUnicodeComparator);
        success = true;
        return ret;
      } finally {
        if (!success) {
          try {
            pulsingWriter.close();
          } finally {
            indexWriter.close();
          }
        }
      }
    }

    @Override
    public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {

      PostingsReaderBase docsReader = new StandardPostingsReader(state.dir, state.segmentInfo, state.readBufferSize);
      PostingsReaderBase pulsingReader = new PulsingPostingsReaderImpl(docsReader);

      // Terms dict index reader
      TermsIndexReaderBase indexReader;

      boolean success = false;
      try {
        indexReader = new FixedGapTermsIndexReader(state.dir,
                                                         state.fieldInfos,
                                                         state.segmentInfo.name,
                                                         state.termsIndexDivisor,
                                                         reverseUnicodeComparator);
        success = true;
      } finally {
        if (!success) {
          pulsingReader.close();
        }
      }

      // Terms dict reader
      success = false;
      try {
        FieldsProducer ret = new PrefixCodedTermsReader(indexReader,
                                                         state.dir,
                                                         state.fieldInfos,
                                                         state.segmentInfo.name,
                                                         pulsingReader,
                                                         state.readBufferSize,
                                                         reverseUnicodeComparator,
                                                         StandardCodec.TERMS_CACHE_SIZE);
        success = true;
        return ret;
      } finally {
        if (!success) {
          try {
            pulsingReader.close();
          } finally {
            indexReader.close();
          }
        }
      }
    }

    @Override
    public void files(Directory dir, SegmentInfo segmentInfo, Set<String> files) throws IOException {
      StandardPostingsReader.files(dir, segmentInfo, files);
      PrefixCodedTermsReader.files(dir, segmentInfo, files);
      FixedGapTermsIndexReader.files(dir, segmentInfo, files);
    }

    @Override
    public void getExtensions(Set<String> extensions) {
      StandardCodec.getStandardExtensions(extensions);
    }
  }


  // tests storing "id" and "field2" fields as pulsing codec,
  // whose term sort is backwards unicode code point, and
  // storing "field1" as a custom entirely-in-RAM codec
  public void testPerFieldCodec() throws Exception {
    
    final int NUM_DOCS = 173;
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir,
                                    newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(MockTokenizer.WHITESPACE, true, true)).setCodecProvider(new MyCodecs()));

    w.setMergeFactor(3);
    Document doc = new Document();
    // uses default codec:
    doc.add(newField("field1", "this field uses the standard codec as the test", Field.Store.NO, Field.Index.ANALYZED));
    // uses pulsing codec:
    doc.add(newField("field2", "this field uses the pulsing codec as the test", Field.Store.NO, Field.Index.ANALYZED));
    
    Field idField = newField("id", "", Field.Store.NO, Field.Index.NOT_ANALYZED);
    doc.add(idField);
    for(int i=0;i<NUM_DOCS;i++) {
      idField.setValue(""+i);
      w.addDocument(doc);
      if ((i+1)%10 == 0) {
        w.commit();
      }
    }
    w.deleteDocuments(new Term("id", "77"));

    IndexReader r = IndexReader.open(w);
    IndexReader[] subs = r.getSequentialSubReaders();
    assertTrue(subs.length > 1);
    // test each segment
    for(int i=0;i<subs.length;i++) {
      //System.out.println("test i=" + i);
      testTermsOrder(subs[i]);
    }
    // test each multi-reader
    testTermsOrder(r);
    
    assertEquals(NUM_DOCS-1, r.numDocs());
    IndexSearcher s = new IndexSearcher(r);
    assertEquals(NUM_DOCS-1, s.search(new TermQuery(new Term("field1", "standard")), 1).totalHits);
    assertEquals(NUM_DOCS-1, s.search(new TermQuery(new Term("field2", "pulsing")), 1).totalHits);
    r.close();
    s.close();

    w.deleteDocuments(new Term("id", "44"));
    w.optimize();
    r = IndexReader.open(w);
    assertEquals(NUM_DOCS-2, r.maxDoc());
    assertEquals(NUM_DOCS-2, r.numDocs());
    s = new IndexSearcher(r);
    assertEquals(NUM_DOCS-2, s.search(new TermQuery(new Term("field1", "standard")), 1).totalHits);
    assertEquals(NUM_DOCS-2, s.search(new TermQuery(new Term("field2", "pulsing")), 1).totalHits);
    assertEquals(1, s.search(new TermQuery(new Term("id", "76")), 1).totalHits);
    assertEquals(0, s.search(new TermQuery(new Term("id", "77")), 1).totalHits);
    assertEquals(0, s.search(new TermQuery(new Term("id", "44")), 1).totalHits);

    testTermsOrder(r);
    r.close();
    s.close();

    w.close();

    dir.close();
  }
  private void testTermsOrder(IndexReader r) throws Exception {

    // Verify sort order matches what my comparator said:
    BytesRef lastBytesRef = null;
    TermsEnum terms = MultiFields.getFields(r).terms("id").iterator();
    //System.out.println("id terms:");
    while(true) {
      BytesRef t = terms.next();
      if (t == null) {
        break;
      }
      //System.out.println("  " + t);
      if (lastBytesRef == null) {
        lastBytesRef = new BytesRef(t);
      } else {
        assertTrue("terms in wrong order last=" + lastBytesRef.utf8ToString() + " current=" + t.utf8ToString(), reverseUnicodeComparator.compare(lastBytesRef, t) < 0);
        lastBytesRef.copy(t);
      }
    }
  }
}
