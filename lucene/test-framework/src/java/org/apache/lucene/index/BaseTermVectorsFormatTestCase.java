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

import static org.apache.lucene.index.PostingsEnum.ALL;
import static org.apache.lucene.index.PostingsEnum.FREQS;
import static org.apache.lucene.index.PostingsEnum.NONE;
import static org.apache.lucene.index.PostingsEnum.OFFSETS;
import static org.apache.lucene.index.PostingsEnum.PAYLOADS;
import static org.apache.lucene.index.PostingsEnum.POSITIONS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.TermVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.TermsEnum.SeekStatus;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.AttributeReflector;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.TestUtil;

/**
 * Base class aiming at testing {@link TermVectorsFormat term vectors formats}.
 * To test a new format, all you need is to register a new {@link Codec} which
 * uses it and extend this class and override {@link #getCodec()}.
 * @lucene.experimental
 */
public abstract class BaseTermVectorsFormatTestCase extends BaseIndexFileFormatTestCase {

  /**
   * A combination of term vectors options.
   */
  protected enum Options {
    NONE(false, false, false),
    POSITIONS(true, false, false),
    OFFSETS(false, true, false),
    POSITIONS_AND_OFFSETS(true, true, false),
    POSITIONS_AND_PAYLOADS(true, false, true),
    POSITIONS_AND_OFFSETS_AND_PAYLOADS(true, true, true);
    final boolean positions, offsets, payloads;
    private Options(boolean positions, boolean offsets, boolean payloads) {
      this.positions = positions;
      this.offsets = offsets;
      this.payloads = payloads;
    }
  }

  protected Set<Options> validOptions() {
    return EnumSet.allOf(Options.class);
  }

  protected Options randomOptions() {
    return RandomPicks.randomFrom(random(), new ArrayList<>(validOptions()));
  }

  protected FieldType fieldType(Options options) {
    FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
    ft.setStoreTermVectors(true);
    ft.setStoreTermVectorPositions(options.positions);
    ft.setStoreTermVectorOffsets(options.offsets);
    ft.setStoreTermVectorPayloads(options.payloads);
    ft.freeze();
    return ft;
  }

  @Override
  protected void addRandomFields(Document doc) {
    for (Options opts : validOptions()) {
      FieldType ft = fieldType(opts);
      final int numFields = random().nextInt(5);
      for (int j = 0; j < numFields; ++j) {
        doc.add(new Field("f_" + opts, TestUtil.randomSimpleString(random(), 2), ft));
      }
    }
  }

  // custom impl to test cases that are forbidden by the default OffsetAttribute impl
  private static class PermissiveOffsetAttributeImpl extends AttributeImpl implements OffsetAttribute {

    int start, end;

    @Override
    public int startOffset() {
      return start;
    }

    @Override
    public int endOffset() {
      return end;
    }

    @Override
    public void setOffset(int startOffset, int endOffset) {
      // no check!
      start = startOffset;
      end = endOffset;
    }

    @Override
    public void clear() {
      start = end = 0;
    }

    @Override
    public boolean equals(Object other) {
      if (other == this) {
        return true;
      }

      if (other instanceof PermissiveOffsetAttributeImpl) {
        PermissiveOffsetAttributeImpl o = (PermissiveOffsetAttributeImpl) other;
        return o.start == start && o.end == end;
      }

      return false;
    }

    @Override
    public int hashCode() {
      return start + 31 * end;
    }

    @Override
    public void copyTo(AttributeImpl target) {
      OffsetAttribute t = (OffsetAttribute) target;
      t.setOffset(start, end);
    }

    @Override
    public void reflectWith(AttributeReflector reflector) {
      reflector.reflect(OffsetAttribute.class, "startOffset", start);
      reflector.reflect(OffsetAttribute.class, "endOffset", end);
    }

  }

  // TODO: use CannedTokenStream?
  // TODO: pull out and make top-level-utility, separate from TermVectors
  /** Produces a random TokenStream based off of provided terms. */
  public static class RandomTokenStream extends TokenStream {

    final String[] terms;
    final BytesRef[] termBytes;
    final int[] positionsIncrements;
    final int[] positions;
    final int[] startOffsets, endOffsets;
    final BytesRef[] payloads;

    final Map<String, Integer> freqs;
    final Map<Integer, Set<Integer>> positionToTerms;
    final Map<Integer, Set<Integer>> startOffsetToTerms;

    final CharTermAttribute termAtt;
    final PositionIncrementAttribute piAtt;
    final OffsetAttribute oAtt;
    final PayloadAttribute pAtt;
    int i = 0;

    public RandomTokenStream(int len, String[] sampleTerms, BytesRef[] sampleTermBytes) {
      this(len, sampleTerms, sampleTermBytes, rarely());
    }

    public RandomTokenStream(int len, String[] sampleTerms, BytesRef[] sampleTermBytes, boolean offsetsGoBackwards) {
      terms = new String[len];
      termBytes = new BytesRef[len];
      positionsIncrements = new int[len];
      positions = new int[len];
      startOffsets = new int[len];
      endOffsets = new int[len];
      payloads = new BytesRef[len];
      for (int i = 0; i < len; ++i) {
        final int o = random().nextInt(sampleTerms.length);
        terms[i] = sampleTerms[o];
        termBytes[i] = sampleTermBytes[o];
        positionsIncrements[i] = TestUtil.nextInt(random(), i == 0 ? 1 : 0, 10);
        if (offsetsGoBackwards) {
          startOffsets[i] = random().nextInt();
          endOffsets[i] = random().nextInt();
        } else {
          if (i == 0) {
            startOffsets[i] = TestUtil.nextInt(random(), 0, 1 << 16);
          } else {
            startOffsets[i] = startOffsets[i-1] + TestUtil.nextInt(random(), 0, rarely() ? 1 << 16 : 20);
          }
          endOffsets[i] = startOffsets[i] + TestUtil.nextInt(random(), 0, rarely() ? 1 << 10 : 20);
        }
      }

      for (int i = 0; i < len; ++i) {
        if (i == 0) {
          positions[i] = positionsIncrements[i] - 1;
        } else {
          positions[i] = positions[i - 1] + positionsIncrements[i];
        }
      }
      if (rarely()) {
        Arrays.fill(payloads, randomPayload());
      } else {
        for (int i = 0; i < len; ++i) {
          payloads[i] = randomPayload();
        }
      }

      positionToTerms = new HashMap<>(len);
      startOffsetToTerms = new HashMap<>(len);
      for (int i = 0; i < len; ++i) {
        if (!positionToTerms.containsKey(positions[i])) {
          positionToTerms.put(positions[i], new HashSet<Integer>(1));
        }
        positionToTerms.get(positions[i]).add(i);
        if (!startOffsetToTerms.containsKey(startOffsets[i])) {
          startOffsetToTerms.put(startOffsets[i], new HashSet<Integer>(1));
        }
        startOffsetToTerms.get(startOffsets[i]).add(i);
      }

      freqs = new HashMap<>();
      for (String term : terms) {
        if (freqs.containsKey(term)) {
          freqs.put(term, freqs.get(term) + 1);
        } else {
          freqs.put(term, 1);
        }
      }

      addAttributeImpl(new PermissiveOffsetAttributeImpl());

      termAtt = addAttribute(CharTermAttribute.class);
      piAtt = addAttribute(PositionIncrementAttribute.class);
      oAtt = addAttribute(OffsetAttribute.class);
      pAtt = addAttribute(PayloadAttribute.class);
    }

    protected BytesRef randomPayload() {
      final int len = random().nextInt(5);
      if (len == 0) {
        return null;
      }
      final BytesRef payload = new BytesRef(len);
      random().nextBytes(payload.bytes);
      payload.length = len;
      return payload;
    }

    public boolean hasPayloads() {
      for (BytesRef payload : payloads) {
        if (payload != null && payload.length > 0) {
          return true;
        }
      }
      return false;
    }

    public String[] getTerms() {
      return terms;
    }

    public BytesRef[] getTermBytes() {
      return termBytes;
    }

    public int[] getPositionsIncrements() {
      return positionsIncrements;
    }

    public int[] getStartOffsets() {
      return startOffsets;
    }

    public int[] getEndOffsets() {
      return endOffsets;
    }

    public BytesRef[] getPayloads() {
      return payloads;
    }

    @Override
    public void reset() throws IOException {
      i = 0;
      super.reset();
    }

    @Override
    public final boolean incrementToken() throws IOException {
      if (i < terms.length) {
        clearAttributes();
        termAtt.setLength(0).append(terms[i]);
        piAtt.setPositionIncrement(positionsIncrements[i]);
        oAtt.setOffset(startOffsets[i], endOffsets[i]);
        pAtt.setPayload(payloads[i]);
        ++i;
        return true;
      } else {
        return false;
      }
    }

  }

  protected class RandomDocument {

    private final String[] fieldNames;
    private final FieldType[] fieldTypes;
    private final RandomTokenStream[] tokenStreams;

    protected RandomDocument(int fieldCount, int maxTermCount, Options options, String[] fieldNames, String[] sampleTerms, BytesRef[] sampleTermBytes) {
      if (fieldCount > fieldNames.length) {
        throw new IllegalArgumentException();
      }
      this.fieldNames = new String[fieldCount];
      fieldTypes = new FieldType[fieldCount];
      tokenStreams = new RandomTokenStream[fieldCount];
      Arrays.fill(fieldTypes, fieldType(options));
      final Set<String> usedFileNames = new HashSet<>();
      for (int i = 0; i < fieldCount; ++i) {
        do {
          this.fieldNames[i] = RandomPicks.randomFrom(random(), fieldNames);
        } while (usedFileNames.contains(this.fieldNames[i]));
        usedFileNames.add(this.fieldNames[i]);
        tokenStreams[i] = new RandomTokenStream(TestUtil.nextInt(random(), 1, maxTermCount), sampleTerms, sampleTermBytes);
      }
    }

    public Document toDocument() {
      final Document doc = new Document();
      for (int i = 0; i < fieldNames.length; ++i) {
        doc.add(new Field(fieldNames[i], tokenStreams[i], fieldTypes[i]));
      }
      return doc;
    }

  }

  protected class RandomDocumentFactory {

    private final String[] fieldNames;
    private final String[] terms;
    private final BytesRef[] termBytes;

    protected RandomDocumentFactory(int distinctFieldNames, int disctinctTerms) {
      final Set<String> fieldNames = new HashSet<>();
      while (fieldNames.size() < distinctFieldNames) {
        fieldNames.add(TestUtil.randomSimpleString(random()));
        fieldNames.remove("id");
      }
      this.fieldNames = fieldNames.toArray(new String[0]);
      terms = new String[disctinctTerms];
      termBytes = new BytesRef[disctinctTerms];
      for (int i = 0; i < disctinctTerms; ++i) {
        terms[i] = TestUtil.randomRealisticUnicodeString(random());
        termBytes[i] = new BytesRef(terms[i]);
      }
    }

    public RandomDocument newDocument(int fieldCount, int maxTermCount, Options options) {
      return new RandomDocument(fieldCount, maxTermCount, options, fieldNames, terms, termBytes);
    }

  }

  protected void assertEquals(RandomDocument doc, Fields fields) throws IOException {
    // compare field names
    assertEquals(doc == null, fields == null);
    assertEquals(doc.fieldNames.length, fields.size());
    final Set<String> fields1 = new HashSet<>();
    final Set<String> fields2 = new HashSet<>();
    for (int i = 0; i < doc.fieldNames.length; ++i) {
      fields1.add(doc.fieldNames[i]);
    }
    for (String field : fields) {
      fields2.add(field);
    }
    assertEquals(fields1, fields2);

    for (int i = 0; i < doc.fieldNames.length; ++i) {
      assertEquals(doc.tokenStreams[i], doc.fieldTypes[i], fields.terms(doc.fieldNames[i]));
    }
  }

  protected static boolean equals(Object o1, Object o2) {
    if (o1 == null) {
      return o2 == null;
    } else {
      return o1.equals(o2);
    }
  }

  // to test reuse
  private final ThreadLocal<PostingsEnum> docsEnum = new ThreadLocal<>();
  private final ThreadLocal<PostingsEnum> docsAndPositionsEnum = new ThreadLocal<>();

  protected void assertEquals(RandomTokenStream tk, FieldType ft, Terms terms) throws IOException {
    assertEquals(1, terms.getDocCount());
    final int termCount = new HashSet<>(Arrays.asList(tk.terms)).size();
    assertEquals(termCount, terms.size());
    assertEquals(termCount, terms.getSumDocFreq());
    assertEquals(ft.storeTermVectorPositions(), terms.hasPositions());
    assertEquals(ft.storeTermVectorOffsets(), terms.hasOffsets());
    assertEquals(ft.storeTermVectorPayloads() && tk.hasPayloads(), terms.hasPayloads());
    final Set<BytesRef> uniqueTerms = new HashSet<>();
    for (String term : tk.freqs.keySet()) {
      uniqueTerms.add(new BytesRef(term));
    }
    final BytesRef[] sortedTerms = uniqueTerms.toArray(new BytesRef[0]);
    Arrays.sort(sortedTerms);
    final TermsEnum termsEnum = terms.iterator();
    for (int i = 0; i < sortedTerms.length; ++i) {
      final BytesRef nextTerm = termsEnum.next();
      assertEquals(sortedTerms[i], nextTerm);
      assertEquals(sortedTerms[i], termsEnum.term());
      assertEquals(1, termsEnum.docFreq());

      PostingsEnum postingsEnum = termsEnum.postings(null);
      postingsEnum = termsEnum.postings(random().nextBoolean() ? null : postingsEnum);
      assertNotNull(postingsEnum);
      assertEquals(0, postingsEnum.nextDoc());
      assertEquals(0, postingsEnum.docID());
      assertEquals(tk.freqs.get(termsEnum.term().utf8ToString()), (Integer) postingsEnum.freq());
      assertEquals(PostingsEnum.NO_MORE_DOCS, postingsEnum.nextDoc());
      this.docsEnum.set(postingsEnum);

      PostingsEnum docsAndPositionsEnum = termsEnum.postings(null);
      docsAndPositionsEnum = termsEnum.postings(random().nextBoolean() ? null : docsAndPositionsEnum, PostingsEnum.POSITIONS);
      if (terms.hasPositions() || terms.hasOffsets()) {
        assertEquals(0, docsAndPositionsEnum.nextDoc());
        final int freq = docsAndPositionsEnum.freq();
        assertEquals(tk.freqs.get(termsEnum.term().utf8ToString()), (Integer) freq);
        if (docsAndPositionsEnum != null) {
          for (int k = 0; k < freq; ++k) {
            final int position = docsAndPositionsEnum.nextPosition();
            final Set<Integer> indexes;
            if (terms.hasPositions()) {
              indexes = tk.positionToTerms.get(position);
              assertNotNull(indexes);
            } else {
              indexes = tk.startOffsetToTerms.get(docsAndPositionsEnum.startOffset());
              assertNotNull(indexes);
            }
            if (terms.hasPositions()) {
              boolean foundPosition = false;
              for (int index : indexes) {
                if (tk.termBytes[index].equals(termsEnum.term()) && tk.positions[index] == position) {
                  foundPosition = true;
                  break;
                }
              }
              assertTrue(foundPosition);
            }
            if (terms.hasOffsets()) {
              boolean foundOffset = false;
              for (int index : indexes) {
                if (tk.termBytes[index].equals(termsEnum.term()) && tk.startOffsets[index] == docsAndPositionsEnum.startOffset() && tk.endOffsets[index] == docsAndPositionsEnum.endOffset()) {
                  foundOffset = true;
                  break;
                }
              }
              assertTrue(foundOffset);
            }
            if (terms.hasPayloads()) {
              boolean foundPayload = false;
              for (int index : indexes) {
                if (tk.termBytes[index].equals(termsEnum.term()) && equals(tk.payloads[index], docsAndPositionsEnum.getPayload())) {
                  foundPayload = true;
                  break;
                }
              }
              assertTrue(foundPayload);
            }
          }
          try {
            docsAndPositionsEnum.nextPosition();
            fail();
          } catch (Exception | AssertionError e) {
            // ok
          }
        }
        assertEquals(PostingsEnum.NO_MORE_DOCS, docsAndPositionsEnum.nextDoc());
      }
      this.docsEnum.set(docsAndPositionsEnum);
    }
    assertNull(termsEnum.next());
    for (int i = 0; i < 5; ++i) {
      if (random().nextBoolean()) {
        assertTrue(termsEnum.seekExact(RandomPicks.randomFrom(random(), tk.termBytes)));
      } else {
        assertEquals(SeekStatus.FOUND, termsEnum.seekCeil(RandomPicks.randomFrom(random(), tk.termBytes)));
      }
    }
  }

  protected Document addId(Document doc, String id) {
    doc.add(new StringField("id", id, Store.NO));
    return doc;
  }

  protected int docID(IndexReader reader, String id) throws IOException {
    return new IndexSearcher(reader).search(new TermQuery(new Term("id", id)), 1).scoreDocs[0].doc;
  }

  // only one doc with vectors
  public void testRareVectors() throws IOException {
    final RandomDocumentFactory docFactory = new RandomDocumentFactory(10, 20);
    for (Options options : validOptions()) {
      final int numDocs = atLeast(200);
      final int docWithVectors = random().nextInt(numDocs);
      final Document emptyDoc = new Document();
      final Directory dir = newDirectory();
      final RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
      final RandomDocument doc = docFactory.newDocument(TestUtil.nextInt(random(), 1, 3), 20, options);
      for (int i = 0; i < numDocs; ++i) {
        if (i == docWithVectors) {
          writer.addDocument(addId(doc.toDocument(), "42"));
        } else {
          writer.addDocument(emptyDoc);
        }
      }
      final IndexReader reader = writer.getReader();
      final int docWithVectorsID = docID(reader, "42");
      for (int i = 0; i < 10; ++i) {
        final int docID = random().nextInt(numDocs);
        final Fields fields = reader.getTermVectors(docID);
        if (docID == docWithVectorsID) {
          assertEquals(doc, fields);
        } else {
          assertNull(fields);
        }
      }
      final Fields fields = reader.getTermVectors(docWithVectorsID);
      assertEquals(doc, fields);
      reader.close();
      writer.close();
      dir.close();
    }
  }

  public void testHighFreqs() throws IOException {
    final RandomDocumentFactory docFactory = new RandomDocumentFactory(3, 5);
    for (Options options : validOptions()) {
      if (options == Options.NONE) {
        continue;
      }
      final Directory dir = newDirectory();
      final RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
      final RandomDocument doc = docFactory.newDocument(TestUtil.nextInt(random(), 1, 2), atLeast(20000), options);
      writer.addDocument(doc.toDocument());
      final IndexReader reader = writer.getReader();
      assertEquals(doc, reader.getTermVectors(0));
      reader.close();
      writer.close();
      dir.close();
    }
  }

  public void testLotsOfFields() throws IOException {
    final RandomDocumentFactory docFactory = new RandomDocumentFactory(5000, 10);
    for (Options options : validOptions()) {
      final Directory dir = newDirectory();
      final RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
      final RandomDocument doc = docFactory.newDocument(atLeast(100), 5, options);
      writer.addDocument(doc.toDocument());
      final IndexReader reader = writer.getReader();
      assertEquals(doc, reader.getTermVectors(0));
      reader.close();
      writer.close();
      dir.close();
    }
  }

  // different options for the same field
  public void testMixedOptions() throws IOException {
    final int numFields = TestUtil.nextInt(random(), 1, 3);
    final RandomDocumentFactory docFactory = new RandomDocumentFactory(numFields, 10);
    for (Options options1 : validOptions()) {
      for (Options options2 : validOptions()) {
        if (options1 == options2) {
          continue;
        }
        final Directory dir = newDirectory();
        final RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
        final RandomDocument doc1 = docFactory.newDocument(numFields, 20, options1);
        final RandomDocument doc2 = docFactory.newDocument(numFields, 20,  options2);
        writer.addDocument(addId(doc1.toDocument(), "1"));
        writer.addDocument(addId(doc2.toDocument(), "2"));
        final IndexReader reader = writer.getReader();
        final int doc1ID = docID(reader, "1");
        assertEquals(doc1, reader.getTermVectors(doc1ID));
        final int doc2ID = docID(reader, "2");
        assertEquals(doc2, reader.getTermVectors(doc2ID));
        reader.close();
        writer.close();
        dir.close();
      }
    }
  }

  public void testRandom() throws IOException {
    final RandomDocumentFactory docFactory = new RandomDocumentFactory(5, 20);
    final int numDocs = atLeast(100);
    final RandomDocument[] docs = new RandomDocument[numDocs];
    for (int i = 0; i < numDocs; ++i) {
      docs[i] = docFactory.newDocument(TestUtil.nextInt(random(), 1, 3), TestUtil.nextInt(random(), 10, 50), randomOptions());
    }
    final Directory dir = newDirectory();
    final RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    for (int i = 0; i < numDocs; ++i) {
      writer.addDocument(addId(docs[i].toDocument(), ""+i));
    }
    final IndexReader reader = writer.getReader();
    for (int i = 0; i < numDocs; ++i) {
      final int docID = docID(reader, ""+i);
      assertEquals(docs[i], reader.getTermVectors(docID));
    }
    reader.close();
    writer.close();
    dir.close();
  }

  public void testMerge() throws IOException {
    final RandomDocumentFactory docFactory = new RandomDocumentFactory(5, 20);
    final int numDocs = atLeast(100);
    final int numDeletes = random().nextInt(numDocs);
    final Set<Integer> deletes = new HashSet<>();
    while (deletes.size() < numDeletes) {
      deletes.add(random().nextInt(numDocs));
    }
    for (Options options : validOptions()) {
      final RandomDocument[] docs = new RandomDocument[numDocs];
      for (int i = 0; i < numDocs; ++i) {
        docs[i] = docFactory.newDocument(TestUtil.nextInt(random(), 1, 3), atLeast(10), options);
      }
      final Directory dir = newDirectory();
      final RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
      for (int i = 0; i < numDocs; ++i) {
        writer.addDocument(addId(docs[i].toDocument(), ""+i));
        if (rarely()) {
          writer.commit();
        }
      }
      for (int delete : deletes) {
        writer.deleteDocuments(new Term("id", "" + delete));
      }
      // merge with deletes
      writer.forceMerge(1);
      final IndexReader reader = writer.getReader();
      for (int i = 0; i < numDocs; ++i) {
        if (!deletes.contains(i)) {
          final int docID = docID(reader, ""+i);
          assertEquals(docs[i], reader.getTermVectors(docID));
        }
      }
      reader.close();
      writer.close();
      dir.close();
    }
  }

  // run random tests from different threads to make sure the per-thread clones
  // don't share mutable data
  public void testClone() throws IOException, InterruptedException {
    final RandomDocumentFactory docFactory = new RandomDocumentFactory(5, 20);
    final int numDocs = atLeast(100);
    for (Options options : validOptions()) {
      final RandomDocument[] docs = new RandomDocument[numDocs];
      for (int i = 0; i < numDocs; ++i) {
        docs[i] = docFactory.newDocument(TestUtil.nextInt(random(), 1, 3), atLeast(10), options);
      }
      final Directory dir = newDirectory();
      final RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
      for (int i = 0; i < numDocs; ++i) {
        writer.addDocument(addId(docs[i].toDocument(), ""+i));
      }
      final IndexReader reader = writer.getReader();
      for (int i = 0; i < numDocs; ++i) {
        final int docID = docID(reader, ""+i);
        assertEquals(docs[i], reader.getTermVectors(docID));
      }

      final AtomicReference<Throwable> exception = new AtomicReference<>();
      final Thread[] threads = new Thread[2];
      for (int i = 0; i < threads.length; ++i) {
        threads[i] = new Thread() {
          @Override
          public void run() {
            try {
              for (int i = 0; i < atLeast(100); ++i) {
                final int idx = random().nextInt(numDocs);
                final int docID = docID(reader, ""+idx);
                assertEquals(docs[idx], reader.getTermVectors(docID));
              }
            } catch (Throwable t) {
              exception.set(t);
            }
          }
        };
      }
      for (Thread thread : threads) {
        thread.start();
      }
      for (Thread thread : threads) {
        thread.join();
      }
      reader.close();
      writer.close();
      dir.close();
      assertNull("One thread threw an exception", exception.get());
    }
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
    ft.setStoreTermVectors(true);
    doc.add(new Field("foo", "bar bar", ft));
    iw.addDocument(doc);
    DirectoryReader reader = DirectoryReader.open(iw);
    
    Terms terms = getOnlyLeafReader(reader).getTermVector(0, "foo");
    TermsEnum termsEnum = terms.iterator();
    assertNotNull(termsEnum);
    assertEquals(new BytesRef("bar"), termsEnum.next());
    
    // simple use (FREQS)
    PostingsEnum postings = termsEnum.postings(null);
    assertEquals(-1, postings.docID());
    assertEquals(0, postings.nextDoc());
    assertEquals(2, postings.freq());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, postings.nextDoc());
    
    // termsenum reuse (FREQS)
    PostingsEnum postings2 = termsEnum.postings(postings);
    assertNotNull(postings2);
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
    FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
    ft.setStoreTermVectors(true);
    ft.setStoreTermVectorPositions(true);
    doc.add(new Field("foo", "bar bar", ft));
    iw.addDocument(doc);
    DirectoryReader reader = DirectoryReader.open(iw);
    
    Terms terms = getOnlyLeafReader(reader).getTermVector(0, "foo");
    TermsEnum termsEnum = terms.iterator();
    assertNotNull(termsEnum);
    assertEquals(new BytesRef("bar"), termsEnum.next());
    
    // simple use (FREQS)
    PostingsEnum postings = termsEnum.postings(null);
    assertEquals(-1, postings.docID());
    assertEquals(0, postings.nextDoc());
    assertEquals(2, postings.freq());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, postings.nextDoc());
    
    // termsenum reuse (FREQS)
    PostingsEnum postings2 = termsEnum.postings(postings);
    assertNotNull(postings2);
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
    // and it had better work
    assertEquals(-1, docsOnly2.docID());
    assertEquals(0, docsOnly2.nextDoc());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsOnly2.freq() == 1 || docsOnly2.freq() == 2);
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsOnly2.nextDoc());
    
    // asking for positions, ok
    PostingsEnum docsAndPositionsEnum = termsEnum.postings(null, PostingsEnum.POSITIONS);
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
    docsAndPositionsEnum = termsEnum.postings(null, PostingsEnum.PAYLOADS);
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
    
    docsAndPositionsEnum = termsEnum.postings(null, PostingsEnum.OFFSETS);
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
    
    docsAndPositionsEnum = termsEnum.postings(null, PostingsEnum.ALL);
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
    ft.setStoreTermVectors(true);
    ft.setStoreTermVectorPositions(true);
    ft.setStoreTermVectorOffsets(true);
    doc.add(new Field("foo", "bar bar", ft));
    iw.addDocument(doc);
    DirectoryReader reader = DirectoryReader.open(iw);
    
    Terms terms = getOnlyLeafReader(reader).getTermVector(0, "foo");
    TermsEnum termsEnum = terms.iterator();
    assertNotNull(termsEnum);
    assertEquals(new BytesRef("bar"), termsEnum.next());
    
    // simple usage (FREQS)
    PostingsEnum postings = termsEnum.postings(null);
    assertEquals(-1, postings.docID());
    assertEquals(0, postings.nextDoc());
    assertEquals(2, postings.freq());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, postings.nextDoc());
    
    // termsenum reuse (FREQS)
    PostingsEnum postings2 = termsEnum.postings(postings);
    assertNotNull(postings2);
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
    // and it had better work
    assertEquals(-1, docsOnly2.docID());
    assertEquals(0, docsOnly2.nextDoc());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsOnly2.freq() == 1 || docsOnly2.freq() == 2);
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsOnly2.nextDoc());
    
    // asking for positions, ok
    PostingsEnum docsAndPositionsEnum = termsEnum.postings(null, PostingsEnum.POSITIONS);
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
    docsAndPositionsEnum = termsEnum.postings(null, PostingsEnum.PAYLOADS);
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
    
    docsAndPositionsEnum = termsEnum.postings(null, PostingsEnum.OFFSETS);
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
    
    docsAndPositionsEnum = termsEnum.postings(null, PostingsEnum.ALL);
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
  
  public void testPostingsEnumOffsetsWithoutPositions() throws Exception {
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
    ft.setStoreTermVectors(true);
    ft.setStoreTermVectorOffsets(true);
    doc.add(new Field("foo", "bar bar", ft));
    iw.addDocument(doc);
    DirectoryReader reader = DirectoryReader.open(iw);
    
    Terms terms = getOnlyLeafReader(reader).getTermVector(0, "foo");
    TermsEnum termsEnum = terms.iterator();
    assertNotNull(termsEnum);
    assertEquals(new BytesRef("bar"), termsEnum.next());
    
    // simple usage (FREQS)
    PostingsEnum postings = termsEnum.postings(null);
    assertEquals(-1, postings.docID());
    assertEquals(0, postings.nextDoc());
    assertEquals(2, postings.freq());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, postings.nextDoc());
    
    // termsenum reuse (FREQS)
    PostingsEnum postings2 = termsEnum.postings(postings);
    assertNotNull(postings2);
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
    // and it had better work
    assertEquals(-1, docsOnly2.docID());
    assertEquals(0, docsOnly2.nextDoc());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsOnly2.freq() == 1 || docsOnly2.freq() == 2);
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsOnly2.nextDoc());
    
    // asking for positions, ok
    PostingsEnum docsAndPositionsEnum = termsEnum.postings(null, PostingsEnum.POSITIONS);
    assertEquals(-1, docsAndPositionsEnum.docID());
    assertEquals(0, docsAndPositionsEnum.nextDoc());
    assertEquals(2, docsAndPositionsEnum.freq());
    assertEquals(-1, docsAndPositionsEnum.nextPosition());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum.startOffset() == -1 || docsAndPositionsEnum.startOffset() == 0);
    assertTrue(docsAndPositionsEnum.endOffset() == -1 || docsAndPositionsEnum.endOffset() == 3);
    assertNull(docsAndPositionsEnum.getPayload());
    assertEquals(-1, docsAndPositionsEnum.nextPosition());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum.startOffset() == -1 || docsAndPositionsEnum.startOffset() == 4);
    assertTrue(docsAndPositionsEnum.endOffset() == -1 || docsAndPositionsEnum.endOffset() == 7);
    assertNull(docsAndPositionsEnum.getPayload());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum.nextDoc());
    
    // now reuse the positions
    PostingsEnum docsAndPositionsEnum2 = termsEnum.postings(docsAndPositionsEnum, PostingsEnum.POSITIONS);
    assertEquals(-1, docsAndPositionsEnum2.docID());
    assertEquals(0, docsAndPositionsEnum2.nextDoc());
    assertEquals(2, docsAndPositionsEnum2.freq());
    assertEquals(-1, docsAndPositionsEnum2.nextPosition());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum2.startOffset() == -1 || docsAndPositionsEnum2.startOffset() == 0);
    assertTrue(docsAndPositionsEnum2.endOffset() == -1 || docsAndPositionsEnum2.endOffset() == 3);
    assertNull(docsAndPositionsEnum2.getPayload());
    assertEquals(-1, docsAndPositionsEnum2.nextPosition());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum2.startOffset() == -1 || docsAndPositionsEnum2.startOffset() == 4);
    assertTrue(docsAndPositionsEnum2.endOffset() == -1 || docsAndPositionsEnum2.endOffset() == 7);
    assertNull(docsAndPositionsEnum2.getPayload());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum2.nextDoc());
    
    // payloads don't cause an error if they aren't there
    docsAndPositionsEnum = termsEnum.postings(null, PostingsEnum.PAYLOADS);
    assertNotNull(docsAndPositionsEnum);
    // but make sure they work
    assertEquals(-1, docsAndPositionsEnum.docID());
    assertEquals(0, docsAndPositionsEnum.nextDoc());
    assertEquals(2, docsAndPositionsEnum.freq());
    assertEquals(-1, docsAndPositionsEnum.nextPosition());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum.startOffset() == -1 || docsAndPositionsEnum.startOffset() == 0);
    assertTrue(docsAndPositionsEnum.endOffset() == -1 || docsAndPositionsEnum.endOffset() == 3);
    assertNull(docsAndPositionsEnum.getPayload());
    assertEquals(-1, docsAndPositionsEnum.nextPosition());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum.startOffset() == -1 || docsAndPositionsEnum.startOffset() == 4);
    assertTrue(docsAndPositionsEnum.endOffset() == -1 || docsAndPositionsEnum.endOffset() == 7);
    assertNull(docsAndPositionsEnum.getPayload());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum.nextDoc());
    // reuse
    docsAndPositionsEnum2 = termsEnum.postings(docsAndPositionsEnum, PostingsEnum.PAYLOADS);
    assertEquals(-1, docsAndPositionsEnum2.docID());
    assertEquals(0, docsAndPositionsEnum2.nextDoc());
    assertEquals(2, docsAndPositionsEnum2.freq());
    assertEquals(-1, docsAndPositionsEnum2.nextPosition());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum2.startOffset() == -1 || docsAndPositionsEnum2.startOffset() == 0);
    assertTrue(docsAndPositionsEnum2.endOffset() == -1 || docsAndPositionsEnum2.endOffset() == 3);
    assertNull(docsAndPositionsEnum2.getPayload());
    assertEquals(-1, docsAndPositionsEnum2.nextPosition());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsAndPositionsEnum2.startOffset() == -1 || docsAndPositionsEnum2.startOffset() == 4);
    assertTrue(docsAndPositionsEnum2.endOffset() == -1 || docsAndPositionsEnum2.endOffset() == 7);
    assertNull(docsAndPositionsEnum2.getPayload());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum2.nextDoc());
    
    docsAndPositionsEnum = termsEnum.postings(null, PostingsEnum.OFFSETS);
    assertNotNull(docsAndPositionsEnum);
    assertEquals(-1, docsAndPositionsEnum.docID());
    assertEquals(0, docsAndPositionsEnum.nextDoc());
    assertEquals(2, docsAndPositionsEnum.freq());
    assertEquals(-1, docsAndPositionsEnum.nextPosition());
    assertEquals(0, docsAndPositionsEnum.startOffset());
    assertEquals(3, docsAndPositionsEnum.endOffset());
    assertNull(docsAndPositionsEnum.getPayload());
    assertEquals(-1, docsAndPositionsEnum.nextPosition());
    assertEquals(4, docsAndPositionsEnum.startOffset());
    assertEquals(7, docsAndPositionsEnum.endOffset());
    assertNull(docsAndPositionsEnum.getPayload());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum.nextDoc());
    // reuse
    docsAndPositionsEnum2 = termsEnum.postings(docsAndPositionsEnum, PostingsEnum.OFFSETS);
    assertEquals(-1, docsAndPositionsEnum2.docID());
    assertEquals(0, docsAndPositionsEnum2.nextDoc());
    assertEquals(2, docsAndPositionsEnum2.freq());
    assertEquals(-1, docsAndPositionsEnum2.nextPosition());
    assertEquals(0, docsAndPositionsEnum2.startOffset());
    assertEquals(3, docsAndPositionsEnum2.endOffset());
    assertNull(docsAndPositionsEnum2.getPayload());
    assertEquals(-1, docsAndPositionsEnum2.nextPosition());
    assertEquals(4, docsAndPositionsEnum2.startOffset());
    assertEquals(7, docsAndPositionsEnum2.endOffset());
    assertNull(docsAndPositionsEnum2.getPayload());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum2.nextDoc());
    
    docsAndPositionsEnum = termsEnum.postings(null, PostingsEnum.ALL);
    assertNotNull(docsAndPositionsEnum);
    assertEquals(-1, docsAndPositionsEnum.docID());
    assertEquals(0, docsAndPositionsEnum.nextDoc());
    assertEquals(2, docsAndPositionsEnum.freq());
    assertEquals(-1, docsAndPositionsEnum.nextPosition());
    assertEquals(0, docsAndPositionsEnum.startOffset());
    assertEquals(3, docsAndPositionsEnum.endOffset());
    assertNull(docsAndPositionsEnum.getPayload());
    assertEquals(-1, docsAndPositionsEnum.nextPosition());
    assertEquals(4, docsAndPositionsEnum.startOffset());
    assertEquals(7, docsAndPositionsEnum.endOffset());
    assertNull(docsAndPositionsEnum.getPayload());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsAndPositionsEnum.nextDoc());
    docsAndPositionsEnum2 = termsEnum.postings(docsAndPositionsEnum, PostingsEnum.ALL);
    assertEquals(-1, docsAndPositionsEnum2.docID());
    assertEquals(0, docsAndPositionsEnum2.nextDoc());
    assertEquals(2, docsAndPositionsEnum2.freq());
    assertEquals(-1, docsAndPositionsEnum2.nextPosition());
    assertEquals(0, docsAndPositionsEnum2.startOffset());
    assertEquals(3, docsAndPositionsEnum2.endOffset());
    assertNull(docsAndPositionsEnum2.getPayload());
    assertEquals(-1, docsAndPositionsEnum2.nextPosition());
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
    FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
    ft.setStoreTermVectors(true);
    ft.setStoreTermVectorPositions(true);
    ft.setStoreTermVectorPayloads(true);
    doc.add(new Field("foo", new CannedTokenStream(token1, token2), ft));
    iw.addDocument(doc);
    DirectoryReader reader = DirectoryReader.open(iw);
    
    Terms terms = getOnlyLeafReader(reader).getTermVector(0, "foo");
    TermsEnum termsEnum = terms.iterator();
    assertNotNull(termsEnum);
    assertEquals(new BytesRef("bar"), termsEnum.next());
    
    // sugar method (FREQS)
    PostingsEnum postings = termsEnum.postings(null);
    assertEquals(-1, postings.docID());
    assertEquals(0, postings.nextDoc());
    assertEquals(2, postings.freq());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, postings.nextDoc());
    
    // termsenum reuse (FREQS)
    PostingsEnum postings2 = termsEnum.postings(postings);
    assertNotNull(postings2);
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
    // and it had better work
    assertEquals(-1, docsOnly2.docID());
    assertEquals(0, docsOnly2.nextDoc());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsOnly2.freq() == 1 || docsOnly2.freq() == 2);
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsOnly2.nextDoc());
    
    // asking for positions, ok
    PostingsEnum docsAndPositionsEnum = termsEnum.postings(null, PostingsEnum.POSITIONS);
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
    docsAndPositionsEnum = termsEnum.postings(null, PostingsEnum.PAYLOADS);
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
    
    docsAndPositionsEnum = termsEnum.postings(null, PostingsEnum.OFFSETS);
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
    
    docsAndPositionsEnum = termsEnum.postings(null, PostingsEnum.ALL);
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
    ft.setStoreTermVectors(true);
    ft.setStoreTermVectorPositions(true);
    ft.setStoreTermVectorPayloads(true);
    ft.setStoreTermVectorOffsets(true);
    doc.add(new Field("foo", new CannedTokenStream(token1, token2), ft));
    iw.addDocument(doc);
    DirectoryReader reader = DirectoryReader.open(iw);
    
    Terms terms = getOnlyLeafReader(reader).getTermVector(0, "foo");
    TermsEnum termsEnum = terms.iterator();
    assertNotNull(termsEnum);
    assertEquals(new BytesRef("bar"), termsEnum.next());
    
    // sugar method (FREQS)
    PostingsEnum postings = termsEnum.postings(null);
    assertEquals(-1, postings.docID());
    assertEquals(0, postings.nextDoc());
    assertEquals(2, postings.freq());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, postings.nextDoc());
    
    // termsenum reuse (FREQS)
    PostingsEnum postings2 = termsEnum.postings(postings);
    assertNotNull(postings2);
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
    // and it had better work
    assertEquals(-1, docsOnly2.docID());
    assertEquals(0, docsOnly2.nextDoc());
    // we don't define what it is, but if its something else, we should look into it?
    assertTrue(docsOnly2.freq() == 1 || docsOnly2.freq() == 2);
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsOnly2.nextDoc());
    
    // asking for positions, ok
    PostingsEnum docsAndPositionsEnum = termsEnum.postings(null, PostingsEnum.POSITIONS);
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
    docsAndPositionsEnum = termsEnum.postings(null, PostingsEnum.PAYLOADS);
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
    
    docsAndPositionsEnum = termsEnum.postings(null, PostingsEnum.OFFSETS);
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
    
    docsAndPositionsEnum = termsEnum.postings(null, PostingsEnum.ALL);
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

}
