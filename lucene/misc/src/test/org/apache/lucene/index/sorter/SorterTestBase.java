package org.apache.lucene.index.sorter;

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.TermsEnum.SeekStatus;
import org.apache.lucene.index.sorter.SortingAtomicReader.SortingDocsAndPositionsEnum;
import org.apache.lucene.index.sorter.SortingAtomicReader.SortingDocsEnum;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.lucene.util._TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

@SuppressCodecs("Lucene3x")
public abstract class SorterTestBase extends LuceneTestCase {

  static final class NormsSimilarity extends Similarity {
    
    private final Similarity in;
    
    public NormsSimilarity(Similarity in) {
      this.in = in;
    }
    
    @Override
    public long computeNorm(FieldInvertState state) {
      if (state.getName().equals(NORMS_FIELD)) {
        return Float.floatToIntBits(state.getBoost());
      } else {
        return in.computeNorm(state);
      }
    }
    
    @Override
    public SimWeight computeWeight(float queryBoost, CollectionStatistics collectionStats, TermStatistics... termStats) {
      return in.computeWeight(queryBoost, collectionStats, termStats);
    }
    
    @Override
    public ExactSimScorer exactSimScorer(SimWeight weight, AtomicReaderContext context) throws IOException {
      return in.exactSimScorer(weight, context);
    }
    
    @Override
    public SloppySimScorer sloppySimScorer(SimWeight weight, AtomicReaderContext context) throws IOException {
      return in.sloppySimScorer(weight, context);
    }
    
  }
  
  static final class PositionsTokenStream extends TokenStream {
    
    private final CharTermAttribute term;
    private final PayloadAttribute payload;
    private final OffsetAttribute offset;
    
    private int pos, off;
    
    public PositionsTokenStream() {
      term = addAttribute(CharTermAttribute.class);
      term.append(DOC_POSITIONS_TERM);
      payload = addAttribute(PayloadAttribute.class);
      offset = addAttribute(OffsetAttribute.class);
    }
    
    @Override
    public boolean incrementToken() throws IOException {
      if (pos == 0) {
        return false;
      }
      
      payload.setPayload(new BytesRef(Integer.toString(pos)));
      offset.setOffset(off, off);
      --pos;
      ++off;
      return true;
    }
    
    void setId(int id) {
      pos = id / 10 + 1;
      off = 0;
    }
  }
  
  protected static final String ID_FIELD = "id";
  protected static final String DOCS_ENUM_FIELD = "docs";
  protected static final String DOCS_ENUM_TERM = "$all$";
  protected static final String DOC_POSITIONS_FIELD = "positions";
  protected static final String DOC_POSITIONS_TERM = "$all$";
  protected static final String NUMERIC_DV_FIELD = "numeric";
  protected static final String NORMS_FIELD = "norm";
  protected static final String BINARY_DV_FIELD = "binary";
  protected static final String SORTED_DV_FIELD = "sorted";
  protected static final String SORTED_SET_DV_FIELD = "sorted_set";
  protected static final String TERM_VECTORS_FIELD = "term_vectors";

  private static final FieldType TERM_VECTORS_TYPE = new FieldType(TextField.TYPE_NOT_STORED);
  static {
    TERM_VECTORS_TYPE.setStoreTermVectors(true);
    TERM_VECTORS_TYPE.freeze();
  }
  
  private static final FieldType POSITIONS_TYPE = new FieldType(TextField.TYPE_NOT_STORED);
  static {
    POSITIONS_TYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    POSITIONS_TYPE.freeze();
  }
  
  protected static Directory dir;
  protected static AtomicReader reader;
  protected static Integer[] sortedValues;

  private static Document doc(final int id, PositionsTokenStream positions) {
    final Document doc = new Document();
    doc.add(new StringField(ID_FIELD, Integer.toString(id), Store.YES));
    doc.add(new StringField(DOCS_ENUM_FIELD, DOCS_ENUM_TERM, Store.NO));
    positions.setId(id);
    if (doesntSupportOffsets.contains(_TestUtil.getPostingsFormat(DOC_POSITIONS_FIELD))) {
      // codec doesnt support offsets: just index positions for the field
      doc.add(new Field(DOC_POSITIONS_FIELD, positions, TextField.TYPE_NOT_STORED));
    } else {
      doc.add(new Field(DOC_POSITIONS_FIELD, positions, POSITIONS_TYPE));
    }
    doc.add(new NumericDocValuesField(NUMERIC_DV_FIELD, id));
    TextField norms = new TextField(NORMS_FIELD, Integer.toString(id), Store.NO);
    norms.setBoost(Float.intBitsToFloat(id));
    doc.add(norms);
    doc.add(new BinaryDocValuesField(BINARY_DV_FIELD, new BytesRef(Integer.toString(id))));
    doc.add(new SortedDocValuesField(SORTED_DV_FIELD, new BytesRef(Integer.toString(id))));
    if (defaultCodecSupportsSortedSet()) {
      doc.add(new SortedSetDocValuesField(SORTED_SET_DV_FIELD, new BytesRef(Integer.toString(id))));
      doc.add(new SortedSetDocValuesField(SORTED_SET_DV_FIELD, new BytesRef(Integer.toString(id + 1))));
    }
    doc.add(new Field(TERM_VECTORS_FIELD, Integer.toString(id), TERM_VECTORS_TYPE));
    return doc;
  }

  /** Creates an index for sorting. */
  public static void createIndex(Directory dir, int numDocs, Random random) throws IOException {
    List<Integer> ids = new ArrayList<Integer>();
    for (int i = 0; i < numDocs; i++) {
      ids.add(Integer.valueOf(i * 10));
    }
    // shuffle them for indexing
    Collections.shuffle(ids, random);
    if (VERBOSE) {
      System.out.println("Shuffled IDs for indexing: " + Arrays.toString(ids.toArray()));
    }
    
    PositionsTokenStream positions = new PositionsTokenStream();
    IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random));
    conf.setMaxBufferedDocs(4); // create some segments
    conf.setSimilarity(new NormsSimilarity(conf.getSimilarity())); // for testing norms field
    RandomIndexWriter writer = new RandomIndexWriter(random, dir, conf);
    writer.setDoRandomForceMerge(false);
    for (int id : ids) {
      writer.addDocument(doc(id, positions));
    }
    // delete some documents
    writer.commit();
    for (Integer id : ids) {
      if (random.nextDouble() < 0.2) {
        if (VERBOSE) {
          System.out.println("delete doc_id " + id);
        }
        writer.deleteDocuments(new Term(ID_FIELD, id.toString()));
      }
    }
    writer.close();
  }
  
  @BeforeClass
  public static void beforeClassSorterTestBase() throws Exception {
    dir = newDirectory();
    int numDocs = atLeast(20);
    createIndex(dir, numDocs, random());
    
    reader = SlowCompositeReaderWrapper.wrap(DirectoryReader.open(dir));
  }
  
  @AfterClass
  public static void afterClassSorterTestBase() throws Exception {
    reader.close();
    dir.close();
  }
  
  @Test
  public void testBinaryDocValuesField() throws Exception {
    BinaryDocValues dv = reader.getBinaryDocValues(BINARY_DV_FIELD);
    BytesRef bytes = new BytesRef();
    for (int i = 0; i < reader.maxDoc(); i++) {
      dv.get(i, bytes);
      assertEquals("incorrect binary DocValues for doc " + i, sortedValues[i].toString(), bytes.utf8ToString());
    }
  }
  
  @Test
  public void testDocsAndPositionsEnum() throws Exception {
    TermsEnum termsEnum = reader.terms(DOC_POSITIONS_FIELD).iterator(null);
    assertEquals(SeekStatus.FOUND, termsEnum.seekCeil(new BytesRef(DOC_POSITIONS_TERM)));
    DocsAndPositionsEnum sortedPositions = termsEnum.docsAndPositions(null, null);
    int doc;
    
    // test nextDoc()
    while ((doc = sortedPositions.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      int freq = sortedPositions.freq();
      assertEquals("incorrect freq for doc=" + doc, sortedValues[doc].intValue() / 10 + 1, freq);
      for (int i = 0; i < freq; i++) {
        assertEquals("incorrect position for doc=" + doc, i, sortedPositions.nextPosition());
        if (!doesntSupportOffsets.contains(_TestUtil.getPostingsFormat(DOC_POSITIONS_FIELD))) {
          assertEquals("incorrect startOffset for doc=" + doc, i, sortedPositions.startOffset());
          assertEquals("incorrect endOffset for doc=" + doc, i, sortedPositions.endOffset());
        }
        assertEquals("incorrect payload for doc=" + doc, freq - i, Integer.parseInt(sortedPositions.getPayload().utf8ToString()));
      }
    }
    
    // test advance()
    final DocsAndPositionsEnum reuse = sortedPositions;
    sortedPositions = termsEnum.docsAndPositions(null, reuse);
    if (sortedPositions instanceof SortingDocsAndPositionsEnum) {
      assertTrue(((SortingDocsAndPositionsEnum) sortedPositions).reused(reuse)); // make sure reuse worked
    }
    doc = 0;
    while ((doc = sortedPositions.advance(doc + _TestUtil.nextInt(random(), 1, 5))) != DocIdSetIterator.NO_MORE_DOCS) {
      int freq = sortedPositions.freq();
      assertEquals("incorrect freq for doc=" + doc, sortedValues[doc].intValue() / 10 + 1, freq);
      for (int i = 0; i < freq; i++) {
        assertEquals("incorrect position for doc=" + doc, i, sortedPositions.nextPosition());
        if (!doesntSupportOffsets.contains(_TestUtil.getPostingsFormat(DOC_POSITIONS_FIELD))) {
          assertEquals("incorrect startOffset for doc=" + doc, i, sortedPositions.startOffset());
          assertEquals("incorrect endOffset for doc=" + doc, i, sortedPositions.endOffset());
        }
        assertEquals("incorrect payload for doc=" + doc, freq - i, Integer.parseInt(sortedPositions.getPayload().utf8ToString()));
      }
    }
  }

  Bits randomLiveDocs(int maxDoc) {
    if (rarely()) {
      if (random().nextBoolean()) {
        return null;
      } else {
        return new Bits.MatchNoBits(maxDoc);
      }
    }
    final FixedBitSet bits = new FixedBitSet(maxDoc);
    final int bitsSet = _TestUtil.nextInt(random(), 1, maxDoc - 1);
    for (int i = 0; i < bitsSet; ++i) {
      while (true) {
        final int index = random().nextInt(maxDoc);
        if (!bits.get(index)) {
          bits.set(index);
          break;
        }
      }
    }
    return bits;
  }

  @Test
  public void testDocsEnum() throws Exception {
    Bits mappedLiveDocs = randomLiveDocs(reader.maxDoc());
    TermsEnum termsEnum = reader.terms(DOCS_ENUM_FIELD).iterator(null);
    assertEquals(SeekStatus.FOUND, termsEnum.seekCeil(new BytesRef(DOCS_ENUM_TERM)));
    DocsEnum docs = termsEnum.docs(mappedLiveDocs, null);

    int doc;
    int prev = -1;
    while ((doc = docs.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      assertTrue("document " + doc + " marked as deleted", mappedLiveDocs == null || mappedLiveDocs.get(doc));
      assertEquals("incorrect value; doc " + doc, sortedValues[doc].intValue(), Integer.parseInt(reader.document(doc).get(ID_FIELD)));
      while (++prev < doc) {
        assertFalse("document " + prev + " not marked as deleted", mappedLiveDocs == null || mappedLiveDocs.get(prev));
      }
    }
    while (++prev < reader.maxDoc()) {
      assertFalse("document " + prev + " not marked as deleted", mappedLiveDocs == null || mappedLiveDocs.get(prev));
    }

    DocsEnum reuse = docs;
    docs = termsEnum.docs(mappedLiveDocs, reuse);
    if (docs instanceof SortingDocsEnum) {
      assertTrue(((SortingDocsEnum) docs).reused(reuse)); // make sure reuse worked
    }
    doc = -1;
    prev = -1;
    while ((doc = docs.advance(doc + 1)) != DocIdSetIterator.NO_MORE_DOCS) {
      assertTrue("document " + doc + " marked as deleted", mappedLiveDocs == null || mappedLiveDocs.get(doc));
      assertEquals("incorrect value; doc " + doc, sortedValues[doc].intValue(), Integer.parseInt(reader.document(doc).get(ID_FIELD)));
      while (++prev < doc) {
        assertFalse("document " + prev + " not marked as deleted", mappedLiveDocs == null || mappedLiveDocs.get(prev));
      }
    }
    while (++prev < reader.maxDoc()) {
      assertFalse("document " + prev + " not marked as deleted", mappedLiveDocs == null || mappedLiveDocs.get(prev));
    }
  }
  
  @Test
  public void testNormValues() throws Exception {
    NumericDocValues dv = reader.getNormValues(NORMS_FIELD);
    int maxDoc = reader.maxDoc();
    for (int i = 0; i < maxDoc; i++) {
      assertEquals("incorrect norm value for doc " + i, sortedValues[i].intValue(), dv.get(i));
    }
  }
  
  @Test
  public void testNumericDocValuesField() throws Exception {
    NumericDocValues dv = reader.getNumericDocValues(NUMERIC_DV_FIELD);
    int maxDoc = reader.maxDoc();
    for (int i = 0; i < maxDoc; i++) {
      assertEquals("incorrect numeric DocValues for doc " + i, sortedValues[i].intValue(), dv.get(i));
    }
  }
  
  @Test
  public void testSortedDocValuesField() throws Exception {
    SortedDocValues dv = reader.getSortedDocValues(SORTED_DV_FIELD);
    int maxDoc = reader.maxDoc();
    BytesRef bytes = new BytesRef();
    for (int i = 0; i < maxDoc; i++) {
      dv.get(i, bytes);
      assertEquals("incorrect sorted DocValues for doc " + i, sortedValues[i].toString(), bytes.utf8ToString());
    }
  }
  
  @Test
  public void testSortedSetDocValuesField() throws Exception {
    assumeTrue("default codec does not support SORTED_SET", defaultCodecSupportsSortedSet());
    SortedSetDocValues dv = reader.getSortedSetDocValues(SORTED_SET_DV_FIELD);
    int maxDoc = reader.maxDoc();
    BytesRef bytes = new BytesRef();
    for (int i = 0; i < maxDoc; i++) {
      dv.setDocument(i);
      dv.lookupOrd(dv.nextOrd(), bytes);
      int value = sortedValues[i].intValue();
      assertEquals("incorrect sorted-set DocValues for doc " + i, Integer.valueOf(value).toString(), bytes.utf8ToString());
      dv.lookupOrd(dv.nextOrd(), bytes);
      assertEquals("incorrect sorted-set DocValues for doc " + i, Integer.valueOf(value + 1).toString(), bytes.utf8ToString());
      assertEquals(SortedSetDocValues.NO_MORE_ORDS, dv.nextOrd());
    }
  }
  
  @Test
  public void testTermVectors() throws Exception {
    int maxDoc = reader.maxDoc();
    for (int i = 0; i < maxDoc; i++) {
      Terms terms = reader.getTermVector(i, TERM_VECTORS_FIELD);
      assertNotNull("term vectors not found for doc " + i + " field [" + TERM_VECTORS_FIELD + "]", terms);
      assertEquals("incorrect term vector for doc " + i, sortedValues[i].toString(), terms.iterator(null).next().utf8ToString());
    }
  }
  
}
