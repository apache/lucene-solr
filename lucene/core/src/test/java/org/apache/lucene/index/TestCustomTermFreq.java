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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.TermFrequencyAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;

import static org.apache.lucene.index.PostingsEnum.NO_MORE_DOCS;

public class TestCustomTermFreq extends LuceneTestCase {

  private static final class CannedTermFreqs extends TokenStream {
    private final String[] terms;
    private final int[] termFreqs;
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final TermFrequencyAttribute termFreqAtt = addAttribute(TermFrequencyAttribute.class);
    private int upto;
    
    public CannedTermFreqs(String[] terms, int[] termFreqs) {
      this.terms = terms;
      this.termFreqs = termFreqs;
      assert terms.length == termFreqs.length;
    }

    @Override
    public boolean incrementToken() {
      if (upto == terms.length) {
        return false;
      }

      clearAttributes();

      termAtt.append(terms[upto]);
      termFreqAtt.setTermFrequency(termFreqs[upto]);

      upto++;
      return true;
    }

    @Override
    public void reset() {
      upto = 0;
    }
  }
  
  public void testSingletonTermsOneDoc() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(new MockAnalyzer(random())));

    Document doc = new Document();
    FieldType fieldType = new FieldType(TextField.TYPE_NOT_STORED);
    fieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
    Field field = new Field("field",
                            new CannedTermFreqs(new String[] {"foo", "bar"},
                                                new int[] {42, 128}),
                            fieldType);
    doc.add(field);
    w.addDocument(doc);
    IndexReader r = DirectoryReader.open(w);
    PostingsEnum postings = MultiTerms.getTermPostingsEnum(r, "field", new BytesRef("bar"), (int) PostingsEnum.FREQS);
    assertNotNull(postings);
    assertEquals(0, postings.nextDoc());
    assertEquals(128, postings.freq());
    assertEquals(NO_MORE_DOCS, postings.nextDoc());

    postings = MultiTerms.getTermPostingsEnum(r, "field", new BytesRef("foo"), (int) PostingsEnum.FREQS);
    assertNotNull(postings);
    assertEquals(0, postings.nextDoc());
    assertEquals(42, postings.freq());
    assertEquals(NO_MORE_DOCS, postings.nextDoc());
    
    IOUtils.close(r, w, dir);
  }

  public void testSingletonTermsTwoDocs() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(new MockAnalyzer(random())));

    Document doc = new Document();
    FieldType fieldType = new FieldType(TextField.TYPE_NOT_STORED);
    fieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
    Field field = new Field("field",
                            new CannedTermFreqs(new String[] {"foo", "bar"},
                                                new int[] {42, 128}),
                            fieldType);
    doc.add(field);
    w.addDocument(doc);

    doc = new Document();
    field = new Field("field",
                      new CannedTermFreqs(new String[] {"foo", "bar"},
                                          new int[] {50, 50}),
                      fieldType);
    doc.add(field);
    w.addDocument(doc);
    
    IndexReader r = DirectoryReader.open(w);
    PostingsEnum postings = MultiTerms.getTermPostingsEnum(r, "field", new BytesRef("bar"), (int) PostingsEnum.FREQS);
    assertNotNull(postings);
    assertEquals(0, postings.nextDoc());
    assertEquals(128, postings.freq());
    assertEquals(1, postings.nextDoc());
    assertEquals(50, postings.freq());
    assertEquals(NO_MORE_DOCS, postings.nextDoc());

    postings = MultiTerms.getTermPostingsEnum(r, "field", new BytesRef("foo"), (int) PostingsEnum.FREQS);
    assertNotNull(postings);
    assertEquals(0, postings.nextDoc());
    assertEquals(42, postings.freq());
    assertEquals(1, postings.nextDoc());
    assertEquals(50, postings.freq());
    assertEquals(NO_MORE_DOCS, postings.nextDoc());
    
    IOUtils.close(r, w, dir);
  }

  public void testRepeatTermsOneDoc() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(new MockAnalyzer(random())));

    Document doc = new Document();
    FieldType fieldType = new FieldType(TextField.TYPE_NOT_STORED);
    fieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
    Field field = new Field("field",
                            new CannedTermFreqs(new String[] {"foo", "bar", "foo", "bar"},
                                                new int[] {42, 128, 17, 100}),
                            fieldType);
    doc.add(field);
    w.addDocument(doc);
    IndexReader r = DirectoryReader.open(w);
    PostingsEnum postings = MultiTerms.getTermPostingsEnum(r, "field", new BytesRef("bar"), (int) PostingsEnum.FREQS);
    assertNotNull(postings);
    assertEquals(0, postings.nextDoc());
    assertEquals(228, postings.freq());
    assertEquals(NO_MORE_DOCS, postings.nextDoc());

    postings = MultiTerms.getTermPostingsEnum(r, "field", new BytesRef("foo"), (int) PostingsEnum.FREQS);
    assertNotNull(postings);
    assertEquals(0, postings.nextDoc());
    assertEquals(59, postings.freq());
    assertEquals(NO_MORE_DOCS, postings.nextDoc());
    
    IOUtils.close(r, w, dir);
  }

  public void testRepeatTermsTwoDocs() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(new MockAnalyzer(random())));

    Document doc = new Document();
    FieldType fieldType = new FieldType(TextField.TYPE_NOT_STORED);
    fieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
    Field field = new Field("field",
                            new CannedTermFreqs(new String[] {"foo", "bar", "foo", "bar"},
                                                new int[] {42, 128, 17, 100}),
                            fieldType);
    doc.add(field);
    w.addDocument(doc);

    doc = new Document();
    fieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
    field = new Field("field",
                      new CannedTermFreqs(new String[] {"foo", "bar", "foo", "bar"},
                                          new int[] {50, 60, 70, 80}),
                      fieldType);
    doc.add(field);
    w.addDocument(doc);

    IndexReader r = DirectoryReader.open(w);
    PostingsEnum postings = MultiTerms.getTermPostingsEnum(r, "field", new BytesRef("bar"), (int) PostingsEnum.FREQS);
    assertNotNull(postings);
    assertEquals(0, postings.nextDoc());
    assertEquals(228, postings.freq());
    assertEquals(1, postings.nextDoc());
    assertEquals(140, postings.freq());
    assertEquals(NO_MORE_DOCS, postings.nextDoc());

    postings = MultiTerms.getTermPostingsEnum(r, "field", new BytesRef("foo"), (int) PostingsEnum.FREQS);
    assertNotNull(postings);
    assertEquals(0, postings.nextDoc());
    assertEquals(59, postings.freq());
    assertEquals(1, postings.nextDoc());
    assertEquals(120, postings.freq());
    assertEquals(NO_MORE_DOCS, postings.nextDoc());

    IOUtils.close(r, w, dir);
  }

  public void testTotalTermFreq() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(new MockAnalyzer(random())));

    Document doc = new Document();
    FieldType fieldType = new FieldType(TextField.TYPE_NOT_STORED);
    fieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
    Field field = new Field("field",
                            new CannedTermFreqs(new String[] {"foo", "bar", "foo", "bar"},
                                                new int[] {42, 128, 17, 100}),
                            fieldType);
    doc.add(field);
    w.addDocument(doc);

    doc = new Document();
    fieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
    field = new Field("field",
                      new CannedTermFreqs(new String[] {"foo", "bar", "foo", "bar"},
                                          new int[] {50, 60, 70, 80}),
                      fieldType);
    doc.add(field);
    w.addDocument(doc);

    IndexReader r = DirectoryReader.open(w);

    TermsEnum termsEnum = MultiTerms.getTerms(r, "field").iterator();
    assertTrue(termsEnum.seekExact(new BytesRef("foo")));
    assertEquals(179, termsEnum.totalTermFreq());
    assertTrue(termsEnum.seekExact(new BytesRef("bar")));
    assertEquals(368, termsEnum.totalTermFreq());
    
    IOUtils.close(r, w, dir);
  }

  // you can't index proximity with custom term freqs:
  public void testInvalidProx() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(new MockAnalyzer(random())));

    Document doc = new Document();
    FieldType fieldType = new FieldType(TextField.TYPE_NOT_STORED);
    Field field = new Field("field",
                            new CannedTermFreqs(new String[] {"foo", "bar", "foo", "bar"},
                                                new int[] {42, 128, 17, 100}),
                            fieldType);
    doc.add(field);
    Exception e = expectThrows(IllegalStateException.class, () -> {w.addDocument(doc);});
    assertEquals("field \"field\": cannot index positions while using custom TermFrequencyAttribute", e.getMessage());
    IOUtils.close(w, dir);
  }

  // you can't index DOCS_ONLY with custom term freq
  public void testInvalidDocsOnly() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(new MockAnalyzer(random())));

    Document doc = new Document();
    FieldType fieldType = new FieldType(TextField.TYPE_NOT_STORED);
    fieldType.setIndexOptions(IndexOptions.DOCS);
    Field field = new Field("field",
                            new CannedTermFreqs(new String[] {"foo", "bar", "foo", "bar"},
                                                new int[] {42, 128, 17, 100}),
                            fieldType);
    doc.add(field);
    Exception e = expectThrows(IllegalStateException.class, () -> {w.addDocument(doc);});
    assertEquals("field \"field\": must index term freq while using custom TermFrequencyAttribute", e.getMessage());
    IOUtils.close(w, dir);
  }

  // sum of term freqs must fit in an int
  public void testOverflowInt() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(new MockAnalyzer(random())));

    FieldType fieldType = new FieldType(TextField.TYPE_NOT_STORED);
    fieldType.setIndexOptions(IndexOptions.DOCS);
    
    Document doc = new Document();
    doc.add(new Field("field", "this field should be indexed", fieldType));
    w.addDocument(doc);

    Document doc2 = new Document();
    Field field = new Field("field",
                            new CannedTermFreqs(new String[] {"foo", "bar"},
                                                new int[] {3, Integer.MAX_VALUE}),
                            fieldType);
    doc2.add(field);
    expectThrows(IllegalArgumentException.class, () -> {w.addDocument(doc2);});

    IndexReader r = DirectoryReader.open(w);
    assertEquals(1, r.numDocs());

    IOUtils.close(r, w, dir);
  }

  public void testInvalidTermVectorPositions() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(new MockAnalyzer(random())));

    Document doc = new Document();
    FieldType fieldType = new FieldType(TextField.TYPE_NOT_STORED);
    fieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
    fieldType.setStoreTermVectors(true);
    fieldType.setStoreTermVectorPositions(true);
    Field field = new Field("field",
                            new CannedTermFreqs(new String[] {"foo", "bar", "foo", "bar"},
                                                new int[] {42, 128, 17, 100}),
                            fieldType);
    doc.add(field);
    Exception e = expectThrows(IllegalArgumentException.class, () -> {w.addDocument(doc);});
    assertEquals("field \"field\": cannot index term vector positions while using custom TermFrequencyAttribute", e.getMessage());
    IOUtils.close(w, dir);
  }

  public void testInvalidTermVectorOffsets() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(new MockAnalyzer(random())));

    Document doc = new Document();
    FieldType fieldType = new FieldType(TextField.TYPE_NOT_STORED);
    fieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
    fieldType.setStoreTermVectors(true);
    fieldType.setStoreTermVectorOffsets(true);
    Field field = new Field("field",
                            new CannedTermFreqs(new String[] {"foo", "bar", "foo", "bar"},
                                                new int[] {42, 128, 17, 100}),
                            fieldType);
    doc.add(field);
    Exception e = expectThrows(IllegalArgumentException.class, () -> {w.addDocument(doc);});
    assertEquals("field \"field\": cannot index term vector offsets while using custom TermFrequencyAttribute", e.getMessage());
    IOUtils.close(w, dir);
  }

  public void testTermVectors() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(new MockAnalyzer(random())));

    Document doc = new Document();
    FieldType fieldType = new FieldType(TextField.TYPE_NOT_STORED);
    fieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
    fieldType.setStoreTermVectors(true);
    Field field = new Field("field",
                            new CannedTermFreqs(new String[] {"foo", "bar", "foo", "bar"},
                                                new int[] {42, 128, 17, 100}),
                            fieldType);
    doc.add(field);
    w.addDocument(doc);

    doc = new Document();
    fieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
    field = new Field("field",
                      new CannedTermFreqs(new String[] {"foo", "bar", "foo", "bar"},
                                          new int[] {50, 60, 70, 80}),
                      fieldType);
    doc.add(field);
    w.addDocument(doc);

    IndexReader r = DirectoryReader.open(w);

    Fields fields = r.getTermVectors(0);
    TermsEnum termsEnum = fields.terms("field").iterator();
    assertTrue(termsEnum.seekExact(new BytesRef("bar")));
    assertEquals(228, termsEnum.totalTermFreq());
    PostingsEnum postings = termsEnum.postings(null);
    assertNotNull(postings);
    assertEquals(0, postings.nextDoc());
    assertEquals(228, postings.freq());
    assertEquals(NO_MORE_DOCS, postings.nextDoc());

    assertTrue(termsEnum.seekExact(new BytesRef("foo")));
    assertEquals(59, termsEnum.totalTermFreq());
    postings = termsEnum.postings(null);
    assertNotNull(postings);
    assertEquals(0, postings.nextDoc());
    assertEquals(59, postings.freq());
    assertEquals(NO_MORE_DOCS, postings.nextDoc());

    fields = r.getTermVectors(1);
    termsEnum = fields.terms("field").iterator();
    assertTrue(termsEnum.seekExact(new BytesRef("bar")));
    assertEquals(140, termsEnum.totalTermFreq());
    postings = termsEnum.postings(null);
    assertNotNull(postings);
    assertEquals(0, postings.nextDoc());
    assertEquals(140, postings.freq());
    assertEquals(NO_MORE_DOCS, postings.nextDoc());

    assertTrue(termsEnum.seekExact(new BytesRef("foo")));
    assertEquals(120, termsEnum.totalTermFreq());
    postings = termsEnum.postings(null);
    assertNotNull(postings);
    assertEquals(0, postings.nextDoc());
    assertEquals(120, postings.freq());
    assertEquals(NO_MORE_DOCS, postings.nextDoc());
    
    IOUtils.close(r, w, dir);
  }

  /**
   * Similarity holds onto the FieldInvertState for subsequent verification.
   */
  private static class NeverForgetsSimilarity extends Similarity {
    public FieldInvertState lastState;
    private final static NeverForgetsSimilarity INSTANCE = new NeverForgetsSimilarity();

    private NeverForgetsSimilarity() {
      // no
    }
    
    @Override
    public long computeNorm(FieldInvertState state) {
      this.lastState = state;
      return 1;
    }
    
    @Override
    public SimScorer scorer(float boost, CollectionStatistics collectionStats, TermStatistics... termStats) {
      throw new UnsupportedOperationException();
    }
  }

  public void testFieldInvertState() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setSimilarity(NeverForgetsSimilarity.INSTANCE);
    IndexWriter w = new IndexWriter(dir, iwc);

    Document doc = new Document();
    FieldType fieldType = new FieldType(TextField.TYPE_NOT_STORED);
    fieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
    Field field = new Field("field",
                            new CannedTermFreqs(new String[] {"foo", "bar", "foo", "bar"},
                                                new int[] {42, 128, 17, 100}),
                            fieldType);
    doc.add(field);
    w.addDocument(doc);
    FieldInvertState fis = NeverForgetsSimilarity.INSTANCE.lastState;
    assertEquals(228, fis.getMaxTermFrequency());
    assertEquals(2, fis.getUniqueTermCount());
    assertEquals(0, fis.getNumOverlap());
    assertEquals(287, fis.getLength());

    IOUtils.close(w, dir);
  }
}
