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
package org.apache.lucene.index.memory;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockPayloadAnalyzer;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.internal.matchers.StringContains.containsString;

public class TestMemoryIndex extends LuceneTestCase {

  private MockAnalyzer analyzer;

  @Before
  public void setup() {
    analyzer = new MockAnalyzer(random());
    analyzer.setEnableChecks(false);    // MemoryIndex can close a TokenStream on init error
  }

  @Test
  public void testFreezeAPI() {

    MemoryIndex mi = new MemoryIndex();
    mi.addField("f1", "some text", analyzer);

    assertThat(mi.search(new MatchAllDocsQuery()), not(is(0.0f)));
    assertThat(mi.search(new TermQuery(new Term("f1", "some"))), not(is(0.0f)));

    // check we can add a new field after searching
    mi.addField("f2", "some more text", analyzer);
    assertThat(mi.search(new TermQuery(new Term("f2", "some"))), not(is(0.0f)));

    // freeze!
    mi.freeze();

    RuntimeException expected = expectThrows(RuntimeException.class, () -> {
      mi.addField("f3", "and yet more", analyzer);
    });
    assertThat(expected.getMessage(), containsString("frozen"));

    expected = expectThrows(RuntimeException.class, () -> {
      mi.setSimilarity(new BM25Similarity(1, 1));
    });
    assertThat(expected.getMessage(), containsString("frozen"));

    assertThat(mi.search(new TermQuery(new Term("f1", "some"))), not(is(0.0f)));

    mi.reset();
    mi.addField("f1", "wibble", analyzer);
    assertThat(mi.search(new TermQuery(new Term("f1", "some"))), is(0.0f));
    assertThat(mi.search(new TermQuery(new Term("f1", "wibble"))), not(is(0.0f)));

    // check we can set the Similarity again
    mi.setSimilarity(new ClassicSimilarity());

  }

  public void testSeekByTermOrd() throws IOException {
    MemoryIndex mi = new MemoryIndex();
    mi.addField("field", "some terms be here", analyzer);
    IndexSearcher searcher = mi.createSearcher();
    LeafReader reader = (LeafReader) searcher.getIndexReader();
    TermsEnum terms = reader.fields().terms("field").iterator();
    terms.seekExact(0);
    assertEquals("be", terms.term().utf8ToString());
    TestUtil.checkReader(reader);
  }
  
  public void testReaderConsistency() throws IOException {
    Analyzer analyzer = new MockPayloadAnalyzer();
    
    // defaults
    MemoryIndex mi = new MemoryIndex();
    mi.addField("field", "some terms be here", analyzer);
    TestUtil.checkReader(mi.createSearcher().getIndexReader());
    
    // all combinations of offsets/payloads options
    mi = new MemoryIndex(true, true);
    mi.addField("field", "some terms be here", analyzer);
    TestUtil.checkReader(mi.createSearcher().getIndexReader());
    
    mi = new MemoryIndex(true, false);
    mi.addField("field", "some terms be here", analyzer);
    TestUtil.checkReader(mi.createSearcher().getIndexReader());
    
    mi = new MemoryIndex(false, true);
    mi.addField("field", "some terms be here", analyzer);
    TestUtil.checkReader(mi.createSearcher().getIndexReader());
    
    mi = new MemoryIndex(false, false);
    mi.addField("field", "some terms be here", analyzer);
    TestUtil.checkReader(mi.createSearcher().getIndexReader());
    
    analyzer.close();
  }

  @Test
  public void testSimilarities() throws IOException {

    MemoryIndex mi = new MemoryIndex();
    mi.addField("f1", "a long text field that contains many many terms", analyzer);

    IndexSearcher searcher = mi.createSearcher();
    LeafReader reader = (LeafReader) searcher.getIndexReader();
    float n1 = reader.getNormValues("f1").get(0);

    // Norms are re-computed when we change the Similarity
    mi.setSimilarity(new ClassicSimilarity() {
      @Override
      public float lengthNorm(FieldInvertState state) {
        return 74;
      }
    });
    float n2 = reader.getNormValues("f1").get(0);

    assertTrue(n1 != n2);
    TestUtil.checkReader(reader);
  }

  @Test
  public void testBuildFromDocument() {

    Document doc = new Document();
    doc.add(new TextField("field1", "some text", Field.Store.NO));
    doc.add(new TextField("field1", "some more text", Field.Store.NO));
    doc.add(new StringField("field2", "untokenized text", Field.Store.NO));

    analyzer.setPositionIncrementGap(100);

    MemoryIndex mi = MemoryIndex.fromDocument(doc, analyzer);

    assertThat(mi.search(new TermQuery(new Term("field1", "text"))), not(0.0f));
    assertThat(mi.search(new TermQuery(new Term("field2", "text"))), is(0.0f));
    assertThat(mi.search(new TermQuery(new Term("field2", "untokenized text"))), not(0.0f));

    assertThat(mi.search(new PhraseQuery("field1", "some", "more", "text")), not(0.0f));
    assertThat(mi.search(new PhraseQuery("field1", "some", "text")), not(0.0f));
    assertThat(mi.search(new PhraseQuery("field1", "text", "some")), is(0.0f));

  }

  public void testDocValues() throws Exception {
    Document doc = new Document();
    doc.add(new NumericDocValuesField("numeric", 29L));
    doc.add(new SortedNumericDocValuesField("sorted_numeric", 33L));
    doc.add(new SortedNumericDocValuesField("sorted_numeric", 32L));
    doc.add(new SortedNumericDocValuesField("sorted_numeric", 32L));
    doc.add(new SortedNumericDocValuesField("sorted_numeric", 31L));
    doc.add(new SortedNumericDocValuesField("sorted_numeric", 30L));
    doc.add(new BinaryDocValuesField("binary", new BytesRef("a")));
    doc.add(new SortedDocValuesField("sorted", new BytesRef("b")));
    doc.add(new SortedSetDocValuesField("sorted_set", new BytesRef("f")));
    doc.add(new SortedSetDocValuesField("sorted_set", new BytesRef("d")));
    doc.add(new SortedSetDocValuesField("sorted_set", new BytesRef("d")));
    doc.add(new SortedSetDocValuesField("sorted_set", new BytesRef("c")));

    MemoryIndex mi = MemoryIndex.fromDocument(doc, analyzer);
    LeafReader leafReader = mi.createSearcher().getIndexReader().leaves().get(0).reader();
    NumericDocValues numericDocValues = leafReader.getNumericDocValues("numeric");
    assertEquals(29L, numericDocValues.get(0));
    SortedNumericDocValues sortedNumericDocValues = leafReader.getSortedNumericDocValues("sorted_numeric");
    sortedNumericDocValues.setDocument(0);
    assertEquals(5, sortedNumericDocValues.count());
    assertEquals(30L, sortedNumericDocValues.valueAt(0));
    assertEquals(31L, sortedNumericDocValues.valueAt(1));
    assertEquals(32L, sortedNumericDocValues.valueAt(2));
    assertEquals(32L, sortedNumericDocValues.valueAt(3));
    assertEquals(33L, sortedNumericDocValues.valueAt(4));
    BinaryDocValues binaryDocValues = leafReader.getBinaryDocValues("binary");
    assertEquals("a", binaryDocValues.get(0).utf8ToString());
    SortedDocValues sortedDocValues = leafReader.getSortedDocValues("sorted");
    assertEquals("b", sortedDocValues.get(0).utf8ToString());
    assertEquals(0, sortedDocValues.getOrd(0));
    assertEquals("b", sortedDocValues.lookupOrd(0).utf8ToString());
    SortedSetDocValues sortedSetDocValues = leafReader.getSortedSetDocValues("sorted_set");
    assertEquals(3, sortedSetDocValues.getValueCount());
    sortedSetDocValues.setDocument(0);
    assertEquals(0L, sortedSetDocValues.nextOrd());
    assertEquals(1L, sortedSetDocValues.nextOrd());
    assertEquals(2L, sortedSetDocValues.nextOrd());
    assertEquals(SortedSetDocValues.NO_MORE_ORDS, sortedSetDocValues.nextOrd());
    assertEquals("c", sortedSetDocValues.lookupOrd(0L).utf8ToString());
    assertEquals("d", sortedSetDocValues.lookupOrd(1L).utf8ToString());
    assertEquals("f", sortedSetDocValues.lookupOrd(2L).utf8ToString());
  }

  public void testInvalidDocValuesUsage() throws Exception {
    Document doc = new Document();
    doc.add(new NumericDocValuesField("field", 29L));
    doc.add(new BinaryDocValuesField("field", new BytesRef("30")));
    try {
      MemoryIndex.fromDocument(doc, analyzer);
    } catch (IllegalArgumentException e) {
      assertEquals("Can't add [BINARY] doc values field [field], because [NUMERIC] doc values field already exists", e.getMessage());
    }

    doc = new Document();
    doc.add(new NumericDocValuesField("field", 29L));
    doc.add(new NumericDocValuesField("field", 30L));
    try {
      MemoryIndex.fromDocument(doc, analyzer);
    } catch (IllegalArgumentException e) {
      assertEquals("Only one value per field allowed for [NUMERIC] doc values field [field]", e.getMessage());
    }

    doc = new Document();
    doc.add(new TextField("field", "a b", Field.Store.NO));
    doc.add(new BinaryDocValuesField("field", new BytesRef("a")));
    doc.add(new BinaryDocValuesField("field", new BytesRef("b")));
    try {
      MemoryIndex.fromDocument(doc, analyzer);
    } catch (IllegalArgumentException e) {
      assertEquals("Only one value per field allowed for [BINARY] doc values field [field]", e.getMessage());
    }

    doc = new Document();
    doc.add(new SortedDocValuesField("field", new BytesRef("a")));
    doc.add(new SortedDocValuesField("field", new BytesRef("b")));
    doc.add(new TextField("field", "a b", Field.Store.NO));
    try {
      MemoryIndex.fromDocument(doc, analyzer);
    } catch (IllegalArgumentException e) {
      assertEquals("Only one value per field allowed for [SORTED] doc values field [field]", e.getMessage());
    }
  }

  public void testDocValuesDoNotAffectBoostPositionsOrOffset() throws Exception {
    Document doc = new Document();
    doc.add(new BinaryDocValuesField("text", new BytesRef("quick brown fox")));
    doc.add(new TextField("text", "quick brown fox", Field.Store.NO));
    MemoryIndex mi = MemoryIndex.fromDocument(doc, analyzer, true, true);
    LeafReader leafReader = mi.createSearcher().getIndexReader().leaves().get(0).reader();
    TermsEnum tenum = leafReader.terms("text").iterator();

    assertEquals("brown", tenum.next().utf8ToString());
    PostingsEnum penum = tenum.postings(null, PostingsEnum.OFFSETS);
    assertEquals(0, penum.nextDoc());
    assertEquals(1, penum.freq());
    assertEquals(1, penum.nextPosition());
    assertEquals(6, penum.startOffset());
    assertEquals(11, penum.endOffset());

    assertEquals("fox", tenum.next().utf8ToString());
    penum = tenum.postings(penum, PostingsEnum.OFFSETS);
    assertEquals(0, penum.nextDoc());
    assertEquals(1, penum.freq());
    assertEquals(2, penum.nextPosition());
    assertEquals(12, penum.startOffset());
    assertEquals(15, penum.endOffset());

    assertEquals("quick", tenum.next().utf8ToString());
    penum = tenum.postings(penum, PostingsEnum.OFFSETS);
    assertEquals(0, penum.nextDoc());
    assertEquals(1, penum.freq());
    assertEquals(0, penum.nextPosition());
    assertEquals(0, penum.startOffset());
    assertEquals(5, penum.endOffset());

    BinaryDocValues binaryDocValues = leafReader.getBinaryDocValues("text");
    assertEquals("quick brown fox", binaryDocValues.get(0).utf8ToString());
  }

}
