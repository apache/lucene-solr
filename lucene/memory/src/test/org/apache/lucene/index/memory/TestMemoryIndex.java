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
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.LongStream;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockPayloadAnalyzer;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.BinaryPoint;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
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

  public void testFieldsOnlyReturnsIndexedFields() throws IOException {
    Document doc = new Document();

    doc.add(new NumericDocValuesField("numeric", 29L));
    doc.add(new TextField("text", "some text", Field.Store.NO));

    MemoryIndex mi = MemoryIndex.fromDocument(doc, analyzer);
    IndexSearcher searcher = mi.createSearcher();
    IndexReader reader = searcher.getIndexReader();

    assertEquals(reader.getTermVectors(0).size(), 1);
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
  public void testOmitNorms() throws IOException {
    MemoryIndex mi = new MemoryIndex();
    FieldType ft = new FieldType();
    ft.setTokenized(true);
    ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
    ft.setOmitNorms(true);
    mi.addField(new Field("f1", "some text in here", ft), analyzer);
    mi.freeze();

    LeafReader leader = (LeafReader) mi.createSearcher().getIndexReader();
    NumericDocValues norms = leader.getNormValues("f1");
    assertNull(norms);
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
      assertEquals("cannot change DocValues type from NUMERIC to BINARY for field \"field\"", e.getMessage());
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

  public void testPointValues() throws Exception {
    List<Function<Long, IndexableField>> fieldFunctions = Arrays.asList(
        (t) -> new IntPoint("number", t.intValue()),
        (t) -> new LongPoint("number", t),
        (t) -> new FloatPoint("number", t.floatValue()),
        (t) -> new DoublePoint("number", t.doubleValue())
    );
    List<Function<Long, Query>> exactQueryFunctions = Arrays.asList(
        (t) -> IntPoint.newExactQuery("number", t.intValue()),
        (t) -> LongPoint.newExactQuery("number", t),
        (t) -> FloatPoint.newExactQuery("number", t.floatValue()),
        (t) -> DoublePoint.newExactQuery("number", t.doubleValue())
    );
    List<Function<long[], Query>> setQueryFunctions = Arrays.asList(
        (t) -> IntPoint.newSetQuery("number", LongStream.of(t).mapToInt(value -> (int) value).toArray()),
        (t) -> LongPoint.newSetQuery("number", t),
        (t) -> FloatPoint.newSetQuery("number", Arrays.asList(LongStream.of(t).mapToObj(value -> (float) value).toArray(Float[]::new))),
        (t) -> DoublePoint.newSetQuery("number", LongStream.of(t).mapToDouble(value -> (double) value).toArray())
    );
    List<BiFunction<Long, Long, Query>> rangeQueryFunctions = Arrays.asList(
        (t, u) -> IntPoint.newRangeQuery("number", t.intValue(), u.intValue()),
        (t, u) -> LongPoint.newRangeQuery("number", t, u),
        (t, u) -> FloatPoint.newRangeQuery("number", t.floatValue(), u.floatValue()),
        (t, u) -> DoublePoint.newRangeQuery("number", t.doubleValue(), u.doubleValue())
    );

    for (int i = 0; i < fieldFunctions.size(); i++) {
      Function<Long, IndexableField> fieldFunction = fieldFunctions.get(i);
      Function<Long, Query> exactQueryFunction = exactQueryFunctions.get(i);
      Function<long[], Query> setQueryFunction = setQueryFunctions.get(i);
      BiFunction<Long, Long, Query> rangeQueryFunction = rangeQueryFunctions.get(i);

      Document doc = new Document();
      for (int number = 1; number < 32; number += 2) {
        doc.add(fieldFunction.apply((long) number));
      }
      MemoryIndex mi = MemoryIndex.fromDocument(doc, analyzer);
      IndexSearcher indexSearcher = mi.createSearcher();
      Query query = exactQueryFunction.apply(5L);
      assertEquals(1, indexSearcher.count(query));
      query = exactQueryFunction.apply(4L);
      assertEquals(0, indexSearcher.count(query));


      query = setQueryFunction.apply(new long[]{3L, 9L, 19L});
      assertEquals(1, indexSearcher.count(query));
      query = setQueryFunction.apply(new long[]{2L, 8L, 13L});
      assertEquals(1, indexSearcher.count(query));
      query = setQueryFunction.apply(new long[]{2L, 8L, 16L});
      assertEquals(0, indexSearcher.count(query));

      query = rangeQueryFunction.apply(2L, 16L);
      assertEquals(1, indexSearcher.count(query));
      query = rangeQueryFunction.apply(24L, 48L);
      assertEquals(1, indexSearcher.count(query));
      query = rangeQueryFunction.apply(48L, 68L);
      assertEquals(0, indexSearcher.count(query));
    }
  }

  public void testPointValuesDoNotAffectBoostPositionsOrOffset() throws Exception {
    MemoryIndex mi = new MemoryIndex(true, true);
    mi.addField(new TextField("text", "quick brown fox", Field.Store.NO), analyzer, 5f);
    mi.addField(new BinaryPoint("text", "quick".getBytes(StandardCharsets.UTF_8)), analyzer, 5f);
    mi.addField(new BinaryPoint("text", "brown".getBytes(StandardCharsets.UTF_8)), analyzer, 5f);
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

    IndexSearcher indexSearcher = mi.createSearcher();
    assertEquals(1, indexSearcher.count(BinaryPoint.newExactQuery("text", "quick".getBytes(StandardCharsets.UTF_8))));
    assertEquals(1, indexSearcher.count(BinaryPoint.newExactQuery("text", "brown".getBytes(StandardCharsets.UTF_8))));
    assertEquals(0, indexSearcher.count(BinaryPoint.newExactQuery("text", "jumps".getBytes(StandardCharsets.UTF_8))));
  }

  public void test2DPoints() throws Exception {
    Document doc = new Document();
    doc.add(new IntPoint("ints", 0, -100));
    doc.add(new IntPoint("ints", 20, 20));
    doc.add(new IntPoint("ints", 100, -100));
    doc.add(new LongPoint("longs", 0L, -100L));
    doc.add(new LongPoint("longs", 20L, 20L));
    doc.add(new LongPoint("longs", 100L, -100L));
    doc.add(new FloatPoint("floats", 0F, -100F));
    doc.add(new FloatPoint("floats", 20F, 20F));
    doc.add(new FloatPoint("floats", 100F, -100F));
    doc.add(new DoublePoint("doubles", 0D, -100D));
    doc.add(new DoublePoint("doubles", 20D, 20D));
    doc.add(new DoublePoint("doubles", 100D, -100D));

    MemoryIndex mi = MemoryIndex.fromDocument(doc, analyzer);
    IndexSearcher s = mi.createSearcher();

    assertEquals(1, s.count(IntPoint.newRangeQuery("ints", new int[] {10, 10}, new int[] {30, 30})));
    assertEquals(1, s.count(LongPoint.newRangeQuery("longs", new long[] {10L, 10L}, new long[] {30L, 30L})));
    assertEquals(1, s.count(FloatPoint.newRangeQuery("floats", new float[] {10F, 10F}, new float[] {30F, 30F})));
    assertEquals(1, s.count(DoublePoint.newRangeQuery("doubles", new double[] {10D, 10D}, new double[] {30D, 30D})));
  }

  public void testIndexingPointsAndDocValues() throws Exception {
    FieldType type = new FieldType();
    type.setDimensions(1, 4);
    type.setDocValuesType(DocValuesType.BINARY);
    type.freeze();
    Document doc = new Document();
    byte[] packedPoint = "term".getBytes(StandardCharsets.UTF_8);
    doc.add(new BinaryPoint("field", packedPoint, type));
    MemoryIndex mi = MemoryIndex.fromDocument(doc, analyzer);
    LeafReader leafReader = mi.createSearcher().getIndexReader().leaves().get(0).reader();

    assertEquals(1, leafReader.getPointValues().size("field"));
    assertArrayEquals(packedPoint, leafReader.getPointValues().getMinPackedValue("field"));
    assertArrayEquals(packedPoint, leafReader.getPointValues().getMaxPackedValue("field"));

    assertEquals("term", leafReader.getBinaryDocValues("field").get(0).utf8ToString());
  }

  public void testToStringDebug() {
    MemoryIndex mi = new MemoryIndex(true, true);
    Analyzer analyzer = new MockPayloadAnalyzer();

    mi.addField("analyzedField", "aa bb aa", analyzer);

    FieldType type = new FieldType();
    type.setDimensions(1, 4);
    type.setDocValuesType(DocValuesType.BINARY);
    type.freeze();
    mi.addField(new BinaryPoint("pointAndDvField", "term".getBytes(StandardCharsets.UTF_8), type), analyzer);

    assertEquals("analyzedField:\n" +
        "\t'[61 61]':2: [(0, 0, 2, [70 6f 73 3a 20 30]), (1, 6, 8, [70 6f 73 3a 20 32])]\n" +
        "\t'[62 62]':1: [(1, 3, 5, [70 6f 73 3a 20 31])]\n" +
        "\tterms=2, positions=3\n" +
        "pointAndDvField:\n" +
        "\tterms=0, positions=0\n" +
        "\n" +
        "fields=2, terms=2, positions=3", mi.toStringDebug());
  }

}
