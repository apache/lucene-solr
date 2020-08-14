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
package org.apache.lucene.search.matchhighlight;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.synonym.SynonymGraphFilter;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.analysis.util.CharTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.intervals.IntervalQuery;
import org.apache.lucene.queries.intervals.Intervals;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.not;

public class TestMatchRegionRetriever extends LuceneTestCase {
  private static final String FLD_ID = "field_id";

  private static final String FLD_TEXT_POS_OFFS1 = "field_text_offs1";
  private static final String FLD_TEXT_POS_OFFS2 = "field_text_offs2";

  private static final String FLD_TEXT_POS_OFFS = "field_text_offs";
  private static final String FLD_TEXT_POS = "field_text";

  private static final String FLD_TEXT_SYNONYMS_POS_OFFS = "field_text_syns_offs";
  private static final String FLD_TEXT_SYNONYMS_POS = "field_text_syns";

  private static final String FLD_TEXT_NOPOS = "field_text_nopos";

  private static final String FLD_NON_EXISTING = "field_missing";

  private FieldType TYPE_STORED_WITH_OFFSETS;
  private FieldType TYPE_STORED_NO_POSITIONS;

  private Analyzer analyzer;

  @Before
  public void setup() {
    TYPE_STORED_WITH_OFFSETS = new FieldType(TextField.TYPE_STORED);
    TYPE_STORED_WITH_OFFSETS.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    TYPE_STORED_WITH_OFFSETS.freeze();

    TYPE_STORED_NO_POSITIONS = new FieldType(TextField.TYPE_STORED);
    TYPE_STORED_NO_POSITIONS.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
    TYPE_STORED_NO_POSITIONS.freeze();

    Analyzer whitespaceAnalyzer =
        new Analyzer() {
          final int offsetGap = RandomizedTest.randomIntBetween(0, 2);
          final int positionGap = RandomizedTest.randomFrom(new int[]{0, 1, 100});

          @Override
          protected TokenStreamComponents createComponents(String fieldName) {
            WhitespaceTokenizer tokenizer =
                new WhitespaceTokenizer(CharTokenizer.DEFAULT_MAX_WORD_LEN);
            return new TokenStreamComponents(tokenizer);
          }

          @Override
          public int getOffsetGap(String fieldName) {
            return offsetGap;
          }

          @Override
          public int getPositionIncrementGap(String fieldName) {
            return positionGap;
          }
        };

    Map<String, Analyzer> fieldAnalyzers = new HashMap<>();
    fieldAnalyzers.put(FLD_TEXT_POS, whitespaceAnalyzer);
    fieldAnalyzers.put(FLD_TEXT_POS_OFFS, whitespaceAnalyzer);
    fieldAnalyzers.put(FLD_TEXT_POS_OFFS1, whitespaceAnalyzer);
    fieldAnalyzers.put(FLD_TEXT_POS_OFFS2, whitespaceAnalyzer);
    fieldAnalyzers.put(FLD_TEXT_NOPOS, whitespaceAnalyzer);

    try {
      SynonymMap.Builder b = new SynonymMap.Builder();
      b.add(new CharsRef("foo\u0000bar"), new CharsRef("syn1"), true);
      b.add(new CharsRef("baz"), new CharsRef("syn2\u0000syn3"), true);
      SynonymMap synonymMap = b.build();
      Analyzer synonymsAnalyzer =
          new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
              Tokenizer tokenizer = new WhitespaceTokenizer();
              TokenStream tokenStream = new SynonymGraphFilter(tokenizer, synonymMap, true);
              return new TokenStreamComponents(tokenizer, tokenStream);
            }
          };
      fieldAnalyzers.put(FLD_TEXT_SYNONYMS_POS_OFFS, synonymsAnalyzer);
      fieldAnalyzers.put(FLD_TEXT_SYNONYMS_POS, synonymsAnalyzer);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    analyzer = new PerFieldAnalyzerWrapper(new MissingAnalyzer(), fieldAnalyzers);
  }

  BiFunction<String, String, Query> stdQueryParser =
      (query, defField) -> {
        try {
          StandardQueryParser parser = new StandardQueryParser(analyzer);
          parser.setDefaultOperator(StandardQueryConfigHandler.Operator.AND);
          return parser.parse(query, defField);
        } catch (QueryNodeException e) {
          throw new RuntimeException(e);
        }
      };

  @Test
  public void testTermQueryWithOffsets() throws IOException {
    checkTermQuery(FLD_TEXT_POS_OFFS);
  }

  @Test
  public void testTermQueryWithPositions() throws IOException {
    checkTermQuery(FLD_TEXT_POS);
  }

  private void checkTermQuery(String field) throws IOException {
    withReader(
        List.of(
            Map.of(field, values("foo bar baz")),
            Map.of(field, values("bar foo baz")),
            Map.of(field, values("bar baz foo")),
            Map.of(field, values("bar bar bar irrelevant"))),
        reader -> {
          assertThat(highlights(reader, new TermQuery(new Term(field, "foo"))),
              containsInAnyOrder(
                  fmt("0: (%s: '>foo< bar baz')", field),
                  fmt("1: (%s: 'bar >foo< baz')", field),
                  fmt("2: (%s: 'bar baz >foo<')", field)));
        });
  }

  @Test
  public void testBooleanMultifieldQueryWithOffsets() throws IOException {
    checkBooleanMultifieldQuery(FLD_TEXT_POS_OFFS);
  }

  @Test
  public void testBooleanMultifieldQueryWithPositions() throws IOException {
    checkBooleanMultifieldQuery(FLD_TEXT_POS);
  }

  private void checkBooleanMultifieldQuery(String field) throws IOException {
    Query query =
        new BooleanQuery.Builder()
            .add(new PhraseQuery(1, field, "foo", "baz"), BooleanClause.Occur.SHOULD)
            .add(new TermQuery(new Term(FLD_NON_EXISTING, "abc")), BooleanClause.Occur.SHOULD)
            .add(new TermQuery(new Term(field, "xyz")), BooleanClause.Occur.MUST_NOT)
            .build();

    withReader(
        List.of(
            Map.of(field, values("foo bar baz abc")),
            Map.of(field, values("bar foo baz def")),
            Map.of(field, values("bar baz foo xyz"))),
        reader -> {
          assertThat(highlights(reader, query),
              containsInAnyOrder(
                  fmt("0: (%s: '>foo bar baz< abc')", field),
                  fmt("1: (%s: 'bar >foo baz< def')", field)));
        });
  }

  @Test
  public void testVariousQueryTypesWithOffsets() throws IOException {
    checkVariousQueryTypes(FLD_TEXT_POS_OFFS);
  }

  @Test
  public void testVariousQueryTypesWithPositions() throws IOException {
    checkVariousQueryTypes(FLD_TEXT_POS);
  }

  private void checkVariousQueryTypes(String field) throws IOException {
    withReader(
        List.of(
            Map.of(field, values("foo bar baz abc")),
            Map.of(field, values("bar foo baz def")),
            Map.of(field, values("bar baz foo xyz"))),
        reader -> {
          assertThat(highlights(reader, stdQueryParser.apply("foo baz", field)),
              containsInAnyOrder(
                  fmt("0: (%s: '>foo< bar >baz< abc')", field),
                  fmt("1: (%s: 'bar >foo< >baz< def')", field),
                  fmt("2: (%s: 'bar >baz< >foo< xyz')", field)));

          assertThat(highlights(reader, stdQueryParser.apply("foo OR xyz", field)),
              containsInAnyOrder(
                  fmt("0: (%s: '>foo< bar baz abc')", field),
                  fmt("1: (%s: 'bar >foo< baz def')", field),
                  fmt("2: (%s: 'bar baz >foo< >xyz<')", field)));

          assertThat(highlights(reader, stdQueryParser.apply("bas~2", field)),
              containsInAnyOrder(
                  fmt("0: (%s: 'foo >bar< >baz< >abc<')", field),
                  fmt("1: (%s: '>bar< foo >baz< def')", field),
                  fmt("2: (%s: '>bar< >baz< foo xyz')", field)));

          assertThat(highlights(reader, stdQueryParser.apply("\"foo bar\"", field)),
              containsInAnyOrder((fmt("0: (%s: '>foo bar< baz abc')", field))));

          assertThat(highlights(reader, stdQueryParser.apply("\"foo bar\"~3", field)),
              containsInAnyOrder(
                  fmt("0: (%s: '>foo bar< baz abc')", field),
                  fmt("1: (%s: '>bar foo< baz def')", field),
                  fmt("2: (%s: '>bar baz foo< xyz')", field)));

          assertThat(highlights(reader, stdQueryParser.apply("ba*", field)),
              containsInAnyOrder(
                  fmt("0: (%s: 'foo >bar< >baz< abc')", field),
                  fmt("1: (%s: '>bar< foo >baz< def')", field),
                  fmt("2: (%s: '>bar< >baz< foo xyz')", field)));

          assertThat(highlights(reader, stdQueryParser.apply("[bar TO bas]", field)),
              containsInAnyOrder(
                  fmt("0: (%s: 'foo >bar< baz abc')", field),
                  fmt("1: (%s: '>bar< foo baz def')", field),
                  fmt("2: (%s: '>bar< baz foo xyz')", field)));

          // Note how document '2' has 'bar' that isn't highlighted (because this
          // document is excluded in the first clause).
          assertThat(
              highlights(reader, stdQueryParser.apply("([bar TO baz] -xyz) OR baz", field)),
              containsInAnyOrder(
                  fmt("0: (%s: 'foo >bar< >>baz<< abc')", field),
                  fmt("1: (%s: '>bar< foo >>baz<< def')", field),
                  fmt("2: (%s: 'bar >baz< foo xyz')", field)));

          assertThat(highlights(reader, new MatchAllDocsQuery()),
              Matchers.hasSize(0));
        });

    withReader(
        List.of(
            Map.of(field, values("foo baz foo")),
            Map.of(field, values("bas baz foo")),
            Map.of(field, values("bar baz foo xyz"))),
        reader -> {
          assertThat(
              highlights(reader, stdQueryParser.apply("[bar TO baz] -bar", field)),
              containsInAnyOrder(
                  fmt("0: (%s: 'foo >baz< foo')", field), fmt("1: (%s: '>bas< >baz< foo')", field)));
        });
  }

  @Test
  public void testIntervalQueries() throws IOException {
    String field = FLD_TEXT_POS_OFFS;

    withReader(
        List.of(
            Map.of(field, values("foo baz foo")),
            Map.of(field, values("bas baz foo")),
            Map.of(field, values("bar baz foo xyz"))),
        reader -> {
          assertThat(
              highlights(reader, new IntervalQuery(field,
                  Intervals.unordered(
                      Intervals.term("foo"),
                      Intervals.term("bas"),
                      Intervals.term("baz")))),
              containsInAnyOrder(
                  fmt("1: (field_text_offs: '>bas baz foo<')", field)
              ));

          assertThat(
              highlights(reader, new IntervalQuery(field,
                  Intervals.maxgaps(1,
                      Intervals.unordered(
                          Intervals.term("foo"),
                          Intervals.term("bar"))))),
              containsInAnyOrder(
                  fmt("2: (field_text_offs: '>bar baz foo< xyz')", field)
              ));

          assertThat(
              highlights(reader, new IntervalQuery(field,
                  Intervals.containing(
                      Intervals.unordered(
                          Intervals.term("foo"),
                          Intervals.term("bar")),
                      Intervals.term("foo")))),
              containsInAnyOrder(
                  fmt("2: (field_text_offs: '>bar baz foo< xyz')", field)
              ));

          assertThat(
              highlights(reader, new IntervalQuery(field,
                  Intervals.containedBy(
                      Intervals.term("foo"),
                      Intervals.unordered(
                          Intervals.term("foo"),
                          Intervals.term("bar"))))),
              containsInAnyOrder(
                  fmt("2: (field_text_offs: '>bar baz foo< xyz')", field)
              ));

          assertThat(
              highlights(reader, new IntervalQuery(field,
                  Intervals.overlapping(
                      Intervals.unordered(
                          Intervals.term("foo"),
                          Intervals.term("bar")),
                      Intervals.term("foo")))),
              containsInAnyOrder(
                  fmt("2: (field_text_offs: '>bar baz foo< xyz')", field)
              ));
        });
  }

  @Test
  public void testMultivaluedFieldsWithOffsets() throws IOException {
    checkMultivaluedFields(FLD_TEXT_POS_OFFS);
  }

  @Test
  public void testMultivaluedFieldsWithPositions() throws IOException {
    checkMultivaluedFields(FLD_TEXT_POS);
  }

  public void checkMultivaluedFields(String field) throws IOException {
    withReader(
        List.of(
            Map.of(field, values("foo bar", "baz abc", "bad baz")),
            Map.of(field, values("bar foo", "baz def")),
            Map.of(field, values("bar baz", "foo xyz"))),
        reader -> {
          assertThat(highlights(reader, stdQueryParser.apply("baz", field)),
              containsInAnyOrder(
                  fmt("0: (%s: '>baz< abc | bad >baz<')", field),
                  fmt("1: (%s: '>baz< def')", field),
                  fmt("2: (%s: 'bar >baz<')", field)));
        });
  }

  @Test
  public void testMultiFieldHighlights() throws IOException {
    for (String[] fields :
        new String[][]{
            {FLD_TEXT_POS_OFFS1, FLD_TEXT_POS_OFFS2},
            {FLD_TEXT_POS, FLD_TEXT_POS_OFFS2},
            {FLD_TEXT_POS_OFFS1, FLD_TEXT_POS}
        }) {
      String field1 = fields[0];
      String field2 = fields[1];
      withReader(
          List.of(
              Map.of(
                  field1, values("foo bar", "baz abc"),
                  field2, values("foo baz", "loo bar"))),
          reader -> {
            String ordered =
                Stream.of(fmt("(%s: '>baz< abc')", field1), fmt("(%s: 'loo >bar<')", field2))
                    .sorted()
                    .collect(Collectors.joining(""));

            assertThat(
                highlights(
                    reader,
                    stdQueryParser.apply(field1 + ":baz" + " OR " + field2 + ":bar", field1)),
                containsInAnyOrder(fmt("0: %s", ordered)));
          });
    }
  }

  /**
   * Rewritten Boolean queries may omit matches from {@link
   * org.apache.lucene.search.BooleanClause.Occur#SHOULD} clauses. Check that this isn't the case.
   */
  @Test
  public void testNoRewrite() throws IOException {
    String field1 = FLD_TEXT_POS_OFFS1;
    String field2 = FLD_TEXT_POS_OFFS2;
    withReader(
        List.of(
            Map.of(
                field1, values("0100"),
                field2, values("loo bar")),
            Map.of(
                field1, values("0200"),
                field2, values("foo bar"))),
        reader -> {
          String expected = fmt("0: (%s: '>0100<')(%s: 'loo >bar<')", field1, field2);
          assertThat(
              highlights(
                  reader,
                  stdQueryParser.apply(fmt("+%s:01* OR %s:bar", field1, field2), field1)),
              containsInAnyOrder(expected));

          assertThat(
              highlights(
                  reader,
                  stdQueryParser.apply(fmt("+%s:01* AND %s:bar", field1, field2), field1)),
              containsInAnyOrder(expected));
        });
  }

  @Test
  public void testNestedQueryHitsWithOffsets() throws IOException {
    checkNestedQueryHits(FLD_TEXT_POS_OFFS);
  }

  @Test
  public void testNestedQueryHitsWithPositions() throws IOException {
    checkNestedQueryHits(FLD_TEXT_POS);
  }

  public void checkNestedQueryHits(String field) throws IOException {
    withReader(
        List.of(Map.of(field, values("foo bar baz abc"))),
        reader -> {
          assertThat(
              highlights(
                  reader,
                  new BooleanQuery.Builder()
                      .add(new PhraseQuery(1, field, "foo", "baz"), BooleanClause.Occur.SHOULD)
                      .add(new TermQuery(new Term(field, "bar")), BooleanClause.Occur.SHOULD)
                      .build()),
              containsInAnyOrder(fmt("0: (%s: '>foo >bar< baz< abc')", field)));

          assertThat(
              highlights(
                  reader,
                  new BooleanQuery.Builder()
                      .add(new PhraseQuery(1, field, "foo", "baz"), BooleanClause.Occur.SHOULD)
                      .add(new TermQuery(new Term(field, "bar")), BooleanClause.Occur.SHOULD)
                      .add(new TermQuery(new Term(field, "baz")), BooleanClause.Occur.SHOULD)
                      .build()),
              containsInAnyOrder(fmt("0: (%s: '>foo >bar< >baz<< abc')", field)));
        });
  }

  @Test
  public void testGraphQueryWithOffsets() throws Exception {
    checkGraphQuery(FLD_TEXT_SYNONYMS_POS_OFFS);
  }

  @Test
  public void testGraphQueryWithPositions() throws Exception {
    checkGraphQuery(FLD_TEXT_SYNONYMS_POS);
  }

  private void checkGraphQuery(String field) throws IOException {
    withReader(
        List.of(
            Map.of(field, values("foo bar baz")),
            Map.of(field, values("bar foo baz")),
            Map.of(field, values("bar baz foo")),
            Map.of(field, values("bar bar bar irrelevant"))),
        reader -> {
          assertThat(highlights(reader, new TermQuery(new Term(field, "syn1"))),
              containsInAnyOrder(fmt("0: (%s: '>foo bar< baz')", field)));

          // [syn2 syn3] = baz
          // so both these queries highlight baz.
          assertThat(highlights(reader, new TermQuery(new Term(field, "syn3"))),
              containsInAnyOrder(
                  fmt("0: (%s: 'foo bar >baz<')", field),
                  fmt("1: (%s: 'bar foo >baz<')", field),
                  fmt("2: (%s: 'bar >baz< foo')", field)));
          assertThat(
              highlights(reader, stdQueryParser.apply(field + ":\"syn2 syn3\"", field)),
              containsInAnyOrder(
                  fmt("0: (%s: 'foo bar >baz<')", field),
                  fmt("1: (%s: 'bar foo >baz<')", field),
                  fmt("2: (%s: 'bar >baz< foo')", field)));
          assertThat(
              highlights(reader, stdQueryParser.apply(field + ":\"foo syn2 syn3\"", field)),
              containsInAnyOrder(fmt("1: (%s: 'bar >foo baz<')", field)));
        });
  }

  @Test
  public void testSpanQueryWithOffsets() throws Exception {
    checkSpanQueries(FLD_TEXT_POS_OFFS);
  }

  @Test
  public void testSpanQueryWithPositions() throws Exception {
    checkSpanQueries(FLD_TEXT_POS);
  }

  private void checkSpanQueries(String field) throws IOException {
    withReader(
        List.of(
            Map.of(field, values("foo bar baz")),
            Map.of(field, values("bar foo baz")),
            Map.of(field, values("bar baz foo")),
            Map.of(field, values("bar bar bar irrelevant"))),
        reader -> {
          assertThat(
              highlights(
                  reader,
                  SpanNearQuery.newOrderedNearQuery(field)
                      .addClause(new SpanTermQuery(new Term(field, "bar")))
                      .addClause(new SpanTermQuery(new Term(field, "foo")))
                      .build()),
              containsInAnyOrder(fmt("1: (%s: '>bar foo< baz')", field)));

          assertThat(
              highlights(
                  reader,
                  SpanNearQuery.newOrderedNearQuery(field)
                      .addClause(new SpanTermQuery(new Term(field, "bar")))
                      .addGap(1)
                      .addClause(new SpanTermQuery(new Term(field, "foo")))
                      .build()),
              containsInAnyOrder(fmt("2: (%s: '>bar baz foo<')", field)));

          assertThat(
              highlights(
                  reader,
                  SpanNearQuery.newUnorderedNearQuery(field)
                      .addClause(new SpanTermQuery(new Term(field, "foo")))
                      .addClause(new SpanTermQuery(new Term(field, "bar")))
                      .build()),
              containsInAnyOrder(
                  fmt("0: (%s: '>foo bar< baz')", field), fmt("1: (%s: '>bar foo< baz')", field)));

          assertThat(
              highlights(
                  reader,
                  SpanNearQuery.newUnorderedNearQuery(field)
                      .addClause(new SpanTermQuery(new Term(field, "foo")))
                      .addClause(new SpanTermQuery(new Term(field, "bar")))
                      .setSlop(1)
                      .build()),
              containsInAnyOrder(
                  fmt("0: (%s: '>foo bar< baz')", field),
                  fmt("1: (%s: '>bar foo< baz')", field),
                  fmt("2: (%s: '>bar baz foo<')", field)));
        });
  }

  /**
   * This test runs a term query against a field with no stored
   * positions or offsets. This test checks the {@link OffsetsFromValues}
   * strategy that returns highlights over entire indexed values.
   */
  @Test
  public void testTextFieldNoPositionsOffsetFromValues() throws Exception {
    String field = FLD_TEXT_NOPOS;

    withReader(
        List.of(
            Map.of(FLD_TEXT_NOPOS, values("foo bar")),
            Map.of(FLD_TEXT_NOPOS, values("foo bar", "baz baz"))
        ),
        reader -> {
          OffsetsRetrievalStrategySupplier defaults = MatchRegionRetriever
              .computeOffsetRetrievalStrategies(reader, analyzer);
          OffsetsRetrievalStrategySupplier customSuppliers = (fld) -> {
            if (fld.equals(field)) {
              return new OffsetsFromValues(field, analyzer);
            } else {
              return defaults.apply(field);
            }
          };

          assertThat(
              highlights(
                  customSuppliers,
                  reader,
                  new TermQuery(new Term(field, "bar"))),
              containsInAnyOrder(
                  fmt("0: (%s: '>foo bar<')", field),
                  fmt("1: (%s: '>foo bar< | >baz baz<')", field)));
        });
  }

  /**
   * This test runs a term query against a field with no stored
   * positions or offsets.
   * <p>
   * Such field structure is often useful for multivalued "keyword-like"
   * fields.
   */
  @Test
  public void testTextFieldNoPositionsOffsetsFromTokens() throws Exception {
    String field = FLD_TEXT_NOPOS;

    withReader(
        List.of(
            Map.of(FLD_TEXT_NOPOS, values("foo bar"),
                   FLD_TEXT_POS, values("bar bar")),
            Map.of(FLD_TEXT_NOPOS, values("foo bar", "baz bar"))
        ),
        reader -> {
          assertThat(
              highlights(
                  reader,
                  new TermQuery(new Term(field, "bar"))),
              containsInAnyOrder(
                  fmt("0: (%s: 'foo >bar<')", field),
                  fmt("1: (%s: 'foo >bar< | baz >bar<')", field)));
        });
  }

  private List<String> highlights(IndexReader reader, Query query) throws IOException {
    return highlights(MatchRegionRetriever.computeOffsetRetrievalStrategies(reader, analyzer),
        reader, query);
  }

  private List<String> highlights(OffsetsRetrievalStrategySupplier offsetsStrategySupplier,
                                  IndexReader reader, Query query) throws IOException {
    IndexSearcher searcher = new IndexSearcher(reader);
    int maxDocs = 1000;

    Query rewrittenQuery = searcher.rewrite(query);
    TopDocs topDocs = searcher.search(rewrittenQuery, maxDocs);

    ArrayList<String> highlights = new ArrayList<>();

    AsciiMatchRangeHighlighter formatter = new AsciiMatchRangeHighlighter(analyzer);

    MatchRegionRetriever.MatchOffsetsConsumer highlightCollector =
        (docId, leafReader, leafDocId, fieldHighlights) -> {
          StringBuilder sb = new StringBuilder();

          Document document = leafReader.document(leafDocId);
          formatter
              .apply(document, new TreeMap<>(fieldHighlights))
              .forEach(
                  (field, snippets) -> {
                    sb.append(
                        String.format(
                            Locale.ROOT, "(%s: '%s')", field, String.join(" | ", snippets)));
                  });

          if (sb.length() > 0) {
            sb.insert(0, document.get(FLD_ID) + ": ");
            highlights.add(sb.toString());
          }
        };

    MatchRegionRetriever highlighter = new MatchRegionRetriever(searcher, rewrittenQuery, analyzer,
        offsetsStrategySupplier);
    highlighter.highlightDocuments(topDocs, highlightCollector);

    return highlights;
  }

  private String[] values(String... values) {
    assertThat(values, not(emptyArray()));
    return values;
  }

  private void withReader(
      Collection<Map<String, String[]>> docs, IOUtils.IOConsumer<DirectoryReader> block)
      throws IOException {
    IndexWriterConfig config = new IndexWriterConfig(analyzer);

    try (Directory directory = new ByteBuffersDirectory()) {
      IndexWriter iw = new IndexWriter(directory, config);

      int seq = 0;
      for (Map<String, String[]> fields : docs) {
        Document doc = new Document();
        doc.add(new StringField(FLD_ID, Integer.toString(seq++), Field.Store.YES));
        for (Map.Entry<String, String[]> field : fields.entrySet()) {
          for (String value : field.getValue()) {
            doc.add(toField(field.getKey(), value));
          }
        }
        iw.addDocument(doc);
        if (RandomizedTest.randomBoolean()) {
          iw.commit();
        }
      }
      iw.flush();

      try (DirectoryReader reader = DirectoryReader.open(iw)) {
        block.accept(reader);
      }
    }
  }

  private IndexableField toField(String name, String value) {
    switch (name) {
      case FLD_TEXT_NOPOS:
        return new Field(name, value, TYPE_STORED_NO_POSITIONS);
      case FLD_TEXT_POS:
      case FLD_TEXT_SYNONYMS_POS:
        return new TextField(name, value, Field.Store.YES);
      case FLD_TEXT_POS_OFFS:
      case FLD_TEXT_POS_OFFS1:
      case FLD_TEXT_POS_OFFS2:
      case FLD_TEXT_SYNONYMS_POS_OFFS:
        return new Field(name, value, TYPE_STORED_WITH_OFFSETS);
      default:
        throw new AssertionError("Don't know how to handle this field: " + name);
    }
  }

  private static String fmt(String string, Object... args) {
    return String.format(Locale.ROOT, string, args);
  }
}
