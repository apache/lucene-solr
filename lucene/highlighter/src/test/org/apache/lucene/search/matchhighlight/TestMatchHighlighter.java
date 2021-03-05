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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.synonym.SynonymGraphFilter;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.intervals.IntervalQuery;
import org.apache.lucene.queries.intervals.Intervals;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.LuceneTestCase;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

public class TestMatchHighlighter extends LuceneTestCase {
  private static final String FLD_ID = "id";
  private static final String FLD_TEXT1 = "text1";
  private static final String FLD_TEXT2 = "text2";

  private FieldType TYPE_TEXT_POSITIONS_OFFSETS;
  private FieldType TYPE_TEXT_POSITIONS;

  private PerFieldAnalyzerWrapper analyzer;

  @Before
  public void setup() throws IOException {
    TYPE_TEXT_POSITIONS = TextField.TYPE_STORED;

    TYPE_TEXT_POSITIONS_OFFSETS = new FieldType(TextField.TYPE_STORED);
    TYPE_TEXT_POSITIONS_OFFSETS.setIndexOptions(
        IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    TYPE_TEXT_POSITIONS_OFFSETS.freeze();

    Map<String, Analyzer> fieldAnalyzers = new HashMap<>();

    // Create an analyzer with some synonyms, just to showcase them.
    SynonymMap synonymMap =
        buildSynonymMap(
            new String[][] {
              {"moon\u0000shine", "firewater"},
              {"firewater", "moon\u0000shine"},
            });

    // Make a non-empty offset gap so that break iterator doesn't go haywire on multivalues
    // glued together.
    final int offsetGap = RandomizedTest.randomIntBetween(1, 2);
    final int positionGap = RandomizedTest.randomFrom(new int[] {0, 1, 100});
    Analyzer synonymsAnalyzer =
        new AnalyzerWithGaps(
            offsetGap,
            positionGap,
            new Analyzer() {
              @Override
              protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer tokenizer = new WhitespaceTokenizer();
                TokenStream tokenStream = new SynonymGraphFilter(tokenizer, synonymMap, true);
                return new TokenStreamComponents(tokenizer, tokenStream);
              }
            });

    fieldAnalyzers.put(FLD_TEXT1, synonymsAnalyzer);
    fieldAnalyzers.put(FLD_TEXT2, synonymsAnalyzer);

    analyzer = new PerFieldAnalyzerWrapper(new MissingAnalyzer(), fieldAnalyzers);
  }

  static SynonymMap buildSynonymMap(String[][] synonyms) throws IOException {
    SynonymMap.Builder builder = new SynonymMap.Builder();
    for (String[] pair : synonyms) {
      assertThat(pair.length, Matchers.equalTo(2));
      builder.add(new CharsRef(pair[0]), new CharsRef(pair[1]), true);
    }
    return builder.build();
  }

  @Test
  public void testBasicUsage() throws IOException {
    new IndexBuilder(this::toField)
        .doc(FLD_TEXT1, "foo bar baz")
        .doc(FLD_TEXT1, "bar foo baz")
        .doc(
            fields -> {
              fields.add(FLD_TEXT1, "Very long content but not matching anything.");
              fields.add(FLD_TEXT2, "no foo but bar");
            })
        .build(
            analyzer,
            reader -> {
              Query query =
                  new BooleanQuery.Builder()
                      .add(new TermQuery(new Term(FLD_TEXT1, "foo")), BooleanClause.Occur.SHOULD)
                      .add(new TermQuery(new Term(FLD_TEXT2, "bar")), BooleanClause.Occur.SHOULD)
                      .build();

              // In the most basic scenario, we run a search against a query, retrieve
              // top docs...
              IndexSearcher searcher = new IndexSearcher(reader);
              Sort sortOrder = Sort.INDEXORDER; // So that results are consistently ordered.
              TopDocs topDocs = searcher.search(query, 10, sortOrder);

              // ...and would want a fixed set of fields from those documents, some of them
              // possibly highlighted if they matched the query.
              //
              // This configures the highlighter so that the FLD_ID field is always returned
              // verbatim,
              // and FLD_TEXT1 is returned *only if it contained a query match*.
              MatchHighlighter highlighter =
                  new MatchHighlighter(searcher, analyzer)
                      .appendFieldHighlighter(FieldValueHighlighters.verbatimValue(FLD_ID))
                      .appendFieldHighlighter(
                          FieldValueHighlighters.highlighted(
                              80 * 3, 1, new PassageFormatter("...", ">", "<"), FLD_TEXT1::equals))
                      .appendFieldHighlighter(FieldValueHighlighters.skipRemaining());

              // Note document field highlights are a stream over documents in topDocs. In the
              // remaining code we will just
              // collect them on the fly into a preformatted string.
              Stream<MatchHighlighter.DocHighlights> highlights =
                  highlighter.highlight(topDocs, query);
              assertHighlights(
                  toDocList(highlights),
                  " 0. id: 0",
                  "    text1: >foo< bar baz",
                  " 1. id: 1",
                  "    text1: bar >foo< baz",
                  " 2. id: 2");

              // In a more realistic use case, you'd want to show the value of a given field
              // *regardless* of whether it
              // contained a highlight or not -- it is odd that document "id: 2" above doesn't have
              // the 'text1' field
              // shown because that field wasn't part of the query match.
              //
              // Let's say the field is also potentially long; if it contains a match,
              // we would want to display the contextual snippet surrounding that match. If it does
              // not contain any
              // matches, we would want to display its content up to a given number of characters
              // (lead lines).
              //
              // Let's do this by adding an appropriate field highlighter on FLD_TEXT1.
              highlighter =
                  new MatchHighlighter(searcher, analyzer)
                      .appendFieldHighlighter(FieldValueHighlighters.verbatimValue(FLD_ID))
                      .appendFieldHighlighter(
                          FieldValueHighlighters.highlighted(
                              80 * 3, 1, new PassageFormatter("...", ">", "<"), FLD_TEXT1::equals))
                      .appendFieldHighlighter(
                          FieldValueHighlighters.maxLeadingCharacters(10, "...", Set.of(FLD_TEXT1)))
                      .appendFieldHighlighter(FieldValueHighlighters.skipRemaining());

              assertHighlights(
                  toDocList(highlighter.highlight(topDocs, query)),
                  " 0. id: 0",
                  "    text1: >foo< bar baz",
                  " 1. id: 1",
                  "    text1: bar >foo< baz",
                  " 2. id: 2",
                  "    text1: Very long...");

              // Field highlighters can apply to multiple fields and be chained for convenience.
              // For example, this defines a combined highlighter over both FLD_TEXT1 and FLD_TEXT2.
              Set<String> fields = Set.of(FLD_TEXT1, FLD_TEXT2);
              MatchHighlighter.FieldValueHighlighter highlightedOrAbbreviated =
                  FieldValueHighlighters.highlighted(
                          80 * 3, 1, new PassageFormatter("...", ">", "<"), fields::contains)
                      .or(FieldValueHighlighters.maxLeadingCharacters(10, "...", fields));

              highlighter =
                  new MatchHighlighter(searcher, analyzer)
                      .appendFieldHighlighter(FieldValueHighlighters.verbatimValue(FLD_ID))
                      .appendFieldHighlighter(highlightedOrAbbreviated)
                      .appendFieldHighlighter(FieldValueHighlighters.skipRemaining());

              assertHighlights(
                  toDocList(highlighter.highlight(topDocs, query)),
                  " 0. id: 0",
                  "    text1: >foo< bar baz",
                  " 1. id: 1",
                  "    text1: bar >foo< baz",
                  " 2. id: 2",
                  "    text1: Very long...",
                  "    text2: no foo but >bar<");
            });
  }

  @Test
  public void testSynonymHighlight() throws IOException {
    // There is nothing special needed to highlight or process complex queries, synonyms, etc.
    // Synonyms defined in the constructor of this class.
    new IndexBuilder(this::toField)
        .doc(FLD_TEXT1, "Where the moon shine falls, firewater flows.")
        .build(
            analyzer,
            reader -> {
              IndexSearcher searcher = new IndexSearcher(reader);
              Sort sortOrder = Sort.INDEXORDER; // So that results are consistently ordered.

              MatchHighlighter highlighter =
                  new MatchHighlighter(searcher, analyzer)
                      .appendFieldHighlighter(
                          FieldValueHighlighters.highlighted(
                              80 * 3, 1, new PassageFormatter("...", ">", "<"), FLD_TEXT1::equals))
                      .appendFieldHighlighter(FieldValueHighlighters.skipRemaining());

              Query query = new TermQuery(new Term(FLD_TEXT1, "firewater"));
              assertHighlights(
                  toDocList(highlighter.highlight(searcher.search(query, 10, sortOrder), query)),
                  "0. text1: Where the >moon shine< falls, >firewater< flows.");

              query = new PhraseQuery(FLD_TEXT1, "moon", "shine");
              assertHighlights(
                  toDocList(highlighter.highlight(searcher.search(query, 10, sortOrder), query)),
                  "0. text1: Where the >moon shine< falls, >firewater< flows.");
            });
  }

  @Test
  public void testCustomFieldHighlightHandling() throws IOException {
    // Match highlighter is a showcase of individual components in this package, suitable
    // to create any kind of field-display designs.
    //
    // In this example we will build a custom field highlighting handler that
    // highlights matches over a multivalued field, shows that field's values if it received
    // no matches and limits the number of values displayed to at most 2 (with an appropriate
    // message).
    new IndexBuilder(this::toField)
        // Just one document, one field, four values.
        .doc(FLD_TEXT1, "foo bar", "bar foo baz", "bar baz foo", "baz baz baz")
        .build(
            analyzer,
            reader -> {
              IndexSearcher searcher = new IndexSearcher(reader);
              Sort sortOrder = Sort.INDEXORDER;

              // Let's start with the simple predefined highlighter so that the field's value shows
              // and is highlighted when it was part of the hit.
              MatchHighlighter.FieldValueHighlighter highlighted =
                  FieldValueHighlighters.highlighted(
                      80 * 3, 2, new PassageFormatter("...", ">", "<"), FLD_TEXT1::equals);
              MatchHighlighter highlighter =
                  new MatchHighlighter(searcher, analyzer)
                      .appendFieldHighlighter(highlighted)
                      .appendFieldHighlighter(FieldValueHighlighters.skipRemaining());

              Query query = new TermQuery(new Term(FLD_TEXT1, "foo"));
              TopDocs topDocs = searcher.search(query, 10, sortOrder);

              // Note the highlighter is configured with at most 2 snippets so the match on the
              // third value ("bar baz foo") is omitted. Ellipsis isn't inserted too because
              // values are displayed in full.
              assertHighlights(
                  toDocList(highlighter.highlight(topDocs, query)),
                  "0. text1: >foo< bar, bar >foo< baz");

              // So the above works fine if the field received a match but omits it otherwise. We
              // can
              // force the display of this field by chaining with verbatim value highlighter:
              highlighter =
                  new MatchHighlighter(searcher, analyzer)
                      .appendFieldHighlighter(
                          highlighted.or(FieldValueHighlighters.verbatimValue(FLD_TEXT1)))
                      .appendFieldHighlighter(FieldValueHighlighters.skipRemaining());

              assertHighlights(
                  toDocList(highlighter.highlight(topDocs, new MatchAllDocsQuery())),
                  "0. text1: foo bar, bar foo baz, bar baz foo, baz baz baz");

              // But this is not exactly what we'd like because we want to limit the display of
              // values to the first two.
              // Let's just write a custom field highlighter handler that does it.
              class AtMostNValuesHighlighter implements MatchHighlighter.FieldValueHighlighter {
                private final String field;
                private final int limit;

                AtMostNValuesHighlighter(String field, int limit) {
                  this.field = field;
                  this.limit = limit;
                }

                @Override
                public boolean isApplicable(String field, boolean hasMatches) {
                  return Objects.equals(field, this.field);
                }

                @Override
                public List<String> format(
                    String field,
                    String[] values,
                    String contiguousValue,
                    List<OffsetRange> valueRanges,
                    List<MatchHighlighter.QueryOffsetRange> matchOffsets) {
                  if (values.length <= limit) {
                    return Arrays.asList(values);
                  } else {
                    List<String> collected =
                        Stream.of(values).limit(limit).collect(Collectors.toList());
                    int remaining = values.length - collected.size();
                    collected.add(String.format(Locale.ROOT, "[%d omitted]", remaining));
                    return collected;
                  }
                }

                @Override
                public Collection<String> alwaysFetchedFields() {
                  return Collections.singleton(field);
                }
              }

              // We can now chain it as usual and contemplate the result.
              highlighter =
                  new MatchHighlighter(searcher, analyzer)
                      .appendFieldHighlighter(
                          highlighted.or(new AtMostNValuesHighlighter(FLD_TEXT1, 2)))
                      .appendFieldHighlighter(FieldValueHighlighters.skipRemaining());

              assertHighlights(
                  toDocList(highlighter.highlight(topDocs, query)),
                  "0. text1: >foo< bar, bar >foo< baz");
              assertHighlights(
                  toDocList(highlighter.highlight(topDocs, new MatchAllDocsQuery())),
                  "0. text1: foo bar, bar foo baz, [2 omitted]");
            });
  }

  @Test
  public void testHighlightMoreQueriesAtOnceShowoff() throws IOException {
    // Match highlighter underlying components are powerful enough to build interesting,
    // if not always super-practical, things. In this case, we would like to highlight
    // a set of matches of *more than one* query over the same set of input documents. This includes
    // highest-scoring passage resolution (from multiple hits) and different highlight markers
    // for each query.
    new IndexBuilder(this::toField)
        .doc(FLD_TEXT1, "foo bar baz")
        .doc(FLD_TEXT1, "foo baz bar")
        .build(
            analyzer,
            reader -> {
              // Let's start with the two queries. The first one will be an unordered
              // query for (foo, baz) with a max gap of 1; let's use intervals for this.
              Query q1 =
                  new IntervalQuery(
                      FLD_TEXT1,
                      Intervals.maxgaps(
                          1, Intervals.unordered(Intervals.term("foo"), Intervals.term("baz"))));

              // The second one will be a simpler term query for "bar".
              Query q2 = new TermQuery(new Term(FLD_TEXT1, "bar"));

              // Let's fetch matching documents by combining the two into a Boolean query.
              Query query =
                  new BooleanQuery.Builder()
                      .add(q1, BooleanClause.Occur.SHOULD)
                      .add(q2, BooleanClause.Occur.SHOULD)
                      .build();

              IndexSearcher searcher = new IndexSearcher(reader);
              Sort sortOrder = Sort.INDEXORDER; // So that results are consistently ordered.
              TopDocs topDocs = searcher.search(query, 10, sortOrder);

              // If we use the "regular" highlighter, the result will be slightly odd: a nested
              // highlight over "bar" within the first match. Also, you can't distinguish which of
              // the sub-queries
              // caused which highlight marker... but if it were HTML then you could give the span
              // some semi-translucent background and layered matches would be visible.
              MatchHighlighter highlighter =
                  new MatchHighlighter(searcher, analyzer)
                      .appendFieldHighlighter(
                          FieldValueHighlighters.highlighted(
                              80 * 3,
                              1,
                              new PassageFormatter("...", "<span>", "</span>"),
                              FLD_TEXT1::equals))
                      .appendFieldHighlighter(FieldValueHighlighters.skipRemaining());

              assertHighlights(
                  toDocList(highlighter.highlight(topDocs, query)),
                  "0. text1: <span>foo <span>bar</span> baz</span>",
                  "1. text1: <span>foo baz</span> <span>bar</span>");

              // To separate highlights for multiple queries we'll pass them separately to the
              // highlighter and differentiate highlight markers upon their application. Let's start
              // with the customized
              // field highlighter first. This utilizes the fact that match ranges passed from
              // MatchHighlighter
              // contain a reference to the original query which brought up the match.
              class SeparateMarkerFieldHighlighter
                  implements MatchHighlighter.FieldValueHighlighter {
                private final String field;
                private final Map<Query, String> queryClassMap;

                SeparateMarkerFieldHighlighter(String field, Map<Query, String> queryClassMap) {
                  this.field = field;
                  this.queryClassMap = queryClassMap;
                }

                @Override
                public boolean isApplicable(String field, boolean hasMatches) {
                  return Objects.equals(field, this.field) && hasMatches;
                }

                @Override
                public List<String> format(
                    String field,
                    String[] values,
                    String contiguousValue,
                    List<OffsetRange> valueRanges,
                    List<MatchHighlighter.QueryOffsetRange> matchOffsets) {
                  PassageSelector passageSelector = new PassageSelector();
                  int maxPassageWindow = 80;
                  int maxPassages = 3;
                  List<Passage> bestPassages =
                      passageSelector.pickBest(
                          contiguousValue,
                          matchOffsets,
                          maxPassageWindow,
                          maxPassages,
                          valueRanges);

                  // We know the offset ranges passed to us by MatchHighlighter are instances of
                  // QueryOffsetRange
                  // so we compute the class based on that.
                  Function<OffsetRange, String> queryToClass =
                      (range) ->
                          queryClassMap.get(((MatchHighlighter.QueryOffsetRange) range).query);

                  PassageFormatter passageFormatter =
                      new PassageFormatter(
                          "...",
                          (range) -> "<span class='" + queryToClass.apply(range) + "'>",
                          (range) -> "</span>");

                  return passageFormatter.format(contiguousValue, bestPassages, valueRanges);
                }
              }

              // And this is pretty much it. We now set up query classes to display, set up the
              // highlighter...
              Map<Query, String> queryClassMap = Map.of(q1, "q1", q2, "q2");
              highlighter =
                  new MatchHighlighter(searcher, analyzer)
                      .appendFieldHighlighter(
                          new SeparateMarkerFieldHighlighter(FLD_TEXT1, queryClassMap))
                      .appendFieldHighlighter(FieldValueHighlighters.skipRemaining());

              // ...and run highlighting. Note the query passed to the highlighter are individual
              // sub-clauses
              // of the Boolean query used to fetch documents.
              assertHighlights(
                  toDocList(highlighter.highlight(topDocs, q1, q2)),
                  "0. text1: <span class='q1'>foo <span class='q2'>bar</span> baz</span>",
                  "1. text1: <span class='q1'>foo baz</span> <span class='q2'>bar</span>");
            });
  }

  private void assertHighlights(List<List<String>> docList, String... expectedFormattedLines) {
    ArrayList<String> actualLines = new ArrayList<>();
    for (int doc = 0; doc < docList.size(); doc++) {
      List<String> fields = docList.get(doc);
      for (int i = 0; i < fields.size(); i++) {
        actualLines.add(
            (i == 0 ? String.format(Locale.ROOT, "%2d. ", doc) : "    ") + fields.get(i));
      }
    }

    if (!Arrays.equals(
        Stream.of(expectedFormattedLines).map(String::trim).toArray(),
        actualLines.stream().map(String::trim).toArray())) {
      throw new AssertionError(
          "Actual hits were:\n"
              + String.join("\n", actualLines)
              + "\n\n but expected them to be:\n"
              + String.join("\n", expectedFormattedLines));
    }
  }

  private List<List<String>> toDocList(Stream<MatchHighlighter.DocHighlights> highlights) {
    return highlights
        .map(
            docHighlights ->
                docHighlights.fields.entrySet().stream()
                    .map(e -> e.getKey() + ": " + String.join(", ", e.getValue()))
                    .collect(Collectors.toList()))
        .collect(Collectors.toList());
  }

  private IndexableField toField(String name, String value) {
    switch (name) {
      case FLD_TEXT1:
        return new Field(name, value, TYPE_TEXT_POSITIONS_OFFSETS);
      case FLD_TEXT2:
        return new Field(name, value, TYPE_TEXT_POSITIONS);
      default:
        throw new AssertionError("Don't know how to handle this field: " + name);
    }
  }
}
