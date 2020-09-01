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
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.LuceneTestCase;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    TYPE_TEXT_POSITIONS_OFFSETS.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    TYPE_TEXT_POSITIONS_OFFSETS.freeze();

    Map<String, Analyzer> fieldAnalyzers = new HashMap<>();

    // Create an analyzer with some synonyms, just to showcase them.
    SynonymMap synonymMap = buildSynonymMap(new String[][]{
        {"moon\u0000shine", "firewater"},
    });

    final int offsetGap = RandomizedTest.randomIntBetween(0, 2);
    final int positionGap = RandomizedTest.randomFrom(new int[]{0, 1, 100});
    Analyzer synonymsAnalyzer =
        new AnalyzerWithGaps(offsetGap, positionGap, new Analyzer() {
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
    SynonymMap synonymMap = builder.build();
    return synonymMap;
  }

  @Test
  public void testBasicScenarios() throws IOException {
    new IndexBuilder(this::toField)
        .doc(FLD_TEXT1, "foo bar baz")
        .doc(FLD_TEXT1, "bar foo baz")
        .doc(FLD_TEXT2, "no  foo but bar")
        .build(analyzer, reader -> {
          Query query = new BooleanQuery.Builder()
              .add(new TermQuery(new Term(FLD_TEXT1, "foo")), BooleanClause.Occur.SHOULD)
              .add(new TermQuery(new Term(FLD_TEXT2, "bar")), BooleanClause.Occur.SHOULD)
              .build();

          // In the most basic scenario, we run a search against a query, retrieve
          // top docs...
          IndexSearcher searcher = new IndexSearcher(reader);
          TopDocs topDocs = searcher.search(query, 10);

          // ...and would want a fixed set of fields from those documents, some of them
          // possibly highlighted if they matched the query.
          //
          // This configures the highlighter so that the FLD_ID fiels is always returned verbatim,
          // and FLD_TEXT1 is returned only if it contained a match highlight.
          MatchHighlighter highlighter =
              new MatchHighlighter(searcher, analyzer)
                .addFieldHighlighter(FieldValueHighlighters.verbatimValue(FLD_ID))
                .addFieldHighlighter(FieldValueHighlighters.highlighted(80 * 3, 3, "...", ">", "<", FLD_TEXT1::equals))
                .addFieldHighlighter(FieldValueHighlighters.skipRemaining());

          Stream<MatchHighlighter.DocHighlights> highlights = highlighter.highlight(topDocs, query);

          System.out.println("Highlights: [" + topDocs.totalHits + "]");
          highlights.forEach(docHighlights -> {
            String out = docHighlights.fields.entrySet().stream().map(e -> e.getKey() + ": " + String.join("|", e.getValue()))
                .collect(Collectors.joining("\n"));
            System.out.println("> " + out);
          });
        });
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
