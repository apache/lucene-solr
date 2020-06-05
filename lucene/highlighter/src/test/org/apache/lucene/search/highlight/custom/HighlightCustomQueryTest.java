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
package org.apache.lucene.search.highlight.custom;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.highlight.InvalidTokenOffsetsException;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.SimpleFragmenter;
import org.apache.lucene.search.highlight.SimpleHTMLFormatter;
import org.apache.lucene.search.highlight.WeightedSpanTerm;
import org.apache.lucene.search.highlight.WeightedSpanTermExtractor;
import org.apache.lucene.util.LuceneTestCase;

/**
 * Tests the extensibility of {@link WeightedSpanTermExtractor} and
 * {@link QueryScorer} in a user defined package
 */
public class HighlightCustomQueryTest extends LuceneTestCase {

  private static final String FIELD_NAME = "contents";

  public void testHighlightCustomQuery() throws IOException,
      InvalidTokenOffsetsException {
    String s1 = "I call our world Flatland, not because we call it so,";

    // Verify that a query against the default field results in text being
    // highlighted
    // regardless of the field name.

    CustomQuery q = new CustomQuery(new Term(FIELD_NAME, "world"));

    String expected = "I call our <B>world</B> Flatland, not because we call it so,";
    String observed = highlightField(q, "SOME_FIELD_NAME", s1);
    if (VERBOSE)
      System.out.println("Expected: \"" + expected + "\n" + "Observed: \""
          + observed);
    assertEquals(
        "Query in the default field results in text for *ANY* field being highlighted",
        expected, observed);

    // Verify that a query against a named field does not result in any
    // highlighting
    // when the query field name differs from the name of the field being
    // highlighted,
    // which in this example happens to be the default field name.
    q = new CustomQuery(new Term("text", "world"));

    expected = s1;
    observed = highlightField(q, FIELD_NAME, s1);
    if (VERBOSE)
      System.out.println("Expected: \"" + expected + "\n" + "Observed: \""
          + observed);
    assertEquals(
        "Query in a named field does not result in highlighting when that field isn't in the query",
        s1, highlightField(q, FIELD_NAME, s1));

  }

  public void testHighlightKnownQuery() throws IOException {
    WeightedSpanTermExtractor extractor = new WeightedSpanTermExtractor() {
      @Override
      protected void extractUnknownQuery(Query query, Map<String,WeightedSpanTerm> terms) throws IOException {
        terms.put("foo", new WeightedSpanTerm(3, "foo"));
      }
    };
    Map<String,WeightedSpanTerm> terms = extractor.getWeightedSpanTerms(
        new TermQuery(new Term("bar", "quux")), 3, new CannedTokenStream());
    // no foo
    assertEquals(Collections.singleton("quux"), terms.keySet());
  }

  /**
   * This method intended for use with
   * <code>testHighlightingWithDefaultField()</code>
   */
  private String highlightField(Query query, String fieldName,
      String text) throws IOException, InvalidTokenOffsetsException {
    try (MockAnalyzer mockAnalyzer = new MockAnalyzer(random(), MockTokenizer.SIMPLE,true,
        MockTokenFilter.ENGLISH_STOPSET); TokenStream tokenStream = mockAnalyzer.tokenStream(fieldName, text)) {
      // Assuming "<B>", "</B>" used to highlight
      SimpleHTMLFormatter formatter = new SimpleHTMLFormatter();
      MyQueryScorer scorer = new MyQueryScorer(query, fieldName, FIELD_NAME);
      Highlighter highlighter = new Highlighter(formatter, scorer);
      highlighter.setTextFragmenter(new SimpleFragmenter(Integer.MAX_VALUE));

      String rv = highlighter.getBestFragments(tokenStream, text, 1,
          "(FIELD TEXT TRUNCATED)");
      return rv.length() == 0 ? text : rv;
    }
  }

  public static class MyWeightedSpanTermExtractor extends
      WeightedSpanTermExtractor {

    public MyWeightedSpanTermExtractor() {
      super();
    }

    public MyWeightedSpanTermExtractor(String defaultField) {
      super(defaultField);
    }

    @Override
    protected void extractUnknownQuery(Query query,
        Map<String, WeightedSpanTerm> terms) throws IOException {
      float boost = 1f;
      while (query instanceof BoostQuery) {
        BoostQuery bq = (BoostQuery) query;
        boost *= bq.getBoost();
        query = bq.getQuery();
      }
      if (query instanceof CustomQuery) {
        extractWeightedTerms(terms, new TermQuery(((CustomQuery) query).term), boost);
      }
    }

  }

  public static class MyQueryScorer extends QueryScorer {

    public MyQueryScorer(Query query, String field, String defaultField) {
      super(query, field, defaultField);
    }

    @Override
    protected WeightedSpanTermExtractor newTermExtractor(String defaultField) {
      return defaultField == null ? new MyWeightedSpanTermExtractor()
          : new MyWeightedSpanTermExtractor(defaultField);
    }

  }

  public static class CustomQuery extends Query {
    private final Term term;

    public CustomQuery(Term term) {
      this.term = term;
    }

    @Override
    public String toString(String field) {
      return new TermQuery(term).toString(field);
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
      return new TermQuery(term);
    }

    @Override
    public void visit(QueryVisitor visitor) {
      visitor.consumeTerms(this, term);
    }

    @Override
    public int hashCode() {
      return classHash() + Objects.hashCode(term);
    }

    @Override
    public boolean equals(Object other) {
      return sameClassAs(other) &&
             Objects.equals(term, ((CustomQuery) other).term);
    }
  }
}
