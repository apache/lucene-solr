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

package org.apache.lucene.luwak.matchers;

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.luwak.DocumentBatch;
import org.apache.lucene.luwak.InputDocument;
import org.apache.lucene.luwak.Matches;
import org.apache.lucene.luwak.Monitor;
import org.apache.lucene.luwak.MonitorQuery;
import org.apache.lucene.luwak.MonitorQueryParser;
import org.apache.lucene.luwak.UpdateException;
import org.apache.lucene.luwak.presearcher.MatchAllPresearcher;
import org.apache.lucene.luwak.queryparsers.LuceneQueryParser;
import org.apache.lucene.queryparser.complexPhrase.ComplexPhraseQueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.util.LuceneTestCase;

import static org.hamcrest.CoreMatchers.containsString;


public class TestHighlightingMatcher extends LuceneTestCase {

  private static final String textfield = "textfield";

  private static final Analyzer WHITESPACE = new WhitespaceAnalyzer();

  public static InputDocument buildDoc(String id, String text) {
    return InputDocument.builder(id)
        .addField(textfield, text, WHITESPACE)
        .build();
  }

  private static Monitor newMonitor() throws IOException {
    return new Monitor(new LuceneQueryParser(textfield), MatchAllPresearcher.INSTANCE);
  }

  public void testSingleTermQueryMatchesSingleDocument() throws IOException, UpdateException {

    try (Monitor monitor = newMonitor()) {
      MonitorQuery mq = new MonitorQuery("query1", "test");
      monitor.update(mq);

      Matches<HighlightsMatch> matches = monitor.match(buildDoc("doc1", "this is a test document"),
          HighlightingMatcher.FACTORY);
      assertEquals(1, matches.getMatchCount("doc1"));
      HighlightsMatch match = matches.matches("query1", "doc1");
      assertTrue(match.getHits(textfield).contains(new HighlightsMatch.Hit(3, 10, 3, 14)));
    }
  }

  public void testSinglePhraseQueryMatchesSingleDocument() throws IOException, UpdateException {

    try (Monitor monitor = newMonitor()) {
      MonitorQuery mq = new MonitorQuery("query1", "\"test document\"");
      monitor.update(mq);

      Matches<HighlightsMatch> matches = monitor.match(buildDoc("doc1", "this is a test document"),
          HighlightingMatcher.FACTORY);
      assertEquals(1, matches.getMatchCount("doc1"));
      HighlightsMatch m = matches.matches("query1", "doc1");
      assertTrue(m.getHits(textfield).contains(new HighlightsMatch.Hit(3, 10, 4, 23)));
    }

  }

  public void testToString() {

    HighlightsMatch match = new HighlightsMatch("1", "1");
    match.addHit("field", 2, 3, -1, -1);
    match.addHit("field", 0, 1, -1, -1);
    match.addHit("afield", 0, 1, 0, 4);

    assertEquals("Match(doc=1,query=1){hits={afield=[0(0)->1(4)], field=[0(-1)->1(-1), 2(-1)->3(-1)]}}", match.toString());
  }

  public void testMultiFieldQueryMatches() throws IOException, UpdateException {

    InputDocument doc = InputDocument.builder("doc1")
        .addField("field1", "this is a test of field one", WHITESPACE)
        .addField("field2", "and this is an additional test", WHITESPACE)
        .build();

    try (Monitor monitor = newMonitor()) {
      monitor.update(new MonitorQuery("query1", "field1:test field2:test"));

      Matches<HighlightsMatch> matches = monitor.match(doc, HighlightingMatcher.FACTORY);
      assertEquals(1, matches.getMatchCount("doc1"));

      HighlightsMatch m = matches.matches("query1", "doc1");
      assertNotNull(m);
      assertTrue(m.getFields().contains("field1"));
      assertTrue(m.getHits("field1").contains(new HighlightsMatch.Hit(3, 10, 3, 14)));
      assertTrue(m.getHits("field2").contains(new HighlightsMatch.Hit(5, 26, 5, 30)));
    }

  }

  public void testQueryErrors() throws IOException, UpdateException {

    try (Monitor monitor = new Monitor((queryString, metadata) -> {
      if (queryString.equals("error!")) {
        return new Query() {
          @Override
          public String toString(String field) {
            return "";
          }

          @Override
          public Query rewrite(IndexReader reader) {
            throw new RuntimeException("Oops!");
          }

          @Override
          public void visit(QueryVisitor visitor) {

          }

          @Override
          public boolean equals(Object o) {
            return false;
          }

          @Override
          public int hashCode() {
            return 0;
          }
        };
      }
      return new LuceneQueryParser(textfield).parse(queryString, metadata);
    }, MatchAllPresearcher.INSTANCE)) {

      monitor.update(new MonitorQuery("1", "test"),
          new MonitorQuery("2", "error!"),
          new MonitorQuery("3", "document"),
          new MonitorQuery("4", "foo"));

      Matches<HighlightsMatch> matches = monitor.match(buildDoc("doc1", "this is a test document"), HighlightingMatcher.FACTORY);
      assertEquals(4, matches.getQueriesRun());
      assertEquals(2, matches.getMatchCount("doc1"));
      assertEquals(1, matches.getErrors().size());
    }
  }

  public void testWildcards() throws IOException, UpdateException {

    try (Monitor monitor = new Monitor(
        (queryString, metadata) -> new RegexpQuery(new Term(textfield, "he.*")), MatchAllPresearcher.INSTANCE)) {

      monitor.update(new MonitorQuery("1", ""));

      Matches<HighlightsMatch> matches = monitor.match(buildDoc("1", "hello world"), HighlightingMatcher.FACTORY);
      assertEquals(1, matches.getQueriesRun());
      assertEquals(1, matches.getMatchCount("1"));
      assertEquals(1, matches.matches("1", "1").getHitCount());
    }
  }

  public void testWildcardCombinations() throws Exception {

    final BooleanQuery bq = new BooleanQuery.Builder()
        .add(new TermQuery(new Term(textfield, "term1")), BooleanClause.Occur.MUST)
        .add(new PrefixQuery(new Term(textfield, "term2")), BooleanClause.Occur.MUST)
        .add(new TermQuery(new Term(textfield, "term3")), BooleanClause.Occur.MUST_NOT)
        .build();

    try (Monitor monitor = new Monitor((queryString, metadata) -> bq, MatchAllPresearcher.INSTANCE)) {
      monitor.update(new MonitorQuery("1", ""));

      Matches<HighlightsMatch> matches = monitor.match(buildDoc("1", "term1 term22 term4"), HighlightingMatcher.FACTORY);
      HighlightsMatch m = matches.matches("1", "1");
      assertNotNull(m);
      assertEquals(2, m.getHitCount());
    }

  }

  public void testDisjunctionMaxQuery() throws IOException, UpdateException {
    final DisjunctionMaxQuery query = new DisjunctionMaxQuery(Arrays.asList(
        new TermQuery(new Term(textfield, "term1")), new PrefixQuery(new Term(textfield, "term2"))
    ), 1.0f);

    try (Monitor monitor = new Monitor((queryString, metadata) -> query, MatchAllPresearcher.INSTANCE)) {
      monitor.update(new MonitorQuery("1", ""));
      Matches<HighlightsMatch> matches = monitor.match(buildDoc("1", "term1 term2 term3"), HighlightingMatcher.FACTORY);
      HighlightsMatch m = matches.matches("1", "1");
      assertNotNull(m);
      assertEquals(2, m.getHitCount());
    }

  }

  public void testIdenticalMatches() throws Exception {

    final BooleanQuery bq = new BooleanQuery.Builder()
        .add(new TermQuery(new Term(textfield, "term1")), BooleanClause.Occur.MUST)
        .add(new TermQuery(new Term(textfield, "term1")), BooleanClause.Occur.SHOULD)
        .build();

    try (Monitor monitor = new Monitor((queryString, metadata) -> bq, MatchAllPresearcher.INSTANCE)) {
      monitor.update(new MonitorQuery("1", ""));
      Matches<HighlightsMatch> matches = monitor.match(buildDoc("1", "term1 term2"), HighlightingMatcher.FACTORY);
      HighlightsMatch m = matches.matches("1", "1");
      assertNotNull(m);
      assertEquals(1, m.getHitCount());
    }

  }

  public void testWildcardBooleanRewrites() throws Exception {

    final Query wc = new PrefixQuery(new Term(textfield, "term1"));

    final Query wrapper = new BooleanQuery.Builder()
        .add(wc, BooleanClause.Occur.MUST)
        .build();

    final Query wrapper2 = new BooleanQuery.Builder()
        .add(wrapper, BooleanClause.Occur.MUST)
        .build();

    final BooleanQuery bq = new BooleanQuery.Builder()
        .add(new PrefixQuery(new Term(textfield, "term2")), BooleanClause.Occur.MUST)
        .add(wrapper2, BooleanClause.Occur.MUST_NOT)
        .build();

    try (Monitor monitor = new Monitor((queryString, metadata) -> bq, MatchAllPresearcher.INSTANCE)) {

      monitor.update(new MonitorQuery("1", ""));
      Matches<HighlightsMatch> matches = monitor.match(buildDoc("1", "term2 term"), HighlightingMatcher.FACTORY);
      HighlightsMatch m = matches.matches("1", "1");
      assertNotNull(m);
      assertEquals(1, m.getHitCount());

      matches = monitor.match(buildDoc("1", "term2 term"), HighlightingMatcher.FACTORY);
      m = matches.matches("1", "1");
      assertNotNull(m);
      assertEquals(1, m.getHitCount());
    }
  }

  public void testWildcardProximityRewrites() throws Exception {
    final SpanNearQuery snq = SpanNearQuery.newOrderedNearQuery(textfield)
        .addClause(new SpanMultiTermQueryWrapper<>(new WildcardQuery(new Term(textfield, "term*"))))
        .addClause(new SpanTermQuery(new Term(textfield, "foo")))
        .build();

    try (Monitor monitor = new Monitor((queryString, metadata) -> snq, MatchAllPresearcher.INSTANCE)) {

      monitor.update(new MonitorQuery("1", ""));

      Matches<HighlightsMatch> matches = monitor.match(buildDoc("1", "term1 foo"), HighlightingMatcher.FACTORY);
      HighlightsMatch m = matches.matches("1", "1");
      assertNotNull(m);
      assertEquals(2, m.getHitCount());
    }
  }

  public void testDisjunctionWithOrderedNearSpans() throws Exception {

    final Query bq = new BooleanQuery.Builder()
        .add(new TermQuery(new Term(textfield, "a")), BooleanClause.Occur.SHOULD)
        .add(SpanNearQuery.newOrderedNearQuery(textfield)
            .addClause(new SpanTermQuery(new Term(textfield, "b")))
            .addClause(new SpanTermQuery(new Term(textfield, "c")))
            .setSlop(1)
            .build(), BooleanClause.Occur.SHOULD)
        .build();
    final Query parent = new BooleanQuery.Builder()
        .add(new TermQuery(new Term(textfield, "a")), BooleanClause.Occur.MUST)
        .add(bq, BooleanClause.Occur.MUST)
        .build();

    try (Monitor monitor = new Monitor((queryString, metadata) -> parent, MatchAllPresearcher.INSTANCE)) {
      monitor.update(new MonitorQuery("1", ""));

      InputDocument doc = buildDoc("1", "a b x x x x c");
      Matches<HighlightsMatch> matches = monitor.match(doc, HighlightingMatcher.FACTORY);

      HighlightsMatch m = matches.matches("1", "1");
      assertNotNull(m);
      assertEquals(1, m.getHitCount());
    }

  }

  public void testDisjunctionWithUnorderedNearSpans() throws Exception {

    final Query bq = new BooleanQuery.Builder()
        .add(new TermQuery(new Term(textfield, "a")), BooleanClause.Occur.SHOULD)
        .add(SpanNearQuery.newUnorderedNearQuery(textfield)
            .addClause(new SpanTermQuery(new Term(textfield, "b")))
            .addClause(new SpanTermQuery(new Term(textfield, "c")))
            .setSlop(1)
            .build(), BooleanClause.Occur.SHOULD)
        .build();
    final Query parent = new BooleanQuery.Builder()
        .add(new TermQuery(new Term(textfield, "a")), BooleanClause.Occur.MUST)
        .add(bq, BooleanClause.Occur.MUST)
        .build();

    try (Monitor monitor = new Monitor((queryString, metadata) -> parent, MatchAllPresearcher.INSTANCE)) {
      monitor.update(new MonitorQuery("1", ""));

      InputDocument doc = buildDoc("1", "a b x x x x c");
      Matches<HighlightsMatch> matches = monitor.match(doc, HighlightingMatcher.FACTORY);

      HighlightsMatch m = matches.matches("1", "1");
      assertNotNull(m);
      assertEquals(1, m.getHitCount());
    }

  }

  public void testEquality() {

    HighlightsMatch m1 = new HighlightsMatch("1", "1");
    m1.addHit("field", 0, 1, 0, 1);

    HighlightsMatch m2 = new HighlightsMatch("1", "1");
    m2.addHit("field", 0, 1, 0, 1);

    HighlightsMatch m3 = new HighlightsMatch("1", "1");
    m3.addHit("field", 0, 2, 0, 1);

    HighlightsMatch m4 = new HighlightsMatch("2", "1");
    m4.addHit("field", 0, 1, 0, 1);

    assertEquals(m1, m2);
    assertEquals(m1.hashCode(), m2.hashCode());
    assertNotEquals(m1, m3);
    assertNotEquals(m1, m4);
  }

  public void testMutliValuedFieldWithNonDefaultGaps() throws IOException, UpdateException {

    Analyzer analyzer = new Analyzer() {
      @Override
      public int getPositionIncrementGap(String fieldName) {
        return 1000;
      }

      @Override
      public int getOffsetGap(String fieldName) {
        return 2000;
      }

      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        return new TokenStreamComponents(new WhitespaceTokenizer());
      }
    };

    MonitorQuery mq = new MonitorQuery("query", textfield + ":\"hello world\"~5");
    try (Monitor monitor = newMonitor()) {
      monitor.update(mq);

      InputDocument doc1 = InputDocument.builder("doc1")
          .addField(textfield, "hello world", analyzer)
          .addField(textfield, "goodbye", analyzer)
          .build();
      Matches<HighlightsMatch> matcher1 = monitor.match(doc1, HighlightingMatcher.FACTORY);
      assertEquals(1, matcher1.getMatchCount("doc1"));
      HighlightsMatch m1 = matcher1.matches("query", "doc1");
      assertNotNull(m1);
      assertTrue(m1.getFields().contains(textfield));
      assertTrue(m1.getHits(textfield).contains(new HighlightsMatch.Hit(0, 0, 1, 11)));

      InputDocument doc2 = InputDocument.builder("doc2")
          .addField(textfield, "hello", analyzer)
          .addField(textfield, "world", analyzer)
          .build();
      Matches<HighlightsMatch> matcher2 = monitor.match(doc2, HighlightingMatcher.FACTORY);
      assertNull(matcher2.matches("query", "doc2"));
      assertEquals(0, matcher2.getMatchCount("doc2"));

      InputDocument doc3 = InputDocument.builder("doc3")
          .addField(textfield, "hello world", analyzer)
          .addField(textfield, "hello goodbye world", analyzer)
          .build();
      Matches<HighlightsMatch> matcher3 = monitor.match(doc3, HighlightingMatcher.FACTORY);
      assertEquals(1, matcher3.getMatchCount("doc3"));
      HighlightsMatch m3 = matcher3.matches("query", "doc3");
      assertNotNull(m3);
      assertTrue(m3.getFields().contains(textfield));
      assertTrue(m3.getHits(textfield).contains(new HighlightsMatch.Hit(0, 0, 1, 11)));
      assertTrue(m3.getHits(textfield).contains(new HighlightsMatch.Hit(1002, 2011, 1004, 2030)));
    }

  }

  public void testDisjunctionWithOrderedNearMatch() throws Exception {

    final Query bq = new BooleanQuery.Builder()
        .add(new TermQuery(new Term(textfield, "a")), BooleanClause.Occur.SHOULD)
        .add(SpanNearQuery.newOrderedNearQuery(textfield)
            .addClause(new SpanTermQuery(new Term(textfield, "b")))
            .addClause(new SpanTermQuery(new Term(textfield, "c")))
            .setSlop(1)
            .build(), BooleanClause.Occur.SHOULD)
        .build();
    final Query parent = new BooleanQuery.Builder()
        .add(new TermQuery(new Term(textfield, "a")), BooleanClause.Occur.MUST)
        .add(bq, BooleanClause.Occur.MUST)
        .build();

    try (Monitor monitor = new Monitor((queryString, metadata) -> parent, MatchAllPresearcher.INSTANCE)) {
      monitor.update(new MonitorQuery("1", ""));

      InputDocument doc = buildDoc("1", "a b c");
      Matches<HighlightsMatch> matches = monitor.match(doc, HighlightingMatcher.FACTORY);

      HighlightsMatch m = matches.matches("1", "1");
      assertNotNull(m);
      assertEquals(3, m.getHitCount());
      assertTrue(m.getFields().contains(textfield));
      assertTrue(m.getHits(textfield).contains(new HighlightsMatch.Hit(0, 0, 0, 1)));
      assertTrue(m.getHits(textfield).contains(new HighlightsMatch.Hit(1, 2, 1, 3)));
      assertTrue(m.getHits(textfield).contains(new HighlightsMatch.Hit(2, 4, 2, 5)));
    }

  }

  public void testUnorderedNearWithinOrderedNear() throws Exception {

    final SpanQuery spanPhrase = SpanNearQuery.newOrderedNearQuery(textfield)
        .addClause(new SpanTermQuery(new Term(textfield, "time")))
        .addClause(new SpanTermQuery(new Term(textfield, "men")))
        .setSlop(1)
        .build();

    final SpanQuery unorderedNear = SpanNearQuery.newUnorderedNearQuery(textfield)
        .addClause(spanPhrase)
        .addClause(new SpanTermQuery(new Term(textfield, "all")))
        .setSlop(5)
        .build();

    final SpanQuery orderedNear = SpanNearQuery.newOrderedNearQuery(textfield)
        .addClause(new SpanTermQuery(new Term(textfield, "the")))
        .addClause(unorderedNear)
        .setSlop(10)
        .build();

    final Query innerConjunct = new BooleanQuery.Builder()
        .add(new TermQuery(new Term(textfield, "is")), BooleanClause.Occur.MUST)
        .add(orderedNear, BooleanClause.Occur.MUST)
        .build();

    final Query disjunct = new BooleanQuery.Builder()
        .add(new TermQuery(new Term(textfield, "now")), BooleanClause.Occur.SHOULD)
        .add(innerConjunct, BooleanClause.Occur.SHOULD)
        .build();

    final Query outerConjunct = new BooleanQuery.Builder()
        .add(disjunct, BooleanClause.Occur.MUST)
        .add(new TermQuery(new Term(textfield, "good")), BooleanClause.Occur.MUST)
        .build();


    try (Monitor monitor = new Monitor((queryString, metadata) -> outerConjunct, MatchAllPresearcher.INSTANCE)) {
      monitor.update(new MonitorQuery("1", ""));

      InputDocument doc = buildDoc("1", "now is the time for all good men");
      Matches<HighlightsMatch> matches = monitor.match(doc, HighlightingMatcher.FACTORY);
      HighlightsMatch m = matches.matches("1", "1");
      assertEquals(2, m.getHitCount());
      assertTrue(m.getFields().contains(textfield));
      assertTrue(m.getHits(textfield).contains(new HighlightsMatch.Hit(0, 0, 0, 3)));
      assertTrue(m.getHits(textfield).contains(new HighlightsMatch.Hit(6, 24, 6, 28)));
    }

  }

  public void testMinShouldMatchQuery() throws Exception {

    final Query minq = new BooleanQuery.Builder()
        .add(new TermQuery(new Term(textfield, "x")), BooleanClause.Occur.SHOULD)
        .add(new TermQuery(new Term(textfield, "y")), BooleanClause.Occur.SHOULD)
        .add(new TermQuery(new Term(textfield, "z")), BooleanClause.Occur.SHOULD)
        .setMinimumNumberShouldMatch(2)
        .build();

    final Query bq = new BooleanQuery.Builder()
        .add(new TermQuery(new Term(textfield, "a")), BooleanClause.Occur.MUST)
        .add(new TermQuery(new Term(textfield, "b")), BooleanClause.Occur.MUST)
        .add(minq, BooleanClause.Occur.SHOULD)
        .build();

    try (Monitor monitor = new Monitor((queryString, metadata) -> bq, MatchAllPresearcher.INSTANCE)) {
      monitor.update(new MonitorQuery("1", ""));

      InputDocument doc = buildDoc("1", "a b x");
      Matches<HighlightsMatch> matches = monitor.match(doc, HighlightingMatcher.FACTORY);
      HighlightsMatch m = matches.matches("1", "1");
      assertNotNull(m);
      assertEquals(2, m.getHitCount());
      assertTrue(m.getFields().contains(textfield));
      assertTrue(m.getHits(textfield).contains(new HighlightsMatch.Hit(0, 0, 0, 1)));
      assertTrue(m.getHits(textfield).contains(new HighlightsMatch.Hit(1, 2, 1, 3)));
    }

  }

  public void testComplexPhraseQueryParser() throws IOException, UpdateException {

    ComplexPhraseQueryParser cpqp = new ComplexPhraseQueryParser(textfield, new StandardAnalyzer());
    MonitorQueryParser cqp = (queryString, metadata) -> cpqp.parse(queryString);
    try (Monitor monitor = new Monitor(cqp, MatchAllPresearcher.INSTANCE)) {
      monitor.update(new MonitorQuery("1", "\"x b\""));

      InputDocument doc = buildDoc("1", "x b c");
      Matches<HighlightsMatch> matches = monitor.match(doc, HighlightingMatcher.FACTORY);
      HighlightsMatch m = matches.matches("1", "1");
      assertNotNull(m);
      assertEquals(2, m.getHitCount());
      assertTrue(m.getFields().contains(textfield));
    }

  }

  public void testHighlightBatches() throws Exception {
    String query = "\"cell biology\"";
    String matching_document = "the cell biology count";

    try (Monitor monitor = newMonitor()) {

      monitor.update(new MonitorQuery("query0", "non matching query"));
      monitor.update(new MonitorQuery("query1", query));
      monitor.update(new MonitorQuery("query2", "biology"));

      DocumentBatch batch = DocumentBatch.of(
          InputDocument.builder("doc1").addField(textfield, matching_document, WHITESPACE).build(),
          InputDocument.builder("doc2").addField(textfield, "nope", WHITESPACE).build(),
          InputDocument.builder("doc3").addField(textfield, "biology text", WHITESPACE).build()
      );

      Matches<HighlightsMatch> matches = monitor.match(batch, HighlightingMatcher.FACTORY);
      assertEquals(2, matches.getMatchCount("doc1"));
      assertEquals(0, matches.getMatchCount("doc2"));
      assertEquals(1, matches.getMatchCount("doc3"));
      HighlightsMatch m1 = matches.matches("query1", "doc1");
      assertTrue(m1.getHits(textfield).contains(new HighlightsMatch.Hit(1, 4, 2, 16)));
      HighlightsMatch m2 = matches.matches("query2", "doc3");
      assertTrue(m2.getHits(textfield).contains(new HighlightsMatch.Hit(0, 0, 0, 7)));
    }
  }

}
