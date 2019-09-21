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

package org.apache.lucene.monitor;

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.complexPhrase.ComplexPhraseQueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;


public class TestHighlightingMatcher extends MonitorTestBase {

  private static final Analyzer WHITESPACE = new WhitespaceAnalyzer();

  public static Document buildDoc(String text) {
    Document doc = new Document();
    doc.add(newTextField(FIELD, text, Field.Store.NO));
    return doc;
  }

  public void testSingleTermQueryMatchesSingleDocument() throws IOException {

    try (Monitor monitor = newMonitor()) {
      MonitorQuery mq = new MonitorQuery("query1", parse("test"));
      monitor.register(mq);

      MatchingQueries<HighlightsMatch> matches = monitor.match(buildDoc("this is a test document"),
          HighlightsMatch.MATCHER);
      assertEquals(1, matches.getMatchCount());
      HighlightsMatch match = matches.matches("query1");
      assertTrue(match.getHits(FIELD).contains(new HighlightsMatch.Hit(3, 10, 3, 14)));
    }
  }

  public void testSinglePhraseQueryMatchesSingleDocument() throws IOException {

    try (Monitor monitor = newMonitor()) {
      MonitorQuery mq = new MonitorQuery("query1", parse("\"test document\""));
      monitor.register(mq);

      MatchingQueries<HighlightsMatch> matches = monitor.match(buildDoc("this is a test document"),
          HighlightsMatch.MATCHER);
      assertEquals(1, matches.getMatchCount());
      HighlightsMatch m = matches.matches("query1");
      assertTrue(m.getHits(FIELD).contains(new HighlightsMatch.Hit(3, 10, 4, 23)));
    }

  }

  public void testToString() {

    HighlightsMatch match = new HighlightsMatch("1");
    match.addHit("field", 2, 3, -1, -1);
    match.addHit("field", 0, 1, -1, -1);
    match.addHit("afield", 0, 1, 0, 4);

    assertEquals("Match(query=1){hits={afield=[0(0)->1(4)], field=[0(-1)->1(-1), 2(-1)->3(-1)]}}", match.toString());
  }

  public void testMultiFieldQueryMatches() throws IOException {

    Document doc = new Document();
    doc.add(newTextField("field1", "this is a test of field one", Field.Store.NO));
    doc.add(newTextField("field2", "and this is an additional test", Field.Store.NO));

    try (Monitor monitor = newMonitor()) {
      monitor.register(new MonitorQuery("query1", parse("field1:test field2:test")));

      MatchingQueries<HighlightsMatch> matches = monitor.match(doc, HighlightsMatch.MATCHER);
      assertEquals(1, matches.getMatchCount());

      HighlightsMatch m = matches.matches("query1");
      assertNotNull(m);
      assertTrue(m.getFields().contains("field1"));
      assertTrue(m.getHits("field1").contains(new HighlightsMatch.Hit(3, 10, 3, 14)));
      assertTrue(m.getHits("field2").contains(new HighlightsMatch.Hit(5, 26, 5, 30)));
    }

  }

  public void testQueryErrors() throws IOException {

    try (Monitor monitor = new Monitor(ANALYZER, Presearcher.NO_FILTERING)) {

      monitor.register(new MonitorQuery("1", parse("test")),
          new MonitorQuery("2", new ThrowOnRewriteQuery()),
          new MonitorQuery("3", parse("document")),
          new MonitorQuery("4", parse("foo")));

      MatchingQueries<HighlightsMatch> matches = monitor.match(buildDoc("this is a test document"), HighlightsMatch.MATCHER);
      assertEquals(4, matches.getQueriesRun());
      assertEquals(2, matches.getMatchCount());
      assertEquals(1, matches.getErrors().size());
    }
  }

  public void testWildcards() throws IOException {

    try (Monitor monitor = newMonitor()) {

      monitor.register(new MonitorQuery("1", new RegexpQuery(new Term(FIELD, "he.*"))));

      MatchingQueries<HighlightsMatch> matches = monitor.match(buildDoc("hello world"), HighlightsMatch.MATCHER);
      assertEquals(1, matches.getQueriesRun());
      assertEquals(1, matches.getMatchCount());
      assertEquals(1, matches.matches("1").getHitCount());
    }
  }

  public void testWildcardCombinations() throws Exception {

    final BooleanQuery bq = new BooleanQuery.Builder()
        .add(new TermQuery(new Term(FIELD, "term1")), BooleanClause.Occur.MUST)
        .add(new PrefixQuery(new Term(FIELD, "term2")), BooleanClause.Occur.MUST)
        .add(new TermQuery(new Term(FIELD, "term3")), BooleanClause.Occur.MUST_NOT)
        .build();

    try (Monitor monitor = newMonitor()) {
      monitor.register(new MonitorQuery("1", bq));

      MatchingQueries<HighlightsMatch> matches = monitor.match(buildDoc("term1 term22 term4"), HighlightsMatch.MATCHER);
      HighlightsMatch m = matches.matches("1");
      assertNotNull(m);
      assertEquals(2, m.getHitCount());
    }

  }

  public void testDisjunctionMaxQuery() throws IOException {
    final DisjunctionMaxQuery query = new DisjunctionMaxQuery(Arrays.asList(
        new TermQuery(new Term(FIELD, "term1")), new PrefixQuery(new Term(FIELD, "term2"))
    ), 1.0f);

    try (Monitor monitor = newMonitor()) {
      monitor.register(new MonitorQuery("1", query));
      MatchingQueries<HighlightsMatch> matches = monitor.match(buildDoc("term1 term2 term3"), HighlightsMatch.MATCHER);
      HighlightsMatch m = matches.matches("1");
      assertNotNull(m);
      assertEquals(2, m.getHitCount());
    }

  }

  public void testIdenticalMatches() throws Exception {

    final BooleanQuery bq = new BooleanQuery.Builder()
        .add(new TermQuery(new Term(FIELD, "term1")), BooleanClause.Occur.MUST)
        .add(new TermQuery(new Term(FIELD, "term1")), BooleanClause.Occur.SHOULD)
        .build();

    try (Monitor monitor = new Monitor(ANALYZER)) {
      monitor.register(new MonitorQuery("1", bq));
      MatchingQueries<HighlightsMatch> matches = monitor.match(buildDoc("term1 term2"), HighlightsMatch.MATCHER);
      HighlightsMatch m = matches.matches("1");
      assertNotNull(m);
      assertEquals(1, m.getHitCount());
    }

  }

  public void testWildcardBooleanRewrites() throws Exception {

    final Query wc = new PrefixQuery(new Term(FIELD, "term1"));

    final Query wrapper = new BooleanQuery.Builder()
        .add(wc, BooleanClause.Occur.MUST)
        .build();

    final Query wrapper2 = new BooleanQuery.Builder()
        .add(wrapper, BooleanClause.Occur.MUST)
        .build();

    final BooleanQuery bq = new BooleanQuery.Builder()
        .add(new PrefixQuery(new Term(FIELD, "term2")), BooleanClause.Occur.MUST)
        .add(wrapper2, BooleanClause.Occur.MUST_NOT)
        .build();

    try (Monitor monitor = new Monitor(ANALYZER)) {

      monitor.register(new MonitorQuery("1", bq));
      MatchingQueries<HighlightsMatch> matches = monitor.match(buildDoc("term2 term"), HighlightsMatch.MATCHER);
      HighlightsMatch m = matches.matches("1");
      assertNotNull(m);
      assertEquals(1, m.getHitCount());

      matches = monitor.match(buildDoc("term2 term"), HighlightsMatch.MATCHER);
      m = matches.matches("1");
      assertNotNull(m);
      assertEquals(1, m.getHitCount());
    }
  }

  public void testWildcardProximityRewrites() throws Exception {
    final SpanNearQuery snq = SpanNearQuery.newOrderedNearQuery(FIELD)
        .addClause(new SpanMultiTermQueryWrapper<>(new WildcardQuery(new Term(FIELD, "term*"))))
        .addClause(new SpanTermQuery(new Term(FIELD, "foo")))
        .build();

    try (Monitor monitor = newMonitor()) {

      monitor.register(new MonitorQuery("1", snq));

      MatchingQueries<HighlightsMatch> matches = monitor.match(buildDoc("term1 foo"), HighlightsMatch.MATCHER);
      HighlightsMatch m = matches.matches("1");
      assertNotNull(m);
      assertEquals(2, m.getHitCount());
    }
  }

  public void testDisjunctionWithOrderedNearSpans() throws Exception {

    final Query bq = new BooleanQuery.Builder()
        .add(new TermQuery(new Term(FIELD, "a")), BooleanClause.Occur.SHOULD)
        .add(SpanNearQuery.newOrderedNearQuery(FIELD)
            .addClause(new SpanTermQuery(new Term(FIELD, "b")))
            .addClause(new SpanTermQuery(new Term(FIELD, "c")))
            .setSlop(1)
            .build(), BooleanClause.Occur.SHOULD)
        .build();
    final Query parent = new BooleanQuery.Builder()
        .add(new TermQuery(new Term(FIELD, "a")), BooleanClause.Occur.MUST)
        .add(bq, BooleanClause.Occur.MUST)
        .build();

    try (Monitor monitor = new Monitor(ANALYZER)) {
      monitor.register(new MonitorQuery("1", parent));

      Document doc = buildDoc("a b x x x x c");
      MatchingQueries<HighlightsMatch> matches = monitor.match(doc, HighlightsMatch.MATCHER);

      HighlightsMatch m = matches.matches("1");
      assertNotNull(m);
      assertEquals(1, m.getHitCount());
    }

  }

  public void testDisjunctionWithUnorderedNearSpans() throws Exception {

    final Query bq = new BooleanQuery.Builder()
        .add(new TermQuery(new Term(FIELD, "a")), BooleanClause.Occur.SHOULD)
        .add(SpanNearQuery.newUnorderedNearQuery(FIELD)
            .addClause(new SpanTermQuery(new Term(FIELD, "b")))
            .addClause(new SpanTermQuery(new Term(FIELD, "c")))
            .setSlop(1)
            .build(), BooleanClause.Occur.SHOULD)
        .build();
    final Query parent = new BooleanQuery.Builder()
        .add(new TermQuery(new Term(FIELD, "a")), BooleanClause.Occur.MUST)
        .add(bq, BooleanClause.Occur.MUST)
        .build();

    try (Monitor monitor = new Monitor(ANALYZER)) {
      monitor.register(new MonitorQuery("1", parent));

      Document doc = buildDoc("a b x x x x c");
      MatchingQueries<HighlightsMatch> matches = monitor.match(doc, HighlightsMatch.MATCHER);

      HighlightsMatch m = matches.matches("1");
      assertNotNull(m);
      assertEquals(1, m.getHitCount());
    }

  }

  public void testEquality() {

    HighlightsMatch m1 = new HighlightsMatch("1");
    m1.addHit("field", 0, 1, 0, 1);

    HighlightsMatch m2 = new HighlightsMatch("1");
    m2.addHit("field", 0, 1, 0, 1);

    HighlightsMatch m3 = new HighlightsMatch("1");
    m3.addHit("field", 0, 2, 0, 1);

    HighlightsMatch m4 = new HighlightsMatch("2");
    m4.addHit("field", 0, 1, 0, 1);

    assertEquals(m1, m2);
    assertEquals(m1.hashCode(), m2.hashCode());
    assertNotEquals(m1, m3);
    assertNotEquals(m1, m4);
  }

  public void testMutliValuedFieldWithNonDefaultGaps() throws IOException {

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

    MonitorQuery mq = new MonitorQuery("query", parse(FIELD + ":\"hello world\"~5"));
    try (Monitor monitor = newMonitor(analyzer)) {
      monitor.register(mq);

      Document doc1 = new Document();
      doc1.add(newTextField(FIELD, "hello world", Field.Store.NO));
      doc1.add(newTextField(FIELD, "goodbye", Field.Store.NO));
      MatchingQueries<HighlightsMatch> matcher1 = monitor.match(doc1, HighlightsMatch.MATCHER);
      assertEquals(1, matcher1.getMatchCount());
      HighlightsMatch m1 = matcher1.matches("query");
      assertNotNull(m1);
      assertTrue(m1.getFields().contains(FIELD));
      assertTrue(m1.getHits(FIELD).contains(new HighlightsMatch.Hit(0, 0, 1, 11)));

      Document doc2 = new Document();
      doc1.add(newTextField(FIELD, "hello", Field.Store.NO));
      doc1.add(newTextField(FIELD, "world", Field.Store.NO));
      MatchingQueries<HighlightsMatch> matcher2 = monitor.match(doc2, HighlightsMatch.MATCHER);
      assertNull(matcher2.matches("query"));
      assertEquals(0, matcher2.getMatchCount());

      Document doc3 = new Document();
      doc3.add(newTextField(FIELD, "hello world", Field.Store.NO));
      doc3.add(newTextField(FIELD, "hello goodbye world", Field.Store.NO));
      MatchingQueries<HighlightsMatch> matcher3 = monitor.match(doc3, HighlightsMatch.MATCHER);
      assertEquals(1, matcher3.getMatchCount());
      HighlightsMatch m3 = matcher3.matches("query");
      assertNotNull(m3);
      assertTrue(m3.getFields().contains(FIELD));
      assertTrue(m3.getHits(FIELD).contains(new HighlightsMatch.Hit(0, 0, 1, 11)));
      assertTrue(m3.getHits(FIELD).contains(new HighlightsMatch.Hit(1002, 2011, 1004, 2030)));
    }

  }

  public void testDisjunctionWithOrderedNearMatch() throws Exception {

    final Query bq = new BooleanQuery.Builder()
        .add(new TermQuery(new Term(FIELD, "a")), BooleanClause.Occur.SHOULD)
        .add(SpanNearQuery.newOrderedNearQuery(FIELD)
            .addClause(new SpanTermQuery(new Term(FIELD, "b")))
            .addClause(new SpanTermQuery(new Term(FIELD, "c")))
            .setSlop(1)
            .build(), BooleanClause.Occur.SHOULD)
        .build();
    final Query parent = new BooleanQuery.Builder()
        .add(new TermQuery(new Term(FIELD, "a")), BooleanClause.Occur.MUST)
        .add(bq, BooleanClause.Occur.MUST)
        .build();

    try (Monitor monitor = newMonitor()) {
      monitor.register(new MonitorQuery("1", parent));

      Document doc = buildDoc("a b c");
      MatchingQueries<HighlightsMatch> matches = monitor.match(doc, HighlightsMatch.MATCHER);

      HighlightsMatch m = matches.matches("1");
      assertNotNull(m);
      assertEquals(3, m.getHitCount());
      assertTrue(m.getFields().contains(FIELD));
      assertTrue(m.getHits(FIELD).contains(new HighlightsMatch.Hit(0, 0, 0, 1)));
      assertTrue(m.getHits(FIELD).contains(new HighlightsMatch.Hit(1, 2, 1, 3)));
      assertTrue(m.getHits(FIELD).contains(new HighlightsMatch.Hit(2, 4, 2, 5)));
    }

  }

  public void testUnorderedNearWithinOrderedNear() throws Exception {

    final SpanQuery spanPhrase = SpanNearQuery.newOrderedNearQuery(FIELD)
        .addClause(new SpanTermQuery(new Term(FIELD, "time")))
        .addClause(new SpanTermQuery(new Term(FIELD, "men")))
        .setSlop(1)
        .build();

    final SpanQuery unorderedNear = SpanNearQuery.newUnorderedNearQuery(FIELD)
        .addClause(spanPhrase)
        .addClause(new SpanTermQuery(new Term(FIELD, "all")))
        .setSlop(5)
        .build();

    final SpanQuery orderedNear = SpanNearQuery.newOrderedNearQuery(FIELD)
        .addClause(new SpanTermQuery(new Term(FIELD, "the")))
        .addClause(unorderedNear)
        .setSlop(10)
        .build();

    final Query innerConjunct = new BooleanQuery.Builder()
        .add(new TermQuery(new Term(FIELD, "is")), BooleanClause.Occur.MUST)
        .add(orderedNear, BooleanClause.Occur.MUST)
        .build();

    final Query disjunct = new BooleanQuery.Builder()
        .add(new TermQuery(new Term(FIELD, "now")), BooleanClause.Occur.SHOULD)
        .add(innerConjunct, BooleanClause.Occur.SHOULD)
        .build();

    final Query outerConjunct = new BooleanQuery.Builder()
        .add(disjunct, BooleanClause.Occur.MUST)
        .add(new TermQuery(new Term(FIELD, "good")), BooleanClause.Occur.MUST)
        .build();


    try (Monitor monitor = newMonitor()) {
      monitor.register(new MonitorQuery("1", outerConjunct));
      Document doc = buildDoc("now is the time for all good men");
      MatchingQueries<HighlightsMatch> matches = monitor.match(doc, HighlightsMatch.MATCHER);
      HighlightsMatch m = matches.matches("1");
      assertEquals(2, m.getHitCount());
      assertTrue(m.getFields().contains(FIELD));
      assertTrue(m.getHits(FIELD).contains(new HighlightsMatch.Hit(0, 0, 0, 3)));
      assertTrue(m.getHits(FIELD).contains(new HighlightsMatch.Hit(6, 24, 6, 28)));
    }

  }

  public void testMinShouldMatchQuery() throws Exception {

    final Query minq = new BooleanQuery.Builder()
        .add(new TermQuery(new Term(FIELD, "x")), BooleanClause.Occur.SHOULD)
        .add(new TermQuery(new Term(FIELD, "y")), BooleanClause.Occur.SHOULD)
        .add(new TermQuery(new Term(FIELD, "z")), BooleanClause.Occur.SHOULD)
        .setMinimumNumberShouldMatch(2)
        .build();

    final Query bq = new BooleanQuery.Builder()
        .add(new TermQuery(new Term(FIELD, "a")), BooleanClause.Occur.MUST)
        .add(new TermQuery(new Term(FIELD, "b")), BooleanClause.Occur.MUST)
        .add(minq, BooleanClause.Occur.SHOULD)
        .build();

    try (Monitor monitor = newMonitor()) {
      monitor.register(new MonitorQuery("1", bq));
      Document doc = buildDoc("a b x");
      MatchingQueries<HighlightsMatch> matches = monitor.match(doc, HighlightsMatch.MATCHER);
      HighlightsMatch m = matches.matches("1");
      assertNotNull(m);
      assertEquals(2, m.getHitCount());
      assertTrue(m.getFields().contains(FIELD));
      assertTrue(m.getHits(FIELD).contains(new HighlightsMatch.Hit(0, 0, 0, 1)));
      assertTrue(m.getHits(FIELD).contains(new HighlightsMatch.Hit(1, 2, 1, 3)));
    }

  }

  public void testComplexPhraseQueryParser() throws Exception {

    ComplexPhraseQueryParser cpqp = new ComplexPhraseQueryParser(FIELD, new StandardAnalyzer());
    Query query = cpqp.parse("\"x b\"");
    try (Monitor monitor = newMonitor()) {
      monitor.register(new MonitorQuery("1", query));
      Document doc = buildDoc("x b c");
      MatchingQueries<HighlightsMatch> matches = monitor.match(doc, HighlightsMatch.MATCHER);
      HighlightsMatch m = matches.matches("1");
      assertNotNull(m);
      assertEquals(2, m.getHitCount());
      assertTrue(m.getFields().contains(FIELD));
    }

  }

  public void testHighlightBatches() throws Exception {
    String query = "\"cell biology\"";

    try (Monitor monitor = newMonitor(WHITESPACE)) {

      monitor.register(new MonitorQuery("query0", parse("non matching query")));
      monitor.register(new MonitorQuery("query1", parse(query)));
      monitor.register(new MonitorQuery("query2", parse("biology")));

      Document doc1 = new Document();
      doc1.add(newTextField(FIELD, "the cell biology count", Field.Store.NO)); // matches
      Document doc2 = new Document();
      doc2.add(newTextField(FIELD, "nope", Field.Store.NO));
      Document doc3 = new Document();
      doc3.add(newTextField(FIELD, "biology text", Field.Store.NO));

      MultiMatchingQueries<HighlightsMatch> matches = monitor.match(new Document[]{doc1, doc2, doc3}, HighlightsMatch.MATCHER);
      assertEquals(2, matches.getMatchCount(0));
      assertEquals(0, matches.getMatchCount(1));
      assertEquals(1, matches.getMatchCount(2));
      HighlightsMatch m1 = matches.matches("query1", 0);
      assertTrue(m1.getHits(FIELD).contains(new HighlightsMatch.Hit(1, 4, 2, 16)));
      HighlightsMatch m2 = matches.matches("query2", 2);
      assertTrue(m2.getHits(FIELD).contains(new HighlightsMatch.Hit(0, 0, 0, 7)));
    }
  }

}
