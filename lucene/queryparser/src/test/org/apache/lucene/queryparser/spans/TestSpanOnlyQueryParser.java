package org.apache.lucene.queryparser.spans;

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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.IOException;

import static org.apache.lucene.util.automaton.BasicAutomata.makeString;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockTokenFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.spans.SpanOnlyParser;
import org.apache.lucene.queryparser.spans.AnalyzingQueryParserBase.NORM_MULTI_TERMS;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.search.spans.Spans;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.OpenBitSet;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.automaton.BasicOperations;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestSpanOnlyQueryParser extends LuceneTestCase {

  private static IndexReader reader;
  private static IndexSearcher searcher;
  private static Directory directory;
  private static Analyzer stopAnalyzer;
  private static Analyzer noStopAnalyzer;
  private static final String FIELD = "f1";

  private static final CharacterRunAutomaton STOP_WORDS = new CharacterRunAutomaton(
      BasicOperations.union(Arrays.asList(makeString("a"), makeString("an"),
          makeString("and"), makeString("are"), makeString("as"),
          makeString("at"), makeString("be"), makeString("but"),
          makeString("by"), makeString("for"), makeString("if"),
          makeString("in"), makeString("into"), makeString("is"),
          makeString("it"), makeString("no"), makeString("not"),
          makeString("of"), makeString("on"), makeString("or"),
          makeString("such"), makeString("that"), makeString("the"),
          makeString("their"), makeString("then"), makeString("there"),
          makeString("these"), makeString("they"), makeString("this"),
          makeString("to"), makeString("was"), makeString("will"),
          makeString("with"), makeString("\u5927"))));

  @BeforeClass
  public static void beforeClass() throws Exception {

    noStopAnalyzer = new Analyzer() {
      @Override
      public TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE,
            true);
        TokenFilter filter = new MockStandardTokenizerFilter(tokenizer);
        return new TokenStreamComponents(tokenizer, filter);
      }
    };

    stopAnalyzer = new Analyzer() {
      @Override
      public TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE,
            true);
        TokenFilter filter = new MockStandardTokenizerFilter(tokenizer);
        filter = new MockTokenFilter(filter, STOP_WORDS);
        return new TokenStreamComponents(tokenizer, filter);
      }
    };

    directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory,
        newIndexWriterConfig(TEST_VERSION_CURRENT, stopAnalyzer)
        .setMaxBufferedDocs(TestUtil.nextInt(random(), 100, 1000))
        .setMergePolicy(newLogMergePolicy()));
    String[] docs = new String[] {
        "the quick brown fox ",
        "jumped over the lazy brown dog and the brown green cat",
        "quick green fox",
        "abcdefghijk",
        "over green lazy",
        // longish doc for recursion test
        "eheu fugaces postume postume labuntur anni nec "
        + "pietas moram rugis et instanti senectae "
        + "adferet indomitaeque morti",
        // non-whitespace language
        "\u666E \u6797 \u65AF \u987F \u5927 \u5B66",
        "reg/exp",
        "/regex/",
        "fuzzy~2",
        "wil*card",
        "wil?card",
        "prefi*",
        "single'quote"

    };

    for (int i = 0; i < docs.length; i++) {
      Document doc = new Document();
      doc.add(newTextField(FIELD, docs[i], Field.Store.YES));
      writer.addDocument(doc);
    }
    reader = writer.getReader();
    searcher = newSearcher(reader);
    writer.close();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    reader.close();
    directory.close();
    reader = null;
    directory = null;
    stopAnalyzer = null;
    noStopAnalyzer = null;
  }

  public void testBasic() throws Exception {
    SpanOnlyParser p = new SpanOnlyParser(TEST_VERSION_CURRENT, FIELD, stopAnalyzer);

    // test null and empty
    boolean ex = false;
    try{
      countSpansDocs(p, null, 0, 0);

    } catch (NullPointerException e) {
      ex = true;
    }
    assertEquals(true, ex);
    countSpansDocs(p, "", 0, 0);

    countSpansDocs(p, "brown", 3, 2);
  }

  public void testNear() throws Exception {
    SpanOnlyParser p = new SpanOnlyParser(TEST_VERSION_CURRENT, FIELD, noStopAnalyzer);

    // unmatched "
    try {
      p.parse("\"brown \"dog\"");
      fail("didn't get expected exception");
    } catch (ParseException expected) {}

    // unmatched [
    try {
      p.parse("[brown [dog]");
      fail("didn't get expected exception");
    } catch (ParseException expected) {}

    testOffsetForSingleSpanMatch(p, "\"brown dog\"", 1, 4, 6);

    countSpansDocs(p, "\"lazy dog\"", 0, 0);

    testOffsetForSingleSpanMatch(p, "\"lazy dog\"~2", 1, 3, 6);

    testOffsetForSingleSpanMatch(p, "\"lazy dog\"~>2", 1, 3, 6);

    testOffsetForSingleSpanMatch(p, "\"dog lazy\"~2", 1, 3, 6);

    countSpansDocs(p, "\"dog lazy\"~>2", 0, 0);

    testOffsetForSingleSpanMatch(p, "[\"lazy dog\"~>2 cat]~10", 1, 3, 11);

    testOffsetForSingleSpanMatch(p, "[\"lazy dog\"~>2 cat]~>10", 1, 3, 11);

    countSpansDocs(p, "[cat \"lazy dog\"~>2]~>10", 0, 0);

    // shows that "intervening" for multiple terms is additive
    // 3 includes "over the" and "brown"
    testOffsetForSingleSpanMatch(p, "[jumped lazy dog]~3", 1, 0, 6);

    // only two words separate each hit, but together, the intervening words > 2
    countSpansDocs(p, "[jumped lazy dog]~2", 0, 0);
  }

  public void testNotNear() throws Exception {
    SpanOnlyParser p = new SpanOnlyParser(TEST_VERSION_CURRENT, FIELD, noStopAnalyzer);

    // must have two components
    try {
      p.parse("\"brown dog car\"!~2,2");
      fail("didn't get expected exception");
    } catch (ParseException expected) {}

    countSpansDocs(p, "\"brown dog\"!~2,2", 2, 2);

    testOffsetForSingleSpanMatch(p, "\"brown (green dog)\"!~1,1", 0, 2, 3);

    countSpansDocs(p, "\"brown (cat dog)\"!~1,1", 2, 2);

    countSpansDocs(p, "\"brown (quick lazy)\"!~0,4", 3, 2);

    countSpansDocs(p, "\"brown quick\"!~1,4", 2, 1);

    testOffsetForSingleSpanMatch(p, "\"brown (quick lazy)\"!~1,4", 1, 8, 9);

    // test empty
    countSpansDocs(p, "\"z y\"!~0,4", 0, 0);

    testOffsetForSingleSpanMatch(p, "[[quick fox]~3 brown]!~1,1", 2, 0, 3);

    // traditional SpanNotQuery
    testOffsetForSingleSpanMatch(p, "[[quick fox]~3 brown]!~", 2, 0, 3);
  }

  public void testWildcard() throws Exception {
    SpanOnlyParser p = new SpanOnlyParser(TEST_VERSION_CURRENT, FIELD, noStopAnalyzer);

    //default: don't allow leading wildcards
    try {
      p.parse("*og");
      fail("didn't get expected exception");
    } catch (ParseException expected) {
    }

    p.setAllowLeadingWildcard(true);

    // lowercasing as default
    testOffsetForSingleSpanMatch(p, "*OG", 1, 5, 6);

    p.setNormMultiTerms(NORM_MULTI_TERMS.NONE);

    countSpansDocs(p, "*OG", 0, 0);

    testOffsetForSingleSpanMatch(p, "*og", 1, 5, 6);
    testOffsetForSingleSpanMatch(p, "?og", 1, 5, 6);

    // brown dog and brown fox
    countSpansDocs(p, "[brown ?o?]", 2, 2);
    countSpansDocs(p, "[br* ?o?]", 2, 2);
  }

  public void testPrefix() throws Exception {
    SpanOnlyParser p = new SpanOnlyParser(TEST_VERSION_CURRENT, FIELD, noStopAnalyzer);

    // lowercasing as default
    countSpansDocs(p, "BR*", 3, 2);

    countSpansDocs(p, "br*", 3, 2);

    p.setNormMultiTerms(NORM_MULTI_TERMS.NONE);
    countSpansDocs(p, "BR*", 0, 0);

    // not actually a prefix query
    countSpansDocs(p, "br?", 0, 0);

    p.setAllowLeadingWildcard(true);
    countSpansDocs(p, "*", 46, 14);
  }

  public void testRegex() throws Exception {
    SpanOnlyParser p = new SpanOnlyParser(TEST_VERSION_CURRENT, FIELD, noStopAnalyzer);


    countSpansDocs(p, "/b[wor]+n/", 3, 2);
    countSpansDocs(p, " /b[wor]+n/ ", 3, 2);

    testOffsetForSingleSpanMatch(p, " [/b[wor]+n/ fox]", 0, 2, 4);

    testOffsetForSingleSpanMatch(p, " [/b[wor]+n/fox]", 0, 2, 4);

    countSpansDocs(p, " [/b[wor]+n/ (fox dog)]", 2, 2);

    p.setNormMultiTerms(NORM_MULTI_TERMS.LOWERCASE);

    //there should be no processing of regexes!
    countSpansDocs(p, "/(?i)B[wor]+n/", 0, 0);

    p.setNormMultiTerms(NORM_MULTI_TERMS.NONE);
    countSpansDocs(p, "/B[wor]+n/", 0, 0);

    //test special regex escape
    countSpansDocs(p, "/reg\\/exp/", 1, 1);
  }

  public void testFuzzy() throws Exception {
    //could use more testing of requested and fuzzyMinSim < 1.0f
    SpanOnlyParser p = new SpanOnlyParser(TEST_VERSION_CURRENT, FIELD, noStopAnalyzer);

    countSpansDocs(p, "bruun~", 3, 2);
    countSpansDocs(p, "bruun~2", 3, 2);

    //default should reduce 3 to 2 and therefore not have any hits
    countSpansDocs(p, "abcdefgh~3", 0, 0);

    p.setFuzzyMinSim(3.0f);
    testOffsetForSingleSpanMatch(p, "abcdefgh~3", 3, 0, 1);

    // default lowercasing
    testOffsetForSingleSpanMatch(p, "Abcdefgh~3", 3, 0, 1);
    p.setNormMultiTerms(NORM_MULTI_TERMS.NONE);
    countSpansDocs(p, "Abcdefgh~3", 0, 0);

    countSpansDocs(p, "brwon~1", 3, 2);
    countSpansDocs(p, "brwon~>1", 0, 0);

    countSpansDocs(p, "crown~1,1", 0, 0);
    countSpansDocs(p, "crown~2,1", 0, 0);
    countSpansDocs(p, "crown~3,1", 0, 0);
    countSpansDocs(p, "brwn~1,1", 3, 2);

    p.setFuzzyMinSim(0.79f);

    countSpansDocs(p, "brwon~2", 3, 2);

    //fuzzy val of 0 should yield straight SpanTermQuery
    Query q = p.parse("brown~0");
    assertTrue("fuzzy val = 0", q instanceof SpanTermQuery);
    q = p.parse("brown~0");
    assertTrue("fuzzy val = 0", q instanceof SpanTermQuery);
  }

  public void testStopWords() throws Exception {
    // Stop word handling has some room for improvement with SpanQuery
    // These tests codify the expectations (for regular behavior,
    // parse exceptions and false hits) as of this writing.

    SpanOnlyParser p = new SpanOnlyParser(TEST_VERSION_CURRENT, FIELD, stopAnalyzer);

    countSpansDocs(p, "the", 0, 0);

    // these are whittled down to just a query for brown
    countSpansDocs(p, "[the brown]", 3, 2);

    countSpansDocs(p, "(the brown)", 3, 2);

    testException(p, "[brown the]!~5,5");

    // this will not match because "the" is silently dropped from the query
    countSpansDocs(p, "[over the lazy]", 0, 0);

    // this will get one right hit, but incorrectly match "over green lazy"
    countSpansDocs(p, "[over the lazy]~1", 2, 2);

    // test throw exception
    p.setThrowExceptionForEmptyTerm(true);
    p.setNormMultiTerms(NORM_MULTI_TERMS.ANALYZE);

    String[] stopExs = new String[]{
        "the",
        "[the brown]",
        "the brown",
        "(the brown)",
        "\"the brown\"",
        "\"the\"",
        "[the brown]!~2,2",
        "[brown the]!~2,2",
        "the*ter",
        "the?ter"
    };
    
    for (String ex : stopExs) {
      testException(p, ex);
    }

    // add tests for surprise phrasal with stopword!!! chinese

    SpanOnlyParser noStopsParser = new SpanOnlyParser(TEST_VERSION_CURRENT, FIELD, noStopAnalyzer);
    noStopsParser.setAutoGeneratePhraseQueries(true);
    // won't match because stop word was dropped in index
    countSpansDocs(noStopsParser, "\u666E\u6797\u65AF\u987F\u5927\u5B66", 0, 0);
    // won't match for same reason
    countSpansDocs(noStopsParser, "[\u666E\u6797\u65AF\u987F\u5927\u5B66]~2",
        0, 0);

    testOffsetForSingleSpanMatch(noStopsParser,
        "[\u666E \u6797 \u65AF \u987F \u5B66]~2", 6, 0, 6);
  }

  public void testNonWhiteSpaceLanguage() throws Exception {
    SpanOnlyParser noStopsParser = new SpanOnlyParser(TEST_VERSION_CURRENT, FIELD, noStopAnalyzer);

    testOffsetForSingleSpanMatch(noStopsParser, "\u666E", 6, 0, 1);

    countSpansDocs(noStopsParser, "\u666E\u6797", 2, 1);

    countSpansDocs(noStopsParser, "\u666E\u65AF", 2, 1);

    noStopsParser.setAutoGeneratePhraseQueries(true);

    testOffsetForSingleSpanMatch(noStopsParser, "\u666E\u6797", 6, 0, 2);

    // this would have a hit if autogenerate phrase queries = false
    countSpansDocs(noStopsParser, "\u666E\u65AF", 0, 0);

    // treat as "or", this should have two spans
    countSpansDocs(noStopsParser, "\u666E \u65AF", 2, 1);

    // stop word removed at indexing time and non existent here,
    // this is treated as an exact phrase and should not match
    countSpansDocs(noStopsParser, "\u666E\u6797\u65AF\u987F\u5B66", 0, 0);

    // this should be the same as above
    countSpansDocs(noStopsParser, "[\u666E \u6797 \u65AF \u987F \u5B66]~0", 0,
        0);

    // look for the same phrase but allow for some slop; this should have one
    // hit because this will skip the stop word

    testOffsetForSingleSpanMatch(noStopsParser,
        "[\u666E \u6797 \u65AF \u987F \u5B66]~1", 6, 0, 6);

    // This tests the #specialHandlingForSpanNearWithOneComponent
    // this is initially treated as [ [\u666E\u6797\u65AF\u5B66]~>0 ]~2
    // with the special treatment, this is rewritten as
    // [\u666E \u6797 \u65AF \u5B66]~2
    testOffsetForSingleSpanMatch(noStopsParser,
        "[\u666E\u6797\u65AF\u5B66]~2", 6, 0, 6);

    //If someone enters in a space delimited phrase within a phrase,
    //treat it literally. There should be no matches.
    countSpansDocs(noStopsParser, "[[lazy dog] ]~4", 0, 0);

    noStopsParser.setAutoGeneratePhraseQueries(false);

    // characters split into 2 tokens and treated as an "or" query
    countSpansDocs(noStopsParser, "\u666E\u65AF", 2, 1);

    // TODO: Not sure i like how this behaves.
    // this is treated as [(\u666E \u6797 \u65AF \u987F \u5B66)]~1
    // which is then simplified to just: (\u666E \u6797 \u65AF \u987F \u5B66)
    // Probably better to be treated as [\u666E \u6797 \u65AF \u987F \u5B66]~1

    testOffsetForSingleSpanMatch(noStopsParser,
        "[\u666E\u6797\u65AF\u987F\u5B66]~1", 6, 0, 6);

    SpanOnlyParser stopsParser = new SpanOnlyParser(TEST_VERSION_CURRENT, FIELD, stopAnalyzer);
    stopsParser.setAutoGeneratePhraseQueries(true);
    countSpansDocs(stopsParser, "\u666E\u6797\u65AF\u987F\u5927\u5B66", 0, 0);

    // now test for throwing of exception
    stopsParser.setThrowExceptionForEmptyTerm(true);
    boolean exc = false;
    try {
      countSpansDocs(stopsParser, "\u666E\u6797\u65AF\u987F\u5927\u5B66", 0, 0);
    } catch (ParseException e) {
      exc = true;
    }
    assertEquals(true, exc);
  }

  public void testQuotedSingleTerm() throws Exception {
    SpanOnlyParser p = new SpanOnlyParser(TEST_VERSION_CURRENT, FIELD, noStopAnalyzer);

    String[] quoteds = new String[] {
        "/regex/",
        "fuzzy~2",
        "wil*card",
        "wil?card",
        "prefi*"
    };

    for (String q : quoteds) {
      countSpansDocs(p, "\""+q+"\"", 1, 1);
    }

    for (String q : quoteds) {
      countSpansDocs(p, "'"+q+"'", 1, 1);
    }

    countSpansDocs(p, "'single''quote'", 1, 1);
  }

  public void testRangeQueries() throws Exception {
    //TODO: add tests, now fairly well covered by TestSPanQPBasedonQPTestBase
    SpanOnlyParser stopsParser = new SpanOnlyParser(TEST_VERSION_CURRENT, FIELD, noStopAnalyzer);

    countSpansDocs(stopsParser, "ponml [ * TO bird] edcba", 4, 3);
    countSpansDocs(stopsParser, "ponml [ '*' TO bird] edcba", 4, 3);
    countSpansDocs(stopsParser, "ponml [ \"*\" TO bird] edcba", 4, 3);
    countSpansDocs(stopsParser, "ponml [ umbrella TO *] edcba", 7, 3);
    countSpansDocs(stopsParser, "ponml [ umbrella TO '*'] edcba", 0, 0);
    countSpansDocs(stopsParser, "ponml [ umbrella TO \"*\"] edcba", 0, 0);
  }

  public void testRecursion() throws Exception {
    /*
     * For easy reference of expected offsets
     * 
     * 0: eheu 1: fugaces 2: postume 3: postume 4: labuntur 5: anni 6: nec 7:
     * pietas 8: moram 9: rugis 10: et 11: instanti 12: senectae 13: adferet 14:
     * indomitaeque 15: morti
     */
    SpanOnlyParser p = new SpanOnlyParser(TEST_VERSION_CURRENT, FIELD, noStopAnalyzer);

    // String q = "[labunt* [pietas [rug?s senec*]!~2,0 ]~4 adferet]~5";
    // String q = "[pietas [rug?s senec*]!~2,0 ]~4";
    // countSpansDocs(p, q, 1, 1);

    // Span extents end at one more than the actual end, e.g.:
    String q = "fugaces";
    testOffsetForSingleSpanMatch(p, q, 5, 1, 2);

    q = "morti";
    testOffsetForSingleSpanMatch(p, q, 5, 15, 16);

    q = "[labunt* [pietas [rug?s senec*]~2 ]~4 adferet]~2";
    testOffsetForSingleSpanMatch(p, q, 5, 4, 14);

    // not near query for rugis senectae
    q = "[labunt* [pietas [rug?s senec*]!~2 ]~4 adferet]~2";
    countSpansDocs(p, q, 0, 0);

    // not near query for rugis senectae, 0 before or 2 after
    // Have to extend overall distance to 5 because hit for
    // "rug?s senec*" matches only "rug?s" now
    q = "[labunt* [pietas [rug?s senec*]!~2,0 ]~4 adferet]~5";
    testOffsetForSingleSpanMatch(p, q, 5, 4, 14);

    // not near query for rugis senectae, 0 before or 2 intervening
    q = "[labunt* [pietas [rug?s senec*]!~0,2 ]~4 adferet]~5";
    testOffsetForSingleSpanMatch(p, q, 5, 4, 14);

    // not near query for rugis senectae, 0 before or 3 intervening
    q = "[labunt* [pietas [rug?s senec*]!~0,3 ]~4 adferet]~2";
    countSpansDocs(p, q, 0, 0);

    // directionality specified
    q = "[labunt* [pietas [rug?s senec*]~>2 ]~>4 adferet]~>2";
    testOffsetForSingleSpanMatch(p, q, 5, 4, 14);

    // no directionality, query order inverted
    q = "[adferet [ [senec* rug?s ]~2 pietas ]~4 labunt*]~2";
    testOffsetForSingleSpanMatch(p, q, 5, 4, 14);

    // more than one word intervenes btwn rugis and senectae
    q = "[labunt* [pietas [rug?s senec*]~1 ]~4 adferet]~2";
    countSpansDocs(p, q, 0, 0);

    // more than one word intervenes btwn labuntur and pietas
    q = "[labunt* [pietas [rug?s senec*]~2 ]~4 adferet]~1";
    countSpansDocs(p, q, 0, 0);
  }

  private void testException(SpanOnlyParser p, String q) throws Exception{
    boolean ex = false;
    try {
      countSpansDocs(p, q, 3, 2);
    } catch (ParseException e) {
      ex = true;
    }
    assertTrue(q, ex);
  }
  
  private void countSpansDocs(SpanOnlyParser p, String s, int spanCount,
      int docCount) throws Exception {
    SpanQuery q = (SpanQuery)p.parse(s);
    assertEquals("spanCount: " + s, spanCount, countSpans(q));
    assertEquals("docCount: " + s, docCount, countDocs(q));
  }

  private long countSpans(SpanQuery q) throws Exception {
    List<AtomicReaderContext> ctxs = reader.leaves();
    assert (ctxs.size() == 1);
    AtomicReaderContext ctx = ctxs.get(0);
    q = (SpanQuery) q.rewrite(ctx.reader());
    Spans spans = q.getSpans(ctx, null, new HashMap<Term, TermContext>());

    long i = 0;
    while (spans.next()) {
      i++;
    }
    return i;
  }

  private long countDocs(SpanQuery q) throws Exception {
    OpenBitSet docs = new OpenBitSet();
    List<AtomicReaderContext> ctxs = reader.leaves();
    assert (ctxs.size() == 1);
    AtomicReaderContext ctx = ctxs.get(0);
    IndexReaderContext parentCtx = reader.getContext();
    q = (SpanQuery) q.rewrite(ctx.reader());

    Set<Term> qTerms = new HashSet<Term>();
    q.extractTerms(qTerms);
    Map<Term, TermContext> termContexts = new HashMap<Term, TermContext>();

    for (Term t : qTerms) {
      TermContext c = TermContext.build(parentCtx, t);
      termContexts.put(t, c);
    }

    Spans spans = q.getSpans(ctx, null, termContexts);

    while (spans.next()) {
      docs.set(spans.doc());
    }
    long spanDocHits = docs.cardinality();
    // double check with a regular searcher
    TotalHitCountCollector coll = new TotalHitCountCollector();
    searcher.search(q, coll);
    assertEquals(coll.getTotalHits(), spanDocHits);
    return spanDocHits;
  }

  private void testOffsetForSingleSpanMatch(SpanOnlyParser p, String s,
      int trueDocID, int trueSpanStart, int trueSpanEnd) throws Exception {
    SpanQuery q = (SpanQuery)p.parse(s);
    List<AtomicReaderContext> ctxs = reader.leaves();
    assert (ctxs.size() == 1);
    AtomicReaderContext ctx = ctxs.get(0);
    q = (SpanQuery) q.rewrite(ctx.reader());
    Spans spans = q.getSpans(ctx, null, new HashMap<Term, TermContext>());

    int i = 0;
    int spanStart = -1;
    int spanEnd = -1;
    int docID = -1;
    while (spans.next()) {
      spanStart = spans.start();
      spanEnd = spans.end();
      docID = spans.doc();
      i++;
    }
    assertEquals("should only be one matching span", 1, i);
    assertEquals("doc id", trueDocID, docID);
    assertEquals("span start", trueSpanStart, spanStart);
    assertEquals("span end", trueSpanEnd, spanEnd);
  }

  /**
   * Mocks StandardAnalyzer for tokenizing Chinese characters (at least for
   * these test cases into individual tokens).
   * 
   */
  private final static class MockStandardTokenizerFilter extends TokenFilter {
    // Only designed to handle test cases. You may need to modify this
    // if adding new test cases. Note that position increment is hardcoded to be
    // 1!!!
    private final Pattern hackCJKPattern = Pattern
        .compile("([\u5900-\u9899])|([\\p{InBasic_Latin}]+)");
    private List<String> buffer = new LinkedList<String>();

    private final CharTermAttribute termAtt;
    private final PositionIncrementAttribute posIncrAtt;

    public MockStandardTokenizerFilter(TokenStream in) {
      super(in);
      termAtt = addAttribute(CharTermAttribute.class);
      posIncrAtt = addAttribute(PositionIncrementAttribute.class);
    }

    @Override
    public final boolean incrementToken() throws java.io.IOException {
      if (buffer.size() > 0) {
        termAtt.setEmpty().append(buffer.remove(0));
        posIncrAtt.setPositionIncrement(1);
        return true;
      } else {
        boolean next = input.incrementToken();
        if (!next) {
          return false;
        }
        // posIncrAtt.setPositionIncrement(1);
        String text = termAtt.toString();
        Matcher m = hackCJKPattern.matcher(text);
        boolean hasCJK = false;
        while (m.find()) {
          if (m.group(1) != null) {
            hasCJK = true;
            buffer.add(m.group(1));
          } else if (m.group(2) != null) {
            buffer.add(m.group(2));
          }
        }
        if (hasCJK == false) {
          // don't change the position increment, the super class will handle
          // stop words properly
          buffer.clear();
          return true;
        }
        if (buffer.size() > 0) {
          termAtt.setEmpty().append(buffer.remove(0));
          posIncrAtt.setPositionIncrement(1);
        }
        return true;
      }
    }

    @Override
    public void reset() throws IOException {
      super.reset();
    }
  }
}
