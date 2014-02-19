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

import java.io.IOException;
import java.text.DateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.TimeZone;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParserBase;
import org.apache.lucene.queryparser.classic.QueryParser.Operator;
import org.apache.lucene.queryparser.flexible.standard.CommonQueryParserConfiguration;
import org.apache.lucene.queryparser.spans.SpanQueryParser;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.automaton.BasicAutomata;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.RegExp;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestSpanQPBasedOnQPTestBase extends LuceneTestCase {

  public static Analyzer qpAnalyzer;
  public static String FIELD = "f1";

  @BeforeClass
  public static void beforeClass() {
    qpAnalyzer = new QPTestAnalyzer();
  }

  @AfterClass
  public static void afterClass() {
    qpAnalyzer = null;
  }

  public static final class QPTestFilter extends TokenFilter {
    CharTermAttribute termAtt;
    OffsetAttribute offsetAtt;

    /**
     * Filter which discards the token 'stop' and which expands the
     * token 'phrase' into 'phrase1 phrase2'
     */
    public QPTestFilter(TokenStream in) {
      super(in);
      termAtt = addAttribute(CharTermAttribute.class);
      offsetAtt = addAttribute(OffsetAttribute.class);
    }

    boolean inPhrase = false;
    int savedStart = 0, savedEnd = 0;

    @Override
    public boolean incrementToken() throws IOException {
      if (inPhrase) {
        inPhrase = false;
        clearAttributes();
        termAtt.append("phrase2");
        offsetAtt.setOffset(savedStart, savedEnd);
        return true;
      } else
        while (input.incrementToken()) {
          if (termAtt.toString().equals("phrase")) {
            inPhrase = true;
            savedStart = offsetAtt.startOffset();
            savedEnd = offsetAtt.endOffset();
            termAtt.setEmpty().append("phrase1");
            offsetAtt.setOffset(savedStart, savedEnd);
            return true;
          } else if (!termAtt.toString().equals("stop"))
            return true;
        }
      return false;
    }
  }

  public static final class QPTestAnalyzer extends Analyzer {

    /** Filters MockTokenizer with StopFilter. */
    @Override
    public TokenStreamComponents createComponents(String fieldName) {
      Tokenizer tokenizer = new MockTokenizer(MockTokenizer.SIMPLE, true);
      return new TokenStreamComponents(tokenizer, new QPTestFilter(tokenizer));
    }
  }

  private int originalMaxClauses;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    originalMaxClauses = BooleanQuery.getMaxClauseCount();
  }

  public CommonQueryParserConfiguration getParserConfig(Analyzer a) throws Exception{
    CommonQueryParserConfiguration cqpc = new SpanQueryParser(TEST_VERSION_CURRENT, FIELD, a);
    return cqpc;
  }
  
  public Query getQuery(String query) throws Exception {
    return getQuery(query, (Analyzer)null);
  }

  private Query getQuery(String query, Analyzer analyzer) throws Exception {
    Analyzer a = (analyzer == null) ? qpAnalyzer : analyzer;
    SpanQueryParser p = new SpanQueryParser(TEST_VERSION_CURRENT, FIELD, a);
    return p.parse(query);
  }

  public Query getQuery(String query, CommonQueryParserConfiguration cqpC) throws Exception{
    SpanQueryParser p = (SpanQueryParser)cqpC;
    return p.parse(query);
  }
  
  public void setDateResolution(CommonQueryParserConfiguration cqpC, CharSequence field, DateTools.Resolution value) {
    assert (cqpC instanceof SpanQueryParser);
    ((SpanQueryParser)cqpC).setDateResolution(field.toString(), value);
  }

  private void setAutoGeneratePhraseQueries(CommonQueryParserConfiguration qp,
      boolean b) {
    assert (qp instanceof SpanQueryParser);
    ((SpanQueryParser)qp).setAutoGeneratePhraseQueries(b);
  }

  public void assertQueryEquals(String query, Analyzer a, String result)
      throws Exception {
    Query q = getQuery(query, a);
    String s = q.toString(FIELD);
    if (!s.equals(result)) {
      fail("Query /" + query + "/ yielded /" + s
          + "/, expecting /" + result + "/");
    }
  }

  public void assertQueryEquals(CommonQueryParserConfiguration cqpC, String field, String query, String result) 
      throws Exception {
    Query q = getQuery(query, cqpC);
    String s = q.toString(field);
    if (!s.equals(result)) {
      fail("Query /" + query + "/ yielded /" + s
          + "/, expecting /" + result + "/");
    }
  }
  
  public void assertBoostEquals(String query, float b)
      throws Exception {
    double precision = 0.00001;
    Query q = getQuery(query);
    if (Math.abs(q.getBoost() - b) > precision) {
      fail("Query /" + query + "/ yielded boost:" + q.getBoost()
          + "/, expecting /" + b + "/");
    }
  }

  private void assertEqualsWrappedRegexp(RegexpQuery q, Query query) {
    assertTrue(query instanceof SpanMultiTermQueryWrapper);

    SpanMultiTermQueryWrapper<RegexpQuery> wrapped = new SpanMultiTermQueryWrapper<RegexpQuery>(q);

    assertEquals(wrapped, query);
  }

  public void assertEscapedQueryEquals(String query, Analyzer a, String result)
      throws Exception {
    String escapedQuery = QueryParserBase.escape(query);
    if (!escapedQuery.equals(result)) {
      fail("Query /" + query + "/ yielded /" + escapedQuery
          + "/, expecting /" + result + "/");
    }
  }

  private void assertMultitermEquals(Query query,
      String expected) throws Exception {
    assertMultitermEquals(FIELD, query, expected);
  }
  
  private void assertMultitermEquals(String field, Query query,
      String expected) throws Exception {
    expected = "SpanMultiTermQueryWrapper("+field+":"+ expected+")";

    //need to trim final .0 for fuzzy queries because
    //sometimes they appear in the string and sometimes they don't
    expected = expected.replace(".0)", ")");
    String qString = query.toString();
    //strip off the boost...wasn't appearing in toString in 4.6, but is in trunk
    qString = qString.replaceAll("\\)\\^\\d+\\.\\d+$", ")");
    qString = qString.replace(".0)", ")");
    assertEquals(expected, qString);
  }

  private void assertMultitermEquals(String s,
      String expected) throws Exception {
    assertMultitermEquals(s, qpAnalyzer, expected);
  }

  private void assertMultitermEquals(String s,
      String expected, float boost) throws Exception {
    Analyzer a = qpAnalyzer;
    SpanQueryParser p = new SpanQueryParser(TEST_VERSION_CURRENT, FIELD, a);
    Query q = p.parse(s);
    assertMultitermEquals(q, expected);
    assertEquals(q.getBoost(), boost, 0.000001f);
  }

  private void assertMultitermEquals(String query, boolean b,
      String expected) throws Exception {
    Analyzer a = qpAnalyzer;
    SpanQueryParser p = new SpanQueryParser(TEST_VERSION_CURRENT, FIELD, a);
    p.setLowercaseExpandedTerms(b);
    Query q = p.parse(query);
    assertMultitermEquals(q, expected);
  }

  private void assertMultitermEquals(String field,
      String query, Analyzer a, String expected) throws Exception{
    SpanQueryParser p = new SpanQueryParser(TEST_VERSION_CURRENT, FIELD, a);
    Query q = p.parse(query);
    assertMultitermEquals(field, q, expected);
  }

  private void assertMultitermEquals(String query, Analyzer a, String expected) throws Exception{
    assertMultitermEquals(FIELD, query, a, expected);
  }

  private void assertMultitermEquals(String query, boolean lowercase,
      String expected, boolean allowLeadingWildcard) throws Exception {
    Analyzer a = qpAnalyzer;
    SpanQueryParser p = new SpanQueryParser(TEST_VERSION_CURRENT, FIELD, a);
    p.setLowercaseExpandedTerms(lowercase);
    p.setAllowLeadingWildcard(allowLeadingWildcard);
    Query q = p.parse(query);
    assertMultitermEquals(q, expected);
  }

  private boolean isQueryParserException(Exception pe) {
    if (pe instanceof ParseException) {
      return true;
    }
    return false;
  }
  
  public void testCJK() throws Exception {
    // Test Ideographic Space - As wide as a CJK character cell (fullwidth)
    // used google to translate the word "term" to japanese -> 用語
    assertQueryEquals("term\u3000term\u3000term", null, "term\u0020term\u0020term");
    assertQueryEquals("用語\u3000用語\u3000用語", null, "用語\u0020用語\u0020用語");
  }

  //individual CJK chars as terms, like StandardAnalyzer
  protected static class SimpleCJKTokenizer extends Tokenizer {
    private CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

    public SimpleCJKTokenizer() {
    }

    @Override
    public final boolean incrementToken() throws IOException {
      int ch = input.read();
      if (ch < 0)
        return false;
      clearAttributes();
      termAtt.setEmpty().append((char) ch);
      return true;
    }
  }

  private class SimpleCJKAnalyzer extends Analyzer {
    @Override
    public TokenStreamComponents createComponents(String fieldName) {
      return new TokenStreamComponents(new SimpleCJKTokenizer());
    }
  }

  public void testCJKTerm() throws Exception {
    // individual CJK chars as terms
    SimpleCJKAnalyzer analyzer = new SimpleCJKAnalyzer(); 

    SpanOrQuery expected = new SpanOrQuery(
        new SpanQuery[]{
            new SpanTermQuery(new Term(FIELD, "中")),
            new SpanTermQuery(new Term(FIELD, "国"))
        });

    assertEquals(expected, getQuery("中国", analyzer));
  }

  public void testCJKBoostedTerm() throws Exception {
    // individual CJK chars as terms
    SimpleCJKAnalyzer analyzer = new SimpleCJKAnalyzer();

    SpanOrQuery expected = new SpanOrQuery(
        new SpanQuery[]{
            new SpanTermQuery(new Term(FIELD, "中")),
            new SpanTermQuery(new Term(FIELD, "国"))
        });
    expected.setBoost(0.5f);

    assertEquals(expected, getQuery("中国^0.5", analyzer));
  }

  public void testCJKPhrase() throws Exception {
    // individual CJK chars as terms
    SimpleCJKAnalyzer analyzer = new SimpleCJKAnalyzer();

    SpanNearQuery expected = new SpanNearQuery(
        new SpanQuery[]{
            new SpanTermQuery(new Term(FIELD, "中")),
            new SpanTermQuery(new Term(FIELD, "国"))
        }, 0, true);

    assertEquals(expected, getQuery("\"中国\"", analyzer));
  }

  public void testCJKBoostedPhrase() throws Exception {
    // individual CJK chars as terms
    SimpleCJKAnalyzer analyzer = new SimpleCJKAnalyzer();

    SpanNearQuery expected = new SpanNearQuery(
        new SpanQuery[]{
            new SpanTermQuery(new Term(FIELD, "中")),
            new SpanTermQuery(new Term(FIELD, "国"))
        }, 0, true);
    expected.setBoost(0.5f);
    assertEquals(expected, getQuery("\"中国\"^0.5", analyzer));
  }

  public void testCJKSloppyPhrase() throws Exception {
    // individual CJK chars as terms
    SimpleCJKAnalyzer analyzer = new SimpleCJKAnalyzer();

    SpanNearQuery expected = new SpanNearQuery(
        new SpanQuery[]{
            new SpanTermQuery(new Term(FIELD, "中")),
            new SpanTermQuery(new Term(FIELD, "国"))
        }, 3, false);

    assertEquals(expected, getQuery("\"中国\"~3", analyzer));
  }

  public void testAutoGeneratePhraseQueriesOn() throws Exception {
    // individual CJK chars as terms
    SimpleCJKAnalyzer analyzer = new SimpleCJKAnalyzer(); 

    SpanNearQuery expected = new SpanNearQuery( 
        new SpanTermQuery[]{
            new SpanTermQuery(new Term(FIELD, "中")),
            new SpanTermQuery(new Term(FIELD, "国"))
        }, 0, true);
    CommonQueryParserConfiguration qp = getParserConfig(analyzer);
    setAutoGeneratePhraseQueries(qp, true);
    assertEquals(expected, getQuery("中国",qp));
  }

  public void testSimple() throws Exception {
    assertQueryEquals("term term term", null, "term term term");
    assertQueryEquals("türm term term", new MockAnalyzer(random()), "türm term term");
    assertQueryEquals("ümlaut", new MockAnalyzer(random()), "ümlaut");

    // FIXME: enhance MockAnalyzer to be able to support this
    // it must no longer extend CharTokenizer
    //assertQueryEquals("\"\"", new KeywordAnalyzer(), "");
    //assertQueryEquals("foo:\"\"", new KeywordAnalyzer(), "foo:");

    assertQueryEquals("a AND b", null, "+a +b");
    assertQueryEquals("(a AND b)", null, "+a +b");
    assertQueryEquals("c (a AND b)", null, "c (+a +b)");
    assertQueryEquals("a AND NOT b", null, "+a -b");
    assertQueryEquals("a AND -b", null, "+a -b");

    assertQueryEquals("a b", null, "a b");
    assertQueryEquals("a -b", null, "a -b");

    assertQueryEquals("+term -term term", null, "+term -term term");
    assertQueryEquals("foo:term AND "+FIELD+":anotherTerm", null,
        "+foo:term +anotherterm");
    assertQueryEquals("term AND \"phrase phrase\"", null,
        "+term +spanNear([spanOr([phrase1, phrase2]), "+
        "spanOr([phrase1, phrase2])], 0, true)");
    assertQueryEquals("\"hello there\"", null, "spanNear([hello, there], 0, true)");
    assertTrue(getQuery("a AND b") instanceof BooleanQuery);
    assertTrue(getQuery("hello") instanceof SpanTermQuery);
    assertTrue(getQuery("\"hello there\"") instanceof SpanNearQuery);

    assertQueryEquals("germ term^2.0", null, "germ term^2.0");
    assertQueryEquals("(term)^2.0", null, "term^2.0");
    assertQueryEquals("(germ term)^2.0", null, "(germ term)^2.0");
    assertQueryEquals("term^2.0", null, "term^2.0");
    assertQueryEquals("term^2", null, "term^2.0");
    assertQueryEquals("\"germ term\"^2.0", null, "spanNear([germ, term], 0, true)^2.0");
    assertQueryEquals("\"term germ\"^2", null, "spanNear([term, germ], 0, true)^2.0");

    assertQueryEquals("(foo bar) AND (baz boo)", null,
        "+(foo bar) +(baz boo)");
    assertQueryEquals("((a b) AND NOT c) d", null,
        "(+(a b) -c) d");
    assertQueryEquals("+(apple \"steve jobs\") -(foo bar baz)", null,
        "+(apple spanNear([steve, jobs], 0, true)) -(foo bar baz)");
    assertQueryEquals("+title:(dog cat) -author:\"bob dole\"", null,
        "+(title:dog title:cat) -spanNear([author:bob, author:dole], 0, true)");
  }

  public void testOperatorVsWhitespace() throws Exception { //LUCENE-2566
    // +,-,! should be directly adjacent to operand (i.e. not separated by whitespace) to be treated as an operator
    Analyzer a = new Analyzer() {
      @Override
      public TokenStreamComponents createComponents(String fieldName) {
        return new TokenStreamComponents(new MockTokenizer(MockTokenizer.WHITESPACE, false));
      }
    };
    assertQueryEquals("a - b", a, "a - b");
    assertQueryEquals("a + b", a, "a + b");
    assertQueryEquals("a ! b", a, "a ! b");  
  }

  public void testPunct() throws Exception {
    Analyzer a = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
    assertQueryEquals("a&b", a, "a&b");
    assertQueryEquals("a&&b", a, "a&&b");
    assertQueryEquals(".NET", a, ".NET");
  }

  public void testSlop() throws Exception {
    assertQueryEquals("\"term germ\"~2", null, "spanNear([term, germ], 2, false)");
    assertQueryEquals("\"term germ\"~2 flork", null, "spanNear([term, germ], 2, false) flork");
    assertQueryEquals("\"term\"~2", null, "term");
    assertQueryEquals("\" \"~2 germ", null, "germ");
    assertQueryEquals("\"term germ\"~2^2", null, "spanNear([term, germ], 2, false)^2.0");
  }

  public void testNumber() throws Exception {
    // The numbers go away because SimpleAnalzyer ignores them
    assertQueryEquals("3", null, "spanOr([])");
    assertQueryEquals("term 1.0 1 2", null, "term");
    assertQueryEquals("term term1 term2", null, "term term term");

    Analyzer a = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, true);
    assertQueryEquals("3", a, "3");
    assertQueryEquals("term 1.0 1 2", a, "term 1.0 1 2");
    assertQueryEquals("term term1 term2", a, "term term1 term2");
  }

  public void testWildcard() throws Exception {
    assertMultitermEquals("term*", "term*");

    assertMultitermEquals("term*^2.0","term*", 2.0f);
    assertMultitermEquals("term~", "term~2.0");
    assertMultitermEquals("term~1", "term~1.0");
    assertMultitermEquals("term~0.7","term~1.0");
    assertMultitermEquals("term~^3", "term~2.0", 3.0f);
    //not currently supported in SpanQueryParser
    //     assertWildcardQueryEquals("term^3~", "term~2.0", 3.0f);
    assertMultitermEquals("term*germ", "term*germ");
    assertMultitermEquals("term*germ^3", "term*germ", 3.0f);


    PrefixQuery p = new PrefixQuery(new Term(FIELD, "term"));
    SpanQuery wrapped = new SpanMultiTermQueryWrapper<PrefixQuery>(p);
    assertEquals(getQuery("term*"), wrapped);

    Query parsed = getQuery("term*^2");
    assertMultitermEquals("term*^2", "term*");
    assertEquals(2.0f, parsed.getBoost(), 0.00001f);

    FuzzyQuery f = new FuzzyQuery(new Term(FIELD, "term"), (int)2.0f);
    wrapped = new SpanMultiTermQueryWrapper<FuzzyQuery>(f);

    //not great test; better if we could retrieve wrapped query for testing.
    //don't want to move these tests to SMTQW package.
    assertTrue(getQuery("term~") instanceof SpanMultiTermQueryWrapper);
    assertTrue(getQuery("term~0.7") instanceof SpanMultiTermQueryWrapper);
    /*can't easily test this;
     //FuzzyQuery fq = (FuzzyQuery)getQuery("term~0.7");
     //assertEquals(1, fq.getMaxEdits());


     assertEquals(FuzzyQuery.defaultPrefixLength, fq.getPrefixLength());
     fq = (FuzzyQuery)getQuery("term~");
     assertEquals(2, fq.getMaxEdits());
     assertEquals(FuzzyQuery.defaultPrefixLength, fq.getPrefixLength());
     */
    //not true of SpanQueryParser...rounds value > 1
    //assertParseException("term~1.1"); // value > 1, throws exception

    assertTrue(getQuery("term*germ") instanceof SpanMultiTermQueryWrapper);

    /* Tests to see that wild card terms are (or are not) properly
     * lower-cased with propery parser configuration
     */
    // First prefix queries:
    // by default, convert to lowercase:

    assertMultitermEquals("Term*", true, "term*");
    // explicitly set lowercase:
    assertMultitermEquals("term*", true, "term*");
    assertMultitermEquals("Term*", true, "term*");
    assertMultitermEquals("TERM*", true, "term*");
    // explicitly disable lowercase conversion:
    assertMultitermEquals("term*", false, "term*");
    assertMultitermEquals("Term*", false, "Term*");
    assertMultitermEquals("TERM*", false, "TERM*");
    // Then 'full' wildcard queries:
    // by default, convert to lowercase:
    assertMultitermEquals("Te?m", "te?m");
    // explicitly set lowercase:
    assertMultitermEquals("te?m", true, "te?m");
    assertMultitermEquals("Te?m", true, "te?m");
    assertMultitermEquals("TE?M", true, "te?m");
    assertMultitermEquals("Te?m*gerM", true, "te?m*germ");
    // explicitly disable lowercase conversion:
    assertMultitermEquals("te?m", false, "te?m");
    assertMultitermEquals("Te?m", false, "Te?m");
    assertMultitermEquals("TE?M", false, "TE?M");
    assertMultitermEquals("Te?m*gerM", false, "Te?m*gerM");
    //   Fuzzy queries:
    assertMultitermEquals("Term~", "term~2.0");
    assertMultitermEquals("Term~", true, "term~2.0");
    assertMultitermEquals("Term~", false, "Term~2.0");
    //   Range queries:
    assertMultitermEquals("[A TO C]", "[a TO c]");
    assertMultitermEquals("[A TO C]", true, "[a TO c]");
    assertMultitermEquals("[A TO C]", false, "[A TO C]");


    // Test suffix queries: first disallow
    try {
      assertMultitermEquals("*Term", true, "*term");
    } catch(Exception pe) {
      // expected exception
      if(!isQueryParserException(pe)) {
        fail();
      }
    }
    try {
      assertMultitermEquals("?Term", true, "?term");
      fail();
    } catch(Exception pe) {
      // expected exception
      if(!isQueryParserException(pe)) {
        fail();
      }
    }
    // Test suffix queries: then allow
    assertMultitermEquals("*Term", true, "*term", true);
    assertMultitermEquals("?Term", true, "?term", true);
  }

  public void testLeadingWildcardType() throws Exception {
    CommonQueryParserConfiguration cqpC = getParserConfig(qpAnalyzer);
    cqpC.setAllowLeadingWildcard(true);
    assertEquals(SpanMultiTermQueryWrapper.class, getQuery("t*erm*",cqpC).getClass());
    assertEquals(SpanMultiTermQueryWrapper.class, getQuery("?term*",cqpC).getClass());
    assertEquals(SpanMultiTermQueryWrapper.class, getQuery("*term*",cqpC).getClass());
  }

  public void testQPA() throws Exception {
    assertQueryEquals("term term^3.0 term", qpAnalyzer, "term term^3.0 term");
    assertQueryEquals("term stop^3.0 term", qpAnalyzer, "term term");

    assertQueryEquals("term term term", qpAnalyzer, "term term term");
    assertQueryEquals("term +stop term", qpAnalyzer, "term term");
    assertQueryEquals("term -stop term", qpAnalyzer, "term term");

    assertQueryEquals("drop AND (stop) AND roll", qpAnalyzer, "+drop +roll");
    assertQueryEquals("term +(stop) term", qpAnalyzer, "term term");
    assertQueryEquals("term -(stop) term", qpAnalyzer, "term term");

    assertQueryEquals("drop AND stop AND roll", qpAnalyzer, "+drop +roll");
    assertQueryEquals("term phrase term", qpAnalyzer,
        "term spanOr([phrase1, phrase2]) term");
    assertQueryEquals("term AND NOT phrase term", qpAnalyzer,
        "+term -spanOr([phrase1, phrase2]) term");
    assertQueryEquals("stop^3", qpAnalyzer, "spanOr([])");
    assertQueryEquals("stop", qpAnalyzer, "spanOr([])");
    assertQueryEquals("(stop)^3", qpAnalyzer, "spanOr([])");
    assertQueryEquals("((stop))^3", qpAnalyzer, "spanOr([])");
    assertQueryEquals("(stop^3)", qpAnalyzer, "spanOr([])");
    assertQueryEquals("((stop)^3)", qpAnalyzer, "spanOr([])");
    assertQueryEquals("(stop)", qpAnalyzer, "spanOr([])");
    assertQueryEquals("((stop))", qpAnalyzer, "spanOr([])");
    assertTrue(getQuery("term term term", qpAnalyzer) instanceof BooleanQuery);
    assertTrue(getQuery("term +stop", qpAnalyzer) instanceof SpanTermQuery);
  }

  public void testRange() throws Exception {
    assertQueryEquals("[ a TO z]", null, "SpanMultiTermQueryWrapper([a TO z])");
    assertQueryEquals("[ a TO z}", null, "SpanMultiTermQueryWrapper([a TO z})");
    assertQueryEquals("{ a TO z]", null, "SpanMultiTermQueryWrapper({a TO z])"); 
    assertQueryEquals("{ a TO z}", null, "SpanMultiTermQueryWrapper({a TO z})"); 

    //SQP:not sure what this should be
    //      assertEquals(MultiTermQuery.CONSTANT_SCORE_AUTO_REWRITE_DEFAULT, 
    //          ((SpanMultiTermQueryWrapper)getQuery("[ a TO z]")).getRewriteMethod());
    //TODO: turn back on
    /*
     CommonQueryParserConfiguration qp = getParserConfig( new MockAnalyzer(random(), MockTokenizer.SIMPLE, true));

     qp.setMultiTermRewriteMethod(MultiTermQuery.SCORING_BOOLEAN_QUERY_REWRITE);
     assertEquals(MultiTermQuery.SCORING_BOOLEAN_QUERY_REWRITE,((TermRangeQuery)getQuery("[ a TO z]", qp)).getRewriteMethod());

     // test open ranges
     assertQueryEquals("[ a TO * ]", null, "[a TO *]");
     assertQueryEquals("[ * TO z ]", null, "[* TO z]");
     assertQueryEquals("[ * TO * ]", null, "[* TO *]");
     */   
    // mixing exclude and include bounds
    assertQueryEquals("{ a TO z ]", null, "SpanMultiTermQueryWrapper({a TO z])");
    assertQueryEquals("[ a TO z }", null, "SpanMultiTermQueryWrapper([a TO z})");
    assertQueryEquals("{ a TO * ]", null, "SpanMultiTermQueryWrapper({a TO *])");
    assertQueryEquals("[ * TO z }", null, "SpanMultiTermQueryWrapper([* TO z})");

    assertQueryEquals("[ a TO z ]", null, "SpanMultiTermQueryWrapper([a TO z])");
    assertQueryEquals("{ a TO z}", null, "SpanMultiTermQueryWrapper({a TO z})");
    assertQueryEquals("{ a TO z }", null, "SpanMultiTermQueryWrapper({a TO z})");
    assertQueryEquals("{ a TO z }^2.0", null, "SpanMultiTermQueryWrapper({a TO z})^2.0");
    assertBoostEquals("{ a TO z }^2.0", 2.0f);
    assertQueryEquals("[ a TO z] OR bar", null, "SpanMultiTermQueryWrapper([a TO z]) bar");
    assertQueryEquals("[ a TO z] AND bar", null, "+SpanMultiTermQueryWrapper([a TO z]) +bar");
    assertQueryEquals("( bar blar { a TO z}) ", null, "bar blar SpanMultiTermQueryWrapper({a TO z})");
    assertQueryEquals("gack ( bar blar { a TO z}) ", null, "gack (bar blar SpanMultiTermQueryWrapper({a TO z}))");

    assertQueryEquals("[* TO Z]",null,"SpanMultiTermQueryWrapper([* TO z])");
    assertQueryEquals("[A TO *]",null,"SpanMultiTermQueryWrapper([a TO *])");
    assertQueryEquals("[* TO *]",null,"SpanMultiTermQueryWrapper([* TO *])");
  }

  public void testRangeWithPhrase() throws Exception {
    assertQueryEquals("[\\* TO \"*\"]",null,"SpanMultiTermQueryWrapper([\\* TO \\*])");
    assertQueryEquals("[\"*\" TO *]",null,"SpanMultiTermQueryWrapper([\\* TO *])");
  }

  private String escapeDateString(String s) {
    if (s.indexOf(" ") > -1 || s.indexOf("/") > -1 || s.indexOf("-") > -1) {
      return "\'" + s + "\'";
    } else {
      return s;
    }
  }

  /** for testing DateTools support */
  private String getDate(String s, DateTools.Resolution resolution) throws Exception {
    // we use the default Locale since LuceneTestCase randomizes it
    DateFormat df = DateFormat.getDateInstance(DateFormat.SHORT, Locale.getDefault());
    return getDate(df.parse(s), resolution);      
  }

  /** for testing DateTools support */
  private String getDate(Date d, DateTools.Resolution resolution) {
    return DateTools.dateToString(d, resolution);
  }

  private String getLocalizedDate(int year, int month, int day) {
    // we use the default Locale/TZ since LuceneTestCase randomizes it
    DateFormat df = DateFormat.getDateInstance(DateFormat.SHORT, Locale.getDefault());
    Calendar calendar = new GregorianCalendar(TimeZone.getDefault(), Locale.getDefault());
    calendar.clear();
    calendar.set(year, month, day);
    calendar.set(Calendar.HOUR_OF_DAY, 23);
    calendar.set(Calendar.MINUTE, 59);
    calendar.set(Calendar.SECOND, 59);
    calendar.set(Calendar.MILLISECOND, 999);
    return df.format(calendar.getTime());
  }

  public void testDateRange() throws Exception {
    String startDate = getLocalizedDate(2002, 1, 1);
    String endDate = getLocalizedDate(2002, 1, 4);
    // we use the default Locale/TZ since LuceneTestCase randomizes it
    Calendar endDateExpected = new GregorianCalendar(TimeZone.getDefault(), Locale.getDefault());
    endDateExpected.clear();
    endDateExpected.set(2002, 1, 4, 23, 59, 59);
    endDateExpected.set(Calendar.MILLISECOND, 999);
    final String defaultField = "default";
    final String monthField = "month";
    final String hourField = "hour";
    Analyzer a = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true);
    CommonQueryParserConfiguration qp = getParserConfig(a);

    // set a field specific date resolution
    setDateResolution(qp, monthField, DateTools.Resolution.MONTH);

    // set default date resolution to MILLISECOND
    qp.setDateResolution(DateTools.Resolution.MILLISECOND);

    // set second field specific date resolution    
    setDateResolution(qp, hourField, DateTools.Resolution.HOUR);


    // for this field no field specific date resolution has been set,
    // so verify if the default resolution is used
    assertDateRangeQueryEquals(qp, defaultField, startDate, endDate, 
        endDateExpected.getTime(), DateTools.Resolution.MILLISECOND);

    // verify if field specific date resolutions are used for these two fields
    assertDateRangeQueryEquals(qp, monthField, startDate, endDate, 
        endDateExpected.getTime(), DateTools.Resolution.MONTH);

    assertDateRangeQueryEquals(qp, hourField, startDate, endDate, 
        endDateExpected.getTime(), DateTools.Resolution.HOUR);  
  }

  public void assertDateRangeQueryEquals(CommonQueryParserConfiguration cqpC, String field, String startDate, String endDate, 
      Date endDateInclusive, DateTools.Resolution resolution) throws Exception {

    assertQueryEquals(cqpC, field, field + ":[" + escapeDateString(startDate) + " TO " + escapeDateString(endDate) + "]",
        "SpanMultiTermQueryWrapper([" + getDate(startDate, resolution) + " TO " + getDate(endDateInclusive, resolution) + "])");

    assertQueryEquals(cqpC, field, field + ":{" + escapeDateString(startDate) + " TO " + escapeDateString(endDate) + "}",
        "SpanMultiTermQueryWrapper({" + getDate(startDate, resolution) + " TO " + getDate(endDate, resolution) + "})");
  }

  public void testEscaped() throws Exception {
    Analyzer a = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
    //commented out in QueryParserTestBase
    /* assertQueryEquals("\\[brackets", a, "\\[brackets");
     assertQueryEquals("\\[brackets", null, "brackets");
     assertQueryEquals("\\\\", a, "\\\\");
     assertQueryEquals("\\+blah", a, "\\+blah");
     assertQueryEquals("\\(blah", a, "\\(blah");

     assertQueryEquals("\\-blah", a, "\\-blah");
     assertQueryEquals("\\!blah", a, "\\!blah");
     assertQueryEquals("\\{blah", a, "\\{blah");
     assertQueryEquals("\\}blah", a, "\\}blah");
     assertQueryEquals("\\:blah", a, "\\:blah");
     assertQueryEquals("\\^blah", a, "\\^blah");
     assertQueryEquals("\\[blah", a, "\\[blah");
     assertQueryEquals("\\]blah", a, "\\]blah");
     assertQueryEquals("\\\"blah", a, "\\\"blah");
     assertQueryEquals("\\(blah", a, "\\(blah");
     assertQueryEquals("\\)blah", a, "\\)blah");
     assertQueryEquals("\\~blah", a, "\\~blah");
     assertQueryEquals("\\*blah", a, "\\*blah");
     assertQueryEquals("\\?blah", a, "\\?blah");*/
    //assertQueryEquals("foo \\&\\& bar", a, "foo \\&\\& bar");
    //assertQueryEquals("foo \\|| bar", a, "foo \\|| bar");
    //assertQueryEquals("foo \\AND bar", a, "foo \\AND bar");

    assertQueryEquals("\\a", a, "a");

    assertQueryEquals("a\\-b:c", a, "a-b:c");
    assertQueryEquals("a\\+b:c", a, "a+b:c");
    assertQueryEquals("a\\:b:c", a, "a:b:c");
    assertQueryEquals("a\\\\b:c", a, "a\\b:c");

    assertQueryEquals("a:b\\-c", a, "a:b-c");
    assertQueryEquals("a:b\\+c", a, "a:b+c");
    assertQueryEquals("a:b\\:c", a, "a:b:c");
    assertQueryEquals("a:b\\\\c", a, "a:b\\c");

    assertMultitermEquals("a", "a:b\\-c*", a, "b-c*");
    assertMultitermEquals("a", "a:b\\+c*", a, "b+c*");
    assertMultitermEquals("a", "a:b\\:c*", a, "b:c*");

    assertMultitermEquals("a", "a:b\\\\c*", a, "b\\c*");

    assertMultitermEquals("a", "a:b\\-c~", a, "b-c~2.0");
    assertMultitermEquals("a", "a:b\\+c~", a, "b+c~2.0");
    assertMultitermEquals("a", "a:b\\:c~", a, "b:c~2.0");
    assertMultitermEquals("a", "a:b\\\\c~", a, "b\\c~2.0");

    assertMultitermEquals("[ a\\- TO a\\+ ]", "[a- TO a+]");
    assertMultitermEquals("[ a\\: TO a\\~ ]", "[a: TO a~]");
    assertMultitermEquals("[ a\\\\ TO a\\* ]", "[a\\ TO a*]");

    //change " to ' for spanquery parser
    assertMultitermEquals("['c\\:\\\\temp\\\\\\~foo0.txt' TO 'c\\:\\\\temp\\\\\\~foo9.txt']", a, 
        "[c:\\temp\\~foo0.txt TO c:\\temp\\~foo9.txt]");

    assertQueryEquals("a\\\\\\+b", a, "a\\+b");

    assertQueryEquals("a \\\"b c\\\" d", a, "a \"b c\" d");
    assertQueryEquals("\"a \\\"b c\\\" d\"", a, "spanNear([a, \"b, c\", d], 0, true)");
    assertQueryEquals("\"a \\+b c d\"", a, "spanNear([a, +b, c, d], 0, true)");

    assertQueryEquals("c\\:\\\\temp\\\\\\~foo.txt", a, "c:\\temp\\~foo.txt");

    assertParseException("XY\\"); // there must be a character after the escape char

    // test unicode escaping
    assertQueryEquals("a\\u0062c", a, "abc");
    assertQueryEquals("XY\\u005a", a, "XYZ");
    assertQueryEquals("XY\\u005A", a, "XYZ");
    assertQueryEquals("\"a \\\\\\u0028\\u0062\\\" c\"", a, "spanNear([a, \\(b\", c], 0, true)");

    assertParseException("XY\\u005G");  // test non-hex character in escaped unicode sequence
    assertParseException("XY\\u005");   // test incomplete escaped unicode sequence

    // Tests bug LUCENE-800
    assertQueryEquals("(item:\\\\ item:ABCD\\\\)", a, "item:\\ item:ABCD\\");
    assertParseException("(item:\\\\ item:ABCD\\\\))"); // unmatched closing paranthesis 
    assertQueryEquals("\\*", a, "*");
    assertQueryEquals("\\\\", a, "\\");  // escaped backslash

    assertParseException("\\"); // a backslash must always be escaped

    // LUCENE-1189
    assertQueryEquals("(\"a\\\\\") or (\"b\")", a ,"a\\ or b");

    //now passes actual LUCENE-1189 test with single quotes.
    assertQueryEquals("(name:'///mike\\\\\\\') or (name:\"alphonse\")", a,
        "name:///mike\\\\\\ or name:alphonse");
  }

  public void testEscapedVsQuestionMarkAsWildcard() throws Exception {
    Analyzer a = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
    //SpanMultiTermQueryWrapper(a:b-?c)
    assertMultitermEquals("a", "a:b\\-?c", a, "b\\-?c");
    assertMultitermEquals("a", "a:b\\+?c", a, "b\\+?c");
    assertMultitermEquals("a", "a:b\\:?c", a, "b\\:?c");

    assertMultitermEquals("a", "a:b\\\\?c", a, "b\\\\?c");
  }

  public void testQueryStringEscaping() throws Exception {
    Analyzer a = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);

    assertEscapedQueryEquals("a-b:c", a, "a\\-b\\:c");
    assertEscapedQueryEquals("a+b:c", a, "a\\+b\\:c");
    assertEscapedQueryEquals("a:b:c", a, "a\\:b\\:c");
    assertEscapedQueryEquals("a\\b:c", a, "a\\\\b\\:c");

    assertEscapedQueryEquals("a:b-c", a, "a\\:b\\-c");
    assertEscapedQueryEquals("a:b+c", a, "a\\:b\\+c");
    assertEscapedQueryEquals("a:b:c", a, "a\\:b\\:c");
    assertEscapedQueryEquals("a:b\\c", a, "a\\:b\\\\c");

    assertEscapedQueryEquals("a:b-c*", a, "a\\:b\\-c\\*");
    assertEscapedQueryEquals("a:b+c*", a, "a\\:b\\+c\\*");
    assertEscapedQueryEquals("a:b:c*", a, "a\\:b\\:c\\*");

    assertEscapedQueryEquals("a:b\\\\c*", a, "a\\:b\\\\\\\\c\\*");

    assertEscapedQueryEquals("a:b-?c", a, "a\\:b\\-\\?c");
    assertEscapedQueryEquals("a:b+?c", a, "a\\:b\\+\\?c");
    assertEscapedQueryEquals("a:b:?c", a, "a\\:b\\:\\?c");

    assertEscapedQueryEquals("a:b?c", a, "a\\:b\\?c");

    assertEscapedQueryEquals("a:b-c~", a, "a\\:b\\-c\\~");
    assertEscapedQueryEquals("a:b+c~", a, "a\\:b\\+c\\~");
    assertEscapedQueryEquals("a:b:c~", a, "a\\:b\\:c\\~");
    assertEscapedQueryEquals("a:b\\c~", a, "a\\:b\\\\c\\~");

    assertEscapedQueryEquals("[ a - TO a+ ]", null, "\\[ a \\- TO a\\+ \\]");
    assertEscapedQueryEquals("[ a : TO a~ ]", null, "\\[ a \\: TO a\\~ \\]");
    assertEscapedQueryEquals("[ a\\ TO a* ]", null, "\\[ a\\\\ TO a\\* \\]");

    // LUCENE-881
    assertEscapedQueryEquals("|| abc ||", a, "\\|\\| abc \\|\\|");
    assertEscapedQueryEquals("&& abc &&", a, "\\&\\& abc \\&\\&");
  }

  public void testTabNewlineCarriageReturn()
      throws Exception {
    assertQueryEqualsDOA("+weltbank +worlbank", null,
        "+weltbank +worlbank");

    assertQueryEqualsDOA("+weltbank\n+worlbank", null,
        "+weltbank +worlbank");
    assertQueryEqualsDOA("weltbank \n+worlbank", null,
        "+weltbank +worlbank");
    assertQueryEqualsDOA("weltbank \n +worlbank", null,
        "+weltbank +worlbank");

    assertQueryEqualsDOA("+weltbank\r+worlbank", null,
        "+weltbank +worlbank");
    assertQueryEqualsDOA("weltbank \r+worlbank", null,
        "+weltbank +worlbank");
    assertQueryEqualsDOA("weltbank \r +worlbank", null,
        "+weltbank +worlbank");

    assertQueryEqualsDOA("+weltbank\r\n+worlbank", null,
        "+weltbank +worlbank");
    assertQueryEqualsDOA("weltbank \r\n+worlbank", null,
        "+weltbank +worlbank");
    assertQueryEqualsDOA("weltbank \r\n +worlbank", null,
        "+weltbank +worlbank");
    assertQueryEqualsDOA("weltbank \r \n +worlbank", null,
        "+weltbank +worlbank");

    assertQueryEqualsDOA("+weltbank\t+worlbank", null,
        "+weltbank +worlbank");
    assertQueryEqualsDOA("weltbank \t+worlbank", null,
        "+weltbank +worlbank");
    assertQueryEqualsDOA("weltbank \t +worlbank", null,
        "+weltbank +worlbank");
  }

  public void testSimpleDAO()
      throws Exception {
    assertQueryEqualsDOA("term term term", null, "+term +term +term");
    assertQueryEqualsDOA("term +term term", null, "+term +term +term");
    assertQueryEqualsDOA("term term +term", null, "+term +term +term");
    assertQueryEqualsDOA("term +term +term", null, "+term +term +term");
    assertQueryEqualsDOA("-term term term", null, "-term +term +term");
  }

  public void testBoost()
      throws Exception {
    CharacterRunAutomaton stopWords = new CharacterRunAutomaton(BasicAutomata.makeString("on"));
    Analyzer oneStopAnalyzer = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true, stopWords);
    CommonQueryParserConfiguration qp = getParserConfig(oneStopAnalyzer);
    Query q = getQuery("on^1.0",qp);
    assertNotNull(q);
    q = getQuery("\"hello\"^2.0",qp);
    assertNotNull(q);
    assertEquals(q.getBoost(), (float) 2.0, (float) 0.01);
    q = getQuery("hello^2.0",qp);
    assertNotNull(q);
    assertEquals(q.getBoost(), (float) 2.0, (float) 0.01);
    q = getQuery("\"on\"^1.0",qp);
    assertNotNull(q);

    Analyzer a2 = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true, MockTokenFilter.ENGLISH_STOPSET); 
    CommonQueryParserConfiguration qp2 = getParserConfig(a2);
    q = getQuery("the^3", qp2);
    // "the" is a stop word so the result is an empty query:
    assertNotNull(q);
    assertEquals("spanOr([])", q.toString());
    assertEquals(1.0f, q.getBoost(), 0.01f);
  }

  public void assertParseException(String queryString) throws Exception {
    try {
      getQuery(queryString);
    } catch (Exception expected) {
      if(isQueryParserException(expected)) {
        return;
      }
    }
    fail("ParseException expected, not thrown");
  }

  public void assertParseException(String queryString, Analyzer a) throws Exception {
    try {
      getQuery(queryString, a);
    } catch (Exception expected) {
      if(isQueryParserException(expected)) {
        return;
      }
    }
    fail("ParseException expected, not thrown");
  }

  public void testException() throws Exception {
    assertParseException("\"some phrase");
    assertParseException("(foo bar");
    assertParseException("foo bar))");
    assertParseException("field:term:with:colon some more terms");
    assertParseException("(sub query)^5.0^2.0 plus more");
    assertParseException("secret AND illegal) AND access:confidential");
  }

  public void testBooleanQuery() throws Exception {
    BooleanQuery.setMaxClauseCount(2);
    Analyzer purWhitespaceAnalyzer = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
    assertParseException("one two three", purWhitespaceAnalyzer);
  }

  /**
   * This test differs from TestPrecedenceQueryParser
   */
  public void testPrecedence() throws Exception {
    CommonQueryParserConfiguration qp = getParserConfig(new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false));
    Query query1 = getQuery("A AND B OR C AND D", qp);
    Query query2 = getQuery("+A +B +C +D", qp);
    assertEquals(query1, query2);
  }

  // Todo: convert this from DateField to DateUtil
  //   public void testLocalDateFormat() throws IOException, ParseException {
  //     Directory ramDir = newDirectory();
  //     IndexWriter iw = new IndexWriter(ramDir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random, MockTokenizer.WHITESPACE, false)));
  //     addDateDoc("a", 2005, 12, 2, 10, 15, 33, iw);
  //     addDateDoc("b", 2005, 12, 4, 22, 15, 00, iw);
  //     iw.close();
  //     IndexSearcher is = new IndexSearcher(ramDir, true);
  //     assertHits(1, "[12/1/2005 TO 12/3/2005]", is);
  //     assertHits(2, "[12/1/2005 TO 12/4/2005]", is);
  //     assertHits(1, "[12/3/2005 TO 12/4/2005]", is);
  //     assertHits(1, "{12/1/2005 TO 12/3/2005}", is);
  //     assertHits(1, "{12/1/2005 TO 12/4/2005}", is);
  //     assertHits(0, "{12/3/2005 TO 12/4/2005}", is);
  //     is.close();
  //     ramDir.close();
  //   }
  //
  //   private void addDateDoc(String content, int year, int month,
  //                           int day, int hour, int minute, int second, IndexWriter iw) throws IOException {
  //     Document d = new Document();
  //     d.add(newField(FIELD, content, Field.Store.YES, Field.Index.ANALYZED));
  //     Calendar cal = Calendar.getInstance(Locale.ENGLISH);
  //     cal.set(year, month - 1, day, hour, minute, second);
  //     d.add(newField("date", DateField.dateToString(cal.getTime()), Field.Store.YES, Field.Index.NOT_ANALYZED));
  //     iw.addDocument(d);
  //   }

  //   public abstract void testStarParsing() throws Exception;

  public void testEscapedWildcard() throws Exception {
    CommonQueryParserConfiguration qp = getParserConfig( new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false));
    WildcardQuery q = new WildcardQuery(new Term(FIELD, "foo\\?ba?r"));
    SpanMultiTermQueryWrapper<WildcardQuery> wq = new SpanMultiTermQueryWrapper<WildcardQuery>(q);
    assertEquals(wq, getQuery("foo\\?ba?r", qp));
  }

  public void testRegexps() throws Exception {
    CommonQueryParserConfiguration qp = getParserConfig( new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false));
    RegexpQuery q = new RegexpQuery(new Term(FIELD, "[a-z][123]"));
    assertEqualsWrappedRegexp(q, getQuery("/[a-z][123]/",qp));

    //regexes can't be lowercased with SpanQueryParser
    //qp.setLowercaseExpandedTerms(true);
    assertEqualsWrappedRegexp(q, getQuery("/[a-z][123]/",qp));
    q.setBoost(0.5f);
    assertBoostEquals("/[a-z][123]/^0.5", 0.5f);
    qp.setMultiTermRewriteMethod(MultiTermQuery.SCORING_BOOLEAN_QUERY_REWRITE);
    q.setRewriteMethod(MultiTermQuery.SCORING_BOOLEAN_QUERY_REWRITE);
    assertTrue(getQuery("/[a-z][123]/^0.5",qp) instanceof SpanMultiTermQueryWrapper);
    //    assertEquals(MultiTermQuery.SCORING_BOOLEAN_QUERY_REWRITE, 
    //        ((SpanMultiTermQueryWrapper)getQuery("/[A-Z][123]/^0.5",qp)).getRewriteMethod());
    //     assertEqualsWrappedRegexp(q, getQuery("/[A-Z][123]/^0.5",qp));
    assertBoostEquals("/[a-z][123]/^0.5", 0.5f);

    qp.setMultiTermRewriteMethod(MultiTermQuery.CONSTANT_SCORE_AUTO_REWRITE_DEFAULT);

    SpanMultiTermQueryWrapper<RegexpQuery> escaped = 
        //SQP changed [a-z]\\/[123]  to [a-z]/[123]
        new SpanMultiTermQueryWrapper<RegexpQuery>(new RegexpQuery(new Term(FIELD, "[a-z]/[123]")));

    assertEquals(escaped, getQuery("/[a-z]\\/[123]/",qp));
    SpanMultiTermQueryWrapper<RegexpQuery> escaped2 = 
        new SpanMultiTermQueryWrapper<RegexpQuery>(new RegexpQuery(new Term(FIELD, "[a-z]\\*[123]")));
    assertEquals(escaped2, getQuery("/[a-z]\\*[123]/",qp));

    BooleanQuery complex = new BooleanQuery();
    complex.add(new SpanMultiTermQueryWrapper<RegexpQuery>(
        new RegexpQuery(new Term(FIELD, "[a-z]/[123]"))), Occur.MUST);
    complex.add(new SpanTermQuery(new Term("path", "/etc/init.d/")), Occur.MUST);
    complex.add(new SpanTermQuery(new Term(FIELD, "/etc/init[.]d/lucene/")), Occur.SHOULD);
    //   assertEquals(complex, getQuery("/[a-z]\\/[123]/ AND path:\"/etc/init.d/\" OR \"/etc\\/init\\[.\\]d/lucene/\" ",qp));
    assertEquals(complex, getQuery("/[a-z]\\/[123]/ AND path:\\/etc\\/init.d\\/ OR \\/etc\\/init\\[.\\]d/lucene\\/ ",qp));

    Query re = new SpanMultiTermQueryWrapper<RegexpQuery>(new RegexpQuery(new Term(FIELD, "http.*")));
    assertEquals(re, getQuery(FIELD+":/http.*/",qp));
    assertEquals(re, getQuery("/http.*/",qp));

    re = new SpanMultiTermQueryWrapper<RegexpQuery>(new RegexpQuery(new Term(FIELD, "http~0.5")));
    assertEquals(re, getQuery(FIELD+":/http~0.5/",qp));
    assertEquals(re, getQuery("/http~0.5/",qp));

    re = new SpanMultiTermQueryWrapper<RegexpQuery>(new RegexpQuery(new Term(FIELD, "boo")));
    assertEquals(re, getQuery(FIELD+":/boo/",qp));
    assertEquals(re, getQuery("/boo/",qp));

    //     assertEquals(new SpanTermQuery(new Term(FIELD, "/boo/")), getQuery("\"/boo/\"",qp));
    assertEquals(new SpanTermQuery(new Term(FIELD, "/boo/")), getQuery("\\/boo\\/",qp));

    BooleanQuery two = new BooleanQuery();
    two.add(new SpanMultiTermQueryWrapper<RegexpQuery>(new RegexpQuery(new Term(FIELD, "foo"))), Occur.SHOULD);
    two.add(new SpanMultiTermQueryWrapper<RegexpQuery>(new RegexpQuery(new Term(FIELD, "bar"))), Occur.SHOULD);
    assertEquals(two, getQuery(FIELD+":/foo/ "+FIELD+":/bar/",qp));
    assertEquals(two, getQuery("/foo/ /bar/",qp));
  }

  public void testStopwords() throws Exception {
    CharacterRunAutomaton stopSet = new CharacterRunAutomaton(new RegExp("the|foo").toAutomaton());
    CommonQueryParserConfiguration qp = getParserConfig(new MockAnalyzer(random(), MockTokenizer.SIMPLE, true, stopSet));
    Query result = getQuery("field:the OR field:foo",qp);
    assertNotNull("result is null and it shouldn't be", result);
    assertTrue("result is not a BooleanQuery", result instanceof SpanOrQuery);
    assertEquals(0, ((SpanOrQuery)result).getClauses().length);
    result = getQuery("field:woo OR field:the",qp);
    assertNotNull("result is null and it shouldn't be", result);
    assertTrue("result is not a TermQuery", result instanceof SpanTermQuery);
    result = getQuery("(fieldX:xxxxx OR fieldy:xxxxxxxx)^2 AND (fieldx:the OR fieldy:foo)",qp);
    assertNotNull("result is null and it shouldn't be", result);
    assertTrue("result is not a BooleanQuery", result instanceof BooleanQuery);
    if (VERBOSE) System.out.println("Result: " + result);
    assertTrue(((BooleanQuery) result).clauses().size() + " does not equal: " + 2, ((BooleanQuery) result).clauses().size() == 2);
  }

  public void testPositionIncrement() throws Exception {
    //For SQP, this only tests whether stop words have been dropped.
    //PositionIncrements are not available in SpanQueries yet.
    CommonQueryParserConfiguration qp = getParserConfig( new MockAnalyzer(random(), MockTokenizer.SIMPLE, true, MockTokenFilter.ENGLISH_STOPSET));
    //qp.setEnablePositionIncrements(true);
    String qtxt = "\"the words in poisitions pos02578 are stopped in this phrasequery\"";
    //               0         2                      5           7  8
    SpanNearQuery pq = (SpanNearQuery) getQuery(qtxt,qp);
    //System.out.println("Query text: "+qtxt);
    //System.out.println("Result: "+pq);
    SpanQuery[] clauses = pq.getClauses();
    assertEquals(clauses.length, 5);
    Set<Term> expected = new HashSet<Term>();
    expected.add(new Term(FIELD, "words"));
    expected.add(new Term(FIELD, "poisitions"));
    expected.add(new Term(FIELD, "pos"));
    expected.add(new Term(FIELD, "stopped"));
    expected.add(new Term(FIELD, "phrasequery"));

    Set<Term> terms = new HashSet<Term>();
    for (int i = 0; i < clauses.length; i++) {
      SpanQuery q = clauses[i];
      q.extractTerms(terms);
    }
    assertEquals(expected, terms);
  }

  public void testMatchAllDocs() throws Exception {
    CommonQueryParserConfiguration qp = getParserConfig( new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false));
    assertEquals(new MatchAllDocsQuery(), getQuery("*:*",qp));
    assertEquals(new MatchAllDocsQuery(), getQuery("(*:*)",qp));
    BooleanQuery bq = (BooleanQuery)getQuery("+*:* -*:*",qp);
    assertTrue(bq.getClauses()[0].getQuery() instanceof MatchAllDocsQuery);
    assertTrue(bq.getClauses()[1].getQuery() instanceof MatchAllDocsQuery);
  }

  @Override
  public void tearDown() throws Exception {
    BooleanQuery.setMaxClauseCount(originalMaxClauses);
    super.tearDown();
  }

  public Query getQueryDOA(String query, Analyzer a)
      throws Exception {
    if (a == null)
      a = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true);
    CommonQueryParserConfiguration qp = getParserConfig(a);
    setDefaultOperatorAND(qp);
    return getQuery(query, qp);
  }

  private void setDefaultOperatorAND(CommonQueryParserConfiguration qp) {
    ((SpanQueryParser)qp).setDefaultOperator(Operator.AND);
  }

  public void assertQueryEqualsDOA(String query, Analyzer a, String result)
      throws Exception {
    Query q = getQueryDOA(query, a);
    String s = q.toString(FIELD);
    if (!s.equals(result)) {
      fail("Query /" + query + "/ yielded /" + s
          + "/, expecting /" + result + "/");
    }
  }
}
