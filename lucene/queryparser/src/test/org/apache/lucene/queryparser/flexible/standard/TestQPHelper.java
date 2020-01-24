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
package org.apache.lucene.queryparser.flexible.standard;

import java.io.IOException;
import java.text.DateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
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
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.messages.QueryParserMessages;
import org.apache.lucene.queryparser.flexible.core.nodes.FuzzyQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessorImpl;
import org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessorPipeline;
import org.apache.lucene.queryparser.flexible.messages.MessageImpl;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler;
import org.apache.lucene.queryparser.flexible.standard.nodes.WildcardQueryNode;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.RegExp;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;

/**
 * This test case is a copy of the core Lucene query parser test, it was adapted
 * to use new QueryParserHelper instead of the old query parser.
 * 
 * Tests QueryParser.
 */
// TODO: really this should extend QueryParserTestBase too!
public class TestQPHelper extends LuceneTestCase {

  public static Analyzer qpAnalyzer;

  @BeforeClass
  public static void beforeClass() {
    qpAnalyzer = new QPTestAnalyzer();
  }

  @AfterClass
  public static void afterClass() {
    qpAnalyzer = null;
  }

  public static final class QPTestFilter extends TokenFilter {
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);

    /**
     * Filter which discards the token 'stop' and which expands the token
     * 'phrase' into 'phrase1 phrase2'
     */
    public QPTestFilter(TokenStream in) {
      super(in);
    }

    private boolean inPhrase = false;
    private int savedStart = 0;
    private int savedEnd = 0;

    @Override
    public boolean incrementToken() throws IOException {
      if (inPhrase) {
        inPhrase = false;
        clearAttributes();
        termAtt.setEmpty().append("phrase2");
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

    @Override
    public void reset() throws IOException {
      super.reset();
      this.inPhrase = false;
      this.savedStart = 0;
      this.savedEnd = 0;
    }
  }

  public static final class QPTestAnalyzer extends Analyzer {

    /** Filters MockTokenizer with StopFilter. */
    @Override
    public final TokenStreamComponents createComponents(String fieldName) {
      Tokenizer tokenizer = new MockTokenizer(MockTokenizer.SIMPLE, true);
      return new TokenStreamComponents(tokenizer, new QPTestFilter(tokenizer));
    }
  }

  public static class QPTestParser extends StandardQueryParser {
    public QPTestParser(Analyzer a) {
      ((QueryNodeProcessorPipeline)getQueryNodeProcessor())
          .add(new QPTestParserQueryNodeProcessor());
      this.setAnalyzer(a);

    }

    private static class QPTestParserQueryNodeProcessor extends
        QueryNodeProcessorImpl {

      @Override
      protected QueryNode postProcessNode(QueryNode node)
          throws QueryNodeException {

        return node;

      }

      @Override
      protected QueryNode preProcessNode(QueryNode node)
          throws QueryNodeException {

        if (node instanceof WildcardQueryNode || node instanceof FuzzyQueryNode) {

          throw new QueryNodeException(new MessageImpl(
              QueryParserMessages.EMPTY_MESSAGE));

        }

        return node;

      }

      @Override
      protected List<QueryNode> setChildrenOrder(List<QueryNode> children)
          throws QueryNodeException {

        return children;

      }

    }

  }

  private int originalMaxClauses;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    originalMaxClauses = IndexSearcher.getMaxClauseCount();
  }

  public StandardQueryParser getParser(Analyzer a) throws Exception {
    if (a == null)
      a = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true);
    StandardQueryParser qp = new StandardQueryParser();
    qp.setAnalyzer(a);

    qp.setDefaultOperator(StandardQueryConfigHandler.Operator.OR);

    return qp;

  }

  public Query getQuery(String query, Analyzer a) throws Exception {
    return getParser(a).parse(query, "field");
  }

  public Query getQueryAllowLeadingWildcard(String query, Analyzer a) throws Exception {
    StandardQueryParser parser = getParser(a);
    parser.setAllowLeadingWildcard(true);
    return parser.parse(query, "field");
  }

  public void assertQueryEquals(String query, Analyzer a, String result)
      throws Exception {
    Query q = getQuery(query, a);
    String s = q.toString("field");
    if (!s.equals(result)) {
      fail("Query /" + query + "/ yielded /" + s + "/, expecting /" + result
          + "/");
    }
  }

  public void assertMatchNoDocsQuery(String queryString, Analyzer a) throws Exception {
    assertMatchNoDocsQuery(getQuery(queryString, a));
  }

  public void assertMatchNoDocsQuery(Query query) throws Exception {
    if (query instanceof MatchNoDocsQuery) {
      // good
    } else if (query instanceof BooleanQuery && ((BooleanQuery) query).clauses().size() == 0) {
      // good
    } else {
      fail("expected MatchNoDocsQuery or an empty BooleanQuery but got: " + query);
    }
  }

  public void assertQueryEqualsAllowLeadingWildcard(String query, Analyzer a, String result)
      throws Exception {
    Query q = getQueryAllowLeadingWildcard(query, a);
    String s = q.toString("field");
    if (!s.equals(result)) {
      fail("Query /" + query + "/ yielded /" + s + "/, expecting /" + result
          + "/");
    }
  }

  public void assertQueryEquals(StandardQueryParser qp, String field,
      String query, String result) throws Exception {
    Query q = qp.parse(query, field);
    String s = q.toString(field);
    if (!s.equals(result)) {
      fail("Query /" + query + "/ yielded /" + s + "/, expecting /" + result
          + "/");
    }
  }

  public void assertEscapedQueryEquals(String query, Analyzer a, String result)
      throws Exception {
    String escapedQuery = QueryParserUtil.escape(query);
    if (!escapedQuery.equals(result)) {
      fail("Query /" + query + "/ yielded /" + escapedQuery + "/, expecting /"
          + result + "/");
    }
  }

  public void assertWildcardQueryEquals(String query,
      String result, boolean allowLeadingWildcard) throws Exception {
    StandardQueryParser qp = getParser(null);
    qp.setAllowLeadingWildcard(allowLeadingWildcard);
    Query q = qp.parse(query, "field");
    String s = q.toString("field");
    if (!s.equals(result)) {
      fail("WildcardQuery /" + query + "/ yielded /" + s + "/, expecting /"
          + result + "/");
    }
  }

  public void assertWildcardQueryEquals(String query,
      String result) throws Exception {
    assertWildcardQueryEquals(query, result, false);
  }

  public Query getQueryDOA(String query, Analyzer a) throws Exception {
    if (a == null)
      a = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true);
    StandardQueryParser qp = new StandardQueryParser();
    qp.setAnalyzer(a);
    qp.setDefaultOperator(StandardQueryConfigHandler.Operator.AND);

    return qp.parse(query, "field");

  }

  public void assertQueryEqualsDOA(String query, Analyzer a, String result)
      throws Exception {
    Query q = getQueryDOA(query, a);
    String s = q.toString("field");
    if (!s.equals(result)) {
      fail("Query /" + query + "/ yielded /" + s + "/, expecting /" + result
          + "/");
    }
  }

  public void testConstantScoreAutoRewrite() throws Exception {
    StandardQueryParser qp = new StandardQueryParser(new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false));
    Query q = qp.parse("foo*bar", "field");
    assertTrue(q instanceof WildcardQuery);
    assertEquals(MultiTermQuery.CONSTANT_SCORE_REWRITE, ((MultiTermQuery) q).getRewriteMethod());

    q = qp.parse("foo*", "field");
    assertTrue(q instanceof PrefixQuery);
    assertEquals(MultiTermQuery.CONSTANT_SCORE_REWRITE, ((MultiTermQuery) q).getRewriteMethod());

    q = qp.parse("[a TO z]", "field");
    assertTrue(q instanceof TermRangeQuery);
    assertEquals(MultiTermQuery.CONSTANT_SCORE_REWRITE, ((MultiTermQuery) q).getRewriteMethod());
  }

  public void testCJK() throws Exception {
    // Test Ideographic Space - As wide as a CJK character cell (fullwidth)
    // used google to translate the word "term" to japanese -> ??
    assertQueryEquals("term\u3000term\u3000term", null,
        "term\u0020term\u0020term");
    assertQueryEqualsAllowLeadingWildcard("??\u3000??\u3000??", null, "??\u0020??\u0020??");
  }

  //individual CJK chars as terms, like StandardAnalyzer
  private static class SimpleCJKTokenizer extends Tokenizer {
    private CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

    public SimpleCJKTokenizer() {
      super();
    }

    @Override
    public boolean incrementToken() throws IOException {
      int ch = input.read();
      if (ch < 0)
        return false;
      clearAttributes();
      termAtt.setEmpty().append((char) ch);
      return true;
    }
  }

  private static class SimpleCJKAnalyzer extends Analyzer {
    @Override
    public TokenStreamComponents createComponents(String fieldName) {
      return new TokenStreamComponents(new SimpleCJKTokenizer());
    }
  }
  
  public void testCJKTerm() throws Exception {
    // individual CJK chars as terms
    SimpleCJKAnalyzer analyzer = new SimpleCJKAnalyzer();
    
    BooleanQuery.Builder expected = new BooleanQuery.Builder();
    expected.add(new TermQuery(new Term("field", "中")), BooleanClause.Occur.SHOULD);
    expected.add(new TermQuery(new Term("field", "国")), BooleanClause.Occur.SHOULD);
    assertEquals(expected.build(), getQuery("中国", analyzer));
    
    expected = new BooleanQuery.Builder();
    expected.add(new TermQuery(new Term("field", "中")), BooleanClause.Occur.MUST);
    BooleanQuery.Builder inner = new BooleanQuery.Builder();
    inner.add(new TermQuery(new Term("field", "中")), BooleanClause.Occur.SHOULD);
    inner.add(new TermQuery(new Term("field", "国")), BooleanClause.Occur.SHOULD);
    expected.add(inner.build(), BooleanClause.Occur.MUST);
    assertEquals(expected.build(), getQuery("中 AND 中国", new SimpleCJKAnalyzer()));

  }
  
  public void testCJKBoostedTerm() throws Exception {
    // individual CJK chars as terms
    SimpleCJKAnalyzer analyzer = new SimpleCJKAnalyzer();
    
    BooleanQuery.Builder expectedB = new BooleanQuery.Builder();
    expectedB.add(new TermQuery(new Term("field", "中")), BooleanClause.Occur.SHOULD);
    expectedB.add(new TermQuery(new Term("field", "国")), BooleanClause.Occur.SHOULD);
    Query expected = expectedB.build();
    expected = new BoostQuery(expected, 0.5f);
    assertEquals(expected, getQuery("中国^0.5", analyzer));
  }
  
  public void testCJKPhrase() throws Exception {
    // individual CJK chars as terms
    SimpleCJKAnalyzer analyzer = new SimpleCJKAnalyzer();
    
    PhraseQuery expected = new PhraseQuery("field", "中", "国");
    
    assertEquals(expected, getQuery("\"中国\"", analyzer));
  }
  
  public void testCJKBoostedPhrase() throws Exception {
    // individual CJK chars as terms
    SimpleCJKAnalyzer analyzer = new SimpleCJKAnalyzer();
    
    Query expected = new PhraseQuery("field", "中", "国");
    expected = new BoostQuery(expected, 0.5f);
    
    assertEquals(expected, getQuery("\"中国\"^0.5", analyzer));
  }
  
  public void testCJKSloppyPhrase() throws Exception {
    // individual CJK chars as terms
    SimpleCJKAnalyzer analyzer = new SimpleCJKAnalyzer(); 
    
    PhraseQuery expected = new PhraseQuery(3, "field", "中", "国");
    
    assertEquals(expected, getQuery("\"中国\"~3", analyzer));
  }

  public void testSimple() throws Exception {
    assertQueryEquals("field=a", null, "a");
    assertQueryEquals("\"term germ\"~2", null, "\"term germ\"~2");
    assertQueryEquals("term term term", null, "term term term");
    assertQueryEquals("t�rm term term", new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false),
        "t�rm term term");
    assertQueryEquals("�mlaut", new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false), "�mlaut");

    // FIXME: change MockAnalyzer to not extend CharTokenizer for this test
    //assertQueryEquals("\"\"", new KeywordAnalyzer(), "");
    //assertQueryEquals("foo:\"\"", new KeywordAnalyzer(), "foo:");

    assertQueryEquals("a AND b", null, "+a +b");
    assertQueryEquals("(a AND b)", null, "+a +b");
    assertQueryEquals("c OR (a AND b)", null, "c (+a +b)");

    assertQueryEquals("a AND NOT b", null, "+a -b");

    assertQueryEquals("a AND -b", null, "+a -b");

    assertQueryEquals("a AND !b", null, "+a -b");

    assertQueryEquals("a && b", null, "+a +b");

    assertQueryEquals("a && ! b", null, "+a -b");

    assertQueryEquals("a OR b", null, "a b");
    assertQueryEquals("a || b", null, "a b");

    assertQueryEquals("a OR !b", null, "a -b");

    assertQueryEquals("a OR ! b", null, "a -b");

    assertQueryEquals("a OR -b", null, "a -b");

    assertQueryEquals("+term -term term", null, "+term -term term");
    assertQueryEquals("foo:term AND field:anotherTerm", null,
        "+foo:term +anotherterm");
    assertQueryEquals("term AND \"phrase phrase\"", null,
        "+term +\"phrase phrase\"");
    assertQueryEquals("\"hello there\"", null, "\"hello there\"");
    assertTrue(getQuery("a AND b", null) instanceof BooleanQuery);
    assertTrue(getQuery("hello", null) instanceof TermQuery);
    assertTrue(getQuery("\"hello there\"", null) instanceof PhraseQuery);

    assertQueryEquals("germ term^2.0", null, "germ (term)^2.0");
    assertQueryEquals("(term)^2.0", null, "(term)^2.0");
    assertQueryEquals("(germ term)^2.0", null, "(germ term)^2.0");
    assertQueryEquals("term^2.0", null, "(term)^2.0");
    assertQueryEquals("term^2", null, "(term)^2.0");
    assertQueryEquals("\"germ term\"^2.0", null, "(\"germ term\")^2.0");
    assertQueryEquals("\"term germ\"^2", null, "(\"term germ\")^2.0");

    assertQueryEquals("(foo OR bar) AND (baz OR boo)", null,
        "+(foo bar) +(baz boo)");
    assertQueryEquals("((a OR b) AND NOT c) OR d", null, "(+(a b) -c) d");
    assertQueryEquals("+(apple \"steve jobs\") -(foo bar baz)", null,
        "+(apple \"steve jobs\") -(foo bar baz)");
    assertQueryEquals("+title:(dog OR cat) -author:\"bob dole\"", null,
        "+(title:dog title:cat) -author:\"bob dole\"");

  }

  public void testPunct() throws Exception {
    Analyzer a = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
    assertQueryEquals("a&b", a, "a&b");
    assertQueryEquals("a&&b", a, "a&&b");
    assertQueryEquals(".NET", a, ".NET");
  }

  public void testGroup() throws Exception {
    assertQueryEquals("!(a AND b) OR c", null, "-(+a +b) c");
    assertQueryEquals("!(a AND b) AND c", null, "-(+a +b) +c");
    assertQueryEquals("((a AND b) AND c)", null, "+(+a +b) +c");
    assertQueryEquals("(a AND b) AND c", null, "+(+a +b) +c");
    assertQueryEquals("b !(a AND b)", null, "b -(+a +b)");
    assertQueryEquals("(a AND b)^4 OR c", null, "(+a +b)^4.0 c");
  }

  public void testSlop() throws Exception {

    assertQueryEquals("\"term germ\"~2", null, "\"term germ\"~2");
    assertQueryEquals("\"term germ\"~2 flork", null, "\"term germ\"~2 flork");
    assertQueryEquals("\"term\"~2", null, "term");
    assertQueryEquals("\" \"~2 germ", null, "germ");
    assertQueryEquals("\"term germ\"~2^2", null, "(\"term germ\"~2)^2.0");
  }

  public void testNumber() throws Exception {
    // The numbers go away because SimpleAnalzyer ignores them
    assertMatchNoDocsQuery("3", null);
    assertQueryEquals("term 1.0 1 2", null, "term");
    assertQueryEquals("term term1 term2", null, "term term term");

    Analyzer a = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
    assertQueryEquals("3", a, "3");
    assertQueryEquals("term 1.0 1 2", a, "term 1.0 1 2");
    assertQueryEquals("term term1 term2", a, "term term1 term2");
  }

  public void testLeadingNegation() throws Exception {
    assertQueryEquals("-term", null, "-term");
    assertQueryEquals("!term", null, "-term");
    assertQueryEquals("NOT term", null, "-term");
  }
  
  public void testNegationInParentheses() throws Exception {
   assertQueryEquals("(-a)", null, "-a");
   assertQueryEquals("(!a)", null, "-a");
   assertQueryEquals("(NOT a)", null, "-a");
   assertQueryEquals("a (!b)", null, "a (-b)");
   assertQueryEquals("+a +(!b)", null, "+a +(-b)");
   assertQueryEquals("a AND (!b)", null, "+a +(-b)");
   assertQueryEquals("a (NOT b)", null, "a (-b)");
   assertQueryEquals("a AND (NOT b)", null, "+a +(-b)");
  }
  
  public void testWildcard() throws Exception {
    assertQueryEquals("term*", null, "term*");
    assertQueryEquals("term*^2", null, "(term*)^2.0");
    assertQueryEquals("term~", null, "term~2");
    assertQueryEquals("term~0.7", null, "term~1");

    assertQueryEquals("term~^3", null, "(term~2)^3.0");

    assertQueryEquals("term^3~", null, "(term~2)^3.0");
    assertQueryEquals("term*germ", null, "term*germ");
    assertQueryEquals("term*germ^3", null, "(term*germ)^3.0");

    assertTrue(getQuery("term*", null) instanceof PrefixQuery);
    assertTrue(getQuery("term*^2", null) instanceof BoostQuery);
    assertTrue(((BoostQuery) getQuery("term*^2", null)).getQuery() instanceof PrefixQuery);
    assertTrue(getQuery("term~", null) instanceof FuzzyQuery);
    assertTrue(getQuery("term~0.7", null) instanceof FuzzyQuery);
    FuzzyQuery fq = (FuzzyQuery) getQuery("term~0.7", null);
    assertEquals(1, fq.getMaxEdits());
    assertEquals(FuzzyQuery.defaultPrefixLength, fq.getPrefixLength());
    fq = (FuzzyQuery) getQuery("term~", null);
    assertEquals(2, fq.getMaxEdits());
    assertEquals(FuzzyQuery.defaultPrefixLength, fq.getPrefixLength());

    assertQueryNodeException("term~1.1"); // value > 1, throws exception

    assertTrue(getQuery("term*germ", null) instanceof WildcardQuery);

    /*
     * Tests to see that wild card terms are (or are not) properly lower-cased
     * with propery parser configuration
     */
    // First prefix queries:
    // by default, convert to lowercase:
    assertWildcardQueryEquals("Term*", "term*");
    // explicitly set lowercase:
    assertWildcardQueryEquals("term*", "term*");
    assertWildcardQueryEquals("Term*", "term*");
    assertWildcardQueryEquals("TERM*", "term*");
    // Then 'full' wildcard queries:
    // by default, convert to lowercase:
    assertWildcardQueryEquals("Te?m", "te?m");
    // explicitly set lowercase:
    assertWildcardQueryEquals("te?m", "te?m");
    assertWildcardQueryEquals("Te?m", "te?m");
    assertWildcardQueryEquals("TE?M", "te?m");
    assertWildcardQueryEquals("Te?m*gerM", "te?m*germ");
    // Fuzzy queries:
    assertWildcardQueryEquals("Term~", "term~2");
    // Range queries:

    // TODO: implement this on QueryParser
    // Q0002E_INVALID_SYNTAX_CANNOT_PARSE: Syntax Error, cannot parse '[A TO
    // C]': Lexical error at line 1, column 1. Encountered: "[" (91), after
    // : ""
    assertWildcardQueryEquals("[A TO C]", "[a TO c]");
    // Test suffix queries: first disallow
    expectThrows(QueryNodeException.class, () -> {
      assertWildcardQueryEquals("*Term", "*term");
    });

    expectThrows(QueryNodeException.class, () -> {
      assertWildcardQueryEquals("?Term", "?term");
    });

    // Test suffix queries: then allow
    assertWildcardQueryEquals("*Term", "*term", true);
    assertWildcardQueryEquals("?Term", "?term", true);
  }

  public void testLeadingWildcardType() throws Exception {
    StandardQueryParser qp = getParser(null);
    qp.setAllowLeadingWildcard(true);
    assertEquals(WildcardQuery.class, qp.parse("t*erm*", "field").getClass());
    assertEquals(WildcardQuery.class, qp.parse("?term*", "field").getClass());
    assertEquals(WildcardQuery.class, qp.parse("*term*", "field").getClass());
  }

  public void testQPA() throws Exception {
    assertQueryEquals("term term^3.0 term", qpAnalyzer, "term (term)^3.0 term");
    assertQueryEquals("term stop^3.0 term", qpAnalyzer, "term term");

    assertQueryEquals("term term term", qpAnalyzer, "term term term");
    assertQueryEquals("term +stop term", qpAnalyzer, "term term");
    assertQueryEquals("term -stop term", qpAnalyzer, "term term");

    assertQueryEquals("drop AND (stop) AND roll", qpAnalyzer, "+drop +roll");
    assertQueryEquals("term +(stop) term", qpAnalyzer, "term term");
    assertQueryEquals("term -(stop) term", qpAnalyzer, "term term");

    assertQueryEquals("drop AND stop AND roll", qpAnalyzer, "+drop +roll");
    assertQueryEquals("term phrase term", qpAnalyzer,
        "term (phrase1 phrase2) term");

    assertQueryEquals("term AND NOT phrase term", qpAnalyzer,
        "+term -(phrase1 phrase2) term");

    assertMatchNoDocsQuery("stop^3", qpAnalyzer);
    assertMatchNoDocsQuery("stop", qpAnalyzer);
    assertMatchNoDocsQuery("(stop)^3", qpAnalyzer);
    assertMatchNoDocsQuery("((stop))^3", qpAnalyzer);
    assertMatchNoDocsQuery("(stop^3)", qpAnalyzer);
    assertMatchNoDocsQuery("((stop)^3)", qpAnalyzer);
    assertMatchNoDocsQuery("(stop)", qpAnalyzer);
    assertMatchNoDocsQuery("((stop))", qpAnalyzer);
    assertTrue(getQuery("term term term", qpAnalyzer) instanceof BooleanQuery);
    assertTrue(getQuery("term +stop", qpAnalyzer) instanceof TermQuery);
  }

  public void testRange() throws Exception {
    assertQueryEquals("[ a TO z]", null, "[a TO z]");
    assertEquals(MultiTermQuery.CONSTANT_SCORE_REWRITE, ((TermRangeQuery)getQuery("[ a TO z]", null)).getRewriteMethod());

    StandardQueryParser qp = new StandardQueryParser();
    
    qp.setMultiTermRewriteMethod(MultiTermQuery.SCORING_BOOLEAN_REWRITE);
    assertEquals(MultiTermQuery.SCORING_BOOLEAN_REWRITE,((TermRangeQuery)qp.parse("[ a TO z]", "field")).getRewriteMethod());

    // test open ranges
    assertQueryEquals("[ a TO * ]", null, "[a TO *]");
    assertQueryEquals("[ * TO z ]", null, "[* TO z]");
    assertQueryEquals("[ * TO * ]", null, "[* TO *]");
    
    assertQueryEquals("field>=a", null, "[a TO *]");
    assertQueryEquals("field>a", null, "{a TO *]");
    assertQueryEquals("field<=a", null, "[* TO a]");
    assertQueryEquals("field<a", null, "[* TO a}");
    
    // mixing exclude and include bounds
    assertQueryEquals("{ a TO z ]", null, "{a TO z]");
    assertQueryEquals("[ a TO z }", null, "[a TO z}");
    assertQueryEquals("{ a TO * ]", null, "{a TO *]");
    assertQueryEquals("[ * TO z }", null, "[* TO z}");
    
    assertQueryEquals("[ a TO z ]", null, "[a TO z]");
    assertQueryEquals("{ a TO z}", null, "{a TO z}");
    assertQueryEquals("{ a TO z }", null, "{a TO z}");
    assertQueryEquals("{ a TO z }^2.0", null, "({a TO z})^2.0");
    assertQueryEquals("[ a TO z] OR bar", null, "[a TO z] bar");
    assertQueryEquals("[ a TO z] AND bar", null, "+[a TO z] +bar");
    assertQueryEquals("( bar blar { a TO z}) ", null, "bar blar {a TO z}");
    assertQueryEquals("gack ( bar blar { a TO z}) ", null,
        "gack (bar blar {a TO z})");
  }

  /** for testing DateTools support */
  private String getDate(String s, DateTools.Resolution resolution)
      throws Exception {
    // we use the default Locale since LuceneTestCase randomizes it
    DateFormat df = DateFormat.getDateInstance(DateFormat.SHORT, Locale.getDefault());
    return getDate(df.parse(s), resolution);
  }

  /** for testing DateTools support */
  private String getDate(Date d, DateTools.Resolution resolution) {
    return DateTools.dateToString(d, resolution);
  }
  
  private String escapeDateString(String s) {
    if (s.contains(" ")) {
      return "\"" + s + "\"";
    } else {
      return s;
    }
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
    StandardQueryParser qp = new StandardQueryParser();

    Map<CharSequence, DateTools.Resolution> dateRes =  new HashMap<>();
    
    // set a field specific date resolution    
    dateRes.put(monthField, DateTools.Resolution.MONTH);
    qp.setDateResolutionMap(dateRes);

    // set default date resolution to MILLISECOND
    qp.setDateResolution(DateTools.Resolution.MILLISECOND);

    // set second field specific date resolution
    dateRes.put(hourField, DateTools.Resolution.HOUR);
    qp.setDateResolutionMap(dateRes);

    // for this field no field specific date resolution has been set,
    // so verify if the default resolution is used
    assertDateRangeQueryEquals(qp, defaultField, startDate, endDate,
        endDateExpected.getTime(), DateTools.Resolution.MILLISECOND);

    // verify if field specific date resolutions are used for these two
    // fields
    assertDateRangeQueryEquals(qp, monthField, startDate, endDate,
        endDateExpected.getTime(), DateTools.Resolution.MONTH);

    assertDateRangeQueryEquals(qp, hourField, startDate, endDate,
        endDateExpected.getTime(), DateTools.Resolution.HOUR);
  }

  public void assertDateRangeQueryEquals(StandardQueryParser qp,
      String field, String startDate, String endDate, Date endDateInclusive,
      DateTools.Resolution resolution) throws Exception {
    assertQueryEquals(qp, field, field + ":[" + escapeDateString(startDate) + " TO " + escapeDateString(endDate)
        + "]", "[" + getDate(startDate, resolution) + " TO "
        + getDate(endDateInclusive, resolution) + "]");
    assertQueryEquals(qp, field, field + ":{" + escapeDateString(startDate) + " TO " + escapeDateString(endDate)
        + "}", "{" + getDate(startDate, resolution) + " TO "
        + getDate(endDate, resolution) + "}");
  }

  public void testEscaped() throws Exception {
    Analyzer a = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);

    /*
     * assertQueryEquals("\\[brackets", a, "\\[brackets");
     * assertQueryEquals("\\[brackets", null, "brackets");
     * assertQueryEquals("\\\\", a, "\\\\"); assertQueryEquals("\\+blah", a,
     * "\\+blah"); assertQueryEquals("\\(blah", a, "\\(blah");
     * 
     * assertQueryEquals("\\-blah", a, "\\-blah"); assertQueryEquals("\\!blah",
     * a, "\\!blah"); assertQueryEquals("\\{blah", a, "\\{blah");
     * assertQueryEquals("\\}blah", a, "\\}blah"); assertQueryEquals("\\:blah",
     * a, "\\:blah"); assertQueryEquals("\\^blah", a, "\\^blah");
     * assertQueryEquals("\\[blah", a, "\\[blah"); assertQueryEquals("\\]blah",
     * a, "\\]blah"); assertQueryEquals("\\\"blah", a, "\\\"blah");
     * assertQueryEquals("\\(blah", a, "\\(blah"); assertQueryEquals("\\)blah",
     * a, "\\)blah"); assertQueryEquals("\\~blah", a, "\\~blah");
     * assertQueryEquals("\\*blah", a, "\\*blah"); assertQueryEquals("\\?blah",
     * a, "\\?blah"); //assertQueryEquals("foo \\&\\& bar", a,
     * "foo \\&\\& bar"); //assertQueryEquals("foo \\|| bar", a,
     * "foo \\|| bar"); //assertQueryEquals("foo \\AND bar", a,
     * "foo \\AND bar");
     */

    assertQueryEquals("\\*", a, "*");
    
    assertQueryEquals("\\a", a, "a");

    assertQueryEquals("a\\-b:c", a, "a-b:c");
    assertQueryEquals("a\\+b:c", a, "a+b:c");
    assertQueryEquals("a\\:b:c", a, "a:b:c");
    assertQueryEquals("a\\\\b:c", a, "a\\b:c");

    assertQueryEquals("a:b\\-c", a, "a:b-c");
    assertQueryEquals("a:b\\+c", a, "a:b+c");
    assertQueryEquals("a:b\\:c", a, "a:b:c");
    assertQueryEquals("a:b\\\\c", a, "a:b\\c");

    assertQueryEquals("a:b\\-c*", a, "a:b-c*");
    assertQueryEquals("a:b\\+c*", a, "a:b+c*");
    assertQueryEquals("a:b\\:c*", a, "a:b:c*");

    assertQueryEquals("a:b\\\\c*", a, "a:b\\c*");

    assertQueryEquals("a:b\\-?c", a, "a:b-?c");
    assertQueryEquals("a:b\\+?c", a, "a:b+?c");
    assertQueryEquals("a:b\\:?c", a, "a:b:?c");

    assertQueryEquals("a:b\\\\?c", a, "a:b\\?c");

    assertQueryEquals("a:b\\-c~", a, "a:b-c~2");
    assertQueryEquals("a:b\\+c~", a, "a:b+c~2");
    assertQueryEquals("a:b\\:c~", a, "a:b:c~2");
    assertQueryEquals("a:b\\\\c~", a, "a:b\\c~2");

    // TODO: implement Range queries on QueryParser
    assertQueryEquals("[ a\\- TO a\\+ ]", null, "[a- TO a+]");
    assertQueryEquals("[ a\\: TO a\\~ ]", null, "[a: TO a~]");
    assertQueryEquals("[ a\\\\ TO a\\* ]", null, "[a\\ TO a*]");

    assertQueryEquals(
        "[\"c\\:\\\\temp\\\\\\~foo0.txt\" TO \"c\\:\\\\temp\\\\\\~foo9.txt\"]",
        a, "[c:\\temp\\~foo0.txt TO c:\\temp\\~foo9.txt]");

    assertQueryEquals("a\\\\\\+b", a, "a\\+b");

    assertQueryEquals("a \\\"b c\\\" d", a, "a \"b c\" d");
    assertQueryEquals("\"a \\\"b c\\\" d\"", a, "\"a \"b c\" d\"");
    assertQueryEquals("\"a \\+b c d\"", a, "\"a +b c d\"");

    assertQueryEquals("c\\:\\\\temp\\\\\\~foo.txt", a, "c:\\temp\\~foo.txt");

    assertQueryNodeException("XY\\"); // there must be a character after the
    // escape char

    // test unicode escaping
    assertQueryEquals("a\\u0062c", a, "abc");
    assertQueryEquals("XY\\u005a", a, "XYZ");
    assertQueryEquals("XY\\u005A", a, "XYZ");
    assertQueryEquals("\"a \\\\\\u0028\\u0062\\\" c\"", a, "\"a \\(b\" c\"");

    assertQueryNodeException("XY\\u005G"); // test non-hex character in escaped
    // unicode sequence
    assertQueryNodeException("XY\\u005"); // test incomplete escaped unicode
    // sequence

    // Tests bug LUCENE-800
    assertQueryEquals("(item:\\\\ item:ABCD\\\\)", a, "item:\\ item:ABCD\\");
    assertQueryNodeException("(item:\\\\ item:ABCD\\\\))"); // unmatched closing
    // paranthesis
    assertQueryEquals("\\*", a, "*");
    assertQueryEquals("\\\\", a, "\\"); // escaped backslash

    assertQueryNodeException("\\"); // a backslash must always be escaped

    // LUCENE-1189
    assertQueryEquals("(\"a\\\\\") or (\"b\")", a, "a\\ or b");
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

  @Ignore("flexible queryparser shouldn't escape wildcard terms")
  public void testEscapedWildcard() throws Exception {
    StandardQueryParser qp = new StandardQueryParser();
    qp.setAnalyzer(new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false));

    WildcardQuery q = new WildcardQuery(new Term("field", "foo\\?ba?r"));
    assertEquals(q, qp.parse("foo\\?ba?r", "field"));
  }

  public void testTabNewlineCarriageReturn() throws Exception {
    assertQueryEqualsDOA("+weltbank +worlbank", null, "+weltbank +worlbank");

    assertQueryEqualsDOA("+weltbank\n+worlbank", null, "+weltbank +worlbank");
    assertQueryEqualsDOA("weltbank \n+worlbank", null, "+weltbank +worlbank");
    assertQueryEqualsDOA("weltbank \n +worlbank", null, "+weltbank +worlbank");

    assertQueryEqualsDOA("+weltbank\r+worlbank", null, "+weltbank +worlbank");
    assertQueryEqualsDOA("weltbank \r+worlbank", null, "+weltbank +worlbank");
    assertQueryEqualsDOA("weltbank \r +worlbank", null, "+weltbank +worlbank");

    assertQueryEqualsDOA("+weltbank\r\n+worlbank", null, "+weltbank +worlbank");
    assertQueryEqualsDOA("weltbank \r\n+worlbank", null, "+weltbank +worlbank");
    assertQueryEqualsDOA("weltbank \r\n +worlbank", null, "+weltbank +worlbank");
    assertQueryEqualsDOA("weltbank \r \n +worlbank", null,
        "+weltbank +worlbank");

    assertQueryEqualsDOA("+weltbank\t+worlbank", null, "+weltbank +worlbank");
    assertQueryEqualsDOA("weltbank \t+worlbank", null, "+weltbank +worlbank");
    assertQueryEqualsDOA("weltbank \t +worlbank", null, "+weltbank +worlbank");
  }

  public void testSimpleDAO() throws Exception {
    assertQueryEqualsDOA("term term term", null, "+term +term +term");
    assertQueryEqualsDOA("term +term term", null, "+term +term +term");
    assertQueryEqualsDOA("term term +term", null, "+term +term +term");
    assertQueryEqualsDOA("term +term +term", null, "+term +term +term");
    assertQueryEqualsDOA("-term term term", null, "-term +term +term");
  }

  public void testBoost() throws Exception {
    CharacterRunAutomaton stopSet = new CharacterRunAutomaton(Automata.makeString("on"));
    Analyzer oneStopAnalyzer = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true, stopSet);
    StandardQueryParser qp = new StandardQueryParser();
    qp.setAnalyzer(oneStopAnalyzer);

    Query q = qp.parse("on^1.0", "field");
    assertNotNull(q);
    q = qp.parse("\"hello\"^2.0", "field");
    assertNotNull(q);
    assertEquals(((BoostQuery) q).getBoost(), (float) 2.0, (float) 0.5);
    q = qp.parse("hello^2.0", "field");
    assertNotNull(q);
    assertEquals(((BoostQuery) q).getBoost(), (float) 2.0, (float) 0.5);
    q = qp.parse("\"on\"^1.0", "field");
    assertNotNull(q);

    StandardQueryParser qp2 = new StandardQueryParser();
    qp2.setAnalyzer(new MockAnalyzer(random(), MockTokenizer.SIMPLE, true, MockTokenFilter.ENGLISH_STOPSET));

    q = qp2.parse("the^3", "field");
    // "the" is a stop word so the result is an empty query:
    assertNotNull(q);
    assertMatchNoDocsQuery(q);
    assertFalse(q instanceof BoostQuery);
  }

  public void assertQueryNodeException(String queryString) throws Exception {
    expectThrows(QueryNodeException.class, () -> {
      getQuery(queryString, null);
    });
  }

  public void testException() throws Exception {
    assertQueryNodeException("*leadingWildcard"); // disallowed by default
    assertQueryNodeException("\"some phrase");
    assertQueryNodeException("(foo bar");
    assertQueryNodeException("foo bar))");
    assertQueryNodeException("field:term:with:colon some more terms");
    assertQueryNodeException("(sub query)^5.0^2.0 plus more");
    assertQueryNodeException("secret AND illegal) AND access:confidential");    
  }

  // Wildcard queries should not be allowed
  public void testCustomQueryParserWildcard() {
    expectThrows(QueryNodeException.class, () -> {
      new QPTestParser(new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false)).parse("a?t", "contents");
    });
  }

  // Fuzzy queries should not be allowed"
  public void testCustomQueryParserFuzzy() throws Exception {
    expectThrows(QueryNodeException.class, () -> {
      new QPTestParser(new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false)).parse("xunit~", "contents");
    });
  }

  // too many boolean clauses, so ParseException is expected
  public void testBooleanQuery() throws Exception {
    IndexSearcher.setMaxClauseCount(2);
    expectThrows(QueryNodeException.class, () -> {
      StandardQueryParser qp = new StandardQueryParser();
      qp.setAnalyzer(new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false));

      qp.parse("one two three", "field");
    });
  }

  /**
   * This test differs from TestPrecedenceQueryParser
   */
  public void testPrecedence() throws Exception {
    StandardQueryParser qp = new StandardQueryParser();
    qp.setAnalyzer(new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false));

    Query query1 = qp.parse("A AND B OR C AND D", "field");
    Query query2 = qp.parse("+A +B +C +D", "field");

    assertEquals(query1, query2);
  }

// Todo: Convert from DateField to DateUtil
//  public void testLocalDateFormat() throws IOException, QueryNodeException {
//    Directory ramDir = newDirectory();
//    IndexWriter iw = new IndexWriter(ramDir, newIndexWriterConfig(new MockAnalyzer(random, MockTokenizer.WHITESPACE, false)));
//    addDateDoc("a", 2005, 12, 2, 10, 15, 33, iw);
//    addDateDoc("b", 2005, 12, 4, 22, 15, 00, iw);
//    iw.close();
//    IndexSearcher is = new IndexSearcher(ramDir, true);
//    assertHits(1, "[12/1/2005 TO 12/3/2005]", is);
//    assertHits(2, "[12/1/2005 TO 12/4/2005]", is);
//    assertHits(1, "[12/3/2005 TO 12/4/2005]", is);
//    assertHits(1, "{12/1/2005 TO 12/3/2005}", is);
//    assertHits(1, "{12/1/2005 TO 12/4/2005}", is);
//    assertHits(0, "{12/3/2005 TO 12/4/2005}", is);
//    is.close();
//    ramDir.close();
//  }
//
//  private void addDateDoc(String content, int year, int month, int day,
//                          int hour, int minute, int second, IndexWriter iw) throws IOException {
//    Document d = new Document();
//    d.add(newField("f", content, Field.Store.YES, Field.Index.ANALYZED));
//    Calendar cal = Calendar.getInstance(Locale.ENGLISH);
//    cal.set(year, month - 1, day, hour, minute, second);
//    d.add(newField("date", DateField.dateToString(cal.getTime()),
//        Field.Store.YES, Field.Index.NOT_ANALYZED));
//    iw.addDocument(d);
//  }


  public void testStarParsing() throws Exception {
    // final int[] type = new int[1];
    // StandardQueryParser qp = new StandardQueryParser("field", new
    // WhitespaceAnalyzer()) {
    // protected Query getWildcardQuery(String field, String termStr) throws
    // ParseException {
    // // override error checking of superclass
    // type[0]=1;
    // return new TermQuery(new Term(field,termStr));
    // }
    // protected Query getPrefixQuery(String field, String termStr) throws
    // ParseException {
    // // override error checking of superclass
    // type[0]=2;
    // return new TermQuery(new Term(field,termStr));
    // }
    //
    // protected Query getFieldQuery(String field, String queryText) throws
    // ParseException {
    // type[0]=3;
    // return super.getFieldQuery(field, queryText);
    // }
    // };
    //
    // TermQuery tq;
    //
    // tq = (TermQuery)qp.parse("foo:zoo*");
    // assertEquals("zoo",tq.getTerm().text());
    // assertEquals(2,type[0]);
    //
    // tq = (TermQuery)qp.parse("foo:zoo*^2");
    // assertEquals("zoo",tq.getTerm().text());
    // assertEquals(2,type[0]);
    // assertEquals(tq.getBoost(),2,0);
    //
    // tq = (TermQuery)qp.parse("foo:*");
    // assertEquals("*",tq.getTerm().text());
    // assertEquals(1,type[0]); // could be a valid prefix query in the
    // future too
    //
    // tq = (TermQuery)qp.parse("foo:*^2");
    // assertEquals("*",tq.getTerm().text());
    // assertEquals(1,type[0]);
    // assertEquals(tq.getBoost(),2,0);
    //
    // tq = (TermQuery)qp.parse("*:foo");
    // assertEquals("*",tq.getTerm().field());
    // assertEquals("foo",tq.getTerm().text());
    // assertEquals(3,type[0]);
    //
    // tq = (TermQuery)qp.parse("*:*");
    // assertEquals("*",tq.getTerm().field());
    // assertEquals("*",tq.getTerm().text());
    // assertEquals(1,type[0]); // could be handled as a prefix query in the
    // future
    //
    // tq = (TermQuery)qp.parse("(*:*)");
    // assertEquals("*",tq.getTerm().field());
    // assertEquals("*",tq.getTerm().text());
    // assertEquals(1,type[0]);

  }
  
  public void testRegexps() throws Exception {
    StandardQueryParser qp = new StandardQueryParser();
    qp.setAnalyzer(new MockAnalyzer(random(), MockTokenizer.WHITESPACE, true));
    final String df = "field" ;
    RegexpQuery q = new RegexpQuery(new Term("field", "[a-z][123]"));
    assertEquals(q, qp.parse("/[a-z][123]/", df));
    assertEquals(q, qp.parse("/[A-Z][123]/", df));
    assertEquals(new BoostQuery(q, 0.5f), qp.parse("/[A-Z][123]/^0.5", df));
    qp.setMultiTermRewriteMethod(MultiTermQuery.SCORING_BOOLEAN_REWRITE);
    q.setRewriteMethod(MultiTermQuery.SCORING_BOOLEAN_REWRITE);
    assertEquals(new BoostQuery(q, 0.5f), qp.parse("/[A-Z][123]/^0.5", df));
    assertEquals(MultiTermQuery.SCORING_BOOLEAN_REWRITE, ((RegexpQuery) (((BoostQuery) qp.parse("/[A-Z][123]/^0.5", df)).getQuery())).getRewriteMethod());
    qp.setMultiTermRewriteMethod(MultiTermQuery.CONSTANT_SCORE_REWRITE);
    
    Query escaped = new RegexpQuery(new Term("field", "[a-z]\\/[123]"));
    assertEquals(escaped, qp.parse("/[a-z]\\/[123]/", df));
    Query escaped2 = new RegexpQuery(new Term("field", "[a-z]\\*[123]"));
    assertEquals(escaped2, qp.parse("/[a-z]\\*[123]/", df));
    
    BooleanQuery.Builder complex = new BooleanQuery.Builder();
    complex.add(new RegexpQuery(new Term("field", "[a-z]\\/[123]")), Occur.MUST);
    complex.add(new TermQuery(new Term("path", "/etc/init.d/")), Occur.MUST);
    complex.add(new TermQuery(new Term("field", "/etc/init[.]d/lucene/")), Occur.SHOULD);
    assertEquals(complex.build(), qp.parse("/[a-z]\\/[123]/ AND path:\"/etc/init.d/\" OR \"/etc\\/init\\[.\\]d/lucene/\" ", df));
    
    Query re = new RegexpQuery(new Term("field", "http.*"));
    assertEquals(re, qp.parse("field:/http.*/", df));
    assertEquals(re, qp.parse("/http.*/", df));
    
    re = new RegexpQuery(new Term("field", "http~0.5"));
    assertEquals(re, qp.parse("field:/http~0.5/", df));
    assertEquals(re, qp.parse("/http~0.5/", df));
    
    re = new RegexpQuery(new Term("field", "boo"));
    assertEquals(re, qp.parse("field:/boo/", df));
    assertEquals(re, qp.parse("/boo/", df));
    
    assertEquals(new TermQuery(new Term("field", "/boo/")), qp.parse("\"/boo/\"", df));
    assertEquals(new TermQuery(new Term("field", "/boo/")), qp.parse("\\/boo\\/", df));
    
    BooleanQuery.Builder two = new BooleanQuery.Builder();
    two.add(new RegexpQuery(new Term("field", "foo")), Occur.SHOULD);
    two.add(new RegexpQuery(new Term("field", "bar")), Occur.SHOULD);
    assertEquals(two.build(), qp.parse("field:/foo/ field:/bar/", df));
    assertEquals(two.build(), qp.parse("/foo/ /bar/", df));
  }

  public void testStopwords() throws Exception {
    StandardQueryParser qp = new StandardQueryParser();
    CharacterRunAutomaton stopSet = new CharacterRunAutomaton(new RegExp("the|foo").toAutomaton());
    qp.setAnalyzer(new MockAnalyzer(random(), MockTokenizer.SIMPLE, true, stopSet));

    Query result = qp.parse("a:the OR a:foo", "a");
    assertNotNull("result is null and it shouldn't be", result);
    assertTrue("result is not a MatchNoDocsQuery", result instanceof MatchNoDocsQuery);
    result = qp.parse("a:woo OR a:the", "a");
    assertNotNull("result is null and it shouldn't be", result);
    assertTrue("result is not a TermQuery", result instanceof TermQuery);
    result = qp.parse(
        "(fieldX:xxxxx OR fieldy:xxxxxxxx)^2 AND (fieldx:the OR fieldy:foo)",
        "a");
    Query expected = new BooleanQuery.Builder()
        .add(new TermQuery(new Term("fieldX", "xxxxx")), Occur.SHOULD)
        .add(new TermQuery(new Term("fieldy", "xxxxxxxx")), Occur.SHOULD)
        .build();
    expected = new BoostQuery(expected, 2f);
    assertEquals(expected, result);
  }

  public void testPositionIncrement() throws Exception {
    StandardQueryParser qp = new StandardQueryParser();
    qp.setAnalyzer(
        new MockAnalyzer(random(), MockTokenizer.SIMPLE, true, MockTokenFilter.ENGLISH_STOPSET));

    qp.setEnablePositionIncrements(true);

    String qtxt = "\"the words in poisitions pos02578 are stopped in this phrasequery\"";
    // 0 2 5 7 8
    int expectedPositions[] = { 1, 3, 4, 6, 9 };
    PhraseQuery pq = (PhraseQuery) qp.parse(qtxt, "a");
    // System.out.println("Query text: "+qtxt);
    // System.out.println("Result: "+pq);
    Term t[] = pq.getTerms();
    int pos[] = pq.getPositions();
    for (int i = 0; i < t.length; i++) {
      // System.out.println(i+". "+t[i]+"  pos: "+pos[i]);
      assertEquals("term " + i + " = " + t[i] + " has wrong term-position!",
          expectedPositions[i], pos[i]);
    }
  }

  public void testMatchAllDocs() throws Exception {
    StandardQueryParser qp = new StandardQueryParser();
    qp.setAnalyzer(new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false));

    assertEquals(new MatchAllDocsQuery(), qp.parse("*:*", "field"));
    assertEquals(new MatchAllDocsQuery(), qp.parse("(*:*)", "field"));
    BooleanQuery bq = (BooleanQuery) qp.parse("+*:* -*:*", "field");
    for (BooleanClause c : bq) {
      assertTrue(c.getQuery().getClass() == MatchAllDocsQuery.class);
    }
  }

  private void assertHits(int expected, String query, IndexSearcher is)
      throws IOException, QueryNodeException {
    StandardQueryParser qp = new StandardQueryParser();
    qp.setAnalyzer(new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false));
    qp.setLocale(Locale.ENGLISH);

    Query q = qp.parse(query, "date");
    ScoreDoc[] hits = is.search(q, 1000).scoreDocs;
    assertEquals(expected, hits.length);
  }

  @Override
  public void tearDown() throws Exception {
    IndexSearcher.setMaxClauseCount(originalMaxClauses);
    super.tearDown();
  }

  private static class CannedTokenizer extends Tokenizer {
    private int upto = 0;
    private final PositionIncrementAttribute posIncr = addAttribute(PositionIncrementAttribute.class);
    private final CharTermAttribute term = addAttribute(CharTermAttribute.class);

    public CannedTokenizer() {
      super();
    }

    @Override
    public boolean incrementToken() {
      clearAttributes();
      if (upto == 4) {
        return false;
      }
      if (upto == 0) {
        posIncr.setPositionIncrement(1);
        term.setEmpty().append("a");
      } else if (upto == 1) {
        posIncr.setPositionIncrement(1);
        term.setEmpty().append("b");
      } else if (upto == 2) {
        posIncr.setPositionIncrement(0);
        term.setEmpty().append("c");
      } else {
        posIncr.setPositionIncrement(0);
        term.setEmpty().append("d");
      }
      upto++;
      return true;
    }

    @Override
    public void reset() throws IOException {
      super.reset();
      this.upto = 0;
    }
  }

  private static class CannedAnalyzer extends Analyzer {
    @Override
    public TokenStreamComponents createComponents(String ignored) {
      return new TokenStreamComponents(new CannedTokenizer());
    }
  }

  public void testMultiPhraseQuery() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new CannedAnalyzer()));
    Document doc = new Document();
    doc.add(newTextField("field", "", Field.Store.NO));
    w.addDocument(doc);
    IndexReader r = DirectoryReader.open(w);
    IndexSearcher s = newSearcher(r);
    
    Query q = new StandardQueryParser(new CannedAnalyzer()).parse("\"a\"", "field");
    assertTrue(q instanceof MultiPhraseQuery);
    assertEquals(1, s.search(q, 10).totalHits.value);
    r.close();
    w.close();
    dir.close();
  }

  public void testRegexQueryParsing() throws Exception {
    final String[] fields = {"b", "t"};

    final StandardQueryParser parser = new StandardQueryParser();
    parser.setMultiFields(fields);
    parser.setDefaultOperator(StandardQueryConfigHandler.Operator.AND);
    parser.setAnalyzer(new MockAnalyzer(random()));

    BooleanQuery.Builder exp = new BooleanQuery.Builder();
    exp.add(new BooleanClause(new RegexpQuery(new Term("b", "ab.+")), BooleanClause.Occur.SHOULD));//TODO spezification? was "MUST"
    exp.add(new BooleanClause(new RegexpQuery(new Term("t", "ab.+")), BooleanClause.Occur.SHOULD));//TODO spezification? was "MUST"

    assertEquals(exp.build(), parser.parse("/ab.+/", null));

    RegexpQuery regexpQueryexp = new RegexpQuery(new Term("test", "[abc]?[0-9]"));

    assertEquals(regexpQueryexp, parser.parse("test:/[abc]?[0-9]/", null));

  }

}
