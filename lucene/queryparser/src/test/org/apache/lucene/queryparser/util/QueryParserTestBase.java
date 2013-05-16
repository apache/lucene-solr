package org.apache.lucene.queryparser.util;

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
import java.io.Reader;
import java.text.DateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.TimeZone;

import org.apache.lucene.analysis.*;
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
//import org.apache.lucene.queryparser.classic.CharStream;
//import org.apache.lucene.queryparser.classic.ParseException;
//import org.apache.lucene.queryparser.classic.QueryParser;
//import org.apache.lucene.queryparser.classic.QueryParserBase;
//import org.apache.lucene.queryparser.classic.QueryParserTokenManager;
import org.apache.lucene.queryparser.classic.QueryParserBase;
import org.apache.lucene.queryparser.flexible.standard.CommonQueryParserConfiguration;
import org.apache.lucene.search.*;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.automaton.BasicAutomata;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.RegExp;

/**
 * Base Test class for QueryParser subclasses
 */
// TODO: it would be better to refactor the parts that are specific really
// to the core QP and subclass/use the parts that are not in the flexible QP
public abstract class QueryParserTestBase extends LuceneTestCase {
  
  public static Analyzer qpAnalyzer = new QPTestAnalyzer();

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
    public TokenStreamComponents createComponents(String fieldName, Reader reader) {
      Tokenizer tokenizer = new MockTokenizer(reader, MockTokenizer.SIMPLE, true);
      return new TokenStreamComponents(tokenizer, new QPTestFilter(tokenizer));
    }
  }

  private int originalMaxClauses;
  
  private String defaultField = "field";
  
  protected String getDefaultField(){
    return defaultField;
  }

  protected void setDefaultField(String defaultField){
    this.defaultField = defaultField;
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    originalMaxClauses = BooleanQuery.getMaxClauseCount();
  }

  public abstract CommonQueryParserConfiguration getParserConfig(Analyzer a) throws Exception;

  public abstract void setDefaultOperatorOR(CommonQueryParserConfiguration cqpC);

  public abstract void setDefaultOperatorAND(CommonQueryParserConfiguration cqpC);

  public abstract void setAnalyzeRangeTerms(CommonQueryParserConfiguration cqpC, boolean value);

  public abstract void setAutoGeneratePhraseQueries(CommonQueryParserConfiguration cqpC, boolean value);

  public abstract void setDateResolution(CommonQueryParserConfiguration cqpC, CharSequence field, DateTools.Resolution value);

  public abstract Query getQuery(String query, CommonQueryParserConfiguration cqpC) throws Exception;

  public abstract Query getQuery(String query, Analyzer a) throws Exception;
  
  public abstract boolean isQueryParserException(Exception exception);

  public Query getQuery(String query) throws Exception {
    return getQuery(query, (Analyzer)null);
  }

  public void assertQueryEquals(String query, Analyzer a, String result)
    throws Exception {
    Query q = getQuery(query, a);
    String s = q.toString("field");
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
  
  public void assertEscapedQueryEquals(String query, Analyzer a, String result)
    throws Exception {
    String escapedQuery = QueryParserBase.escape(query);
    if (!escapedQuery.equals(result)) {
      fail("Query /" + query + "/ yielded /" + escapedQuery
          + "/, expecting /" + result + "/");
    }
  }

  public void assertWildcardQueryEquals(String query, boolean lowercase, String result, boolean allowLeadingWildcard)
    throws Exception {
    CommonQueryParserConfiguration cqpC = getParserConfig(null);
    cqpC.setLowercaseExpandedTerms(lowercase);
    cqpC.setAllowLeadingWildcard(allowLeadingWildcard);
    Query q = getQuery(query, cqpC);
    String s = q.toString("field");
    if (!s.equals(result)) {
      fail("WildcardQuery /" + query + "/ yielded /" + s
           + "/, expecting /" + result + "/");
    }
  }

  public void assertWildcardQueryEquals(String query, boolean lowercase, String result)
    throws Exception {
    assertWildcardQueryEquals(query, lowercase, result, false);
  }

  public void assertWildcardQueryEquals(String query, String result) throws Exception {
    Query q = getQuery(query);
    String s = q.toString("field");
    if (!s.equals(result)) {
      fail("WildcardQuery /" + query + "/ yielded /" + s + "/, expecting /"
          + result + "/");
    }
  }

  public Query getQueryDOA(String query, Analyzer a)
    throws Exception {
    if (a == null)
      a = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true);
    CommonQueryParserConfiguration qp = getParserConfig(a);
    setDefaultOperatorAND(qp);
    return getQuery(query, qp);
  }

  public void assertQueryEqualsDOA(String query, Analyzer a, String result)
    throws Exception {
    Query q = getQueryDOA(query, a);
    String s = q.toString("field");
    if (!s.equals(result)) {
      fail("Query /" + query + "/ yielded /" + s
           + "/, expecting /" + result + "/");
    }
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

    public SimpleCJKTokenizer(Reader input) {
      super(input);
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
    public TokenStreamComponents createComponents(String fieldName, Reader reader) {
      return new TokenStreamComponents(new SimpleCJKTokenizer(reader));
    }
  }

  public void testCJKTerm() throws Exception {
    // individual CJK chars as terms
    SimpleCJKAnalyzer analyzer = new SimpleCJKAnalyzer(); 
    
    BooleanQuery expected = new BooleanQuery();
    expected.add(new TermQuery(new Term("field", "中")), BooleanClause.Occur.SHOULD);
    expected.add(new TermQuery(new Term("field", "国")), BooleanClause.Occur.SHOULD);
    
    assertEquals(expected, getQuery("中国", analyzer));
  }
  
  public void testCJKBoostedTerm() throws Exception {
    // individual CJK chars as terms
    SimpleCJKAnalyzer analyzer = new SimpleCJKAnalyzer();
    
    BooleanQuery expected = new BooleanQuery();
    expected.setBoost(0.5f);
    expected.add(new TermQuery(new Term("field", "中")), BooleanClause.Occur.SHOULD);
    expected.add(new TermQuery(new Term("field", "国")), BooleanClause.Occur.SHOULD);
    
    assertEquals(expected, getQuery("中国^0.5", analyzer));
  }
  
  public void testCJKPhrase() throws Exception {
    // individual CJK chars as terms
    SimpleCJKAnalyzer analyzer = new SimpleCJKAnalyzer();
    
    PhraseQuery expected = new PhraseQuery();
    expected.add(new Term("field", "中"));
    expected.add(new Term("field", "国"));
    
    assertEquals(expected, getQuery("\"中国\"", analyzer));
  }
  
  public void testCJKBoostedPhrase() throws Exception {
    // individual CJK chars as terms
    SimpleCJKAnalyzer analyzer = new SimpleCJKAnalyzer();
    
    PhraseQuery expected = new PhraseQuery();
    expected.setBoost(0.5f);
    expected.add(new Term("field", "中"));
    expected.add(new Term("field", "国"));
    
    assertEquals(expected, getQuery("\"中国\"^0.5", analyzer));
  }
  
  public void testCJKSloppyPhrase() throws Exception {
    // individual CJK chars as terms
    SimpleCJKAnalyzer analyzer = new SimpleCJKAnalyzer();
    
    PhraseQuery expected = new PhraseQuery();
    expected.setSlop(3);
    expected.add(new Term("field", "中"));
    expected.add(new Term("field", "国"));
    
    assertEquals(expected, getQuery("\"中国\"~3", analyzer));
  }
  
  public void testAutoGeneratePhraseQueriesOn() throws Exception {
    // individual CJK chars as terms
    SimpleCJKAnalyzer analyzer = new SimpleCJKAnalyzer(); 
  
    PhraseQuery expected = new PhraseQuery();
    expected.add(new Term("field", "中"));
    expected.add(new Term("field", "国"));
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
    assertQueryEquals("c OR (a AND b)", null, "c (+a +b)");
    assertQueryEquals("a AND NOT b", null, "+a -b");
    assertQueryEquals("a AND -b", null, "+a -b");
    assertQueryEquals("a AND !b", null, "+a -b");
    assertQueryEquals("a && b", null, "+a +b");
//    assertQueryEquals("a && ! b", null, "+a -b");

    assertQueryEquals("a OR b", null, "a b");
    assertQueryEquals("a || b", null, "a b");
    assertQueryEquals("a OR !b", null, "a -b");
//    assertQueryEquals("a OR ! b", null, "a -b");
    assertQueryEquals("a OR -b", null, "a -b");

    assertQueryEquals("+term -term term", null, "+term -term term");
    assertQueryEquals("foo:term AND field:anotherTerm", null,
                      "+foo:term +anotherterm");
    assertQueryEquals("term AND \"phrase phrase\"", null,
                      "+term +\"phrase phrase\"");
    assertQueryEquals("\"hello there\"", null, "\"hello there\"");
    assertTrue(getQuery("a AND b") instanceof BooleanQuery);
    assertTrue(getQuery("hello") instanceof TermQuery);
    assertTrue(getQuery("\"hello there\"") instanceof PhraseQuery);

    assertQueryEquals("germ term^2.0", null, "germ term^2.0");
    assertQueryEquals("(term)^2.0", null, "term^2.0");
    assertQueryEquals("(germ term)^2.0", null, "(germ term)^2.0");
    assertQueryEquals("term^2.0", null, "term^2.0");
    assertQueryEquals("term^2", null, "term^2.0");
    assertQueryEquals("\"germ term\"^2.0", null, "\"germ term\"^2.0");
    assertQueryEquals("\"term germ\"^2", null, "\"term germ\"^2.0");

    assertQueryEquals("(foo OR bar) AND (baz OR boo)", null,
                      "+(foo bar) +(baz boo)");
    assertQueryEquals("((a OR b) AND NOT c) OR d", null,
                      "(+(a b) -c) d");
    assertQueryEquals("+(apple \"steve jobs\") -(foo bar baz)", null,
                      "+(apple \"steve jobs\") -(foo bar baz)");
    assertQueryEquals("+title:(dog OR cat) -author:\"bob dole\"", null,
                      "+(title:dog title:cat) -author:\"bob dole\"");
    
  }

  public abstract void testDefaultOperator() throws Exception;
  
  
  public void testOperatorVsWhitespace() throws Exception { //LUCENE-2566
    // +,-,! should be directly adjacent to operand (i.e. not separated by whitespace) to be treated as an operator
    Analyzer a = new Analyzer() {
      @Override
      public TokenStreamComponents createComponents(String fieldName, Reader reader) {
        return new TokenStreamComponents(new MockTokenizer(reader, MockTokenizer.WHITESPACE, false));
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
    assertQueryEquals("\"term germ\"~2", null, "\"term germ\"~2");
    assertQueryEquals("\"term germ\"~2 flork", null, "\"term germ\"~2 flork");
    assertQueryEquals("\"term\"~2", null, "term");
    assertQueryEquals("\" \"~2 germ", null, "germ");
    assertQueryEquals("\"term germ\"~2^2", null, "\"term germ\"~2^2.0");
  }

  public void testNumber() throws Exception {
// The numbers go away because SimpleAnalzyer ignores them
    assertQueryEquals("3", null, "");
    assertQueryEquals("term 1.0 1 2", null, "term");
    assertQueryEquals("term term1 term2", null, "term term term");

    Analyzer a = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, true);
    assertQueryEquals("3", a, "3");
    assertQueryEquals("term 1.0 1 2", a, "term 1.0 1 2");
    assertQueryEquals("term term1 term2", a, "term term1 term2");
  }

  public void testWildcard() throws Exception {
    assertQueryEquals("term*", null, "term*");
    assertQueryEquals("term*^2", null, "term*^2.0");
    assertQueryEquals("term~", null, "term~2");
    assertQueryEquals("term~1", null, "term~1");
    assertQueryEquals("term~0.7", null, "term~1");
    assertQueryEquals("term~^3", null, "term~2^3.0");
    assertQueryEquals("term^3~", null, "term~2^3.0");
    assertQueryEquals("term*germ", null, "term*germ");
    assertQueryEquals("term*germ^3", null, "term*germ^3.0");

    assertTrue(getQuery("term*") instanceof PrefixQuery);
    assertTrue(getQuery("term*^2") instanceof PrefixQuery);
    assertTrue(getQuery("term~") instanceof FuzzyQuery);
    assertTrue(getQuery("term~0.7") instanceof FuzzyQuery);
    FuzzyQuery fq = (FuzzyQuery)getQuery("term~0.7");
    assertEquals(1, fq.getMaxEdits());
    assertEquals(FuzzyQuery.defaultPrefixLength, fq.getPrefixLength());
    fq = (FuzzyQuery)getQuery("term~");
    assertEquals(2, fq.getMaxEdits());
    assertEquals(FuzzyQuery.defaultPrefixLength, fq.getPrefixLength());
    
    assertParseException("term~1.1"); // value > 1, throws exception

    assertTrue(getQuery("term*germ") instanceof WildcardQuery);

/* Tests to see that wild card terms are (or are not) properly
   * lower-cased with propery parser configuration
   */
// First prefix queries:
    // by default, convert to lowercase:
    assertWildcardQueryEquals("Term*", true, "term*");
    // explicitly set lowercase:
    assertWildcardQueryEquals("term*", true, "term*");
    assertWildcardQueryEquals("Term*", true, "term*");
    assertWildcardQueryEquals("TERM*", true, "term*");
    // explicitly disable lowercase conversion:
    assertWildcardQueryEquals("term*", false, "term*");
    assertWildcardQueryEquals("Term*", false, "Term*");
    assertWildcardQueryEquals("TERM*", false, "TERM*");
// Then 'full' wildcard queries:
    // by default, convert to lowercase:
    assertWildcardQueryEquals("Te?m", "te?m");
    // explicitly set lowercase:
    assertWildcardQueryEquals("te?m", true, "te?m");
    assertWildcardQueryEquals("Te?m", true, "te?m");
    assertWildcardQueryEquals("TE?M", true, "te?m");
    assertWildcardQueryEquals("Te?m*gerM", true, "te?m*germ");
    // explicitly disable lowercase conversion:
    assertWildcardQueryEquals("te?m", false, "te?m");
    assertWildcardQueryEquals("Te?m", false, "Te?m");
    assertWildcardQueryEquals("TE?M", false, "TE?M");
    assertWildcardQueryEquals("Te?m*gerM", false, "Te?m*gerM");
//  Fuzzy queries:
    assertWildcardQueryEquals("Term~", "term~2");
    assertWildcardQueryEquals("Term~", true, "term~2");
    assertWildcardQueryEquals("Term~", false, "Term~2");
//  Range queries:
    assertWildcardQueryEquals("[A TO C]", "[a TO c]");
    assertWildcardQueryEquals("[A TO C]", true, "[a TO c]");
    assertWildcardQueryEquals("[A TO C]", false, "[A TO C]");
    // Test suffix queries: first disallow
    try {
      assertWildcardQueryEquals("*Term", true, "*term");
    } catch(Exception pe) {
      // expected exception
      if(!isQueryParserException(pe)){
        fail();
      }
    }
    try {
      assertWildcardQueryEquals("?Term", true, "?term");
      fail();
    } catch(Exception pe) {
      // expected exception
      if(!isQueryParserException(pe)){
        fail();
      }
    }
    // Test suffix queries: then allow
    assertWildcardQueryEquals("*Term", true, "*term", true);
    assertWildcardQueryEquals("?Term", true, "?term", true);
  }
  
  public void testLeadingWildcardType() throws Exception {
    CommonQueryParserConfiguration cqpC = getParserConfig(null);
    cqpC.setAllowLeadingWildcard(true);
    assertEquals(WildcardQuery.class, getQuery("t*erm*",cqpC).getClass());
    assertEquals(WildcardQuery.class, getQuery("?term*",cqpC).getClass());
    assertEquals(WildcardQuery.class, getQuery("*term*",cqpC).getClass());
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
                      "term (phrase1 phrase2) term");
    assertQueryEquals("term AND NOT phrase term", qpAnalyzer,
                      "+term -(phrase1 phrase2) term");
    assertQueryEquals("stop^3", qpAnalyzer, "");
    assertQueryEquals("stop", qpAnalyzer, "");
    assertQueryEquals("(stop)^3", qpAnalyzer, "");
    assertQueryEquals("((stop))^3", qpAnalyzer, "");
    assertQueryEquals("(stop^3)", qpAnalyzer, "");
    assertQueryEquals("((stop)^3)", qpAnalyzer, "");
    assertQueryEquals("(stop)", qpAnalyzer, "");
    assertQueryEquals("((stop))", qpAnalyzer, "");
    assertTrue(getQuery("term term term", qpAnalyzer) instanceof BooleanQuery);
    assertTrue(getQuery("term +stop", qpAnalyzer) instanceof TermQuery);
  }

  public void testRange() throws Exception {
    assertQueryEquals("[ a TO z]", null, "[a TO z]");
    assertQueryEquals("[ a TO z}", null, "[a TO z}");
    assertQueryEquals("{ a TO z]", null, "{a TO z]"); 

     assertEquals(MultiTermQuery.CONSTANT_SCORE_AUTO_REWRITE_DEFAULT, ((TermRangeQuery)getQuery("[ a TO z]")).getRewriteMethod());

    CommonQueryParserConfiguration qp = getParserConfig( new MockAnalyzer(random(), MockTokenizer.SIMPLE, true));
    
    qp.setMultiTermRewriteMethod(MultiTermQuery.SCORING_BOOLEAN_QUERY_REWRITE);
    assertEquals(MultiTermQuery.SCORING_BOOLEAN_QUERY_REWRITE,((TermRangeQuery)getQuery("[ a TO z]", qp)).getRewriteMethod());
    
    // test open ranges
    assertQueryEquals("[ a TO * ]", null, "[a TO *]");
    assertQueryEquals("[ * TO z ]", null, "[* TO z]");
    assertQueryEquals("[ * TO * ]", null, "[* TO *]");
    
    // mixing exclude and include bounds
    assertQueryEquals("{ a TO z ]", null, "{a TO z]");
    assertQueryEquals("[ a TO z }", null, "[a TO z}");
    assertQueryEquals("{ a TO * ]", null, "{a TO *]");
    assertQueryEquals("[ * TO z }", null, "[* TO z}");
    
    assertQueryEquals("[ a TO z ]", null, "[a TO z]");
    assertQueryEquals("{ a TO z}", null, "{a TO z}");
    assertQueryEquals("{ a TO z }", null, "{a TO z}");
    assertQueryEquals("{ a TO z }^2.0", null, "{a TO z}^2.0");
    assertQueryEquals("[ a TO z] OR bar", null, "[a TO z] bar");
    assertQueryEquals("[ a TO z] AND bar", null, "+[a TO z] +bar");
    assertQueryEquals("( bar blar { a TO z}) ", null, "bar blar {a TO z}");
    assertQueryEquals("gack ( bar blar { a TO z}) ", null, "gack (bar blar {a TO z})");

    assertQueryEquals("[* TO Z]",null,"[* TO z]");
    assertQueryEquals("[A TO *]",null,"[a TO *]");
    assertQueryEquals("[* TO *]",null,"[* TO *]");
 }

  public void testRangeWithPhrase() throws Exception {
    assertQueryEquals("[\\* TO \"*\"]",null,"[\\* TO \\*]");
    assertQueryEquals("[\"*\" TO *]",null,"[\\* TO *]");
  }

  private String escapeDateString(String s) {
    if (s.indexOf(" ") > -1) {
      return "\"" + s + "\"";
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
               "[" + getDate(startDate, resolution) + " TO " + getDate(endDateInclusive, resolution) + "]");
    assertQueryEquals(cqpC, field, field + ":{" + escapeDateString(startDate) + " TO " + escapeDateString(endDate) + "}",
               "{" + getDate(startDate, resolution) + " TO " + getDate(endDate, resolution) + "}");
  }

  public void testEscaped() throws Exception {
    Analyzer a = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
    
    /*assertQueryEquals("\\[brackets", a, "\\[brackets");
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
    assertQueryEquals("\\?blah", a, "\\?blah");
    //assertQueryEquals("foo \\&\\& bar", a, "foo \\&\\& bar");
    //assertQueryEquals("foo \\|| bar", a, "foo \\|| bar");
    //assertQueryEquals("foo \\AND bar", a, "foo \\AND bar");*/

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

    assertQueryEquals("a:b\\-c~", a, "a:b-c~2");
    assertQueryEquals("a:b\\+c~", a, "a:b+c~2");
    assertQueryEquals("a:b\\:c~", a, "a:b:c~2");
    assertQueryEquals("a:b\\\\c~", a, "a:b\\c~2");

    assertQueryEquals("[ a\\- TO a\\+ ]", null, "[a- TO a+]");
    assertQueryEquals("[ a\\: TO a\\~ ]", null, "[a: TO a~]");
    assertQueryEquals("[ a\\\\ TO a\\* ]", null, "[a\\ TO a*]");

    assertQueryEquals("[\"c\\:\\\\temp\\\\\\~foo0.txt\" TO \"c\\:\\\\temp\\\\\\~foo9.txt\"]", a, 
                      "[c:\\temp\\~foo0.txt TO c:\\temp\\~foo9.txt]");
    
    assertQueryEquals("a\\\\\\+b", a, "a\\+b");
    
    assertQueryEquals("a \\\"b c\\\" d", a, "a \"b c\" d");
    assertQueryEquals("\"a \\\"b c\\\" d\"", a, "\"a \"b c\" d\"");
    assertQueryEquals("\"a \\+b c d\"", a, "\"a +b c d\"");
    
    assertQueryEquals("c\\:\\\\temp\\\\\\~foo.txt", a, "c:\\temp\\~foo.txt");
    
    assertParseException("XY\\"); // there must be a character after the escape char
    
    // test unicode escaping
    assertQueryEquals("a\\u0062c", a, "abc");
    assertQueryEquals("XY\\u005a", a, "XYZ");
    assertQueryEquals("XY\\u005A", a, "XYZ");
    assertQueryEquals("\"a \\\\\\u0028\\u0062\\\" c\"", a, "\"a \\(b\" c\"");
    
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
  }
  
  public void testEscapedVsQuestionMarkAsWildcard() throws Exception {
    Analyzer a = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
    assertQueryEquals("a:b\\-?c", a, "a:b\\-?c");
    assertQueryEquals("a:b\\+?c", a, "a:b\\+?c");
    assertQueryEquals("a:b\\:?c", a, "a:b\\:?c");

    assertQueryEquals("a:b\\\\?c", a, "a:b\\\\?c");
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
    Analyzer oneStopAnalyzer = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true, stopWords, true);
    CommonQueryParserConfiguration qp = getParserConfig(oneStopAnalyzer);
    Query q = getQuery("on^1.0",qp);
    assertNotNull(q);
    q = getQuery("\"hello\"^2.0",qp);
    assertNotNull(q);
    assertEquals(q.getBoost(), (float) 2.0, (float) 0.5);
    q = getQuery("hello^2.0",qp);
    assertNotNull(q);
    assertEquals(q.getBoost(), (float) 2.0, (float) 0.5);
    q = getQuery("\"on\"^1.0",qp);
    assertNotNull(q);

    Analyzer a2 = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true, MockTokenFilter.ENGLISH_STOPSET, true); 
    CommonQueryParserConfiguration qp2 = getParserConfig(a2);
    q = getQuery("the^3", qp2);
    // "the" is a stop word so the result is an empty query:
    assertNotNull(q);
    assertEquals("", q.toString());
    assertEquals(1.0f, q.getBoost(), 0.01f);
  }

  public void assertParseException(String queryString) throws Exception {
    try {
      getQuery(queryString);
    } catch (Exception expected) {
      if(isQueryParserException(expected)){
        return;
      }
    }
    fail("ParseException expected, not thrown");
  }

  public void assertParseException(String queryString, Analyzer a) throws Exception {
    try {
      getQuery(queryString, a);
    } catch (Exception expected) {
      if(isQueryParserException(expected)){
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
//  public void testLocalDateFormat() throws IOException, ParseException {
//    Directory ramDir = newDirectory();
//    IndexWriter iw = new IndexWriter(ramDir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random, MockTokenizer.WHITESPACE, false)));
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
//  private void addDateDoc(String content, int year, int month,
//                          int day, int hour, int minute, int second, IndexWriter iw) throws IOException {
//    Document d = new Document();
//    d.add(newField("f", content, Field.Store.YES, Field.Index.ANALYZED));
//    Calendar cal = Calendar.getInstance(Locale.ENGLISH);
//    cal.set(year, month - 1, day, hour, minute, second);
//    d.add(newField("date", DateField.dateToString(cal.getTime()), Field.Store.YES, Field.Index.NOT_ANALYZED));
//    iw.addDocument(d);
//  }

  public abstract void testStarParsing() throws Exception;

  public void testEscapedWildcard() throws Exception {
    CommonQueryParserConfiguration qp = getParserConfig( new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false));
    WildcardQuery q = new WildcardQuery(new Term("field", "foo\\?ba?r"));
    assertEquals(q, getQuery("foo\\?ba?r", qp));
  }
  
  public void testRegexps() throws Exception {
    CommonQueryParserConfiguration qp = getParserConfig( new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false));
    RegexpQuery q = new RegexpQuery(new Term("field", "[a-z][123]"));
    assertEquals(q, getQuery("/[a-z][123]/",qp));
    qp.setLowercaseExpandedTerms(true);
    assertEquals(q, getQuery("/[A-Z][123]/",qp));
    q.setBoost(0.5f);
    assertEquals(q, getQuery("/[A-Z][123]/^0.5",qp));
    qp.setMultiTermRewriteMethod(MultiTermQuery.SCORING_BOOLEAN_QUERY_REWRITE);
    q.setRewriteMethod(MultiTermQuery.SCORING_BOOLEAN_QUERY_REWRITE);
    assertTrue(getQuery("/[A-Z][123]/^0.5",qp) instanceof RegexpQuery);
    assertEquals(MultiTermQuery.SCORING_BOOLEAN_QUERY_REWRITE, ((RegexpQuery)getQuery("/[A-Z][123]/^0.5",qp)).getRewriteMethod());
    assertEquals(q, getQuery("/[A-Z][123]/^0.5",qp));
    qp.setMultiTermRewriteMethod(MultiTermQuery.CONSTANT_SCORE_AUTO_REWRITE_DEFAULT);
    
    Query escaped = new RegexpQuery(new Term("field", "[a-z]\\/[123]"));
    assertEquals(escaped, getQuery("/[a-z]\\/[123]/",qp));
    Query escaped2 = new RegexpQuery(new Term("field", "[a-z]\\*[123]"));
    assertEquals(escaped2, getQuery("/[a-z]\\*[123]/",qp));
    
    BooleanQuery complex = new BooleanQuery();
    complex.add(new RegexpQuery(new Term("field", "[a-z]\\/[123]")), Occur.MUST);
    complex.add(new TermQuery(new Term("path", "/etc/init.d/")), Occur.MUST);
    complex.add(new TermQuery(new Term("field", "/etc/init[.]d/lucene/")), Occur.SHOULD);
    assertEquals(complex, getQuery("/[a-z]\\/[123]/ AND path:\"/etc/init.d/\" OR \"/etc\\/init\\[.\\]d/lucene/\" ",qp));
    
    Query re = new RegexpQuery(new Term("field", "http.*"));
    assertEquals(re, getQuery("field:/http.*/",qp));
    assertEquals(re, getQuery("/http.*/",qp));
    
    re = new RegexpQuery(new Term("field", "http~0.5"));
    assertEquals(re, getQuery("field:/http~0.5/",qp));
    assertEquals(re, getQuery("/http~0.5/",qp));
    
    re = new RegexpQuery(new Term("field", "boo"));
    assertEquals(re, getQuery("field:/boo/",qp));
    assertEquals(re, getQuery("/boo/",qp));
    
    assertEquals(new TermQuery(new Term("field", "/boo/")), getQuery("\"/boo/\"",qp));
    assertEquals(new TermQuery(new Term("field", "/boo/")), getQuery("\\/boo\\/",qp));
    
    BooleanQuery two = new BooleanQuery();
    two.add(new RegexpQuery(new Term("field", "foo")), Occur.SHOULD);
    two.add(new RegexpQuery(new Term("field", "bar")), Occur.SHOULD);
    assertEquals(two, getQuery("field:/foo/ field:/bar/",qp));
    assertEquals(two, getQuery("/foo/ /bar/",qp));
  }
  
  public void testStopwords() throws Exception {
    CharacterRunAutomaton stopSet = new CharacterRunAutomaton(new RegExp("the|foo").toAutomaton());
    CommonQueryParserConfiguration qp = getParserConfig(new MockAnalyzer(random(), MockTokenizer.SIMPLE, true, stopSet, true));
    Query result = getQuery("field:the OR field:foo",qp);
    assertNotNull("result is null and it shouldn't be", result);
    assertTrue("result is not a BooleanQuery", result instanceof BooleanQuery);
    assertTrue(((BooleanQuery) result).clauses().size() + " does not equal: " + 0, ((BooleanQuery) result).clauses().size() == 0);
    result = getQuery("field:woo OR field:the",qp);
    assertNotNull("result is null and it shouldn't be", result);
    assertTrue("result is not a TermQuery", result instanceof TermQuery);
    result = getQuery("(fieldX:xxxxx OR fieldy:xxxxxxxx)^2 AND (fieldx:the OR fieldy:foo)",qp);
    assertNotNull("result is null and it shouldn't be", result);
    assertTrue("result is not a BooleanQuery", result instanceof BooleanQuery);
    if (VERBOSE) System.out.println("Result: " + result);
    assertTrue(((BooleanQuery) result).clauses().size() + " does not equal: " + 2, ((BooleanQuery) result).clauses().size() == 2);
  }

  public void testPositionIncrement() throws Exception {
    CommonQueryParserConfiguration qp = getParserConfig( new MockAnalyzer(random(), MockTokenizer.SIMPLE, true, MockTokenFilter.ENGLISH_STOPSET, true));
    qp.setEnablePositionIncrements(true);
    String qtxt = "\"the words in poisitions pos02578 are stopped in this phrasequery\"";
    //               0         2                      5           7  8
    int expectedPositions[] = {1,3,4,6,9};
    PhraseQuery pq = (PhraseQuery) getQuery(qtxt,qp);
    //System.out.println("Query text: "+qtxt);
    //System.out.println("Result: "+pq);
    Term t[] = pq.getTerms();
    int pos[] = pq.getPositions();
    for (int i = 0; i < t.length; i++) {
      //System.out.println(i+". "+t[i]+"  pos: "+pos[i]);
      assertEquals("term "+i+" = "+t[i]+" has wrong term-position!",expectedPositions[i],pos[i]);
    }
  }

  public void testMatchAllDocs() throws Exception {
    CommonQueryParserConfiguration qp = getParserConfig( new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false));
    assertEquals(new MatchAllDocsQuery(), getQuery("*:*",qp));
    assertEquals(new MatchAllDocsQuery(), getQuery("(*:*)",qp));
    BooleanQuery bq = (BooleanQuery)getQuery("+*:* -*:*",qp);
    assertTrue(bq.getClauses()[0].getQuery() instanceof MatchAllDocsQuery);
    assertTrue(bq.getClauses()[1].getQuery() instanceof MatchAllDocsQuery);
  }
  
  private void assertHits(int expected, String query, IndexSearcher is) throws Exception {
    String oldDefaultField = getDefaultField();
    setDefaultField("date");
    CommonQueryParserConfiguration qp = getParserConfig( new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false));
    qp.setLocale(Locale.ENGLISH);
    Query q = getQuery(query,qp);
    ScoreDoc[] hits = is.search(q, null, 1000).scoreDocs;
    assertEquals(expected, hits.length);
    setDefaultField( oldDefaultField );
  }

  @Override
  public void tearDown() throws Exception {
    BooleanQuery.setMaxClauseCount(originalMaxClauses);
    super.tearDown();
  }

  // LUCENE-2002: make sure defaults for StandardAnalyzer's
  // enableStopPositionIncr & QueryParser's enablePosIncr
  // "match"
  public void testPositionIncrements() throws Exception {
    Directory dir = newDirectory();
    Analyzer a = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true, MockTokenFilter.ENGLISH_STOPSET, true);
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, a));
    Document doc = new Document();
    doc.add(newTextField("field", "the wizard of ozzy", Field.Store.NO));
    w.addDocument(doc);
    IndexReader r = DirectoryReader.open(w, true);
    w.close();
    IndexSearcher s = newSearcher(r);
    
    Query q = getQuery("\"wizard of ozzy\"",a);
    assertEquals(1, s.search(q, 1).totalHits);
    r.close();
    dir.close();
  }

  /**
   * adds synonym of "dog" for "dogs".
   */
  protected static class MockSynonymFilter extends TokenFilter {
    CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    PositionIncrementAttribute posIncAtt = addAttribute(PositionIncrementAttribute.class);
    boolean addSynonym = false;
    
    public MockSynonymFilter(TokenStream input) {
      super(input);
    }

    @Override
    public final boolean incrementToken() throws IOException {
      if (addSynonym) { // inject our synonym
        clearAttributes();
        termAtt.setEmpty().append("dog");
        posIncAtt.setPositionIncrement(0);
        addSynonym = false;
        return true;
      }
      
      if (input.incrementToken()) {
        addSynonym = termAtt.toString().equals("dogs");
        return true;
      } else {
        return false;
      }
    } 
  }
  
  /** whitespace+lowercase analyzer with synonyms */
  protected class Analyzer1 extends Analyzer {
    public Analyzer1(){
      super();
    }
    @Override
    public TokenStreamComponents createComponents(String fieldName, Reader reader) {
      Tokenizer tokenizer = new MockTokenizer(reader, MockTokenizer.WHITESPACE, true);
      return new TokenStreamComponents(tokenizer, new MockSynonymFilter(tokenizer));
    }
  }
  
  /** whitespace+lowercase analyzer without synonyms */
  protected class Analyzer2 extends Analyzer {
    public Analyzer2(){
      super();
    }
    @Override
    public TokenStreamComponents createComponents(String fieldName, Reader reader) {
      return new TokenStreamComponents(new MockTokenizer(reader, MockTokenizer.WHITESPACE, true));
    }
  }
  
  public abstract void testNewFieldQuery() throws Exception;
  
  /**
   * Mock collation analyzer: indexes terms as "collated" + term
   */
  private class MockCollationFilter extends TokenFilter {
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

    protected MockCollationFilter(TokenStream input) {
      super(input);
    }

    @Override
    public boolean incrementToken() throws IOException {
      if (input.incrementToken()) {
        String term = termAtt.toString();
        termAtt.setEmpty().append("collated").append(term);
        return true;
      } else {
        return false;
      }
    }
    
  }
  private class MockCollationAnalyzer extends Analyzer {
    @Override
    public TokenStreamComponents createComponents(String fieldName, Reader reader) {
      Tokenizer tokenizer = new MockTokenizer(reader, MockTokenizer.WHITESPACE, true);
      return new TokenStreamComponents(tokenizer, new MockCollationFilter(tokenizer));
    }
  }
  
  public void testCollatedRange() throws Exception {
    CommonQueryParserConfiguration qp = getParserConfig(new MockCollationAnalyzer());
    setAnalyzeRangeTerms(qp, true);
    Query expected = TermRangeQuery.newStringRange(getDefaultField(), "collatedabc", "collateddef", true, true);
    Query actual = getQuery("[abc TO def]", qp);
    assertEquals(expected, actual);
  }

  public void testDistanceAsEditsParsing() throws Exception {
    FuzzyQuery q = (FuzzyQuery) getQuery("foobar~2",new MockAnalyzer(random()));
    assertEquals(2, q.getMaxEdits());
  }

  public void testPhraseQueryToString() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true, MockTokenFilter.ENGLISH_STOPSET, true);
    CommonQueryParserConfiguration qp = getParserConfig(analyzer);
    qp.setEnablePositionIncrements(true);
    PhraseQuery q = (PhraseQuery)getQuery("\"this hi this is a test is\"", qp);
    assertEquals("field:\"? hi ? ? ? test\"", q.toString());
  }

  public void testParseWildcardAndPhraseQueries() throws Exception {
    String field = "content";
    String oldDefaultField = getDefaultField();
    setDefaultField(field);
    CommonQueryParserConfiguration qp = getParserConfig(new MockAnalyzer(random()));
    qp.setAllowLeadingWildcard(true);

    String prefixQueries[][] = {
        {"a*", "ab*", "abc*",},
        {"h*", "hi*", "hij*", "\\\\7*"},
        {"o*", "op*", "opq*", "\\\\\\\\*"},
    };

    String wildcardQueries[][] = {
        {"*a*", "*ab*", "*abc**", "ab*e*", "*g?", "*f?1", "abc**"},
        {"*h*", "*hi*", "*hij**", "hi*k*", "*n?", "*m?1", "hij**"},
        {"*o*", "*op*", "*opq**", "op*q*", "*u?", "*t?1", "opq**"},
    };

     // test queries that must be prefix queries
    for (int i = 0; i < prefixQueries.length; i++) {
      for (int j = 0; j < prefixQueries[i].length; j++) {
        String queryString = prefixQueries[i][j];
        Query q = getQuery(queryString,qp);
        assertEquals(PrefixQuery.class, q.getClass());
      }
    }

    // test queries that must be wildcard queries
    for (int i = 0; i < wildcardQueries.length; i++) {
      for (int j = 0; j < wildcardQueries[i].length; j++) {
        String qtxt = wildcardQueries[i][j];
        Query q = getQuery(qtxt,qp);
        assertEquals(WildcardQuery.class, q.getClass());
      }
    }
    setDefaultField(oldDefaultField);
  }

  public void testPhraseQueryPositionIncrements() throws Exception {
    CharacterRunAutomaton stopStopList =
    new CharacterRunAutomaton(new RegExp("[sS][tT][oO][pP]").toAutomaton());

    CommonQueryParserConfiguration qp = getParserConfig(new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false, stopStopList, false));

    PhraseQuery phraseQuery = new PhraseQuery();
    phraseQuery.add(new Term("field", "1"));
    phraseQuery.add(new Term("field", "2"));

    assertEquals(phraseQuery, getQuery("\"1 2\"",qp));
    assertEquals(phraseQuery, getQuery("\"1 stop 2\"",qp));

    qp.setEnablePositionIncrements(true);
    assertEquals(phraseQuery, getQuery("\"1 stop 2\"",qp));

    qp.setEnablePositionIncrements(false);
    assertEquals(phraseQuery, getQuery("\"1 stop 2\"",qp));

    qp = getParserConfig(
                         new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false, stopStopList, true));
    qp.setEnablePositionIncrements(true);

    phraseQuery = new PhraseQuery();
    phraseQuery.add(new Term("field", "1"));
    phraseQuery.add(new Term("field", "2"), 2);
    assertEquals(phraseQuery, getQuery("\"1 stop 2\"",qp));
  }

  public void testMatchAllQueryParsing() throws Exception {
    // test simple parsing of MatchAllDocsQuery
    String oldDefaultField = getDefaultField();
    setDefaultField("key");
    CommonQueryParserConfiguration qp = getParserConfig( new MockAnalyzer(random()));
    assertEquals(new MatchAllDocsQuery(), getQuery(new MatchAllDocsQuery().toString(),qp));

    // test parsing with non-default boost
    MatchAllDocsQuery query = new MatchAllDocsQuery();
    query.setBoost(2.3f);
    assertEquals(query, getQuery(query.toString(),qp));
    setDefaultField(oldDefaultField);
  }

  public void testNestedAndClausesFoo() throws Exception {
    String query = "(field1:[1 TO *] AND field1:[* TO 2]) AND field2:(z)";
    BooleanQuery q = new BooleanQuery();
    BooleanQuery bq = new BooleanQuery();
    bq.add(TermRangeQuery.newStringRange("field1", "1", null, true, true), BooleanClause.Occur.MUST);
    bq.add(TermRangeQuery.newStringRange("field1", null, "2", true, true), BooleanClause.Occur.MUST);
    q.add(bq, BooleanClause.Occur.MUST);
    q.add(new TermQuery(new Term("field2", "z")), BooleanClause.Occur.MUST);
    assertEquals(q, getQuery(query, new MockAnalyzer(random())));
  }
}
