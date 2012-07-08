package org.apache.lucene.queryparser.flexible.precedence;

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
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.QueryNodeParseException;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler;
import org.apache.lucene.queryparser.flexible.standard.parser.ParseException;
import org.apache.lucene.queryparser.util.QueryParserTestBase; // javadocs
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.automaton.BasicAutomata;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;

/**
 * <p>
 * This test case tests {@link PrecedenceQueryParser}.
 * </p>
 * <p>
 * It contains all tests from {@link QueryParserTestBase}
 * with some adjusted to fit the precedence requirement, plus some precedence test cases.
 * </p>
 * 
 * @see QueryParserTestBase
 */
//TODO: refactor this to actually extend that class, overriding the tests
//that it adjusts to fit the precedence requirement, adding its extra tests.
public class TestPrecedenceQueryParser extends LuceneTestCase {

  public static Analyzer qpAnalyzer = new QPTestAnalyzer();

  public static final class QPTestFilter extends TokenFilter {
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

    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

    private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);

    @Override
    public boolean incrementToken() throws IOException {
      if (inPhrase) {
        inPhrase = false;
        termAtt.setEmpty().append("phrase2");
        offsetAtt.setOffset(savedStart, savedEnd);
        return true;
      } else
        while (input.incrementToken())
          if (termAtt.toString().equals("phrase")) {
            inPhrase = true;
            savedStart = offsetAtt.startOffset();
            savedEnd = offsetAtt.endOffset();
            termAtt.setEmpty().append("phrase1");
            offsetAtt.setOffset(savedStart, savedEnd);
            return true;
          } else if (!termAtt.toString().equals("stop"))
            return true;
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
    public final TokenStreamComponents createComponents(String fieldName, Reader reader) {
      Tokenizer tokenizer = new MockTokenizer(reader, MockTokenizer.SIMPLE, true);
      return new TokenStreamComponents(tokenizer, new QPTestFilter(tokenizer));
    }
  }

  private int originalMaxClauses;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    originalMaxClauses = BooleanQuery.getMaxClauseCount();
  }

  public PrecedenceQueryParser getParser(Analyzer a) throws Exception {
    if (a == null)
      a = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true);
    PrecedenceQueryParser qp = new PrecedenceQueryParser();
    qp.setAnalyzer(a);
    qp.setDefaultOperator(StandardQueryConfigHandler.Operator.OR);
    return qp;
  }

  public Query getQuery(String query, Analyzer a) throws Exception {
    return getParser(a).parse(query, "field");
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

  public void assertWildcardQueryEquals(String query, boolean lowercase,
      String result) throws Exception {
    PrecedenceQueryParser qp = getParser(null);
    qp.setLowercaseExpandedTerms(lowercase);
    Query q = qp.parse(query, "field");
    String s = q.toString("field");
    if (!s.equals(result)) {
      fail("WildcardQuery /" + query + "/ yielded /" + s + "/, expecting /"
          + result + "/");
    }
  }

  public void assertWildcardQueryEquals(String query, String result)
      throws Exception {
    PrecedenceQueryParser qp = getParser(null);
    Query q = qp.parse(query, "field");
    String s = q.toString("field");
    if (!s.equals(result)) {
      fail("WildcardQuery /" + query + "/ yielded /" + s + "/, expecting /"
          + result + "/");
    }
  }

  public Query getQueryDOA(String query, Analyzer a) throws Exception {
    if (a == null)
      a = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true);
    PrecedenceQueryParser qp = new PrecedenceQueryParser();
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

  public void testSimple() throws Exception {
    assertQueryEquals("term term term", null, "term term term");
    assertQueryEquals("türm term term", null, "türm term term");
    assertQueryEquals("ümlaut", null, "ümlaut");

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

    assertQueryEquals("+term -term term", null, "+term -term term");
    assertQueryEquals("foo:term AND field:anotherTerm", null,
        "+foo:term +anotherterm");
    assertQueryEquals("term AND \"phrase phrase\"", null,
        "+term +\"phrase phrase\"");
    assertQueryEquals("\"hello there\"", null, "\"hello there\"");
    assertTrue(getQuery("a AND b", null) instanceof BooleanQuery);
    assertTrue(getQuery("hello", null) instanceof TermQuery);
    assertTrue(getQuery("\"hello there\"", null) instanceof PhraseQuery);

    assertQueryEquals("germ term^2.0", null, "germ term^2.0");
    assertQueryEquals("(term)^2.0", null, "term^2.0");
    assertQueryEquals("(germ term)^2.0", null, "(germ term)^2.0");
    assertQueryEquals("term^2.0", null, "term^2.0");
    assertQueryEquals("term^2", null, "term^2.0");
    assertQueryEquals("\"germ term\"^2.0", null, "\"germ term\"^2.0");
    assertQueryEquals("\"term germ\"^2", null, "\"term germ\"^2.0");

    assertQueryEquals("(foo OR bar) AND (baz OR boo)", null,
        "+(foo bar) +(baz boo)");
    assertQueryEquals("((a OR b) AND NOT c) OR d", null, "(+(a b) -c) d");
    assertQueryEquals("+(apple \"steve jobs\") -(foo bar baz)", null,
        "+(apple \"steve jobs\") -(foo bar baz)");
    assertQueryEquals("+title:(dog OR cat) -author:\"bob dole\"", null,
        "+(title:dog title:cat) -author:\"bob dole\"");

    PrecedenceQueryParser qp = new PrecedenceQueryParser();
    qp.setAnalyzer(new MockAnalyzer(random()));
    // make sure OR is the default:
    assertEquals(StandardQueryConfigHandler.Operator.OR, qp.getDefaultOperator());
    qp.setDefaultOperator(StandardQueryConfigHandler.Operator.AND);
    assertEquals(StandardQueryConfigHandler.Operator.AND, qp.getDefaultOperator());
    qp.setDefaultOperator(StandardQueryConfigHandler.Operator.OR);
    assertEquals(StandardQueryConfigHandler.Operator.OR, qp.getDefaultOperator());

    assertQueryEquals("a OR !b", null, "a -b");
    assertQueryEquals("a OR ! b", null, "a -b");
    assertQueryEquals("a OR -b", null, "a -b");
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

    Analyzer a = new MockAnalyzer(random());
    assertQueryEquals("3", a, "3");
    assertQueryEquals("term 1.0 1 2", a, "term 1.0 1 2");
    assertQueryEquals("term term1 term2", a, "term term1 term2");
  }

  public void testWildcard() throws Exception {
    assertQueryEquals("term*", null, "term*");
    assertQueryEquals("term*^2", null, "term*^2.0");
    assertQueryEquals("term~", null, "term~2");
    assertQueryEquals("term~0.7", null, "term~1");
    assertQueryEquals("term~^3", null, "term~2^3.0");
    assertQueryEquals("term^3~", null, "term~2^3.0");
    assertQueryEquals("term*germ", null, "term*germ");
    assertQueryEquals("term*germ^3", null, "term*germ^3.0");

    assertTrue(getQuery("term*", null) instanceof PrefixQuery);
    assertTrue(getQuery("term*^2", null) instanceof PrefixQuery);
    assertTrue(getQuery("term~", null) instanceof FuzzyQuery);
    assertTrue(getQuery("term~0.7", null) instanceof FuzzyQuery);
    FuzzyQuery fq = (FuzzyQuery) getQuery("term~0.7", null);
    assertEquals(1, fq.getMaxEdits());
    assertEquals(FuzzyQuery.defaultPrefixLength, fq.getPrefixLength());
    fq = (FuzzyQuery) getQuery("term~", null);
    assertEquals(2, fq.getMaxEdits());
    assertEquals(FuzzyQuery.defaultPrefixLength, fq.getPrefixLength());
    try {
      getQuery("term~1.1", null); // value > 1, throws exception
      fail();
    } catch (ParseException pe) {
      // expected exception
    }
    assertTrue(getQuery("term*germ", null) instanceof WildcardQuery);

    /*
     * Tests to see that wild card terms are (or are not) properly lower-cased
     * with propery parser configuration
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
    // Fuzzy queries:
    assertWildcardQueryEquals("Term~", "term~2");
    assertWildcardQueryEquals("Term~", true, "term~2");
    assertWildcardQueryEquals("Term~", false, "Term~2");
    // Range queries:
    assertWildcardQueryEquals("[A TO C]", "[a TO c]");
    assertWildcardQueryEquals("[A TO C]", true, "[a TO c]");
    assertWildcardQueryEquals("[A TO C]", false, "[A TO C]");
  }

  public void testQPA() throws Exception {
    assertQueryEquals("term term term", qpAnalyzer, "term term term");
    assertQueryEquals("term +stop term", qpAnalyzer, "term term");
    assertQueryEquals("term -stop term", qpAnalyzer, "term term");
    assertQueryEquals("drop AND stop AND roll", qpAnalyzer, "+drop +roll");
    assertQueryEquals("term phrase term", qpAnalyzer,
        "term (phrase1 phrase2) term");
    // note the parens in this next assertion differ from the original
    // QueryParser behavior
    assertQueryEquals("term AND NOT phrase term", qpAnalyzer,
        "(+term -(phrase1 phrase2)) term");
    assertQueryEquals("stop", qpAnalyzer, "");
    assertQueryEquals("stop OR stop AND stop", qpAnalyzer, "");
    assertTrue(getQuery("term term term", qpAnalyzer) instanceof BooleanQuery);
    assertTrue(getQuery("term +stop", qpAnalyzer) instanceof TermQuery);
  }

  public void testRange() throws Exception {
    assertQueryEquals("[ a TO z]", null, "[a TO z]");
    assertTrue(getQuery("[ a TO z]", null) instanceof TermRangeQuery);
    assertQueryEquals("[ a TO z ]", null, "[a TO z]");
    assertQueryEquals("{ a TO z}", null, "{a TO z}");
    assertQueryEquals("{ a TO z }", null, "{a TO z}");
    assertQueryEquals("{ a TO z }^2.0", null, "{a TO z}^2.0");
    assertQueryEquals("[ a TO z] OR bar", null, "[a TO z] bar");
    assertQueryEquals("[ a TO z] AND bar", null, "+[a TO z] +bar");
    assertQueryEquals("( bar blar { a TO z}) ", null, "bar blar {a TO z}");
    assertQueryEquals("gack ( bar blar { a TO z}) ", null,
        "gack (bar blar {a TO z})");
  }

  private String escapeDateString(String s) {
    if (s.contains(" ")) {
      return "\"" + s + "\"";
    } else {
      return s;
    }
  }

  public String getDate(String s) throws Exception {
    // we use the default Locale since LuceneTestCase randomizes it
    DateFormat df = DateFormat.getDateInstance(DateFormat.SHORT, Locale.getDefault());
    return DateTools.dateToString(df.parse(s), DateTools.Resolution.DAY);
  }

  private String getLocalizedDate(int year, int month, int day,
      boolean extendLastDate) {
    // we use the default Locale/TZ since LuceneTestCase randomizes it
    DateFormat df = DateFormat.getDateInstance(DateFormat.SHORT, Locale.getDefault());
    Calendar calendar = new GregorianCalendar(TimeZone.getDefault(), Locale.getDefault());
    calendar.set(year, month, day);
    if (extendLastDate) {
      calendar.set(Calendar.HOUR_OF_DAY, 23);
      calendar.set(Calendar.MINUTE, 59);
      calendar.set(Calendar.SECOND, 59);
      calendar.set(Calendar.MILLISECOND, 999);
    }
    return df.format(calendar.getTime());
  }

  public void testDateRange() throws Exception {
    String startDate = getLocalizedDate(2002, 1, 1, false);
    String endDate = getLocalizedDate(2002, 1, 4, false);
    // we use the default Locale/TZ since LuceneTestCase randomizes it
    Calendar endDateExpected = new GregorianCalendar(TimeZone.getDefault(), Locale.getDefault());
    endDateExpected.set(2002, 1, 4, 23, 59, 59);
    endDateExpected.set(Calendar.MILLISECOND, 999);
    final String defaultField = "default";
    final String monthField = "month";
    final String hourField = "hour";
    PrecedenceQueryParser qp = new PrecedenceQueryParser(new MockAnalyzer(random()));

    Map<CharSequence, DateTools.Resolution> fieldMap = new HashMap<CharSequence,DateTools.Resolution>();
    // set a field specific date resolution
    fieldMap.put(monthField, DateTools.Resolution.MONTH);
    qp.setDateResolution(fieldMap);

    // set default date resolution to MILLISECOND
    qp.setDateResolution(DateTools.Resolution.MILLISECOND);

    // set second field specific date resolution
    fieldMap.put(hourField, DateTools.Resolution.HOUR);
    qp.setDateResolution(fieldMap);

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

  public void assertQueryEquals(PrecedenceQueryParser qp, String field, String query,
      String result) throws Exception {
    Query q = qp.parse(query, field);
    String s = q.toString(field);
    if (!s.equals(result)) {
      fail("Query /" + query + "/ yielded /" + s + "/, expecting /" + result
          + "/");
    }
  }

  public void assertDateRangeQueryEquals(PrecedenceQueryParser qp, String field,
      String startDate, String endDate, Date endDateInclusive,
      DateTools.Resolution resolution) throws Exception {
    assertQueryEquals(qp, field, field + ":[" + escapeDateString(startDate)
        + " TO " + escapeDateString(endDate) + "]", "["
        + getDate(startDate, resolution) + " TO "
        + getDate(endDateInclusive, resolution) + "]");
    assertQueryEquals(qp, field, field + ":{" + escapeDateString(startDate)
        + " TO " + escapeDateString(endDate) + "}", "{"
        + getDate(startDate, resolution) + " TO "
        + getDate(endDate, resolution) + "}");
  }

  public void testEscaped() throws Exception {
    Analyzer a = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);

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

    assertQueryEquals("[ a\\- TO a\\+ ]", null, "[a- TO a+]");
    assertQueryEquals("[ a\\: TO a\\~ ]", null, "[a: TO a~]");
    assertQueryEquals("[ a\\\\ TO a\\* ]", null, "[a\\ TO a*]");
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
    CharacterRunAutomaton stopSet = new CharacterRunAutomaton(BasicAutomata.makeString("on"));
    Analyzer oneStopAnalyzer = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true, stopSet, true);

    PrecedenceQueryParser qp = new PrecedenceQueryParser();
    qp.setAnalyzer(oneStopAnalyzer);
    Query q = qp.parse("on^1.0", "field");
    assertNotNull(q);
    q = qp.parse("\"hello\"^2.0", "field");
    assertNotNull(q);
    assertEquals(q.getBoost(), (float) 2.0, (float) 0.5);
    q = qp.parse("hello^2.0", "field");
    assertNotNull(q);
    assertEquals(q.getBoost(), (float) 2.0, (float) 0.5);
    q = qp.parse("\"on\"^1.0", "field");
    assertNotNull(q);

    q = getParser(new MockAnalyzer(random(), MockTokenizer.SIMPLE, true, MockTokenFilter.ENGLISH_STOPSET, true)).parse("the^3",
        "field");
    assertNotNull(q);
  }

  public void testException() throws Exception {
    try {
      assertQueryEquals("\"some phrase", null, "abc");
      fail("ParseException expected, not thrown");
    } catch (QueryNodeParseException expected) {
    }
  }

  public void testBooleanQuery() throws Exception {
    BooleanQuery.setMaxClauseCount(2);
    try {
      getParser(new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false)).parse("one two three", "field");
      fail("ParseException expected due to too many boolean clauses");
    } catch (QueryNodeException expected) {
      // too many boolean clauses, so ParseException is expected
    }
  }
  
  // LUCENE-792
  public void testNOT() throws Exception {
    Analyzer a = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
    assertQueryEquals("NOT foo AND bar", a, "-foo +bar");
  }

  /**
   * This test differs from the original QueryParser, showing how the precedence
   * issue has been corrected.
   */
  public void testPrecedence() throws Exception {
    PrecedenceQueryParser parser = getParser(new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false));
    Query query1 = parser.parse("A AND B OR C AND D", "field");
    Query query2 = parser.parse("(A AND B) OR (C AND D)", "field");
    assertEquals(query1, query2);

    query1 = parser.parse("A OR B C", "field");
    query2 = parser.parse("(A B) C", "field");
    assertEquals(query1, query2);

    query1 = parser.parse("A AND B C", "field");
    query2 = parser.parse("(+A +B) C", "field");
    assertEquals(query1, query2);

    query1 = parser.parse("A AND NOT B", "field");
    query2 = parser.parse("+A -B", "field");
    assertEquals(query1, query2);

    query1 = parser.parse("A OR NOT B", "field");
    query2 = parser.parse("A -B", "field");
    assertEquals(query1, query2);

    query1 = parser.parse("A OR NOT B AND C", "field");
    query2 = parser.parse("A (-B +C)", "field");
    assertEquals(query1, query2);
    
    parser.setDefaultOperator(StandardQueryConfigHandler.Operator.AND);
    query1 = parser.parse("A AND B OR C AND D", "field");
    query2 = parser.parse("(A AND B) OR (C AND D)", "field");
    assertEquals(query1, query2);

    query1 = parser.parse("A AND B C", "field");
    query2 = parser.parse("(A B) C", "field");
    assertEquals(query1, query2);

    query1 = parser.parse("A AND B C", "field");
    query2 = parser.parse("(+A +B) C", "field");
    assertEquals(query1, query2);

    query1 = parser.parse("A AND NOT B", "field");
    query2 = parser.parse("+A -B", "field");
    assertEquals(query1, query2);

    query1 = parser.parse("A AND NOT B OR C", "field");
    query2 = parser.parse("(+A -B) OR C", "field");
    assertEquals(query1, query2);
    
  }

  @Override
  public void tearDown() throws Exception {
    BooleanQuery.setMaxClauseCount(originalMaxClauses);
    super.tearDown();
  }

}
