package org.apache.lucene.queryParser;

/**
 * Copyright 2002-2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import junit.framework.TestCase;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseTokenizer;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.DateField;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RangeQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import java.io.IOException;
import java.io.Reader;
import java.text.DateFormat;
import java.util.Calendar;

/**
 * Tests QueryParser.
 */
public class TestQueryParser extends TestCase {

  public static Analyzer qpAnalyzer = new QPTestAnalyzer();

  public static class QPTestFilter extends TokenFilter {
    /**
     * Filter which discards the token 'stop' and which expands the
     * token 'phrase' into 'phrase1 phrase2'
     */
    public QPTestFilter(TokenStream in) {
      super(in);
    }

    boolean inPhrase = false;
    int savedStart = 0, savedEnd = 0;

    public Token next() throws IOException {
      if (inPhrase) {
        inPhrase = false;
        return new Token("phrase2", savedStart, savedEnd);
      } else
        for (Token token = input.next(); token != null; token = input.next()) {
          if (token.termText().equals("phrase")) {
            inPhrase = true;
            savedStart = token.startOffset();
            savedEnd = token.endOffset();
            return new Token("phrase1", savedStart, savedEnd);
          } else if (!token.termText().equals("stop"))
            return token;
        }
      return null;
    }
  }

  public static class QPTestAnalyzer extends Analyzer {

    /** Filters LowerCaseTokenizer with StopFilter. */
    public final TokenStream tokenStream(String fieldName, Reader reader) {
      return new QPTestFilter(new LowerCaseTokenizer(reader));
    }
  }

  public static class QPTestParser extends QueryParser {
    public QPTestParser(String f, Analyzer a) {
      super(f, a);
    }

    protected Query getFuzzyQuery(String field, String termStr) throws ParseException {
      throw new ParseException("Fuzzy queries not allowed");
    }

    protected Query getWildcardQuery(String field, String termStr) throws ParseException {
      throw new ParseException("Wildcard queries not allowed");
    }
  }

  private int originalMaxClauses;

  public void setUp() {
    originalMaxClauses = BooleanQuery.getMaxClauseCount();
  }

  public QueryParser getParser(Analyzer a) throws Exception {
    if (a == null)
      a = new SimpleAnalyzer();
    QueryParser qp = new QueryParser("field", a);
    qp.setOperator(QueryParser.DEFAULT_OPERATOR_OR);
    return qp;
  }

  public Query getQuery(String query, Analyzer a) throws Exception {
    return getParser(a).parse(query);
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

  public void assertWildcardQueryEquals(String query, boolean lowercase, String result)
    throws Exception {
    QueryParser qp = getParser(null);
    qp.setLowercaseWildcardTerms(lowercase);
    Query q = qp.parse(query);
    String s = q.toString("field");
    if (!s.equals(result)) {
      fail("WildcardQuery /" + query + "/ yielded /" + s
           + "/, expecting /" + result + "/");
    }
  }

  public Query getQueryDOA(String query, Analyzer a)
    throws Exception {
    if (a == null)
      a = new SimpleAnalyzer();
    QueryParser qp = new QueryParser("field", a);
    qp.setOperator(QueryParser.DEFAULT_OPERATOR_AND);
    return qp.parse(query);
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

  public void testPunct() throws Exception {
    Analyzer a = new WhitespaceAnalyzer();
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

    Analyzer a = new StandardAnalyzer();
    assertQueryEquals("3", a, "3");
    assertQueryEquals("term 1.0 1 2", a, "term 1.0 1 2");
    assertQueryEquals("term term1 term2", a, "term term1 term2");
  }

  public void testWildcard() throws Exception {
    assertQueryEquals("term*", null, "term*");
    assertQueryEquals("term*^2", null, "term*^2.0");
    assertQueryEquals("term~", null, "term~");
    assertQueryEquals("term~^2", null, "term^2.0~");
    assertQueryEquals("term^2~", null, "term^2.0~");
    assertQueryEquals("term*germ", null, "term*germ");
    assertQueryEquals("term*germ^3", null, "term*germ^3.0");

    assertTrue(getQuery("term*", null) instanceof PrefixQuery);
    assertTrue(getQuery("term*^2", null) instanceof PrefixQuery);
    assertTrue(getQuery("term~", null) instanceof FuzzyQuery);
    assertTrue(getQuery("term*germ", null) instanceof WildcardQuery);

/* Tests to see that wild card terms are (or are not) properly
	 * lower-cased with propery parser configuration
	 */
// First prefix queries:
    assertWildcardQueryEquals("term*", true, "term*");
    assertWildcardQueryEquals("Term*", true, "term*");
    assertWildcardQueryEquals("TERM*", true, "term*");
    assertWildcardQueryEquals("term*", false, "term*");
    assertWildcardQueryEquals("Term*", false, "Term*");
    assertWildcardQueryEquals("TERM*", false, "TERM*");
// Then 'full' wildcard queries:
    assertWildcardQueryEquals("te?m", true, "te?m");
    assertWildcardQueryEquals("Te?m", true, "te?m");
    assertWildcardQueryEquals("TE?M", true, "te?m");
    assertWildcardQueryEquals("Te?m*gerM", true, "te?m*germ");
    assertWildcardQueryEquals("te?m", false, "te?m");
    assertWildcardQueryEquals("Te?m", false, "Te?m");
    assertWildcardQueryEquals("TE?M", false, "TE?M");
    assertWildcardQueryEquals("Te?m*gerM", false, "Te?m*gerM");
  }

  public void testQPA() throws Exception {
    assertQueryEquals("term term term", qpAnalyzer, "term term term");
    assertQueryEquals("term +stop term", qpAnalyzer, "term term");
    assertQueryEquals("term -stop term", qpAnalyzer, "term term");
    assertQueryEquals("drop AND stop AND roll", qpAnalyzer, "+drop +roll");
    assertQueryEquals("term phrase term", qpAnalyzer,
                      "term \"phrase1 phrase2\" term");
    assertQueryEquals("term AND NOT phrase term", qpAnalyzer,
                      "+term -\"phrase1 phrase2\" term");
    assertQueryEquals("stop", qpAnalyzer, "");
    assertTrue(getQuery("term term term", qpAnalyzer) instanceof BooleanQuery);
    assertTrue(getQuery("term +stop", qpAnalyzer) instanceof TermQuery);
  }

  public void testRange() throws Exception {
    assertQueryEquals("[ a TO z]", null, "[a TO z]");
    assertTrue(getQuery("[ a TO z]", null) instanceof RangeQuery);
    assertQueryEquals("[ a TO z ]", null, "[a TO z]");
    assertQueryEquals("{ a TO z}", null, "{a TO z}");
    assertQueryEquals("{ a TO z }", null, "{a TO z}");
    assertQueryEquals("{ a TO z }^2.0", null, "{a TO z}^2.0");
    assertQueryEquals("[ a TO z] OR bar", null, "[a TO z] bar");
    assertQueryEquals("[ a TO z] AND bar", null, "+[a TO z] +bar");
    assertQueryEquals("( bar blar { a TO z}) ", null, "bar blar {a TO z}");
    assertQueryEquals("gack ( bar blar { a TO z}) ", null, "gack (bar blar {a TO z})");
  }

  public String getDate(String s) throws Exception {
    DateFormat df = DateFormat.getDateInstance(DateFormat.SHORT);
    return DateField.dateToString(df.parse(s));
  }

  public String getLocalizedDate(int year, int month, int day) {
    DateFormat df = DateFormat.getDateInstance(DateFormat.SHORT);
    Calendar calendar = Calendar.getInstance();
    calendar.set(year, month, day);
    return df.format(calendar.getTime());
  }

  public void testDateRange() throws Exception {
    String startDate = getLocalizedDate(2002, 1, 1);
    String endDate = getLocalizedDate(2002, 1, 4);
    assertQueryEquals("[ " + startDate + " TO " + endDate + "]", null,
                      "[" + getDate(startDate) + " TO " + getDate(endDate) + "]");
    assertQueryEquals("{  " + startDate + "    " + endDate + "   }", null,
                      "{" + getDate(startDate) + " TO " + getDate(endDate) + "}");
  }

  public void testEscaped() throws Exception {
    Analyzer a = new WhitespaceAnalyzer();
 /*   assertQueryEquals("\\[brackets", a, "\\[brackets");
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
    assertQueryEquals("foo \\&& bar", a, "foo \\&& bar");
    assertQueryEquals("foo \\|| bar", a, "foo \\|| bar");
    assertQueryEquals("foo \\AND bar", a, "foo \\AND bar"); */

	assertQueryEquals("a\\-b:c",a,"a-b:c");
	assertQueryEquals("a\\+b:c",a,"a+b:c");
	assertQueryEquals("a\\:b:c",a,"a:b:c");
	assertQueryEquals("a\\\\b:c",a,"a\\b:c");

	assertQueryEquals("a:b\\-c",a,"a:b-c");
	assertQueryEquals("a:b\\+c",a,"a:b+c");
	assertQueryEquals("a:b\\:c",a,"a:b:c");
	assertQueryEquals("a:b\\\\c",a,"a:b\\c");

	assertQueryEquals("a:b\\-c*",a,"a:b-c*");
	assertQueryEquals("a:b\\+c*",a,"a:b+c*");
	assertQueryEquals("a:b\\:c*",a,"a:b:c*");
	assertQueryEquals("a:b\\\\c*",a,"a:b\\c*");

	assertQueryEquals("a:b\\-?c",a,"a:b-?c");
	assertQueryEquals("a:b\\+?c",a,"a:b+?c");
	assertQueryEquals("a:b\\:?c",a,"a:b:?c");
	assertQueryEquals("a:b\\\\?c",a,"a:b\\?c");

	assertQueryEquals("a:b\\-c~",a,"a:b-c~");
	assertQueryEquals("a:b\\+c~",a,"a:b+c~");
	assertQueryEquals("a:b\\:c~",a,"a:b:c~");
	assertQueryEquals("a:b\\\\c~",a,"a:b\\c~");

	assertQueryEquals("[ a\\- TO a\\+ ]", null, "[a- TO a+]");
	assertQueryEquals("[ a\\: TO a\\~ ]", null, "[a: TO a~]");
	assertQueryEquals("[ a\\\\ TO a\\* ]", null, "[a\\ TO a*]");
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
    StandardAnalyzer oneStopAnalyzer = new StandardAnalyzer(new String[]{"on"});
    QueryParser qp = new QueryParser("field", oneStopAnalyzer);
    Query q = qp.parse("on^1.0");
    assertNotNull(q);
    q = qp.parse("\"hello\"^2.0");
    assertNotNull(q);
    assertEquals(q.getBoost(), (float) 2.0, (float) 0.5);
    q = qp.parse("hello^2.0");
    assertNotNull(q);
    assertEquals(q.getBoost(), (float) 2.0, (float) 0.5);
    q = qp.parse("\"on\"^1.0");
    assertNotNull(q);

    q = QueryParser.parse("the^3", "field", new StandardAnalyzer());
    assertNotNull(q);
  }

  public void testException() throws Exception {
    try {
      assertQueryEquals("\"some phrase", null, "abc");
      fail("ParseException expected, not thrown");
    } catch (ParseException expected) {
    }
  }

  public void testCustomQueryParserWildcard() {
    try {
      new QPTestParser("contents", new WhitespaceAnalyzer()).parse("a?t");
    } catch (ParseException expected) {
      return;
    }
    fail("Wildcard queries should not be allowed");
  }

  public void testCustomQueryParserFuzzy() throws Exception {
    try {
      new QPTestParser("contents", new WhitespaceAnalyzer()).parse("xunit~");
    } catch (ParseException expected) {
      return;
    }
    fail("Fuzzy queries should not be allowed");
  }

  public void testBooleanQuery() throws Exception {
    BooleanQuery.setMaxClauseCount(2);
    try {
      QueryParser.parse("one two three", "field", new WhitespaceAnalyzer());
      fail("ParseException expected due to too many boolean clauses");
    } catch (ParseException expected) {
      // too many boolean clauses, so ParseException is expected
    }
  }

  public void tearDown() {
    BooleanQuery.setMaxClauseCount(originalMaxClauses);
  }

}
