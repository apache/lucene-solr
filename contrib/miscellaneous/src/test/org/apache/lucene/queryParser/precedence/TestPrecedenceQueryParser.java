package org.apache.lucene.queryParser.precedence;

/**
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

import junit.framework.TestCase;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseTokenizer;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.DateTools;
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

public class TestPrecedenceQueryParser extends TestCase {

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

    public Token next(final Token reusableToken) throws IOException {
      assert reusableToken != null;
      if (inPhrase) {
        inPhrase = false;
        reusableToken.setTermBuffer("phrase2");
        reusableToken.setStartOffset(savedStart);
        reusableToken.setEndOffset(savedEnd);
        return reusableToken;
      } else
        for (Token nextToken = input.next(reusableToken); nextToken != null; nextToken = input.next(reusableToken)) {
          if (nextToken.term().equals("phrase")) {
            inPhrase = true;
            savedStart = nextToken.startOffset();
            savedEnd = nextToken.endOffset();
            nextToken.setTermBuffer("phrase1");
            nextToken.setStartOffset(savedStart);
            nextToken.setEndOffset(savedEnd);
            return nextToken;
          } else if (!nextToken.term().equals("stop"))
            return nextToken;
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

  public static class QPTestParser extends PrecedenceQueryParser {
    public QPTestParser(String f, Analyzer a) {
      super(f, a);
    }

    protected Query getFuzzyQuery(String field, String termStr, float minSimilarity) throws ParseException {
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

  public PrecedenceQueryParser getParser(Analyzer a) throws Exception {
    if (a == null)
      a = new SimpleAnalyzer();
    PrecedenceQueryParser qp = new PrecedenceQueryParser("field", a);
    qp.setDefaultOperator(PrecedenceQueryParser.OR_OPERATOR);
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
    PrecedenceQueryParser qp = getParser(null);
    qp.setLowercaseExpandedTerms(lowercase);
    Query q = qp.parse(query);
    String s = q.toString("field");
    if (!s.equals(result)) {
      fail("WildcardQuery /" + query + "/ yielded /" + s
           + "/, expecting /" + result + "/");
    }
  }

  public void assertWildcardQueryEquals(String query, String result) throws Exception {
    PrecedenceQueryParser qp = getParser(null);
    Query q = qp.parse(query);
    String s = q.toString("field");
    if (!s.equals(result)) {
      fail("WildcardQuery /" + query + "/ yielded /" + s + "/, expecting /"
          + result + "/");
    }
  }

  public Query getQueryDOA(String query, Analyzer a)
    throws Exception {
    if (a == null)
      a = new SimpleAnalyzer();
    PrecedenceQueryParser qp = new PrecedenceQueryParser("field", a);
    qp.setDefaultOperator(PrecedenceQueryParser.AND_OPERATOR);
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

  // failing tests disabled since PrecedenceQueryParser
  // is currently unmaintained
  public void _testSimple() throws Exception {
    assertQueryEquals("", null, "");

    assertQueryEquals("term term term", null, "term term term");
    assertQueryEquals("türm term term", null, "türm term term");
    assertQueryEquals("ümlaut", null, "ümlaut");

    assertQueryEquals("+a", null, "+a");
    assertQueryEquals("-a", null, "-a");
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
    assertQueryEquals("((a OR b) AND NOT c) OR d", null,
                      "(+(a b) -c) d");
    assertQueryEquals("+(apple \"steve jobs\") -(foo bar baz)", null,
                      "+(apple \"steve jobs\") -(foo bar baz)");
    assertQueryEquals("+title:(dog OR cat) -author:\"bob dole\"", null,
                      "+(title:dog title:cat) -author:\"bob dole\"");
    
    PrecedenceQueryParser qp = new PrecedenceQueryParser("field", new StandardAnalyzer());
    // make sure OR is the default:
    assertEquals(PrecedenceQueryParser.OR_OPERATOR, qp.getDefaultOperator());
    qp.setDefaultOperator(PrecedenceQueryParser.AND_OPERATOR);
    assertEquals(PrecedenceQueryParser.AND_OPERATOR, qp.getDefaultOperator());
    qp.setDefaultOperator(PrecedenceQueryParser.OR_OPERATOR);
    assertEquals(PrecedenceQueryParser.OR_OPERATOR, qp.getDefaultOperator());

    assertQueryEquals("a OR !b", null, "a (-b)");
    assertQueryEquals("a OR ! b", null, "a (-b)");
    assertQueryEquals("a OR -b", null, "a (-b)");
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

  // failing tests disabled since PrecedenceQueryParser
  // is currently unmaintained
  public void _testWildcard() throws Exception {
    assertQueryEquals("term*", null, "term*");
    assertQueryEquals("term*^2", null, "term*^2.0");
    assertQueryEquals("term~", null, "term~0.5");
    assertQueryEquals("term~0.7", null, "term~0.7");
    assertQueryEquals("term~^2", null, "term^2.0~0.5");
    assertQueryEquals("term^2~", null, "term^2.0~0.5");
    assertQueryEquals("term*germ", null, "term*germ");
    assertQueryEquals("term*germ^3", null, "term*germ^3.0");

    assertTrue(getQuery("term*", null) instanceof PrefixQuery);
    assertTrue(getQuery("term*^2", null) instanceof PrefixQuery);
    assertTrue(getQuery("term~", null) instanceof FuzzyQuery);
    assertTrue(getQuery("term~0.7", null) instanceof FuzzyQuery);
    FuzzyQuery fq = (FuzzyQuery)getQuery("term~0.7", null);
    assertEquals(0.7f, fq.getMinSimilarity(), 0.1f);
    assertEquals(FuzzyQuery.defaultPrefixLength, fq.getPrefixLength());
    fq = (FuzzyQuery)getQuery("term~", null);
    assertEquals(0.5f, fq.getMinSimilarity(), 0.1f);
    assertEquals(FuzzyQuery.defaultPrefixLength, fq.getPrefixLength());
    try {
      getQuery("term~1.1", null);   // value > 1, throws exception
      fail();
    } catch(ParseException pe) {
      // expected exception
    }
    assertTrue(getQuery("term*germ", null) instanceof WildcardQuery);

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
    assertWildcardQueryEquals("Term~", "term~0.5");
    assertWildcardQueryEquals("Term~", true, "term~0.5");
    assertWildcardQueryEquals("Term~", false, "Term~0.5");
//  Range queries:
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
                      "term \"phrase1 phrase2\" term");
    // note the parens in this next assertion differ from the original
    // QueryParser behavior
    assertQueryEquals("term AND NOT phrase term", qpAnalyzer,
                      "(+term -\"phrase1 phrase2\") term");
    assertQueryEquals("stop", qpAnalyzer, "");
    assertQueryEquals("stop OR stop AND stop", qpAnalyzer, "");
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
    return DateTools.dateToString(df.parse(s), DateTools.Resolution.DAY);
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

    assertQueryEquals("a:b\\-c~", a, "a:b-c~0.5");
    assertQueryEquals("a:b\\+c~", a, "a:b+c~0.5");
    assertQueryEquals("a:b\\:c~", a, "a:b:c~0.5");
    assertQueryEquals("a:b\\\\c~", a, "a:b\\c~0.5");

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
    PrecedenceQueryParser qp = new PrecedenceQueryParser("field", oneStopAnalyzer);
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

    q = getParser(new StandardAnalyzer()).parse("the^3");
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
      getParser(new WhitespaceAnalyzer()).parse("one two three");
      fail("ParseException expected due to too many boolean clauses");
    } catch (ParseException expected) {
      // too many boolean clauses, so ParseException is expected
    }
  }

  /**
   * This test differs from the original QueryParser, showing how the
   * precedence issue has been corrected.
   */
  // failing tests disabled since PrecedenceQueryParser
  // is currently unmaintained
  public void _testPrecedence() throws Exception {
    PrecedenceQueryParser parser = getParser(new WhitespaceAnalyzer());
    Query query1 = parser.parse("A AND B OR C AND D");
    Query query2 = parser.parse("(A AND B) OR (C AND D)");
    assertEquals(query1, query2);

    query1 = parser.parse("A OR B C");
    query2 = parser.parse("A B C");
    assertEquals(query1, query2);

    query1 = parser.parse("A AND B C");
    query2 = parser.parse("(+A +B) C");
    assertEquals(query1, query2);

    query1 = parser.parse("A AND NOT B");
    query2 = parser.parse("+A -B");
    assertEquals(query1, query2);

    query1 = parser.parse("A OR NOT B");
    query2 = parser.parse("A -B");
    assertEquals(query1, query2);

    query1 = parser.parse("A OR NOT B AND C");
    query2 = parser.parse("A (-B +C)");
    assertEquals(query1, query2);
  }


  public void tearDown() {
    BooleanQuery.setMaxClauseCount(originalMaxClauses);
  }

}
