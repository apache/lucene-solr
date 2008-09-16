package org.apache.lucene.queryParser;

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

import java.io.IOException;
import java.io.Reader;
import java.text.DateFormat;
import java.text.Collator;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.analysis.LowerCaseTokenizer;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.analysis.StopAnalyzer;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.DateField;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreRangeQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RangeQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.LuceneTestCase;

/**
 * Tests QueryParser.
 */
public class TestQueryParser extends LuceneTestCase {

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
        return reusableToken.reinit("phrase2", savedStart, savedEnd);
      } else
        for (Token nextToken = input.next(reusableToken); nextToken != null; nextToken = input.next(reusableToken)) {
          if (nextToken.term().equals("phrase")) {
            inPhrase = true;
            savedStart = nextToken.startOffset();
            savedEnd = nextToken.endOffset();
            return nextToken.reinit("phrase1", savedStart, savedEnd);
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

  public static class QPTestParser extends QueryParser {
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

  public void setUp() throws Exception {
    super.setUp();
    originalMaxClauses = BooleanQuery.getMaxClauseCount();
  }

  public QueryParser getParser(Analyzer a) throws Exception {
    if (a == null)
      a = new SimpleAnalyzer();
    QueryParser qp = new QueryParser("field", a);
    qp.setDefaultOperator(QueryParser.OR_OPERATOR);
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

  public void assertQueryEquals(QueryParser qp, String field, String query, String result) 
    throws Exception {
    Query q = qp.parse(query);
    String s = q.toString(field);
    if (!s.equals(result)) {
      fail("Query /" + query + "/ yielded /" + s
           + "/, expecting /" + result + "/");
    }
  }
  
  public void assertEscapedQueryEquals(String query, Analyzer a, String result)
    throws Exception {
    String escapedQuery = QueryParser.escape(query);
    if (!escapedQuery.equals(result)) {
      fail("Query /" + query + "/ yielded /" + escapedQuery
          + "/, expecting /" + result + "/");
    }
  }

  public void assertWildcardQueryEquals(String query, boolean lowercase, String result, boolean allowLeadingWildcard)
    throws Exception {
    QueryParser qp = getParser(null);
    qp.setLowercaseExpandedTerms(lowercase);
    qp.setAllowLeadingWildcard(allowLeadingWildcard);
    Query q = qp.parse(query);
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
    QueryParser qp = getParser(null);
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
    QueryParser qp = new QueryParser("field", a);
    qp.setDefaultOperator(QueryParser.AND_OPERATOR);
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
    assertQueryEquals("türm term term", new WhitespaceAnalyzer(), "türm term term");
    assertQueryEquals("ümlaut", new WhitespaceAnalyzer(), "ümlaut");

    assertQueryEquals("\"\"", new KeywordAnalyzer(), "");
    assertQueryEquals("foo:\"\"", new KeywordAnalyzer(), "foo:");

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
    
    QueryParser qp = new QueryParser("field", new StandardAnalyzer());
    // make sure OR is the default:
    assertEquals(QueryParser.OR_OPERATOR, qp.getDefaultOperator());
    qp.setDefaultOperator(QueryParser.AND_OPERATOR);
    assertEquals(QueryParser.AND_OPERATOR, qp.getDefaultOperator());
    qp.setDefaultOperator(QueryParser.OR_OPERATOR);
    assertEquals(QueryParser.OR_OPERATOR, qp.getDefaultOperator());
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
    assertQueryEquals("term~", null, "term~0.5");
    assertQueryEquals("term~0.7", null, "term~0.7");
    assertQueryEquals("term~^2", null, "term~0.5^2.0");
    assertQueryEquals("term^2~", null, "term~0.5^2.0");
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
    
    assertParseException("term~1.1"); // value > 1, throws exception

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
    // Test suffix queries: first disallow
    try {
      assertWildcardQueryEquals("*Term", true, "*term");
      fail();
    } catch(ParseException pe) {
      // expected exception
    }
    try {
      assertWildcardQueryEquals("?Term", true, "?term");
      fail();
    } catch(ParseException pe) {
      // expected exception
    }
    // Test suffix queries: then allow
    assertWildcardQueryEquals("*Term", true, "*term", true);
    assertWildcardQueryEquals("?Term", true, "?term", true);
  }
  
  public void testLeadingWildcardType() throws Exception {
    QueryParser qp = getParser(null);
    qp.setAllowLeadingWildcard(true);
    assertEquals(WildcardQuery.class, qp.parse("t*erm*").getClass());
    assertEquals(WildcardQuery.class, qp.parse("?term*").getClass());
    assertEquals(WildcardQuery.class, qp.parse("*term*").getClass());
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
                      "term \"phrase1 phrase2\" term");
    assertQueryEquals("term AND NOT phrase term", qpAnalyzer,
                      "+term -\"phrase1 phrase2\" term");
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
    assertTrue(getQuery("[ a TO z]", null) instanceof ConstantScoreRangeQuery);

    QueryParser qp = new QueryParser("field", new SimpleAnalyzer());
	qp.setUseOldRangeQuery(true);
    assertTrue(qp.parse("[ a TO z]") instanceof RangeQuery);
    
    assertQueryEquals("[ a TO z ]", null, "[a TO z]");
    assertQueryEquals("{ a TO z}", null, "{a TO z}");
    assertQueryEquals("{ a TO z }", null, "{a TO z}");
    assertQueryEquals("{ a TO z }^2.0", null, "{a TO z}^2.0");
    assertQueryEquals("[ a TO z] OR bar", null, "[a TO z] bar");
    assertQueryEquals("[ a TO z] AND bar", null, "+[a TO z] +bar");
    assertQueryEquals("( bar blar { a TO z}) ", null, "bar blar {a TO z}");
    assertQueryEquals("gack ( bar blar { a TO z}) ", null, "gack (bar blar {a TO z})");
  }
    
  public void testFarsiRangeCollating() throws Exception {
    
    RAMDirectory ramDir = new RAMDirectory();
    IndexWriter iw = new IndexWriter(ramDir, new WhitespaceAnalyzer(), true, 
                                     IndexWriter.MaxFieldLength.LIMITED);
    Document doc = new Document();
    doc.add(new Field("content","\u0633\u0627\u0628", 
                      Field.Store.YES, Field.Index.UN_TOKENIZED));
    iw.addDocument(doc);
    iw.close();
    IndexSearcher is = new IndexSearcher(ramDir);

    QueryParser qp = new QueryParser("content", new WhitespaceAnalyzer());

    // Neither Java 1.4.2 nor 1.5.0 has Farsi Locale collation available in
    // RuleBasedCollator.  However, the Arabic Locale seems to order the Farsi
    // characters properly.
    Collator c = Collator.getInstance(new Locale("ar"));
    qp.setRangeCollator(c);

    // Unicode order would include U+0633 in [ U+062F - U+0698 ], but Farsi
    // orders the U+0698 character before the U+0633 character, so the single
    // index Term below should NOT be returned by a ConstantScoreRangeQuery
    // with a Farsi Collator (or an Arabic one for the case when Farsi is not
    // supported).
      
    // Test ConstantScoreRangeQuery
    qp.setUseOldRangeQuery(false);
    ScoreDoc[] result = is.search(qp.parse("[ \u062F TO \u0698 ]"), null, 1000).scoreDocs;
    assertEquals("The index Term should not be included.", 0, result.length);

    result = is.search(qp.parse("[ \u0633 TO \u0638 ]"), null, 1000).scoreDocs;
    assertEquals("The index Term should be included.", 1, result.length);

    // Test RangeQuery
    qp.setUseOldRangeQuery(true);
    result = is.search(qp.parse("[ \u062F TO \u0698 ]"), null, 1000).scoreDocs;
    assertEquals("The index Term should not be included.", 0, result.length);

    result = is.search(qp.parse("[ \u0633 TO \u0638 ]"), null, 1000).scoreDocs;
    assertEquals("The index Term should be included.", 1, result.length);

    is.close();
  }
  
  /** for testing legacy DateField support */
  private String getLegacyDate(String s) throws Exception {
    DateFormat df = DateFormat.getDateInstance(DateFormat.SHORT);
    return DateField.dateToString(df.parse(s));
  }

  /** for testing DateTools support */
  private String getDate(String s, DateTools.Resolution resolution) throws Exception {
    DateFormat df = DateFormat.getDateInstance(DateFormat.SHORT);
    return getDate(df.parse(s), resolution);      
  }
  
  /** for testing DateTools support */
  private String getDate(Date d, DateTools.Resolution resolution) throws Exception {
      if (resolution == null) {
        return DateField.dateToString(d);      
      } else {
        return DateTools.dateToString(d, resolution);
      }
    }
  
  private String getLocalizedDate(int year, int month, int day, boolean extendLastDate) {
    DateFormat df = DateFormat.getDateInstance(DateFormat.SHORT);
    Calendar calendar = Calendar.getInstance();
    calendar.set(year, month, day);
    if (extendLastDate) {
      calendar.set(Calendar.HOUR_OF_DAY, 23);
      calendar.set(Calendar.MINUTE, 59);
      calendar.set(Calendar.SECOND, 59);
      calendar.set(Calendar.MILLISECOND, 999);
    }
    return df.format(calendar.getTime());
  }

  /** for testing legacy DateField support */
  public void testLegacyDateRange() throws Exception {
    String startDate = getLocalizedDate(2002, 1, 1, false);
    String endDate = getLocalizedDate(2002, 1, 4, false);
    Calendar endDateExpected = Calendar.getInstance();
    endDateExpected.set(2002, 1, 4, 23, 59, 59);
    endDateExpected.set(Calendar.MILLISECOND, 999);
    assertQueryEquals("[ " + startDate + " TO " + endDate + "]", null,
                      "[" + getLegacyDate(startDate) + " TO " + DateField.dateToString(endDateExpected.getTime()) + "]");
    assertQueryEquals("{  " + startDate + "    " + endDate + "   }", null,
                      "{" + getLegacyDate(startDate) + " TO " + getLegacyDate(endDate) + "}");
  }
  
  public void testDateRange() throws Exception {
    String startDate = getLocalizedDate(2002, 1, 1, false);
    String endDate = getLocalizedDate(2002, 1, 4, false);
    Calendar endDateExpected = Calendar.getInstance();
    endDateExpected.set(2002, 1, 4, 23, 59, 59);
    endDateExpected.set(Calendar.MILLISECOND, 999);
    final String defaultField = "default";
    final String monthField = "month";
    final String hourField = "hour";
    QueryParser qp = new QueryParser("field", new SimpleAnalyzer());
    
    // Don't set any date resolution and verify if DateField is used
    assertDateRangeQueryEquals(qp, defaultField, startDate, endDate, 
                               endDateExpected.getTime(), null);
    
    // set a field specific date resolution
    qp.setDateResolution(monthField, DateTools.Resolution.MONTH);
    
    // DateField should still be used for defaultField
    assertDateRangeQueryEquals(qp, defaultField, startDate, endDate, 
                               endDateExpected.getTime(), null);
    
    // set default date resolution to MILLISECOND 
    qp.setDateResolution(DateTools.Resolution.MILLISECOND);
    
    // set second field specific date resolution    
    qp.setDateResolution(hourField, DateTools.Resolution.HOUR);

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
  
  public void assertDateRangeQueryEquals(QueryParser qp, String field, String startDate, String endDate, 
                                         Date endDateInclusive, DateTools.Resolution resolution) throws Exception {
    assertQueryEquals(qp, field, field + ":[" + startDate + " TO " + endDate + "]",
               "[" + getDate(startDate, resolution) + " TO " + getDate(endDateInclusive, resolution) + "]");
    assertQueryEquals(qp, field, field + ":{" + startDate + " TO " + endDate + "}",
               "{" + getDate(startDate, resolution) + " TO " + getDate(endDate, resolution) + "}");
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

    assertQueryEquals("a:b\\-c~", a, "a:b-c~0.5");
    assertQueryEquals("a:b\\+c~", a, "a:b+c~0.5");
    assertQueryEquals("a:b\\:c~", a, "a:b:c~0.5");
    assertQueryEquals("a:b\\\\c~", a, "a:b\\c~0.5");

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

  public void testQueryStringEscaping() throws Exception {
    Analyzer a = new WhitespaceAnalyzer();

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

    QueryParser qp2 = new QueryParser("field", new StandardAnalyzer());
    q = qp2.parse("the^3");
    // "the" is a stop word so the result is an empty query:
    assertNotNull(q);
    assertEquals("", q.toString());
    assertEquals(1.0f, q.getBoost(), 0.01f);
  }

  public void assertParseException(String queryString) throws Exception {
    try {
      Query q = getQuery(queryString, null);
    } catch (ParseException expected) {
      return;
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
  

  public void testCustomQueryParserWildcard() {
    try {
      new QPTestParser("contents", new WhitespaceAnalyzer()).parse("a?t");
      fail("Wildcard queries should not be allowed");
    } catch (ParseException expected) {
      // expected exception
    }
  }

  public void testCustomQueryParserFuzzy() throws Exception {
    try {
      new QPTestParser("contents", new WhitespaceAnalyzer()).parse("xunit~");
      fail("Fuzzy queries should not be allowed");
    } catch (ParseException expected) {
      // expected exception
    }
  }

  public void testBooleanQuery() throws Exception {
    BooleanQuery.setMaxClauseCount(2);
    try {
      QueryParser qp = new QueryParser("field", new WhitespaceAnalyzer());
      qp.parse("one two three");
      fail("ParseException expected due to too many boolean clauses");
    } catch (ParseException expected) {
      // too many boolean clauses, so ParseException is expected
    }
  }

  /**
   * This test differs from TestPrecedenceQueryParser
   */
  public void testPrecedence() throws Exception {
    QueryParser qp = new QueryParser("field", new WhitespaceAnalyzer());
    Query query1 = qp.parse("A AND B OR C AND D");
    Query query2 = qp.parse("+A +B +C +D");
    assertEquals(query1, query2);
  }

  public void testLocalDateFormat() throws IOException, ParseException {
    RAMDirectory ramDir = new RAMDirectory();
    IndexWriter iw = new IndexWriter(ramDir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
    addDateDoc("a", 2005, 12, 2, 10, 15, 33, iw);
    addDateDoc("b", 2005, 12, 4, 22, 15, 00, iw);
    iw.close();
    IndexSearcher is = new IndexSearcher(ramDir);
    assertHits(1, "[12/1/2005 TO 12/3/2005]", is);
    assertHits(2, "[12/1/2005 TO 12/4/2005]", is);
    assertHits(1, "[12/3/2005 TO 12/4/2005]", is);
    assertHits(1, "{12/1/2005 TO 12/3/2005}", is);
    assertHits(1, "{12/1/2005 TO 12/4/2005}", is);
    assertHits(0, "{12/3/2005 TO 12/4/2005}", is);
    is.close();
  }

  public void testStarParsing() throws Exception {
    final int[] type = new int[1];
    QueryParser qp = new QueryParser("field", new WhitespaceAnalyzer()) {
      protected Query getWildcardQuery(String field, String termStr) throws ParseException {
        // override error checking of superclass
        type[0]=1;
        return new TermQuery(new Term(field,termStr));
      }
      protected Query getPrefixQuery(String field, String termStr) throws ParseException {
        // override error checking of superclass
        type[0]=2;        
        return new TermQuery(new Term(field,termStr));
      }

      protected Query getFieldQuery(String field, String queryText) throws ParseException {
        type[0]=3;
        return super.getFieldQuery(field, queryText);
      }
    };

    TermQuery tq;

    tq = (TermQuery)qp.parse("foo:zoo*");
    assertEquals("zoo",tq.getTerm().text());
    assertEquals(2,type[0]);

    tq = (TermQuery)qp.parse("foo:zoo*^2");
    assertEquals("zoo",tq.getTerm().text());
    assertEquals(2,type[0]);
    assertEquals(tq.getBoost(),2,0);

    tq = (TermQuery)qp.parse("foo:*");
    assertEquals("*",tq.getTerm().text());
    assertEquals(1,type[0]);  // could be a valid prefix query in the future too

    tq = (TermQuery)qp.parse("foo:*^2");
    assertEquals("*",tq.getTerm().text());
    assertEquals(1,type[0]);
    assertEquals(tq.getBoost(),2,0);    

    tq = (TermQuery)qp.parse("*:foo");
    assertEquals("*",tq.getTerm().field());
    assertEquals("foo",tq.getTerm().text());
    assertEquals(3,type[0]);

    tq = (TermQuery)qp.parse("*:*");
    assertEquals("*",tq.getTerm().field());
    assertEquals("*",tq.getTerm().text());
    assertEquals(1,type[0]);  // could be handled as a prefix query in the future

     tq = (TermQuery)qp.parse("(*:*)");
    assertEquals("*",tq.getTerm().field());
    assertEquals("*",tq.getTerm().text());
    assertEquals(1,type[0]);

  }

  public void testStopwords() throws Exception {
    QueryParser qp = new QueryParser("a", new StopAnalyzer(new String[]{"the", "foo"}));
    Query result = qp.parse("a:the OR a:foo");
    assertNotNull("result is null and it shouldn't be", result);
    assertTrue("result is not a BooleanQuery", result instanceof BooleanQuery);
    assertTrue(((BooleanQuery) result).clauses().size() + " does not equal: " + 0, ((BooleanQuery) result).clauses().size() == 0);
    result = qp.parse("a:woo OR a:the");
    assertNotNull("result is null and it shouldn't be", result);
    assertTrue("result is not a TermQuery", result instanceof TermQuery);
    result = qp.parse("(fieldX:xxxxx OR fieldy:xxxxxxxx)^2 AND (fieldx:the OR fieldy:foo)");
    assertNotNull("result is null and it shouldn't be", result);
    assertTrue("result is not a BooleanQuery", result instanceof BooleanQuery);
    System.out.println("Result: " + result);
    assertTrue(((BooleanQuery) result).clauses().size() + " does not equal: " + 2, ((BooleanQuery) result).clauses().size() == 2);
  }

  public void testPositionIncrement() throws Exception {
    boolean dflt = StopFilter.getEnablePositionIncrementsDefault();
    StopFilter.setEnablePositionIncrementsDefault(true);
    try {
      QueryParser qp = new QueryParser("a", new StopAnalyzer(new String[]{"the", "in", "are", "this"}));
      qp.setEnablePositionIncrements(true);
      String qtxt = "\"the words in poisitions pos02578 are stopped in this phrasequery\"";
      //               0         2                      5           7  8
      int expectedPositions[] = {1,3,4,6,9};
      PhraseQuery pq = (PhraseQuery) qp.parse(qtxt);
      //System.out.println("Query text: "+qtxt);
      //System.out.println("Result: "+pq);
      Term t[] = pq.getTerms();
      int pos[] = pq.getPositions();
      for (int i = 0; i < t.length; i++) {
        //System.out.println(i+". "+t[i]+"  pos: "+pos[i]);
        assertEquals("term "+i+" = "+t[i]+" has wrong term-position!",expectedPositions[i],pos[i]);
      }
    } finally {
      StopFilter.setEnablePositionIncrementsDefault(dflt);
    }
  }

  public void testMatchAllDocs() throws Exception {
    QueryParser qp = new QueryParser("field", new WhitespaceAnalyzer());
    assertEquals(new MatchAllDocsQuery(), qp.parse("*:*"));
    assertEquals(new MatchAllDocsQuery(), qp.parse("(*:*)"));
    BooleanQuery bq = (BooleanQuery)qp.parse("+*:* -*:*");
    assertTrue(bq.getClauses()[0].getQuery() instanceof MatchAllDocsQuery);
    assertTrue(bq.getClauses()[1].getQuery() instanceof MatchAllDocsQuery);
  }
  
  private void assertHits(int expected, String query, IndexSearcher is) throws ParseException, IOException {
    QueryParser qp = new QueryParser("date", new WhitespaceAnalyzer());
    qp.setLocale(Locale.ENGLISH);
    Query q = qp.parse(query);
    ScoreDoc[] hits = is.search(q, null, 1000).scoreDocs;
    assertEquals(expected, hits.length);
  }

  private static void addDateDoc(String content, int year, int month,
      int day, int hour, int minute, int second, IndexWriter iw) throws IOException {
    Document d = new Document();
    d.add(new Field("f", content, Field.Store.YES, Field.Index.ANALYZED));
    Calendar cal = Calendar.getInstance();
    cal.set(year, month-1, day, hour, minute, second);
    d.add(new Field("date", DateField.dateToString(cal.getTime()), Field.Store.YES, Field.Index.NOT_ANALYZED));
    iw.addDocument(d);
  }

  public void tearDown() throws Exception {
    super.tearDown();
    BooleanQuery.setMaxClauseCount(originalMaxClauses);
  }

}
