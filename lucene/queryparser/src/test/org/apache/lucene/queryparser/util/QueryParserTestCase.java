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
import java.text.DateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.TimeZone;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.queryparser.classic.QueryParserBase;
import org.apache.lucene.queryparser.flexible.standard.CommonQueryParserConfiguration;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/** Utilities and so on for testing queryparsers */
public abstract class QueryParserTestCase extends LuceneTestCase {
  public static Analyzer qpAnalyzer;

  @BeforeClass
  public static void beforeClass() {
    qpAnalyzer = new QPTestAnalyzer();
  }

  @AfterClass
  public static void afterClass() {
    qpAnalyzer = null;
  }

  /**
   * Filter which discards the token 'stop' and which expands the
   * token 'phrase' into 'phrase1 phrase2'
   */
  public static final class QPTestFilter extends TokenFilter {
    CharTermAttribute termAtt;
    OffsetAttribute offsetAtt;
        
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

  /** Filters MockTokenizer with StopFilter. */
  public static final class QPTestAnalyzer extends Analyzer {
    @Override
    public TokenStreamComponents createComponents(String fieldName) {
      Tokenizer tokenizer = new MockTokenizer(MockTokenizer.SIMPLE, true);
      return new TokenStreamComponents(tokenizer, new QPTestFilter(tokenizer));
    }
  }
  
  /**
   * Mock collation analyzer: indexes terms as "collated" + term
   */
  public final class MockCollationFilter extends TokenFilter {
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
  
  /** Filters whitespace with MockCollationFilter */
  public final class MockCollationAnalyzer extends Analyzer {
    @Override
    public TokenStreamComponents createComponents(String fieldName) {
      Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, true);
      return new TokenStreamComponents(tokenizer, new MockCollationFilter(tokenizer));
    }
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
  public final class Analyzer1 extends Analyzer {
    @Override
    public TokenStreamComponents createComponents(String fieldName) {
      Tokenizer tokenizer = new MockTokenizer( MockTokenizer.WHITESPACE, true);
      return new TokenStreamComponents(tokenizer, new MockSynonymFilter(tokenizer));
    }
  }
  
  /** whitespace+lowercase analyzer without synonyms */
  public final class Analyzer2 extends Analyzer {
    @Override
    public TokenStreamComponents createComponents(String fieldName) {
      return new TokenStreamComponents(new MockTokenizer(MockTokenizer.WHITESPACE, true));
    }
  }
  
  //individual CJK chars as terms, like StandardAnalyzer
  public static class SimpleCJKTokenizer extends Tokenizer {
    private CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

    public SimpleCJKTokenizer() {
      super();
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

  public class SimpleCJKAnalyzer extends Analyzer {
    @Override
    public TokenStreamComponents createComponents(String fieldName) {
      return new TokenStreamComponents(new SimpleCJKTokenizer());
    }
  }

  private int originalMaxClauses;
  
  private String defaultField = "field";
  
  protected String getDefaultField() {
    return defaultField;
  }

  protected void setDefaultField(String defaultField) {
    this.defaultField = defaultField;
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    originalMaxClauses = BooleanQuery.getMaxClauseCount();
  }

  @Override
  public void tearDown() throws Exception {
    BooleanQuery.setMaxClauseCount(originalMaxClauses);
    super.tearDown();
  }
  
  protected String escapeDateString(String s) {
    if (s.indexOf(" ") > -1) {
      return "\"" + s + "\"";
    } else {
      return s;
    }
  }
  
  /** for testing DateTools support */
  protected String getDate(String s, DateTools.Resolution resolution) throws Exception {
    // we use the default Locale since LuceneTestCase randomizes it
    DateFormat df = DateFormat.getDateInstance(DateFormat.SHORT, Locale.getDefault());
    return getDate(df.parse(s), resolution);      
  }
  
  /** for testing DateTools support */
  protected String getDate(Date d, DateTools.Resolution resolution) {
     return DateTools.dateToString(d, resolution);
  }
  
  public void assertDateRangeQueryEquals(CommonQueryParserConfiguration cqpC, String field, String startDate, String endDate, 
                                         Date endDateInclusive, DateTools.Resolution resolution) throws Exception {
    assertQueryEquals(cqpC, field, field + ":[" + escapeDateString(startDate) + " TO " + escapeDateString(endDate) + "]",
               "[" + getDate(startDate, resolution) + " TO " + getDate(endDateInclusive, resolution) + "]");
    assertQueryEquals(cqpC, field, field + ":{" + escapeDateString(startDate) + " TO " + escapeDateString(endDate) + "}",
               "{" + getDate(startDate, resolution) + " TO " + getDate(endDate, resolution) + "}");
  }
  
  /** formats the given year+month+day as a localized date in the gregorian calendar */
  protected String getLocalizedDate(int year, int month, int day) {
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

  public void assertQueryEquals(String query, Analyzer a, String result) throws Exception {
    Query q = getQuery(query, a);
    assertEquals(result, q.toString("field"));
  }

  public void assertQueryEquals(CommonQueryParserConfiguration cqpC, String field, String query, String result) throws Exception {
    Query q = getQuery(query, cqpC);
    assertEquals(result, q.toString(field));
  }

  public void assertQueryEquals(Query expected, Query test) {
    assertEquals(expected.toString(), test.toString());
  }

  
  public void assertEscapedQueryEquals(String query, Analyzer a, String result) throws Exception {
    assertEquals(result, QueryParserBase.escape(query));
  }

  public void assertWildcardQueryEquals(String query, boolean lowercase, String result, boolean allowLeadingWildcard) throws Exception {
    CommonQueryParserConfiguration cqpC = getParserConfig(null);
    cqpC.setLowercaseExpandedTerms(lowercase);
    cqpC.setAllowLeadingWildcard(allowLeadingWildcard);
    Query q = getQuery(query, cqpC);
    assertEquals(result, q.toString("field"));
  }

  public void assertWildcardQueryEquals(String query, boolean lowercase, String result) throws Exception {
    assertWildcardQueryEquals(query, lowercase, result, false);
  }

  public void assertWildcardQueryEquals(String query, String result) throws Exception {
    Query q = getQuery(query);
    assertEquals(result, q.toString("field"));
  }

  public void assertFuzzyQueryEquals(String field, String term, int maxEdits, int prefixLen, Query query) {
    assert(query instanceof FuzzyQuery);
    FuzzyQuery fq = (FuzzyQuery)query;
    assertEquals(field, fq.getField());
    assertEquals(term, fq.getTerm().text());
    assertEquals(maxEdits, fq.getMaxEdits());
    assertEquals(prefixLen, fq.getPrefixLength());
  }
  
  @SuppressWarnings("rawtypes")
  public void assertInstanceOf(Query q, Class other) {
    assertTrue(q.getClass().isAssignableFrom(other));
  }
  
  public void assertEmpty(Query q) {
    boolean e = false;
    if (q instanceof BooleanQuery && ((BooleanQuery)q).getClauses().length == 0) {
      e = true;
    }
    assertTrue("Empty: "+q.toString(), e);
  }
  public Query getQueryDOA(String query, Analyzer a) throws Exception {
    if (a == null) {
      a = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true);
    }
    CommonQueryParserConfiguration qp = getParserConfig(a);
    setDefaultOperatorAND(qp);
    return getQuery(query, qp);
  }

  public void assertQueryEqualsDOA(String query, Analyzer a, String result) throws Exception {
    Query q = getQueryDOA(query, a);
    assertEquals(result, q.toString("field"));
  }
  
  public void assertParseException(String queryString) throws Exception {
    try {
      getQuery(queryString);
      fail("ParseException expected, not thrown");
    } catch (Exception expected) {
      assertTrue(isQueryParserException(expected));
    }
  }

  public void assertParseException(String queryString, Analyzer a) throws Exception {
    try {
      getQuery(queryString, a);
      fail("ParseException expected, not thrown");
    } catch (Exception expected) {
      assertTrue(isQueryParserException(expected));
    }
  }
}
