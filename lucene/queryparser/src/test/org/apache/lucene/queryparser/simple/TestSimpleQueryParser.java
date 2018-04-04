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
package org.apache.lucene.queryparser.simple;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.automaton.LevenshteinAutomata;

import static org.apache.lucene.queryparser.simple.SimpleQueryParser.AND_OPERATOR;
import static org.apache.lucene.queryparser.simple.SimpleQueryParser.ESCAPE_OPERATOR;
import static org.apache.lucene.queryparser.simple.SimpleQueryParser.FUZZY_OPERATOR;
import static org.apache.lucene.queryparser.simple.SimpleQueryParser.NOT_OPERATOR;
import static org.apache.lucene.queryparser.simple.SimpleQueryParser.OR_OPERATOR;
import static org.apache.lucene.queryparser.simple.SimpleQueryParser.PHRASE_OPERATOR;
import static org.apache.lucene.queryparser.simple.SimpleQueryParser.PRECEDENCE_OPERATORS;
import static org.apache.lucene.queryparser.simple.SimpleQueryParser.PREFIX_OPERATOR;
import static org.apache.lucene.queryparser.simple.SimpleQueryParser.NEAR_OPERATOR;
import static org.apache.lucene.queryparser.simple.SimpleQueryParser.WHITESPACE_OPERATOR;

/** Tests for {@link SimpleQueryParser} */
public class TestSimpleQueryParser extends LuceneTestCase {

  /**
   * helper to parse a query with whitespace+lowercase analyzer across "field",
   * with default operator of MUST
   */
  private Query parse(String text) {
    Analyzer analyzer = new MockAnalyzer(random());
    SimpleQueryParser parser = new SimpleQueryParser(analyzer, "field");
    parser.setDefaultOperator(Occur.MUST);
    return parser.parse(text);
  }

  /**
   * helper to parse a query with whitespace+lowercase analyzer across "field",
   * with default operator of MUST
   */
  private Query parse(String text, int flags) {
    Analyzer analyzer = new MockAnalyzer(random());
    SimpleQueryParser parser = new SimpleQueryParser(analyzer,
        Collections.singletonMap("field", 1f), flags);
    parser.setDefaultOperator(Occur.MUST);
    return parser.parse(text);
  }

  /** test a simple term */
  public void testTerm() throws Exception {
    Query expected = new TermQuery(new Term("field", "foobar"));

    assertEquals(expected, parse("foobar"));
  }

  /** test a fuzzy query */
  public void testFuzzy() throws Exception {
    Query regular = new TermQuery(new Term("field", "foobar"));
    Query expected = new FuzzyQuery(new Term("field", "foobar"), 2);

    assertEquals(expected, parse("foobar~2"));
    assertEquals(expected, parse("foobar~"));
    assertEquals(regular, parse("foobar~a"));
    assertEquals(regular, parse("foobar~1a"));

    BooleanQuery.Builder bool = new BooleanQuery.Builder();
    FuzzyQuery fuzzy = new FuzzyQuery(new Term("field", "foo"), LevenshteinAutomata.MAXIMUM_SUPPORTED_DISTANCE);
    bool.add(fuzzy, Occur.MUST);
    bool.add(new TermQuery(new Term("field", "bar")), Occur.MUST);

    assertEquals(bool.build(), parse("foo~" + LevenshteinAutomata.MAXIMUM_SUPPORTED_DISTANCE + 1 + " bar"));
  }

  /** test a simple phrase */
  public void testPhrase() throws Exception {
    PhraseQuery expected = new PhraseQuery("field", "foo", "bar");

    assertEquals(expected, parse("\"foo bar\""));
  }

  /** test a simple phrase with various slop settings */
  public void testPhraseWithSlop() throws Exception {
    PhraseQuery expectedWithSlop = new PhraseQuery(2, "field", "foo", "bar");

    assertEquals(expectedWithSlop, parse("\"foo bar\"~2"));

    PhraseQuery expectedWithMultiDigitSlop = new PhraseQuery(10, "field", "foo", "bar");

    assertEquals(expectedWithMultiDigitSlop, parse("\"foo bar\"~10"));

    PhraseQuery expectedNoSlop = new PhraseQuery("field", "foo", "bar");

    assertEquals("Ignore trailing tilde with no slop", expectedNoSlop, parse("\"foo bar\"~"));
    assertEquals("Ignore non-numeric trailing slop", expectedNoSlop, parse("\"foo bar\"~a"));
    assertEquals("Ignore non-numeric trailing slop", expectedNoSlop, parse("\"foo bar\"~1a"));
    assertEquals("Ignore negative trailing slop", expectedNoSlop, parse("\"foo bar\"~-1"));

    PhraseQuery pq = new PhraseQuery(12, "field", "foo", "bar");

    BooleanQuery.Builder expectedBoolean = new BooleanQuery.Builder();
    expectedBoolean.add(pq, Occur.MUST);
    expectedBoolean.add(new TermQuery(new Term("field", "baz")), Occur.MUST);

    assertEquals(expectedBoolean.build(), parse("\"foo bar\"~12 baz"));
  }

  /** test a simple prefix */
  public void testPrefix() throws Exception {
    PrefixQuery expected = new PrefixQuery(new Term("field", "foobar"));

    assertEquals(expected, parse("foobar*"));
  }

  /** test some AND'd terms using '+' operator */
  public void testAND() throws Exception {
    BooleanQuery.Builder expected = new BooleanQuery.Builder();
    expected.add(new TermQuery(new Term("field", "foo")), Occur.MUST);
    expected.add(new TermQuery(new Term("field", "bar")), Occur.MUST);

    assertEquals(expected.build(), parse("foo+bar"));
  }

  /** test some AND'd phrases using '+' operator */
  public void testANDPhrase() throws Exception {
    PhraseQuery phrase1 = new PhraseQuery("field", "foo", "bar");
    PhraseQuery phrase2 = new PhraseQuery("field", "star", "wars");
    BooleanQuery.Builder expected = new BooleanQuery.Builder();
    expected.add(phrase1, Occur.MUST);
    expected.add(phrase2, Occur.MUST);

    assertEquals(expected.build(), parse("\"foo bar\"+\"star wars\""));
  }

  /** test some AND'd terms (just using whitespace) */
  public void testANDImplicit() throws Exception {
    BooleanQuery.Builder expected = new BooleanQuery.Builder();
    expected.add(new TermQuery(new Term("field", "foo")), Occur.MUST);
    expected.add(new TermQuery(new Term("field", "bar")), Occur.MUST);

    assertEquals(expected.build(), parse("foo bar"));
  }

  /** test some OR'd terms */
  public void testOR() throws Exception {
    BooleanQuery.Builder expected = new BooleanQuery.Builder();
    expected.add(new TermQuery(new Term("field", "foo")), Occur.SHOULD);
    expected.add(new TermQuery(new Term("field", "bar")), Occur.SHOULD);

    assertEquals(expected.build(), parse("foo|bar"));
    assertEquals(expected.build(), parse("foo||bar"));
  }

  /** test some OR'd terms (just using whitespace) */
  public void testORImplicit() throws Exception {
    BooleanQuery.Builder expected = new BooleanQuery.Builder();
    expected.add(new TermQuery(new Term("field", "foo")), Occur.SHOULD);
    expected.add(new TermQuery(new Term("field", "bar")), Occur.SHOULD);

    SimpleQueryParser parser = new SimpleQueryParser(new MockAnalyzer(random()), "field");
    assertEquals(expected.build(), parser.parse("foo bar"));
  }

  /** test some OR'd phrases using '|' operator */
  public void testORPhrase() throws Exception {
    PhraseQuery phrase1 = new PhraseQuery("field", "foo", "bar");
    PhraseQuery phrase2 = new PhraseQuery("field", "star", "wars");
    BooleanQuery.Builder expected = new BooleanQuery.Builder();
    expected.add(phrase1, Occur.SHOULD);
    expected.add(phrase2, Occur.SHOULD);

    assertEquals(expected.build(), parse("\"foo bar\"|\"star wars\""));
  }

  /** test negated term */
  public void testNOT() throws Exception {
    BooleanQuery.Builder expected = new BooleanQuery.Builder();
    expected.add(new TermQuery(new Term("field", "foo")), Occur.MUST_NOT);
    expected.add(new MatchAllDocsQuery(), Occur.SHOULD);

    assertEquals(expected.build(), parse("-foo"));
    assertEquals(expected.build(), parse("-(foo)"));
    assertEquals(expected.build(), parse("---foo"));
  }

  /** test crazy prefixes with multiple asterisks */
  public void testCrazyPrefixes1() throws Exception {
    Query expected = new PrefixQuery(new Term("field", "st*ar"));

    assertEquals(expected, parse("st*ar*"));
  }

  /** test prefixes with some escaping */
  public void testCrazyPrefixes2() throws Exception {
    Query expected = new PrefixQuery(new Term("field", "st*ar\\*"));

    assertEquals(expected, parse("st*ar\\\\**"));
  }

  /** not a prefix query! the prefix operator is escaped */
  public void testTermInDisguise() throws Exception {
    Query expected = new TermQuery(new Term("field", "st*ar\\*"));

    assertEquals(expected, parse("sT*Ar\\\\\\*"));
  }

  // a number of test cases here have garbage/errors in
  // the syntax passed in to test that the query can
  // still be interpreted as a guess to what the human
  // input was trying to be

  public void testGarbageTerm() throws Exception {
    Query expected = new TermQuery(new Term("field", "star"));

    assertEquals(expected, parse("star"));
    assertEquals(expected, parse("star\n"));
    assertEquals(expected, parse("star\r"));
    assertEquals(expected, parse("star\t"));
    assertEquals(expected, parse("star("));
    assertEquals(expected, parse("star)"));
    assertEquals(expected, parse("star\""));
    assertEquals(expected, parse("\t \r\n\nstar   \n \r \t "));
    assertEquals(expected, parse("- + \"\" - star \\"));
  }

  public void testGarbageEmpty() throws Exception {
    MatchNoDocsQuery expected = new MatchNoDocsQuery();

    assertEquals(expected, parse(""));
    assertEquals(expected, parse("  "));
    assertEquals(expected, parse("  "));
    assertEquals(expected, parse("\\ "));
    assertEquals(expected, parse("\\ \\ "));
    assertEquals(expected, parse("\"\""));
    assertEquals(expected, parse("\" \""));
    assertEquals(expected, parse("\" \"|\" \""));
    assertEquals(expected, parse("(\" \"|\" \")"));
    assertEquals(expected, parse("\" \" \" \""));
    assertEquals(expected, parse("(\" \" \" \")"));
  }

  public void testGarbageAND() throws Exception {
    BooleanQuery.Builder expected = new BooleanQuery.Builder();
    expected.add(new TermQuery(new Term("field", "star")), Occur.MUST);
    expected.add(new TermQuery(new Term("field", "wars")), Occur.MUST);

    assertEquals(expected.build(), parse("star wars"));
    assertEquals(expected.build(), parse("star+wars"));
    assertEquals(expected.build(), parse("     star     wars   "));
    assertEquals(expected.build(), parse("     star +    wars   "));
    assertEquals(expected.build(), parse("  |     star + + |   wars   "));
    assertEquals(expected.build(), parse("  |     star + + |   wars   \\"));
  }

  public void testGarbageOR() throws Exception {
    BooleanQuery.Builder expected = new BooleanQuery.Builder();
    expected.add(new TermQuery(new Term("field", "star")), Occur.SHOULD);
    expected.add(new TermQuery(new Term("field", "wars")), Occur.SHOULD);

    assertEquals(expected.build(), parse("star|wars"));
    assertEquals(expected.build(), parse("     star |    wars   "));
    assertEquals(expected.build(), parse("  |     star | + |   wars   "));
    assertEquals(expected.build(), parse("  +     star | + +   wars   \\"));
  }

  public void testGarbageNOT() throws Exception {
    BooleanQuery.Builder expected = new BooleanQuery.Builder();
    expected.add(new TermQuery(new Term("field", "star")), Occur.MUST_NOT);
    expected.add(new MatchAllDocsQuery(), Occur.SHOULD);

    assertEquals(expected.build(), parse("-star"));
    assertEquals(expected.build(), parse("---star"));
    assertEquals(expected.build(), parse("- -star -"));
  }

  public void testGarbagePhrase() throws Exception {
    PhraseQuery expected = new PhraseQuery("field", "star", "wars");

    assertEquals(expected, parse("\"star wars\""));
    assertEquals(expected, parse("\"star wars\\ \""));
    assertEquals(expected, parse("\"\" | \"star wars\""));
    assertEquals(expected, parse("          \"star wars\"        \"\"\\"));
  }

  public void testGarbageSubquery() throws Exception {
    Query expected = new TermQuery(new Term("field", "star"));

    assertEquals(expected, parse("(star)"));
    assertEquals(expected, parse("(star))"));
    assertEquals(expected, parse("((star)"));
    assertEquals(expected, parse("     -()(star)        \n\n\r     "));
    assertEquals(expected, parse("| + - ( + - |      star    \n      ) \n"));
  }

  public void testCompoundAnd() throws Exception {
    BooleanQuery.Builder expected = new BooleanQuery.Builder();
    expected.add(new TermQuery(new Term("field", "star")), Occur.MUST);
    expected.add(new TermQuery(new Term("field", "wars")), Occur.MUST);
    expected.add(new TermQuery(new Term("field", "empire")), Occur.MUST);

    assertEquals(expected.build(), parse("star wars empire"));
    assertEquals(expected.build(), parse("star+wars + empire"));
    assertEquals(expected.build(), parse(" | --star wars empire \n\\"));
  }

  public void testCompoundOr() throws Exception {
    BooleanQuery.Builder expected = new BooleanQuery.Builder();
    expected.add(new TermQuery(new Term("field", "star")), Occur.SHOULD);
    expected.add(new TermQuery(new Term("field", "wars")), Occur.SHOULD);
    expected.add(new TermQuery(new Term("field", "empire")), Occur.SHOULD);

    assertEquals(expected.build(), parse("star|wars|empire"));
    assertEquals(expected.build(), parse("star|wars | empire"));
    assertEquals(expected.build(), parse(" | --star|wars|empire \n\\"));
  }

  public void testComplex00() throws Exception {
    BooleanQuery.Builder expected = new BooleanQuery.Builder();
    BooleanQuery.Builder inner = new BooleanQuery.Builder();
    inner.add(new TermQuery(new Term("field", "star")), Occur.SHOULD);
    inner.add(new TermQuery(new Term("field", "wars")), Occur.SHOULD);
    expected.add(inner.build(), Occur.MUST);
    expected.add(new TermQuery(new Term("field", "empire")), Occur.MUST);

    assertEquals(expected.build(), parse("star|wars empire"));
    assertEquals(expected.build(), parse("star|wars + empire"));
    assertEquals(expected.build(), parse("star| + wars + ----empire |"));
  }

  public void testComplex01() throws Exception {
    BooleanQuery.Builder expected = new BooleanQuery.Builder();
    BooleanQuery.Builder inner = new BooleanQuery.Builder();
    inner.add(new TermQuery(new Term("field", "star")), Occur.MUST);
    inner.add(new TermQuery(new Term("field", "wars")), Occur.MUST);
    expected.add(inner.build(), Occur.SHOULD);
    expected.add(new TermQuery(new Term("field", "empire")), Occur.SHOULD);

    assertEquals(expected.build(), parse("star wars | empire"));
    assertEquals(expected.build(), parse("star + wars|empire"));
    assertEquals(expected.build(), parse("star + | wars | ----empire +"));
  }

  public void testComplex02() throws Exception {
    BooleanQuery.Builder expected = new BooleanQuery.Builder();
    BooleanQuery.Builder inner = new BooleanQuery.Builder();
    inner.add(new TermQuery(new Term("field", "star")), Occur.MUST);
    inner.add(new TermQuery(new Term("field", "wars")), Occur.MUST);
    expected.add(inner.build(), Occur.SHOULD);
    expected.add(new TermQuery(new Term("field", "empire")), Occur.SHOULD);
    expected.add(new TermQuery(new Term("field", "strikes")), Occur.SHOULD);

    assertEquals(expected.build(), parse("star wars | empire | strikes"));
    assertEquals(expected.build(), parse("star + wars|empire | strikes"));
    assertEquals(expected.build(), parse("star + | wars | ----empire | + --strikes \\"));
  }

  public void testComplex03() throws Exception {
    BooleanQuery.Builder expected = new BooleanQuery.Builder();
    BooleanQuery.Builder inner = new BooleanQuery.Builder();
    BooleanQuery.Builder inner2 = new BooleanQuery.Builder();
    inner2.add(new TermQuery(new Term("field", "star")), Occur.MUST);
    inner2.add(new TermQuery(new Term("field", "wars")), Occur.MUST);
    inner.add(inner2.build(), Occur.SHOULD);
    inner.add(new TermQuery(new Term("field", "empire")), Occur.SHOULD);
    inner.add(new TermQuery(new Term("field", "strikes")), Occur.SHOULD);
    expected.add(inner.build(), Occur.MUST);
    expected.add(new TermQuery(new Term("field", "back")), Occur.MUST);

    assertEquals(expected.build(), parse("star wars | empire | strikes back"));
    assertEquals(expected.build(), parse("star + wars|empire | strikes + back"));
    assertEquals(expected.build(), parse("star + | wars | ----empire | + --strikes + | --back \\"));
  }

  public void testComplex04() throws Exception {
    BooleanQuery.Builder expected = new BooleanQuery.Builder();
    BooleanQuery.Builder inner = new BooleanQuery.Builder();
    BooleanQuery.Builder inner2 = new BooleanQuery.Builder();
    inner.add(new TermQuery(new Term("field", "star")), Occur.MUST);
    inner.add(new TermQuery(new Term("field", "wars")), Occur.MUST);
    inner2.add(new TermQuery(new Term("field", "strikes")), Occur.MUST);
    inner2.add(new TermQuery(new Term("field", "back")), Occur.MUST);
    expected.add(inner.build(), Occur.SHOULD);
    expected.add(new TermQuery(new Term("field", "empire")), Occur.SHOULD);
    expected.add(inner2.build(), Occur.SHOULD);

    assertEquals(expected.build(), parse("(star wars) | empire | (strikes back)"));
    assertEquals(expected.build(), parse("(star + wars) |empire | (strikes + back)"));
    assertEquals(expected.build(), parse("(star + | wars |) | ----empire | + --(strikes + | --back) \\"));
  }

  public void testComplex05() throws Exception {
    BooleanQuery.Builder expected = new BooleanQuery.Builder();
    BooleanQuery.Builder inner1 = new BooleanQuery.Builder();
    BooleanQuery.Builder inner2 = new BooleanQuery.Builder();
    BooleanQuery.Builder inner3 = new BooleanQuery.Builder();
    BooleanQuery.Builder inner4 = new BooleanQuery.Builder();

    inner1.add(new TermQuery(new Term("field", "star")), Occur.MUST);
    inner1.add(new TermQuery(new Term("field", "wars")), Occur.MUST);

    inner2.add(new TermQuery(new Term("field", "empire")), Occur.SHOULD);

    inner3.add(new TermQuery(new Term("field", "strikes")), Occur.MUST);
    inner3.add(new TermQuery(new Term("field", "back")), Occur.MUST);

    inner4.add(new TermQuery(new Term("field", "jarjar")), Occur.MUST_NOT);
    inner4.add(new MatchAllDocsQuery(), Occur.SHOULD);

    inner3.add(inner4.build(), Occur.MUST);
    inner2.add(inner3.build(), Occur.SHOULD);
    
    expected.add(inner1.build(), Occur.SHOULD);
    expected.add(inner2.build(), Occur.SHOULD);
    
    assertEquals(expected.build(), parse("(star wars) | (empire | (strikes back -jarjar))"));
    assertEquals(expected.build(), parse("(star + wars) |(empire | (strikes + back -jarjar) () )"));
    assertEquals(expected.build(), parse("(star + | wars |) | --(--empire | + --(strikes + | --back + -jarjar) \"\" ) \""));
  }

  public void testComplex06() throws Exception {
    BooleanQuery.Builder expected = new BooleanQuery.Builder();
    BooleanQuery.Builder inner1 = new BooleanQuery.Builder();
    BooleanQuery.Builder inner2 = new BooleanQuery.Builder();
    BooleanQuery.Builder inner3 = new BooleanQuery.Builder();

    expected.add(new TermQuery(new Term("field", "star")), Occur.MUST);

    inner1.add(new TermQuery(new Term("field", "wars")), Occur.SHOULD);

    inner3.add(new TermQuery(new Term("field", "empire")), Occur.SHOULD);
    inner3.add(new TermQuery(new Term("field", "strikes")), Occur.SHOULD);
    inner2.add(inner3.build(), Occur.MUST);

    inner2.add(new TermQuery(new Term("field", "back")), Occur.MUST);
    inner2.add(new TermQuery(new Term("field", "jar+|jar")), Occur.MUST);
    inner1.add(inner2.build(), Occur.SHOULD);

    expected.add(inner1.build(), Occur.MUST);

    assertEquals(expected.build(), parse("star (wars | (empire | strikes back jar\\+\\|jar))"));
    assertEquals(expected.build(), parse("star + (wars |(empire | strikes + back jar\\+\\|jar) () )"));
    assertEquals(expected.build(), parse("star + (| wars | | --(--empire | + --strikes + | --back + jar\\+\\|jar) \"\" ) \""));
  }

  /** test a term with field weights */
  public void testWeightedTerm() throws Exception {
    Map<String,Float> weights = new LinkedHashMap<>();
    weights.put("field0", 5f);
    weights.put("field1", 10f);

    BooleanQuery.Builder expected = new BooleanQuery.Builder();
    Query field0 = new TermQuery(new Term("field0", "foo"));
    field0 = new BoostQuery(field0, 5f);
    expected.add(field0, Occur.SHOULD);
    Query field1 = new TermQuery(new Term("field1", "foo"));
    field1 = new BoostQuery(field1, 10f);
    expected.add(field1, Occur.SHOULD);

    Analyzer analyzer = new MockAnalyzer(random());
    SimpleQueryParser parser = new SimpleQueryParser(analyzer, weights);
    assertEquals(expected.build(), parser.parse("foo"));
  }

  /** test a more complex query with field weights */
  public void testWeightedOR() throws Exception {
    Map<String,Float> weights = new LinkedHashMap<>();
    weights.put("field0", 5f);
    weights.put("field1", 10f);

    BooleanQuery.Builder expected = new BooleanQuery.Builder();
    BooleanQuery.Builder foo = new BooleanQuery.Builder();
    Query field0 = new TermQuery(new Term("field0", "foo"));
    field0 = new BoostQuery(field0, 5f);
    foo.add(field0, Occur.SHOULD);
    Query field1 = new TermQuery(new Term("field1", "foo"));
    field1 = new BoostQuery(field1, 10f);
    foo.add(field1, Occur.SHOULD);
    expected.add(foo.build(), Occur.SHOULD);

    BooleanQuery.Builder bar = new BooleanQuery.Builder();
    field0 = new TermQuery(new Term("field0", "bar"));
    field0 = new BoostQuery(field0, 5f);
    bar.add(field0, Occur.SHOULD);
    field1 = new TermQuery(new Term("field1", "bar"));
    field1 = new BoostQuery(field1, 10f);
    bar.add(field1, Occur.SHOULD);
    expected.add(bar.build(), Occur.SHOULD);

    Analyzer analyzer = new MockAnalyzer(random());
    SimpleQueryParser parser = new SimpleQueryParser(analyzer, weights);
    assertEquals(expected.build(), parser.parse("foo|bar"));
  }

  /** helper to parse a query with keyword analyzer across "field" */
  private Query parseKeyword(String text, int flags) {
    Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.KEYWORD, false);
    SimpleQueryParser parser = new SimpleQueryParser(analyzer,
        Collections.singletonMap("field", 1f),
        flags);
    return parser.parse(text);
  }

  /** test the ability to enable/disable phrase operator */
  public void testDisablePhrase() {
    Query expected = new TermQuery(new Term("field", "\"test\""));
    assertEquals(expected, parseKeyword("\"test\"", ~PHRASE_OPERATOR));
  }

  /** test the ability to enable/disable prefix operator */
  public void testDisablePrefix() {
    Query expected = new TermQuery(new Term("field", "test*"));
    assertEquals(expected, parseKeyword("test*", ~PREFIX_OPERATOR));
  }

  /** test the ability to enable/disable AND operator */
  public void testDisableAND() {
    Query expected = new TermQuery(new Term("field", "foo+bar"));
    assertEquals(expected, parseKeyword("foo+bar", ~AND_OPERATOR));
    expected = new TermQuery(new Term("field", "+foo+bar"));
    assertEquals(expected, parseKeyword("+foo+bar", ~AND_OPERATOR));
  }

  /** test the ability to enable/disable OR operator */
  public void testDisableOR() {
    Query expected = new TermQuery(new Term("field", "foo|bar"));
    assertEquals(expected, parseKeyword("foo|bar", ~OR_OPERATOR));
    expected = new TermQuery(new Term("field", "|foo|bar"));
    assertEquals(expected, parseKeyword("|foo|bar", ~OR_OPERATOR));
  }

  /** test the ability to enable/disable NOT operator */
  public void testDisableNOT() {
    Query expected = new TermQuery(new Term("field", "-foo"));
    assertEquals(expected, parseKeyword("-foo", ~NOT_OPERATOR));
  }

  /** test the ability to enable/disable precedence operators */
  public void testDisablePrecedence() {
    Query expected = new TermQuery(new Term("field", "(foo)"));
    assertEquals(expected, parseKeyword("(foo)", ~PRECEDENCE_OPERATORS));
    expected = new TermQuery(new Term("field", ")foo("));
    assertEquals(expected, parseKeyword(")foo(", ~PRECEDENCE_OPERATORS));
  }

  /** test the ability to enable/disable escape operators */
  public void testDisableEscape() {
    Query expected = new TermQuery(new Term("field", "foo\\bar"));
    assertEquals(expected, parseKeyword("foo\\bar", ~ESCAPE_OPERATOR));
    assertEquals(expected, parseKeyword("(foo\\bar)", ~ESCAPE_OPERATOR));
    assertEquals(expected, parseKeyword("\"foo\\bar\"", ~ESCAPE_OPERATOR));
  }

  public void testDisableWhitespace() {
    Query expected = new TermQuery(new Term("field", "foo foo"));
    assertEquals(expected, parseKeyword("foo foo", ~WHITESPACE_OPERATOR));
    expected = new TermQuery(new Term("field", " foo foo\n "));
    assertEquals(expected, parseKeyword(" foo foo\n ", ~WHITESPACE_OPERATOR));
    expected = new TermQuery(new Term("field", "\t\tfoo foo foo"));
    assertEquals(expected, parseKeyword("\t\tfoo foo foo", ~WHITESPACE_OPERATOR));
  }

  public void testDisableFuzziness() {
    Query expected = new TermQuery(new Term("field", "foo~1"));
    assertEquals(expected, parseKeyword("foo~1", ~FUZZY_OPERATOR));
  }

  public void testDisableSlop() {
    PhraseQuery expectedPhrase = new PhraseQuery("field", "foo", "bar");

    BooleanQuery.Builder expected = new BooleanQuery.Builder();
    expected.add(expectedPhrase, Occur.MUST);
    expected.add(new TermQuery(new Term("field", "~2")), Occur.MUST);
    assertEquals(expected.build(), parse("\"foo bar\"~2", ~NEAR_OPERATOR));
  }

  // we aren't supposed to barf on any input...
  public void testRandomQueries() throws Exception {
    for (int i = 0; i < 1000; i++) {
      String query = TestUtil.randomUnicodeString(random());
      parse(query); // no exception
      parseKeyword(query, TestUtil.nextInt(random(), 0, 1024)); // no exception
    }
  }

  public void testRandomQueries2() throws Exception {
    char chars[] = new char[] { 'a', '1', '|', '&', ' ', '(', ')', '"', '-', '~'};
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 1000; i++) {
      sb.setLength(0);
      int queryLength = random().nextInt(20);
      for (int j = 0; j < queryLength; j++) {
        sb.append(chars[random().nextInt(chars.length)]);
      }
      parse(sb.toString()); // no exception
      parseKeyword(sb.toString(), TestUtil.nextInt(random(), 0, 1024)); // no exception
    }
  }

  public void testStarBecomesMatchAll() throws Exception {
    Query q = parse("*");
    assertEquals(q, new MatchAllDocsQuery());
    q = parse(" *   ");
    assertEquals(q, new MatchAllDocsQuery());
  }
}
