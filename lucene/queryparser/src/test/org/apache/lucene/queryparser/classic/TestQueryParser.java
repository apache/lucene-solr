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
package org.apache.lucene.queryparser.classic;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockBytesAnalyzer;
import org.apache.lucene.analysis.MockLowerCaseFilter;
import org.apache.lucene.analysis.MockSynonymAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.document.DateTools.Resolution;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser.Operator;
import org.apache.lucene.queryparser.flexible.standard.CommonQueryParserConfiguration;
import org.apache.lucene.queryparser.util.QueryParserTestBase;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;

/**
 * Tests QueryParser.
 */
public class TestQueryParser extends QueryParserTestBase {

  protected boolean splitOnWhitespace = QueryParser.DEFAULT_SPLIT_ON_WHITESPACE;
  private static final String FIELD = "field";

  public static class QPTestParser extends QueryParser {
    public QPTestParser(String f, Analyzer a) {
      super(f, a);
    }
    
    @Override
    protected Query getFuzzyQuery(String field, String termStr,
        float minSimilarity) throws ParseException {
      throw new ParseException("Fuzzy queries not allowed");
    }
    
    @Override
    protected Query getWildcardQuery(String field, String termStr)
        throws ParseException {
      throw new ParseException("Wildcard queries not allowed");
    }
  }
  
  public QueryParser getParser(Analyzer a) throws Exception {
    if (a == null) a = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true);
    QueryParser qp = new QueryParser(getDefaultField(), a);
    qp.setDefaultOperator(QueryParserBase.OR_OPERATOR);
    qp.setSplitOnWhitespace(splitOnWhitespace);
    return qp;
  }
  
  @Override
  public CommonQueryParserConfiguration getParserConfig(Analyzer a)
      throws Exception {
    return getParser(a);
  }
  
  @Override
  public Query getQuery(String query, CommonQueryParserConfiguration cqpC)
      throws Exception {
    assert cqpC != null : "Parameter must not be null";
    assert (cqpC instanceof QueryParser) : "Parameter must be instance of QueryParser";
    QueryParser qp = (QueryParser) cqpC;
    return qp.parse(query);
  }
  
  @Override
  public Query getQuery(String query, Analyzer a) throws Exception {
    return getParser(a).parse(query);
  }
  
  @Override
  public boolean isQueryParserException(Exception exception) {
    return exception instanceof ParseException;
  }
  
  @Override
  public void setDefaultOperatorOR(CommonQueryParserConfiguration cqpC) {
    assert (cqpC instanceof QueryParser);
    QueryParser qp = (QueryParser) cqpC;
    qp.setDefaultOperator(Operator.OR);
  }
  
  @Override
  public void setDefaultOperatorAND(CommonQueryParserConfiguration cqpC) {
    assert (cqpC instanceof QueryParser);
    QueryParser qp = (QueryParser) cqpC;
    qp.setDefaultOperator(Operator.AND);
  }
  
  @Override
  public void setAutoGeneratePhraseQueries(CommonQueryParserConfiguration cqpC,
      boolean value) {
    assert (cqpC instanceof QueryParser);
    QueryParser qp = (QueryParser) cqpC;
    qp.setAutoGeneratePhraseQueries(value);
  }
  
  @Override
  public void setDateResolution(CommonQueryParserConfiguration cqpC,
      CharSequence field, Resolution value) {
    assert (cqpC instanceof QueryParser);
    QueryParser qp = (QueryParser) cqpC;
    qp.setDateResolution(field.toString(), value);
  }
  
  @Override
  public void testDefaultOperator() throws Exception {
    QueryParser qp = getParser(new MockAnalyzer(random()));
    // make sure OR is the default:
    assertEquals(QueryParserBase.OR_OPERATOR, qp.getDefaultOperator());
    setDefaultOperatorAND(qp);
    assertEquals(QueryParserBase.AND_OPERATOR, qp.getDefaultOperator());
    setDefaultOperatorOR(qp);
    assertEquals(QueryParserBase.OR_OPERATOR, qp.getDefaultOperator());
  }
  
  // LUCENE-2002: when we run javacc to regen QueryParser,
  // we also run a replaceregexp step to fix 2 of the public
  // ctors (change them to protected):
  //
  // protected QueryParser(CharStream stream)
  //
  // protected QueryParser(QueryParserTokenManager tm)
  //
  // This test is here as a safety, in case that ant step
  // doesn't work for some reason.
  @SuppressWarnings("rawtype")
  public void testProtectedCtors() throws Exception {
    expectThrows(NoSuchMethodException.class, () -> QueryParser.class.getConstructor(CharStream.class));
    expectThrows(NoSuchMethodException.class, () -> QueryParser.class.getConstructor(QueryParserTokenManager.class));
  }
  
  public void testFuzzySlopeExtendability() throws ParseException {
    QueryParser qp = new QueryParser("a",  new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false)) {

      @Override
      Query handleBareFuzzy(String qfield, Token fuzzySlop, String termImage)
          throws ParseException {
        
        if(fuzzySlop.image.endsWith("€")) {
          float fms = fuzzyMinSim;
          try {
            fms = Float.parseFloat(fuzzySlop.image.substring(1, fuzzySlop.image.length()-1));
          } catch (Exception ignored) { }
          float value = Float.parseFloat(termImage);
          return getRangeQuery(qfield, Float.toString(value-fms/2.f), Float.toString(value+fms/2.f), true, true);
        }
        return super.handleBareFuzzy(qfield, fuzzySlop, termImage);
      }
      
    };
    assertEquals(qp.parse("a:[11.95 TO 12.95]"), qp.parse("12.45~1€"));
  }
  
  @Override
  public void testStarParsing() throws Exception {
    final int[] type = new int[1];
    QueryParser qp = new QueryParser(FIELD,
        new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false)) {
      @Override
      protected Query getWildcardQuery(String field, String termStr) {
        // override error checking of superclass
        type[0] = 1;
        return new TermQuery(new Term(field, termStr));
      }
      
      @Override
      protected Query getPrefixQuery(String field, String termStr) {
        // override error checking of superclass
        type[0] = 2;
        return new TermQuery(new Term(field, termStr));
      }
      
      @Override
      protected Query getFieldQuery(String field, String queryText,
          boolean quoted) throws ParseException {
        type[0] = 3;
        return super.getFieldQuery(field, queryText, quoted);
      }
    };
    
    TermQuery tq;
    
    tq = (TermQuery) qp.parse("foo:zoo*");
    assertEquals("zoo", tq.getTerm().text());
    assertEquals(2, type[0]);

    BoostQuery bq = (BoostQuery) qp.parse("foo:zoo*^2");
    tq = (TermQuery) bq.getQuery();
    assertEquals("zoo", tq.getTerm().text());
    assertEquals(2, type[0]);
    assertEquals(bq.getBoost(), 2, 0);
    
    tq = (TermQuery) qp.parse("foo:*");
    assertEquals("*", tq.getTerm().text());
    assertEquals(1, type[0]); // could be a valid prefix query in the future too
    
    bq = (BoostQuery) qp.parse("foo:*^2");
    tq = (TermQuery) bq.getQuery();
    assertEquals("*", tq.getTerm().text());
    assertEquals(1, type[0]);
    assertEquals(bq.getBoost(), 2, 0);
    
    tq = (TermQuery) qp.parse("*:foo");
    assertEquals("*", tq.getTerm().field());
    assertEquals("foo", tq.getTerm().text());
    assertEquals(3, type[0]);
    
    tq = (TermQuery) qp.parse("*:*");
    assertEquals("*", tq.getTerm().field());
    assertEquals("*", tq.getTerm().text());
    assertEquals(1, type[0]); // could be handled as a prefix query in the
                              // future
    
    tq = (TermQuery) qp.parse("(*:*)");
    assertEquals("*", tq.getTerm().field());
    assertEquals("*", tq.getTerm().text());
    assertEquals(1, type[0]);
    
  }
  
  // Wildcard queries should not be allowed
  public void testCustomQueryParserWildcard() {
    expectThrows(ParseException.class, () -> {
      new QPTestParser("contents", new MockAnalyzer(random(),
          MockTokenizer.WHITESPACE, false)).parse("a?t");
    });
  }
  
  // Fuzzy queries should not be allowed
  public void testCustomQueryParserFuzzy() throws Exception {
    expectThrows(ParseException.class, () -> {
      new QPTestParser("contents", new MockAnalyzer(random(),
          MockTokenizer.WHITESPACE, false)).parse("xunit~");
    });
  }
  
  /** query parser that doesn't expand synonyms when users use double quotes */
  private class SmartQueryParser extends QueryParser {
    Analyzer morePrecise = new Analyzer2();
    
    public SmartQueryParser() {
      super(FIELD, new Analyzer1());
    }
    
    @Override
    protected Query getFieldQuery(String field, String queryText, boolean quoted)
        throws ParseException {
      if (quoted) return newFieldQuery(morePrecise, field, queryText, quoted);
      else return super.getFieldQuery(field, queryText, quoted);
    }
  }
  
  @Override
  public void testNewFieldQuery() throws Exception {
    /** ordinary behavior, synonyms form uncoordinated boolean query */
    QueryParser dumb = new QueryParser(FIELD,
        new Analyzer1());
    Query expanded = new SynonymQuery.Builder(FIELD)
        .addTerm(new Term(FIELD, "dogs"))
        .addTerm(new Term(FIELD, "dog"))
        .build();
    assertEquals(expanded, dumb.parse("\"dogs\""));
    /** even with the phrase operator the behavior is the same */
    assertEquals(expanded, dumb.parse("dogs"));
    
    /**
     * custom behavior, the synonyms are expanded, unless you use quote operator
     */
    QueryParser smart = new SmartQueryParser();
    assertEquals(expanded, smart.parse("dogs"));
    
    Query unexpanded = new TermQuery(new Term(FIELD, "dogs"));
    assertEquals(unexpanded, smart.parse("\"dogs\""));
  }

  /** simple synonyms test */
  public void testSynonyms() throws Exception {
    Query expected = new SynonymQuery.Builder(FIELD)
        .addTerm(new Term(FIELD, "dogs"))
        .addTerm(new Term(FIELD, "dog"))
        .build();
    QueryParser qp = new QueryParser(FIELD, new MockSynonymAnalyzer());
    assertEquals(expected, qp.parse("dogs"));
    assertEquals(expected, qp.parse("\"dogs\""));
    qp.setDefaultOperator(Operator.AND);
    assertEquals(expected, qp.parse("dogs"));
    assertEquals(expected, qp.parse("\"dogs\""));
    expected = new BoostQuery(expected, 2f);
    assertEquals(expected, qp.parse("dogs^2"));
    assertEquals(expected, qp.parse("\"dogs\"^2"));
  }
  
  /** forms multiphrase query */
  public void testSynonymsPhrase() throws Exception {
    MultiPhraseQuery.Builder expectedQBuilder = new MultiPhraseQuery.Builder();
    expectedQBuilder.add(new Term(FIELD, "old"));
    expectedQBuilder.add(new Term[] { new Term(FIELD, "dogs"), new Term(FIELD, "dog") });
    QueryParser qp = new QueryParser(FIELD, new MockSynonymAnalyzer());
    assertEquals(expectedQBuilder.build(), qp.parse("\"old dogs\""));
    qp.setDefaultOperator(Operator.AND);
    assertEquals(expectedQBuilder.build(), qp.parse("\"old dogs\""));
    BoostQuery expected = new BoostQuery(expectedQBuilder.build(), 2f);
    assertEquals(expected, qp.parse("\"old dogs\"^2"));
    expectedQBuilder.setSlop(3);
    expected = new BoostQuery(expectedQBuilder.build(), 2f);
    assertEquals(expected, qp.parse("\"old dogs\"~3^2"));
  }
  
  /**
   * adds synonym of "國" for "国".
   */
  protected static class MockCJKSynonymFilter extends TokenFilter {
    CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    PositionIncrementAttribute posIncAtt = addAttribute(PositionIncrementAttribute.class);
    boolean addSynonym = false;
    
    public MockCJKSynonymFilter(TokenStream input) {
      super(input);
    }

    @Override
    public final boolean incrementToken() throws IOException {
      if (addSynonym) { // inject our synonym
        clearAttributes();
        termAtt.setEmpty().append("國");
        posIncAtt.setPositionIncrement(0);
        addSynonym = false;
        return true;
      }
      
      if (input.incrementToken()) {
        addSynonym = termAtt.toString().equals("国");
        return true;
      } else {
        return false;
      }
    } 
  }
  
  static class MockCJKSynonymAnalyzer extends Analyzer {
    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
      Tokenizer tokenizer = new SimpleCJKTokenizer();
      return new TokenStreamComponents(tokenizer, new MockCJKSynonymFilter(tokenizer));
    }
  }
  
  /** simple CJK synonym test */
  public void testCJKSynonym() throws Exception {
    Query expected = new SynonymQuery.Builder(FIELD)
        .addTerm(new Term(FIELD, "国"))
        .addTerm(new Term(FIELD, "國"))
        .build();
    QueryParser qp = new QueryParser(FIELD, new MockCJKSynonymAnalyzer());
    assertEquals(expected, qp.parse("国"));
    qp.setDefaultOperator(Operator.AND);
    assertEquals(expected, qp.parse("国"));
    expected = new BoostQuery(expected, 2f);
    assertEquals(expected, qp.parse("国^2"));
  }
  
  /** synonyms with default OR operator */
  public void testCJKSynonymsOR() throws Exception {
    BooleanQuery.Builder expectedB = new BooleanQuery.Builder();
    expectedB.add(new TermQuery(new Term(FIELD, "中")), BooleanClause.Occur.SHOULD);
    Query inner = new SynonymQuery.Builder(FIELD)
        .addTerm(new Term(FIELD, "国"))
        .addTerm(new Term(FIELD, "國"))
        .build();
    expectedB.add(inner, BooleanClause.Occur.SHOULD);
    Query expected = expectedB.build();
    QueryParser qp = new QueryParser(FIELD, new MockCJKSynonymAnalyzer());
    assertEquals(expected, qp.parse("中国"));
    expected = new BoostQuery(expected, 2f);
    assertEquals(expected, qp.parse("中国^2"));
  }
  
  /** more complex synonyms with default OR operator */
  public void testCJKSynonymsOR2() throws Exception {
    BooleanQuery.Builder expectedB = new BooleanQuery.Builder();
    expectedB.add(new TermQuery(new Term(FIELD, "中")), BooleanClause.Occur.SHOULD);
    SynonymQuery inner = new SynonymQuery.Builder(FIELD)
      .addTerm(new Term(FIELD, "国"))
      .addTerm(new Term(FIELD, "國"))
        .build();
    expectedB.add(inner, BooleanClause.Occur.SHOULD);
    SynonymQuery inner2 = new SynonymQuery.Builder(FIELD)
        .addTerm(new Term(FIELD, "国"))
        .addTerm(new Term(FIELD, "國"))
        .build();
    expectedB.add(inner2, BooleanClause.Occur.SHOULD);
    Query expected = expectedB.build();
    QueryParser qp = new QueryParser(FIELD, new MockCJKSynonymAnalyzer());
    assertEquals(expected, qp.parse("中国国"));
    expected = new BoostQuery(expected, 2f);
    assertEquals(expected, qp.parse("中国国^2"));
  }
  
  /** synonyms with default AND operator */
  public void testCJKSynonymsAND() throws Exception {
    BooleanQuery.Builder expectedB = new BooleanQuery.Builder();
    expectedB.add(new TermQuery(new Term(FIELD, "中")), BooleanClause.Occur.MUST);
    Query inner = new SynonymQuery.Builder(FIELD)
        .addTerm(new Term(FIELD, "国"))
        .addTerm(new Term(FIELD, "國"))
        .build();
    expectedB.add(inner, BooleanClause.Occur.MUST);
    Query expected = expectedB.build();
    QueryParser qp = new QueryParser(FIELD, new MockCJKSynonymAnalyzer());
    qp.setDefaultOperator(Operator.AND);
    assertEquals(expected, qp.parse("中国"));
    expected = new BoostQuery(expected, 2f);
    assertEquals(expected, qp.parse("中国^2"));
  }
  
  /** more complex synonyms with default AND operator */
  public void testCJKSynonymsAND2() throws Exception {
    BooleanQuery.Builder expectedB = new BooleanQuery.Builder();
    expectedB.add(new TermQuery(new Term(FIELD, "中")), BooleanClause.Occur.MUST);
    Query inner = new SynonymQuery.Builder(FIELD)
        .addTerm(new Term(FIELD, "国"))
        .addTerm(new Term(FIELD, "國"))
        .build();
    expectedB.add(inner, BooleanClause.Occur.MUST);
    Query inner2 = new SynonymQuery.Builder(FIELD)
        .addTerm(new Term(FIELD, "国"))
        .addTerm(new Term(FIELD, "國"))
        .build();
    expectedB.add(inner2, BooleanClause.Occur.MUST);
    Query expected = expectedB.build();
    QueryParser qp = new QueryParser(FIELD, new MockCJKSynonymAnalyzer());
    qp.setDefaultOperator(Operator.AND);
    assertEquals(expected, qp.parse("中国国"));
    expected = new BoostQuery(expected, 2f);
    assertEquals(expected, qp.parse("中国国^2"));
  }
  
  /** forms multiphrase query */
  public void testCJKSynonymsPhrase() throws Exception {
    MultiPhraseQuery.Builder expectedQBuilder = new MultiPhraseQuery.Builder();
    expectedQBuilder.add(new Term(FIELD, "中"));
    expectedQBuilder.add(new Term[] { new Term(FIELD, "国"), new Term(FIELD, "國")});
    QueryParser qp = new QueryParser(FIELD, new MockCJKSynonymAnalyzer());
    qp.setDefaultOperator(Operator.AND);
    assertEquals(expectedQBuilder.build(), qp.parse("\"中国\""));
    Query expected = new BoostQuery(expectedQBuilder.build(), 2f);
    assertEquals(expected, qp.parse("\"中国\"^2"));
    expectedQBuilder.setSlop(3);
    expected = new BoostQuery(expectedQBuilder.build(), 2f);
    assertEquals(expected, qp.parse("\"中国\"~3^2"));
  }

  /** LUCENE-6677: make sure wildcard query respects maxDeterminizedStates. */
  public void testWildcardMaxDeterminizedStates() throws Exception {
    QueryParser qp = new QueryParser(FIELD, new MockAnalyzer(random()));
    qp.setMaxDeterminizedStates(10);
    expectThrows(TooComplexToDeterminizeException.class, () -> {
      qp.parse("a*aaaaaaa");
    });
  }

  // TODO: Remove this specialization once the flexible standard parser gets multi-word synonym support
  @Override
  public void testQPA() throws Exception {
    boolean oldSplitOnWhitespace = splitOnWhitespace;
    splitOnWhitespace = false;

    assertQueryEquals("term phrase term", qpAnalyzer, "term phrase1 phrase2 term");

    CommonQueryParserConfiguration cqpc = getParserConfig(qpAnalyzer);
    setDefaultOperatorAND(cqpc);
    assertQueryEquals(cqpc, "field", "term phrase term", "+term +phrase1 +phrase2 +term");

    splitOnWhitespace = oldSplitOnWhitespace;
  }

  // TODO: Move to QueryParserTestBase once standard flexible parser gets this capability
  public void testMultiWordSynonyms() throws Exception {
    QueryParser dumb = new QueryParser("field", new Analyzer1());
    dumb.setSplitOnWhitespace(false);

    TermQuery guinea = new TermQuery(new Term("field", "guinea"));
    TermQuery pig = new TermQuery(new Term("field", "pig"));
    TermQuery cavy = new TermQuery(new Term("field", "cavy"));

    // A multi-word synonym source will form a graph query for synonyms that formed the graph token stream
    BooleanQuery.Builder synonym = new BooleanQuery.Builder();
    synonym.add(guinea, BooleanClause.Occur.MUST);
    synonym.add(pig, BooleanClause.Occur.MUST);
    BooleanQuery guineaPig = synonym.build();

    PhraseQuery phraseGuineaPig = new PhraseQuery.Builder()
        .add(new Term("field", "guinea"))
        .add(new Term("field", "pig"))
        .build();

    BooleanQuery graphQuery = new BooleanQuery.Builder()
        .add(new BooleanQuery.Builder()
            .add(guineaPig, BooleanClause.Occur.SHOULD)
            .add(cavy, BooleanClause.Occur.SHOULD)
            .build(), BooleanClause.Occur.SHOULD)
        .build();
    assertEquals(graphQuery, dumb.parse("guinea pig"));

    // With the phrase operator, a multi-word synonym source will form span near queries.
    SpanNearQuery spanGuineaPig = SpanNearQuery.newOrderedNearQuery("field")
        .addClause(new SpanTermQuery(new Term("field", "guinea")))
        .addClause(new SpanTermQuery(new Term("field", "pig")))
        .setSlop(0)
        .build();
    SpanTermQuery spanCavy = new SpanTermQuery(new Term("field", "cavy"));
    SpanOrQuery spanPhrase = new SpanOrQuery(new SpanQuery[]{spanGuineaPig, spanCavy});
    assertEquals(spanPhrase, dumb.parse("\"guinea pig\""));

    // custom behavior, the synonyms are expanded, unless you use quote operator
    QueryParser smart = new SmartQueryParser();
    smart.setSplitOnWhitespace(false);
    graphQuery = new BooleanQuery.Builder()
        .add(new BooleanQuery.Builder()
            .add(guineaPig, BooleanClause.Occur.SHOULD)
            .add(cavy, BooleanClause.Occur.SHOULD)
            .build(), BooleanClause.Occur.SHOULD)
        .build();
    assertEquals(graphQuery, smart.parse("guinea pig"));
    assertEquals(phraseGuineaPig, smart.parse("\"guinea pig\""));

    // with the AND operator
    dumb.setDefaultOperator(Operator.AND);
    BooleanQuery graphAndQuery = new BooleanQuery.Builder()
        .add(new BooleanQuery.Builder()
            .add(guineaPig, BooleanClause.Occur.SHOULD)
            .add(cavy, BooleanClause.Occur.SHOULD)
            .build(), BooleanClause.Occur.MUST)
        .build();
    assertEquals(graphAndQuery, dumb.parse("guinea pig"));

    graphAndQuery = new BooleanQuery.Builder()
        .add(new BooleanQuery.Builder()
            .add(guineaPig, BooleanClause.Occur.SHOULD)
            .add(cavy, BooleanClause.Occur.SHOULD)
            .build(), BooleanClause.Occur.MUST)
        .add(cavy, BooleanClause.Occur.MUST)
        .build();
    assertEquals(graphAndQuery, dumb.parse("guinea pig cavy"));
  }

  public void testEnableGraphQueries() throws Exception {
    QueryParser dumb = new QueryParser("field", new Analyzer1());
    dumb.setSplitOnWhitespace(false);
    dumb.setEnableGraphQueries(false);
    
    TermQuery pig = new TermQuery(new Term("field", "pig"));

    // A multi-word synonym source will just form a boolean query when graph queries are disabled:
    Query inner = new SynonymQuery.Builder("field")
        .addTerm(new Term("field", "cavy"))
        .addTerm(new Term("field", "guinea"))
        .build();
    BooleanQuery.Builder b = new BooleanQuery.Builder();
    b.add(inner, BooleanClause.Occur.SHOULD);
    b.add(pig, BooleanClause.Occur.SHOULD);
    BooleanQuery query = b.build();
    assertEquals(query, dumb.parse("guinea pig"));
  }

  // TODO: Move to QueryParserTestBase once standard flexible parser gets this capability
  public void testOperatorsAndMultiWordSynonyms() throws Exception {
    Analyzer a = new MockSynonymAnalyzer();

    boolean oldSplitOnWhitespace = splitOnWhitespace;
    splitOnWhitespace = false;

    // Operators should interrupt multiword analysis of adjacent words if they associate
    assertQueryEquals("+guinea pig", a, "+guinea pig");
    assertQueryEquals("-guinea pig", a, "-guinea pig");
    assertQueryEquals("!guinea pig", a, "-guinea pig");
    assertQueryEquals("guinea* pig", a, "guinea* pig");
    assertQueryEquals("guinea? pig", a, "guinea? pig");
    assertQueryEquals("guinea~2 pig", a, "guinea~2 pig");
    assertQueryEquals("guinea^2 pig", a, "(guinea)^2.0 pig");

    assertQueryEquals("guinea +pig", a, "guinea +pig");
    assertQueryEquals("guinea -pig", a, "guinea -pig");
    assertQueryEquals("guinea !pig", a, "guinea -pig");
    assertQueryEquals("guinea pig*", a, "guinea pig*");
    assertQueryEquals("guinea pig?", a, "guinea pig?");
    assertQueryEquals("guinea pig~2", a, "guinea pig~2");
    assertQueryEquals("guinea pig^2", a, "guinea (pig)^2.0");

    assertQueryEquals("field:guinea pig", a, "guinea pig");
    assertQueryEquals("guinea field:pig", a, "guinea pig");

    assertQueryEquals("NOT guinea pig", a, "-guinea pig");
    assertQueryEquals("guinea NOT pig", a, "guinea -pig");

    assertQueryEquals("guinea pig AND dogs", a, "guinea +pig +Synonym(dog dogs)");
    assertQueryEquals("dogs AND guinea pig", a, "+Synonym(dog dogs) +guinea pig");
    assertQueryEquals("guinea pig && dogs", a, "guinea +pig +Synonym(dog dogs)");
    assertQueryEquals("dogs && guinea pig", a, "+Synonym(dog dogs) +guinea pig");

    assertQueryEquals("guinea pig OR dogs", a, "guinea pig Synonym(dog dogs)");
    assertQueryEquals("dogs OR guinea pig", a, "Synonym(dog dogs) guinea pig");
    assertQueryEquals("guinea pig || dogs", a, "guinea pig Synonym(dog dogs)");
    assertQueryEquals("dogs || guinea pig", a, "Synonym(dog dogs) guinea pig");

    assertQueryEquals("\"guinea\" pig", a, "guinea pig");
    assertQueryEquals("guinea \"pig\"", a, "guinea pig");

    assertQueryEquals("(guinea) pig", a, "guinea pig");
    assertQueryEquals("guinea (pig)", a, "guinea pig");

    assertQueryEquals("/guinea/ pig", a, "/guinea/ pig");
    assertQueryEquals("guinea /pig/", a, "guinea /pig/");

    // Operators should not interrupt multiword analysis if not don't associate
    assertQueryEquals("(guinea pig)", a, "((+guinea +pig) cavy)");
    assertQueryEquals("+(guinea pig)", a, "+(((+guinea +pig) cavy))");
    assertQueryEquals("-(guinea pig)", a, "-(((+guinea +pig) cavy))");
    assertQueryEquals("!(guinea pig)", a, "-(((+guinea +pig) cavy))");
    assertQueryEquals("NOT (guinea pig)", a, "-(((+guinea +pig) cavy))");
    assertQueryEquals("(guinea pig)^2", a, "(((+guinea +pig) cavy))^2.0");

    assertQueryEquals("field:(guinea pig)", a, "((+guinea +pig) cavy)");

    assertQueryEquals("+small guinea pig", a, "+small ((+guinea +pig) cavy)");
    assertQueryEquals("-small guinea pig", a, "-small ((+guinea +pig) cavy)");
    assertQueryEquals("!small guinea pig", a, "-small ((+guinea +pig) cavy)");
    assertQueryEquals("NOT small guinea pig", a, "-small ((+guinea +pig) cavy)");
    assertQueryEquals("small* guinea pig", a, "small* ((+guinea +pig) cavy)");
    assertQueryEquals("small? guinea pig", a, "small? ((+guinea +pig) cavy)");
    assertQueryEquals("\"small\" guinea pig", a, "small ((+guinea +pig) cavy)");

    assertQueryEquals("guinea pig +running", a, "((+guinea +pig) cavy) +running");
    assertQueryEquals("guinea pig -running", a, "((+guinea +pig) cavy) -running");
    assertQueryEquals("guinea pig !running", a, "((+guinea +pig) cavy) -running");
    assertQueryEquals("guinea pig NOT running", a, "((+guinea +pig) cavy) -running");
    assertQueryEquals("guinea pig running*", a, "((+guinea +pig) cavy) running*");
    assertQueryEquals("guinea pig running?", a, "((+guinea +pig) cavy) running?");
    assertQueryEquals("guinea pig \"running\"", a, "((+guinea +pig) cavy) running");

    assertQueryEquals("\"guinea pig\"~2", a, "spanOr([spanNear([guinea, pig], 0, true), cavy])");

    assertQueryEquals("field:\"guinea pig\"", a, "spanOr([spanNear([guinea, pig], 0, true), cavy])");

    splitOnWhitespace = oldSplitOnWhitespace;
  }

  public void testOperatorsAndMultiWordSynonymsSplitOnWhitespace() throws Exception {
    Analyzer a = new MockSynonymAnalyzer();

    boolean oldSplitOnWhitespace = splitOnWhitespace;
    splitOnWhitespace = true;

    assertQueryEquals("+guinea pig", a, "+guinea pig");
    assertQueryEquals("-guinea pig", a, "-guinea pig");
    assertQueryEquals("!guinea pig", a, "-guinea pig");
    assertQueryEquals("guinea* pig", a, "guinea* pig");
    assertQueryEquals("guinea? pig", a, "guinea? pig");
    assertQueryEquals("guinea~2 pig", a, "guinea~2 pig");
    assertQueryEquals("guinea^2 pig", a, "(guinea)^2.0 pig");

    assertQueryEquals("guinea +pig", a, "guinea +pig");
    assertQueryEquals("guinea -pig", a, "guinea -pig");
    assertQueryEquals("guinea !pig", a, "guinea -pig");
    assertQueryEquals("guinea pig*", a, "guinea pig*");
    assertQueryEquals("guinea pig?", a, "guinea pig?");
    assertQueryEquals("guinea pig~2", a, "guinea pig~2");
    assertQueryEquals("guinea pig^2", a, "guinea (pig)^2.0");

    assertQueryEquals("field:guinea pig", a, "guinea pig");
    assertQueryEquals("guinea field:pig", a, "guinea pig");

    assertQueryEquals("NOT guinea pig", a, "-guinea pig");
    assertQueryEquals("guinea NOT pig", a, "guinea -pig");

    assertQueryEquals("guinea pig AND dogs", a, "guinea +pig +Synonym(dog dogs)");
    assertQueryEquals("dogs AND guinea pig", a, "+Synonym(dog dogs) +guinea pig");
    assertQueryEquals("guinea pig && dogs", a, "guinea +pig +Synonym(dog dogs)");
    assertQueryEquals("dogs && guinea pig", a, "+Synonym(dog dogs) +guinea pig");

    assertQueryEquals("guinea pig OR dogs", a, "guinea pig Synonym(dog dogs)");
    assertQueryEquals("dogs OR guinea pig", a, "Synonym(dog dogs) guinea pig");
    assertQueryEquals("guinea pig || dogs", a, "guinea pig Synonym(dog dogs)");
    assertQueryEquals("dogs || guinea pig", a, "Synonym(dog dogs) guinea pig");

    assertQueryEquals("\"guinea\" pig", a, "guinea pig");
    assertQueryEquals("guinea \"pig\"", a, "guinea pig");

    assertQueryEquals("(guinea) pig", a, "guinea pig");
    assertQueryEquals("guinea (pig)", a, "guinea pig");

    assertQueryEquals("/guinea/ pig", a, "/guinea/ pig");
    assertQueryEquals("guinea /pig/", a, "guinea /pig/");

    assertQueryEquals("(guinea pig)", a, "guinea pig");
    assertQueryEquals("+(guinea pig)", a, "+(guinea pig)");
    assertQueryEquals("-(guinea pig)", a, "-(guinea pig)");
    assertQueryEquals("!(guinea pig)", a, "-(guinea pig)");
    assertQueryEquals("NOT (guinea pig)", a, "-(guinea pig)");
    assertQueryEquals("(guinea pig)^2", a, "(guinea pig)^2.0");

    assertQueryEquals("field:(guinea pig)", a, "guinea pig");

    assertQueryEquals("+small guinea pig", a, "+small guinea pig");
    assertQueryEquals("-small guinea pig", a, "-small guinea pig");
    assertQueryEquals("!small guinea pig", a, "-small guinea pig");
    assertQueryEquals("NOT small guinea pig", a, "-small guinea pig");
    assertQueryEquals("small* guinea pig", a, "small* guinea pig");
    assertQueryEquals("small? guinea pig", a, "small? guinea pig");
    assertQueryEquals("\"small\" guinea pig", a, "small guinea pig");

    assertQueryEquals("guinea pig +running", a, "guinea pig +running");
    assertQueryEquals("guinea pig -running", a, "guinea pig -running");
    assertQueryEquals("guinea pig !running", a, "guinea pig -running");
    assertQueryEquals("guinea pig NOT running", a, "guinea pig -running");
    assertQueryEquals("guinea pig running*", a, "guinea pig running*");
    assertQueryEquals("guinea pig running?", a, "guinea pig running?");
    assertQueryEquals("guinea pig \"running\"", a, "guinea pig running");

    assertQueryEquals("\"guinea pig\"~2", a, "spanOr([spanNear([guinea, pig], 0, true), cavy])");

    assertQueryEquals("field:\"guinea pig\"", a, "spanOr([spanNear([guinea, pig], 0, true), cavy])");

    splitOnWhitespace = oldSplitOnWhitespace;
  }

  public void testDefaultSplitOnWhitespace() throws Exception {
    QueryParser parser = new QueryParser("field", new Analyzer1());

    assertFalse(parser.getSplitOnWhitespace()); // default is false

    // A multi-word synonym source will form a synonym query for the same-starting-position tokens
    TermQuery guinea = new TermQuery(new Term("field", "guinea"));
    TermQuery pig = new TermQuery(new Term("field", "pig"));
    TermQuery cavy = new TermQuery(new Term("field", "cavy"));

    // A multi-word synonym source will form a graph query for synonyms that formed the graph token stream
    BooleanQuery.Builder synonym = new BooleanQuery.Builder();
    synonym.add(guinea, BooleanClause.Occur.MUST);
    synonym.add(pig, BooleanClause.Occur.MUST);
    BooleanQuery guineaPig = synonym.build();

    BooleanQuery graphQuery = new BooleanQuery.Builder()
        .add(new BooleanQuery.Builder()
            .add(guineaPig, BooleanClause.Occur.SHOULD)
            .add(cavy, BooleanClause.Occur.SHOULD)
            .build(), BooleanClause.Occur.SHOULD)
        .build();
    assertEquals(graphQuery, parser.parse("guinea pig"));

    boolean oldSplitOnWhitespace = splitOnWhitespace;
    splitOnWhitespace = QueryParser.DEFAULT_SPLIT_ON_WHITESPACE;
    assertQueryEquals("guinea pig", new MockSynonymAnalyzer(), "((+guinea +pig) cavy)");
    splitOnWhitespace = oldSplitOnWhitespace;
  }
   
  public void testWildcardAlone() throws ParseException {
    //seems like crazy edge case, but can be useful in concordance 
    QueryParser parser = new QueryParser(FIELD, new ASCIIAnalyzer());
    parser.setAllowLeadingWildcard(false);
    expectThrows(ParseException.class, () -> {
      parser.parse("*");
    });

    QueryParser parser2 = new QueryParser("*", new ASCIIAnalyzer());
    parser2.setAllowLeadingWildcard(false);
    assertEquals(new MatchAllDocsQuery(), parser2.parse("*"));
  }

  public void testWildCardEscapes() throws ParseException, IOException {
    Analyzer a = new ASCIIAnalyzer();
    QueryParser parser = new QueryParser(FIELD, a);
    assertTrue(isAHit(parser.parse("mö*tley"), "moatley", a));
    // need to have at least one genuine wildcard to trigger the wildcard analysis
    // hence the * before the y
    assertTrue(isAHit(parser.parse("mö\\*tl*y"), "mo*tley", a));
    // escaped backslash then true wildcard
    assertTrue(isAHit(parser.parse("mö\\\\*tley"), "mo\\atley", a));
    // escaped wildcard then true wildcard
    assertTrue(isAHit(parser.parse("mö\\??ley"), "mo?tley", a));

    // the first is an escaped * which should yield a miss
    assertFalse(isAHit(parser.parse("mö\\*tl*y"), "moatley", a));
  }

  public void testWildcardDoesNotNormalizeEscapedChars() throws Exception {
    Analyzer asciiAnalyzer = new ASCIIAnalyzer();
    Analyzer keywordAnalyzer = new MockAnalyzer(random());
    QueryParser parser = new QueryParser(FIELD, asciiAnalyzer);

    assertTrue(isAHit(parser.parse("e*e"), "étude", asciiAnalyzer));
    assertTrue(isAHit(parser.parse("é*e"), "etude", asciiAnalyzer));
    assertFalse(isAHit(parser.parse("\\é*e"), "etude", asciiAnalyzer));
    assertTrue(isAHit(parser.parse("\\é*e"), "étude", keywordAnalyzer));
  }

  public void testWildCardQuery() throws ParseException {
    Analyzer a = new ASCIIAnalyzer();
    QueryParser parser = new QueryParser(FIELD, a);
    parser.setAllowLeadingWildcard(true);
    assertEquals("*bersetzung uber*ung", parser.parse("*bersetzung über*ung").toString(FIELD));
    parser.setAllowLeadingWildcard(false);
    assertEquals("motley crue motl?* cru?", parser.parse("Mötley Cr\u00fce Mötl?* Crü?").toString(FIELD));
    assertEquals("renee zellweger ren?? zellw?ger", parser.parse("Renée Zellweger Ren?? Zellw?ger").toString(FIELD));
  }


  public void testPrefixQuery() throws ParseException {
    Analyzer a = new ASCIIAnalyzer();
    QueryParser parser = new QueryParser(FIELD, a);
    assertEquals("ubersetzung ubersetz*", parser.parse("übersetzung übersetz*").toString(FIELD));
    assertEquals("motley crue motl* cru*", parser.parse("Mötley Crüe Mötl* crü*").toString(FIELD));
    assertEquals("rene? zellw*", parser.parse("René? Zellw*").toString(FIELD));
  }

  public void testRangeQuery() throws ParseException {
    Analyzer a = new ASCIIAnalyzer();
    QueryParser parser = new QueryParser(FIELD, a);
    assertEquals("[aa TO bb]", parser.parse("[aa TO bb]").toString(FIELD));
    assertEquals("{anais TO zoe}", parser.parse("{Anaïs TO Zoé}").toString(FIELD));
  }

  public void testFuzzyQuery() throws ParseException {
    Analyzer a = new ASCIIAnalyzer();
    QueryParser parser = new QueryParser(FIELD, a);
    assertEquals("ubersetzung ubersetzung~1", parser.parse("Übersetzung Übersetzung~0.9").toString(FIELD));
    assertEquals("motley crue motley~1 crue~2", parser.parse("Mötley Crüe Mötley~0.75 Crüe~0.5").toString(FIELD));
    assertEquals("renee zellweger renee~0 zellweger~2", parser.parse("Renée Zellweger Renée~0.9 Zellweger~").toString(FIELD));
  }

  final static class FoldingFilter extends TokenFilter {
    final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

    public FoldingFilter(TokenStream input) {
      super(input);
    }

    @Override
    public boolean incrementToken() throws IOException {
      if (input.incrementToken()) {
        char term[] = termAtt.buffer();
        for (int i = 0; i < term.length; i++)
          switch(term[i]) {
            case 'ü':
              term[i] = 'u'; 
              break;
            case 'ö': 
              term[i] = 'o'; 
              break;
            case 'é': 
              term[i] = 'e'; 
              break;
            case 'ï': 
              term[i] = 'i'; 
              break;
          }
        return true;
      } else {
        return false;
      }
    }
  }

  final static class ASCIIAnalyzer extends Analyzer {
    @Override
    public TokenStreamComponents createComponents(String fieldName) {
      Tokenizer result = new MockTokenizer(MockTokenizer.WHITESPACE, true);
      return new TokenStreamComponents(result, new FoldingFilter(result));
    }
    @Override
    protected TokenStream normalize(String fieldName, TokenStream in) {
      return new FoldingFilter(new MockLowerCaseFilter(in));
    }
  }

  // LUCENE-4176
  public void testByteTerms() throws Exception {
    String s = "เข";
    Analyzer analyzer = new MockBytesAnalyzer();
    QueryParser qp = new QueryParser(FIELD, analyzer);

    assertTrue(isAHit(qp.parse("[เข TO เข]"), s, analyzer));
    assertTrue(isAHit(qp.parse("เข~1"), s, analyzer));
    assertTrue(isAHit(qp.parse("เข*"), s, analyzer));
    assertTrue(isAHit(qp.parse("เ*"), s, analyzer));
    assertTrue(isAHit(qp.parse("เ??"), s, analyzer));
  }
   
  // LUCENE-7533
  public void test_splitOnWhitespace_with_autoGeneratePhraseQueries() {
    final QueryParser qp = new QueryParser(FIELD, new MockAnalyzer(random()));
    expectThrows(IllegalArgumentException.class, () -> {
      qp.setSplitOnWhitespace(false);
      qp.setAutoGeneratePhraseQueries(true);
    });
    final QueryParser qp2 = new QueryParser(FIELD, new MockAnalyzer(random()));
    expectThrows(IllegalArgumentException.class, () -> {
      qp2.setSplitOnWhitespace(true);
      qp2.setAutoGeneratePhraseQueries(true);
      qp2.setSplitOnWhitespace(false);
    });
  }
  
  private boolean isAHit(Query q, String content, Analyzer analyzer) throws IOException{
    Directory ramDir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), ramDir, analyzer);
    Document doc = new Document();
    FieldType fieldType = new FieldType();
    fieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    fieldType.setTokenized(true);
    fieldType.setStored(true);
    Field field = new Field(FIELD, content, fieldType);
    doc.add(field);
    writer.addDocument(doc);
    writer.close();
    DirectoryReader ir = DirectoryReader.open(ramDir);
    IndexSearcher is = new IndexSearcher(ir);
      
    long hits = is.count(q);
    ir.close();
    ramDir.close();
    if (hits == 1){
      return true;
    } else {
      return false;
    }

  }
}
