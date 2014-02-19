package org.apache.lucene.util;

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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.RegExp;

public class TestQueryBuilder extends LuceneTestCase {
  
  public void testTerm() {
    TermQuery expected = new TermQuery(new Term("field", "test"));
    QueryBuilder builder = new QueryBuilder(new MockAnalyzer(random()));
    assertEquals(expected, builder.createBooleanQuery("field", "test"));
  }
  
  public void testBoolean() {
    BooleanQuery expected = new BooleanQuery();
    expected.add(new TermQuery(new Term("field", "foo")), BooleanClause.Occur.SHOULD);
    expected.add(new TermQuery(new Term("field", "bar")), BooleanClause.Occur.SHOULD);
    QueryBuilder builder = new QueryBuilder(new MockAnalyzer(random()));
    assertEquals(expected, builder.createBooleanQuery("field", "foo bar"));
  }
  
  public void testBooleanMust() {
    BooleanQuery expected = new BooleanQuery();
    expected.add(new TermQuery(new Term("field", "foo")), BooleanClause.Occur.MUST);
    expected.add(new TermQuery(new Term("field", "bar")), BooleanClause.Occur.MUST);
    QueryBuilder builder = new QueryBuilder(new MockAnalyzer(random()));
    assertEquals(expected, builder.createBooleanQuery("field", "foo bar", BooleanClause.Occur.MUST));
  }
  
  public void testMinShouldMatchNone() {
    QueryBuilder builder = new QueryBuilder(new MockAnalyzer(random()));
    assertEquals(builder.createBooleanQuery("field", "one two three four"),
                 builder.createMinShouldMatchQuery("field", "one two three four", 0f));
  }
  
  public void testMinShouldMatchAll() {
    QueryBuilder builder = new QueryBuilder(new MockAnalyzer(random()));
    assertEquals(builder.createBooleanQuery("field", "one two three four", BooleanClause.Occur.MUST),
                 builder.createMinShouldMatchQuery("field", "one two three four", 1f));
  }
  
  public void testMinShouldMatch() {
    BooleanQuery expected = new BooleanQuery();
    expected.add(new TermQuery(new Term("field", "one")), BooleanClause.Occur.SHOULD);
    expected.add(new TermQuery(new Term("field", "two")), BooleanClause.Occur.SHOULD);
    expected.add(new TermQuery(new Term("field", "three")), BooleanClause.Occur.SHOULD);
    expected.add(new TermQuery(new Term("field", "four")), BooleanClause.Occur.SHOULD);
    expected.setMinimumNumberShouldMatch(0);
    
    QueryBuilder builder = new QueryBuilder(new MockAnalyzer(random()));
    assertEquals(expected, builder.createMinShouldMatchQuery("field", "one two three four", 0.1f));
    assertEquals(expected, builder.createMinShouldMatchQuery("field", "one two three four", 0.24f));
    
    expected.setMinimumNumberShouldMatch(1);
    assertEquals(expected, builder.createMinShouldMatchQuery("field", "one two three four", 0.25f));
    assertEquals(expected, builder.createMinShouldMatchQuery("field", "one two three four", 0.49f));

    expected.setMinimumNumberShouldMatch(2);
    assertEquals(expected, builder.createMinShouldMatchQuery("field", "one two three four", 0.5f));
    assertEquals(expected, builder.createMinShouldMatchQuery("field", "one two three four", 0.74f));
    
    expected.setMinimumNumberShouldMatch(3);
    assertEquals(expected, builder.createMinShouldMatchQuery("field", "one two three four", 0.75f));
    assertEquals(expected, builder.createMinShouldMatchQuery("field", "one two three four", 0.99f));
  }
  
  public void testPhraseQueryPositionIncrements() throws Exception {
    PhraseQuery expected = new PhraseQuery();
    expected.add(new Term("field", "1"));
    expected.add(new Term("field", "2"), 2);
    
    CharacterRunAutomaton stopList = new CharacterRunAutomaton(new RegExp("[sS][tT][oO][pP]").toAutomaton());

    Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false, stopList);

    QueryBuilder builder = new QueryBuilder(analyzer);
    assertEquals(expected, builder.createPhraseQuery("field", "1 stop 2"));
  }
  
  public void testEmpty() {
    QueryBuilder builder = new QueryBuilder(new MockAnalyzer(random()));
    assertNull(builder.createBooleanQuery("field", ""));
  }
  
  /** adds synonym of "dog" for "dogs". */
  static class MockSynonymAnalyzer extends Analyzer {
    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
      MockTokenizer tokenizer = new MockTokenizer();
      return new TokenStreamComponents(tokenizer, new MockSynonymFilter(tokenizer));
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
  
  /** simple synonyms test */
  public void testSynonyms() throws Exception {
    BooleanQuery expected = new BooleanQuery(true);
    expected.add(new TermQuery(new Term("field", "dogs")), BooleanClause.Occur.SHOULD);
    expected.add(new TermQuery(new Term("field", "dog")), BooleanClause.Occur.SHOULD);
    QueryBuilder builder = new QueryBuilder(new MockSynonymAnalyzer());
    assertEquals(expected, builder.createBooleanQuery("field", "dogs"));
    assertEquals(expected, builder.createPhraseQuery("field", "dogs"));
    assertEquals(expected, builder.createBooleanQuery("field", "dogs", BooleanClause.Occur.MUST));
    assertEquals(expected, builder.createPhraseQuery("field", "dogs"));
  }
  
  /** forms multiphrase query */
  public void testSynonymsPhrase() throws Exception {
    MultiPhraseQuery expected = new MultiPhraseQuery();
    expected.add(new Term("field", "old"));
    expected.add(new Term[] { new Term("field", "dogs"), new Term("field", "dog") });
    QueryBuilder builder = new QueryBuilder(new MockSynonymAnalyzer());
    assertEquals(expected, builder.createPhraseQuery("field", "old dogs"));
  }

  protected static class SimpleCJKTokenizer extends Tokenizer {
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
  
  private class SimpleCJKAnalyzer extends Analyzer {
    @Override
    public TokenStreamComponents createComponents(String fieldName) {
      return new TokenStreamComponents(new SimpleCJKTokenizer());
    }
  }
  
  public void testCJKTerm() throws Exception {
    // individual CJK chars as terms
    SimpleCJKAnalyzer analyzer = new SimpleCJKAnalyzer(); 
    
    BooleanQuery expected = new BooleanQuery();
    expected.add(new TermQuery(new Term("field", "中")), BooleanClause.Occur.SHOULD);
    expected.add(new TermQuery(new Term("field", "国")), BooleanClause.Occur.SHOULD);
    
    QueryBuilder builder = new QueryBuilder(analyzer);
    assertEquals(expected, builder.createBooleanQuery("field", "中国"));
  }
  
  public void testCJKPhrase() throws Exception {
    // individual CJK chars as terms
    SimpleCJKAnalyzer analyzer = new SimpleCJKAnalyzer();
    
    PhraseQuery expected = new PhraseQuery();
    expected.add(new Term("field", "中"));
    expected.add(new Term("field", "国"));
    
    QueryBuilder builder = new QueryBuilder(analyzer);
    assertEquals(expected, builder.createPhraseQuery("field", "中国"));
  }
  
  public void testCJKSloppyPhrase() throws Exception {
    // individual CJK chars as terms
    SimpleCJKAnalyzer analyzer = new SimpleCJKAnalyzer();
    
    PhraseQuery expected = new PhraseQuery();
    expected.setSlop(3);
    expected.add(new Term("field", "中"));
    expected.add(new Term("field", "国"));
    
    QueryBuilder builder = new QueryBuilder(analyzer);
    assertEquals(expected, builder.createPhraseQuery("field", "中国", 3));
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
    BooleanQuery expected = new BooleanQuery(true);
    expected.add(new TermQuery(new Term("field", "国")), BooleanClause.Occur.SHOULD);
    expected.add(new TermQuery(new Term("field", "國")), BooleanClause.Occur.SHOULD);
    QueryBuilder builder = new QueryBuilder(new MockCJKSynonymAnalyzer());
    assertEquals(expected, builder.createBooleanQuery("field", "国"));
    assertEquals(expected, builder.createPhraseQuery("field", "国"));
    assertEquals(expected, builder.createBooleanQuery("field", "国", BooleanClause.Occur.MUST));
  }
  
  /** synonyms with default OR operator */
  public void testCJKSynonymsOR() throws Exception {
    BooleanQuery expected = new BooleanQuery();
    expected.add(new TermQuery(new Term("field", "中")), BooleanClause.Occur.SHOULD);
    BooleanQuery inner = new BooleanQuery(true);
    inner.add(new TermQuery(new Term("field", "国")), BooleanClause.Occur.SHOULD);
    inner.add(new TermQuery(new Term("field", "國")), BooleanClause.Occur.SHOULD);
    expected.add(inner, BooleanClause.Occur.SHOULD);
    QueryBuilder builder = new QueryBuilder(new MockCJKSynonymAnalyzer());
    assertEquals(expected, builder.createBooleanQuery("field", "中国"));
  }
  
  /** more complex synonyms with default OR operator */
  public void testCJKSynonymsOR2() throws Exception {
    BooleanQuery expected = new BooleanQuery();
    expected.add(new TermQuery(new Term("field", "中")), BooleanClause.Occur.SHOULD);
    BooleanQuery inner = new BooleanQuery(true);
    inner.add(new TermQuery(new Term("field", "国")), BooleanClause.Occur.SHOULD);
    inner.add(new TermQuery(new Term("field", "國")), BooleanClause.Occur.SHOULD);
    expected.add(inner, BooleanClause.Occur.SHOULD);
    BooleanQuery inner2 = new BooleanQuery(true);
    inner2.add(new TermQuery(new Term("field", "国")), BooleanClause.Occur.SHOULD);
    inner2.add(new TermQuery(new Term("field", "國")), BooleanClause.Occur.SHOULD);
    expected.add(inner2, BooleanClause.Occur.SHOULD);
    QueryBuilder builder = new QueryBuilder(new MockCJKSynonymAnalyzer());
    assertEquals(expected, builder.createBooleanQuery("field", "中国国"));
  }
  
  /** synonyms with default AND operator */
  public void testCJKSynonymsAND() throws Exception {
    BooleanQuery expected = new BooleanQuery();
    expected.add(new TermQuery(new Term("field", "中")), BooleanClause.Occur.MUST);
    BooleanQuery inner = new BooleanQuery(true);
    inner.add(new TermQuery(new Term("field", "国")), BooleanClause.Occur.SHOULD);
    inner.add(new TermQuery(new Term("field", "國")), BooleanClause.Occur.SHOULD);
    expected.add(inner, BooleanClause.Occur.MUST);
    QueryBuilder builder = new QueryBuilder(new MockCJKSynonymAnalyzer());
    assertEquals(expected, builder.createBooleanQuery("field", "中国", BooleanClause.Occur.MUST));
  }
  
  /** more complex synonyms with default AND operator */
  public void testCJKSynonymsAND2() throws Exception {
    BooleanQuery expected = new BooleanQuery();
    expected.add(new TermQuery(new Term("field", "中")), BooleanClause.Occur.MUST);
    BooleanQuery inner = new BooleanQuery(true);
    inner.add(new TermQuery(new Term("field", "国")), BooleanClause.Occur.SHOULD);
    inner.add(new TermQuery(new Term("field", "國")), BooleanClause.Occur.SHOULD);
    expected.add(inner, BooleanClause.Occur.MUST);
    BooleanQuery inner2 = new BooleanQuery(true);
    inner2.add(new TermQuery(new Term("field", "国")), BooleanClause.Occur.SHOULD);
    inner2.add(new TermQuery(new Term("field", "國")), BooleanClause.Occur.SHOULD);
    expected.add(inner2, BooleanClause.Occur.MUST);
    QueryBuilder builder = new QueryBuilder(new MockCJKSynonymAnalyzer());
    assertEquals(expected, builder.createBooleanQuery("field", "中国国", BooleanClause.Occur.MUST));
  }
  
  /** forms multiphrase query */
  public void testCJKSynonymsPhrase() throws Exception {
    MultiPhraseQuery expected = new MultiPhraseQuery();
    expected.add(new Term("field", "中"));
    expected.add(new Term[] { new Term("field", "国"), new Term("field", "國")});
    QueryBuilder builder = new QueryBuilder(new MockCJKSynonymAnalyzer());
    assertEquals(expected, builder.createPhraseQuery("field", "中国"));
    expected.setSlop(3);
    assertEquals(expected, builder.createPhraseQuery("field", "中国", 3));
  }
}
