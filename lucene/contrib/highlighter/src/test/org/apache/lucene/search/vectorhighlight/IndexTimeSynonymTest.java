package org.apache.lucene.search.vectorhighlight;

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
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.util.AttributeImpl;

public class IndexTimeSynonymTest extends AbstractTestCase {
  
  public void testFieldTermStackIndex1wSearch1term() throws Exception {
    makeIndex1w();
    
    FieldQuery fq = new FieldQuery( tq( "Mac" ), true, true );
    FieldTermStack stack = new FieldTermStack( reader, 0, F, fq );
    assertEquals( 1, stack.termList.size() );
    assertEquals( "Mac(11,20,3)", stack.pop().toString() );
  }
  
  public void testFieldTermStackIndex1wSearch2terms() throws Exception {
    makeIndex1w();

    BooleanQuery bq = new BooleanQuery();
    bq.add( tq( "Mac" ), Occur.SHOULD );
    bq.add( tq( "MacBook" ), Occur.SHOULD );
    FieldQuery fq = new FieldQuery( bq, true, true );
    FieldTermStack stack = new FieldTermStack( reader, 0, F, fq );
    assertEquals( 2, stack.termList.size() );
    Set<String> expectedSet = new HashSet<String>();
    expectedSet.add( "Mac(11,20,3)" );
    expectedSet.add( "MacBook(11,20,3)" );
    assertTrue( expectedSet.contains( stack.pop().toString() ) );
    assertTrue( expectedSet.contains( stack.pop().toString() ) );
  }
  
  public void testFieldTermStackIndex1w2wSearch1term() throws Exception {
    makeIndex1w2w();
    
    FieldQuery fq = new FieldQuery( tq( "pc" ), true, true );
    FieldTermStack stack = new FieldTermStack( reader, 0, F, fq );
    assertEquals( 1, stack.termList.size() );
    assertEquals( "pc(3,5,1)", stack.pop().toString() );
  }
  
  public void testFieldTermStackIndex1w2wSearch1phrase() throws Exception {
    makeIndex1w2w();
    
    FieldQuery fq = new FieldQuery( pqF( "personal", "computer" ), true, true );
    FieldTermStack stack = new FieldTermStack( reader, 0, F, fq );
    assertEquals( 2, stack.termList.size() );
    assertEquals( "personal(3,5,1)", stack.pop().toString() );
    assertEquals( "computer(3,5,2)", stack.pop().toString() );
  }
  
  public void testFieldTermStackIndex1w2wSearch1partial() throws Exception {
    makeIndex1w2w();
    
    FieldQuery fq = new FieldQuery( tq( "computer" ), true, true );
    FieldTermStack stack = new FieldTermStack( reader, 0, F, fq );
    assertEquals( 1, stack.termList.size() );
    assertEquals( "computer(3,5,2)", stack.pop().toString() );
  }
  
  public void testFieldTermStackIndex1w2wSearch1term1phrase() throws Exception {
    makeIndex1w2w();

    BooleanQuery bq = new BooleanQuery();
    bq.add( tq( "pc" ), Occur.SHOULD );
    bq.add( pqF( "personal", "computer" ), Occur.SHOULD );
    FieldQuery fq = new FieldQuery( bq, true, true );
    FieldTermStack stack = new FieldTermStack( reader, 0, F, fq );
    assertEquals( 3, stack.termList.size() );
    Set<String> expectedSet = new HashSet<String>();
    expectedSet.add( "pc(3,5,1)" );
    expectedSet.add( "personal(3,5,1)" );
    assertTrue( expectedSet.contains( stack.pop().toString() ) );
    assertTrue( expectedSet.contains( stack.pop().toString() ) );
    assertEquals( "computer(3,5,2)", stack.pop().toString() );
  }
  
  public void testFieldTermStackIndex2w1wSearch1term() throws Exception {
    makeIndex2w1w();
    
    FieldQuery fq = new FieldQuery( tq( "pc" ), true, true );
    FieldTermStack stack = new FieldTermStack( reader, 0, F, fq );
    assertEquals( 1, stack.termList.size() );
    assertEquals( "pc(3,20,1)", stack.pop().toString() );
  }
  
  public void testFieldTermStackIndex2w1wSearch1phrase() throws Exception {
    makeIndex2w1w();
    
    FieldQuery fq = new FieldQuery( pqF( "personal", "computer" ), true, true );
    FieldTermStack stack = new FieldTermStack( reader, 0, F, fq );
    assertEquals( 2, stack.termList.size() );
    assertEquals( "personal(3,20,1)", stack.pop().toString() );
    assertEquals( "computer(3,20,2)", stack.pop().toString() );
  }
  
  public void testFieldTermStackIndex2w1wSearch1partial() throws Exception {
    makeIndex2w1w();
    
    FieldQuery fq = new FieldQuery( tq( "computer" ), true, true );
    FieldTermStack stack = new FieldTermStack( reader, 0, F, fq );
    assertEquals( 1, stack.termList.size() );
    assertEquals( "computer(3,20,2)", stack.pop().toString() );
  }
  
  public void testFieldTermStackIndex2w1wSearch1term1phrase() throws Exception {
    makeIndex2w1w();

    BooleanQuery bq = new BooleanQuery();
    bq.add( tq( "pc" ), Occur.SHOULD );
    bq.add( pqF( "personal", "computer" ), Occur.SHOULD );
    FieldQuery fq = new FieldQuery( bq, true, true );
    FieldTermStack stack = new FieldTermStack( reader, 0, F, fq );
    assertEquals( 3, stack.termList.size() );
    Set<String> expectedSet = new HashSet<String>();
    expectedSet.add( "pc(3,20,1)" );
    expectedSet.add( "personal(3,20,1)" );
    assertTrue( expectedSet.contains( stack.pop().toString() ) );
    assertTrue( expectedSet.contains( stack.pop().toString() ) );
    assertEquals( "computer(3,20,2)", stack.pop().toString() );
  }
  
  public void testFieldPhraseListIndex1w2wSearch1phrase() throws Exception {
    makeIndex1w2w();
    
    FieldQuery fq = new FieldQuery( pqF( "personal", "computer" ), true, true );
    FieldTermStack stack = new FieldTermStack( reader, 0, F, fq );
    FieldPhraseList fpl = new FieldPhraseList( stack, fq );
    assertEquals( 1, fpl.phraseList.size() );
    assertEquals( "personalcomputer(1.0)((3,5))", fpl.phraseList.get( 0 ).toString() );
    assertEquals( 3, fpl.phraseList.get( 0 ).getStartOffset() );
    assertEquals( 5, fpl.phraseList.get( 0 ).getEndOffset() );
  }
  
  public void testFieldPhraseListIndex1w2wSearch1partial() throws Exception {
    makeIndex1w2w();
    
    FieldQuery fq = new FieldQuery( tq( "computer" ), true, true );
    FieldTermStack stack = new FieldTermStack( reader, 0, F, fq );
    FieldPhraseList fpl = new FieldPhraseList( stack, fq );
    assertEquals( 1, fpl.phraseList.size() );
    assertEquals( "computer(1.0)((3,5))", fpl.phraseList.get( 0 ).toString() );
    assertEquals( 3, fpl.phraseList.get( 0 ).getStartOffset() );
    assertEquals( 5, fpl.phraseList.get( 0 ).getEndOffset() );
  }
  
  public void testFieldPhraseListIndex1w2wSearch1term1phrase() throws Exception {
    makeIndex1w2w();

    BooleanQuery bq = new BooleanQuery();
    bq.add( tq( "pc" ), Occur.SHOULD );
    bq.add( pqF( "personal", "computer" ), Occur.SHOULD );
    FieldQuery fq = new FieldQuery( bq, true, true );
    FieldTermStack stack = new FieldTermStack( reader, 0, F, fq );
    FieldPhraseList fpl = new FieldPhraseList( stack, fq );
    assertEquals( 1, fpl.phraseList.size() );
    assertTrue( fpl.phraseList.get( 0 ).toString().indexOf( "(1.0)((3,5))" ) > 0 );
    assertEquals( 3, fpl.phraseList.get( 0 ).getStartOffset() );
    assertEquals( 5, fpl.phraseList.get( 0 ).getEndOffset() );
  }
  
  public void testFieldPhraseListIndex2w1wSearch1term() throws Exception {
    makeIndex2w1w();
    
    FieldQuery fq = new FieldQuery( tq( "pc" ), true, true );
    FieldTermStack stack = new FieldTermStack( reader, 0, F, fq );
    FieldPhraseList fpl = new FieldPhraseList( stack, fq );
    assertEquals( 1, fpl.phraseList.size() );
    assertEquals( "pc(1.0)((3,20))", fpl.phraseList.get( 0 ).toString() );
    assertEquals( 3, fpl.phraseList.get( 0 ).getStartOffset() );
    assertEquals( 20, fpl.phraseList.get( 0 ).getEndOffset() );
  }
  
  public void testFieldPhraseListIndex2w1wSearch1phrase() throws Exception {
    makeIndex2w1w();
    
    FieldQuery fq = new FieldQuery( pqF( "personal", "computer" ), true, true );
    FieldTermStack stack = new FieldTermStack( reader, 0, F, fq );
    FieldPhraseList fpl = new FieldPhraseList( stack, fq );
    assertEquals( 1, fpl.phraseList.size() );
    assertEquals( "personalcomputer(1.0)((3,20))", fpl.phraseList.get( 0 ).toString() );
    assertEquals( 3, fpl.phraseList.get( 0 ).getStartOffset() );
    assertEquals( 20, fpl.phraseList.get( 0 ).getEndOffset() );
  }
  
  public void testFieldPhraseListIndex2w1wSearch1partial() throws Exception {
    makeIndex2w1w();
    
    FieldQuery fq = new FieldQuery( tq( "computer" ), true, true );
    FieldTermStack stack = new FieldTermStack( reader, 0, F, fq );
    FieldPhraseList fpl = new FieldPhraseList( stack, fq );
    assertEquals( 1, fpl.phraseList.size() );
    assertEquals( "computer(1.0)((3,20))", fpl.phraseList.get( 0 ).toString() );
    assertEquals( 3, fpl.phraseList.get( 0 ).getStartOffset() );
    assertEquals( 20, fpl.phraseList.get( 0 ).getEndOffset() );
  }
  
  public void testFieldPhraseListIndex2w1wSearch1term1phrase() throws Exception {
    makeIndex2w1w();

    BooleanQuery bq = new BooleanQuery();
    bq.add( tq( "pc" ), Occur.SHOULD );
    bq.add( pqF( "personal", "computer" ), Occur.SHOULD );
    FieldQuery fq = new FieldQuery( bq, true, true );
    FieldTermStack stack = new FieldTermStack( reader, 0, F, fq );
    FieldPhraseList fpl = new FieldPhraseList( stack, fq );
    assertEquals( 1, fpl.phraseList.size() );
    assertTrue( fpl.phraseList.get( 0 ).toString().indexOf( "(1.0)((3,20))" ) > 0 );
    assertEquals( 3, fpl.phraseList.get( 0 ).getStartOffset() );
    assertEquals( 20, fpl.phraseList.get( 0 ).getEndOffset() );
  }

  private void makeIndex1w() throws Exception {
    //           11111111112
    // 012345678901234567890
    // I'll buy a Macintosh
    //            Mac
    //            MacBook
    // 0    1   2 3
    makeSynonymIndex( "I'll buy a Macintosh",
        t("I'll",0,4),
        t("buy",5,8),
        t("a",9,10),
        t("Macintosh",11,20),t("Mac",11,20,0),t("MacBook",11,20,0));
  }

  private void makeIndex1w2w() throws Exception {
    //           1111111
    // 01234567890123456
    // My pc was broken
    //    personal computer
    // 0  1  2   3
    makeSynonymIndex( "My pc was broken",
        t("My",0,2),
        t("pc",3,5),t("personal",3,5,0),t("computer",3,5),
        t("was",6,9),
        t("broken",10,16));
  }

  private void makeIndex2w1w() throws Exception {
    //           1111111111222222222233
    // 01234567890123456789012345678901
    // My personal computer was broken
    //    pc
    // 0  1        2        3   4
    makeSynonymIndex( "My personal computer was broken",
        t("My",0,2),
        t("personal",3,20),t("pc",3,20,0),t("computer",3,20),
        t("was",21,24),
        t("broken",25,31));
  }
  
  void makeSynonymIndex( String value, Token... tokens ) throws Exception {
    Analyzer analyzer = new TokenArrayAnalyzer( tokens );
    make1dmfIndex( analyzer, value );
  }

  public static Token t( String text, int startOffset, int endOffset ){
    return t( text, startOffset, endOffset, 1 );
  }
  
  public static Token t( String text, int startOffset, int endOffset, int positionIncrement ){
    Token token = new Token( text, startOffset, endOffset );
    token.setPositionIncrement( positionIncrement );
    return token;
  }
  
  public static final class TokenArrayAnalyzer extends Analyzer {
    final Token[] tokens;
    public TokenArrayAnalyzer(Token... tokens) {
      this.tokens = tokens;
    }
    
    @Override
    public TokenStreamComponents createComponents(String fieldName, Reader reader) {
      Tokenizer ts = new Tokenizer(Token.TOKEN_ATTRIBUTE_FACTORY, reader) {
        final AttributeImpl reusableToken = (AttributeImpl) addAttribute(CharTermAttribute.class);
        int p = 0;
        
        @Override
        public boolean incrementToken() throws IOException {
          if( p >= tokens.length ) return false;
          clearAttributes();
          tokens[p++].copyTo(reusableToken);
          return true;
        }

        @Override
        public void reset() throws IOException {
          super.reset();
          this.p = 0;
        }
      };
      return new TokenStreamComponents(ts);
    }
  }
}
