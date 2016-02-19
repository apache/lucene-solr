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
package org.apache.lucene.search.vectorhighlight;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;

public class SimpleFragListBuilderTest extends AbstractTestCase {
  
  public void testNullFieldFragList() throws Exception {
    SimpleFragListBuilder sflb = new SimpleFragListBuilder();
    FieldFragList ffl = sflb.createFieldFragList( fpl(new TermQuery(new Term(F, "a")), "b c d" ), 100 );
    assertEquals( 0, ffl.getFragInfos().size() );
  }
  
  public void testTooSmallFragSize() throws Exception {
    expectThrows(IllegalArgumentException.class, () -> {
      SimpleFragListBuilder sflb = new SimpleFragListBuilder();
      sflb.createFieldFragList(fpl(new TermQuery(new Term(F, "a")), "b c d"), sflb.minFragCharSize - 1);
    });
  }
  
  public void testSmallerFragSizeThanTermQuery() throws Exception {
    SimpleFragListBuilder sflb = new SimpleFragListBuilder();
    FieldFragList ffl = sflb.createFieldFragList( fpl(new TermQuery(new Term(F, "abcdefghijklmnopqrs")), "abcdefghijklmnopqrs" ), sflb.minFragCharSize );
    assertEquals( 1, ffl.getFragInfos().size() );
    assertEquals( "subInfos=(abcdefghijklmnopqrs((0,19)))/1.0(0,19)", ffl.getFragInfos().get( 0 ).toString() );
  }
  
  public void testSmallerFragSizeThanPhraseQuery() throws Exception {
    SimpleFragListBuilder sflb = new SimpleFragListBuilder();

    PhraseQuery phraseQuery = new PhraseQuery(F, "abcdefgh", "jklmnopqrs");

    FieldFragList ffl = sflb.createFieldFragList( fpl(phraseQuery, "abcdefgh   jklmnopqrs" ), sflb.minFragCharSize );
    assertEquals( 1, ffl.getFragInfos().size() );
    if (VERBOSE) System.out.println( ffl.getFragInfos().get( 0 ).toString() );
    assertEquals( "subInfos=(abcdefghjklmnopqrs((0,21)))/1.0(0,21)", ffl.getFragInfos().get( 0 ).toString() );
  }
  
  public void test1TermIndex() throws Exception {
    SimpleFragListBuilder sflb = new SimpleFragListBuilder();
    FieldFragList ffl = sflb.createFieldFragList( fpl(new TermQuery(new Term(F, "a")), "a" ), 100 );
    assertEquals( 1, ffl.getFragInfos().size() );
    assertEquals( "subInfos=(a((0,1)))/1.0(0,100)", ffl.getFragInfos().get( 0 ).toString() );
  }
  
  public void test2TermsIndex1Frag() throws Exception {
    SimpleFragListBuilder sflb = new SimpleFragListBuilder();
    FieldFragList ffl = sflb.createFieldFragList( fpl(new TermQuery(new Term(F, "a")), "a a" ), 100 );
    assertEquals( 1, ffl.getFragInfos().size() );
    assertEquals( "subInfos=(a((0,1))a((2,3)))/2.0(0,100)", ffl.getFragInfos().get( 0 ).toString() );
  
    ffl = sflb.createFieldFragList( fpl(new TermQuery(new Term(F, "a")), "a b b b b b b b b a" ), 20 );
    assertEquals( 1, ffl.getFragInfos().size() );
    assertEquals( "subInfos=(a((0,1))a((18,19)))/2.0(0,20)", ffl.getFragInfos().get( 0 ).toString() );

    ffl = sflb.createFieldFragList( fpl(new TermQuery(new Term(F, "a")), "b b b b a b b b b a" ), 20 );
    assertEquals( 1, ffl.getFragInfos().size() );
    assertEquals( "subInfos=(a((8,9))a((18,19)))/2.0(4,24)", ffl.getFragInfos().get( 0 ).toString() );
  }
  
  public void test2TermsIndex2Frags() throws Exception {
    SimpleFragListBuilder sflb = new SimpleFragListBuilder();
    FieldFragList ffl = sflb.createFieldFragList( fpl(new TermQuery(new Term(F, "a")), "a b b b b b b b b b b b b b a" ), 20 );
    assertEquals( 2, ffl.getFragInfos().size() );
    assertEquals( "subInfos=(a((0,1)))/1.0(0,20)", ffl.getFragInfos().get( 0 ).toString() );
    assertEquals( "subInfos=(a((28,29)))/1.0(20,40)", ffl.getFragInfos().get( 1 ).toString() );

    ffl = sflb.createFieldFragList( fpl(new TermQuery(new Term(F, "a")), "a b b b b b b b b b b b b a" ), 20 );
    assertEquals( 2, ffl.getFragInfos().size() );
    assertEquals( "subInfos=(a((0,1)))/1.0(0,20)", ffl.getFragInfos().get( 0 ).toString() );
    assertEquals( "subInfos=(a((26,27)))/1.0(20,40)", ffl.getFragInfos().get( 1 ).toString() );

    ffl = sflb.createFieldFragList( fpl(new TermQuery(new Term(F, "a")), "a b b b b b b b b b a" ), 20 );
    assertEquals( 2, ffl.getFragInfos().size() );
    assertEquals( "subInfos=(a((0,1)))/1.0(0,20)", ffl.getFragInfos().get( 0 ).toString() );
    assertEquals( "subInfos=(a((20,21)))/1.0(20,40)", ffl.getFragInfos().get( 1 ).toString() );
  }
  
  public void test2TermsQuery() throws Exception {
    SimpleFragListBuilder sflb = new SimpleFragListBuilder();

    BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();
    booleanQuery.add(new TermQuery(new Term(F, "a")), BooleanClause.Occur.SHOULD);
    booleanQuery.add(new TermQuery(new Term(F, "b")), BooleanClause.Occur.SHOULD);

    FieldFragList ffl = sflb.createFieldFragList( fpl(booleanQuery.build(), "c d e" ), 20 );
    assertEquals( 0, ffl.getFragInfos().size() );

    ffl = sflb.createFieldFragList( fpl(booleanQuery.build(), "d b c" ), 20 );
    assertEquals( 1, ffl.getFragInfos().size() );
    assertEquals( "subInfos=(b((2,3)))/1.0(0,20)", ffl.getFragInfos().get( 0 ).toString() );

    ffl = sflb.createFieldFragList( fpl(booleanQuery.build(), "a b c" ), 20 );
    assertEquals( 1, ffl.getFragInfos().size() );
    assertEquals( "subInfos=(a((0,1))b((2,3)))/2.0(0,20)", ffl.getFragInfos().get( 0 ).toString() );
  }
  
  public void testPhraseQuery() throws Exception {
    SimpleFragListBuilder sflb = new SimpleFragListBuilder();

    PhraseQuery phraseQuery = new PhraseQuery(F, "a", "b");

    FieldFragList ffl = sflb.createFieldFragList( fpl(phraseQuery, "c d e" ), 20 );
    assertEquals( 0, ffl.getFragInfos().size() );

    ffl = sflb.createFieldFragList( fpl(phraseQuery, "a c b" ), 20 );
    assertEquals( 0, ffl.getFragInfos().size() );

    ffl = sflb.createFieldFragList( fpl(phraseQuery, "a b c" ), 20 );
    assertEquals( 1, ffl.getFragInfos().size() );
    assertEquals( "subInfos=(ab((0,3)))/1.0(0,20)", ffl.getFragInfos().get( 0 ).toString() );
  }
  
  public void testPhraseQuerySlop() throws Exception {
    SimpleFragListBuilder sflb = new SimpleFragListBuilder();

    PhraseQuery phraseQuery = new PhraseQuery(1, F, "a", "b");

    FieldFragList ffl = sflb.createFieldFragList( fpl(phraseQuery, "a c b" ), 20 );
    assertEquals( 1, ffl.getFragInfos().size() );
    assertEquals( "subInfos=(ab((0,1)(4,5)))/1.0(0,20)", ffl.getFragInfos().get( 0 ).toString() );
  }

  private FieldPhraseList fpl(Query query, String indexValue ) throws Exception {
    make1d1fIndex( indexValue );
    FieldQuery fq = new FieldQuery( query, true, true );
    FieldTermStack stack = new FieldTermStack( reader, 0, F, fq );
    return new FieldPhraseList( stack, fq );
  }
  
  public void test1PhraseShortMV() throws Exception {
    makeIndexShortMV();

    FieldQuery fq = new FieldQuery( tq( "d" ), true, true );
    FieldTermStack stack = new FieldTermStack( reader, 0, F, fq );
    FieldPhraseList fpl = new FieldPhraseList( stack, fq );
    SimpleFragListBuilder sflb = new SimpleFragListBuilder();
    FieldFragList ffl = sflb.createFieldFragList( fpl, 100 );
    assertEquals( 1, ffl.getFragInfos().size() );
    assertEquals( "subInfos=(d((9,10)))/1.0(0,100)", ffl.getFragInfos().get( 0 ).toString() );
  }
  
  public void test1PhraseLongMV() throws Exception {
    makeIndexLongMV();

    FieldQuery fq = new FieldQuery( pqF( "search", "engines" ), true, true );
    FieldTermStack stack = new FieldTermStack( reader, 0, F, fq );
    FieldPhraseList fpl = new FieldPhraseList( stack, fq );
    SimpleFragListBuilder sflb = new SimpleFragListBuilder();
    FieldFragList ffl = sflb.createFieldFragList( fpl, 100 );
    assertEquals( 1, ffl.getFragInfos().size() );
    assertEquals( "subInfos=(searchengines((102,116))searchengines((157,171)))/2.0(87,187)", ffl.getFragInfos().get( 0 ).toString() );
  }

  public void test1PhraseLongMVB() throws Exception {
    makeIndexLongMVB();

    FieldQuery fq = new FieldQuery( pqF( "sp", "pe", "ee", "ed" ), true, true ); // "speed" -(2gram)-> "sp","pe","ee","ed"
    FieldTermStack stack = new FieldTermStack( reader, 0, F, fq );
    FieldPhraseList fpl = new FieldPhraseList( stack, fq );
    SimpleFragListBuilder sflb = new SimpleFragListBuilder();
    FieldFragList ffl = sflb.createFieldFragList( fpl, 100 );
    assertEquals( 1, ffl.getFragInfos().size() );
    assertEquals( "subInfos=(sppeeeed((88,93)))/1.0(41,141)", ffl.getFragInfos().get( 0 ).toString() );
  }
}
