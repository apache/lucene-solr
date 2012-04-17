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

import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BooleanClause.Occur;

public class FieldPhraseListTest extends AbstractTestCase {
  
  public void test1TermIndex() throws Exception {
    make1d1fIndex( "a" );

    FieldQuery fq = new FieldQuery( tq( "a" ), true, true );
    FieldTermStack stack = new FieldTermStack( reader, 0, F, fq );
    FieldPhraseList fpl = new FieldPhraseList( stack, fq );
    assertEquals( 1, fpl.phraseList.size() );
    assertEquals( "a(1.0)((0,1))", fpl.phraseList.get( 0 ).toString() );

    fq = new FieldQuery( tq( "b" ), true, true );
    stack = new FieldTermStack( reader, 0, F, fq );
    fpl = new FieldPhraseList( stack, fq );
    assertEquals( 0, fpl.phraseList.size() );
  }
  
  public void test2TermsIndex() throws Exception {
    make1d1fIndex( "a a" );

    FieldQuery fq = new FieldQuery( tq( "a" ), true, true );
    FieldTermStack stack = new FieldTermStack( reader, 0, F, fq );
    FieldPhraseList fpl = new FieldPhraseList( stack, fq );
    assertEquals( 2, fpl.phraseList.size() );
    assertEquals( "a(1.0)((0,1))", fpl.phraseList.get( 0 ).toString() );
    assertEquals( "a(1.0)((2,3))", fpl.phraseList.get( 1 ).toString() );
  }
  
  public void test1PhraseIndex() throws Exception {
    make1d1fIndex( "a b" );

    FieldQuery fq = new FieldQuery( pqF( "a", "b" ), true, true );
    FieldTermStack stack = new FieldTermStack( reader, 0, F, fq );
    FieldPhraseList fpl = new FieldPhraseList( stack, fq );
    assertEquals( 1, fpl.phraseList.size() );
    assertEquals( "ab(1.0)((0,3))", fpl.phraseList.get( 0 ).toString() );

    fq = new FieldQuery( tq( "b" ), true, true );
    stack = new FieldTermStack( reader, 0, F, fq );
    fpl = new FieldPhraseList( stack, fq );
    assertEquals( 1, fpl.phraseList.size() );
    assertEquals( "b(1.0)((2,3))", fpl.phraseList.get( 0 ).toString() );
  }
  
  public void test1PhraseIndexB() throws Exception {
    // 01 12 23 34 45 56 67 78 (offsets)
    // bb|bb|ba|ac|cb|ba|ab|bc
    //  0  1  2  3  4  5  6  7 (positions)
    make1d1fIndexB( "bbbacbabc" );

    FieldQuery fq = new FieldQuery( pqF( "ba", "ac" ), true, true );
    FieldTermStack stack = new FieldTermStack( reader, 0, F, fq );
    FieldPhraseList fpl = new FieldPhraseList( stack, fq );
    assertEquals( 1, fpl.phraseList.size() );
    assertEquals( "baac(1.0)((2,5))", fpl.phraseList.get( 0 ).toString() );
  }
  
  public void test2ConcatTermsIndexB() throws Exception {
    // 01 12 23 (offsets)
    // ab|ba|ab
    //  0  1  2 (positions)
    make1d1fIndexB( "abab" );

    FieldQuery fq = new FieldQuery( tq( "ab" ), true, true );
    FieldTermStack stack = new FieldTermStack( reader, 0, F, fq );
    FieldPhraseList fpl = new FieldPhraseList( stack, fq );
    assertEquals( 2, fpl.phraseList.size() );
    assertEquals( "ab(1.0)((0,2))", fpl.phraseList.get( 0 ).toString() );
    assertEquals( "ab(1.0)((2,4))", fpl.phraseList.get( 1 ).toString() );
  }
  
  public void test2Terms1PhraseIndex() throws Exception {
    make1d1fIndex( "c a a b" );

    // phraseHighlight = true
    FieldQuery fq = new FieldQuery( pqF( "a", "b" ), true, true );
    FieldTermStack stack = new FieldTermStack( reader, 0, F, fq );
    FieldPhraseList fpl = new FieldPhraseList( stack, fq );
    assertEquals( 1, fpl.phraseList.size() );
    assertEquals( "ab(1.0)((4,7))", fpl.phraseList.get( 0 ).toString() );

    // phraseHighlight = false
    fq = new FieldQuery( pqF( "a", "b" ), false, true );
    stack = new FieldTermStack( reader, 0, F, fq );
    fpl = new FieldPhraseList( stack, fq );
    assertEquals( 2, fpl.phraseList.size() );
    assertEquals( "a(1.0)((2,3))", fpl.phraseList.get( 0 ).toString() );
    assertEquals( "ab(1.0)((4,7))", fpl.phraseList.get( 1 ).toString() );
  }
  
  public void testPhraseSlop() throws Exception {
    make1d1fIndex( "c a a b c" );

    FieldQuery fq = new FieldQuery( pqF( 2F, 1, "a", "c" ), true, true );
    FieldTermStack stack = new FieldTermStack( reader, 0, F, fq );
    FieldPhraseList fpl = new FieldPhraseList( stack, fq );
    assertEquals( 1, fpl.phraseList.size() );
    assertEquals( "ac(2.0)((4,5)(8,9))", fpl.phraseList.get( 0 ).toString() );
    assertEquals( 4, fpl.phraseList.get( 0 ).getStartOffset() );
    assertEquals( 9, fpl.phraseList.get( 0 ).getEndOffset() );
  }
  
  public void test2PhrasesOverlap() throws Exception {
    make1d1fIndex( "d a b c d" );

    BooleanQuery query = new BooleanQuery();
    query.add( pqF( "a", "b" ), Occur.SHOULD );
    query.add( pqF( "b", "c" ), Occur.SHOULD );
    FieldQuery fq = new FieldQuery( query, true, true );
    FieldTermStack stack = new FieldTermStack( reader, 0, F, fq );
    FieldPhraseList fpl = new FieldPhraseList( stack, fq );
    assertEquals( 1, fpl.phraseList.size() );
    assertEquals( "abc(1.0)((2,7))", fpl.phraseList.get( 0 ).toString() );
  }
  
  public void test3TermsPhrase() throws Exception {
    make1d1fIndex( "d a b a b c d" );

    FieldQuery fq = new FieldQuery( pqF( "a", "b", "c" ), true, true );
    FieldTermStack stack = new FieldTermStack( reader, 0, F, fq );
    FieldPhraseList fpl = new FieldPhraseList( stack, fq );
    assertEquals( 1, fpl.phraseList.size() );
    assertEquals( "abc(1.0)((6,11))", fpl.phraseList.get( 0 ).toString() );
  }
  
  public void testSearchLongestPhrase() throws Exception {
    make1d1fIndex( "d a b d c a b c" );

    BooleanQuery query = new BooleanQuery();
    query.add( pqF( "a", "b" ), Occur.SHOULD );
    query.add( pqF( "a", "b", "c" ), Occur.SHOULD );
    FieldQuery fq = new FieldQuery( query, true, true );
    FieldTermStack stack = new FieldTermStack( reader, 0, F, fq );
    FieldPhraseList fpl = new FieldPhraseList( stack, fq );
    assertEquals( 2, fpl.phraseList.size() );
    assertEquals( "ab(1.0)((2,5))", fpl.phraseList.get( 0 ).toString() );
    assertEquals( "abc(1.0)((10,15))", fpl.phraseList.get( 1 ).toString() );
  }
  
  public void test1PhraseShortMV() throws Exception {
    makeIndexShortMV();

    FieldQuery fq = new FieldQuery( tq( "d" ), true, true );
    FieldTermStack stack = new FieldTermStack( reader, 0, F, fq );
    FieldPhraseList fpl = new FieldPhraseList( stack, fq );
    assertEquals( 1, fpl.phraseList.size() );
    assertEquals( "d(1.0)((9,10))", fpl.phraseList.get( 0 ).toString() );
  }
  
  public void test1PhraseLongMV() throws Exception {
    makeIndexLongMV();

    FieldQuery fq = new FieldQuery( pqF( "search", "engines" ), true, true );
    FieldTermStack stack = new FieldTermStack( reader, 0, F, fq );
    FieldPhraseList fpl = new FieldPhraseList( stack, fq );
    assertEquals( 2, fpl.phraseList.size() );
    assertEquals( "searchengines(1.0)((102,116))", fpl.phraseList.get( 0 ).toString() );
    assertEquals( "searchengines(1.0)((157,171))", fpl.phraseList.get( 1 ).toString() );
  }

  public void test1PhraseLongMVB() throws Exception {
    makeIndexLongMVB();

    FieldQuery fq = new FieldQuery( pqF( "sp", "pe", "ee", "ed" ), true, true ); // "speed" -(2gram)-> "sp","pe","ee","ed"
    FieldTermStack stack = new FieldTermStack( reader, 0, F, fq );
    FieldPhraseList fpl = new FieldPhraseList( stack, fq );
    assertEquals( 1, fpl.phraseList.size() );
    assertEquals( "sppeeeed(1.0)((88,93))", fpl.phraseList.get( 0 ).toString() );
  }
  
  /* This test shows a big speedup from limiting the number of analyzed phrases in 
   * this bad case for FieldPhraseList */
  /* But it is not reliable as a unit test since it is timing-dependent
  public void testManyRepeatedTerms() throws Exception {
      long t = System.currentTimeMillis();
      testManyTermsWithLimit (-1);
      long t1 = System.currentTimeMillis();
      testManyTermsWithLimit (1);
      long t2 = System.currentTimeMillis();
      assertTrue (t2-t1 * 1000 < t1-t);
  }
  private void testManyTermsWithLimit (int limit) throws Exception {
      StringBuilder buf = new StringBuilder ();
      for (int i = 0; i < 16000; i++) {
          buf.append("a b c ");
      }
      make1d1fIndex( buf.toString());

      Query query = tq("a");
      FieldQuery fq = new FieldQuery( query, true, true );
      FieldTermStack stack = new FieldTermStack( reader, 0, F, fq );
      FieldPhraseList fpl = new FieldPhraseList( stack, fq, limit);
      if (limit < 0 || limit > 16000)
          assertEquals( 16000, fpl.phraseList.size() );
      else
          assertEquals( limit, fpl.phraseList.size() );
      assertEquals( "a(1.0)((0,1))", fpl.phraseList.get( 0 ).toString() );      
  }
  */
}
