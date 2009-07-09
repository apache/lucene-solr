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

package org.apache.solr.analysis;

import java.io.StringReader;
import java.util.List;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;

public class TestMappingCharFilter extends BaseTokenTestCase {

  NormalizeMap normMap;

  public void setUp() throws Exception {
    normMap = new NormalizeMap();

    normMap.add( "aa", "a" );
    normMap.add( "bbb", "b" );
    normMap.add( "cccc", "cc" );

    normMap.add( "h", "i" );
    normMap.add( "j", "jj" );
    normMap.add( "k", "kkk" );
    normMap.add( "ll", "llll" );

    normMap.add( "empty", "" );
  }

  public void testReaderReset() throws Exception {
    CharStream cs = new MappingCharFilter( normMap, CharReader.get( new StringReader( "x" ) ) );
    char[] buf = new char[10];
    int len = cs.read(buf, 0, 10);
    assertEquals( 1, len );
    assertEquals( 'x', buf[0]) ;
    len = cs.read(buf, 0, 10);
    assertEquals( -1, len );

    // rewind
    cs.reset();
    len = cs.read(buf, 0, 10);
    assertEquals( 1, len );
    assertEquals( 'x', buf[0]) ;
  }

  public void testNothingChange() throws Exception {
    CharStream cs = new MappingCharFilter( normMap, CharReader.get( new StringReader( "x" ) ) );
    TokenStream ts = new CharStreamAwareWhitespaceTokenizer( cs );
    List<Token> real = getTokens( ts );
    List<Token> expect = tokens( "x" );
    assertTokEqualOff( expect, real );
  }

  public void test1to1() throws Exception {
    CharStream cs = new MappingCharFilter( normMap, CharReader.get( new StringReader( "h" ) ) );
    TokenStream ts = new CharStreamAwareWhitespaceTokenizer( cs );
    List<Token> real = getTokens( ts );
    List<Token> expect = tokens( "i" );
    assertTokEqualOff( expect, real );
  }

  public void test1to2() throws Exception {
    CharStream cs = new MappingCharFilter( normMap, CharReader.get( new StringReader( "j" ) ) );
    TokenStream ts = new CharStreamAwareWhitespaceTokenizer( cs );
    List<Token> real = getTokens( ts );
    List<Token> expect = tokens( "jj,1,0,1" );
    assertTokEqualOff( expect, real );
  }

  public void test1to3() throws Exception {
    CharStream cs = new MappingCharFilter( normMap, CharReader.get( new StringReader( "k" ) ) );
    TokenStream ts = new CharStreamAwareWhitespaceTokenizer( cs );
    List<Token> real = getTokens( ts );
    List<Token> expect = tokens( "kkk,1,0,1" );
    assertTokEqualOff( expect, real );
  }

  public void test2to4() throws Exception {
    CharStream cs = new MappingCharFilter( normMap, CharReader.get( new StringReader( "ll" ) ) );
    TokenStream ts = new CharStreamAwareWhitespaceTokenizer( cs );
    List<Token> real = getTokens( ts );
    List<Token> expect = tokens( "llll,1,0,2" );
    assertTokEqualOff( expect, real );
  }

  public void test2to1() throws Exception {
    CharStream cs = new MappingCharFilter( normMap, CharReader.get( new StringReader( "aa" ) ) );
    TokenStream ts = new CharStreamAwareWhitespaceTokenizer( cs );
    List<Token> real = getTokens( ts );
    List<Token> expect = tokens( "a,1,0,2" );
    assertTokEqualOff( expect, real );
  }

  public void test3to1() throws Exception {
    CharStream cs = new MappingCharFilter( normMap, CharReader.get( new StringReader( "bbb" ) ) );
    TokenStream ts = new CharStreamAwareWhitespaceTokenizer( cs );
    List<Token> real = getTokens( ts );
    List<Token> expect = tokens( "b,1,0,3" );
    assertTokEqualOff( expect, real );
  }

  public void test4to2() throws Exception {
    CharStream cs = new MappingCharFilter( normMap, CharReader.get( new StringReader( "cccc" ) ) );
    TokenStream ts = new CharStreamAwareWhitespaceTokenizer( cs );
    List<Token> real = getTokens( ts );
    List<Token> expect = tokens( "cc,1,0,4" );
    assertTokEqualOff( expect, real );
  }

  public void test5to0() throws Exception {
    CharStream cs = new MappingCharFilter( normMap, CharReader.get( new StringReader( "empty" ) ) );
    TokenStream ts = new CharStreamAwareWhitespaceTokenizer( cs );
    List<Token> real = getTokens( ts );
    assertEquals( 0, real.size() );
  }

  //
  //                1111111111222
  //      01234567890123456789012
  //(in)  h i j k ll cccc bbb aa
  //
  //                1111111111222
  //      01234567890123456789012
  //(out) i i jj kkk llll cc b a
  //
  //    h, 0, 1 =>    i, 0, 1
  //    i, 2, 3 =>    i, 2, 3
  //    j, 4, 5 =>   jj, 4, 5
  //    k, 6, 7 =>  kkk, 6, 7
  //   ll, 8,10 => llll, 8,10
  // cccc,11,15 =>   cc,11,15
  //  bbb,16,19 =>    b,16,19
  //   aa,20,22 =>    a,20,22
  //
  public void testTokenStream() throws Exception {
    CharStream cs = new MappingCharFilter( normMap, CharReader.get( new StringReader( "h i j k ll cccc bbb aa" ) ) );
    TokenStream ts = new CharStreamAwareWhitespaceTokenizer( cs );
    List<Token> real = getTokens( ts );
    List<Token> expect = tokens( "i,1,0,1 i,1,2,3 jj,1,4,5 kkk,1,6,7 llll,1,8,10 cc,1,11,15 b,1,16,19 a,1,20,22" );
    assertTokEqualOff( expect, real );
  }

  //
  //
  //        0123456789
  //(in)    aaaa ll h
  //(out-1) aa llll i
  //(out-2) a llllllll i
  //
  // aaaa,0,4 => a,0,4
  //   ll,5,7 => llllllll,5,7
  //    h,8,9 => i,8,9
  public void testChained() throws Exception {
    CharStream cs = new MappingCharFilter( normMap,
        new MappingCharFilter( normMap, CharReader.get( new StringReader( "aaaa ll h" ) ) ) );
    TokenStream ts = new CharStreamAwareWhitespaceTokenizer( cs );
    List<Token> real = getTokens( ts );
    List<Token> expect = tokens( "a,1,0,4 llllllll,1,5,7 i,1,8,9" );
    assertTokEqualOff( expect, real );
  }
}
