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

import java.io.IOException;
import java.io.StringReader;

import org.apache.lucene.analysis.CharReader;
import org.apache.lucene.analysis.CharStream;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WhitespaceTokenizer;

/**
 * 
 * @version $Id$
 *
 */
public class TestPatternReplaceCharFilter extends BaseTokenTestCase {
  
  //           1111
  // 01234567890123
  // this is test.
  public void testNothingChange() throws IOException {
    final String BLOCK = "this is test.";
    CharStream cs = new PatternReplaceCharFilter( "(aa)\\s+(bb)\\s+(cc)", "$1$2$3",
          CharReader.get( new StringReader( BLOCK ) ) );
    TokenStream ts = new WhitespaceTokenizer( cs );
    assertTokEqualOff( tokens( "this,1,0,4 is,1,5,7 test.,1,8,13" ), getTokens( ts ) );
  }
  
  // 012345678
  // aa bb cc
  public void testReplaceByEmpty() throws IOException {
    final String BLOCK = "aa bb cc";
    CharStream cs = new PatternReplaceCharFilter( "(aa)\\s+(bb)\\s+(cc)", "",
          CharReader.get( new StringReader( BLOCK ) ) );
    TokenStream ts = new WhitespaceTokenizer( cs );
    assertEquals( 0, getTokens( ts ).size() );
  }
  
  // 012345678
  // aa bb cc
  // aa#bb#cc
  public void test1block1matchSameLength() throws IOException {
    final String BLOCK = "aa bb cc";
    CharStream cs = new PatternReplaceCharFilter( "(aa)\\s+(bb)\\s+(cc)", "$1#$2#$3",
          CharReader.get( new StringReader( BLOCK ) ) );
    TokenStream ts = new WhitespaceTokenizer( cs );
    assertTokEqualOff( tokens( "aa#bb#cc,1,0,8" ), getTokens( ts ) );
  }

  //           11111
  // 012345678901234
  // aa bb cc dd
  // aa##bb###cc dd
  public void test1block1matchLonger() throws IOException {
    final String BLOCK = "aa bb cc dd";
    CharStream cs = new PatternReplaceCharFilter( "(aa)\\s+(bb)\\s+(cc)", "$1##$2###$3",
          CharReader.get( new StringReader( BLOCK ) ) );
    TokenStream ts = new WhitespaceTokenizer( cs );
    assertTokEqualOff( tokens( "aa##bb###cc,1,0,8 dd,1,9,11" ), getTokens( ts ) );
  }

  // 01234567
  //  a  a
  //  aa  aa
  public void test1block2matchLonger() throws IOException {
    final String BLOCK = " a  a";
    CharStream cs = new PatternReplaceCharFilter( "a", "aa",
          CharReader.get( new StringReader( BLOCK ) ) );
    TokenStream ts = new WhitespaceTokenizer( cs );
    assertTokEqualOff( tokens( "aa,1,1,2 aa,1,4,5" ), getTokens( ts ) );
  }

  //           11111
  // 012345678901234
  // aa  bb   cc dd
  // aa#bb dd
  public void test1block1matchShorter() throws IOException {
    final String BLOCK = "aa  bb   cc dd";
    CharStream cs = new PatternReplaceCharFilter( "(aa)\\s+(bb)\\s+(cc)", "$1#$2",
          CharReader.get( new StringReader( BLOCK ) ) );
    TokenStream ts = new WhitespaceTokenizer( cs );
    assertTokEqualOff( tokens( "aa#bb,1,0,11 dd,1,12,14" ), getTokens( ts ) );
  }

  //           111111111122222222223333
  // 0123456789012345678901234567890123
  //   aa bb cc --- aa bb aa   bb   cc
  //   aa  bb  cc --- aa bb aa  bb  cc
  public void test1blockMultiMatches() throws IOException {
    final String BLOCK = "  aa bb cc --- aa bb aa   bb   cc";
    CharStream cs = new PatternReplaceCharFilter( "(aa)\\s+(bb)\\s+(cc)", "$1  $2  $3",
          CharReader.get( new StringReader( BLOCK ) ) );
    TokenStream ts = new WhitespaceTokenizer( cs );
    assertTokEqualOff( tokens( "aa,1,2,4 bb,1,6,8 cc,1,9,10 ---,1,11,14 aa,1,15,17 bb,1,18,20 aa,1,21,23 bb,1,25,27 cc,1,29,33" ),
        getTokens( ts ) );
  }

  //           11111111112222222222333333333
  // 012345678901234567890123456789012345678
  //   aa bb cc --- aa bb aa. bb aa   bb cc
  //   aa##bb cc --- aa##bb aa. bb aa##bb cc
  public void test2blocksMultiMatches() throws IOException {
    final String BLOCK = "  aa bb cc --- aa bb aa. bb aa   bb cc";
    CharStream cs = new PatternReplaceCharFilter( "(aa)\\s+(bb)", "$1##$2", ".",
          CharReader.get( new StringReader( BLOCK ) ) );
    TokenStream ts = new WhitespaceTokenizer( cs );
    assertTokEqualOff( tokens( "aa##bb,1,2,7 cc,1,8,10 ---,1,11,14 aa##bb,1,15,20 aa.,1,21,24 bb,1,25,27 aa##bb,1,28,35 cc,1,36,38" ),
        getTokens( ts ) );
  }

  //           11111111112222222222333333333
  // 012345678901234567890123456789012345678
  //  a bb - ccc . --- bb a . ccc ccc bb
  //  aa b - c . --- b aa . c c b
  public void testChain() throws IOException {
    final String BLOCK = " a bb - ccc . --- bb a . ccc ccc bb";
    CharStream cs = new PatternReplaceCharFilter( "a", "aa", ".",
        CharReader.get( new StringReader( BLOCK ) ) );
    cs = new PatternReplaceCharFilter( "bb", "b", ".", cs );
    cs = new PatternReplaceCharFilter( "ccc", "c", ".", cs );
    TokenStream ts = new WhitespaceTokenizer( cs );
    assertTokEqualOff( tokens( "aa,1,1,2 b,1,3,5 -,1,6,7 c,1,8,11 .,1,12,13 ---,1,14,17 b,1,18,20 aa,1,21,22 .,1,23,24 c,1,25,28 c,1,29,32 b,1,33,35" ),
        getTokens( ts ) );
  }
}
