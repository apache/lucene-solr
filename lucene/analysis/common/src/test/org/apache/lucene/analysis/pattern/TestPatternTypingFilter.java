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
package org.apache.lucene.analysis.pattern;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.pattern.PatternTypingFilter.PatternTypingRule;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * Test that this filter sets a type for tokens matching patterns defined in a patterns.txt file
 */
public class TestPatternTypingFilter extends BaseTokenStreamTestCase {

  /**
   * Test the straight forward cases. When all flags match the token should be dropped
   */
  public void testPatterns() throws Exception {

    Token tokenA1 = new Token("One", 0, 2);
    Token tokenA2 = new Token("401(k)", 4, 9);
    Token tokenA3 = new Token("two", 11, 13);
    Token tokenB1 = new Token("three", 15, 19);
    Token tokenB2 = new Token("401k", 21, 24);

    TokenStream ts = new CannedTokenStream(tokenA1, tokenA2, tokenA3, tokenB1, tokenB2);

    //2 ^(\d+)\(?([a-z])\)?$ ::: legal2_$1_$2
    ts = new PatternTypingFilter(ts,
        new PatternTypingRule(Pattern.compile("^(\\d+)\\(?([a-z])\\)?$"),2,"legal2_$1_$2"));

    assertTokenStreamContents(ts, new String[]{
            "One", "401(k)", "two", "three", "401k"}, null, null,
        new String[]{"word", "legal2_401_k", "word", "word", "legal2_401_k"},
        null, null, null, null, null, false, null,
        new int[]{0, 2, 0, 0, 2});
  }

  public void testFirstPatternWins() throws IOException {
    Token tokenA1 = new Token("One", 0, 2);
    Token tokenA3 = new Token("forty-two", 11, 13);
    Token tokenB1 = new Token("4-2", 15, 19);

    TokenStream ts = new CannedTokenStream(tokenA1, tokenA3, tokenB1);

    //2 ^(\d+)\(?([a-z])\)?$ ::: legal2_$1_$2
    PatternTypingRule p1 = new PatternTypingRule(Pattern.compile("^(\\d+)-(\\d+)$"), 6, "$1_hnum_$2");
    PatternTypingRule p2 = new PatternTypingRule(Pattern.compile("^(\\w+)-(\\w+)$"), 2, "$1_hword_$2");

    ts = new PatternTypingFilter(ts, p1,p2); // 101

    assertTokenStreamContents(ts, new String[]{
            "One", "forty-two", "4-2"}, null, null,
        new String[]{"word", "forty_hword_two", "4_hnum_2"},
        null, null, null, null, null, false, null,
        new int[]{0, 2, 6});
  }

}
