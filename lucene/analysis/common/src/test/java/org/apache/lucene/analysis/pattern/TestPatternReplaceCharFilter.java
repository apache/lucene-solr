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

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CharFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.util.TestUtil;
import org.junit.Ignore;

/**
 * Tests {@link PatternReplaceCharFilter}
 */
public class TestPatternReplaceCharFilter extends BaseTokenStreamTestCase {
  public void testFailingDot() throws IOException {
    checkOutput(
        "A. .B.", "\\.[\\s]*", ".",
        "A..B.",
        "A..B.");
  }

  public void testLongerReplacement() throws IOException {
    checkOutput(
        "XXabcZZabcYY", "abc", "abcde",
        "XXabcdeZZabcdeYY",
        "XXabcccZZabcccYY");
    checkOutput(
        "XXabcabcYY", "abc", "abcde",
        "XXabcdeabcdeYY",
        "XXabcccabcccYY");
    checkOutput(
        "abcabcYY", "abc", "abcde",
        "abcdeabcdeYY",
        "abcccabcccYY");
    checkOutput(
        "YY", "^", "abcde",
        "abcdeYY",
        // Should be: "-----YY" but we're enforcing non-negative offsets.
        "YYYYYYY");
    checkOutput(
        "YY", "$", "abcde",
        "YYabcde",
        "YYYYYYY");
    checkOutput(
        "XYZ", ".", "abc",
        "abcabcabc",
        "XXXYYYZZZ");
    checkOutput(
        "XYZ", ".", "$0abc",
        "XabcYabcZabc",
        "XXXXYYYYZZZZ");
  }

  public void testShorterReplacement() throws IOException {
    checkOutput(
        "XXabcZZabcYY", "abc", "xy",
        "XXxyZZxyYY",
        "XXabZZabYY");
    checkOutput(
        "XXabcabcYY", "abc", "xy",
        "XXxyxyYY",
        "XXababYY");
    checkOutput(
        "abcabcYY", "abc", "xy",
        "xyxyYY",
        "ababYY");
    checkOutput(
        "abcabcYY", "abc", "",
        "YY",
        "YY");
    checkOutput(
        "YYabcabc", "abc", "",
        "YY",
        "YY");
  }

  private void checkOutput(String input, String pattern, String replacement,
      String expectedOutput, String expectedIndexMatchedOutput) throws IOException {
      CharFilter cs = new PatternReplaceCharFilter(pattern(pattern), replacement,
        new StringReader(input));

    StringBuilder output = new StringBuilder();
    for (int chr = cs.read(); chr > 0; chr = cs.read()) {
      output.append((char) chr);
    }

    StringBuilder indexMatched = new StringBuilder();
    for (int i = 0; i < output.length(); i++) {
      indexMatched.append((cs.correctOffset(i) < 0 ? "-" : input.charAt(cs.correctOffset(i))));
    }

    boolean outputGood = expectedOutput.equals(output.toString());
    boolean indexMatchedGood = expectedIndexMatchedOutput.equals(indexMatched.toString());

    if (!outputGood || !indexMatchedGood || false) {
      System.out.println("Pattern : " + pattern);
      System.out.println("Replac. : " + replacement);
      System.out.println("Input   : " + input);
      System.out.println("Output  : " + output);
      System.out.println("Expected: " + expectedOutput);
      System.out.println("Output/i: " + indexMatched);
      System.out.println("Expected: " + expectedIndexMatchedOutput);
      System.out.println();
    }

    assertTrue("Output doesn't match.", outputGood);
    assertTrue("Index-matched output doesn't match.", indexMatchedGood);
  }

  //           1111
  // 01234567890123
  // this is test.
  public void testNothingChange() throws IOException {
    final String BLOCK = "this is test.";
    CharFilter cs = new PatternReplaceCharFilter( pattern("(aa)\\s+(bb)\\s+(cc)"), "$1$2$3",
          new StringReader( BLOCK ) );
    TokenStream ts = whitespaceMockTokenizer(cs);
    assertTokenStreamContents(ts,
        new String[] { "this", "is", "test." },
        new int[] { 0, 5, 8 },
        new int[] { 4, 7, 13 }, 
        BLOCK.length());
  }
  
  // 012345678
  // aa bb cc
  public void testReplaceByEmpty() throws IOException {
    final String BLOCK = "aa bb cc";
    CharFilter cs = new PatternReplaceCharFilter( pattern("(aa)\\s+(bb)\\s+(cc)"), "",
          new StringReader( BLOCK ) );
    TokenStream ts = whitespaceMockTokenizer(cs);
    assertTokenStreamContents(ts, new String[] {});
  }
  
  // 012345678
  // aa bb cc
  // aa#bb#cc
  public void test1block1matchSameLength() throws IOException {
    final String BLOCK = "aa bb cc";
    CharFilter cs = new PatternReplaceCharFilter( pattern("(aa)\\s+(bb)\\s+(cc)"), "$1#$2#$3",
          new StringReader( BLOCK ) );
    TokenStream ts = whitespaceMockTokenizer(cs);
    assertTokenStreamContents(ts,
        new String[] { "aa#bb#cc" },
        new int[] { 0 },
        new int[] { 8 }, 
        BLOCK.length());
  }

  //           11111
  // 012345678901234
  // aa bb cc dd
  // aa##bb###cc dd
  public void test1block1matchLonger() throws IOException {
    final String BLOCK = "aa bb cc dd";
    CharFilter cs = new PatternReplaceCharFilter( pattern("(aa)\\s+(bb)\\s+(cc)"), "$1##$2###$3",
          new StringReader( BLOCK ) );
    TokenStream ts = whitespaceMockTokenizer(cs);
    assertTokenStreamContents(ts,
        new String[] { "aa##bb###cc", "dd" },
        new int[] { 0, 9 },
        new int[] { 8, 11 },
        BLOCK.length());
  }

  // 01234567
  //  a  a
  //  aa  aa
  public void test1block2matchLonger() throws IOException {
    final String BLOCK = " a  a";
    CharFilter cs = new PatternReplaceCharFilter( pattern("a"), "aa",
          new StringReader( BLOCK ) );
    TokenStream ts = whitespaceMockTokenizer(cs);
    assertTokenStreamContents(ts,
        new String[] { "aa", "aa" },
        new int[] { 1, 4 },
        new int[] { 2, 5 },
        BLOCK.length());
  }

  //           11111
  // 012345678901234
  // aa  bb   cc dd
  // aa#bb dd
  public void test1block1matchShorter() throws IOException {
    final String BLOCK = "aa  bb   cc dd";
    CharFilter cs = new PatternReplaceCharFilter( pattern("(aa)\\s+(bb)\\s+(cc)"), "$1#$2",
          new StringReader( BLOCK ) );
    TokenStream ts = whitespaceMockTokenizer(cs);
    assertTokenStreamContents(ts,
        new String[] { "aa#bb", "dd" },
        new int[] { 0, 12 },
        new int[] { 11, 14 },
        BLOCK.length());
  }

  //           111111111122222222223333
  // 0123456789012345678901234567890123
  //   aa bb cc --- aa bb aa   bb   cc
  //   aa  bb  cc --- aa bb aa  bb  cc
  public void test1blockMultiMatches() throws IOException {
    final String BLOCK = "  aa bb cc --- aa bb aa   bb   cc";
    CharFilter cs = new PatternReplaceCharFilter( pattern("(aa)\\s+(bb)\\s+(cc)"), "$1  $2  $3",
          new StringReader( BLOCK ) );
    TokenStream ts = whitespaceMockTokenizer(cs);
    assertTokenStreamContents(ts,
        new String[] { "aa", "bb", "cc", "---", "aa", "bb", "aa", "bb", "cc" },
        new int[] { 2, 6, 9, 11, 15, 18, 21, 25, 29 },
        new int[] { 4, 8, 10, 14, 17, 20, 23, 27, 33 },
        BLOCK.length());
  }

  //           11111111112222222222333333333
  // 012345678901234567890123456789012345678
  //   aa bb cc --- aa bb aa. bb aa   bb cc
  //   aa##bb cc --- aa##bb aa. bb aa##bb cc

  //   aa bb cc --- aa bbbaa. bb aa   b cc
  
  public void test2blocksMultiMatches() throws IOException {
    final String BLOCK = "  aa bb cc --- aa bb aa. bb aa   bb cc";

    CharFilter cs = new PatternReplaceCharFilter( pattern("(aa)\\s+(bb)"), "$1##$2",
          new StringReader( BLOCK ) );
    TokenStream ts = whitespaceMockTokenizer(cs);
    assertTokenStreamContents(ts,
        new String[] { "aa##bb", "cc", "---", "aa##bb", "aa.", "bb", "aa##bb", "cc" },
        new int[] { 2, 8, 11, 15, 21, 25, 28, 36 },
        new int[] { 7, 10, 14, 20, 24, 27, 35, 38 },
        BLOCK.length());
  }

  //           11111111112222222222333333333
  // 012345678901234567890123456789012345678
  //  a bb - ccc . --- bb a . ccc ccc bb
  //  aa b - c . --- b aa . c c b
  public void testChain() throws IOException {
    final String BLOCK = " a bb - ccc . --- bb a . ccc ccc bb";
    CharFilter cs = new PatternReplaceCharFilter( pattern("a"), "aa",
        new StringReader( BLOCK ) );
    cs = new PatternReplaceCharFilter( pattern("bb"), "b", cs );
    cs = new PatternReplaceCharFilter( pattern("ccc"), "c", cs );
    TokenStream ts = whitespaceMockTokenizer(cs);
    assertTokenStreamContents(ts,
        new String[] { "aa", "b", "-", "c", ".", "---", "b", "aa", ".", "c", "c", "b" },
        new int[] { 1, 3, 6, 8, 12, 14, 18, 21, 23, 25, 29, 33 },
        new int[] { 2, 5, 7, 11, 13, 17, 20, 22, 24, 28, 32, 35 },
        BLOCK.length());
  }
  
  private Pattern pattern( String p ){
    return Pattern.compile( p );
  }

  /**
   * A demonstration of how backtracking regular expressions can lead to relatively 
   * easy DoS attacks.
   * 
   * @see <a href="http://swtch.com/~rsc/regexp/regexp1.html">"http://swtch.com/~rsc/regexp/regexp1.html"</a>
   */
  @Ignore
  public void testNastyPattern() throws Exception {
    Pattern p = Pattern.compile("(c.+)*xy");
    String input = "[;<!--aecbbaa--><    febcfdc fbb = \"fbeeebff\" fc = dd   >\\';<eefceceaa e= babae\" eacbaff =\"fcfaccacd\" = bcced>>><  bccaafe edb = ecfccdff\"   <?</script><    edbd ebbcd=\"faacfcc\" aeca= bedbc ceeaac =adeafde aadccdaf = \"afcc ffda=aafbe &#x16921ed5\"1843785582']";
    for (int i = 0; i < input.length(); i++) {
      Matcher matcher = p.matcher(input.substring(0, i));
      long t = System.currentTimeMillis();
      if (matcher.find()) {
        System.out.println(matcher.group());
      }
      System.out.println(i + " > " + (System.currentTimeMillis() - t) / 1000.0);
    }
  }

  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    int numPatterns = 10 + random().nextInt(20);
    Random random = new Random(random().nextLong());
    for (int i = 0; i < numPatterns; i++) {
      final Pattern p = TestUtil.randomPattern(random());

      final String replacement = TestUtil.randomSimpleString(random);
      Analyzer a = new Analyzer() {
        @Override
        protected TokenStreamComponents createComponents(String fieldName) {
          Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
          return new TokenStreamComponents(tokenizer, tokenizer);
        }

        @Override
        protected Reader initReader(String fieldName, Reader reader) {
          return new PatternReplaceCharFilter(p, replacement, reader);
        }
      };

      /* max input length. don't make it longer -- exponential processing
       * time for certain patterns. */ 
      final int maxInputLength = 30;
      /* ASCII only input?: */
      final boolean asciiOnly = true;
      checkRandomData(random, a, 250 * RANDOM_MULTIPLIER, maxInputLength, asciiOnly);
      a.close();
    }
  }
 }
