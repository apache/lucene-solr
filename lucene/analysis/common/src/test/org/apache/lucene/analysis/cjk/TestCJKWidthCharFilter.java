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

package org.apache.lucene.analysis.cjk;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CharFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

public class TestCJKWidthCharFilter extends BaseTokenStreamTestCase {
  /**
   * Full-width ASCII forms normalized to half-width (basic latin)
   */
  public void testFullWidthASCII() throws IOException {
    CharFilter reader = new CJKWidthCharFilter(new StringReader("Ｔｅｓｔ １２３４"));
    TokenStream ts = whitespaceMockTokenizer(reader);
    assertTokenStreamContents(ts, new String[]{"Test", "1234"}, new int[]{0, 5}, new int[]{4, 9}, 9);
  }

  /**
   * Half-width katakana forms normalized to standard katakana.
   * A bit trickier in some cases, since half-width forms are decomposed
   * and voice marks need to be recombined with a preceding base form.
   */
  public void testHalfWidthKana() throws IOException {
    CharFilter reader = new CJKWidthCharFilter(new StringReader("ｶﾀｶﾅ"));
    TokenStream ts = whitespaceMockTokenizer(reader);
    assertTokenStreamContents(ts, new String[]{"カタカナ"}, new int[]{0}, new int[]{4}, 4);

    reader = new CJKWidthCharFilter(new StringReader("ｳﾞｨｯﾂ"));
    ts = whitespaceMockTokenizer(reader);
    assertTokenStreamContents(ts, new String[]{"ヴィッツ"}, new int[]{0}, new int[]{5}, 5);

    reader = new CJKWidthCharFilter(new StringReader("ﾊﾟﾅｿﾆｯｸ"));
    ts = whitespaceMockTokenizer(reader);
    assertTokenStreamContents(ts, new String[]{"パナソニック"}, new int[]{0}, new int[]{7}, 7);

    reader = new CJKWidthCharFilter(new StringReader("ｳﾞｨｯﾂ ﾊﾟﾅｿﾆｯｸ"));
    ts = whitespaceMockTokenizer(reader);
    assertTokenStreamContents(ts, new String[]{"ヴィッツ", "パナソニック"}, new int[]{0, 6}, new int[]{5, 13}, 13);
  }

  /**
   * Input may contain orphan voiced marks that cannot be combined with the previous character.
   */
  public void testOrphanVoiceMark() throws Exception {
    CharFilter reader = new CJKWidthCharFilter(new StringReader("ｱﾞｨｯﾂ"));
    TokenStream ts = whitespaceMockTokenizer(reader);
    assertTokenStreamContents(ts, new String[]{"ア\u3099ィッツ"}, new int[]{0}, new int[]{5}, 5);

    reader = new CJKWidthCharFilter(new StringReader("ﾞｨｯﾂ"));
    ts = whitespaceMockTokenizer(reader);
    assertTokenStreamContents(ts, new String[]{"\u3099ィッツ"}, new int[]{0}, new int[]{4}, 4);

    reader = new CJKWidthCharFilter(new StringReader("ｱﾟﾅｿﾆｯｸ"));
    ts = whitespaceMockTokenizer(reader);
    assertTokenStreamContents(ts, new String[]{"ア\u309Aナソニック"}, new int[]{0}, new int[]{7}, 7);

    reader = new CJKWidthCharFilter(new StringReader("ﾟﾅｿﾆｯｸ"));
    ts = whitespaceMockTokenizer(reader);
    assertTokenStreamContents(ts, new String[]{"\u309Aナソニック"}, new int[]{0}, new int[]{6}, 6);
  }

  public void testComplexInput() throws Exception {
    CharFilter reader = new CJKWidthCharFilter(new StringReader("Ｔｅst １２34"));
    TokenStream ts = whitespaceMockTokenizer(reader);
    assertTokenStreamContents(ts, new String[]{"Test", "1234"}, new int[]{0, 5}, new int[]{4, 9}, 9);

    reader = new CJKWidthCharFilter(new StringReader("ｶﾀカナ ｳﾞｨッツ ﾊﾟﾅｿニック"));
    ts = whitespaceMockTokenizer(reader);
    assertTokenStreamContents(ts, new String[]{"カタカナ", "ヴィッツ", "パナソニック"}, new int[]{0, 5, 11}, new int[]{4, 10, 18}, 18);
  }

  public void testEmptyInput() throws Exception {
    CharFilter reader = new CJKWidthCharFilter(new StringReader(""));
    TokenStream ts = whitespaceMockTokenizer(reader);
    assertTokenStreamContents(ts, new String[]{});
  }

  public void testRandom() throws Exception {
    Analyzer analyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
        return new TokenStreamComponents(tokenizer, tokenizer);
      }
      @Override
      protected Reader initReader(String fieldName, Reader reader) {
        return new CJKWidthCharFilter(reader);
      }
    };
    int numRounds = RANDOM_MULTIPLIER * 1000;
    checkRandomData(random(), analyzer, numRounds);
    analyzer.close();
  }

}
