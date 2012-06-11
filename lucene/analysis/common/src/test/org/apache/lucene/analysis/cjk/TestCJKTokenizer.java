package org.apache.lucene.analysis.cjk;

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

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.util.Version;

/** @deprecated Remove when CJKTokenizer is removed (5.0) */
@Deprecated
public class TestCJKTokenizer extends BaseTokenStreamTestCase {
  
  class TestToken {
    String termText;
    int start;
    int end;
    String type;
  }

  public TestToken newToken(String termText, int start, int end, int type) {
    TestToken token = new TestToken();
    token.termText = termText;
    token.type = CJKTokenizer.TOKEN_TYPE_NAMES[type];
    token.start = start;
    token.end = end;
    return token;
  }

  public void checkCJKToken(final String str, final TestToken[] out_tokens) throws IOException {
    Analyzer analyzer = new CJKAnalyzer(Version.LUCENE_30);
    String terms[] = new String[out_tokens.length];
    int startOffsets[] = new int[out_tokens.length];
    int endOffsets[] = new int[out_tokens.length];
    String types[] = new String[out_tokens.length];
    for (int i = 0; i < out_tokens.length; i++) {
      terms[i] = out_tokens[i].termText;
      startOffsets[i] = out_tokens[i].start;
      endOffsets[i] = out_tokens[i].end;
      types[i] = out_tokens[i].type;
    }
    assertAnalyzesTo(analyzer, str, terms, startOffsets, endOffsets, types, null);
  }
  
  public void checkCJKTokenReusable(final Analyzer a, final String str, final TestToken[] out_tokens) throws IOException {
    Analyzer analyzer = new CJKAnalyzer(Version.LUCENE_30);
    String terms[] = new String[out_tokens.length];
    int startOffsets[] = new int[out_tokens.length];
    int endOffsets[] = new int[out_tokens.length];
    String types[] = new String[out_tokens.length];
    for (int i = 0; i < out_tokens.length; i++) {
      terms[i] = out_tokens[i].termText;
      startOffsets[i] = out_tokens[i].start;
      endOffsets[i] = out_tokens[i].end;
      types[i] = out_tokens[i].type;
    }
    assertAnalyzesToReuse(analyzer, str, terms, startOffsets, endOffsets, types, null);
  }
  
  public void testJa1() throws IOException {
    String str = "\u4e00\u4e8c\u4e09\u56db\u4e94\u516d\u4e03\u516b\u4e5d\u5341";
       
    TestToken[] out_tokens = { 
      newToken("\u4e00\u4e8c", 0, 2, CJKTokenizer.DOUBLE_TOKEN_TYPE), 
      newToken("\u4e8c\u4e09", 1, 3, CJKTokenizer.DOUBLE_TOKEN_TYPE),
      newToken("\u4e09\u56db", 2, 4, CJKTokenizer.DOUBLE_TOKEN_TYPE),
      newToken("\u56db\u4e94", 3, 5, CJKTokenizer.DOUBLE_TOKEN_TYPE), 
      newToken("\u4e94\u516d", 4, 6, CJKTokenizer.DOUBLE_TOKEN_TYPE), 
      newToken("\u516d\u4e03", 5, 7, CJKTokenizer.DOUBLE_TOKEN_TYPE),
      newToken("\u4e03\u516b", 6, 8, CJKTokenizer.DOUBLE_TOKEN_TYPE),
      newToken("\u516b\u4e5d", 7, 9, CJKTokenizer.DOUBLE_TOKEN_TYPE),
      newToken("\u4e5d\u5341", 8,10, CJKTokenizer.DOUBLE_TOKEN_TYPE)
    };
    checkCJKToken(str, out_tokens);
  }
  
  public void testJa2() throws IOException {
    String str = "\u4e00 \u4e8c\u4e09\u56db \u4e94\u516d\u4e03\u516b\u4e5d \u5341";
       
    TestToken[] out_tokens = { 
      newToken("\u4e00", 0, 1, CJKTokenizer.DOUBLE_TOKEN_TYPE), 
      newToken("\u4e8c\u4e09", 2, 4, CJKTokenizer.DOUBLE_TOKEN_TYPE),
      newToken("\u4e09\u56db", 3, 5, CJKTokenizer.DOUBLE_TOKEN_TYPE),
      newToken("\u4e94\u516d", 6, 8, CJKTokenizer.DOUBLE_TOKEN_TYPE), 
      newToken("\u516d\u4e03", 7, 9, CJKTokenizer.DOUBLE_TOKEN_TYPE),
      newToken("\u4e03\u516b", 8, 10, CJKTokenizer.DOUBLE_TOKEN_TYPE),
      newToken("\u516b\u4e5d", 9, 11, CJKTokenizer.DOUBLE_TOKEN_TYPE),
      newToken("\u5341", 12,13, CJKTokenizer.DOUBLE_TOKEN_TYPE)
    };
    checkCJKToken(str, out_tokens);
  }
  
  public void testC() throws IOException {
    String str = "abc defgh ijklmn opqrstu vwxy z";
       
    TestToken[] out_tokens = { 
      newToken("abc", 0, 3, CJKTokenizer.SINGLE_TOKEN_TYPE), 
      newToken("defgh", 4, 9, CJKTokenizer.SINGLE_TOKEN_TYPE),
      newToken("ijklmn", 10, 16, CJKTokenizer.SINGLE_TOKEN_TYPE),
      newToken("opqrstu", 17, 24, CJKTokenizer.SINGLE_TOKEN_TYPE), 
      newToken("vwxy", 25, 29, CJKTokenizer.SINGLE_TOKEN_TYPE), 
      newToken("z", 30, 31, CJKTokenizer.SINGLE_TOKEN_TYPE),
    };
    checkCJKToken(str, out_tokens);
  }
  
  public void testMix() throws IOException {
    String str = "\u3042\u3044\u3046\u3048\u304aabc\u304b\u304d\u304f\u3051\u3053";
       
    TestToken[] out_tokens = { 
      newToken("\u3042\u3044", 0, 2, CJKTokenizer.DOUBLE_TOKEN_TYPE), 
      newToken("\u3044\u3046", 1, 3, CJKTokenizer.DOUBLE_TOKEN_TYPE),
      newToken("\u3046\u3048", 2, 4, CJKTokenizer.DOUBLE_TOKEN_TYPE),
      newToken("\u3048\u304a", 3, 5, CJKTokenizer.DOUBLE_TOKEN_TYPE), 
      newToken("abc", 5, 8, CJKTokenizer.SINGLE_TOKEN_TYPE), 
      newToken("\u304b\u304d", 8, 10, CJKTokenizer.DOUBLE_TOKEN_TYPE),
      newToken("\u304d\u304f", 9, 11, CJKTokenizer.DOUBLE_TOKEN_TYPE),
      newToken("\u304f\u3051", 10,12, CJKTokenizer.DOUBLE_TOKEN_TYPE),
      newToken("\u3051\u3053", 11,13, CJKTokenizer.DOUBLE_TOKEN_TYPE)
    };
    checkCJKToken(str, out_tokens);
  }
  
  public void testMix2() throws IOException {
    String str = "\u3042\u3044\u3046\u3048\u304aab\u3093c\u304b\u304d\u304f\u3051 \u3053";
       
    TestToken[] out_tokens = { 
      newToken("\u3042\u3044", 0, 2, CJKTokenizer.DOUBLE_TOKEN_TYPE), 
      newToken("\u3044\u3046", 1, 3, CJKTokenizer.DOUBLE_TOKEN_TYPE),
      newToken("\u3046\u3048", 2, 4, CJKTokenizer.DOUBLE_TOKEN_TYPE),
      newToken("\u3048\u304a", 3, 5, CJKTokenizer.DOUBLE_TOKEN_TYPE), 
      newToken("ab", 5, 7, CJKTokenizer.SINGLE_TOKEN_TYPE), 
      newToken("\u3093", 7, 8, CJKTokenizer.DOUBLE_TOKEN_TYPE), 
      newToken("c", 8, 9, CJKTokenizer.SINGLE_TOKEN_TYPE), 
      newToken("\u304b\u304d", 9, 11, CJKTokenizer.DOUBLE_TOKEN_TYPE),
      newToken("\u304d\u304f", 10, 12, CJKTokenizer.DOUBLE_TOKEN_TYPE),
      newToken("\u304f\u3051", 11,13, CJKTokenizer.DOUBLE_TOKEN_TYPE),
      newToken("\u3053", 14,15, CJKTokenizer.DOUBLE_TOKEN_TYPE)
    };
    checkCJKToken(str, out_tokens);
  }

  public void testSingleChar() throws IOException {
    String str = "\u4e00";
       
    TestToken[] out_tokens = { 
      newToken("\u4e00", 0, 1, CJKTokenizer.DOUBLE_TOKEN_TYPE), 
    };
    checkCJKToken(str, out_tokens);
  }
  
  /*
   * Full-width text is normalized to half-width 
   */
  public void testFullWidth() throws Exception {
    String str = "Ｔｅｓｔ １２３４";
    TestToken[] out_tokens = { 
        newToken("test", 0, 4, CJKTokenizer.SINGLE_TOKEN_TYPE), 
        newToken("1234", 5, 9, CJKTokenizer.SINGLE_TOKEN_TYPE)
    };
    checkCJKToken(str, out_tokens);
  }
  
  /*
   * Non-english text (not just CJK) is treated the same as CJK: C1C2 C2C3 
   */
  public void testNonIdeographic() throws Exception {
    String str = "\u4e00 روبرت موير";
    TestToken[] out_tokens = {
        newToken("\u4e00", 0, 1, CJKTokenizer.DOUBLE_TOKEN_TYPE),
        newToken("رو", 2, 4, CJKTokenizer.DOUBLE_TOKEN_TYPE),
        newToken("وب", 3, 5, CJKTokenizer.DOUBLE_TOKEN_TYPE),
        newToken("بر", 4, 6, CJKTokenizer.DOUBLE_TOKEN_TYPE),
        newToken("رت", 5, 7, CJKTokenizer.DOUBLE_TOKEN_TYPE),
        newToken("مو", 8, 10, CJKTokenizer.DOUBLE_TOKEN_TYPE),
        newToken("وي", 9, 11, CJKTokenizer.DOUBLE_TOKEN_TYPE),
        newToken("ير", 10, 12, CJKTokenizer.DOUBLE_TOKEN_TYPE)
    };
    checkCJKToken(str, out_tokens);
  }
  
  /*
   * Non-english text with nonletters (non-spacing marks,etc) is treated as C1C2 C2C3,
   * except for words are split around non-letters.
   */
  public void testNonIdeographicNonLetter() throws Exception {
    String str = "\u4e00 رُوبرت موير";
    TestToken[] out_tokens = {
        newToken("\u4e00", 0, 1, CJKTokenizer.DOUBLE_TOKEN_TYPE),
        newToken("ر", 2, 3, CJKTokenizer.DOUBLE_TOKEN_TYPE),
        newToken("وب", 4, 6, CJKTokenizer.DOUBLE_TOKEN_TYPE),
        newToken("بر", 5, 7, CJKTokenizer.DOUBLE_TOKEN_TYPE),
        newToken("رت", 6, 8, CJKTokenizer.DOUBLE_TOKEN_TYPE),
        newToken("مو", 9, 11, CJKTokenizer.DOUBLE_TOKEN_TYPE),
        newToken("وي", 10, 12, CJKTokenizer.DOUBLE_TOKEN_TYPE),
        newToken("ير", 11, 13, CJKTokenizer.DOUBLE_TOKEN_TYPE)
    };
    checkCJKToken(str, out_tokens);
  }
  
  public void testTokenStream() throws Exception {
    Analyzer analyzer = new CJKAnalyzer(Version.LUCENE_30);
    assertAnalyzesTo(analyzer, "\u4e00\u4e01\u4e02", 
        new String[] { "\u4e00\u4e01", "\u4e01\u4e02"});
  }
  
  public void testReusableTokenStream() throws Exception {
    Analyzer analyzer = new CJKAnalyzer(Version.LUCENE_30);
    String str = "\u3042\u3044\u3046\u3048\u304aabc\u304b\u304d\u304f\u3051\u3053";
    
    TestToken[] out_tokens = { 
      newToken("\u3042\u3044", 0, 2, CJKTokenizer.DOUBLE_TOKEN_TYPE), 
      newToken("\u3044\u3046", 1, 3, CJKTokenizer.DOUBLE_TOKEN_TYPE),
      newToken("\u3046\u3048", 2, 4, CJKTokenizer.DOUBLE_TOKEN_TYPE),
      newToken("\u3048\u304a", 3, 5, CJKTokenizer.DOUBLE_TOKEN_TYPE), 
      newToken("abc", 5, 8, CJKTokenizer.SINGLE_TOKEN_TYPE), 
      newToken("\u304b\u304d", 8, 10, CJKTokenizer.DOUBLE_TOKEN_TYPE),
      newToken("\u304d\u304f", 9, 11, CJKTokenizer.DOUBLE_TOKEN_TYPE),
      newToken("\u304f\u3051", 10,12, CJKTokenizer.DOUBLE_TOKEN_TYPE),
      newToken("\u3051\u3053", 11,13, CJKTokenizer.DOUBLE_TOKEN_TYPE)
    };
    checkCJKTokenReusable(analyzer, str, out_tokens);
    
    str = "\u3042\u3044\u3046\u3048\u304aab\u3093c\u304b\u304d\u304f\u3051 \u3053";
    TestToken[] out_tokens2 = { 
      newToken("\u3042\u3044", 0, 2, CJKTokenizer.DOUBLE_TOKEN_TYPE), 
      newToken("\u3044\u3046", 1, 3, CJKTokenizer.DOUBLE_TOKEN_TYPE),
      newToken("\u3046\u3048", 2, 4, CJKTokenizer.DOUBLE_TOKEN_TYPE),
      newToken("\u3048\u304a", 3, 5, CJKTokenizer.DOUBLE_TOKEN_TYPE), 
      newToken("ab", 5, 7, CJKTokenizer.SINGLE_TOKEN_TYPE), 
      newToken("\u3093", 7, 8, CJKTokenizer.DOUBLE_TOKEN_TYPE), 
      newToken("c", 8, 9, CJKTokenizer.SINGLE_TOKEN_TYPE), 
      newToken("\u304b\u304d", 9, 11, CJKTokenizer.DOUBLE_TOKEN_TYPE),
      newToken("\u304d\u304f", 10, 12, CJKTokenizer.DOUBLE_TOKEN_TYPE),
      newToken("\u304f\u3051", 11,13, CJKTokenizer.DOUBLE_TOKEN_TYPE),
      newToken("\u3053", 14,15, CJKTokenizer.DOUBLE_TOKEN_TYPE)
    };
    checkCJKTokenReusable(analyzer, str, out_tokens2);
  }
  
  /**
   * LUCENE-2207: wrong offset calculated by end() 
   */
  public void testFinalOffset() throws IOException {
    checkCJKToken("あい", new TestToken[] { 
        newToken("あい", 0, 2, CJKTokenizer.DOUBLE_TOKEN_TYPE) });
    checkCJKToken("あい   ", new TestToken[] { 
        newToken("あい", 0, 2, CJKTokenizer.DOUBLE_TOKEN_TYPE) });
    checkCJKToken("test", new TestToken[] { 
        newToken("test", 0, 4, CJKTokenizer.SINGLE_TOKEN_TYPE) });
    checkCJKToken("test   ", new TestToken[] { 
        newToken("test", 0, 4, CJKTokenizer.SINGLE_TOKEN_TYPE) });
    checkCJKToken("あいtest", new TestToken[] {
        newToken("あい", 0, 2, CJKTokenizer.DOUBLE_TOKEN_TYPE),
        newToken("test", 2, 6, CJKTokenizer.SINGLE_TOKEN_TYPE) });
    checkCJKToken("testあい    ", new TestToken[] { 
        newToken("test", 0, 4, CJKTokenizer.SINGLE_TOKEN_TYPE),
        newToken("あい", 4, 6, CJKTokenizer.DOUBLE_TOKEN_TYPE) });
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    checkRandomData(random(), new CJKAnalyzer(Version.LUCENE_30), 10000*RANDOM_MULTIPLIER);
  }
}
