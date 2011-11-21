package org.apache.lucene.analysis;

import java.io.StringReader;

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

public class TestMockAnalyzer extends BaseTokenStreamTestCase {

  /** Test a configuration that behaves a lot like WhitespaceAnalyzer */
  public void testWhitespace() throws Exception {
    Analyzer a = new MockAnalyzer(random);
    assertAnalyzesTo(a, "A bc defg hiJklmn opqrstuv wxy z ",
        new String[] { "a", "bc", "defg", "hijklmn", "opqrstuv", "wxy", "z" });
    assertAnalyzesToReuse(a, "aba cadaba shazam",
        new String[] { "aba", "cadaba", "shazam" });
    assertAnalyzesToReuse(a, "break on whitespace",
        new String[] { "break", "on", "whitespace" });
  }
  
  /** Test a configuration that behaves a lot like SimpleAnalyzer */
  public void testSimple() throws Exception {
    Analyzer a = new MockAnalyzer(random, MockTokenizer.SIMPLE, true);
    assertAnalyzesTo(a, "a-bc123 defg+hijklmn567opqrstuv78wxy_z ",
        new String[] { "a", "bc", "defg", "hijklmn", "opqrstuv", "wxy", "z" });
    assertAnalyzesToReuse(a, "aba4cadaba-Shazam",
        new String[] { "aba", "cadaba", "shazam" });
    assertAnalyzesToReuse(a, "break+on/Letters",
        new String[] { "break", "on", "letters" });
  }
  
  /** Test a configuration that behaves a lot like KeywordAnalyzer */
  public void testKeyword() throws Exception {
    Analyzer a = new MockAnalyzer(random, MockTokenizer.KEYWORD, false);
    assertAnalyzesTo(a, "a-bc123 defg+hijklmn567opqrstuv78wxy_z ",
        new String[] { "a-bc123 defg+hijklmn567opqrstuv78wxy_z " });
    assertAnalyzesToReuse(a, "aba4cadaba-Shazam",
        new String[] { "aba4cadaba-Shazam" });
    assertAnalyzesToReuse(a, "break+on/Nothing",
        new String[] { "break+on/Nothing" });
  }
  
  /** Test a configuration that behaves a lot like StopAnalyzer */
  public void testStop() throws Exception {
    Analyzer a = new MockAnalyzer(random, MockTokenizer.SIMPLE, true, (CharArraySet) StopAnalyzer.ENGLISH_STOP_WORDS_SET, true);
    assertAnalyzesTo(a, "the quick brown a fox",
        new String[] { "quick", "brown", "fox" },
        new int[] { 2, 1, 2 });
    
    // disable positions
    a = new MockAnalyzer(random, MockTokenizer.SIMPLE, true, (CharArraySet) StopAnalyzer.ENGLISH_STOP_WORDS_SET, false);
    assertAnalyzesTo(a, "the quick brown a fox",
        new String[] { "quick", "brown", "fox" },
        new int[] { 1, 1, 1 });
  }
  
  public void testLUCENE_3042() throws Exception {
    String testString = "t";
    
    Analyzer analyzer = new MockAnalyzer(random);
    TokenStream stream = analyzer.reusableTokenStream("dummy", new StringReader(testString));
    stream.reset();
    while (stream.incrementToken()) {
      // consume
    }
    stream.end();
    stream.close();
    
    assertAnalyzesToReuse(analyzer, testString, new String[] { "t" });
  }

  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    checkRandomData(random, new MockAnalyzer(random), atLeast(1000));
  }
}
