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
package org.apache.lucene.analysis.ngram;


import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.util.TestUtil;

import java.io.IOException;
import java.io.StringReader;
import java.util.Random;

/**
 * Tests {@link NGramTokenFilter} for correctness.
 */
public class NGramTokenFilterTest extends BaseTokenStreamTestCase {
  private TokenStream input;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    input = whitespaceMockTokenizer("abcde");
  }
  
  public void testInvalidInput() throws Exception {
    expectThrows(IllegalArgumentException.class, () -> {
      new NGramTokenFilter(input, 2, 1);
    });
  }
  
  public void testInvalidInput2() throws Exception {
    expectThrows(IllegalArgumentException.class, () -> {     
      new NGramTokenFilter(input, 0, 1);
    });
  }

  public void testUnigrams() throws Exception {
    NGramTokenFilter filter = new NGramTokenFilter(input, 1, 1);
    assertTokenStreamContents(filter, new String[]{"a","b","c","d","e"}, new int[]{0,0,0,0,0}, new int[]{5,5,5,5,5}, new int[]{1,0,0,0,0});
  }
  
  public void testBigrams() throws Exception {
    NGramTokenFilter filter = new NGramTokenFilter(input, 2, 2);
    assertTokenStreamContents(filter, new String[]{"ab","bc","cd","de"}, new int[]{0,0,0,0}, new int[]{5,5,5,5}, new int[]{1,0,0,0});
  }
  
  public void testNgrams() throws Exception {
    NGramTokenFilter filter = new NGramTokenFilter(input, 1, 3);
    assertTokenStreamContents(filter,
        new String[]{"a","ab","abc","b","bc","bcd","c","cd","cde","d","de","e"},
        new int[]{0,0,0,0,0,0,0,0,0,0,0,0},
        new int[]{5,5,5,5,5,5,5,5,5,5,5,5},
        null,
        new int[]{1,0,0,0,0,0,0,0,0,0,0,0},
        null, null, false
        );
  }

  public void testNgramsNoIncrement() throws Exception {
    NGramTokenFilter filter = new NGramTokenFilter(input, 1, 3);
    assertTokenStreamContents(filter,
        new String[]{"a","ab","abc","b","bc","bcd","c","cd","cde","d","de","e"},
        new int[]{0,0,0,0,0,0,0,0,0,0,0,0},
        new int[]{5,5,5,5,5,5,5,5,5,5,5,5},
        null,
        new int[]{1,0,0,0,0,0,0,0,0,0,0,0},
        null, null, false
        );
  }

  public void testOversizedNgrams() throws Exception {
    NGramTokenFilter filter = new NGramTokenFilter(input, 6, 7);
    assertTokenStreamContents(filter, new String[0], new int[0], new int[0]);
  }
  
  public void testSmallTokenInStream() throws Exception {
    input = whitespaceMockTokenizer("abc de fgh");
    NGramTokenFilter filter = new NGramTokenFilter(input, 3, 3);
    assertTokenStreamContents(filter, new String[]{"abc","fgh"}, new int[]{0,7}, new int[]{3,10}, new int[] {1, 2});
  }
  
  public void testReset() throws Exception {
    WhitespaceTokenizer tokenizer = new WhitespaceTokenizer();
    tokenizer.setReader(new StringReader("abcde"));
    NGramTokenFilter filter = new NGramTokenFilter(tokenizer, 1, 1);
    assertTokenStreamContents(filter, new String[]{"a","b","c","d","e"}, new int[]{0,0,0,0,0}, new int[]{5,5,5,5,5}, new int[]{1,0,0,0,0});
    tokenizer.setReader(new StringReader("abcde"));
    assertTokenStreamContents(filter, new String[]{"a","b","c","d","e"}, new int[]{0,0,0,0,0}, new int[]{5,5,5,5,5}, new int[]{1,0,0,0,0});
  }
  
  // LUCENE-3642
  // EdgeNgram blindly adds term length to offset, but this can take things out of bounds
  // wrt original text if a previous filter increases the length of the word (in this case æ -> ae)
  // so in this case we behave like WDF, and preserve any modified offsets
  public void testInvalidOffsets() throws Exception {
    Analyzer analyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
        TokenFilter filters = new ASCIIFoldingFilter(tokenizer);
        filters = new NGramTokenFilter(filters, 2, 2);
        return new TokenStreamComponents(tokenizer, filters);
      }
    };
    assertAnalyzesTo(analyzer, "mosfellsbær",
        new String[] { "mo", "os", "sf", "fe", "el", "ll", "ls", "sb", "ba", "ae", "er" },
        new int[]    {    0,    0,    0,    0,    0,    0,    0,    0,    0,    0,    0 },
        new int[]    {   11,   11,   11,   11,   11,   11,   11,   11,   11,   11,   11 },
        new int[]    {     1,   0,    0,    0,    0,    0,    0,    0,    0,    0,    0  });
    analyzer.close();
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    for (int i = 0; i < 10; i++) {
      final int min = TestUtil.nextInt(random(), 2, 10);
      final int max = TestUtil.nextInt(random(), min, 20);
      Analyzer a = new Analyzer() {
        @Override
        protected TokenStreamComponents createComponents(String fieldName) {
          Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
          return new TokenStreamComponents(tokenizer, 
              new NGramTokenFilter(tokenizer, min, max));
        }    
      };
      checkRandomData(random(), a, 200*RANDOM_MULTIPLIER, 20);
      a.close();
    }
  }
  
  public void testEmptyTerm() throws Exception {
    Random random = random();
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new KeywordTokenizer();
        return new TokenStreamComponents(tokenizer, 
            new NGramTokenFilter(tokenizer, 2, 15));
      }    
    };
    checkAnalysisConsistency(random, a, random.nextBoolean(), "");
    a.close();
  }

  public void testSupplementaryCharacters() throws IOException {
    final String s = TestUtil.randomUnicodeString(random(), 10);
    final int codePointCount = s.codePointCount(0, s.length());
    final int minGram = TestUtil.nextInt(random(), 1, 3);
    final int maxGram = TestUtil.nextInt(random(), minGram, 10);
    TokenStream tk = new KeywordTokenizer();
    ((Tokenizer)tk).setReader(new StringReader(s));
    tk = new NGramTokenFilter(tk, minGram, maxGram);
    final CharTermAttribute termAtt = tk.addAttribute(CharTermAttribute.class);
    final OffsetAttribute offsetAtt = tk.addAttribute(OffsetAttribute.class);
    tk.reset();
    for (int start = 0; start < codePointCount; ++start) {
      for (int end = start + minGram; end <= Math.min(codePointCount, start + maxGram); ++end) {
        assertTrue(tk.incrementToken());
        assertEquals(0, offsetAtt.startOffset());
        assertEquals(s.length(), offsetAtt.endOffset());
        final int startIndex = Character.offsetByCodePoints(s, 0, start);
        final int endIndex = Character.offsetByCodePoints(s, 0, end);
        assertEquals(s.substring(startIndex, endIndex), termAtt.toString());
      }
    }
    assertFalse(tk.incrementToken());
  }

}
