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


import java.io.IOException;
import java.io.StringReader;
import java.util.Random;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.util.TestUtil;

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
      new NGramTokenFilter(input, 2, 1, false);
    });
  }
  
  public void testInvalidInput2() throws Exception {
    expectThrows(IllegalArgumentException.class, () -> {     
      new NGramTokenFilter(input, 0, 1, false);
    });
  }

  public void testUnigrams() throws Exception {
    NGramTokenFilter filter = new NGramTokenFilter(input, 1, 1, false);
    assertTokenStreamContents(filter, new String[]{"a","b","c","d","e"}, new int[]{0,0,0,0,0}, new int[]{5,5,5,5,5}, new int[]{1,0,0,0,0});
  }
  
  public void testBigrams() throws Exception {
    NGramTokenFilter filter = new NGramTokenFilter(input, 2, 2, false);
    assertTokenStreamContents(filter, new String[]{"ab","bc","cd","de"}, new int[]{0,0,0,0}, new int[]{5,5,5,5}, new int[]{1,0,0,0});
  }
  
  public void testNgrams() throws Exception {
    NGramTokenFilter filter = new NGramTokenFilter(input, 1, 3, false);
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
    NGramTokenFilter filter = new NGramTokenFilter(input, 1, 3, false);
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
    NGramTokenFilter filter = new NGramTokenFilter(input, 6, 7, false);
    assertTokenStreamContents(filter, new String[0], new int[0], new int[0]);
  }
  
  public void testOversizedNgramsPreserveOriginal() throws Exception {
    NGramTokenFilter tokenizer = new NGramTokenFilter(input, 6, 6, true);
    assertTokenStreamContents(tokenizer, new String[] {"abcde"}, new int[] {0}, new int[] {5});
  }
  
  public void testSmallTokenInStream() throws Exception {
    input = whitespaceMockTokenizer("abc de fgh");
    NGramTokenFilter tokenizer = new NGramTokenFilter(input, 3, 3, false);
    assertTokenStreamContents(tokenizer, new String[]{"abc","fgh"}, new int[]{0,7}, new int[]{3,10}, new int[] {1, 2});
  }
  
  public void testSmallTokenInStreamPreserveOriginal() throws Exception {
    input = whitespaceMockTokenizer("abc de fgh");
    NGramTokenFilter tokenizer = new NGramTokenFilter(input, 3, 3, true);
    assertTokenStreamContents(tokenizer, new String[]{"abc","de","fgh"}, new int[]{0,4,7}, new int[]{3,6,10}, new int[] {1, 1, 1});

  }
  
  public void testReset() throws Exception {
    WhitespaceTokenizer tokenizer = new WhitespaceTokenizer();
    tokenizer.setReader(new StringReader("abcde"));
    NGramTokenFilter filter = new NGramTokenFilter(tokenizer, 1, 1, false);
    assertTokenStreamContents(filter, new String[]{"a","b","c","d","e"}, new int[]{0,0,0,0,0}, new int[]{5,5,5,5,5}, new int[]{1,0,0,0,0});
    tokenizer.setReader(new StringReader("abcde"));
    assertTokenStreamContents(filter, new String[]{"a","b","c","d","e"}, new int[]{0,0,0,0,0}, new int[]{5,5,5,5,5}, new int[]{1,0,0,0,0});
  }
  
  public void testKeepShortTermKeepLongTerm() throws Exception {
    final String inputString = "a bcd efghi jk";

    { // preserveOriginal = false
      TokenStream ts = whitespaceMockTokenizer(inputString);
      NGramTokenFilter filter = new NGramTokenFilter(ts, 2, 3, false);
      assertTokenStreamContents(filter,
          new String[] { "bc", "bcd",  "cd", "ef", "efg", "fg", "fgh", "gh", "ghi", "hi", "jk" },
          new int[]    {    2,     2,     2,    6,     6,    6,     6,    6,     6,    6,   12 },
          new int[]    {    5,     5,     5,   11,    11,   11,    11,   11,    11,   11,   14 },
          new int[]    {    2,     0,     0,    1,     0,    0,     0,    0,     0,    0,    1 });
    }

    { // preserveOriginal = true
      TokenStream ts = whitespaceMockTokenizer(inputString);
      NGramTokenFilter filter = new NGramTokenFilter(ts, 2, 3, true);
      assertTokenStreamContents(filter,
          new String[] { "a", "bc", "bcd",  "cd", "ef", "efg", "fg", "fgh", "gh", "ghi", "hi", "efghi", "jk" },
          new int[]    {   0,    2,     2,     2,    6,     6,    6,     6,    6,     6,    6,       6,   12 },
          new int[]    {   1,    5,     5,     5,   11,    11,   11,    11,   11,    11,   11,      11,   14 },
          new int[]    {   1,    1,     0,     0,    1,     0,    0,     0,    0,     0,    0,       0,    1 });
    }
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
        filters = new NGramTokenFilter(filters, 2, 2, false);
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

  public void testEndPositionIncrement() throws IOException {
    TokenStream source = whitespaceMockTokenizer("seventeen one two three four");
    TokenStream input = new NGramTokenFilter(source, 8, 8, false);
    PositionIncrementAttribute posIncAtt = input.addAttribute(PositionIncrementAttribute.class);
    input.reset();
    while (input.incrementToken()) {}
    input.end();
    assertEquals(4, posIncAtt.getPositionIncrement());
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    for (int i = 0; i < 10; i++) {
      final int min = TestUtil.nextInt(random(), 2, 10);
      final int max = TestUtil.nextInt(random(), min, 20);
      final boolean preserveOriginal = TestUtil.nextInt(random(), 0, 1) % 2 == 0;
      
      Analyzer a = new Analyzer() {
        @Override
        protected TokenStreamComponents createComponents(String fieldName) {
          Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
          return new TokenStreamComponents(tokenizer, 
              new NGramTokenFilter(tokenizer, min, max, preserveOriginal));
        }    
      };
      checkRandomData(random(), a, 10*RANDOM_MULTIPLIER, 20);
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
            new NGramTokenFilter(tokenizer, 2, 15, false));
      }    
    };
    checkAnalysisConsistency(random, a, random.nextBoolean(), "");
    a.close();
  }

  public void testSupplementaryCharacters() throws IOException {
    for (int i = 0; i < 20; i++) {
      final String s = TestUtil.randomUnicodeString(random(), 10);
      final int codePointCount = s.codePointCount(0, s.length());
      final int minGram = TestUtil.nextInt(random(), 1, 3);
      final int maxGram = TestUtil.nextInt(random(), minGram, 10);
      final boolean preserveOriginal = TestUtil.nextInt(random(), 0, 1) % 2 == 0;

      TokenStream tk = new KeywordTokenizer();
      ((Tokenizer)tk).setReader(new StringReader(s));
      tk = new NGramTokenFilter(tk, minGram, maxGram, preserveOriginal);
      final CharTermAttribute termAtt = tk.addAttribute(CharTermAttribute.class);
      final OffsetAttribute offsetAtt = tk.addAttribute(OffsetAttribute.class);
      tk.reset();

      if (codePointCount < minGram && preserveOriginal) {
        assertTrue(tk.incrementToken());
        assertEquals(0, offsetAtt.startOffset());
        assertEquals(s.length(), offsetAtt.endOffset());
        assertEquals(s, termAtt.toString());
      }
      
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
      
      if (codePointCount > maxGram && preserveOriginal) {
        assertTrue(tk.incrementToken());
        assertEquals(0, offsetAtt.startOffset());
        assertEquals(s.length(), offsetAtt.endOffset());
        assertEquals(s, termAtt.toString());
      }
      
      assertFalse(tk.incrementToken());
      tk.close();
    }
  }
}
