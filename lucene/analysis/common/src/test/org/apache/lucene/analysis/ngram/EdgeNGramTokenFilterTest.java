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
import org.apache.lucene.analysis.core.LetterTokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.util.TestUtil;

/**
 * Tests {@link EdgeNGramTokenFilter} for correctness.
 */
public class EdgeNGramTokenFilterTest extends BaseTokenStreamTestCase {
  private TokenStream input;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    input = whitespaceMockTokenizer("abcde");
  }

  public void testInvalidInput() throws Exception {
    expectThrows(IllegalArgumentException.class, () -> {
      new EdgeNGramTokenFilter(input, 0, 0, false);
    });
  }

  public void testInvalidInput2() throws Exception {
    expectThrows(IllegalArgumentException.class, () -> {   
      new EdgeNGramTokenFilter(input, 2, 1, false);
    });
  }

  public void testInvalidInput3() throws Exception {
    expectThrows(IllegalArgumentException.class, () -> {      
      new EdgeNGramTokenFilter(input, -1, 2, false);
    });
  }

  public void testFrontUnigram() throws Exception {
    EdgeNGramTokenFilter tokenizer = new EdgeNGramTokenFilter(input, 1, 1, false);
    assertTokenStreamContents(tokenizer, new String[]{"a"}, new int[]{0}, new int[]{5});
  }

  public void testOversizedNgrams() throws Exception {
    EdgeNGramTokenFilter tokenizer = new EdgeNGramTokenFilter(input, 6, 6, false);
    assertTokenStreamContents(tokenizer, new String[0], new int[0], new int[0]);
  }

  public void testOversizedNgramsPreserveOriginal() throws Exception {
    EdgeNGramTokenFilter tokenizer = new EdgeNGramTokenFilter(input, 6, 6, true);
    assertTokenStreamContents(tokenizer, new String[] {"abcde"}, new int[] {0}, new int[] {5});
  }

  public void testPreserveOriginal() throws Exception {
    final String inputString = "a bcd efghi jk";

    { // preserveOriginal = false
      TokenStream ts = whitespaceMockTokenizer(inputString);
      EdgeNGramTokenFilter filter = new EdgeNGramTokenFilter(ts, 2, 3, false);
      assertTokenStreamContents(filter,
          new String[] { "bc", "bcd",  "ef", "efg",  "jk" },
          new int[]    {    2,     2,    6,      6,    12 },
          new int[]    {    5,     5,   11,     11,    14 },
          new int[]    {    2,     0,    1,      0,     1 });
    }

    { // preserveOriginal = true
      TokenStream ts = whitespaceMockTokenizer(inputString);
      EdgeNGramTokenFilter filter = new EdgeNGramTokenFilter(ts, 2, 3, true);
      assertTokenStreamContents(filter,
          new String[] { "a", "bc", "bcd",  "ef", "efg", "efghi", "jk" },
          new int[]    {  0,     2,     2,    6,      6,      6,    12 },
          new int[]    {  1,     5,     5,   11,     11,     11,    14 },
          new int[]    {  1,     1,     0,    1,      0,      0,     1 });
    }
  }

  public void testFrontRangeOfNgrams() throws Exception {
    EdgeNGramTokenFilter tokenizer = new EdgeNGramTokenFilter(input, 1, 3, false);
    assertTokenStreamContents(tokenizer, new String[]{"a","ab","abc"}, new int[]{0,0,0}, new int[]{5,5,5});
  }

  public void testFilterPositions() throws Exception {
    TokenStream ts = whitespaceMockTokenizer("abcde vwxyz");
    EdgeNGramTokenFilter tokenizer = new EdgeNGramTokenFilter(ts, 1, 3, false);
    assertTokenStreamContents(tokenizer,
        new String[] {"a","ab","abc","v","vw","vwx"},
        new int[]      {0,   0,    0,  6,   6,    6},
        new int[]      {5,   5,    5, 11,  11,   11});
  }

  private static class PositionFilter extends TokenFilter {
    
    private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);
    private boolean started;
    
    PositionFilter(final TokenStream input) {
      super(input);
    }
    
    @Override
    public final boolean incrementToken() throws IOException {
      if (input.incrementToken()) {
        if (started) {
          posIncrAtt.setPositionIncrement(0);
        } else {
          started = true;
        }
        return true;
      } else {
        return false;
      }
    }
    
    @Override
    public void reset() throws IOException {
      super.reset();
      started = false;
    }
  }

  public void testFirstTokenPositionIncrement() throws Exception {
    TokenStream ts = whitespaceMockTokenizer("a abc");
    ts = new PositionFilter(ts); // All but first token will get 0 position increment
    EdgeNGramTokenFilter filter = new EdgeNGramTokenFilter(ts, 2, 3, false);
    // The first token "a" will not be output, since it's smaller than the mingram size of 2.
    // The second token on input to EdgeNGramTokenFilter will have position increment of 0,
    // which should be increased to 1, since this is the first output token in the stream.
    assertTokenStreamContents(filter,
        new String[] { "ab", "abc" },
        new int[]    {    2,     2 },
        new int[]    {    5,     5 },
        new int[]    {    1,     0 }
    );
  }
  
  public void testSmallTokenInStream() throws Exception {
    input = whitespaceMockTokenizer("abc de fgh");
    EdgeNGramTokenFilter tokenizer = new EdgeNGramTokenFilter(input, 3, 3, false);
    assertTokenStreamContents(tokenizer, new String[]{"abc","fgh"}, new int[]{0,7}, new int[]{3,10});
  }
  
  public void testReset() throws Exception {
    WhitespaceTokenizer tokenizer = new WhitespaceTokenizer();
    tokenizer.setReader(new StringReader("abcde"));
    EdgeNGramTokenFilter filter = new EdgeNGramTokenFilter(tokenizer, 1, 3, false);
    assertTokenStreamContents(filter, new String[]{"a","ab","abc"}, new int[]{0,0,0}, new int[]{5,5,5});
    tokenizer.setReader(new StringReader("abcde"));
    assertTokenStreamContents(filter, new String[]{"a","ab","abc"}, new int[]{0,0,0}, new int[]{5,5,5});
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
            new EdgeNGramTokenFilter(tokenizer, min, max, preserveOriginal));
        }    
      };
      checkRandomData(random(), a, 10*RANDOM_MULTIPLIER);
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
            new EdgeNGramTokenFilter(tokenizer, 2, 15, false));
      }    
    };
    checkAnalysisConsistency(random, a, random.nextBoolean(), "");
    a.close();
  }

  public void testGraphs() throws IOException {
    TokenStream tk = new LetterTokenizer();
    ((Tokenizer)tk).setReader(new StringReader("abc d efgh ij klmno p q"));
    tk = new ShingleFilter(tk);
    tk = new EdgeNGramTokenFilter(tk, 7, 10, false);
    assertTokenStreamContents(tk,
        new String[] { "efgh ij", "ij klmn", "ij klmno", "klmno p" },
        new int[]    { 6,11,11,14 },
        new int[]    { 13,19,19,21 },
        new int[]    { 3,1,0,1 },
        new int[]    { 2,2,2,2 },
        23
    );
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
      tk = new EdgeNGramTokenFilter(tk, minGram, maxGram, preserveOriginal);
      final CharTermAttribute termAtt = tk.addAttribute(CharTermAttribute.class);
      final OffsetAttribute offsetAtt = tk.addAttribute(OffsetAttribute.class);
      tk.reset();

      if (codePointCount < minGram && preserveOriginal) {
        assertTrue(tk.incrementToken());
        assertEquals(0, offsetAtt.startOffset());
        assertEquals(s.length(), offsetAtt.endOffset());
        assertEquals(s, termAtt.toString());
      }

      for (int j = minGram; j <= Math.min(codePointCount, maxGram); j++) {
        assertTrue(tk.incrementToken());
        assertEquals(0, offsetAtt.startOffset());
        assertEquals(s.length(), offsetAtt.endOffset());
        final int end = Character.offsetByCodePoints(s, 0, j);
        assertEquals(s.substring(0, end), termAtt.toString());
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

  public void testEndPositionIncrement() throws IOException {
    TokenStream source = whitespaceMockTokenizer("seventeen one two three four");
    TokenStream input = new EdgeNGramTokenFilter(source, 8, 8, false);
    PositionIncrementAttribute posIncAtt = input.addAttribute(PositionIncrementAttribute.class);
    input.reset();
    while (input.incrementToken()) {}
    input.end();
    assertEquals(4, posIncAtt.getPositionIncrement());
  }
}
