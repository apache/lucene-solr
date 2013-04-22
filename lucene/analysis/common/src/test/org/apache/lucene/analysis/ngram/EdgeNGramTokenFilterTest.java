package org.apache.lucene.analysis.ngram;

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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;
import org.apache.lucene.analysis.position.PositionFilter;

import java.io.Reader;
import java.io.StringReader;
import java.util.Random;

/**
 * Tests {@link EdgeNGramTokenFilter} for correctness.
 */
public class EdgeNGramTokenFilterTest extends BaseTokenStreamTestCase {
  private TokenStream input;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    input = new MockTokenizer(new StringReader("abcde"), MockTokenizer.WHITESPACE, false);
  }

  public void testInvalidInput() throws Exception {
    boolean gotException = false;
    try {        
      new EdgeNGramTokenFilter(input, EdgeNGramTokenFilter.Side.FRONT, 0, 0);
    } catch (IllegalArgumentException e) {
      gotException = true;
    }
    assertTrue(gotException);
  }

  public void testInvalidInput2() throws Exception {
    boolean gotException = false;
    try {        
      new EdgeNGramTokenFilter(input, EdgeNGramTokenFilter.Side.FRONT, 2, 1);
    } catch (IllegalArgumentException e) {
      gotException = true;
    }
    assertTrue(gotException);
  }

  public void testInvalidInput3() throws Exception {
    boolean gotException = false;
    try {        
      new EdgeNGramTokenFilter(input, EdgeNGramTokenFilter.Side.FRONT, -1, 2);
    } catch (IllegalArgumentException e) {
      gotException = true;
    }
    assertTrue(gotException);
  }

  public void testFrontUnigram() throws Exception {
    EdgeNGramTokenFilter tokenizer = new EdgeNGramTokenFilter(input, EdgeNGramTokenFilter.Side.FRONT, 1, 1);
    assertTokenStreamContents(tokenizer, new String[]{"a"}, new int[]{0}, new int[]{1});
  }

  public void testBackUnigram() throws Exception {
    EdgeNGramTokenFilter tokenizer = new EdgeNGramTokenFilter(input, EdgeNGramTokenFilter.Side.BACK, 1, 1);
    assertTokenStreamContents(tokenizer, new String[]{"e"}, new int[]{4}, new int[]{5});
  }

  public void testOversizedNgrams() throws Exception {
    EdgeNGramTokenFilter tokenizer = new EdgeNGramTokenFilter(input, EdgeNGramTokenFilter.Side.FRONT, 6, 6);
    assertTokenStreamContents(tokenizer, new String[0], new int[0], new int[0]);
  }

  public void testFrontRangeOfNgrams() throws Exception {
    EdgeNGramTokenFilter tokenizer = new EdgeNGramTokenFilter(input, EdgeNGramTokenFilter.Side.FRONT, 1, 3);
    assertTokenStreamContents(tokenizer, new String[]{"a","ab","abc"}, new int[]{0,0,0}, new int[]{1,2,3});
  }

  public void testBackRangeOfNgrams() throws Exception {
    EdgeNGramTokenFilter tokenizer = new EdgeNGramTokenFilter(input, EdgeNGramTokenFilter.Side.BACK, 1, 3);
    assertTokenStreamContents(tokenizer,
                              new String[]{"e","de","cde"},
                              new int[]{4,3,2},
                              new int[]{5,5,5},
                              null,
                              null,
                              null,
                              null,
                              false);
  }

  public void testFilterPositions() throws Exception {
    TokenStream ts = new MockTokenizer(new StringReader("abcde vwxyz"), MockTokenizer.WHITESPACE, false);
    EdgeNGramTokenFilter tokenizer = new EdgeNGramTokenFilter(ts, EdgeNGramTokenFilter.Side.FRONT, 1, 3);
    assertTokenStreamContents(tokenizer,
                              new String[]{"a","ab","abc","v","vw","vwx"},
                              new int[]{0,0,0,6,6,6},
                              new int[]{1,2,3,7,8,9},
                              null,
                              new int[]{1,0,0,1,0,0},
                              null,
                              null,
                              false);
  }

  public void testFirstTokenPositionIncrement() throws Exception {
    TokenStream ts = new MockTokenizer(new StringReader("a abc"), MockTokenizer.WHITESPACE, false);
    ts = new PositionFilter(ts, 0); // All but first token will get 0 position increment
    EdgeNGramTokenFilter filter = new EdgeNGramTokenFilter(ts, EdgeNGramTokenFilter.Side.FRONT, 2, 3);
    // The first token "a" will not be output, since it's smaller than the mingram size of 2.
    // The second token on input to EdgeNGramTokenFilter will have position increment of 0,
    // which should be increased to 1, since this is the first output token in the stream.
    assertTokenStreamContents(filter,
        new String[] { "ab", "abc" },
        new int[]    {    2,     2 },
        new int[]    {    4,     5 },
        new int[]    {    1,     0 }
    );
  }

  public void testTokenizerPositions() throws Exception {
    EdgeNGramTokenizer tokenizer = new EdgeNGramTokenizer(new StringReader("abcde"), EdgeNGramTokenizer.Side.FRONT, 1, 3);
    assertTokenStreamContents(tokenizer,
                              new String[]{"a","ab","abc"},
                              new int[]{0,0,0},
                              new int[]{1,2,3},
                              null,
                              new int[]{1,0,0},
                              null,
                              null,
                              false);
  }
  
  public void testSmallTokenInStream() throws Exception {
    input = new MockTokenizer(new StringReader("abc de fgh"), MockTokenizer.WHITESPACE, false);
    EdgeNGramTokenFilter tokenizer = new EdgeNGramTokenFilter(input, EdgeNGramTokenFilter.Side.FRONT, 3, 3);
    assertTokenStreamContents(tokenizer, new String[]{"abc","fgh"}, new int[]{0,7}, new int[]{3,10});
  }
  
  public void testReset() throws Exception {
    WhitespaceTokenizer tokenizer = new WhitespaceTokenizer(TEST_VERSION_CURRENT, new StringReader("abcde"));
    EdgeNGramTokenFilter filter = new EdgeNGramTokenFilter(tokenizer, EdgeNGramTokenFilter.Side.FRONT, 1, 3);
    assertTokenStreamContents(filter, new String[]{"a","ab","abc"}, new int[]{0,0,0}, new int[]{1,2,3});
    tokenizer.setReader(new StringReader("abcde"));
    assertTokenStreamContents(filter, new String[]{"a","ab","abc"}, new int[]{0,0,0}, new int[]{1,2,3});
  }
  
  // LUCENE-3642
  // EdgeNgram blindly adds term length to offset, but this can take things out of bounds
  // wrt original text if a previous filter increases the length of the word (in this case æ -> ae)
  // so in this case we behave like WDF, and preserve any modified offsets
  public void testInvalidOffsets() throws Exception {
    Analyzer analyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer tokenizer = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
        TokenFilter filters = new ASCIIFoldingFilter(tokenizer);
        filters = new EdgeNGramTokenFilter(filters, EdgeNGramTokenFilter.Side.FRONT, 2, 15);
        return new TokenStreamComponents(tokenizer, filters);
      }
    };
    assertAnalyzesTo(analyzer, "mosfellsbær",
        new String[] { "mo", "mos", "mosf", "mosfe", "mosfel", "mosfell", "mosfells", "mosfellsb", "mosfellsba", "mosfellsbae", "mosfellsbaer" },
        new int[]    {    0,     0,      0,       0,        0,         0,          0,           0,            0,             0,              0 },
        new int[]    {   11,    11,     11,      11,       11,        11,         11,          11,           11,            11,             11 });
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer tokenizer = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
        return new TokenStreamComponents(tokenizer, 
            new EdgeNGramTokenFilter(tokenizer, EdgeNGramTokenFilter.Side.FRONT, 2, 4));
      }    
    };
    checkRandomData(random(), a, 1000*RANDOM_MULTIPLIER);
    
    Analyzer b = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer tokenizer = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
        return new TokenStreamComponents(tokenizer, 
            new EdgeNGramTokenFilter(tokenizer, EdgeNGramTokenFilter.Side.BACK, 2, 4));
      }    
    };
    checkRandomData(random(), b, 1000*RANDOM_MULTIPLIER, 20, false, false);
  }
  
  public void testEmptyTerm() throws Exception {
    Random random = random();
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer tokenizer = new KeywordTokenizer(reader);
        return new TokenStreamComponents(tokenizer, 
            new EdgeNGramTokenFilter(tokenizer, EdgeNGramTokenFilter.Side.FRONT, 2, 15));
      }    
    };
    checkAnalysisConsistency(random, a, random.nextBoolean(), "");
    
    Analyzer b = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer tokenizer = new KeywordTokenizer(reader);
        return new TokenStreamComponents(tokenizer, 
            new EdgeNGramTokenFilter(tokenizer, EdgeNGramTokenFilter.Side.BACK, 2, 15));
      }    
    };
    checkAnalysisConsistency(random, b, random.nextBoolean(), "");
  }
}
