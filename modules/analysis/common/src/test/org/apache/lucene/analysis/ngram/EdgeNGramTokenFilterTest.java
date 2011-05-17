package org.apache.lucene.analysis.ngram;

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

import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;

import java.io.StringReader;

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
    assertTokenStreamContents(tokenizer, new String[]{"e","de","cde"}, new int[]{4,3,2}, new int[]{5,5,5});
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
    tokenizer.reset(new StringReader("abcde"));
    assertTokenStreamContents(filter, new String[]{"a","ab","abc"}, new int[]{0,0,0}, new int[]{1,2,3});
  }
}
