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

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WhitespaceTokenizer;

import java.io.StringReader;

import junit.framework.TestCase;

/**
 * Tests {@link EdgeNGramTokenFilter} for correctness.
 */
public class EdgeNGramTokenFilterTest extends TestCase {
  private TokenStream input;

  public void setUp() {
    input = new WhitespaceTokenizer(new StringReader("abcde"));
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
    final Token reusableToken = new Token();
    Token nextToken = tokenizer.next(reusableToken);
    assertEquals("(a,0,1)", nextToken.toString());
    assertNull(tokenizer.next(reusableToken));
  }

  public void testBackUnigram() throws Exception {
    EdgeNGramTokenFilter tokenizer = new EdgeNGramTokenFilter(input, EdgeNGramTokenFilter.Side.BACK, 1, 1);
    final Token reusableToken = new Token();
    Token nextToken = tokenizer.next(reusableToken);
    assertEquals("(e,4,5)", nextToken.toString());
    assertNull(tokenizer.next(reusableToken));
  }

  public void testOversizedNgrams() throws Exception {
    EdgeNGramTokenFilter tokenizer = new EdgeNGramTokenFilter(input, EdgeNGramTokenFilter.Side.FRONT, 6, 6);
    assertNull(tokenizer.next(new Token()));
  }

  public void testFrontRangeOfNgrams() throws Exception {
    EdgeNGramTokenFilter tokenizer = new EdgeNGramTokenFilter(input, EdgeNGramTokenFilter.Side.FRONT, 1, 3);
    final Token reusableToken = new Token();
    Token nextToken = tokenizer.next(reusableToken);
    assertEquals("(a,0,1)", nextToken.toString());
    nextToken = tokenizer.next(reusableToken);
    assertEquals("(ab,0,2)", nextToken.toString());
    nextToken = tokenizer.next(reusableToken);
    assertEquals("(abc,0,3)", nextToken.toString());
    assertNull(tokenizer.next(reusableToken));
  }

  public void testBackRangeOfNgrams() throws Exception {
    EdgeNGramTokenFilter tokenizer = new EdgeNGramTokenFilter(input, EdgeNGramTokenFilter.Side.BACK, 1, 3);
    final Token reusableToken = new Token();
    Token nextToken = tokenizer.next(reusableToken);
    assertEquals("(e,4,5)", nextToken.toString());
    nextToken = tokenizer.next(reusableToken);
    assertEquals("(de,3,5)", nextToken.toString());
    nextToken = tokenizer.next(reusableToken);
    assertEquals("(cde,2,5)", nextToken.toString());
    assertNull(tokenizer.next(reusableToken));
  }
}
