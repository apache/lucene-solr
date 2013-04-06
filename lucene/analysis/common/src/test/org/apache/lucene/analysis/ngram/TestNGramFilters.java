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

import java.io.Reader;
import java.io.StringReader;

import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.BaseTokenStreamFactoryTestCase;

/**
 * Simple tests to ensure the NGram filter factories are working.
 */
public class TestNGramFilters extends BaseTokenStreamFactoryTestCase {
  /**
   * Test NGramTokenizerFactory
   */
  public void testNGramTokenizer() throws Exception {
    Reader reader = new StringReader("test");
    TokenStream stream = tokenizerFactory("NGram").create(reader);
    assertTokenStreamContents(stream, 
        new String[] { "t", "e", "s", "t", "te", "es", "st" });
  }

  /**
   * Test NGramTokenizerFactory with min and max gram options
   */
  public void testNGramTokenizer2() throws Exception {
    Reader reader = new StringReader("test");
    TokenStream stream = tokenizerFactory("NGram",
        "minGramSize", "2",
        "maxGramSize", "3").create(reader);
    assertTokenStreamContents(stream, 
        new String[] { "te", "es", "st", "tes", "est" });
  }

  /**
   * Test the NGramFilterFactory
   */
  public void testNGramFilter() throws Exception {
    Reader reader = new StringReader("test");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    stream = tokenFilterFactory("NGram").create(stream);
    assertTokenStreamContents(stream, 
        new String[] { "t", "e", "s", "t", "te", "es", "st" });
  }

  /**
   * Test the NGramFilterFactory with min and max gram options
   */
  public void testNGramFilter2() throws Exception {
    Reader reader = new StringReader("test");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    stream = tokenFilterFactory("NGram",
        "minGramSize", "2",
        "maxGramSize", "3").create(stream);
    assertTokenStreamContents(stream, 
        new String[] { "te", "es", "st", "tes", "est" });
  }

  /**
   * Test EdgeNGramTokenizerFactory
   */
  public void testEdgeNGramTokenizer() throws Exception {
    Reader reader = new StringReader("test");
    TokenStream stream = tokenizerFactory("EdgeNGram").create(reader);
    assertTokenStreamContents(stream, 
        new String[] { "t" });
  }

  /**
   * Test EdgeNGramTokenizerFactory with min and max gram size
   */
  public void testEdgeNGramTokenizer2() throws Exception {
    Reader reader = new StringReader("test");
    TokenStream stream = tokenizerFactory("EdgeNGram",
        "minGramSize", "1",
        "maxGramSize", "2").create(reader);
    assertTokenStreamContents(stream, 
        new String[] { "t", "te" });
  }

  /**
   * Test EdgeNGramTokenizerFactory with side option
   */
  public void testEdgeNGramTokenizer3() throws Exception {
    Reader reader = new StringReader("ready");
    TokenStream stream = tokenizerFactory("EdgeNGram",
        "side", "back").create(reader);
    assertTokenStreamContents(stream, 
        new String[] { "y" });
  }

  /**
   * Test EdgeNGramFilterFactory
   */
  public void testEdgeNGramFilter() throws Exception {
    Reader reader = new StringReader("test");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    stream = tokenFilterFactory("EdgeNGram").create(stream);
    assertTokenStreamContents(stream, 
        new String[] { "t" });
  }

  /**
   * Test EdgeNGramFilterFactory with min and max gram size
   */
  public void testEdgeNGramFilter2() throws Exception {
    Reader reader = new StringReader("test");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    stream = tokenFilterFactory("EdgeNGram",
        "minGramSize", "1",
        "maxGramSize", "2").create(stream);
    assertTokenStreamContents(stream, 
        new String[] { "t", "te" });
  }

  /**
   * Test EdgeNGramFilterFactory with side option
   */
  public void testEdgeNGramFilter3() throws Exception {
    Reader reader = new StringReader("ready");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    stream = tokenFilterFactory("EdgeNGram",
        "side", "back").create(stream);
    assertTokenStreamContents(stream, 
        new String[] { "y" });
  }
  
  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    try {
      tokenizerFactory("NGram", "bogusArg", "bogusValue");
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("Unknown parameters"));
    }
    
    try {
      tokenizerFactory("EdgeNGram", "bogusArg", "bogusValue");
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("Unknown parameters"));
    }
    
    try {
      tokenFilterFactory("NGram", "bogusArg", "bogusValue");
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("Unknown parameters"));
    }
    
    try {
      tokenFilterFactory("EdgeNGram", "bogusArg", "bogusValue");
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("Unknown parameters"));
    }
  }
}
