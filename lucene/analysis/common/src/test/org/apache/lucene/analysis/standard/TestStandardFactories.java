package org.apache.lucene.analysis.standard;

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
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.util.BaseTokenStreamFactoryTestCase;

/**
 * Simple tests to ensure the standard lucene factories are working.
 */
public class TestStandardFactories extends BaseTokenStreamFactoryTestCase {
  /**
   * Test StandardTokenizerFactory
   */
  public void testStandardTokenizer() throws Exception {
    Reader reader = new StringReader("Wha\u0301t's this thing do?");
    TokenStream stream = tokenizerFactory("Standard").create(reader);
    assertTokenStreamContents(stream, 
        new String[] { "Wha\u0301t's", "this", "thing", "do" });
  }
  
  public void testStandardTokenizerMaxTokenLength() throws Exception {
    StringBuilder builder = new StringBuilder();
    for (int i = 0 ; i < 100 ; ++i) {
      builder.append("abcdefg"); // 7 * 100 = 700 char "word"
    }
    String longWord = builder.toString();
    String content = "one two three " + longWord + " four five six";
    Reader reader = new StringReader(content);
    Tokenizer stream = tokenizerFactory("Standard",
        "maxTokenLength", "1000").create(reader);
    assertTokenStreamContents(stream, 
        new String[] { "one", "two", "three", longWord, "four", "five", "six" });
  }
  
  /**
   * Test ClassicTokenizerFactory
   */
  public void testClassicTokenizer() throws Exception {
    Reader reader = new StringReader("What's this thing do?");
    TokenStream stream = tokenizerFactory("Classic").create(reader);
    assertTokenStreamContents(stream, 
        new String[] { "What's", "this", "thing", "do" });
  }
  
  public void testClassicTokenizerMaxTokenLength() throws Exception {
    StringBuilder builder = new StringBuilder();
    for (int i = 0 ; i < 100 ; ++i) {
      builder.append("abcdefg"); // 7 * 100 = 700 char "word"
    }
    String longWord = builder.toString();
    String content = "one two three " + longWord + " four five six";
    Reader reader = new StringReader(content);
    Tokenizer stream = tokenizerFactory("Classic",
        "maxTokenLength", "1000").create(reader);
    assertTokenStreamContents(stream, 
        new String[] { "one", "two", "three", longWord, "four", "five", "six" });
  }
  
  /**
   * Test ClassicFilterFactory
   */
  public void testStandardFilter() throws Exception {
    Reader reader = new StringReader("What's this thing do?");
    TokenStream stream = tokenizerFactory("Classic").create(reader);
    stream = tokenFilterFactory("Classic").create(stream);
    assertTokenStreamContents(stream, 
        new String[] { "What", "this", "thing", "do" });
  }
  
  /**
   * Test KeywordTokenizerFactory
   */
  public void testKeywordTokenizer() throws Exception {
    Reader reader = new StringReader("What's this thing do?");
    TokenStream stream = tokenizerFactory("Keyword").create(reader);
    assertTokenStreamContents(stream, 
        new String[] { "What's this thing do?" });
  }
  
  /**
   * Test WhitespaceTokenizerFactory
   */
  public void testWhitespaceTokenizer() throws Exception {
    Reader reader = new StringReader("What's this thing do?");
    TokenStream stream = tokenizerFactory("Whitespace").create(reader);
    assertTokenStreamContents(stream, 
        new String[] { "What's", "this", "thing", "do?" });
  }
  
  /**
   * Test LetterTokenizerFactory
   */
  public void testLetterTokenizer() throws Exception {
    Reader reader = new StringReader("What's this thing do?");
    TokenStream stream = tokenizerFactory("Letter").create(reader);
    assertTokenStreamContents(stream, 
        new String[] { "What", "s", "this", "thing", "do" });
  }
  
  /**
   * Test LowerCaseTokenizerFactory
   */
  public void testLowerCaseTokenizer() throws Exception {
    Reader reader = new StringReader("What's this thing do?");
    TokenStream stream = tokenizerFactory("LowerCase").create(reader);
    assertTokenStreamContents(stream, 
        new String[] { "what", "s", "this", "thing", "do" });
  }
  
  /**
   * Ensure the ASCIIFoldingFilterFactory works
   */
  public void testASCIIFolding() throws Exception {
    Reader reader = new StringReader("Česká");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    stream = tokenFilterFactory("ASCIIFolding").create(stream);
    assertTokenStreamContents(stream, new String[] { "Ceska" });
  }
  
  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    try {
      tokenizerFactory("Standard", "bogusArg", "bogusValue");
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("Unknown parameters"));
    }
    
    try {
      tokenizerFactory("Classic", "bogusArg", "bogusValue");
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("Unknown parameters"));
    }
    
    try {
      tokenizerFactory("Whitespace", "bogusArg", "bogusValue");
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("Unknown parameters"));
    }
    
    try {
      tokenizerFactory("Letter", "bogusArg", "bogusValue");
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("Unknown parameters"));
    }
    
    try {
      tokenizerFactory("LowerCase", "bogusArg", "bogusValue");
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("Unknown parameters"));
    }
    
    try {
      tokenFilterFactory("ASCIIFolding", "bogusArg", "bogusValue");
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("Unknown parameters"));
    }
    
    try {
      tokenFilterFactory("Standard", "bogusArg", "bogusValue");
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("Unknown parameters"));
    }
    
    try {
      tokenFilterFactory("Classic", "bogusArg", "bogusValue");
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("Unknown parameters"));
    }
  }
}
