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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizerFactory;
import org.apache.lucene.analysis.core.LetterTokenizerFactory;
import org.apache.lucene.analysis.core.LowerCaseTokenizerFactory;
import org.apache.lucene.analysis.core.WhitespaceTokenizerFactory;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilterFactory;

/**
 * Simple tests to ensure the standard lucene factories are working.
 */
public class TestStandardFactories extends BaseTokenStreamTestCase {
  /**
   * Test StandardTokenizerFactory
   */
  public void testStandardTokenizer() throws Exception {
    Reader reader = new StringReader("Wha\u0301t's this thing do?");
    StandardTokenizerFactory factory = new StandardTokenizerFactory();
    factory.setLuceneMatchVersion(TEST_VERSION_CURRENT);
    Map<String, String> args = Collections.emptyMap();
    factory.init(args);
    Tokenizer stream = factory.create(reader);
    assertTokenStreamContents(stream, 
        new String[] {"Wha\u0301t's", "this", "thing", "do" });
  }
  
  public void testStandardTokenizerMaxTokenLength() throws Exception {
    StringBuilder builder = new StringBuilder();
    for (int i = 0 ; i < 100 ; ++i) {
      builder.append("abcdefg"); // 7 * 100 = 700 char "word"
    }
    String longWord = builder.toString();
    String content = "one two three " + longWord + " four five six";
    Reader reader = new StringReader(content);
    Map<String,String> args = new HashMap<String,String>();
    args.put("maxTokenLength", "1000");
    StandardTokenizerFactory factory = new StandardTokenizerFactory();
    factory.setLuceneMatchVersion(TEST_VERSION_CURRENT);
    factory.init(args);
    Tokenizer stream = factory.create(reader);
    assertTokenStreamContents(stream, 
        new String[] {"one", "two", "three", longWord, "four", "five", "six" });
  }
  
  /**
   * Test ClassicTokenizerFactory
   */
  public void testClassicTokenizer() throws Exception {
    Reader reader = new StringReader("What's this thing do?");
    ClassicTokenizerFactory factory = new ClassicTokenizerFactory();
    factory.setLuceneMatchVersion(TEST_VERSION_CURRENT);
    Map<String, String> args = Collections.emptyMap();
    factory.init(args);
    Tokenizer stream = factory.create(reader);
    assertTokenStreamContents(stream, 
        new String[] {"What's", "this", "thing", "do" });
  }
  
  public void testClassicTokenizerMaxTokenLength() throws Exception {
    StringBuilder builder = new StringBuilder();
    for (int i = 0 ; i < 100 ; ++i) {
      builder.append("abcdefg"); // 7 * 100 = 700 char "word"
    }
    String longWord = builder.toString();
    String content = "one two three " + longWord + " four five six";
    Reader reader = new StringReader(content);
    Map<String,String> args = new HashMap<String,String>();
    args.put("maxTokenLength", "1000");
    ClassicTokenizerFactory factory = new ClassicTokenizerFactory();
    factory.setLuceneMatchVersion(TEST_VERSION_CURRENT);
    factory.init(args);
    Tokenizer stream = factory.create(reader);
    assertTokenStreamContents(stream, 
        new String[] {"one", "two", "three", longWord, "four", "five", "six" });
  }
  
  /**
   * Test ClassicFilterFactory
   */
  public void testStandardFilter() throws Exception {
    Reader reader = new StringReader("What's this thing do?");
    ClassicTokenizerFactory factory = new ClassicTokenizerFactory();
    factory.setLuceneMatchVersion(TEST_VERSION_CURRENT);
    Map<String, String> args = Collections.emptyMap();
    factory.init(args);
    ClassicFilterFactory filterFactory = new ClassicFilterFactory();
    filterFactory.setLuceneMatchVersion(TEST_VERSION_CURRENT);
    filterFactory.init(args);
    Tokenizer tokenizer = factory.create(reader);
    TokenStream stream = filterFactory.create(tokenizer);
    assertTokenStreamContents(stream, 
        new String[] {"What", "this", "thing", "do"});
  }
  
  /**
   * Test KeywordTokenizerFactory
   */
  public void testKeywordTokenizer() throws Exception {
    Reader reader = new StringReader("What's this thing do?");
    KeywordTokenizerFactory factory = new KeywordTokenizerFactory();
    factory.setLuceneMatchVersion(TEST_VERSION_CURRENT);
    Map<String, String> args = Collections.emptyMap();
    factory.init(args);
    Tokenizer stream = factory.create(reader);
    assertTokenStreamContents(stream, 
        new String[] {"What's this thing do?"});
  }
  
  /**
   * Test WhitespaceTokenizerFactory
   */
  public void testWhitespaceTokenizer() throws Exception {
    Reader reader = new StringReader("What's this thing do?");
    WhitespaceTokenizerFactory factory = new WhitespaceTokenizerFactory();
    factory.setLuceneMatchVersion(TEST_VERSION_CURRENT);
    Map<String, String> args = Collections.emptyMap();
    factory.init(args);
    Tokenizer stream = factory.create(reader);
    assertTokenStreamContents(stream, 
        new String[] {"What's", "this", "thing", "do?"});
  }
  
  /**
   * Test LetterTokenizerFactory
   */
  public void testLetterTokenizer() throws Exception {
    Reader reader = new StringReader("What's this thing do?");
    LetterTokenizerFactory factory = new LetterTokenizerFactory();
    factory.setLuceneMatchVersion(TEST_VERSION_CURRENT);
    Map<String, String> args = Collections.emptyMap();
    factory.init(args);
    Tokenizer stream = factory.create(reader);
    assertTokenStreamContents(stream, 
        new String[] {"What", "s", "this", "thing", "do"});
  }
  
  /**
   * Test LowerCaseTokenizerFactory
   */
  public void testLowerCaseTokenizer() throws Exception {
    Reader reader = new StringReader("What's this thing do?");
    LowerCaseTokenizerFactory factory = new LowerCaseTokenizerFactory();
    factory.setLuceneMatchVersion(TEST_VERSION_CURRENT);
    Map<String, String> args = Collections.emptyMap();
    factory.init(args);
    Tokenizer stream = factory.create(reader);
    assertTokenStreamContents(stream, 
        new String[] {"what", "s", "this", "thing", "do"});
  }
  
  /**
   * Ensure the ASCIIFoldingFilterFactory works
   */
  public void testASCIIFolding() throws Exception {
    Reader reader = new StringReader("Česká");
    Tokenizer tokenizer = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    ASCIIFoldingFilterFactory factory = new ASCIIFoldingFilterFactory();
    factory.setLuceneMatchVersion(TEST_VERSION_CURRENT);
    Map<String, String> args = Collections.emptyMap();
    factory.init(args);
    TokenStream stream = factory.create(tokenizer);
    assertTokenStreamContents(stream, new String[] { "Ceska" });
  }
}
