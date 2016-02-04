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
package org.apache.lucene.analysis.util;


import java.io.Reader;
import java.io.StringReader;

import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;

/**
 * Simple tests to ensure the French elision filter factory is working.
 */
public class TestElisionFilterFactory extends BaseTokenStreamFactoryTestCase {
  /**
   * Ensure the filter actually normalizes text.
   */
  public void testElision() throws Exception {
    Reader reader = new StringReader("l'avion");
    TokenStream stream = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    ((Tokenizer)stream).setReader(reader);
    stream = tokenFilterFactory("Elision", "articles", "frenchArticles.txt").create(stream);
    assertTokenStreamContents(stream, new String[] { "avion" });
  }
  
  /**
   * Test creating an elision filter without specifying any articles
   */
  public void testDefaultArticles() throws Exception {
    Reader reader = new StringReader("l'avion");
    TokenStream stream = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    ((Tokenizer)stream).setReader(reader);
    stream = tokenFilterFactory("Elision").create(stream);
    assertTokenStreamContents(stream, new String[] { "avion" });
  }
  
  /**
   * Test setting ignoreCase=true
   */
  public void testCaseInsensitive() throws Exception {
    Reader reader = new StringReader("L'avion");
    TokenStream stream = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    ((Tokenizer)stream).setReader(reader);
    stream = tokenFilterFactory("Elision",
        "articles", "frenchArticles.txt",
        "ignoreCase", "true").create(stream);
    assertTokenStreamContents(stream, new String[] { "avion" });
  }
  
  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    try {
      tokenFilterFactory("Elision", "bogusArg", "bogusValue");
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("Unknown parameters"));
    }
  }
}
