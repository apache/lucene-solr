package org.apache.solr.analysis;

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

import java.io.Reader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;

/**
 * Simple tests to ensure the NGram filter factories are working.
 */
public class TestNGramFilters extends BaseTokenTestCase {
  /**
   * Test NGramTokenizerFactory
   */
  public void testNGramTokenizer() throws Exception {
    Reader reader = new StringReader("test");
    Map<String,String> args = new HashMap<String,String>();
    NGramTokenizerFactory factory = new NGramTokenizerFactory();
    factory.init(args);
    Tokenizer stream = factory.create(reader);
    assertTokenStreamContents(stream, 
        new String[] { "t", "e", "s", "t", "te", "es", "st" });
  }
  /**
   * Test NGramTokenizerFactory with min and max gram options
   */
  public void testNGramTokenizer2() throws Exception {
    Reader reader = new StringReader("test");
    Map<String,String> args = new HashMap<String,String>();
    args.put("minGramSize", "2");
    args.put("maxGramSize", "3");
    NGramTokenizerFactory factory = new NGramTokenizerFactory();
    factory.init(args);
    Tokenizer stream = factory.create(reader);
    assertTokenStreamContents(stream, 
        new String[] { "te", "es", "st", "tes", "est" });
  }
  /**
   * Test the NGramFilterFactory
   */
  public void testNGramFilter() throws Exception {
    Reader reader = new StringReader("test");
    Map<String,String> args = new HashMap<String,String>();
    NGramFilterFactory factory = new NGramFilterFactory();
    factory.init(args);
    TokenStream stream = factory.create(new WhitespaceTokenizer(DEFAULT_VERSION, reader));
    assertTokenStreamContents(stream, 
        new String[] { "t", "e", "s", "t", "te", "es", "st" });
  }
  /**
   * Test the NGramFilterFactory with min and max gram options
   */
  public void testNGramFilter2() throws Exception {
    Reader reader = new StringReader("test");
    Map<String,String> args = new HashMap<String,String>();
    args.put("minGramSize", "2");
    args.put("maxGramSize", "3");
    NGramFilterFactory factory = new NGramFilterFactory();
    factory.init(args);
    TokenStream stream = factory.create(new WhitespaceTokenizer(DEFAULT_VERSION, reader));
    assertTokenStreamContents(stream, 
        new String[] { "te", "es", "st", "tes", "est" });
  }
  /**
   * Test EdgeNGramTokenizerFactory
   */
  public void testEdgeNGramTokenizer() throws Exception {
    Reader reader = new StringReader("test");
    Map<String,String> args = new HashMap<String,String>();
    EdgeNGramTokenizerFactory factory = new EdgeNGramTokenizerFactory();
    factory.init(args);
    Tokenizer stream = factory.create(reader);
    assertTokenStreamContents(stream, 
        new String[] { "t" });
  }
  /**
   * Test EdgeNGramTokenizerFactory with min and max gram size
   */
  public void testEdgeNGramTokenizer2() throws Exception {
    Reader reader = new StringReader("test");
    Map<String,String> args = new HashMap<String,String>();
    args.put("minGramSize", "1");
    args.put("maxGramSize", "2");
    EdgeNGramTokenizerFactory factory = new EdgeNGramTokenizerFactory();
    factory.init(args);
    Tokenizer stream = factory.create(reader);
    assertTokenStreamContents(stream, 
        new String[] { "t", "te" });
  }
  /**
   * Test EdgeNGramTokenizerFactory with side option
   */
  public void testEdgeNGramTokenizer3() throws Exception {
    Reader reader = new StringReader("ready");
    Map<String,String> args = new HashMap<String,String>();
    args.put("side", "back");
    EdgeNGramTokenizerFactory factory = new EdgeNGramTokenizerFactory();
    factory.init(args);
    Tokenizer stream = factory.create(reader);
    assertTokenStreamContents(stream, 
        new String[] { "y" });
  }
  /**
   * Test EdgeNGramFilterFactory
   */
  public void testEdgeNGramFilter() throws Exception {
    Reader reader = new StringReader("test");
    Map<String,String> args = new HashMap<String,String>();
    EdgeNGramFilterFactory factory = new EdgeNGramFilterFactory();
    factory.init(args);
    TokenStream stream = factory.create(new WhitespaceTokenizer(DEFAULT_VERSION, reader));
    assertTokenStreamContents(stream, 
        new String[] { "t" });
  }
  /**
   * Test EdgeNGramFilterFactory with min and max gram size
   */
  public void testEdgeNGramFilter2() throws Exception {
    Reader reader = new StringReader("test");
    Map<String,String> args = new HashMap<String,String>();
    args.put("minGramSize", "1");
    args.put("maxGramSize", "2");
    EdgeNGramFilterFactory factory = new EdgeNGramFilterFactory();
    factory.init(args);
    TokenStream stream = factory.create(new WhitespaceTokenizer(DEFAULT_VERSION, reader));
    assertTokenStreamContents(stream, 
        new String[] { "t", "te" });
  }
  /**
   * Test EdgeNGramFilterFactory with side option
   */
  public void testEdgeNGramFilter3() throws Exception {
    Reader reader = new StringReader("ready");
    Map<String,String> args = new HashMap<String,String>();
    args.put("side", "back");
    EdgeNGramFilterFactory factory = new EdgeNGramFilterFactory();
    factory.init(args);
    TokenStream stream = factory.create(new WhitespaceTokenizer(DEFAULT_VERSION, reader));
    assertTokenStreamContents(stream, 
        new String[] { "y" });
  }
}
