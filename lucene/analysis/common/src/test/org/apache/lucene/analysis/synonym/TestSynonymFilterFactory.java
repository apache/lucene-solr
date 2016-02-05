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
package org.apache.lucene.analysis.synonym;


import java.io.Reader;
import java.io.StringReader;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.pattern.PatternTokenizerFactory;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.analysis.util.BaseTokenStreamFactoryTestCase;
import org.apache.lucene.analysis.util.StringMockResourceLoader;
import org.apache.lucene.analysis.cjk.CJKAnalyzer;
import org.apache.lucene.util.Version;

public class TestSynonymFilterFactory extends BaseTokenStreamFactoryTestCase {

  /** checks for synonyms of "GB" in synonyms.txt */
  private void checkSolrSynonyms(TokenFilterFactory factory) throws Exception {
    Reader reader = new StringReader("GB");
    TokenStream stream = whitespaceMockTokenizer(reader);
    stream = factory.create(stream);
    assertTrue(stream instanceof SynonymFilter);
    assertTokenStreamContents(stream,
        new String[] { "GB", "gib", "gigabyte", "gigabytes" },
        new int[] { 1, 0, 0, 0 });
  }

  /** checks for synonyms of "second" in synonyms-wordnet.txt */
  private void checkWordnetSynonyms(TokenFilterFactory factory) throws Exception {
    Reader reader = new StringReader("second");
    TokenStream stream = whitespaceMockTokenizer(reader);
    stream = factory.create(stream);
    assertTrue(stream instanceof SynonymFilter);
    assertTokenStreamContents(stream,
        new String[] { "second", "2nd", "two" },
        new int[] { 1, 0, 0 });
  }

  /** test that we can parse and use the solr syn file */
  public void testSynonyms() throws Exception {
    checkSolrSynonyms(tokenFilterFactory("Synonym", "synonyms", "synonyms.txt"));
  }
  
  /** if the synonyms are completely empty, test that we still analyze correctly */
  public void testEmptySynonyms() throws Exception {
    Reader reader = new StringReader("GB");
    TokenStream stream = whitespaceMockTokenizer(reader);
    stream = tokenFilterFactory("Synonym", Version.LATEST,
        new StringMockResourceLoader(""), // empty file!
        "synonyms", "synonyms.txt").create(stream);
    assertTokenStreamContents(stream, new String[] { "GB" });
  }

  public void testFormat() throws Exception {
    checkSolrSynonyms(tokenFilterFactory("Synonym", "synonyms", "synonyms.txt", "format", "solr"));
    checkWordnetSynonyms(tokenFilterFactory("Synonym", "synonyms", "synonyms-wordnet.txt", "format", "wordnet"));
    // explicit class should work the same as the "solr" alias
    checkSolrSynonyms(tokenFilterFactory("Synonym", "synonyms", "synonyms.txt",
        "format", SolrSynonymParser.class.getName()));
  }
  
  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    try {
      tokenFilterFactory("Synonym", 
          "synonyms", "synonyms.txt", 
          "bogusArg", "bogusValue");
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("Unknown parameters"));
    }
  }

  /** Test that analyzer and tokenizerFactory is both specified */
  public void testAnalyzer() throws Exception {
    final String analyzer = CJKAnalyzer.class.getName();
    final String tokenizerFactory = PatternTokenizerFactory.class.getName();
    TokenFilterFactory factory = null;

    factory = tokenFilterFactory("Synonym",
        "synonyms", "synonyms2.txt",
        "analyzer", analyzer);
    assertNotNull(factory);

    try {
      tokenFilterFactory("Synonym",
          "synonyms", "synonyms.txt",
          "analyzer", analyzer,
          "tokenizerFactory", tokenizerFactory);
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("Analyzer and TokenizerFactory can't be specified both"));
    }
  }

  static final String TOK_SYN_ARG_VAL = "argument";
  static final String TOK_FOO_ARG_VAL = "foofoofoo";

  /** Test that we can parse TokenierFactory's arguments */
  public void testTokenizerFactoryArguments() throws Exception {
    final String clazz = PatternTokenizerFactory.class.getName();
    TokenFilterFactory factory = null;

    // simple arg form
    factory = tokenFilterFactory("Synonym", 
        "synonyms", "synonyms.txt", 
        "tokenizerFactory", clazz,
        "pattern", "(.*)",
        "group", "0");
    assertNotNull(factory);
    // prefix
    factory = tokenFilterFactory("Synonym", 
        "synonyms", "synonyms.txt", 
        "tokenizerFactory", clazz,
        "tokenizerFactory.pattern", "(.*)",
        "tokenizerFactory.group", "0");
    assertNotNull(factory);

    // sanity check that sub-PatternTokenizerFactory fails w/o pattern
    try {
      factory = tokenFilterFactory("Synonym", 
          "synonyms", "synonyms.txt", 
          "tokenizerFactory", clazz);
      fail("tokenizerFactory should have complained about missing pattern arg");
    } catch (Exception expected) {
      // :NOOP:
    }

    // sanity check that sub-PatternTokenizerFactory fails on unexpected
    try {
      factory = tokenFilterFactory("Synonym", 
          "synonyms", "synonyms.txt", 
          "tokenizerFactory", clazz,
          "tokenizerFactory.pattern", "(.*)",
          "tokenizerFactory.bogusbogusbogus", "bogus",
          "tokenizerFactory.group", "0");
      fail("tokenizerFactory should have complained about missing pattern arg");
    } catch (Exception expected) {
      // :NOOP:
    }
  }


}


