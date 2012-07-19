package org.apache.solr.analysis;

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
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.solr.core.SolrResourceLoader;

/**
 * Simple tests to ensure the French elision filter factory is working.
 */
public class TestElisionFilterFactory extends BaseTokenStreamTestCase {
  /**
   * Ensure the filter actually normalizes text.
   */
  public void testElision() throws Exception {
    Reader reader = new StringReader("l'avion");
    Tokenizer tokenizer = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    ElisionFilterFactory factory = new ElisionFilterFactory();
    factory.setLuceneMatchVersion(TEST_VERSION_CURRENT);
    ResourceLoader loader = new SolrResourceLoader("solr/collection1");
    Map<String,String> args = new HashMap<String,String>();
    args.put("articles", "frenchArticles.txt");
    factory.init(args);
    factory.inform(loader);
    TokenStream stream = factory.create(tokenizer);
    assertTokenStreamContents(stream, new String[] { "avion" });
  }
  
  /**
   * Test creating an elision filter without specifying any articles
   */
  public void testDefaultArticles() throws Exception {
    Reader reader = new StringReader("l'avion");
    Tokenizer tokenizer = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    ElisionFilterFactory factory = new ElisionFilterFactory();
    factory.setLuceneMatchVersion(TEST_VERSION_CURRENT);
    Map<String, String> args = Collections.emptyMap();
    factory.init(args);
    ResourceLoader loader = new SolrResourceLoader("solr/collection1");
    factory.inform(loader);
    TokenStream stream = factory.create(tokenizer);
    assertTokenStreamContents(stream, new String[] { "avion" });
  }
  
  /**
   * Test setting ignoreCase=true
   */
  public void testCaseInsensitive() throws Exception {
    Reader reader = new StringReader("L'avion");
    Tokenizer tokenizer = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    ElisionFilterFactory factory = new ElisionFilterFactory();
    factory.setLuceneMatchVersion(TEST_VERSION_CURRENT);
    ResourceLoader loader = new SolrResourceLoader("solr/collection1");
    Map<String,String> args = new HashMap<String,String>();
    args.put("articles", "frenchArticles.txt");
    args.put("ignoreCase", "true");
    factory.init(args);
    factory.inform(loader);
    TokenStream stream = factory.create(tokenizer);
    assertTokenStreamContents(stream, new String[] { "avion" });
  }
  
}
