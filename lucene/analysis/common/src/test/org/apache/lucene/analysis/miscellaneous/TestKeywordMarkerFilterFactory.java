package org.apache.lucene.analysis.miscellaneous;

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

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.analysis.util.StringMockResourceLoader;

/**
 * Simple tests to ensure the keyword marker filter factory is working.
 */
public class TestKeywordMarkerFilterFactory extends BaseTokenStreamTestCase {
  
  public void testKeywords() throws IOException {
    Reader reader = new StringReader("dogs cats");
    Tokenizer tokenizer = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    KeywordMarkerFilterFactory factory = new KeywordMarkerFilterFactory();
    Map<String,String> args = new HashMap<String,String>();
    ResourceLoader loader = new StringMockResourceLoader("cats");
    args.put("protected", "protwords.txt");
    factory.setLuceneMatchVersion(TEST_VERSION_CURRENT);
    factory.init(args);
    factory.inform(loader);
    
    TokenStream ts = new PorterStemFilter(factory.create(tokenizer));
    assertTokenStreamContents(ts, new String[] { "dog", "cats" });
    
    
    reader = new StringReader("dogs cats");
    tokenizer = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    factory = new KeywordMarkerFilterFactory();
    args = new HashMap<String,String>();
    
    args.put("pattern", "cats|Dogs");
    factory.setLuceneMatchVersion(TEST_VERSION_CURRENT);
    factory.init(args);
    factory.inform(null);
    
    ts = new PorterStemFilter(factory.create(tokenizer));
    assertTokenStreamContents(ts, new String[] { "dog", "cats" });
  }
  
  public void testKeywordsMixed() throws IOException {
    Reader reader = new StringReader("dogs cats birds");
    Tokenizer tokenizer = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    KeywordMarkerFilterFactory factory = new KeywordMarkerFilterFactory();
    Map<String,String> args = new HashMap<String,String>();
    ResourceLoader loader = new StringMockResourceLoader("cats");
    args.put("protected", "protwords.txt");
    args.put("pattern", "birds|Dogs");
    factory.setLuceneMatchVersion(TEST_VERSION_CURRENT);
    factory.init(args);
    factory.inform(loader);
    
    TokenStream ts = new PorterStemFilter(factory.create(tokenizer));
    assertTokenStreamContents(ts, new String[] { "dog", "cats", "birds" });
  }
  
  public void testKeywordsCaseInsensitive() throws IOException {
    Reader reader = new StringReader("dogs cats Cats");
    Tokenizer tokenizer = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    KeywordMarkerFilterFactory factory = new KeywordMarkerFilterFactory();
    Map<String,String> args = new HashMap<String,String>();
    ResourceLoader loader = new StringMockResourceLoader("cats");
    args.put("protected", "protwords.txt");
    args.put("ignoreCase", "true");
    factory.setLuceneMatchVersion(TEST_VERSION_CURRENT);
    factory.init(args);
    factory.inform(loader);
    
    TokenStream ts = new PorterStemFilter(factory.create(tokenizer));
    assertTokenStreamContents(ts, new String[] { "dog", "cats", "Cats" });
    
    reader = new StringReader("dogs cats Cats");
    tokenizer = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    factory = new KeywordMarkerFilterFactory();
    args = new HashMap<String,String>();
    
    args.put("pattern", "Cats");
    args.put("ignoreCase", "true");
    factory.setLuceneMatchVersion(TEST_VERSION_CURRENT);
    factory.init(args);
    factory.inform(null);
    
    ts = new PorterStemFilter(factory.create(tokenizer));
    assertTokenStreamContents(ts, new String[] { "dog", "cats", "Cats" });
  }
  
  public void testKeywordsCaseInsensitiveMixed() throws IOException {
    Reader reader = new StringReader("dogs cats Cats Birds birds");
    Tokenizer tokenizer = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    KeywordMarkerFilterFactory factory = new KeywordMarkerFilterFactory();
    Map<String,String> args = new HashMap<String,String>();
    ResourceLoader loader = new StringMockResourceLoader("cats");
    args.put("protected", "protwords.txt");
    args.put("pattern", "birds");
    args.put("ignoreCase", "true");
    factory.setLuceneMatchVersion(TEST_VERSION_CURRENT);
    factory.init(args);
    factory.inform(loader);
    
    TokenStream ts = new PorterStemFilter(factory.create(tokenizer));
    assertTokenStreamContents(ts, new String[] { "dog", "cats", "Cats", "Birds", "birds" });
  }
}
