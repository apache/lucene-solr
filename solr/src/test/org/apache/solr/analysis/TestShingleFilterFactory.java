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
import org.apache.lucene.analysis.WhitespaceTokenizer;

/**
 * Simple tests to ensure the Shingle filter factory works.
 */
public class TestShingleFilterFactory extends BaseTokenTestCase { 
  /**
   * Test the defaults
   */
  public void testDefaults() throws Exception {
    Reader reader = new StringReader("this is a test");
    Map<String,String> args = new HashMap<String,String>();
    ShingleFilterFactory factory = new ShingleFilterFactory();
    factory.init(args);
    TokenStream stream = factory.create(new WhitespaceTokenizer(reader));
    assertTokenStreamContents(stream, new String[] {"this", "this is", "is",
        "is a", "a", "a test", "test"});
  }
  
  /**
   * Test with unigrams disabled
   */
  public void testNoUnigrams() throws Exception {
    Reader reader = new StringReader("this is a test");
    Map<String,String> args = new HashMap<String,String>();
    args.put("outputUnigrams", "false");
    ShingleFilterFactory factory = new ShingleFilterFactory();
    factory.init(args);
    TokenStream stream = factory.create(new WhitespaceTokenizer(reader));
    assertTokenStreamContents(stream,
        new String[] {"this is", "is a", "a test"});
  }
  
  /**
   * Test with a higher max shingle size
   */
  public void testMaxShingleSize() throws Exception {
    Reader reader = new StringReader("this is a test");
    Map<String,String> args = new HashMap<String,String>();
    args.put("maxShingleSize", "3");
    ShingleFilterFactory factory = new ShingleFilterFactory();
    factory.init(args);
    TokenStream stream = factory.create(new WhitespaceTokenizer(reader));
    assertTokenStreamContents(stream, 
        new String[] {"this", "this is", "this is a", "is",
        "is a", "is a test", "a", "a test", "test"});
  }
}
