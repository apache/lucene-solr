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
import org.apache.solr.common.ResourceLoader;
import org.apache.solr.core.SolrResourceLoader;

/**
 * Simple tests to ensure the Hyphenation compound filter factory is working.
 */
public class TestHyphenationCompoundWordTokenFilterFactory extends BaseTokenTestCase {
  /**
   * Ensure the factory works with hyphenation grammar+dictionary: using default options.
   */
  public void testHyphenationWithDictionary() throws Exception {
    Reader reader = new StringReader("min veninde som er lidt af en læsehest");
    Tokenizer tokenizer = new WhitespaceTokenizer(DEFAULT_VERSION, reader);
    HyphenationCompoundWordTokenFilterFactory factory = new HyphenationCompoundWordTokenFilterFactory();
    ResourceLoader loader = new SolrResourceLoader(null, null);
    Map<String,String> args = new HashMap<String,String>(DEFAULT_VERSION_PARAM);
    args.put("hyphenator", "da_UTF8.xml");
    args.put("dictionary", "da_compoundDictionary.txt");
    factory.init(args);
    factory.inform(loader);
    TokenStream stream = factory.create(tokenizer);
    
    assertTokenStreamContents(stream, 
        new String[] { "min", "veninde", "som", "er", "lidt", "af", "en", "læsehest", "læse", "hest" },
        new int[] { 1, 1, 1, 1, 1, 1, 1, 1, 0, 0 }
    );
  }

  /**
   * Ensure the factory works with no dictionary: using hyphenation grammar only.
   * Also change the min/max subword sizes from the default. When using no dictionary,
   * its generally necessary to tweak these, or you get lots of expansions.
   */
  public void testHyphenationOnly() throws Exception {
    Reader reader = new StringReader("basketballkurv");
    Tokenizer tokenizer = new WhitespaceTokenizer(DEFAULT_VERSION, reader);
    HyphenationCompoundWordTokenFilterFactory factory = new HyphenationCompoundWordTokenFilterFactory();
    ResourceLoader loader = new SolrResourceLoader(null, null);
    Map<String,String> args = new HashMap<String,String>(DEFAULT_VERSION_PARAM);
    args.put("hyphenator", "da_UTF8.xml");
    args.put("minSubwordSize", "2");
    args.put("maxSubwordSize", "4");
    factory.init(args);
    factory.inform(loader);
    TokenStream stream = factory.create(tokenizer);
    
    assertTokenStreamContents(stream,
        new String[] { "basketballkurv", "ba", "sket", "bal", "ball", "kurv" }
    );
  }
}
