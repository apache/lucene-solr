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
package org.apache.lucene.analysis.compound;


import java.io.Reader;
import java.io.StringReader;

import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.util.BaseTokenStreamFactoryTestCase;

/**
 * Simple tests to ensure the Hyphenation compound filter factory is working.
 */
public class TestHyphenationCompoundWordTokenFilterFactory extends BaseTokenStreamFactoryTestCase {
  /**
   * Ensure the factory works with hyphenation grammar+dictionary: using default options.
   */
  public void testHyphenationWithDictionary() throws Exception {
    Reader reader = new StringReader("min veninde som er lidt af en læsehest");
    TokenStream stream = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    ((Tokenizer)stream).setReader(reader);
    stream = tokenFilterFactory("HyphenationCompoundWord", 
        "hyphenator", "da_UTF8.xml",
        "dictionary", "da_compoundDictionary.txt").create(stream);
    
    assertTokenStreamContents(stream, 
        new String[] { "min", "veninde", "som", "er", "lidt", "af", "en", "læsehest", "læse", "hest" },
        new int[] { 1, 1, 1, 1, 1, 1, 1, 1, 0, 0 }
    );
  }

  /**
   * Ensure the factory works with no dictionary: using hyphenation grammar only.
   * Also change the min/max subword sizes from the default. When using no dictionary,
   * it's generally necessary to tweak these, or you get lots of expansions.
   */
  public void testHyphenationOnly() throws Exception {
    Reader reader = new StringReader("basketballkurv");
    TokenStream stream = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    ((Tokenizer)stream).setReader(reader);
    stream = tokenFilterFactory("HyphenationCompoundWord", 
        "hyphenator", "da_UTF8.xml",
        "minSubwordSize", "2",
        "maxSubwordSize", "4").create(stream);
    
    assertTokenStreamContents(stream,
        new String[] { "basketballkurv", "ba", "sket", "bal", "ball", "kurv" }
    );
  }
  
  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    try {
      tokenFilterFactory("HyphenationCompoundWord", 
          "hyphenator", "da_UTF8.xml",
          "bogusArg", "bogusValue");
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("Unknown parameters"));
    }
  }
}
