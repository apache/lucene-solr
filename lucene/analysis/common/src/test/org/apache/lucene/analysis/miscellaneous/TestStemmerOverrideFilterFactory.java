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

import java.io.Reader;
import java.io.StringReader;

import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.BaseTokenStreamFactoryTestCase;
import org.apache.lucene.analysis.util.StringMockResourceLoader;

/**
 * Simple tests to ensure the stemmer override filter factory is working.
 */
public class TestStemmerOverrideFilterFactory extends BaseTokenStreamFactoryTestCase {
  public void testKeywords() throws Exception {
    // our stemdict stems dogs to 'cat'
    Reader reader = new StringReader("testing dogs");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    stream = tokenFilterFactory("StemmerOverride", TEST_VERSION_CURRENT,
        new StringMockResourceLoader("dogs\tcat"),
        "dictionary", "stemdict.txt").create(stream);
    stream = tokenFilterFactory("PorterStem").create(stream);

    assertTokenStreamContents(stream, new String[] { "test", "cat" });
  }
  
  public void testKeywordsCaseInsensitive() throws Exception {
    Reader reader = new StringReader("testing DoGs");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    stream = tokenFilterFactory("StemmerOverride", TEST_VERSION_CURRENT,
        new StringMockResourceLoader("dogs\tcat"),
        "dictionary", "stemdict.txt",
        "ignoreCase", "true").create(stream);
    stream = tokenFilterFactory("PorterStem").create(stream);
    
    assertTokenStreamContents(stream, new String[] { "test", "cat" });
  }
  
  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    try {
      tokenFilterFactory("StemmerOverride", "bogusArg", "bogusValue");
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("Unknown parameters"));
    }
  }
}
