package org.apache.lucene.analysis.snowball;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.BaseTokenStreamFactoryTestCase;
import org.apache.lucene.analysis.util.StringMockResourceLoader;
import org.tartarus.snowball.ext.EnglishStemmer;

import java.io.Reader;
import java.io.StringReader;

public class TestSnowballPorterFilterFactory extends BaseTokenStreamFactoryTestCase {

  public void test() throws Exception {
    String text = "The fledgling banks were counting on a big boom in banking";
    EnglishStemmer stemmer = new EnglishStemmer();
    String[] test = text.split("\\s");
    String[] gold = new String[test.length];
    for (int i = 0; i < test.length; i++) {
      stemmer.setCurrent(test[i]);
      stemmer.stem();
      gold[i] = stemmer.getCurrent();
    }
    
    Reader reader = new StringReader(text);
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    stream = tokenFilterFactory("SnowballPorter", "language", "English").create(stream);
    assertTokenStreamContents(stream, gold);
  }
  
  /**
   * Test the protected words mechanism of SnowballPorterFilterFactory
   */
  public void testProtected() throws Exception {
    Reader reader = new StringReader("ridding of some stemming");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    stream = tokenFilterFactory("SnowballPorter", TEST_VERSION_CURRENT,
        new StringMockResourceLoader("ridding"),
        "protected", "protwords.txt",
        "language", "English").create(stream);

    assertTokenStreamContents(stream, new String[] { "ridding", "of", "some", "stem" });
  }
  
  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    try {
      tokenFilterFactory("SnowballPorter", "bogusArg", "bogusValue");
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("Unknown parameters"));
    }
  }
}

