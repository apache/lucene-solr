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

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.analysis.util.StringMockResourceLoader;
import org.tartarus.snowball.ext.EnglishStemmer;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestSnowballPorterFilterFactory extends BaseTokenStreamTestCase {

  public void test() throws IOException {
    EnglishStemmer stemmer = new EnglishStemmer();
    String[] test = {"The", "fledgling", "banks", "were", "counting", "on", "a", "big", "boom", "in", "banking"};
    String[] gold = new String[test.length];
    for (int i = 0; i < test.length; i++) {
      stemmer.setCurrent(test[i]);
      stemmer.stem();
      gold[i] = stemmer.getCurrent();
    }

    SnowballPorterFilterFactory factory = new SnowballPorterFilterFactory();
    Map<String, String> args = new HashMap<String, String>();
    args.put("language", "English");

    factory.setLuceneMatchVersion(TEST_VERSION_CURRENT);
    factory.init(args);
    factory.inform(new StringMockResourceLoader(""));
    Tokenizer tokenizer = new MockTokenizer(
        new StringReader(join(test, ' ')), MockTokenizer.WHITESPACE, false);
    TokenStream stream = factory.create(tokenizer);
    assertTokenStreamContents(stream, gold);
  }
  
  String join(String[] stuff, char sep) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < stuff.length; i++) {
      if (i > 0) {
        sb.append(sep);
      }
      sb.append(stuff[i]);
    }
    return sb.toString();
  }
  
  /**
   * Test the protected words mechanism of SnowballPorterFilterFactory
   */
  public void testProtected() throws Exception {
    SnowballPorterFilterFactory factory = new SnowballPorterFilterFactory();
    ResourceLoader loader = new StringMockResourceLoader("ridding");
    Map<String,String> args = new HashMap<String,String>();
    args.put("protected", "protwords.txt");
    args.put("language", "English");
    factory.setLuceneMatchVersion(TEST_VERSION_CURRENT);
    factory.init(args);
    factory.inform(loader);
    Reader reader = new StringReader("ridding of some stemming");
    Tokenizer tokenizer = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    TokenStream stream = factory.create(tokenizer);
    assertTokenStreamContents(stream, new String[] { "ridding", "of", "some", "stem" });
  }
}

