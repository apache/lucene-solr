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
package org.apache.lucene.analysis.miscellaneous;


import java.io.Reader;
import java.io.StringReader;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.BaseTokenStreamFactoryTestCase;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.util.ClasspathResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.util.Version;

public class TestKeepFilterFactory extends BaseTokenStreamFactoryTestCase {

  public void testInform() throws Exception {
    ResourceLoader loader = new ClasspathResourceLoader(getClass());
    assertTrue("loader is null and it shouldn't be", loader != null);
    KeepWordFilterFactory factory = (KeepWordFilterFactory) tokenFilterFactory("KeepWord",
        "words", "keep-1.txt",
        "ignoreCase", "true");
    CharArraySet words = factory.getWords();
    assertTrue("words is null and it shouldn't be", words != null);
    assertTrue("words Size: " + words.size() + " is not: " + 2, words.size() == 2);

    factory = (KeepWordFilterFactory) tokenFilterFactory("KeepWord",
        "words", "keep-1.txt, keep-2.txt",
        "ignoreCase", "true");
    words = factory.getWords();
    assertTrue("words is null and it shouldn't be", words != null);
    assertTrue("words Size: " + words.size() + " is not: " + 4, words.size() == 4);
  }
  
  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    try {
      tokenFilterFactory("KeepWord", "bogusArg", "bogusValue");
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("Unknown parameters"));
    }
  }
  
  public void test43Backcompat() throws Exception {
    Reader reader = new StringReader("a foo bar");
    TokenStream stream = whitespaceMockTokenizer(reader);
    stream = tokenFilterFactory("KeepWord", Version.LUCENE_4_3_1,
        "enablePositionIncrements", "false",
        "words", "keep-1.txt").create(stream);
    assertTrue(stream instanceof Lucene43KeepWordFilter);
    assertTokenStreamContents(stream, new String[] {"foo", "bar"}, new int[] {2, 6}, new int[] {5, 9}, new int[] {1, 1});

    try {
      tokenFilterFactory("KeepWord", Version.LUCENE_4_4_0, "enablePositionIncrements", "false");
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("enablePositionIncrements=false is not supported"));
    }
    tokenFilterFactory("KeepWord", Version.LUCENE_4_4_0, "enablePositionIncrements", "true");

    try {
      tokenFilterFactory("KeepWord", "enablePositionIncrements", "true");
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("not a valid option"));
    }
  }
}