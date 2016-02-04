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
package org.apache.lucene.analysis.core;


import java.io.Reader;
import java.io.StringReader;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.BaseTokenStreamFactoryTestCase;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.util.ClasspathResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.util.Version;

public class TestStopFilterFactory extends BaseTokenStreamFactoryTestCase {

  public void testInform() throws Exception {
    ResourceLoader loader = new ClasspathResourceLoader(getClass());
    assertTrue("loader is null and it shouldn't be", loader != null);
    StopFilterFactory factory = (StopFilterFactory) tokenFilterFactory("Stop",
        "words", "stop-1.txt",
        "ignoreCase", "true");
    CharArraySet words = factory.getStopWords();
    assertTrue("words is null and it shouldn't be", words != null);
    assertTrue("words Size: " + words.size() + " is not: " + 2, words.size() == 2);
    assertTrue(factory.isIgnoreCase() + " does not equal: " + true, factory.isIgnoreCase() == true);

    factory = (StopFilterFactory) tokenFilterFactory("Stop",
        "words", "stop-1.txt, stop-2.txt",
        "ignoreCase", "true");
    words = factory.getStopWords();
    assertTrue("words is null and it shouldn't be", words != null);
    assertTrue("words Size: " + words.size() + " is not: " + 4, words.size() == 4);
    assertTrue(factory.isIgnoreCase() + " does not equal: " + true, factory.isIgnoreCase() == true);

    factory = (StopFilterFactory) tokenFilterFactory("Stop",
        "words", "stop-snowball.txt",
        "format", "snowball",
        "ignoreCase", "true");
    words = factory.getStopWords();
    assertEquals(8, words.size());
    assertTrue(words.contains("he"));
    assertTrue(words.contains("him"));
    assertTrue(words.contains("his"));
    assertTrue(words.contains("himself"));
    assertTrue(words.contains("she"));
    assertTrue(words.contains("her"));
    assertTrue(words.contains("hers"));
    assertTrue(words.contains("herself"));

    // defaults
    factory = (StopFilterFactory) tokenFilterFactory("Stop");
    assertEquals(StopAnalyzer.ENGLISH_STOP_WORDS_SET, factory.getStopWords());
    assertEquals(false, factory.isIgnoreCase());
  }
  
  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    try {
      tokenFilterFactory("Stop", "bogusArg", "bogusValue");
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("Unknown parameters"));
    }
  }

  /** Test that bogus arguments result in exception */
  public void testBogusFormats() throws Exception {
    try {
      tokenFilterFactory("Stop", 
                         "words", "stop-snowball.txt",
                         "format", "bogus");
      fail();
    } catch (IllegalArgumentException expected) {
      String msg = expected.getMessage();
      assertTrue(msg, msg.contains("Unknown"));
      assertTrue(msg, msg.contains("format"));
      assertTrue(msg, msg.contains("bogus"));
    }
    try {
      tokenFilterFactory("Stop", 
                         // implicit default words file
                         "format", "bogus");
      fail();
    } catch (IllegalArgumentException expected) {
      String msg = expected.getMessage();
      assertTrue(msg, msg.contains("can not be specified"));
      assertTrue(msg, msg.contains("format"));
      assertTrue(msg, msg.contains("bogus"));
    }
  }

  public void test43Backcompat() throws Exception {
    Reader reader = new StringReader("foo bar");
    TokenStream stream = whitespaceMockTokenizer(reader);
    stream = tokenFilterFactory("Stop", Version.LUCENE_4_3_1,
        "enablePositionIncrements", "false",
        "words", "stop-2.txt").create(stream);
    assertTrue(stream instanceof Lucene43StopFilter);
    assertTokenStreamContents(stream, new String[] {"foo", "bar"}, new int[] {0, 4}, new int[] {3, 7}, new int[] {1, 1});

    try {
      tokenFilterFactory("Stop", Version.LUCENE_4_4_0, "enablePositionIncrements", "false", "words", "stop-2.txt");
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("enablePositionIncrements=false is not supported"));
    }
    tokenFilterFactory("Stop", Version.LUCENE_4_4_0, "enablePositionIncrements", "true", "words", "stop-2.txt");

    try {
      tokenFilterFactory("Stop", "enablePositionIncrements", "true", "words", "stop-2.txt");
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("not a valid option"));
    }
  }
}
