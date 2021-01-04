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

import org.apache.lucene.analysis.BaseTokenStreamFactoryTestCase;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.util.ClasspathResourceLoader;
import org.apache.lucene.util.ResourceLoader;

public class TestStopFilterFactory extends BaseTokenStreamFactoryTestCase {

  public void testInform() throws Exception {
    ResourceLoader loader = new ClasspathResourceLoader(getClass());
    assertTrue("loader is null and it shouldn't be", loader != null);
    StopFilterFactory factory =
        (StopFilterFactory) tokenFilterFactory("Stop", "words", "stop-1.txt", "ignoreCase", "true");
    CharArraySet words = factory.getStopWords();
    assertTrue("words is null and it shouldn't be", words != null);
    assertTrue("words Size: " + words.size() + " is not: " + 2, words.size() == 2);
    assertTrue(factory.isIgnoreCase() + " does not equal: " + true, factory.isIgnoreCase() == true);

    factory =
        (StopFilterFactory)
            tokenFilterFactory("Stop", "words", "stop-1.txt, stop-2.txt", "ignoreCase", "true");
    words = factory.getStopWords();
    assertTrue("words is null and it shouldn't be", words != null);
    assertTrue("words Size: " + words.size() + " is not: " + 4, words.size() == 4);
    assertTrue(factory.isIgnoreCase() + " does not equal: " + true, factory.isIgnoreCase() == true);

    factory =
        (StopFilterFactory)
            tokenFilterFactory(
                "Stop", "words", "stop-snowball.txt", "format", "snowball", "ignoreCase", "true");
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
    assertEquals(EnglishAnalyzer.ENGLISH_STOP_WORDS_SET, factory.getStopWords());
    assertEquals(false, factory.isIgnoreCase());
  }

  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    IllegalArgumentException expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              tokenFilterFactory("Stop", "bogusArg", "bogusValue");
            });
    assertTrue(expected.getMessage().contains("Unknown parameters"));
  }

  /** Test that bogus arguments result in exception */
  public void testBogusFormats() throws Exception {
    IllegalArgumentException expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              tokenFilterFactory("Stop", "words", "stop-snowball.txt", "format", "bogus");
            });
    String msg = expected.getMessage();
    assertTrue(msg, msg.contains("Unknown"));
    assertTrue(msg, msg.contains("format"));
    assertTrue(msg, msg.contains("bogus"));

    expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              tokenFilterFactory(
                  "Stop",
                  // implicit default words file
                  "format",
                  "bogus");
              fail();
            });
    msg = expected.getMessage();
    assertTrue(msg, msg.contains("can not be specified"));
    assertTrue(msg, msg.contains("format"));
    assertTrue(msg, msg.contains("bogus"));
  }
}
