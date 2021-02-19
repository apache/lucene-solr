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
package org.apache.lucene.analysis.commongrams;

import org.apache.lucene.analysis.BaseTokenStreamFactoryTestCase;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.TestStopFilterFactory;
import org.apache.lucene.util.ClasspathResourceLoader;
import org.apache.lucene.util.ResourceLoader;
import org.apache.lucene.util.Version;

/**
 * Tests pretty much copied from StopFilterFactoryTest We use the test files used by the
 * StopFilterFactoryTest TODO: consider creating separate test files so this won't break if stop
 * filter test files change
 */
public class TestCommonGramsQueryFilterFactory extends BaseTokenStreamFactoryTestCase {

  public void testInform() throws Exception {
    ResourceLoader loader = new ClasspathResourceLoader(TestStopFilterFactory.class);
    assertTrue("loader is null and it shouldn't be", loader != null);
    CommonGramsQueryFilterFactory factory =
        (CommonGramsQueryFilterFactory)
            tokenFilterFactory(
                "CommonGramsQuery",
                Version.LATEST,
                loader,
                "words",
                "stop-1.txt",
                "ignoreCase",
                "true");
    CharArraySet words = factory.getCommonWords();
    assertTrue("words is null and it shouldn't be", words != null);
    assertTrue("words Size: " + words.size() + " is not: " + 2, words.size() == 2);
    assertTrue(factory.isIgnoreCase() + " does not equal: " + true, factory.isIgnoreCase() == true);

    factory =
        (CommonGramsQueryFilterFactory)
            tokenFilterFactory(
                "CommonGramsQuery",
                Version.LATEST,
                loader,
                "words",
                "stop-1.txt, stop-2.txt",
                "ignoreCase",
                "true");
    words = factory.getCommonWords();
    assertTrue("words is null and it shouldn't be", words != null);
    assertTrue("words Size: " + words.size() + " is not: " + 4, words.size() == 4);
    assertTrue(factory.isIgnoreCase() + " does not equal: " + true, factory.isIgnoreCase() == true);

    factory =
        (CommonGramsQueryFilterFactory)
            tokenFilterFactory(
                "CommonGramsQuery",
                Version.LATEST,
                loader,
                "words",
                "stop-snowball.txt",
                "format",
                "snowball",
                "ignoreCase",
                "true");
    words = factory.getCommonWords();
    assertEquals(8, words.size());
    assertTrue(words.contains("he"));
    assertTrue(words.contains("him"));
    assertTrue(words.contains("his"));
    assertTrue(words.contains("himself"));
    assertTrue(words.contains("she"));
    assertTrue(words.contains("her"));
    assertTrue(words.contains("hers"));
    assertTrue(words.contains("herself"));
  }

  /** If no words are provided, then a set of english default stopwords is used. */
  public void testDefaults() throws Exception {
    CommonGramsQueryFilterFactory factory =
        (CommonGramsQueryFilterFactory) tokenFilterFactory("CommonGramsQuery");
    CharArraySet words = factory.getCommonWords();
    assertTrue("words is null and it shouldn't be", words != null);
    assertTrue(words.contains("the"));
    Tokenizer tokenizer = whitespaceMockTokenizer("testing the factory");
    TokenStream stream = factory.create(tokenizer);
    assertTokenStreamContents(stream, new String[] {"testing_the", "the_factory"});
  }

  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    IllegalArgumentException expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              tokenFilterFactory("CommonGramsQuery", "bogusArg", "bogusValue");
            });
    assertTrue(expected.getMessage().contains("Unknown parameters"));
  }

  public void testCompleteGraph() throws Exception {
    CommonGramsQueryFilterFactory factory =
        (CommonGramsQueryFilterFactory) tokenFilterFactory("CommonGramsQuery");
    CharArraySet words = factory.getCommonWords();
    assertTrue("words is null and it shouldn't be", words != null);
    assertTrue(words.contains("the"));
    Tokenizer tokenizer = whitespaceMockTokenizer("testing the factory works");
    TokenStream stream = factory.create(tokenizer);
    assertGraphStrings(stream, "testing_the the_factory factory works");
  }
}
