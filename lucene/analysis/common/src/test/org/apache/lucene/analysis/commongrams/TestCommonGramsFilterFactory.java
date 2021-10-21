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


import java.io.StringReader;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.util.BaseTokenStreamFactoryTestCase;
import org.apache.lucene.analysis.util.ClasspathResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.util.Version;

public class TestCommonGramsFilterFactory extends BaseTokenStreamFactoryTestCase {

  public void testInform() throws Exception {
    ResourceLoader loader = new ClasspathResourceLoader(getClass());
    assertTrue("loader is null and it shouldn't be", loader != null);
    CommonGramsFilterFactory factory =
        (CommonGramsFilterFactory)
            tokenFilterFactory(
                "CommonGrams",
                Version.LATEST,
                loader,
                "words",
                "common-1.txt",
                "ignoreCase",
                "true");
    CharArraySet words = factory.getCommonWords();
    assertTrue("words is null and it shouldn't be", words != null);
    assertTrue("words Size: " + words.size() + " is not: " + 2,
        words.size() == 2);
    assertTrue(factory.isIgnoreCase() + " does not equal: " + true, factory
        .isIgnoreCase() == true);

    factory =
        (CommonGramsFilterFactory)
            tokenFilterFactory(
                "CommonGrams",
                Version.LATEST,
                loader,
                "words",
                "common-1.txt, common-2.txt",
                "ignoreCase",
                "true");
    words = factory.getCommonWords();
    assertTrue("words is null and it shouldn't be", words != null);
    assertTrue("words Size: " + words.size() + " is not: " + 4,
        words.size() == 4);
    assertTrue(factory.isIgnoreCase() + " does not equal: " + true, factory
        .isIgnoreCase() == true);

    factory =
        (CommonGramsFilterFactory)
            tokenFilterFactory(
                "CommonGrams",
                Version.LATEST,
                loader,
                "words",
                "common-snowball.txt",
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
  
  /**
   * If no words are provided, then a set of english default stopwords is used.
   */
  public void testDefaults() throws Exception {
    CommonGramsFilterFactory factory = (CommonGramsFilterFactory) tokenFilterFactory("CommonGrams");
    CharArraySet words = factory.getCommonWords();
    assertTrue("words is null and it shouldn't be", words != null);
    assertTrue(words.contains("the"));
    Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    tokenizer.setReader(new StringReader("testing the factory"));
    TokenStream stream = factory.create(tokenizer);
    assertTokenStreamContents(stream, 
        new String[] { "testing", "testing_the", "the", "the_factory", "factory" });
  }

  /**
   * Test that ignoreCase flag is honored when no words are provided and default stopwords are used.
   */
  public void testIgnoreCase() throws Exception {
    ResourceLoader loader = new ClasspathResourceLoader(getClass());
    CommonGramsFilterFactory factory =
        (CommonGramsFilterFactory)
            tokenFilterFactory("CommonGrams", Version.LATEST, loader, "ignoreCase", "true");
    CharArraySet words = factory.getCommonWords();
    assertTrue("words is null and it shouldn't be", words != null);
    assertTrue(words.contains("the"));
    assertTrue(words.contains("The"));
    Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    tokenizer.setReader(new StringReader("testing The factory"));
    TokenStream stream = factory.create(tokenizer);
    assertTokenStreamContents(
        stream, new String[] {"testing", "testing_The", "The", "The_factory", "factory"});
  }

  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {      
      tokenFilterFactory("CommonGrams", "bogusArg", "bogusValue");
    });
    assertTrue(expected.getMessage().contains("Unknown parameters"));
  }
}

