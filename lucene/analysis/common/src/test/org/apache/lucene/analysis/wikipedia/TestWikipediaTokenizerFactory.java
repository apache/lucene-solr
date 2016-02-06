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
package org.apache.lucene.analysis.wikipedia;


import java.io.Reader;
import java.io.StringReader;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.util.BaseTokenStreamFactoryTestCase;
import org.apache.lucene.analysis.wikipedia.WikipediaTokenizer;

/**
 * Simple tests to ensure the wikipedia tokenizer is working.
 */
public class TestWikipediaTokenizerFactory extends BaseTokenStreamFactoryTestCase {
  public void testTokenizer() throws Exception {
    Reader reader = new StringReader("This is a [[Category:foo]]");
    Tokenizer tokenizer = tokenizerFactory("Wikipedia").create(newAttributeFactory());
    tokenizer.setReader(reader);
    assertTokenStreamContents(tokenizer,
        new String[] { "This", "is", "a", "foo" },
        new int[] { 0, 5, 8, 21 },
        new int[] { 4, 7, 9, 24 },
        new String[] { "<ALPHANUM>", "<ALPHANUM>", "<ALPHANUM>", WikipediaTokenizer.CATEGORY },
        new int[] { 1, 1, 1, 1, });
  }
  
  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    try {
      tokenizerFactory("Wikipedia", "bogusArg", "bogusValue");
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("Unknown parameters"));
    }
  }
}
