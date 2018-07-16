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


import java.io.StringReader;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.util.BaseTokenStreamFactoryTestCase;

/**
 * Simple tests to ensure the wikipedia tokenizer is working.
 */
public class TestWikipediaTokenizerFactory extends BaseTokenStreamFactoryTestCase {

  private final String WIKIPEDIA = "Wikipedia";
  private final String TOKEN_OUTPUT = "tokenOutput";
  private final String UNTOKENIZED_TYPES = "untokenizedTypes";

  public void testTokenizer() throws Exception {
    String text = "This is a [[Category:foo]]";
    Tokenizer tf = tokenizerFactory(WIKIPEDIA).create(newAttributeFactory());
    tf.setReader(new StringReader(text));
    assertTokenStreamContents(tf,
                              new String[] { "This", "is", "a", "foo" },
                              new int[] { 0, 5, 8, 21 },
                              new int[] { 4, 7, 9, 24 },
                              new String[] { "<ALPHANUM>", "<ALPHANUM>", "<ALPHANUM>", WikipediaTokenizer.CATEGORY },
                              new int[] { 1, 1, 1, 1, },
                              text.length());
  }

  public void testTokenizerTokensOnly() throws Exception {
    String text = "This is a [[Category:foo]]";
    Tokenizer tf = tokenizerFactory(WIKIPEDIA, TOKEN_OUTPUT, Integer.toString(WikipediaTokenizer.TOKENS_ONLY)).create(newAttributeFactory());
    tf.setReader(new StringReader(text));
    assertTokenStreamContents(tf,
                              new String[] { "This", "is", "a", "foo" },
                              new int[] { 0, 5, 8, 21 },
                              new int[] { 4, 7, 9, 24 },
                              new String[] { "<ALPHANUM>", "<ALPHANUM>", "<ALPHANUM>", WikipediaTokenizer.CATEGORY },
                              new int[] { 1, 1, 1, 1, },
                              text.length());
  }

    public void testTokenizerUntokenizedOnly() throws Exception {
      String test = "[[Category:a b c d]] [[Category:e f g]] [[link here]] [[link there]] ''italics here'' something ''more italics'' [[Category:h   i   j]]";
      Set<String> untoks = new HashSet<>();
      untoks.add(WikipediaTokenizer.CATEGORY);
      untoks.add(WikipediaTokenizer.ITALICS);
      Tokenizer tf = tokenizerFactory(WIKIPEDIA, TOKEN_OUTPUT, Integer.toString(WikipediaTokenizer.UNTOKENIZED_ONLY), UNTOKENIZED_TYPES, WikipediaTokenizer.CATEGORY + ", " + WikipediaTokenizer.ITALICS).create(newAttributeFactory());
      tf.setReader(new StringReader(test));
      assertTokenStreamContents(tf,
                                new String[] { "a b c d", "e f g", "link", "here", "link",
                                               "there", "italics here", "something", "more italics", "h   i   j" },
                                new int[] { 11, 32, 42, 47, 56, 61, 71, 86, 98, 124 },
                                new int[] { 18, 37, 46, 51, 60, 66, 83, 95, 110, 133 },
                                new int[] { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 }
      );
    }

    public void testTokenizerBoth() throws Exception {
      String test = "[[Category:a b c d]] [[Category:e f g]] [[link here]] [[link there]] ''italics here'' something ''more italics'' [[Category:h   i   j]]";
      Tokenizer tf = tokenizerFactory(WIKIPEDIA, TOKEN_OUTPUT, Integer.toString(WikipediaTokenizer.BOTH), UNTOKENIZED_TYPES, WikipediaTokenizer.CATEGORY + ", " + WikipediaTokenizer.ITALICS).create(newAttributeFactory());
      tf.setReader(new StringReader(test));
      assertTokenStreamContents(tf,
                                new String[] { "a b c d", "a", "b", "c", "d", "e f g", "e", "f", "g",
                                               "link", "here", "link", "there", "italics here", "italics", "here",
                                               "something", "more italics", "more", "italics", "h   i   j", "h", "i", "j" },
                                new int[] { 11, 11, 13, 15, 17, 32, 32, 34, 36, 42, 47, 56, 61, 71, 71, 79, 86, 98,  98,  103, 124, 124, 128, 132 },
                                new int[] { 18, 12, 14, 16, 18, 37, 33, 35, 37, 46, 51, 60, 66, 83, 78, 83, 95, 110, 102, 110, 133, 125, 129, 133 },
                                new int[] { 1,  0,  1,  1,  1,  1,  0,  1,  1,  1,  1,  1,  1,  1,  0,  1,  1,  1,   0,   1,   1,   0,   1,   1 }
      );
  }

  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      tokenizerFactory(WIKIPEDIA, "bogusArg", "bogusValue").create(newAttributeFactory());
    });
    assertTrue(expected.getMessage().contains("Unknown parameters"));
  }

  public void testIllegalArguments() throws Exception {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      Tokenizer tf = tokenizerFactory(WIKIPEDIA, TOKEN_OUTPUT, "-1").create(newAttributeFactory());
    });
    assertTrue(expected.getMessage().contains("tokenOutput must be TOKENS_ONLY, UNTOKENIZED_ONLY or BOTH"));
  }
}