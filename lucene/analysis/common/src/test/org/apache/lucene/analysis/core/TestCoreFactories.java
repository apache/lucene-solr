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
import org.apache.lucene.analysis.BaseTokenStreamFactoryTestCase;
import org.apache.lucene.analysis.Tokenizer;

/** Simple tests to ensure the core lucene factories are working. */
public class TestCoreFactories extends BaseTokenStreamFactoryTestCase {

  /** Test KeywordTokenizerFactory */
  public void testKeywordTokenizer() throws Exception {
    Reader reader = new StringReader("What's this thing do?");
    Tokenizer stream = tokenizerFactory("Keyword").create();
    stream.setReader(reader);
    assertTokenStreamContents(stream, new String[] {"What's this thing do?"});
  }

  /** Test WhitespaceTokenizerFactory */
  public void testWhitespaceTokenizer() throws Exception {
    Reader reader = new StringReader("What's this thing do?");
    Tokenizer stream = tokenizerFactory("Whitespace").create(newAttributeFactory());
    stream.setReader(reader);
    assertTokenStreamContents(stream, new String[] {"What's", "this", "thing", "do?"});
  }

  /** Test LetterTokenizerFactory */
  public void testLetterTokenizer() throws Exception {
    Reader reader = new StringReader("What's this thing do?");
    Tokenizer stream = tokenizerFactory("Letter").create(newAttributeFactory());
    stream.setReader(reader);
    assertTokenStreamContents(stream, new String[] {"What", "s", "this", "thing", "do"});
  }

  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    IllegalArgumentException expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              tokenizerFactory("Whitespace", "bogusArg", "bogusValue");
            });
    assertTrue(expected.getMessage().contains("Unknown parameters"));

    expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              tokenizerFactory("Letter", "bogusArg", "bogusValue");
            });
    assertTrue(expected.getMessage().contains("Unknown parameters"));
  }
}
