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

import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.BaseTokenStreamFactoryTestCase;
import org.apache.lucene.util.Version;

public class TestConcatenateGraphFilterFactory extends BaseTokenStreamFactoryTestCase {
  public void test() throws Exception {
    for (final boolean consumeAll : new boolean[]{true, false}) {
      final String input = "A1 B2 A1 D4 C3";
      Reader reader = new StringReader(input);
      MockTokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
      tokenizer.setReader(reader);
      tokenizer.setEnableChecks(consumeAll);
      TokenStream stream = tokenizer;
      stream = tokenFilterFactory("ConcatenateGraph",
          "tokenSeparator", "\u001F"
      ).create(stream);
      assertTokenStreamContents(stream, new String[]{input.replace(' ', (char) ConcatenateGraphFilter.SEP_LABEL)});
    }
  }

  public void testEmptyTokenSeparator() throws Exception {
    final String input = "A1 B2 A1 D4 C3";
    final String output = "A1A1D4C3";
    Reader reader = new StringReader(input);
    MockTokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    tokenizer.setReader(reader);
    TokenStream stream = tokenizer;
    stream = new StopFilter(stream, StopFilter.makeStopSet("B2"));
    stream = tokenFilterFactory("ConcatenateGraph",
        "tokenSeparator", ""
    ).create(stream);
    assertTokenStreamContents(stream, new String[]{output});
  }

  public void testPreserveSep() throws Exception {
    final String input = "A1 B2 A1 D4 C3";
    final String output = "A1A1D4C3";
    Reader reader = new StringReader(input);
    MockTokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    tokenizer.setReader(reader);
    TokenStream stream = tokenizer;
    stream = new StopFilter(stream, StopFilter.makeStopSet("B2"));
    stream = tokenFilterFactory("ConcatenateGraph",
        Version.LUCENE_8_0_0,
        "preserveSep", "false"
    ).create(stream);
    assertTokenStreamContents(stream, new String[]{output});
  }

  public void testPreservePositionIncrements() throws Exception {
    final String input = "A1 B2 A1 D4 C3";
    final String output = "A1 A1 D4 C3";
    Reader reader = new StringReader(input);
    MockTokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    tokenizer.setReader(reader);
    TokenStream stream = tokenizer;
    stream = new StopFilter(stream, StopFilter.makeStopSet("B2"));
    stream = tokenFilterFactory("ConcatenateGraph",
        "tokenSeparator", "\u001F",
        "preservePositionIncrements", "false"
        ).create(stream);
    assertTokenStreamContents(stream, new String[]{output.replace(' ', (char) ConcatenateGraphFilter.SEP_LABEL)});
  }

  public void testRequired() throws Exception {
    // no params are required
    tokenFilterFactory("ConcatenateGraph");
  }

  /**
   * Test that bogus arguments result in exception
   */
  public void testBogusArguments() throws Exception {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () ->
        tokenFilterFactory("ConcatenateGraph", "bogusArg", "bogusValue"));
    assertTrue(expected.getMessage().contains("Unknown parameters"));
  }

  public void testSeparator() throws Exception {
    final String input = "A B C D E F J H";
    final String output = "B-C-F-H";
    Reader reader = new StringReader(input);
    MockTokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    tokenizer.setReader(reader);
    TokenStream stream = tokenizer;
    stream = new StopFilter(stream, StopFilter.makeStopSet("A", "D", "E", "J"));
    stream = tokenFilterFactory("ConcatenateGraph",
        "tokenSeparator", "-",
        "preservePositionIncrements", "false"
    ).create(stream);
    assertTokenStreamContents(stream, new String[]{output});
  }
}
