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


import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.util.BaseTokenStreamFactoryTestCase;

import java.io.Reader;
import java.io.StringReader;

/**
 * Simple tests to ensure the simple truncation filter factory is working.
 */
public class TestTruncateTokenFilterFactory extends BaseTokenStreamFactoryTestCase {
  /**
   * Ensure the filter actually truncates text.
   */
  public void testTruncating() throws Exception {
    Reader reader = new StringReader("abcdefg 1234567 ABCDEFG abcde abc 12345 123");
    TokenStream stream = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    ((Tokenizer) stream).setReader(reader);
    stream = tokenFilterFactory("Truncate",
        TruncateTokenFilterFactory.PREFIX_LENGTH_KEY, "5").create(stream);
    assertTokenStreamContents(stream, new String[]{"abcde", "12345", "ABCDE", "abcde", "abc", "12345", "123"});
  }

  /**
   * Test that bogus arguments result in exception
   */
  public void testBogusArguments() throws Exception {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      tokenFilterFactory("Truncate",
          TruncateTokenFilterFactory.PREFIX_LENGTH_KEY, "5",
          "bogusArg", "bogusValue");
    });
    assertTrue(expected.getMessage().contains("Unknown parameter(s):"));
  }

  /**
   * Test that negative prefix length result in exception
   */
  public void testNonPositivePrefixLengthArgument() throws Exception {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      tokenFilterFactory("Truncate",
          TruncateTokenFilterFactory.PREFIX_LENGTH_KEY, "-5"
      );
    });
    assertTrue(expected.getMessage().contains(TruncateTokenFilterFactory.PREFIX_LENGTH_KEY + " parameter must be a positive number: -5"));
  }
}


