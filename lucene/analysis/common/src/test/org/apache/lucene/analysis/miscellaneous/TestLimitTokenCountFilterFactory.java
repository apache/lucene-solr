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
import org.apache.lucene.analysis.BaseTokenStreamFactoryTestCase;

import java.io.Reader;
import java.io.StringReader;

public class TestLimitTokenCountFilterFactory extends BaseTokenStreamFactoryTestCase {

  public void test() throws Exception {
    for (final boolean consumeAll : new boolean[]{true, false}) {
      Reader reader = new StringReader("A1 B2 C3 D4 E5 F6");
      MockTokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
      tokenizer.setReader(reader);
      tokenizer.setEnableChecks(consumeAll);
      TokenStream stream = tokenizer;
      stream = tokenFilterFactory("LimitTokenCount",
          LimitTokenCountFilterFactory.MAX_TOKEN_COUNT_KEY, "3",
          LimitTokenCountFilterFactory.CONSUME_ALL_TOKENS_KEY, Boolean.toString(consumeAll)
      ).create(stream);
      assertTokenStreamContents(stream, new String[]{"A1", "B2", "C3"});
    }
  }

  public void testRequired() throws Exception {
    // param is required
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      tokenFilterFactory("LimitTokenCount");
    });
    assertTrue("exception doesn't mention param: " + expected.getMessage(),
        0 < expected.getMessage().indexOf(LimitTokenCountFilterFactory.MAX_TOKEN_COUNT_KEY));
  }

  /**
   * Test that bogus arguments result in exception
   */
  public void testBogusArguments() throws Exception {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      tokenFilterFactory("LimitTokenCount",
          LimitTokenCountFilterFactory.MAX_TOKEN_COUNT_KEY, "3",
          "bogusArg", "bogusValue");
    });
    assertTrue(expected.getMessage().contains("Unknown parameters"));
  }
}
