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
import org.apache.lucene.analysis.util.BaseTokenStreamFactoryTestCase;

import java.io.Reader;
import java.io.StringReader;

public class TestLimitTokenPositionFilterFactory extends BaseTokenStreamFactoryTestCase {

  public void testMaxPosition1() throws Exception {
    for (final boolean consumeAll : new boolean[]{true, false}) {
      Reader reader = new StringReader("A1 B2 C3 D4 E5 F6");
      MockTokenizer tokenizer = whitespaceMockTokenizer(reader);
      // if we are consuming all tokens, we can use the checks, otherwise we can't
      tokenizer.setEnableChecks(consumeAll);
      TokenStream stream = tokenizer;
      stream = tokenFilterFactory("LimitTokenPosition",
          LimitTokenPositionFilterFactory.MAX_TOKEN_POSITION_KEY, "1",
          LimitTokenPositionFilterFactory.CONSUME_ALL_TOKENS_KEY, Boolean.toString(consumeAll)
      ).create(stream);
      assertTokenStreamContents(stream, new String[]{"A1"});
    }
  }

  public void testMissingParam() throws Exception {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      tokenFilterFactory("LimitTokenPosition");
    });
    assertTrue("exception doesn't mention param: " + expected.getMessage(),
        0 < expected.getMessage().indexOf(LimitTokenPositionFilterFactory.MAX_TOKEN_POSITION_KEY));
  }

  public void testMaxPosition1WithShingles() throws Exception {
    for (final boolean consumeAll : new boolean[]{true, false}) {
      Reader reader = new StringReader("one two three four five");
      MockTokenizer tokenizer = whitespaceMockTokenizer(reader);
      // if we are consuming all tokens, we can use the checks, otherwise we can't
      tokenizer.setEnableChecks(consumeAll);
      TokenStream stream = tokenizer;
      stream = tokenFilterFactory("Shingle",
          "minShingleSize", "2",
          "maxShingleSize", "3",
          "outputUnigrams", "true").create(stream);
      stream = tokenFilterFactory("LimitTokenPosition",
          LimitTokenPositionFilterFactory.MAX_TOKEN_POSITION_KEY, "1",
          LimitTokenPositionFilterFactory.CONSUME_ALL_TOKENS_KEY, Boolean.toString(consumeAll)
      ).create(stream);
      assertTokenStreamContents(stream, new String[]{"one", "one two", "one two three"});
    }
  }

  /**
   * Test that bogus arguments result in exception
   */
  public void testBogusArguments() throws Exception {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      tokenFilterFactory("LimitTokenPosition",
          "maxTokenPosition", "3",
          "bogusArg", "bogusValue");
    });
    assertTrue(expected.getMessage().contains("Unknown parameters"));
  }
}
