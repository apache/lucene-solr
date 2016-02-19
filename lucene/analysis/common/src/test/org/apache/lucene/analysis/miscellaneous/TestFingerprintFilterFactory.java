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
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.BaseTokenStreamFactoryTestCase;

public class TestFingerprintFilterFactory extends BaseTokenStreamFactoryTestCase {

  public void test() throws Exception {
    for (final boolean consumeAll : new boolean[]{true, false}) {
      Reader reader = new StringReader("A1 B2 A1 D4 C3");
      MockTokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
      tokenizer.setReader(reader);
      tokenizer.setEnableChecks(consumeAll);
      TokenStream stream = tokenizer;
      stream = tokenFilterFactory("Fingerprint",
          FingerprintFilterFactory.MAX_OUTPUT_TOKEN_SIZE_KEY, "256",
          FingerprintFilterFactory.SEPARATOR_KEY, "_"
      ).create(stream);
      assertTokenStreamContents(stream, new String[]{"A1_B2_C3_D4"});
    }
  }

  public void testRequired() throws Exception {
    // no params are required
      tokenFilterFactory("Fingerprint");
  }

  /**
   * Test that bogus arguments result in exception
   */
  public void testBogusArguments() throws Exception {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      tokenFilterFactory("Fingerprint",
          FingerprintFilterFactory.MAX_OUTPUT_TOKEN_SIZE_KEY, "3",
          "bogusArg", "bogusValue");
    });
    assertTrue(expected.getMessage().contains("Unknown parameters"));
  }
}
