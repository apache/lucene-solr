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


import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;

public class TestFingerprintFilter extends BaseTokenStreamTestCase {

  public void testDupsAndSorting() throws Exception {
    for (final boolean consumeAll : new boolean[] { true, false }) {
      MockTokenizer tokenizer = whitespaceMockTokenizer("B A B E");
      tokenizer.setEnableChecks(consumeAll);
      TokenStream stream = new FingerprintFilter(tokenizer);
      assertTokenStreamContents(stream, new String[] { "A B E" });
    }
  }

  public void testAllDupValues() throws Exception {
    for (final boolean consumeAll : new boolean[] { true, false }) {
      MockTokenizer tokenizer = whitespaceMockTokenizer("B2 B2");
      tokenizer.setEnableChecks(consumeAll);
      TokenStream stream = new FingerprintFilter(tokenizer);
      assertTokenStreamContents(stream, new String[] { "B2" });
    }
  }

  public void testMaxFingerprintSize() throws Exception {
    for (final boolean consumeAll : new boolean[] { true, false }) {
      MockTokenizer tokenizer = whitespaceMockTokenizer("B2 A1 C3 D4 E5 F6 G7 H1");
      tokenizer.setEnableChecks(consumeAll);
      TokenStream stream = new FingerprintFilter(tokenizer, 4, ' ');
      assertTokenStreamContents(stream, new String[] {});
    }
  }

  public void testCustomSeparator() throws Exception {
    for (final boolean consumeAll : new boolean[] { true, false }) {
      MockTokenizer tokenizer = whitespaceMockTokenizer("B2 A1 C3 B2");
      tokenizer.setEnableChecks(consumeAll);
      TokenStream stream = new FingerprintFilter(tokenizer,
          FingerprintFilter.DEFAULT_MAX_OUTPUT_TOKEN_SIZE, '_');
      assertTokenStreamContents(stream, new String[] { "A1_B2_C3" });
    }
  }

  public void testSingleToken() throws Exception {
    for (final boolean consumeAll : new boolean[] { true, false }) {
      MockTokenizer tokenizer = whitespaceMockTokenizer("A1");
      tokenizer.setEnableChecks(consumeAll);
      TokenStream stream = new FingerprintFilter(tokenizer);
      assertTokenStreamContents(stream, new String[] { "A1" });
    }
  }

}
