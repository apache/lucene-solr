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
package org.apache.lucene.analysis;

import java.io.IOException;
import java.util.Random;

public class TestLookaheadTokenFilter extends BaseTokenStreamTestCase {

  public void testRandomStrings() throws Exception {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Random random = random();
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, random.nextBoolean());
        TokenStream output = new MockRandomLookaheadTokenFilter(random, tokenizer);
        return new TokenStreamComponents(tokenizer, output);
      }
    };
    int maxLength = TEST_NIGHTLY ? 8192 : 1024;
    checkRandomData(random(), a, 50*RANDOM_MULTIPLIER, maxLength);
  }

  private static class NeverPeeksLookaheadTokenFilter extends LookaheadTokenFilter<LookaheadTokenFilter.Position> {
    public NeverPeeksLookaheadTokenFilter(TokenStream input) {
      super(input);
    }

    @Override
    public Position newPosition() {
      return new Position();
    }

    @Override
    public boolean incrementToken() throws IOException {
      return nextToken();
    }
  }

  public void testNeverCallingPeek() throws Exception {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, random().nextBoolean());
        TokenStream output = new NeverPeeksLookaheadTokenFilter(tokenizer);
        return new TokenStreamComponents(tokenizer, output);
      }
    };
    int maxLength = TEST_NIGHTLY ? 8192 : 1024;
    checkRandomData(random(), a, 50*RANDOM_MULTIPLIER, maxLength);
  }

  public void testMissedFirstToken() throws Exception {
    Analyzer analyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer source = new MockTokenizer(MockTokenizer.WHITESPACE, false);
        TrivialLookaheadFilter filter = new TrivialLookaheadFilter(source);
        return new TokenStreamComponents(source, filter);
     }
    };

    assertAnalyzesTo(analyzer,
        "Only he who is running knows .",
        new String[]{
            "Only",
            "Only-huh?",
            "he",
            "he-huh?",
            "who",
            "who-huh?",
            "is",
            "is-huh?",
            "running",
            "running-huh?",
            "knows",
            "knows-huh?",
            ".",
            ".-huh?"
        });
  }
}
