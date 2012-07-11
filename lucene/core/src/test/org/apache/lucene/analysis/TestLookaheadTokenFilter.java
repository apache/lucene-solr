package org.apache.lucene.analysis;

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

import java.io.IOException;
import java.io.Reader;
import java.util.Random;

import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

public class TestLookaheadTokenFilter extends BaseTokenStreamTestCase {

  public void testRandomStrings() throws Exception {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Random random = random();
        Tokenizer tokenizer = new MockTokenizer(reader, MockTokenizer.WHITESPACE, random.nextBoolean());
        TokenStream output = new MockRandomLookaheadTokenFilter(random, tokenizer);
        return new TokenStreamComponents(tokenizer, output);
      }
      };
    checkRandomData(random(), a, 200*RANDOM_MULTIPLIER, 8192);
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
      protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer tokenizer = new MockTokenizer(reader, MockTokenizer.WHITESPACE, random().nextBoolean());
        TokenStream output = new NeverPeeksLookaheadTokenFilter(tokenizer);
        return new TokenStreamComponents(tokenizer, output);
      }
      };
    checkRandomData(random(), a, 200*RANDOM_MULTIPLIER, 8192);
  }
}
