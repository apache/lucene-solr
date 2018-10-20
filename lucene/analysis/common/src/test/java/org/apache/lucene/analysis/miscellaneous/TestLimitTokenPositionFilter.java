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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.synonym.SynonymFilter;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;

public class TestLimitTokenPositionFilter extends BaseTokenStreamTestCase {

  public void testMaxPosition2() throws IOException {
    for (final boolean consumeAll : new boolean[]{true, false}) {
      Analyzer a = new Analyzer() {
        @Override
        protected TokenStreamComponents createComponents(String fieldName) {
          MockTokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
          // if we are consuming all tokens, we can use the checks, otherwise we can't
          tokenizer.setEnableChecks(consumeAll);
          return new TokenStreamComponents(tokenizer, new LimitTokenPositionFilter(tokenizer, 2, consumeAll));
        }
      };

      // don't use assertAnalyzesTo here, as the end offset is not the end of the string (unless consumeAll is true, in which case it's correct)!
      assertTokenStreamContents(a.tokenStream("dummy", "1  2     3  4  5"),
          new String[]{"1", "2"}, new int[]{0, 3}, new int[]{1, 4}, consumeAll ? 16 : null);
      assertTokenStreamContents(a.tokenStream("dummy", new StringReader("1 2 3 4 5")),
          new String[]{"1", "2"}, new int[]{0, 2}, new int[]{1, 3}, consumeAll ? 9 : null);

      // less than the limit, ensure we behave correctly
      assertTokenStreamContents(a.tokenStream("dummy", "1  "),
          new String[]{"1"}, new int[]{0}, new int[]{1}, consumeAll ? 3 : null);

      // equal to limit
      assertTokenStreamContents(a.tokenStream("dummy", "1  2  "),
          new String[]{"1", "2"}, new int[]{0, 3}, new int[]{1, 4}, consumeAll ? 6 : null);
      a.close();
    }
  }

  public void testMaxPosition3WithSynomyms() throws IOException {
    for (final boolean consumeAll : new boolean[]{true, false}) {
      MockTokenizer tokenizer = whitespaceMockTokenizer("one two three four five");
      // if we are consuming all tokens, we can use the checks, otherwise we can't
      tokenizer.setEnableChecks(consumeAll);

      SynonymMap.Builder builder = new SynonymMap.Builder(true);
      builder.add(new CharsRef("one"), new CharsRef("first"), true);
      builder.add(new CharsRef("one"), new CharsRef("alpha"), true);
      builder.add(new CharsRef("one"), new CharsRef("beguine"), true);
      CharsRefBuilder multiWordCharsRef = new CharsRefBuilder();
      SynonymMap.Builder.join(new String[]{"and", "indubitably", "single", "only"}, multiWordCharsRef);
      builder.add(new CharsRef("one"), multiWordCharsRef.get(), true);
      SynonymMap.Builder.join(new String[]{"dopple", "ganger"}, multiWordCharsRef);
      builder.add(new CharsRef("two"), multiWordCharsRef.get(), true);
      SynonymMap synonymMap = builder.build();
      TokenStream stream = new SynonymFilter(tokenizer, synonymMap, true);
      stream = new LimitTokenPositionFilter(stream, 3, consumeAll);

      // "only", the 4th word of multi-word synonym "and indubitably single only" is not emitted, since its position is greater than 3.
      assertTokenStreamContents(stream,
          new String[]{"one", "first", "alpha", "beguine", "and", "two", "indubitably", "dopple", "three", "single", "ganger"},
          new int[]{1, 0, 0, 0, 0, 1, 0, 0, 1, 0, 0});
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIllegalArguments() throws Exception {
    new LimitTokenPositionFilter(whitespaceMockTokenizer("one two three four five"), 0);
  }
}
