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
package org.apache.lucene.analysis.phonetic;

import java.io.IOException;
import java.io.StringReader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;

/**
 * Tests {@link DaitchMokotoffSoundexFilter}
 */
public class TestDaitchMokotoffSoundexFilter extends BaseTokenStreamTestCase {

  public void testAlgorithms() throws Exception {
    assertAlgorithm(true, "aaa bbb ccc easgasg",
      new String[] { "aaa", "000000", "bbb", "700000", "ccc", "400000", "450000", "454000",
        "540000", "545000", "500000", "easgasg", "045450" });
    assertAlgorithm(false, "aaa bbb ccc easgasg",
      new String[] { "000000", "700000", "400000", "450000", "454000", "540000", "545000",
        "500000", "045450" });
  }

  static void assertAlgorithm(boolean inject, String input, String[] expected) throws Exception {
    Tokenizer tokenizer = new WhitespaceTokenizer();
    tokenizer.setReader(new StringReader(input));
    DaitchMokotoffSoundexFilter filter = new DaitchMokotoffSoundexFilter(tokenizer, inject);
    assertTokenStreamContents(filter, expected);
  }

  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws IOException {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
        return new TokenStreamComponents(tokenizer, new DaitchMokotoffSoundexFilter(tokenizer, false));
      }
    };

    checkRandomData(random(), a, 1000 * RANDOM_MULTIPLIER);
    a.close();

    Analyzer b = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
        return new TokenStreamComponents(tokenizer, new DaitchMokotoffSoundexFilter(tokenizer, false));
      }
    };

    checkRandomData(random(), b, 1000 * RANDOM_MULTIPLIER);
    b.close();
  }

  public void testEmptyTerm() throws IOException {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new KeywordTokenizer();
        return new TokenStreamComponents(tokenizer, new DaitchMokotoffSoundexFilter(tokenizer, random().nextBoolean()));
      }
    };
    checkOneTerm(a, "", "");
    a.close();
  }

}
