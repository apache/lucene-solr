package org.apache.lucene.analysis.tr;

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
import java.io.StringReader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;

/**
 * Test the Turkish lowercase filter.
 */
public class TestTurkishLowerCaseFilter extends BaseTokenStreamTestCase {
  
  /**
   * Test composed forms
   */
  public void testTurkishLowerCaseFilter() throws Exception {
    TokenStream stream = new MockTokenizer(new StringReader(
        "\u0130STANBUL \u0130ZM\u0130R ISPARTA"), MockTokenizer.WHITESPACE, false);
    TurkishLowerCaseFilter filter = new TurkishLowerCaseFilter(stream);
    assertTokenStreamContents(filter, new String[] {"istanbul", "izmir",
        "\u0131sparta",});
  }
  
  /**
   * Test decomposed forms
   */
  public void testDecomposed() throws Exception {
    TokenStream stream = new MockTokenizer(new StringReader(
        "\u0049\u0307STANBUL \u0049\u0307ZM\u0049\u0307R ISPARTA"), MockTokenizer.WHITESPACE, false);
    TurkishLowerCaseFilter filter = new TurkishLowerCaseFilter(stream);
    assertTokenStreamContents(filter, new String[] {"istanbul", "izmir",
        "\u0131sparta",});
  }
  
  /**
   * Test decomposed forms with additional accents
   * In this example, U+0049 + U+0316 + U+0307 is canonically equivalent
   * to U+0130 + U+0316, and is lowercased the same way.
   */
  public void testDecomposed2() throws Exception {
    TokenStream stream = new MockTokenizer(new StringReader(
        "\u0049\u0316\u0307STANBUL \u0049\u0307ZM\u0049\u0307R I\u0316SPARTA"), MockTokenizer.WHITESPACE, false);
    TurkishLowerCaseFilter filter = new TurkishLowerCaseFilter(stream);
    assertTokenStreamContents(filter, new String[] {"i\u0316stanbul", "izmir",
        "\u0131\u0316sparta",});
  }
  
  public void testDecomposed3() throws Exception {
    TokenStream stream = new MockTokenizer(new StringReader(
        "\u0049\u0307"), MockTokenizer.WHITESPACE, false);
    TurkishLowerCaseFilter filter = new TurkishLowerCaseFilter(stream);
    assertTokenStreamContents(filter, new String[] {"i"});
  }
  
  public void testEmptyTerm() throws IOException {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer tokenizer = new KeywordTokenizer(reader);
        return new TokenStreamComponents(tokenizer, new TurkishLowerCaseFilter(tokenizer));
      }
    };
    checkOneTermReuse(a, "", "");
  }
}
