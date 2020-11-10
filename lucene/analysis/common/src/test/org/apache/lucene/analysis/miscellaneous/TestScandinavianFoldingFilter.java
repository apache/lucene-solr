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
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;

public class TestScandinavianFoldingFilter extends BaseTokenStreamTestCase {
  private Analyzer analyzer;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    analyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String field) {
        final Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
        final TokenStream stream = new ScandinavianFoldingFilter(tokenizer);
        return new TokenStreamComponents(tokenizer, stream);
      }
    };
  }
  
  @Override
  public void tearDown() throws Exception {
    analyzer.close();
    super.tearDown();
  }

  public void test() throws Exception {

    checkOneTerm(analyzer, "aeäaeeea", "aaaeea"); // should not cause ArrayOutOfBoundsException

    checkOneTerm(analyzer, "aeäaeeeae", "aaaeea");
    checkOneTerm(analyzer, "aeaeeeae", "aaeea");

    checkOneTerm(analyzer, "bøen", "boen");
    checkOneTerm(analyzer, "åene", "aene");


    checkOneTerm(analyzer, "blåbærsyltetøj", "blabarsyltetoj");
    checkOneTerm(analyzer, "blaabaarsyltetoej", "blabarsyltetoj");
    checkOneTerm(analyzer, "blåbärsyltetöj", "blabarsyltetoj");

    checkOneTerm(analyzer, "raksmorgas", "raksmorgas");
    checkOneTerm(analyzer, "räksmörgås", "raksmorgas");
    checkOneTerm(analyzer, "ræksmørgås", "raksmorgas");
    checkOneTerm(analyzer, "raeksmoergaas", "raksmorgas");
    checkOneTerm(analyzer, "ræksmörgaos", "raksmorgas");


    checkOneTerm(analyzer, "ab", "ab");
    checkOneTerm(analyzer, "ob", "ob");
    checkOneTerm(analyzer, "Ab", "Ab");
    checkOneTerm(analyzer, "Ob", "Ob");

    checkOneTerm(analyzer, "å", "a");

    checkOneTerm(analyzer, "aa", "a");
    checkOneTerm(analyzer, "aA", "a");
    checkOneTerm(analyzer, "ao", "a");
    checkOneTerm(analyzer, "aO", "a");

    checkOneTerm(analyzer, "AA", "A");
    checkOneTerm(analyzer, "Aa", "A");
    checkOneTerm(analyzer, "Ao", "A");
    checkOneTerm(analyzer, "AO", "A");

    checkOneTerm(analyzer, "æ", "a");
    checkOneTerm(analyzer, "ä", "a");

    checkOneTerm(analyzer, "Æ", "A");
    checkOneTerm(analyzer, "Ä", "A");

    checkOneTerm(analyzer, "ae", "a");
    checkOneTerm(analyzer, "aE", "a");

    checkOneTerm(analyzer, "Ae", "A");
    checkOneTerm(analyzer, "AE", "A");


    checkOneTerm(analyzer, "ö", "o");
    checkOneTerm(analyzer, "ø", "o");
    checkOneTerm(analyzer, "Ö", "O");
    checkOneTerm(analyzer, "Ø", "O");


    checkOneTerm(analyzer, "oo", "o");
    checkOneTerm(analyzer, "oe", "o");
    checkOneTerm(analyzer, "oO", "o");
    checkOneTerm(analyzer, "oE", "o");

    checkOneTerm(analyzer, "Oo", "O");
    checkOneTerm(analyzer, "Oe", "O");
    checkOneTerm(analyzer, "OO", "O");
    checkOneTerm(analyzer, "OE", "O");
  }
  
  /** check that the empty string doesn't cause issues */
  public void testEmptyTerm() throws Exception {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new KeywordTokenizer();
        return new TokenStreamComponents(tokenizer, new ScandinavianFoldingFilter(tokenizer));
      } 
    };
    checkOneTerm(a, "", "");
    a.close();
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomData() throws Exception {
    checkRandomData(random(), analyzer, 200 * RANDOM_MULTIPLIER);
  }
}
