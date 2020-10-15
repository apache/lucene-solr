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

public class TestScandinavianNormalizationFilter extends BaseTokenStreamTestCase {
  private Analyzer analyzer;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    analyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String field) {
        final Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
        final TokenStream stream = new ScandinavianNormalizationFilter(tokenizer);
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

    checkOneTerm(analyzer, "aeäaeeea", "æææeea"); // should not cause ArrayIndexOutOfBoundsException

    checkOneTerm(analyzer, "aeäaeeeae", "æææeeæ");
    checkOneTerm(analyzer, "aeaeeeae", "ææeeæ");

    checkOneTerm(analyzer, "bøen", "bøen");
    checkOneTerm(analyzer, "bOEen", "bØen");
    checkOneTerm(analyzer, "åene", "åene");


    checkOneTerm(analyzer, "blåbærsyltetøj", "blåbærsyltetøj");
    checkOneTerm(analyzer, "blaabaersyltetöj", "blåbærsyltetøj");
    checkOneTerm(analyzer, "räksmörgås", "ræksmørgås");
    checkOneTerm(analyzer, "raeksmörgaos", "ræksmørgås");
    checkOneTerm(analyzer, "raeksmörgaas", "ræksmørgås");
    checkOneTerm(analyzer, "raeksmoergås", "ræksmørgås");


    checkOneTerm(analyzer, "ab", "ab");
    checkOneTerm(analyzer, "ob", "ob");
    checkOneTerm(analyzer, "Ab", "Ab");
    checkOneTerm(analyzer, "Ob", "Ob");

    checkOneTerm(analyzer, "å", "å");

    checkOneTerm(analyzer, "aa", "å");
    checkOneTerm(analyzer, "aA", "å");
    checkOneTerm(analyzer, "ao", "å");
    checkOneTerm(analyzer, "aO", "å");

    checkOneTerm(analyzer, "AA", "Å");
    checkOneTerm(analyzer, "Aa", "Å");
    checkOneTerm(analyzer, "Ao", "Å");
    checkOneTerm(analyzer, "AO", "Å");

    checkOneTerm(analyzer, "æ", "æ");
    checkOneTerm(analyzer, "ä", "æ");

    checkOneTerm(analyzer, "Æ", "Æ");
    checkOneTerm(analyzer, "Ä", "Æ");

    checkOneTerm(analyzer, "ae", "æ");
    checkOneTerm(analyzer, "aE", "æ");

    checkOneTerm(analyzer, "Ae", "Æ");
    checkOneTerm(analyzer, "AE", "Æ");


    checkOneTerm(analyzer, "ö", "ø");
    checkOneTerm(analyzer, "ø", "ø");
    checkOneTerm(analyzer, "Ö", "Ø");
    checkOneTerm(analyzer, "Ø", "Ø");


    checkOneTerm(analyzer, "oo", "ø");
    checkOneTerm(analyzer, "oe", "ø");
    checkOneTerm(analyzer, "oO", "ø");
    checkOneTerm(analyzer, "oE", "ø");

    checkOneTerm(analyzer, "Oo", "Ø");
    checkOneTerm(analyzer, "Oe", "Ø");
    checkOneTerm(analyzer, "OO", "Ø");
    checkOneTerm(analyzer, "OE", "Ø");
  }
  
  /** check that the empty string doesn't cause issues */
  public void testEmptyTerm() throws Exception {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new KeywordTokenizer();
        return new TokenStreamComponents(tokenizer, new ScandinavianNormalizationFilter(tokenizer));
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
