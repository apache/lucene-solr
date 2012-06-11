package org.apache.lucene.analysis.icu;

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
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.TokenStream;

import com.ibm.icu.text.Transliterator;
import com.ibm.icu.text.UnicodeSet;


/**
 * Test the ICUTransformFilter with some basic examples.
 */
public class TestICUTransformFilter extends BaseTokenStreamTestCase {
  
  public void testBasicFunctionality() throws Exception {
    checkToken(Transliterator.getInstance("Traditional-Simplified"), 
        "簡化字", "简化字"); 
    checkToken(Transliterator.getInstance("Katakana-Hiragana"), 
        "ヒラガナ", "ひらがな");
    checkToken(Transliterator.getInstance("Fullwidth-Halfwidth"), 
        "アルアノリウ", "ｱﾙｱﾉﾘｳ");
    checkToken(Transliterator.getInstance("Any-Latin"), 
        "Αλφαβητικός Κατάλογος", "Alphabētikós Katálogos");
    checkToken(Transliterator.getInstance("NFD; [:Nonspacing Mark:] Remove"), 
        "Alphabētikós Katálogos", "Alphabetikos Katalogos");
    checkToken(Transliterator.getInstance("Han-Latin"),
        "中国", "zhōng guó");
  }
  
  public void testCustomFunctionality() throws Exception {
    String rules = "a > b; b > c;"; // convert a's to b's and b's to c's
    checkToken(Transliterator.createFromRules("test", rules, Transliterator.FORWARD), "abacadaba", "bcbcbdbcb");
  }
  
  public void testCustomFunctionality2() throws Exception {
    String rules = "c { a > b; a > d;"; // convert a's to b's and b's to c's
    checkToken(Transliterator.createFromRules("test", rules, Transliterator.FORWARD), "caa", "cbd");
  }
  
  public void testOptimizer() throws Exception {
    String rules = "a > b; b > c;"; // convert a's to b's and b's to c's
    Transliterator custom = Transliterator.createFromRules("test", rules, Transliterator.FORWARD);
    assertTrue(custom.getFilter() == null);
    new ICUTransformFilter(new KeywordTokenizer(new StringReader("")), custom);
    assertTrue(custom.getFilter().equals(new UnicodeSet("[ab]")));
  }
  
  public void testOptimizer2() throws Exception {
    checkToken(Transliterator.getInstance("Traditional-Simplified; CaseFold"), 
        "ABCDE", "abcde");
  }
  
  public void testOptimizerSurrogate() throws Exception {
    String rules = "\\U00020087 > x;"; // convert CJK UNIFIED IDEOGRAPH-20087 to an x
    Transliterator custom = Transliterator.createFromRules("test", rules, Transliterator.FORWARD);
    assertTrue(custom.getFilter() == null);
    new ICUTransformFilter(new KeywordTokenizer(new StringReader("")), custom);
    assertTrue(custom.getFilter().equals(new UnicodeSet("[\\U00020087]")));
  }

  private void checkToken(Transliterator transform, String input, String expected) throws IOException {
    TokenStream ts = new ICUTransformFilter(new KeywordTokenizer((new StringReader(input))), transform);
    assertTokenStreamContents(ts, new String[] { expected });
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    final Transliterator transform = Transliterator.getInstance("Any-Latin");
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer tokenizer = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
        return new TokenStreamComponents(tokenizer, new ICUTransformFilter(tokenizer, transform));
      }
    };
    checkRandomData(random(), a, 1000*RANDOM_MULTIPLIER);
  }
  
  public void testEmptyTerm() throws IOException {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer tokenizer = new KeywordTokenizer(reader);
        return new TokenStreamComponents(tokenizer, new ICUTransformFilter(tokenizer, Transliterator.getInstance("Any-Latin")));
      }
    };
    checkOneTermReuse(a, "", "");
  }
}
