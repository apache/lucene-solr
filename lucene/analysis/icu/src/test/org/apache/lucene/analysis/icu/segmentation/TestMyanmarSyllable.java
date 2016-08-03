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
package org.apache.lucene.analysis.icu.segmentation;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.Tokenizer;

/** Test tokenizing Myanmar text into syllables */
public class TestMyanmarSyllable extends BaseTokenStreamTestCase {

  Analyzer a;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new ICUTokenizer(newAttributeFactory(), new DefaultICUTokenizerConfig(false, false));
        return new TokenStreamComponents(tokenizer);
      }
    };
  }
  
  @Override
  public void tearDown() throws Exception {
    a.close();
    super.tearDown();
  }
  
  /** as opposed to dictionary break of သက်ဝင်|လှုပ်ရှား|စေ|ပြီး */
  public void testBasics() throws Exception {
    assertAnalyzesTo(a, "သက်ဝင်လှုပ်ရှားစေပြီး", new String[] { "သက်", "ဝင်", "လှုပ်", "ရှား", "စေ", "ပြီး" });
  }
  
  // simple tests from "A Rule-based Syllable Segmentation of Myanmar Text" 
  // * http://www.aclweb.org/anthology/I08-3010
  // (see also the presentation: http://gii2.nagaokaut.ac.jp/gii/media/share/20080901-ZMM%20Presentation.pdf)
  // The words are fake, we just test the categories.
  // note that currently our algorithm is not sophisticated enough to handle some of the special cases!
  
  /** constant */
  public void testC() throws Exception {
    assertAnalyzesTo(a, "ကက", new String[] { "က", "က" });
  }
  
  /** consonant + sign */
  public void testCF() throws Exception {
    assertAnalyzesTo(a, "ကံကံ", new String[] { "ကံ", "ကံ" });
  }
  
  /** consonant + consonant + asat */
  public void testCCA() throws Exception {
    assertAnalyzesTo(a, "ကင်ကင်", new String[] { "ကင်", "ကင်" });
  }
  
  /** consonant + consonant + asat + sign */
  public void testCCAF() throws Exception {
    assertAnalyzesTo(a, "ကင်းကင်း", new String[] { "ကင်း", "ကင်း" });
  }
  
  /** consonant + vowel */
  public void testCV() throws Exception {
    assertAnalyzesTo(a, "ကာကာ", new String[] { "ကာ", "ကာ" });
  }
  
  /** consonant + vowel + sign */
  public void testCVF() throws Exception {
    assertAnalyzesTo(a, "ကားကား", new String[] { "ကား", "ကား" });
  }
  
  /** consonant + vowel + vowel + asat */
  public void testCVVA() throws Exception {
    assertAnalyzesTo(a, "ကော်ကော်", new String[] { "ကော်", "ကော်" });
  }
  
  /** consonant + vowel + vowel + consonant + asat */
  public void testCVVCA() throws Exception {
    assertAnalyzesTo(a, "ကောင်ကောင်", new String[] { "ကောင်", "ကောင်" });
  }
  
  /** consonant + vowel + vowel + consonant + asat + sign */
  public void testCVVCAF() throws Exception {
    assertAnalyzesTo(a, "ကောင်းကောင်း", new String[] { "ကောင်း", "ကောင်း" });
  }
  
  /** consonant + medial */
  public void testCM() throws Exception {
    assertAnalyzesTo(a, "ကျကျ", new String[] { "ကျ", "ကျ" });
  }
  
  /** consonant + medial + sign */
  public void testCMF() throws Exception {
    assertAnalyzesTo(a, "ကျံကျံ", new String[] { "ကျံ", "ကျံ" });
  }
  
  /** consonant + medial + consonant + asat */
  public void testCMCA() throws Exception {
    assertAnalyzesTo(a, "ကျင်ကျင်", new String[] { "ကျင်", "ကျင်" });
  }
  
  /** consonant + medial + consonant + asat + sign */
  public void testCMCAF() throws Exception {
    assertAnalyzesTo(a, "ကျင်းကျင်း", new String[] { "ကျင်း", "ကျင်း" });
  }
  
  /** consonant + medial + vowel */
  public void testCMV() throws Exception {
    assertAnalyzesTo(a, "ကျာကျာ", new String[] { "ကျာ", "ကျာ" });
  }
  
  /** consonant + medial + vowel + sign */
  public void testCMVF() throws Exception {
    assertAnalyzesTo(a, "ကျားကျား", new String[] { "ကျား", "ကျား" });
  }
  
  /** consonant + medial + vowel + vowel + asat */
  public void testCMVVA() throws Exception {
    assertAnalyzesTo(a, "ကျော်ကျော်", new String[] { "ကျော်", "ကျော်" });
  }
  
  /** consonant + medial + vowel + vowel + consonant + asat */
  public void testCMVVCA() throws Exception {
    assertAnalyzesTo(a, "ကြောင်ကြောင်", new String[] { "ကြောင်", "ကြောင်"});
  }
  
  /** consonant + medial + vowel + vowel + consonant + asat + sign */
  public void testCMVVCAF() throws Exception {
    assertAnalyzesTo(a, "ကြောင်းကြောင်း", new String[] { "ကြောင်း", "ကြောင်း"});
  }
  
  /** independent vowel */
  public void testI() throws Exception {
    assertAnalyzesTo(a, "ဪဪ", new String[] { "ဪ", "ဪ" });
  }
  
  /** independent vowel */
  public void testE() throws Exception {
    assertAnalyzesTo(a, "ဣဣ", new String[] { "ဣ", "ဣ" });
  }
}
