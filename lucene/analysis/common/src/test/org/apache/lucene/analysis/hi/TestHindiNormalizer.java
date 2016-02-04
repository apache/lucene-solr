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
package org.apache.lucene.analysis.hi;


import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;

/**
 * Test HindiNormalizer
 */
public class TestHindiNormalizer extends BaseTokenStreamTestCase {
  /**
   * Test some basic normalization, with an example from the paper.
   */
  public void testBasics() throws IOException {
    check("अँगरेज़ी", "अंगरेजि");
    check("अँगरेजी", "अंगरेजि");
    check("अँग्रेज़ी", "अंगरेजि");
    check("अँग्रेजी", "अंगरेजि");
    check("अंगरेज़ी", "अंगरेजि");
    check("अंगरेजी", "अंगरेजि");
    check("अंग्रेज़ी", "अंगरेजि");
    check("अंग्रेजी", "अंगरेजि");
  }
  
  public void testDecompositions() throws IOException {
    // removing nukta dot
    check("क़िताब", "किताब");
    check("फ़र्ज़", "फरज");
    check("क़र्ज़", "करज");
    // some other composed nukta forms
    check("ऱऴख़ग़ड़ढ़य़", "रळखगडढय");
    // removal of format (ZWJ/ZWNJ)
    check("शार्‍मा", "शारमा");
    check("शार्‌मा", "शारमा");
    // removal of chandra
    check("ॅॆॉॊऍऎऑऒ\u0972", "ेेोोएएओओअ");
    // vowel shortening
    check("आईऊॠॡऐऔीूॄॣैौ", "अइउऋऌएओिुृॢेो");
  }
  private void check(String input, String output) throws IOException {
    Tokenizer tokenizer = whitespaceMockTokenizer(input);
    TokenFilter tf = new HindiNormalizationFilter(tokenizer);
    assertTokenStreamContents(tf, new String[] { output });
  }
  
  public void testEmptyTerm() throws IOException {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new KeywordTokenizer();
        return new TokenStreamComponents(tokenizer, new HindiNormalizationFilter(tokenizer));
      }
    };
    checkOneTerm(a, "", "");
    a.close();
  }
}
