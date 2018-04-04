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
package org.apache.lucene.analysis.bn;


import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.util.TestUtil;

import java.io.IOException;

/**
 * Test BengaliNormalizer
 */
public class TestBengaliNormalizer extends BaseTokenStreamTestCase {
  /**
   * Test some basic normalization, with an example from the paper.
   */
  public void testChndrobindu() throws IOException {
    check("চাঁদ", "চাদ");
  }

  public void testRosshoIKar() throws IOException {
    check("বাড়ী", "বারি");
    check("তীর", "তির");
  }

  public void testRosshoUKar() throws IOException {
    check("ভূল", "ভুল");
    check("অনূপ", "অনুপ");
  }

  public void testNga() throws IOException {
    check("বাঙলা", "বাংলা");
  }

  public void testJaPhaala() throws IOException {
    check("ব্যাক্তি", "বেক্তি");
    check( "সন্ধ্যা", "সন্ধা");
  }

  public void testBaPhalaa() throws IOException {
    check("স্বদেশ", "সদেস");
    check("তত্ত্ব", "তত্ত");
    check("বিশ্ব", "বিসস");
  }

  public void testVisarga() throws IOException {
    check("দুঃখ", "দুখখ");
    check("উঃ", "উহ");
    check("পুনঃ", "পুন");
  }

  public void testBasics() throws IOException {
    check("কণা", "কনা");
    check("শরীর", "সরির");
    check("বাড়ি", "বারি");
  }

  /** creates random strings in the bengali block and ensures the normalizer doesn't trip up on them */
  public void testRandom() throws IOException {
    BengaliNormalizer normalizer = new BengaliNormalizer();
    for (int i = 0; i < 100000; i++) {
      String randomBengali = TestUtil.randomSimpleStringRange(random(), '\u0980', '\u09FF', 7);
      try {
        int newLen = normalizer.normalize(randomBengali.toCharArray(), randomBengali.length());
        assertTrue(newLen >= 0); // should not return negative length
        assertTrue(newLen <= randomBengali.length()); // should not increase length of string
      } catch (Exception e) {
        System.err.println("normalizer failed on input: '" + randomBengali + "' (" + escape(randomBengali) + ")");
        throw e;
      }
    }
  }

  private void check(String input, String output) throws IOException {
    Tokenizer tokenizer = whitespaceMockTokenizer(input);
    TokenFilter tf = new BengaliNormalizationFilter(tokenizer);
    assertTokenStreamContents(tf, new String[] { output });
  }
  
  public void testEmptyTerm() throws IOException {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new KeywordTokenizer();
        return new TokenStreamComponents(tokenizer, new BengaliNormalizationFilter(tokenizer));
      }
    };
    checkOneTerm(a, "", "");
    a.close();
  }
}
