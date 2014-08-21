package org.apache.lucene.analysis.ckb;

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

import static org.apache.lucene.analysis.VocabularyAssert.assertVocabulary;

import java.io.IOException;
import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;

/**
 * Test the Sorani Stemmer.
 */
public class TestSoraniStemFilter extends BaseTokenStreamTestCase {
  SoraniAnalyzer a = new SoraniAnalyzer();
  
  public void testIndefiniteSingular() throws Exception {
    checkOneTerm(a, "پیاوێک", "پیاو"); // -ek
    checkOneTerm(a, "دەرگایەک", "دەرگا"); // -yek
  }
  
  public void testDefiniteSingular() throws Exception {
    checkOneTerm(a, "پیاوەكە", "پیاو"); // -aka
    checkOneTerm(a, "دەرگاكە", "دەرگا"); // -ka
  }
  
  public void testDemonstrativeSingular() throws Exception {
    checkOneTerm(a, "کتاویە", "کتاوی"); // -a
    checkOneTerm(a, "دەرگایە", "دەرگا"); // -ya
  }
  
  public void testIndefinitePlural() throws Exception {
    checkOneTerm(a, "پیاوان", "پیاو"); // -An
    checkOneTerm(a, "دەرگایان", "دەرگا"); // -yAn
  }
  
  public void testDefinitePlural() throws Exception {
    checkOneTerm(a, "پیاوەکان", "پیاو"); // -akAn
    checkOneTerm(a, "دەرگاکان", "دەرگا"); // -kAn
  }
  
  public void testDemonstrativePlural() throws Exception {
    checkOneTerm(a, "پیاوانە", "پیاو"); // -Ana
    checkOneTerm(a, "دەرگایانە", "دەرگا"); // -yAna
  }
  
  public void testEzafe() throws Exception {
    checkOneTerm(a, "هۆتیلی", "هۆتیل"); // singular
    checkOneTerm(a, "هۆتیلێکی", "هۆتیل"); // indefinite
    checkOneTerm(a, "هۆتیلانی", "هۆتیل"); // plural
  }
  
  public void testPostpositions() throws Exception {
    checkOneTerm(a, "دوورەوە", "دوور"); // -awa
    checkOneTerm(a, "نیوەشەودا", "نیوەشەو"); // -dA
    checkOneTerm(a, "سۆرانا", "سۆران"); // -A
  }
  
  public void testPossessives() throws Exception {
    checkOneTerm(a, "پارەمان", "پارە"); // -mAn
    checkOneTerm(a, "پارەتان", "پارە"); // -tAn
    checkOneTerm(a, "پارەیان", "پارە"); // -yAn
  }
  
  public void testEmptyTerm() throws IOException {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer tokenizer = new KeywordTokenizer(reader);
        return new TokenStreamComponents(tokenizer, new SoraniStemFilter(tokenizer));
      }
    };
    checkOneTerm(a, "", "");
  }
  
  /** test against a basic vocabulary file */
  public void testVocabulary() throws Exception {
    // top 8k words or so: freq > 1000
    assertVocabulary(a, getDataFile("ckbtestdata.zip"), "testdata.txt");
  }
}
