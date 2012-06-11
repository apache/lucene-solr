package org.apache.lucene.analysis.ar;

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
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;

/**
 * Test the Arabic Normalization Filter
 */
public class TestArabicNormalizationFilter extends BaseTokenStreamTestCase {

  public void testAlifMadda() throws IOException {
    check("آجن", "اجن");
  }
  
  public void testAlifHamzaAbove() throws IOException {
    check("أحمد", "احمد");
  }
  
  public void testAlifHamzaBelow() throws IOException {
    check("إعاذ", "اعاذ");
  }
  
  public void testAlifMaksura() throws IOException {
    check("بنى", "بني");
  }

  public void testTehMarbuta() throws IOException {
    check("فاطمة", "فاطمه");
  }
  
  public void testTatweel() throws IOException {
    check("روبرـــــت", "روبرت");
  }
  
  public void testFatha() throws IOException {
    check("مَبنا", "مبنا");
  }
  
  public void testKasra() throws IOException {
    check("علِي", "علي");
  }
  
  public void testDamma() throws IOException {
    check("بُوات", "بوات");
  }
  
  public void testFathatan() throws IOException {
    check("ولداً", "ولدا");
  }
  
  public void testKasratan() throws IOException {
    check("ولدٍ", "ولد");
  }
  
  public void testDammatan() throws IOException {
    check("ولدٌ", "ولد");
  }  
  
  public void testSukun() throws IOException {
    check("نلْسون", "نلسون");
  }
  
  public void testShaddah() throws IOException {
    check("هتميّ", "هتمي");
  }  
  
  private void check(final String input, final String expected) throws IOException {
    ArabicLetterTokenizer tokenStream = new ArabicLetterTokenizer(TEST_VERSION_CURRENT, new StringReader(input));
    ArabicNormalizationFilter filter = new ArabicNormalizationFilter(tokenStream);
    assertTokenStreamContents(filter, new String[]{expected});
  }
  
  public void testEmptyTerm() throws IOException {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer tokenizer = new KeywordTokenizer(reader);
        return new TokenStreamComponents(tokenizer, new ArabicNormalizationFilter(tokenizer));
      }
    };
    checkOneTermReuse(a, "", "");
  }

}
