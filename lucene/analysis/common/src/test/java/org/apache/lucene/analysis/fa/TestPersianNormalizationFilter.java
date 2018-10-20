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
package org.apache.lucene.analysis.fa;


import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;

/**
 * Test the Persian Normalization Filter
 * 
 */
public class TestPersianNormalizationFilter extends BaseTokenStreamTestCase {

  public void testFarsiYeh() throws IOException {
    check("های", "هاي");
  }

  public void testYehBarree() throws IOException {
    check("هاے", "هاي");
  }

  public void testKeheh() throws IOException {
    check("کشاندن", "كشاندن");
  }

  public void testHehYeh() throws IOException {
    check("كتابۀ", "كتابه");
  }

  public void testHehHamzaAbove() throws IOException {
    check("كتابهٔ", "كتابه");
  }

  public void testHehGoal() throws IOException {
    check("زادہ", "زاده");
  }

  private void check(final String input, final String expected) throws IOException {
    MockTokenizer tokenStream = whitespaceMockTokenizer(input);
    PersianNormalizationFilter filter = new PersianNormalizationFilter(
        tokenStream);
    assertTokenStreamContents(filter, new String[]{expected});
  }
  
  public void testEmptyTerm() throws IOException {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new KeywordTokenizer();
        return new TokenStreamComponents(tokenizer, new PersianNormalizationFilter(tokenizer));
      }
    };
    checkOneTerm(a, "", "");
    a.close();
  }

}
