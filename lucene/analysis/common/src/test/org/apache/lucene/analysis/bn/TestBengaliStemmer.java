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

import java.io.IOException;

/**
 * Test Codes for BengaliStemmer
 */
public class TestBengaliStemmer extends BaseTokenStreamTestCase {

  /**
   * Testing few verbal words
   */
  public void testVerbsInShadhuForm() throws IOException {
    check("করেছিলাম", "কর");
    check("করিতেছিলে", "কর");
    check("খাইতাম", "খাই");
    check("যাইবে", "যা");
  }

  public void testVerbsInCholitoForm() throws IOException {
    check("করছিলাম", "কর");
    check("করছিলে", "কর");
    check("করতাম", "কর");
    check("যাব", "যা");
    check("যাবে", "যা");
    check("করি", "কর");
    check("করো", "কর");
  }

  public void testNouns() throws IOException {
    check("মেয়েরা", "মে");
    check("মেয়েদেরকে", "মে");
    check("মেয়েদের", "মে");

    check("একটি", "এক");
    check("মানুষগুলি", "মানুষ");
  }

  private void check(String input, String output) throws IOException {
    Tokenizer tokenizer = whitespaceMockTokenizer(input);
    TokenFilter tf = new BengaliStemFilter(tokenizer);
    assertTokenStreamContents(tf, new String[] { output });
  }
  
  public void testEmptyTerm() throws IOException {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new KeywordTokenizer();
        return new TokenStreamComponents(tokenizer, new BengaliStemFilter(tokenizer));
      }
    };
    checkOneTerm(a, "", "");
    a.close();
  }
}
