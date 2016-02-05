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
package org.apache.lucene.analysis.util;


import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.fr.FrenchAnalyzer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.CharArraySet;

/**
 * 
 */
public class TestElision extends BaseTokenStreamTestCase {

  public void testElision() throws Exception {
    String test = "Plop, juste pour voir l'embrouille avec O'brian. M'enfin.";
    Tokenizer tokenizer = new StandardTokenizer(newAttributeFactory());
    tokenizer.setReader(new StringReader(test));
    CharArraySet articles = new CharArraySet(asSet("l", "M"), false);
    TokenFilter filter = new ElisionFilter(tokenizer, articles);
    List<String> tas = filter(filter);
    assertEquals("embrouille", tas.get(4));
    assertEquals("O'brian", tas.get(6));
    assertEquals("enfin", tas.get(7));
  }

  private List<String> filter(TokenFilter filter) throws IOException {
    List<String> tas = new ArrayList<>();
    CharTermAttribute termAtt = filter.getAttribute(CharTermAttribute.class);
    filter.reset();
    while (filter.incrementToken()) {
      tas.add(termAtt.toString());
    }
    filter.end();
    filter.close();
    return tas;
  }
  
  public void testEmptyTerm() throws IOException {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new KeywordTokenizer();
        return new TokenStreamComponents(tokenizer, new ElisionFilter(tokenizer, FrenchAnalyzer.DEFAULT_ARTICLES));
      }
    };
    checkOneTerm(a, "", "");
    a.close();
  }

}
