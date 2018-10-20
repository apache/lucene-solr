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
package org.apache.lucene.analysis.phonetic;


import java.io.IOException;
import java.io.StringReader;
import java.util.HashSet;
import java.util.regex.Pattern;

import org.apache.commons.codec.language.bm.NameType;
import org.apache.commons.codec.language.bm.PhoneticEngine;
import org.apache.commons.codec.language.bm.RuleType;
import org.apache.commons.codec.language.bm.Languages.LanguageSet;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.miscellaneous.PatternKeywordMarkerFilter;
import org.apache.lucene.analysis.tokenattributes.KeywordAttribute;

/** Tests {@link BeiderMorseFilter} */
public class TestBeiderMorseFilter extends BaseTokenStreamTestCase {
  private Analyzer analyzer;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    analyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
        return new TokenStreamComponents(tokenizer, 
            new BeiderMorseFilter(tokenizer, new PhoneticEngine(NameType.GENERIC, RuleType.EXACT, true)));
      }
    };
  }
  
  @Override
  public void tearDown() throws Exception {
    analyzer.close();
    super.tearDown();
  }
  
  /** generic, "exact" configuration */
  public void testBasicUsage() throws Exception {    
    assertAnalyzesTo(analyzer, "Angelo",
        new String[] { "anZelo", "andZelo", "angelo", "anhelo", "anjelo", "anxelo" },
        new int[] { 0, 0, 0, 0, 0, 0 },
        new int[] { 6, 6, 6, 6, 6, 6 },
        new int[] { 1, 0, 0, 0, 0, 0 });
    
    assertAnalyzesTo(analyzer, "D'Angelo",
        new String[] { "anZelo", "andZelo", "angelo", "anhelo", "anjelo", "anxelo",
                  "danZelo", "dandZelo", "dangelo", "danhelo", "danjelo", "danxelo" },
        new int[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 },
        new int[] { 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8 },
        new int[] { 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 });
  }
  
  /** restrict the output to a set of possible origin languages */
  public void testLanguageSet() throws Exception {
    final LanguageSet languages = LanguageSet.from(new HashSet<String>() {{
      add("italian"); add("greek"); add("spanish");
    }});
    Analyzer analyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new MockTokenizer( MockTokenizer.WHITESPACE, false);
        return new TokenStreamComponents(tokenizer, 
            new BeiderMorseFilter(tokenizer, 
                new PhoneticEngine(NameType.GENERIC, RuleType.EXACT, true), languages));
      }
    };
    assertAnalyzesTo(analyzer, "Angelo",
        new String[] { "andZelo", "angelo", "anxelo" },
        new int[] { 0, 0, 0, },
        new int[] { 6, 6, 6, },
        new int[] { 1, 0, 0, });
    analyzer.close();
  }
  
  /** for convenience, if the input yields no output, we pass it thru as-is */
  public void testNumbers() throws Exception {
    assertAnalyzesTo(analyzer, "1234",
        new String[] { "1234" },
        new int[] { 0 },
        new int[] { 4 },
        new int[] { 1 });
  }

  public void testRandom() throws Exception {
    checkRandomData(random(), analyzer, 1000 * RANDOM_MULTIPLIER); 
  }
  
  public void testEmptyTerm() throws IOException {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new KeywordTokenizer();
        return new TokenStreamComponents(tokenizer, new BeiderMorseFilter(tokenizer, new PhoneticEngine(NameType.GENERIC, RuleType.EXACT, true)));
      }
    };
    checkOneTerm(a, "", "");
    a.close();
  }
  
  public void testCustomAttribute() throws IOException {
    TokenStream stream = new MockTokenizer(MockTokenizer.KEYWORD, false);
    ((Tokenizer)stream).setReader(new StringReader("D'Angelo"));
    stream = new PatternKeywordMarkerFilter(stream, Pattern.compile(".*"));
    stream = new BeiderMorseFilter(stream, new PhoneticEngine(NameType.GENERIC, RuleType.EXACT, true));
    KeywordAttribute keyAtt = stream.addAttribute(KeywordAttribute.class);
    stream.reset();
    int i = 0;
    while(stream.incrementToken()) {
      assertTrue(keyAtt.isKeyword());
      i++;
    }
    assertEquals(12, i);
    stream.end();
    stream.close();
  }
}
