package org.apache.lucene.analysis.pt;

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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.Analyzer.TokenStreamComponents;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.miscellaneous.SetKeywordMarkerFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.util.CharArraySet;

import static org.apache.lucene.analysis.VocabularyAssert.*;

/**
 * Simple tests for {@link PortugueseLightStemFilter}
 */
public class TestPortugueseLightStemFilter extends BaseTokenStreamTestCase {
  private Analyzer analyzer = new Analyzer() {
    @Override
    protected TokenStreamComponents createComponents(String fieldName,
        Reader reader) {
      Tokenizer source = new StandardTokenizer(TEST_VERSION_CURRENT, reader);
      TokenStream result = new LowerCaseFilter(TEST_VERSION_CURRENT, source);
      return new TokenStreamComponents(source, new PortugueseLightStemFilter(result));
    }
  };
  
  /**
   * Test the example from the paper "Assessing the impact of stemming accuracy
   * on information retrieval"
   */
  public void testExamples() throws IOException {
    assertAnalyzesTo(
        analyzer,
    "O debate político, pelo menos o que vem a público, parece, de modo nada "
    + "surpreendente, restrito a temas menores. Mas há, evidentemente, "
    + "grandes questões em jogo nas eleições que se aproximam.",
    new String[] { 
      "o", "debat", "politic", "pelo", "meno", "o", "que", "vem", "a", 
      "public", "parec", "de", "modo", "nada", "surpreendent", "restrit",
      "a", "tema", "menor", "mas", "há", "evident", "grand", "questa",
      "em", "jogo", "nas", "eleica", "que", "se", "aproximam"
    });
  }
  
  /**
   * Test examples from the c implementation
   */
  public void testMoreExamples() throws IOException {
     checkOneTerm(analyzer, "doutores", "doutor");
     checkOneTerm(analyzer, "doutor", "doutor");
     
     checkOneTerm(analyzer, "homens", "homem");
     checkOneTerm(analyzer, "homem", "homem");
     
     checkOneTerm(analyzer, "papéis", "papel");
     checkOneTerm(analyzer, "papel", "papel");
     
     checkOneTerm(analyzer, "normais", "normal");
     checkOneTerm(analyzer, "normal", "normal");
     
     checkOneTerm(analyzer, "lencóis", "lencol");
     checkOneTerm(analyzer, "lencol", "lencol");
     
     checkOneTerm(analyzer, "barris", "barril");
     checkOneTerm(analyzer, "barril", "barril");
     
     checkOneTerm(analyzer, "botões", "bota");
     checkOneTerm(analyzer, "botão", "bota");
  }
  
  /** Test against a vocabulary from the reference impl */
  public void testVocabulary() throws IOException {
    assertVocabulary(analyzer, getDataFile("ptlighttestdata.zip"), "ptlight.txt");
  }
  
  public void testKeyword() throws IOException {
    final CharArraySet exclusionSet = new CharArraySet(TEST_VERSION_CURRENT, asSet("quilométricas"), false);
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer source = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
        TokenStream sink = new SetKeywordMarkerFilter(source, exclusionSet);
        return new TokenStreamComponents(source, new PortugueseLightStemFilter(sink));
      }
    };
    checkOneTerm(a, "quilométricas", "quilométricas");
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    checkRandomData(random(), analyzer, 1000*RANDOM_MULTIPLIER);
  }
  
  public void testEmptyTerm() throws IOException {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer tokenizer = new KeywordTokenizer(reader);
        return new TokenStreamComponents(tokenizer, new PortugueseLightStemFilter(tokenizer));
      }
    };
    checkOneTermReuse(a, "", "");
  }
}
