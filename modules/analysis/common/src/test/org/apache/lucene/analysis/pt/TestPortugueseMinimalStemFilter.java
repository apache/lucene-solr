package org.apache.lucene.analysis.pt;

/**
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
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.util.ReusableAnalyzerBase;

import static org.apache.lucene.analysis.util.VocabularyAssert.*;

/**
 * Simple tests for {@link PortugueseMinimalStemFilter}
 */
public class TestPortugueseMinimalStemFilter extends BaseTokenStreamTestCase {
  private Analyzer analyzer = new ReusableAnalyzerBase() {
    @Override
    protected TokenStreamComponents createComponents(String fieldName,
        Reader reader) {
      Tokenizer source = new StandardTokenizer(TEST_VERSION_CURRENT, reader);
      TokenStream result = new LowerCaseFilter(TEST_VERSION_CURRENT, source);
      return new TokenStreamComponents(source, new PortugueseMinimalStemFilter(result));
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
      "o", "debate", "político", "pelo", "menos", "o", "que", "vem", "a", 
      "público", "parece", "de", "modo", "nada", "surpreendente", "restrito",
      "a", "tema", "menor", "mas", "há", "evidentemente", "grande", "questão",
      "em", "jogo", "na", "eleição", "que", "se", "aproximam"
    });
  }
  
  /** Test against a vocabulary from the reference impl */
  public void testVocabulary() throws IOException {
    assertVocabulary(analyzer, getDataFile("ptminimaltestdata.zip"), "ptminimal.txt");
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    checkRandomData(random, analyzer, 10000*RANDOM_MULTIPLIER);
  }
}
