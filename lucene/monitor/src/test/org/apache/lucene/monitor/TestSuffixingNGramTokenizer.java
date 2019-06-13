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

package org.apache.lucene.monitor;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;

public class TestSuffixingNGramTokenizer extends BaseTokenStreamTestCase {

  private Analyzer analyzer = new Analyzer() {
    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
      Tokenizer source = new WhitespaceTokenizer();
      TokenStream sink = new SuffixingNGramTokenFilter(source, "XX", "ANY", 10);
      return new TokenStreamComponents(source, sink);
    }
  };

  public void testTokensAreSuffixed() throws IOException {
    assertAnalyzesTo(analyzer, "term", new String[]{
        "term", "termXX", "terXX", "teXX", "tXX", "ermXX", "erXX", "eXX", "rmXX", "rXX", "mXX", "XX"
    });
  }

  public void testRepeatedSuffixesAreNotEmitted() throws IOException {
    assertAnalyzesTo(analyzer, "arm harm term", new String[]{
        "arm", "armXX", "arXX", "aXX", "rmXX", "rXX", "mXX", "XX",
        "harm", "harmXX", "harXX", "haXX", "hXX",
        "term", "termXX", "terXX", "teXX", "tXX", "ermXX", "erXX", "eXX"
    });
  }

  public void testRepeatedInfixesAreNotEmitted() throws IOException {
    assertAnalyzesTo(analyzer, "alarm alas harm", new String[]{
        "alarm", "alarmXX", "alarXX", "alaXX", "alXX", "aXX",
        "larmXX", "larXX", "laXX", "lXX", "armXX", "arXX", "rmXX", "rXX", "mXX", "XX",
        "alas", "alasXX", "lasXX", "asXX", "sXX", "harm", "harmXX", "harXX", "haXX", "hXX"
    });
  }

  public void testLengthyTokensAreNotNgrammed() throws IOException {
    assertAnalyzesTo(analyzer, "alongtermthatshouldntbengrammed", new String[]{
        "alongtermthatshouldntbengrammed", "ANY"
    });
  }

}
