package org.apache.lucene.analysis.gl;

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
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;

/**
 * Simple tests for {@link GalicianMinimalStemmer}
 */
public class TestGalicianMinimalStemFilter extends BaseTokenStreamTestCase {
  Analyzer a = new Analyzer() {
    @Override
    protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
      Tokenizer tokenizer = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
      return new TokenStreamComponents(tokenizer, new GalicianMinimalStemFilter(tokenizer));
    }
  };
  
  public void testPlural() throws Exception {
    checkOneTerm(a, "elefantes", "elefante");
    checkOneTerm(a, "elefante", "elefante");
    checkOneTerm(a, "kalóres", "kalór");
    checkOneTerm(a, "kalór", "kalór");
  }
  
  public void testExceptions() throws Exception {
    checkOneTerm(a, "mas", "mas");
    checkOneTerm(a, "barcelonês", "barcelonês");
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    checkRandomData(random(), a, 10000*RANDOM_MULTIPLIER);
  }
  
  public void testEmptyTerm() throws IOException {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer tokenizer = new KeywordTokenizer(reader);
        return new TokenStreamComponents(tokenizer, new GalicianMinimalStemFilter(tokenizer));
      }
    };
    checkOneTermReuse(a, "", "");
  }
}
