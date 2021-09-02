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
package org.apache.lucene.analysis.te;

import java.io.IOException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;

/** Test TeluguNormalizer */
public class TestTeluguNormalizer extends BaseTokenStreamTestCase {

  /** Long matras should be shortened. */
  public void testMatra() throws IOException {
    check("పదాలూ", "పదాలు");
    check("అబ్బాయీ", "అబ్బాయి");
  }

  public void testVowels() throws IOException {
    // removal of visarga (ః)
    check("ఃౌైాిు", "ౌైాిు");
    // vowel shortening
    check("ఔఐఆఈఊ", "ఓఏఅఇఉ");
  }

  private void check(String input, String output) throws IOException {
    Tokenizer tokenizer = whitespaceMockTokenizer(input);
    TokenFilter tf = new TeluguNormalizationFilter(tokenizer);
    assertTokenStreamContents(tf, new String[] {output});
  }

  public void testEmptyTerm() throws IOException {
    Analyzer a =
        new Analyzer() {
          @Override
          protected TokenStreamComponents createComponents(String fieldName) {
            Tokenizer tokenizer = new KeywordTokenizer();
            return new TokenStreamComponents(tokenizer, new TeluguNormalizationFilter(tokenizer));
          }
        };
    checkOneTerm(a, "", "");
    a.close();
  }
}
