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
package org.apache.lucene.analysis;

import java.io.IOException;
import java.io.Reader;

public class TestMockCharFilter extends BaseTokenStreamTestCase {
  
  public void test() throws IOException {
    Analyzer analyzer = new Analyzer() {

      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
        return new TokenStreamComponents(tokenizer, tokenizer);
      }

      @Override
      protected Reader initReader(String fieldName, Reader reader) {
        return new MockCharFilter(reader, 7);
      }
    };
    
    assertAnalyzesTo(analyzer, "ab",
        new String[] { "aab" },
        new int[] { 0 },
        new int[] { 2 }
    );
    
    assertAnalyzesTo(analyzer, "aba",
        new String[] { "aabaa" },
        new int[] { 0 },
        new int[] { 3 }
    );
    
    assertAnalyzesTo(analyzer, "abcdefga",
        new String[] { "aabcdefgaa" },
        new int[] { 0 },
        new int[] { 8 }
    );
  }
}
