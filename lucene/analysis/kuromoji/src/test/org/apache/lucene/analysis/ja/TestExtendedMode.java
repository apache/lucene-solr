package org.apache.lucene.analysis.ja;

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
import java.io.StringReader;
import java.util.Random;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ja.JapaneseTokenizer.Mode;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util._TestUtil;
import org.apache.lucene.util.LuceneTestCase.Slow;

@Slow
public class TestExtendedMode extends BaseTokenStreamTestCase {
  private final Analyzer analyzer = new Analyzer() {
    
    @Override
    protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
      Tokenizer tokenizer = new JapaneseTokenizer(reader, null, true, Mode.EXTENDED);
      return new TokenStreamComponents(tokenizer, tokenizer);
    }
  };
  
  /** simple test for supplementary characters */
  public void testSurrogates() throws IOException {
    assertAnalyzesTo(analyzer, "𩬅艱鍟䇹愯瀛",
      new String[] { "𩬅", "艱", "鍟", "䇹", "愯", "瀛" });
  }
  
  /** random test ensuring we don't ever split supplementaries */
  public void testSurrogates2() throws IOException {
    int numIterations = atLeast(1000);
    for (int i = 0; i < numIterations; i++) {
      String s = _TestUtil.randomUnicodeString(random(), 100);
      TokenStream ts = analyzer.tokenStream("foo", new StringReader(s));
      CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
      ts.reset();
      while (ts.incrementToken()) {
        assertTrue(UnicodeUtil.validUTF16String(termAtt));
      }
    }
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    Random random = random();
    checkRandomData(random, analyzer, 1000*RANDOM_MULTIPLIER);
  }
  
  /** blast some random large strings through the analyzer */
  public void testRandomHugeStrings() throws Exception {
    Random random = random();
    checkRandomData(random, analyzer, 100*RANDOM_MULTIPLIER, 8192);
  }
}
