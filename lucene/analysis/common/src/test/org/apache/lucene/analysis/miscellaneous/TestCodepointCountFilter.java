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
package org.apache.lucene.analysis.miscellaneous;


import java.io.IOException;
import java.io.StringReader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.util.TestUtil;
import org.junit.Test;

public class TestCodepointCountFilter extends BaseTokenStreamTestCase {
  public void testFilterWithPosIncr() throws Exception {
    TokenStream stream = whitespaceMockTokenizer("short toolong evenmuchlongertext a ab toolong foo");
    CodepointCountFilter filter = new CodepointCountFilter(stream, 2, 6);
    assertTokenStreamContents(filter,
      new String[]{"short", "ab", "foo"},
      new int[]{1, 4, 2}
    );
  }
  
  public void testEmptyTerm() throws IOException {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new KeywordTokenizer();
        return new TokenStreamComponents(tokenizer, new CodepointCountFilter(tokenizer, 0, 5));
      }
    };
    checkOneTerm(a, "", "");
    a.close();
  }
  
  public void testRandomStrings() throws IOException {
    for (int i = 0; i < 10000; i++) {
      String text = TestUtil.randomUnicodeString(random(), 100);
      int min = TestUtil.nextInt(random(), 0, 100);
      int max = TestUtil.nextInt(random(), 0, 100);
      int count = text.codePointCount(0, text.length());
      if(min>max){
        int temp = min;
        min = max;
        max = temp;
      }
      boolean expected = count >= min && count <= max;
      TokenStream stream = new KeywordTokenizer();
      ((Tokenizer)stream).setReader(new StringReader(text));
      stream = new CodepointCountFilter(stream, min, max);
      stream.reset();
      assertEquals(expected, stream.incrementToken());
      stream.end();
      stream.close();
    }
  }

  /**
   * checking the validity of constructor arguments
   */
  @Test(expected = IllegalArgumentException.class)
  public void testIllegalArguments() throws Exception {
    new CodepointCountFilter(whitespaceMockTokenizer("accept only valid arguments"), 4, 1);
  }
}
