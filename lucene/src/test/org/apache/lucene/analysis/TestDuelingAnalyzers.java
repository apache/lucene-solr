package org.apache.lucene.analysis;

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

import java.io.Reader;
import java.io.StringReader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

/**
 * Compares MockTokenizer (which is simple with no optimizations) with equivalent 
 * core tokenizers (that have optimizations like buffering).
 * 
 * Any tests here need to probably consider unicode version of the JRE (it could
 * cause false fails).
 */
public class TestDuelingAnalyzers extends LuceneTestCase {
  
  public void testLetterAscii() throws Exception {
    Analyzer left = new MockAnalyzer(random, MockTokenizer.SIMPLE, false);
    Analyzer right = new ReusableAnalyzerBase() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer tokenizer = new LetterTokenizer(TEST_VERSION_CURRENT, reader);
        return new TokenStreamComponents(tokenizer, tokenizer);
      }
    };
    for (int i = 0; i < 10000; i++) {
      String s = _TestUtil.randomSimpleString(random);
      assertEquals(s, left.tokenStream("foo", new StringReader(s)), 
                   right.tokenStream("foo", new StringReader(s)));
    }
  }
  
  public void testLetterUnicode() throws Exception {
    Analyzer left = new MockAnalyzer(random, MockTokenizer.SIMPLE, false);
    Analyzer right = new ReusableAnalyzerBase() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer tokenizer = new LetterTokenizer(TEST_VERSION_CURRENT, reader);
        return new TokenStreamComponents(tokenizer, tokenizer);
      }
    };
    for (int i = 0; i < 10000; i++) {
      String s = _TestUtil.randomUnicodeString(random);
      assertEquals(s, left.tokenStream("foo", new StringReader(s)), 
                   right.tokenStream("foo", new StringReader(s)));
    }
  }
  
  // we only check a few core attributes here.
  // TODO: test other things
  public void assertEquals(String s, TokenStream left, TokenStream right) throws Exception {
    left.reset();
    right.reset();
    CharTermAttribute leftTerm = left.addAttribute(CharTermAttribute.class);
    CharTermAttribute rightTerm = right.addAttribute(CharTermAttribute.class);
    OffsetAttribute leftOffset = left.addAttribute(OffsetAttribute.class);
    OffsetAttribute rightOffset = right.addAttribute(OffsetAttribute.class);
    PositionIncrementAttribute leftPos = left.addAttribute(PositionIncrementAttribute.class);
    PositionIncrementAttribute rightPos = right.addAttribute(PositionIncrementAttribute.class);
    
    while (left.incrementToken()) {
      assertTrue("wrong number of tokens for input: " + s, right.incrementToken());
      assertEquals("wrong term text for input: " + s, leftTerm.toString(), rightTerm.toString());
      assertEquals("wrong position for input: " + s, leftPos.getPositionIncrement(), rightPos.getPositionIncrement());
      assertEquals("wrong start offset for input: " + s, leftOffset.startOffset(), rightOffset.startOffset());
      assertEquals("wrong end offset for input: " + s, leftOffset.endOffset(), rightOffset.endOffset());
    };
    assertFalse("wrong number of tokens for input: " + s, right.incrementToken());
    left.end();
    right.end();
    assertEquals("wrong final offset for input: " + s, leftOffset.endOffset(), rightOffset.endOffset());
    left.close();
    right.close();
  }
}
