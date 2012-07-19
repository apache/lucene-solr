package org.apache.lucene.analysis.util;

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
import java.util.Locale;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LetterTokenizer;
import org.apache.lucene.analysis.core.LowerCaseTokenizer;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.util._TestUtil;


/**
 * Testcase for {@link CharTokenizer} subclasses
 */
public class TestCharTokenizers extends BaseTokenStreamTestCase {

  /*
   * test to read surrogate pairs without loosing the pairing 
   * if the surrogate pair is at the border of the internal IO buffer
   */
  public void testReadSupplementaryChars() throws IOException {
    StringBuilder builder = new StringBuilder();
    // create random input
    int num = 1024 + random().nextInt(1024);
    num *= RANDOM_MULTIPLIER;
    for (int i = 1; i < num; i++) {
      builder.append("\ud801\udc1cabc");
      if((i % 10) == 0)
        builder.append(" ");
    }
    // internal buffer size is 1024 make sure we have a surrogate pair right at the border
    builder.insert(1023, "\ud801\udc1c");
    Tokenizer tokenizer = new LowerCaseTokenizer(TEST_VERSION_CURRENT, new StringReader(builder.toString()));
    assertTokenStreamContents(tokenizer, builder.toString().toLowerCase(Locale.ROOT).split(" "));
  }
  
  /*
   * test to extend the buffer TermAttribute buffer internally. If the internal
   * alg that extends the size of the char array only extends by 1 char and the
   * next char to be filled in is a supplementary codepoint (using 2 chars) an
   * index out of bound exception is triggered.
   */
  public void testExtendCharBuffer() throws IOException {
    for (int i = 0; i < 40; i++) {
      StringBuilder builder = new StringBuilder();
      for (int j = 0; j < 1+i; j++) {
        builder.append("a");
      }
      builder.append("\ud801\udc1cabc");
      Tokenizer tokenizer = new LowerCaseTokenizer(TEST_VERSION_CURRENT, new StringReader(builder.toString()));
      assertTokenStreamContents(tokenizer, new String[] {builder.toString().toLowerCase(Locale.ROOT)});
    }
  }
  
  /*
   * tests the max word length of 255 - tokenizer will split at the 255 char no matter what happens
   */
  public void testMaxWordLength() throws IOException {
    StringBuilder builder = new StringBuilder();

    for (int i = 0; i < 255; i++) {
      builder.append("A");
    }
    Tokenizer tokenizer = new LowerCaseTokenizer(TEST_VERSION_CURRENT, new StringReader(builder.toString() + builder.toString()));
    assertTokenStreamContents(tokenizer, new String[] {builder.toString().toLowerCase(Locale.ROOT), builder.toString().toLowerCase(Locale.ROOT)});
  }
  
  /*
   * tests the max word length of 255 with a surrogate pair at position 255
   */
  public void testMaxWordLengthWithSupplementary() throws IOException {
    StringBuilder builder = new StringBuilder();

    for (int i = 0; i < 254; i++) {
      builder.append("A");
    }
    builder.append("\ud801\udc1c");
    Tokenizer tokenizer = new LowerCaseTokenizer(TEST_VERSION_CURRENT, new StringReader(builder.toString() + builder.toString()));
    assertTokenStreamContents(tokenizer, new String[] {builder.toString().toLowerCase(Locale.ROOT), builder.toString().toLowerCase(Locale.ROOT)});
  }
  
  // LUCENE-3642: normalize SMP->BMP and check that offsets are correct
  public void testCrossPlaneNormalization() throws IOException {
    Analyzer analyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer tokenizer = new LetterTokenizer(TEST_VERSION_CURRENT, reader) {
          @Override
          protected int normalize(int c) {
            if (c > 0xffff) {
              return 'δ';
            } else {
              return c;
            }
          }
        };
        return new TokenStreamComponents(tokenizer, tokenizer);
      }
    };
    int num = 1000 * RANDOM_MULTIPLIER;
    for (int i = 0; i < num; i++) {
      String s = _TestUtil.randomUnicodeString(random());
      TokenStream ts = analyzer.tokenStream("foo", new StringReader(s));
      ts.reset();
      OffsetAttribute offsetAtt = ts.addAttribute(OffsetAttribute.class);
      while (ts.incrementToken()) {
        String highlightedText = s.substring(offsetAtt.startOffset(), offsetAtt.endOffset());
        for (int j = 0, cp = 0; j < highlightedText.length(); j += Character.charCount(cp)) {
          cp = highlightedText.codePointAt(j);
          assertTrue("non-letter:" + Integer.toHexString(cp), Character.isLetter(cp));
        }
      }
      ts.end();
      ts.close();
    }
    // just for fun
    checkRandomData(random(), analyzer, num);
  }
  
  // LUCENE-3642: normalize BMP->SMP and check that offsets are correct
  public void testCrossPlaneNormalization2() throws IOException {
    Analyzer analyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer tokenizer = new LetterTokenizer(TEST_VERSION_CURRENT, reader) {
          @Override
          protected int normalize(int c) {
            if (c <= 0xffff) {
              return 0x1043C;
            } else {
              return c;
            }
          }
        };
        return new TokenStreamComponents(tokenizer, tokenizer);
      }
    };
    int num = 1000 * RANDOM_MULTIPLIER;
    for (int i = 0; i < num; i++) {
      String s = _TestUtil.randomUnicodeString(random());
      TokenStream ts = analyzer.tokenStream("foo", new StringReader(s));
      ts.reset();
      OffsetAttribute offsetAtt = ts.addAttribute(OffsetAttribute.class);
      while (ts.incrementToken()) {
        String highlightedText = s.substring(offsetAtt.startOffset(), offsetAtt.endOffset());
        for (int j = 0, cp = 0; j < highlightedText.length(); j += Character.charCount(cp)) {
          cp = highlightedText.codePointAt(j);
          assertTrue("non-letter:" + Integer.toHexString(cp), Character.isLetter(cp));
        }
      }
      ts.end();
      ts.close();
    }
    // just for fun
    checkRandomData(random(), analyzer, num);
  }
}
