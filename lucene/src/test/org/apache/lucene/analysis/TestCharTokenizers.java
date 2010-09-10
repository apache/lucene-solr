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

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

import org.apache.lucene.util.Version;

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
    int num = 1024 + random.nextInt(1024);
    num *= RANDOM_MULTIPLIER;
    for (int i = 1; i < num; i++) {
      builder.append("\ud801\udc1cabc");
      if((i % 10) == 0)
        builder.append(" ");
    }
    // internal buffer size is 1024 make sure we have a surrogate pair right at the border
    builder.insert(1023, "\ud801\udc1c");
    LowerCaseTokenizer tokenizer = new LowerCaseTokenizer(
        TEST_VERSION_CURRENT, new StringReader(builder.toString()));
    assertTokenStreamContents(tokenizer, builder.toString().toLowerCase().split(" "));
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
      LowerCaseTokenizer tokenizer = new LowerCaseTokenizer(
          TEST_VERSION_CURRENT, new StringReader(builder.toString()));
      assertTokenStreamContents(tokenizer, new String[] {builder.toString().toLowerCase()});
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
    LowerCaseTokenizer tokenizer = new LowerCaseTokenizer(
        TEST_VERSION_CURRENT, new StringReader(builder.toString() + builder.toString()));
    assertTokenStreamContents(tokenizer, new String[] {builder.toString().toLowerCase(), builder.toString().toLowerCase()});
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
    LowerCaseTokenizer tokenizer = new LowerCaseTokenizer(
        TEST_VERSION_CURRENT, new StringReader(builder.toString() + builder.toString()));
    assertTokenStreamContents(tokenizer, new String[] {builder.toString().toLowerCase(), builder.toString().toLowerCase()});
  }

  public void testLowerCaseTokenizer() throws IOException {
    StringReader reader = new StringReader("Tokenizer \ud801\udc1ctest");
    LowerCaseTokenizer tokenizer = new LowerCaseTokenizer(TEST_VERSION_CURRENT,
        reader);
    assertTokenStreamContents(tokenizer, new String[] { "tokenizer",
        "\ud801\udc44test" });
  }

  public void testLowerCaseTokenizerBWCompat() throws IOException {
    StringReader reader = new StringReader("Tokenizer \ud801\udc1ctest");
    LowerCaseTokenizer tokenizer = new LowerCaseTokenizer(Version.LUCENE_30,
        reader);
    assertTokenStreamContents(tokenizer, new String[] { "tokenizer", "test" });
  }

  public void testWhitespaceTokenizer() throws IOException {
    StringReader reader = new StringReader("Tokenizer \ud801\udc1ctest");
    WhitespaceTokenizer tokenizer = new WhitespaceTokenizer(TEST_VERSION_CURRENT,
        reader);
    assertTokenStreamContents(tokenizer, new String[] { "Tokenizer",
        "\ud801\udc1ctest" });
  }

  public void testWhitespaceTokenizerBWCompat() throws IOException {
    StringReader reader = new StringReader("Tokenizer \ud801\udc1ctest");
    WhitespaceTokenizer tokenizer = new WhitespaceTokenizer(Version.LUCENE_30,
        reader);
    assertTokenStreamContents(tokenizer, new String[] { "Tokenizer",
        "\ud801\udc1ctest" });
  }

  public void testIsTokenCharCharInSubclass() {
    new TestingCharTokenizer(Version.LUCENE_30, new StringReader(""));
    try {
      new TestingCharTokenizer(TEST_VERSION_CURRENT, new StringReader(""));
      fail("version 3.1 is not permitted if char based method is implemented");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  public void testNormalizeCharInSubclass() {
    new TestingCharTokenizerNormalize(Version.LUCENE_30, new StringReader(""));
    try {
      new TestingCharTokenizerNormalize(TEST_VERSION_CURRENT,
          new StringReader(""));
      fail("version 3.1 is not permitted if char based method is implemented");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  public void testNormalizeAndIsTokenCharCharInSubclass() {
    new TestingCharTokenizerNormalizeIsTokenChar(Version.LUCENE_30,
        new StringReader(""));
    try {
      new TestingCharTokenizerNormalizeIsTokenChar(TEST_VERSION_CURRENT,
          new StringReader(""));
      fail("version 3.1 is not permitted if char based method is implemented");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  static final class TestingCharTokenizer extends CharTokenizer {
    public TestingCharTokenizer(Version matchVersion, Reader input) {
      super(matchVersion, input);
    }

    @Override
    protected boolean isTokenChar(int c) {
      return Character.isLetter(c);
    }

    @Deprecated @Override
    protected boolean isTokenChar(char c) {
      return Character.isLetter(c);
    }
  }

  static final class TestingCharTokenizerNormalize extends CharTokenizer {
    public TestingCharTokenizerNormalize(Version matchVersion, Reader input) {
      super(matchVersion, input);
    }

    @Deprecated @Override
    protected char normalize(char c) {
      return c;
    }

    @Override
    protected int normalize(int c) {
      return c;
    }
  }

  static final class TestingCharTokenizerNormalizeIsTokenChar extends CharTokenizer {
    public TestingCharTokenizerNormalizeIsTokenChar(Version matchVersion,
        Reader input) {
      super(matchVersion, input);
    }

    @Deprecated @Override
    protected char normalize(char c) {
      return c;
    }

    @Override
    protected int normalize(int c) {
      return c;
    }

    @Override
    protected boolean isTokenChar(int c) {
      return Character.isLetter(c);
    }

    @Deprecated @Override
    protected boolean isTokenChar(char c) {
      return Character.isLetter(c);
    }
  }
}
