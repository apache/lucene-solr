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
package org.apache.lucene.analysis.util;


import java.io.IOException;
import java.io.StringReader;
import java.util.Locale;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.core.LetterTokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;


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
    Tokenizer tokenizer = new LetterTokenizer(newAttributeFactory());
    tokenizer.setReader(new StringReader(builder.toString()));
    assertTokenStreamContents(new LowerCaseFilter(tokenizer), builder.toString().toLowerCase(Locale.ROOT).split(" "));
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
      Tokenizer tokenizer = new LetterTokenizer(newAttributeFactory());
      tokenizer.setReader(new StringReader(builder.toString()));
      assertTokenStreamContents(new LowerCaseFilter(tokenizer), new String[] {builder.toString().toLowerCase(Locale.ROOT)});
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
    Tokenizer tokenizer = new LetterTokenizer(newAttributeFactory());
    tokenizer.setReader(new StringReader(builder.toString() + builder.toString()));
    assertTokenStreamContents(new LowerCaseFilter(tokenizer), new String[] {builder.toString().toLowerCase(Locale.ROOT), builder.toString().toLowerCase(Locale.ROOT)});
  }

  /*
   * tests the max word length passed as parameter - tokenizer will split at the passed position char no matter what happens
   */
  public void testCustomMaxTokenLength() throws IOException {

    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < 100; i++) {
      builder.append("A");
    }
    Tokenizer tokenizer = new LetterTokenizer(newAttributeFactory(), 100);
    // Tricky, passing two copies of the string to the reader....
    tokenizer.setReader(new StringReader(builder.toString() + builder.toString()));
    assertTokenStreamContents(new LowerCaseFilter(tokenizer), new String[]{builder.toString().toLowerCase(Locale.ROOT),
        builder.toString().toLowerCase(Locale.ROOT) });

    Exception e = expectThrows(IllegalArgumentException.class, () ->
        new LetterTokenizer(newAttributeFactory(), -1));
    assertEquals("maxTokenLen must be greater than 0 and less than 1048576 passed: -1", e.getMessage());

    tokenizer = new LetterTokenizer(newAttributeFactory(), 100);
    tokenizer.setReader(new StringReader(builder.toString() + builder.toString()));
    assertTokenStreamContents(tokenizer, new String[]{builder.toString(), builder.toString()});


    // Let's test that we can get a token longer than 255 through.
    builder.setLength(0);
    for (int i = 0; i < 500; i++) {
      builder.append("Z");
    }
    tokenizer = new LetterTokenizer(newAttributeFactory(), 500);
    tokenizer.setReader(new StringReader(builder.toString()));
    assertTokenStreamContents(tokenizer, new String[]{builder.toString()});

    
    // Just to be sure what is happening here, token lengths of zero make no sense, 
    // Let's try the edge cases, token > I/O buffer (4096)
    builder.setLength(0);
    for (int i = 0; i < 600; i++) {
      builder.append("aUrOkIjq"); // 600 * 8 = 4800 chars.
    }

    e = expectThrows(IllegalArgumentException.class, () ->
        new LetterTokenizer(newAttributeFactory(), 0));
    assertEquals("maxTokenLen must be greater than 0 and less than 1048576 passed: 0", e.getMessage());

    e = expectThrows(IllegalArgumentException.class, () ->
        new LetterTokenizer(newAttributeFactory(), 10_000_000));
    assertEquals("maxTokenLen must be greater than 0 and less than 1048576 passed: 10000000", e.getMessage());

    tokenizer = new LetterTokenizer(newAttributeFactory(), 4800);
    tokenizer.setReader(new StringReader(builder.toString()));
    assertTokenStreamContents(new LowerCaseFilter(tokenizer), new String[]{builder.toString().toLowerCase(Locale.ROOT)});


    e = expectThrows(IllegalArgumentException.class, () ->
        new KeywordTokenizer(newAttributeFactory(), 0));
    assertEquals("maxTokenLen must be greater than 0 and less than 1048576 passed: 0", e.getMessage());

    e = expectThrows(IllegalArgumentException.class, () ->
        new KeywordTokenizer(newAttributeFactory(), 10_000_000));
    assertEquals("maxTokenLen must be greater than 0 and less than 1048576 passed: 10000000", e.getMessage());


    tokenizer = new KeywordTokenizer(newAttributeFactory(), 4800);
    tokenizer.setReader(new StringReader(builder.toString()));
    assertTokenStreamContents(tokenizer, new String[]{builder.toString()});

    e = expectThrows(IllegalArgumentException.class, () ->
        new LetterTokenizer(newAttributeFactory(), 0));
    assertEquals("maxTokenLen must be greater than 0 and less than 1048576 passed: 0", e.getMessage());

    e = expectThrows(IllegalArgumentException.class, () ->
        new LetterTokenizer(newAttributeFactory(), 2_000_000));
    assertEquals("maxTokenLen must be greater than 0 and less than 1048576 passed: 2000000", e.getMessage());

    tokenizer = new LetterTokenizer(newAttributeFactory(), 4800);
    tokenizer.setReader(new StringReader(builder.toString()));
    assertTokenStreamContents(tokenizer, new String[]{builder.toString()});

    e = expectThrows(IllegalArgumentException.class, () ->
        new WhitespaceTokenizer(newAttributeFactory(), 0));
    assertEquals("maxTokenLen must be greater than 0 and less than 1048576 passed: 0", e.getMessage());

    e = expectThrows(IllegalArgumentException.class, () ->
        new WhitespaceTokenizer(newAttributeFactory(), 3_000_000));
    assertEquals("maxTokenLen must be greater than 0 and less than 1048576 passed: 3000000", e.getMessage());

    tokenizer = new WhitespaceTokenizer(newAttributeFactory(), 4800);
    tokenizer.setReader(new StringReader(builder.toString()));
    assertTokenStreamContents(tokenizer, new String[]{builder.toString()});

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
    Tokenizer tokenizer = new LetterTokenizer(newAttributeFactory());
    tokenizer.setReader(new StringReader(builder.toString() + builder.toString()));
    assertTokenStreamContents(new LowerCaseFilter(tokenizer), new String[] {builder.toString().toLowerCase(Locale.ROOT), builder.toString().toLowerCase(Locale.ROOT)});
  }
  
  public void testDefinitionUsingMethodReference1() throws Exception {
    final StringReader reader = new StringReader("Tokenizer Test");
    final Tokenizer tokenizer = CharTokenizer.fromSeparatorCharPredicate(Character::isWhitespace);
    tokenizer.setReader(reader);
    assertTokenStreamContents(tokenizer, new String[] { "Tokenizer", "Test" });
  }
  
  public void testDefinitionUsingMethodReference2() throws Exception {
    final StringReader reader = new StringReader("Tokenizer(Test)");
    final Tokenizer tokenizer = CharTokenizer.fromTokenCharPredicate(Character::isLetter);
    tokenizer.setReader(reader);
    assertTokenStreamContents(tokenizer, new String[] { "Tokenizer", "Test" });
  }
  
  public void testDefinitionUsingLambda() throws Exception {
    final StringReader reader = new StringReader("Tokenizer\u00A0Test Foo");
    final Tokenizer tokenizer = CharTokenizer.fromSeparatorCharPredicate(c -> c == '\u00A0' || Character.isWhitespace(c));
    tokenizer.setReader(reader);
    assertTokenStreamContents(tokenizer, new String[] { "Tokenizer", "Test", "Foo" });
  }
  
}
