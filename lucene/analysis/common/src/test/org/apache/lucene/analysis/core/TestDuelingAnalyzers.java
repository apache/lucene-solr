package org.apache.lucene.analysis.core;

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

import java.io.Reader;
import java.io.StringReader;
import java.util.Random;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockReaderWrapper;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.BasicOperations;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.State;
import org.apache.lucene.util.automaton.Transition;

/**
 * Compares MockTokenizer (which is simple with no optimizations) with equivalent 
 * core tokenizers (that have optimizations like buffering).
 * 
 * Any tests here need to probably consider unicode version of the JRE (it could
 * cause false fails).
 */
public class TestDuelingAnalyzers extends LuceneTestCase {
  private CharacterRunAutomaton jvmLetter;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    // build an automaton matching this jvm's letter definition
    State initial = new State();
    State accept = new State();
    accept.setAccept(true);
    for (int i = 0; i <= 0x10FFFF; i++) {
      if (Character.isLetter(i)) {
        initial.addTransition(new Transition(i, i, accept));
      }
    }
    Automaton single = new Automaton(initial);
    single.reduce();
    Automaton repeat = BasicOperations.repeat(single);
    jvmLetter = new CharacterRunAutomaton(repeat);
  }
  
  public void testLetterAscii() throws Exception {
    Random random = random();
    Analyzer left = new MockAnalyzer(random, jvmLetter, false);
    Analyzer right = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer tokenizer = new LetterTokenizer(TEST_VERSION_CURRENT, reader);
        return new TokenStreamComponents(tokenizer, tokenizer);
      }
    };
    for (int i = 0; i < 1000; i++) {
      String s = _TestUtil.randomSimpleString(random);
      assertEquals(s, left.tokenStream("foo", newStringReader(s)), 
                   right.tokenStream("foo", newStringReader(s)));
    }
  }
  
  // not so useful since its all one token?!
  public void testLetterAsciiHuge() throws Exception {
    Random random = random();
    int maxLength = 8192; // CharTokenizer.IO_BUFFER_SIZE*2
    MockAnalyzer left = new MockAnalyzer(random, jvmLetter, false);
    left.setMaxTokenLength(255); // match CharTokenizer's max token length
    Analyzer right = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer tokenizer = new LetterTokenizer(TEST_VERSION_CURRENT, reader);
        return new TokenStreamComponents(tokenizer, tokenizer);
      }
    };
    int numIterations = atLeast(50);
    for (int i = 0; i < numIterations; i++) {
      String s = _TestUtil.randomSimpleString(random, maxLength);
      assertEquals(s, left.tokenStream("foo", newStringReader(s)), 
                   right.tokenStream("foo", newStringReader(s)));
    }
  }
  
  public void testLetterHtmlish() throws Exception {
    Random random = random();
    Analyzer left = new MockAnalyzer(random, jvmLetter, false);
    Analyzer right = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer tokenizer = new LetterTokenizer(TEST_VERSION_CURRENT, reader);
        return new TokenStreamComponents(tokenizer, tokenizer);
      }
    };
    for (int i = 0; i < 1000; i++) {
      String s = _TestUtil.randomHtmlishString(random, 20);
      assertEquals(s, left.tokenStream("foo", newStringReader(s)), 
                   right.tokenStream("foo", newStringReader(s)));
    }
  }
  
  public void testLetterHtmlishHuge() throws Exception {
    Random random = random();
    int maxLength = 1024; // this is number of elements, not chars!
    MockAnalyzer left = new MockAnalyzer(random, jvmLetter, false);
    left.setMaxTokenLength(255); // match CharTokenizer's max token length
    Analyzer right = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer tokenizer = new LetterTokenizer(TEST_VERSION_CURRENT, reader);
        return new TokenStreamComponents(tokenizer, tokenizer);
      }
    };
    int numIterations = atLeast(50);
    for (int i = 0; i < numIterations; i++) {
      String s = _TestUtil.randomHtmlishString(random, maxLength);
      assertEquals(s, left.tokenStream("foo", newStringReader(s)), 
                   right.tokenStream("foo", newStringReader(s)));
    }
  }
  
  public void testLetterUnicode() throws Exception {
    Random random = random();
    Analyzer left = new MockAnalyzer(random(), jvmLetter, false);
    Analyzer right = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer tokenizer = new LetterTokenizer(TEST_VERSION_CURRENT, reader);
        return new TokenStreamComponents(tokenizer, tokenizer);
      }
    };
    for (int i = 0; i < 1000; i++) {
      String s = _TestUtil.randomUnicodeString(random);
      assertEquals(s, left.tokenStream("foo", newStringReader(s)), 
                   right.tokenStream("foo", newStringReader(s)));
    }
  }
  
  public void testLetterUnicodeHuge() throws Exception {
    Random random = random();
    int maxLength = 4300; // CharTokenizer.IO_BUFFER_SIZE + fudge
    MockAnalyzer left = new MockAnalyzer(random, jvmLetter, false);
    left.setMaxTokenLength(255); // match CharTokenizer's max token length
    Analyzer right = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer tokenizer = new LetterTokenizer(TEST_VERSION_CURRENT, reader);
        return new TokenStreamComponents(tokenizer, tokenizer);
      }
    };
    int numIterations = atLeast(50);
    for (int i = 0; i < numIterations; i++) {
      String s = _TestUtil.randomUnicodeString(random, maxLength);
      assertEquals(s, left.tokenStream("foo", newStringReader(s)), 
                   right.tokenStream("foo", newStringReader(s)));
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
  
  // TODO: maybe push this out to _TestUtil or LuceneTestCase and always use it instead?
  private static Reader newStringReader(String s) {
    Random random = random();
    Reader r = new StringReader(s);
    if (random.nextBoolean()) {
      r = new MockReaderWrapper(random, r);
    }
    return r;
  }
}
