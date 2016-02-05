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
import java.nio.CharBuffer;
import java.util.Random;

import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.util.AttributeFactory;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.RegExp;

import com.carrotsearch.randomizedtesting.RandomizedContext;

/**
 * Tokenizer for testing.
 * <p>
 * This tokenizer is a replacement for {@link #WHITESPACE}, {@link #SIMPLE}, and {@link #KEYWORD}
 * tokenizers. If you are writing a component such as a TokenFilter, it's a great idea to test
 * it wrapping this tokenizer instead for extra checks. This tokenizer has the following behavior:
 * <ul>
 *   <li>An internal state-machine is used for checking consumer consistency. These checks can
 *       be disabled with {@link #setEnableChecks(boolean)}.
 *   <li>For convenience, optionally lowercases terms that it outputs.
 * </ul>
 */
public class MockTokenizer extends Tokenizer {
  /** Acts Similar to WhitespaceTokenizer */
  public static final CharacterRunAutomaton WHITESPACE = 
    new CharacterRunAutomaton(new RegExp("[^ \t\r\n]+").toAutomaton());
  /** Acts Similar to KeywordTokenizer.
   * TODO: Keyword returns an "empty" token for an empty reader... 
   */
  public static final CharacterRunAutomaton KEYWORD =
    new CharacterRunAutomaton(new RegExp(".*").toAutomaton());
  /** Acts like LetterTokenizer. */
  // the ugly regex below is incomplete Unicode 5.2 [:Letter:]
  public static final CharacterRunAutomaton SIMPLE =
    new CharacterRunAutomaton(new RegExp("[A-Za-zªµºÀ-ÖØ-öø-ˁ一-鿌]+").toAutomaton());

  private final CharacterRunAutomaton runAutomaton;
  private final boolean lowerCase;
  private final int maxTokenLength;
  public static final int DEFAULT_MAX_TOKEN_LENGTH = Integer.MAX_VALUE;
  private int state;

  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
  int off = 0;
  
  // buffered state (previous codepoint and offset). we replay this once we
  // hit a reject state in case it's permissible as the start of a new term.
  int bufferedCodePoint = -1; // -1 indicates empty buffer
  int bufferedOff = -1;

  // TODO: "register" with LuceneTestCase to ensure all streams are closed() ?
  // currently, we can only check that the lifecycle is correct if someone is reusing,
  // but not for "one-offs".
  private static enum State { 
    SETREADER,       // consumer set a reader input either via ctor or via reset(Reader)
    RESET,           // consumer has called reset()
    INCREMENT,       // consumer is consuming, has called incrementToken() == true
    INCREMENT_FALSE, // consumer has called incrementToken() which returned false
    END,             // consumer has called end() to perform end of stream operations
    CLOSE            // consumer has called close() to release any resources
  };
  
  private State streamState = State.CLOSE;
  private int lastOffset = 0; // only for checks
  private boolean enableChecks = true;
  
  // evil: but we don't change the behavior with this random, we only switch up how we read
  private final Random random = new Random(RandomizedContext.current().getRandom().nextLong());
  
  public MockTokenizer(AttributeFactory factory, CharacterRunAutomaton runAutomaton, boolean lowerCase, int maxTokenLength) {
    super(factory);
    this.runAutomaton = runAutomaton;
    this.lowerCase = lowerCase;
    this.state = runAutomaton.getInitialState();
    this.maxTokenLength = maxTokenLength;
  }

  public MockTokenizer(CharacterRunAutomaton runAutomaton, boolean lowerCase, int maxTokenLength) {
    this(BaseTokenStreamTestCase.newAttributeFactory(), runAutomaton, lowerCase, maxTokenLength);
  }

  public MockTokenizer(CharacterRunAutomaton runAutomaton, boolean lowerCase) {
    this(runAutomaton, lowerCase, DEFAULT_MAX_TOKEN_LENGTH);
  }
  /** Calls {@link #MockTokenizer(CharacterRunAutomaton, boolean) MockTokenizer(Reader, WHITESPACE, true)} */
  public MockTokenizer() {
    this(WHITESPACE, true);
  }

  public MockTokenizer(AttributeFactory factory, CharacterRunAutomaton runAutomaton, boolean lowerCase) {
    this(factory, runAutomaton, lowerCase, DEFAULT_MAX_TOKEN_LENGTH);
  }

  /** Calls {@link #MockTokenizer(AttributeFactory,CharacterRunAutomaton,boolean)
   *                MockTokenizer(AttributeFactory, Reader, WHITESPACE, true)} */
  public MockTokenizer(AttributeFactory factory) {
    this(factory, WHITESPACE, true);
  }
  
  // we allow some checks (e.g. state machine) to be turned off.
  // turning off checks just means we suppress exceptions from them
  private void fail(String message) {
    if (enableChecks) {
      throw new IllegalStateException(message);
    }
  }
  
  private void failAlways(String message) {
    throw new IllegalStateException(message);
  }

  @Override
  public final boolean incrementToken() throws IOException {
    if (streamState != State.RESET && streamState != State.INCREMENT) {
      fail("incrementToken() called while in wrong state: " + streamState);
    }

    clearAttributes();
    for (;;) {
      int startOffset;
      int cp;
      if (bufferedCodePoint >= 0) {
        cp = bufferedCodePoint;
        startOffset = bufferedOff;
        bufferedCodePoint = -1;
      } else {
        startOffset = off;
        cp = readCodePoint();
      }
      if (cp < 0) {
        break;
      } else if (isTokenChar(cp)) {
        int endOffset;
        do {
          char chars[] = Character.toChars(normalize(cp));
          for (int i = 0; i < chars.length; i++)
            termAtt.append(chars[i]);
          endOffset = off;
          if (termAtt.length() >= maxTokenLength) {
            break;
          }
          cp = readCodePoint();
        } while (cp >= 0 && isTokenChar(cp));
        
        if (termAtt.length() < maxTokenLength) {
          // buffer up, in case the "rejected" char can start a new word of its own
          bufferedCodePoint = cp;
          bufferedOff = endOffset;
        } else {
          // otherwise, it's because we hit term limit.
          bufferedCodePoint = -1;
        }
        int correctedStartOffset = correctOffset(startOffset);
        int correctedEndOffset = correctOffset(endOffset);
        if (correctedStartOffset < 0) {
          failAlways("invalid start offset: " + correctedStartOffset + ", before correction: " + startOffset);
        }
        if (correctedEndOffset < 0) {
          failAlways("invalid end offset: " + correctedEndOffset + ", before correction: " + endOffset);
        }
        if (correctedStartOffset < lastOffset) {
          failAlways("start offset went backwards: " + correctedStartOffset + ", before correction: " + startOffset + ", lastOffset: " + lastOffset);
        }
        lastOffset = correctedStartOffset;
        if (correctedEndOffset < correctedStartOffset) {
          failAlways("end offset: " + correctedEndOffset + " is before start offset: " + correctedStartOffset);
        }
        offsetAtt.setOffset(correctedStartOffset, correctedEndOffset);
        if (state == -1 || runAutomaton.isAccept(state)) {
          // either we hit a reject state (longest match), or end-of-text, but in an accept state
          streamState = State.INCREMENT;
          return true;
        }
      }
    }
    streamState = State.INCREMENT_FALSE;
    return false;
  }

  protected int readCodePoint() throws IOException {
    int ch = readChar();
    if (ch < 0) {
      return ch;
    } else {
      if (Character.isLowSurrogate((char) ch)) {
        failAlways("unpaired low surrogate: " + Integer.toHexString(ch));
      }
      off++;
      if (Character.isHighSurrogate((char) ch)) {
        int ch2 = readChar();
        if (ch2 >= 0) {
          off++;
          if (!Character.isLowSurrogate((char) ch2)) {
            failAlways("unpaired high surrogate: " + Integer.toHexString(ch) + ", followed by: " + Integer.toHexString(ch2));
          }
          return Character.toCodePoint((char) ch, (char) ch2);
        } else {
          failAlways("stream ends with unpaired high surrogate: " + Integer.toHexString(ch));
        }
      }
      return ch;
    }
  }
  
  protected int readChar() throws IOException {
    switch(random.nextInt(10)) {
      case 0: {
        // read(char[])
        char c[] = new char[1];
        int ret = input.read(c);
        return ret < 0 ? ret : c[0];
      }
      case 1: {
        // read(char[], int, int)
        char c[] = new char[2];
        int ret = input.read(c, 1, 1);
        return ret < 0 ? ret : c[1];
      }
      case 2: {
        // read(CharBuffer)
        char c[] = new char[1];
        CharBuffer cb = CharBuffer.wrap(c);
        int ret = input.read(cb);
        return ret < 0 ? ret : c[0];
      }
      default: 
        // read()
        return input.read();
    }
  }

  protected boolean isTokenChar(int c) {
    if (state < 0) {
      state = runAutomaton.getInitialState();
    }
    state = runAutomaton.step(state, c);
    if (state < 0) {
      return false;
    } else {
      return true;
    }
  }
  
  protected int normalize(int c) {
    return lowerCase ? Character.toLowerCase(c) : c;
  }

  @Override
  public void reset() throws IOException {
    try {
      super.reset();
      state = runAutomaton.getInitialState();
      lastOffset = off = 0;
      bufferedCodePoint = -1;
      if (streamState == State.RESET) {
        fail("double reset()");
      }
    } finally {
      streamState = State.RESET;
    }
  }
  
  @Override
  public void close() throws IOException {
    try {
      super.close();
      // in some exceptional cases (e.g. TestIndexWriterExceptions) a test can prematurely close()
      // these tests should disable this check, by default we check the normal workflow.
      // TODO: investigate the CachingTokenFilter "double-close"... for now we ignore this
      if (!(streamState == State.END || streamState == State.CLOSE)) {
        fail("close() called in wrong state: " + streamState);
      }
    } finally {
      streamState = State.CLOSE;
    }
  }

  @Override
  void setReaderTestPoint() {
    try {
      if (streamState != State.CLOSE) {
        fail("setReader() called in wrong state: " + streamState);
      }
    } finally {
      streamState = State.SETREADER;
    }
  }

  @Override
  public void end() throws IOException {
    try {
      super.end();
      int finalOffset = correctOffset(off);
      offsetAtt.setOffset(finalOffset, finalOffset);
      // some tokenizers, such as limiting tokenizers, call end() before incrementToken() returns false.
      // these tests should disable this check (in general you should consume the entire stream)
      if (streamState != State.INCREMENT_FALSE) {
        fail("end() called before incrementToken() returned false!");
      }
    } finally {
      streamState = State.END;
    }
  }

  /** 
   * Toggle consumer workflow checking: if your test consumes tokenstreams normally you
   * should leave this enabled.
   */
  public void setEnableChecks(boolean enableChecks) {
    this.enableChecks = enableChecks;
  }
}
