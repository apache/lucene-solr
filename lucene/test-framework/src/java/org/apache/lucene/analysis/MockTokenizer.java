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

import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.util.AttributeSource.AttributeFactory;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.RegExp;

/**
 * Tokenizer for testing.
 * <p>
 * This tokenizer is a replacement for {@link #WHITESPACE}, {@link #SIMPLE}, and {@link #KEYWORD}
 * tokenizers. If you are writing a component such as a TokenFilter, its a great idea to test
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
    new CharacterRunAutomaton(new RegExp("[A-Za-zªµºÀ-ÖØ-öø-Ｚ]+").toAutomaton());

  private final CharacterRunAutomaton runAutomaton;
  private final boolean lowerCase;
  private final int maxTokenLength;
  public static final int DEFAULT_MAX_TOKEN_LENGTH = Integer.MAX_VALUE;
  private int state;

  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
  int off = 0;

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
  private int lastOffset = 0; // only for asserting
  private boolean enableChecks = true;
  
  public MockTokenizer(AttributeFactory factory, Reader input, CharacterRunAutomaton runAutomaton, boolean lowerCase, int maxTokenLength) {
    super(factory, input);
    this.runAutomaton = runAutomaton;
    this.lowerCase = lowerCase;
    this.state = runAutomaton.getInitialState();
    this.streamState = State.SETREADER;
    this.maxTokenLength = maxTokenLength;
  }

  public MockTokenizer(Reader input, CharacterRunAutomaton runAutomaton, boolean lowerCase, int maxTokenLength) {
    this(AttributeFactory.DEFAULT_ATTRIBUTE_FACTORY, input, runAutomaton, lowerCase, maxTokenLength);
  }

  public MockTokenizer(Reader input, CharacterRunAutomaton runAutomaton, boolean lowerCase) {
    this(input, runAutomaton, lowerCase, DEFAULT_MAX_TOKEN_LENGTH);
  }
  
  /** Calls {@link #MockTokenizer(Reader, CharacterRunAutomaton, boolean) MockTokenizer(Reader, WHITESPACE, true)} */
  public MockTokenizer(Reader input) {
    this(input, WHITESPACE, true);
  }
  
  @Override
  public final boolean incrementToken() throws IOException {
    assert !enableChecks || (streamState == State.RESET || streamState == State.INCREMENT) 
                            : "incrementToken() called while in wrong state: " + streamState;
    clearAttributes();
    for (;;) {
      int startOffset = off;
      int cp = readCodePoint();
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
        
        int correctedStartOffset = correctOffset(startOffset);
        int correctedEndOffset = correctOffset(endOffset);
        assert correctedStartOffset >= 0;
        assert correctedEndOffset >= 0;
        assert correctedStartOffset >= lastOffset;
        lastOffset = correctedStartOffset;
        assert correctedEndOffset >= correctedStartOffset;
        offsetAtt.setOffset(correctedStartOffset, correctedEndOffset);
        streamState = State.INCREMENT;
        return true;
      }
    }
    streamState = State.INCREMENT_FALSE;
    return false;
  }

  protected int readCodePoint() throws IOException {
    int ch = input.read();
    if (ch < 0) {
      return ch;
    } else {
      assert !Character.isLowSurrogate((char) ch);
      off++;
      if (Character.isHighSurrogate((char) ch)) {
        int ch2 = input.read();
        if (ch2 >= 0) {
          off++;
          assert Character.isLowSurrogate((char) ch2);
          return Character.toCodePoint((char) ch, (char) ch2);
        }
      }
      return ch;
    }
  }

  protected boolean isTokenChar(int c) {
    state = runAutomaton.step(state, c);
    if (state < 0) {
      state = runAutomaton.getInitialState();
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
    super.reset();
    state = runAutomaton.getInitialState();
    lastOffset = off = 0;
    assert !enableChecks || streamState != State.RESET : "double reset()";
    streamState = State.RESET;
  }
  
  @Override
  public void close() throws IOException {
    super.close();
    // in some exceptional cases (e.g. TestIndexWriterExceptions) a test can prematurely close()
    // these tests should disable this check, by default we check the normal workflow.
    // TODO: investigate the CachingTokenFilter "double-close"... for now we ignore this
    assert !enableChecks || streamState == State.END || streamState == State.CLOSE : "close() called in wrong state: " + streamState;
    streamState = State.CLOSE;
  }

  @Override
  public void reset(Reader input) throws IOException {
    super.reset(input);
    assert !enableChecks || streamState == State.CLOSE : "setReader() called in wrong state: " + streamState;
    streamState = State.SETREADER;
  }

  @Override
  public void end() throws IOException {
    int finalOffset = correctOffset(off);
    offsetAtt.setOffset(finalOffset, finalOffset);
    // some tokenizers, such as limiting tokenizers, call end() before incrementToken() returns false.
    // these tests should disable this check (in general you should consume the entire stream)
    try {
      assert !enableChecks || streamState == State.INCREMENT_FALSE : "end() called before incrementToken() returned false!";
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
