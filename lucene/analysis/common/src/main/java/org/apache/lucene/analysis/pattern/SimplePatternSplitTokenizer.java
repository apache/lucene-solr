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
package org.apache.lucene.analysis.pattern;

import java.io.IOException;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.AttributeFactory;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;

/**
 * This tokenizer uses a Lucene {@link RegExp} or (expert usage) a pre-built determinized {@link Automaton}, to locate tokens.
 * The regexp syntax is more limited than {@link PatternTokenizer}, but the tokenization is quite a bit faster.  This is just
 * like {@link SimplePatternTokenizer} except that the pattern shold make valid token separator characters, like
 * {@code String.split}.  Empty string tokens are never produced.
 *
 * @lucene.experimental
 */

public final class SimplePatternSplitTokenizer extends Tokenizer {

  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);

  private final CharacterRunAutomaton runDFA;

  // TODO: this is copied from SimplePatternTokenizer, but there are subtle differences e.g. we track sepUpto an tokenUpto;
  // find a clean way to share it:

  // TODO: we could likely use a single rolling buffer instead of two separate char buffers here.  We could also use PushBackReader but I
  // suspect it's slowish:

  private char[] pendingChars = new char[8];
  private int tokenUpto;
  private int pendingLimit;
  private int pendingUpto;
  private int offset;
  private int sepUpto;
  private final char[] buffer = new char[1024];
  private int bufferLimit;
  private int bufferNextRead;

  /** See {@link RegExp} for the accepted syntax. */
  public SimplePatternSplitTokenizer(String regexp) {
    this(DEFAULT_TOKEN_ATTRIBUTE_FACTORY, regexp, Operations.DEFAULT_MAX_DETERMINIZED_STATES);
  }

  /** Runs a pre-built automaton. */
  public SimplePatternSplitTokenizer(Automaton dfa) {
    this(DEFAULT_TOKEN_ATTRIBUTE_FACTORY, dfa);
  }

  /** See {@link RegExp} for the accepted syntax. */
  public SimplePatternSplitTokenizer(AttributeFactory factory, String regexp, int maxDeterminizedStates) {
    this(factory, new RegExp(regexp).toAutomaton());
  }

  /** Runs a pre-built automaton. */
  public SimplePatternSplitTokenizer(AttributeFactory factory, Automaton dfa) {
    super(factory);

    // we require user to do this up front because it is a possibly very costly operation, and user may be creating us frequently, not
    // realizing this ctor is otherwise trappy
    if (dfa.isDeterministic() == false) {
      throw new IllegalArgumentException("please determinize the incoming automaton first");
    }

    runDFA = new CharacterRunAutomaton(dfa, Operations.DEFAULT_MAX_DETERMINIZED_STATES);
  }

  private void fillToken(int offsetStart) {
    termAtt.setLength(tokenUpto);
    offsetAtt.setOffset(correctOffset(offsetStart), correctOffset(offsetStart+tokenUpto));
  }

  @Override
  public boolean incrementToken() throws IOException {

    int offsetStart = offset;

    clearAttributes();

    tokenUpto = 0;

    while (true) {
      sepUpto = 0;

      // The runDFA operates in Unicode space, not UTF16 (java's char):
      int ch = nextCodePoint();
      if (ch == -1) {
        if (tokenUpto > 0) {
          fillToken(offsetStart);
          return true;
        } else {
          return false;
        }
      }
      int state = runDFA.step(0, ch);

      if (state != -1) {
        // a token separator just possibly started; keep scanning to see if the token is accepted:
        int lastAcceptLength = -1;
        do {

          if (runDFA.isAccept(state)) {
            // record that the token separator matches here, but keep scanning in case a longer match also works (greedy):
            lastAcceptLength = sepUpto;
          }

          ch = nextCodePoint();
          if (ch == -1) {
            break;
          }
          state = runDFA.step(state, ch);
        } while (state != -1);
        
        if (lastAcceptLength != -1) {
          // we found a token separator; strip the trailing separator we just matched from the token:
          int extra = sepUpto - lastAcceptLength;
          if (extra != 0) {
            pushBack(extra);
          }
          tokenUpto -= lastAcceptLength;
          if (tokenUpto > 0) {
            fillToken(offsetStart);
            return true;
          } else {
            // we matched one token separator immediately after another
            offsetStart = offset;
          }
        } else if (ch == -1) {
          if (tokenUpto > 0) {
            fillToken(offsetStart);
            return true;
          } else {
            return false;
          }
        } else {
          // false alarm: there was no token separator here; push back all but the first character we scanned
          pushBack(sepUpto-1);
        }
      }
    }
  }

  @Override
  public void end() throws IOException {
    super.end();
    final int ofs = correctOffset(offset + pendingLimit - pendingUpto);
    offsetAtt.setOffset(ofs, ofs);
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    offset = 0;
    pendingUpto = 0;
    pendingLimit = 0;
    sepUpto = 0;
    bufferNextRead = 0;
    bufferLimit = 0;
  }

  /** Pushes back the last {@code count} characters in current token's buffer. */
  private void pushBack(int count) {
    tokenUpto -= count;
    assert tokenUpto >= 0;
    if (pendingLimit == 0) {
      if (bufferLimit != -1 && bufferNextRead >= count) {
        // optimize common case when the chars we are pushing back are still in the buffer
        bufferNextRead -= count;
      } else {
        if (count > pendingChars.length) {
          pendingChars = ArrayUtil.grow(pendingChars, count);
        }
        System.arraycopy(termAtt.buffer(), tokenUpto, pendingChars, 0, count);
        pendingLimit = count;
      }
    } else {
      // we are pushing back what is already in our pending buffer
      pendingUpto -= count;
      assert pendingUpto >= 0;
    }
    offset -= count;
  }

  private void appendToToken(char ch) {
    char[] buffer = termAtt.buffer();
    if (tokenUpto == buffer.length) {
      buffer = termAtt.resizeBuffer(tokenUpto + 1);
    }
    buffer[tokenUpto++] = ch;
    sepUpto++;
  }

  private int nextCodeUnit() throws IOException {
    int result;
    if (pendingUpto < pendingLimit) {
      result = pendingChars[pendingUpto++];
      if (pendingUpto == pendingLimit) {
        // We used up the pending buffer
        pendingUpto = 0;
        pendingLimit = 0;
      }
      appendToToken((char) result);
      offset++;
    } else if (bufferLimit == -1) {
      return -1;
    } else {
      assert bufferNextRead <= bufferLimit: "bufferNextRead=" + bufferNextRead + " bufferLimit=" + bufferLimit;
      if (bufferNextRead == bufferLimit) {
        bufferLimit = input.read(buffer, 0, buffer.length);
        if (bufferLimit == -1) {
          return -1;
        }
        bufferNextRead = 0;
      }
      result = buffer[bufferNextRead++];
      offset++;
      appendToToken((char) result);
    }
    return result;
  }
  
  private int nextCodePoint() throws IOException {

    int ch = nextCodeUnit();
    if (ch == -1) {
      return ch;
    }
    if (Character.isHighSurrogate((char) ch)) {
      return Character.toCodePoint((char) ch, (char) nextCodeUnit());
    } else {
      return ch;
    }
  }
}
