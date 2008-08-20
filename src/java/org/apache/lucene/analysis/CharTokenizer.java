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

/** An abstract base class for simple, character-oriented tokenizers.*/
public abstract class CharTokenizer extends Tokenizer {
  public CharTokenizer(Reader input) {
    super(input);
  }

  private int offset = 0, bufferIndex = 0, dataLen = 0;
  private static final int MAX_WORD_LEN = 255;
  private static final int IO_BUFFER_SIZE = 4096;
  private final char[] ioBuffer = new char[IO_BUFFER_SIZE];

  /** Returns true iff a character should be included in a token.  This
   * tokenizer generates as tokens adjacent sequences of characters which
   * satisfy this predicate.  Characters for which this is false are used to
   * define token boundaries and are not included in tokens. */
  protected abstract boolean isTokenChar(char c);

  /** Called on each token character to normalize it before it is added to the
   * token.  The default implementation does nothing.  Subclasses may use this
   * to, e.g., lowercase tokens. */
  protected char normalize(char c) {
    return c;
  }

  public final Token next(final Token reusableToken) throws IOException {
    assert reusableToken != null;
    reusableToken.clear();
    int length = 0;
    int start = bufferIndex;
    char[] buffer = reusableToken.termBuffer();
    while (true) {

      if (bufferIndex >= dataLen) {
        offset += dataLen;
        dataLen = input.read(ioBuffer);
        if (dataLen == -1) {
          if (length > 0)
            break;
          else
            return null;
        }
        bufferIndex = 0;
      }

      final char c = ioBuffer[bufferIndex++];

      if (isTokenChar(c)) {               // if it's a token char

        if (length == 0)			           // start of token
          start = offset + bufferIndex - 1;
        else if (length == buffer.length)
          buffer = reusableToken.resizeTermBuffer(1+length);

        buffer[length++] = normalize(c); // buffer it, normalized

        if (length == MAX_WORD_LEN)		   // buffer overflow!
          break;

      } else if (length > 0)             // at non-Letter w/ chars
        break;                           // return 'em
    }

    reusableToken.setTermLength(length);
    reusableToken.setStartOffset(start);
    reusableToken.setEndOffset(start+length);
    return reusableToken;
  }

  public void reset(Reader input) throws IOException {
    super.reset(input);
    bufferIndex = 0;
    offset = 0;
    dataLen = 0;
  }
}
