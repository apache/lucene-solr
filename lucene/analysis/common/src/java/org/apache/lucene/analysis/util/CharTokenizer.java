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

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import org.apache.lucene.analysis.util.CharacterUtils.CharacterBuffer;

/**
 * An abstract base class for simple, character-oriented tokenizers. 
 * <p>
 * <a name="version">You must specify the required {@link Version} compatibility
 * when creating {@link CharTokenizer}:
 * <ul>
 * <li>As of 3.1, {@link CharTokenizer} uses an int based API to normalize and
 * detect token codepoints. See {@link #isTokenChar(int)} and
 * {@link #normalize(int)} for details.</li>
 * </ul>
 * <p>
 * A new {@link CharTokenizer} API has been introduced with Lucene 3.1. This API
 * moved from UTF-16 code units to UTF-32 codepoints to eventually add support
 * for <a href=
 * "http://java.sun.com/j2se/1.5.0/docs/api/java/lang/Character.html#supplementary"
 * >supplementary characters</a>. The old <i>char</i> based API has been
 * deprecated and should be replaced with the <i>int</i> based methods
 * {@link #isTokenChar(int)} and {@link #normalize(int)}.
 * </p>
 * <p>
 * As of Lucene 3.1 each {@link CharTokenizer} - constructor expects a
 * {@link Version} argument. Based on the given {@link Version} either the new
 * API or a backwards compatibility layer is used at runtime. For
 * {@link Version} < 3.1 the backwards compatibility layer ensures correct
 * behavior even for indexes build with previous versions of Lucene. If a
 * {@link Version} >= 3.1 is used {@link CharTokenizer} requires the new API to
 * be implemented by the instantiated class. Yet, the old <i>char</i> based API
 * is not required anymore even if backwards compatibility must be preserved.
 * {@link CharTokenizer} subclasses implementing the new API are fully backwards
 * compatible if instantiated with {@link Version} < 3.1.
 * </p>
 * <p>
 * <strong>Note:</strong> If you use a subclass of {@link CharTokenizer} with {@link Version} >=
 * 3.1 on an index build with a version < 3.1, created tokens might not be
 * compatible with the terms in your index.
 * </p>
 **/
public abstract class CharTokenizer extends Tokenizer {
  
  /**
   * Creates a new {@link CharTokenizer} instance
   * 
   * @param matchVersion
   *          Lucene version to match
   * @param input
   *          the input to split up into tokens
   */
  public CharTokenizer(Version matchVersion, Reader input) {
    super(input);
    charUtils = CharacterUtils.getInstance(matchVersion);
  }
  
  /**
   * Creates a new {@link CharTokenizer} instance
   * 
   * @param matchVersion
   *          Lucene version to match
   * @param factory
   *          the attribute factory to use for this {@link Tokenizer}
   * @param input
   *          the input to split up into tokens
   */
  public CharTokenizer(Version matchVersion, AttributeFactory factory,
      Reader input) {
    super(factory, input);
    charUtils = CharacterUtils.getInstance(matchVersion);
  }
  
  // note: bufferIndex is -1 here to best-effort AIOOBE consumers that don't call reset()
  private int offset = 0, bufferIndex = -1, dataLen = 0, finalOffset = 0;
  private static final int MAX_WORD_LEN = 255;
  private static final int IO_BUFFER_SIZE = 4096;
  
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
  
  private final CharacterUtils charUtils;
  private final CharacterBuffer ioBuffer = CharacterUtils.newCharacterBuffer(IO_BUFFER_SIZE);
  
  /**
   * Returns true iff a codepoint should be included in a token. This tokenizer
   * generates as tokens adjacent sequences of codepoints which satisfy this
   * predicate. Codepoints for which this is false are used to define token
   * boundaries and are not included in tokens.
   */
  protected abstract boolean isTokenChar(int c);

  /**
   * Called on each token character to normalize it before it is added to the
   * token. The default implementation does nothing. Subclasses may use this to,
   * e.g., lowercase tokens.
   */
  protected int normalize(int c) {
    return c;
  }

  @Override
  public final boolean incrementToken() throws IOException {
    clearAttributes();
    int length = 0;
    int start = -1; // this variable is always initialized
    int end = -1;
    char[] buffer = termAtt.buffer();
    while (true) {
      if (bufferIndex >= dataLen) {
        offset += dataLen;
        if(!charUtils.fill(ioBuffer, input)) { // read supplementary char aware with CharacterUtils
          dataLen = 0; // so next offset += dataLen won't decrement offset
          if (length > 0) {
            break;
          } else {
            finalOffset = correctOffset(offset);
            return false;
          }
        }
        dataLen = ioBuffer.getLength();
        bufferIndex = 0;
      }
      // use CharacterUtils here to support < 3.1 UTF-16 code unit behavior if the char based methods are gone
      final int c = charUtils.codePointAt(ioBuffer.getBuffer(), bufferIndex);
      final int charCount = Character.charCount(c);
      bufferIndex += charCount;

      if (isTokenChar(c)) {               // if it's a token char
        if (length == 0) {                // start of token
          assert start == -1;
          start = offset + bufferIndex - charCount;
          end = start;
        } else if (length >= buffer.length-1) { // check if a supplementary could run out of bounds
          buffer = termAtt.resizeBuffer(2+length); // make sure a supplementary fits in the buffer
        }
        end += charCount;
        length += Character.toChars(normalize(c), buffer, length); // buffer it, normalized
        if (length >= MAX_WORD_LEN) // buffer overflow! make sure to check for >= surrogate pair could break == test
          break;
      } else if (length > 0)             // at non-Letter w/ chars
        break;                           // return 'em
    }

    termAtt.setLength(length);
    assert start != -1;
    offsetAtt.setOffset(correctOffset(start), finalOffset = correctOffset(end));
    return true;
    
  }
  
  @Override
  public final void end() {
    // set final offset
    offsetAtt.setOffset(finalOffset, finalOffset);
  }

  @Override
  public void reset() throws IOException {
    bufferIndex = 0;
    offset = 0;
    dataLen = 0;
    finalOffset = 0;
    ioBuffer.reset(); // make sure to reset the IO buffer!!
  }
}