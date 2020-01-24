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
package org.apache.solr.analysis;

import java.io.IOException;

import org.apache.lucene.analysis.CharacterUtils;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LetterTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.util.AttributeFactory;

/**
 * LowerCaseTokenizer performs the function of LetterTokenizer
 * and LowerCaseFilter together.  It divides text at non-letters and converts
 * them to lower case.  While it is functionally equivalent to the combination
 * of LetterTokenizer and LowerCaseFilter, there is a performance advantage
 * to doing the two tasks at once, hence this (redundant) implementation.
 * <P>
 * Note: this does a decent job for most European languages, but does a terrible
 * job for some Asian languages, where words are not separated by spaces.
 * </p>
 *
 * @deprecated Use {@link LetterTokenizer} and {@link org.apache.lucene.analysis.LowerCaseFilter}
 */
@Deprecated
public final class LowerCaseTokenizer extends Tokenizer {

  /**
   * Construct a new LowerCaseTokenizer.
   */
  public LowerCaseTokenizer() {
    this.maxTokenLen = DEFAULT_MAX_WORD_LEN;
  }

  /**
   * Construct a new LowerCaseTokenizer using a given
   * {@link org.apache.lucene.util.AttributeFactory}.
   *
   * @param factory
   *          the attribute factory to use for this {@link Tokenizer}
   */
  public LowerCaseTokenizer(AttributeFactory factory) {
    this(factory, DEFAULT_MAX_WORD_LEN);
  }

  /**
   * Construct a new LowerCaseTokenizer using a given
   * {@link org.apache.lucene.util.AttributeFactory}.
   *
   * @param factory the attribute factory to use for this {@link Tokenizer}
   * @param maxTokenLen maximum token length the tokenizer will emit.
   *        Must be greater than 0 and less than MAX_TOKEN_LENGTH_LIMIT (1024*1024)
   * @throws IllegalArgumentException if maxTokenLen is invalid.
   */
  public LowerCaseTokenizer(AttributeFactory factory, int maxTokenLen) {
    super(factory);
    this.maxTokenLen = maxTokenLen;
  }

  private int offset = 0, bufferIndex = 0, dataLen = 0, finalOffset = 0;
  public static final int DEFAULT_MAX_WORD_LEN = 255;
  private static final int IO_BUFFER_SIZE = 4096;
  private final int maxTokenLen;

  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);

  private final CharacterUtils.CharacterBuffer ioBuffer = CharacterUtils.newCharacterBuffer(IO_BUFFER_SIZE);

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
        CharacterUtils.fill(ioBuffer, input); // read supplementary char aware with CharacterUtils
        if (ioBuffer.getLength() == 0) {
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
      final int c = Character.codePointAt(ioBuffer.getBuffer(), bufferIndex, ioBuffer.getLength());
      final int charCount = Character.charCount(c);
      bufferIndex += charCount;

      if (Character.isLetter(c)) {               // if it's a token char
        if (length == 0) {                // start of token
          assert start == -1;
          start = offset + bufferIndex - charCount;
          end = start;
        } else if (length >= buffer.length-1) { // check if a supplementary could run out of bounds
          buffer = termAtt.resizeBuffer(2+length); // make sure a supplementary fits in the buffer
        }
        end += charCount;
        length += Character.toChars(Character.toLowerCase(c), buffer, length); // buffer it, normalized
        if (length >= maxTokenLen) { // buffer overflow! make sure to check for >= surrogate pair could break == test
          break;
        }
      } else if (length > 0) {           // at non-Letter w/ chars
        break;                           // return 'em
      }
    }

    termAtt.setLength(length);
    assert start != -1;
    offsetAtt.setOffset(correctOffset(start), finalOffset = correctOffset(end));
    return true;

  }

  @Override
  public final void end() throws IOException {
    super.end();
    // set final offset
    offsetAtt.setOffset(finalOffset, finalOffset);
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    bufferIndex = 0;
    offset = 0;
    dataLen = 0;
    finalOffset = 0;
    ioBuffer.reset(); // make sure to reset the IO buffer!!
  }

}
