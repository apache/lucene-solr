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
import java.util.Objects;
import java.util.function.IntPredicate;
import java.util.function.IntUnaryOperator;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LetterTokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.LowerCaseTokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.AttributeFactory;
import org.apache.lucene.analysis.util.CharacterUtils;
import org.apache.lucene.analysis.util.CharacterUtils.CharacterBuffer;

/**
 * An abstract base class for simple, character-oriented tokenizers.
 * <p>
 * The base class also provides factories to create instances of
 * {@code CharTokenizer} using Java 8 lambdas or method references.
 * It is possible to create an instance which behaves exactly like
 * {@link LetterTokenizer}:
 * <pre class="prettyprint lang-java">
 * Tokenizer tok = CharTokenizer.fromTokenCharPredicate(Character::isLetter);
 * </pre>
 */
public abstract class CharTokenizer extends Tokenizer {
  
  /**
   * Creates a new {@link CharTokenizer} instance
   */
  public CharTokenizer() {
  }
  
  /**
   * Creates a new {@link CharTokenizer} instance
   * 
   * @param factory
   *          the attribute factory to use for this {@link Tokenizer}
   */
  public CharTokenizer(AttributeFactory factory) {
    super(factory);
  }
  
  /**
   * Creates a new instance of CharTokenizer using a custom predicate, supplied as method reference or lambda expression.
   * The predicate should return {@code true} for all valid token characters.
   * <p>
   * This factory is intended to be used with lambdas or method references. E.g., an elegant way
   * to create an instance which behaves exactly as {@link LetterTokenizer} is:
   * <pre class="prettyprint lang-java">
   * Tokenizer tok = CharTokenizer.fromTokenCharPredicate(Character::isLetter);
   * </pre>
   */
  public static CharTokenizer fromTokenCharPredicate(final IntPredicate tokenCharPredicate) {
    return fromTokenCharPredicate(DEFAULT_TOKEN_ATTRIBUTE_FACTORY, tokenCharPredicate);
  }
  
  /**
   * Creates a new instance of CharTokenizer with the supplied attribute factory using a custom predicate, supplied as method reference or lambda expression.
   * The predicate should return {@code true} for all valid token characters.
   * <p>
   * This factory is intended to be used with lambdas or method references. E.g., an elegant way
   * to create an instance which behaves exactly as {@link LetterTokenizer} is:
   * <pre class="prettyprint lang-java">
   * Tokenizer tok = CharTokenizer.fromTokenCharPredicate(factory, Character::isLetter);
   * </pre>
   */
  public static CharTokenizer fromTokenCharPredicate(AttributeFactory factory, final IntPredicate tokenCharPredicate) {
    return fromTokenCharPredicate(factory, tokenCharPredicate, IntUnaryOperator.identity());
  }
  
  /**
   * Creates a new instance of CharTokenizer using a custom predicate, supplied as method reference or lambda expression.
   * The predicate should return {@code true} for all valid token characters.
   * This factory also takes a function to normalize chars, e.g., lowercasing them, supplied as method reference or lambda expression.
   * <p>
   * This factory is intended to be used with lambdas or method references. E.g., an elegant way
   * to create an instance which behaves exactly as {@link LowerCaseTokenizer} is:
   * <pre class="prettyprint lang-java">
   * Tokenizer tok = CharTokenizer.fromTokenCharPredicate(Character::isLetter, Character::toLowerCase);
   * </pre>
   */
  public static CharTokenizer fromTokenCharPredicate(final IntPredicate tokenCharPredicate, final IntUnaryOperator normalizer) {
    return fromTokenCharPredicate(DEFAULT_TOKEN_ATTRIBUTE_FACTORY, tokenCharPredicate, normalizer);
  }
  
  /**
   * Creates a new instance of CharTokenizer with the supplied attribute factory using a custom predicate, supplied as method reference or lambda expression.
   * The predicate should return {@code true} for all valid token characters.
   * This factory also takes a function to normalize chars, e.g., lowercasing them, supplied as method reference or lambda expression.
   * <p>
   * This factory is intended to be used with lambdas or method references. E.g., an elegant way
   * to create an instance which behaves exactly as {@link LowerCaseTokenizer} is:
   * <pre class="prettyprint lang-java">
   * Tokenizer tok = CharTokenizer.fromTokenCharPredicate(factory, Character::isLetter, Character::toLowerCase);
   * </pre>
   */
  public static CharTokenizer fromTokenCharPredicate(AttributeFactory factory, final IntPredicate tokenCharPredicate, final IntUnaryOperator normalizer) {
    Objects.requireNonNull(tokenCharPredicate, "predicate must not be null.");
    Objects.requireNonNull(normalizer, "normalizer must not be null");
    return new CharTokenizer(factory) {
      @Override
      protected boolean isTokenChar(int c) {
        return tokenCharPredicate.test(c);
      }

      @Override
      protected int normalize(int c) {
        return normalizer.applyAsInt(c);
      }
    };
  }
  
  /**
   * Creates a new instance of CharTokenizer using a custom predicate, supplied as method reference or lambda expression.
   * The predicate should return {@code true} for all valid token separator characters.
   * This method is provided for convenience to easily use predicates that are negated
   * (they match the separator characters, not the token characters).
   * <p>
   * This factory is intended to be used with lambdas or method references. E.g., an elegant way
   * to create an instance which behaves exactly as {@link WhitespaceTokenizer} is:
   * <pre class="prettyprint lang-java">
   * Tokenizer tok = CharTokenizer.fromSeparatorCharPredicate(Character::isWhitespace);
   * </pre>
   */
  public static CharTokenizer fromSeparatorCharPredicate(final IntPredicate separatorCharPredicate) {
    return fromSeparatorCharPredicate(DEFAULT_TOKEN_ATTRIBUTE_FACTORY, separatorCharPredicate);
  }
  
  /**
   * Creates a new instance of CharTokenizer with the supplied attribute factory using a custom predicate, supplied as method reference or lambda expression.
   * The predicate should return {@code true} for all valid token separator characters.
   * <p>
   * This factory is intended to be used with lambdas or method references. E.g., an elegant way
   * to create an instance which behaves exactly as {@link WhitespaceTokenizer} is:
   * <pre class="prettyprint lang-java">
   * Tokenizer tok = CharTokenizer.fromSeparatorCharPredicate(factory, Character::isWhitespace);
   * </pre>
   */
  public static CharTokenizer fromSeparatorCharPredicate(AttributeFactory factory, final IntPredicate separatorCharPredicate) {
    return fromSeparatorCharPredicate(factory, separatorCharPredicate, IntUnaryOperator.identity());
  }
  
  /**
   * Creates a new instance of CharTokenizer using a custom predicate, supplied as method reference or lambda expression.
   * The predicate should return {@code true} for all valid token separator characters.
   * This factory also takes a function to normalize chars, e.g., lowercasing them, supplied as method reference or lambda expression.
   * <p>
   * This factory is intended to be used with lambdas or method references. E.g., an elegant way
   * to create an instance which behaves exactly as the combination {@link WhitespaceTokenizer} and {@link LowerCaseFilter} is:
   * <pre class="prettyprint lang-java">
   * Tokenizer tok = CharTokenizer.fromSeparatorCharPredicate(Character::isWhitespace, Character::toLowerCase);
   * </pre>
   */
  public static CharTokenizer fromSeparatorCharPredicate(final IntPredicate separatorCharPredicate, final IntUnaryOperator normalizer) {
    return fromSeparatorCharPredicate(DEFAULT_TOKEN_ATTRIBUTE_FACTORY, separatorCharPredicate, normalizer);
  }
  
  /**
   * Creates a new instance of CharTokenizer with the supplied attribute factory using a custom predicate.
   * The predicate should return {@code true} for all valid token separator characters.
   * This factory also takes a function to normalize chars, e.g., lowercasing them, supplied as method reference or lambda expression.
   * <p>
   * This factory is intended to be used with lambdas or method references. E.g., an elegant way
   * to create an instance which behaves exactly as {@link WhitespaceTokenizer} and {@link LowerCaseFilter} is:
   * <pre class="prettyprint lang-java">
   * Tokenizer tok = CharTokenizer.fromSeparatorCharPredicate(factory, Character::isWhitespace, Character::toLowerCase);
   * </pre>
   */
  public static CharTokenizer fromSeparatorCharPredicate(AttributeFactory factory, final IntPredicate separatorCharPredicate, final IntUnaryOperator normalizer) {
    return fromTokenCharPredicate(factory, separatorCharPredicate.negate(), normalizer);
  }
  
  private int offset = 0, bufferIndex = 0, dataLen = 0, finalOffset = 0;
  private static final int MAX_WORD_LEN = 255;
  private static final int IO_BUFFER_SIZE = 4096;
  
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
  
  private final CharacterUtils charUtils = CharacterUtils.getInstance();
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
        charUtils.fill(ioBuffer, input); // read supplementary char aware with CharacterUtils
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
      final int c = charUtils.codePointAt(ioBuffer.getBuffer(), bufferIndex, ioBuffer.getLength());
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