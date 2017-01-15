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
package org.apache.lucene.analysis.icu.segmentation;


import java.io.IOException;
import java.io.Reader;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.icu.tokenattributes.ScriptAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.util.AttributeFactory;

import com.ibm.icu.lang.UCharacter;
import com.ibm.icu.text.BreakIterator;

/**
 * Breaks text into words according to UAX #29: Unicode Text Segmentation
 * (http://www.unicode.org/reports/tr29/)
 * <p>
 * Words are broken across script boundaries, then segmented according to
 * the BreakIterator and typing provided by the {@link ICUTokenizerConfig}
 * </p>
 * @see ICUTokenizerConfig
 * @lucene.experimental
 */
public final class ICUTokenizer extends Tokenizer {
  private static final int IOBUFFER = 4096;
  private final char buffer[] = new char[IOBUFFER];
  /** true length of text in the buffer */
  private int length = 0; 
  /** length in buffer that can be evaluated safely, up to a safe end point */
  private int usableLength = 0; 
  /** accumulated offset of previous buffers for this reader, for offsetAtt */
  private int offset = 0; 

  private final CompositeBreakIterator breaker; /* tokenizes a char[] of text */
  private final ICUTokenizerConfig config;
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);
  private final ScriptAttribute scriptAtt = addAttribute(ScriptAttribute.class);

  /**
   * Construct a new ICUTokenizer that breaks text into words from the given
   * Reader.
   * <p>
   * The default script-specific handling is used.
   * <p>
   * The default attribute factory is used.
   * 
   * @see DefaultICUTokenizerConfig
   */
  public ICUTokenizer() {
    this(new DefaultICUTokenizerConfig(true, true));
  }

  /**
   * Construct a new ICUTokenizer that breaks text into words from the given
   * Reader, using a tailored BreakIterator configuration.
   * <p>
   * The default attribute factory is used.
   *
   * @param config Tailored BreakIterator configuration
   */
  public ICUTokenizer(ICUTokenizerConfig config) {
    this(DEFAULT_TOKEN_ATTRIBUTE_FACTORY, config);
  }

  /**
   * Construct a new ICUTokenizer that breaks text into words from the given
   * Reader, using a tailored BreakIterator configuration.
   *
   * @param factory AttributeFactory to use
   * @param config Tailored BreakIterator configuration
   */
  public ICUTokenizer(AttributeFactory factory, ICUTokenizerConfig config) {
    super(factory);
    this.config = config;
    breaker = new CompositeBreakIterator(config);
  }

  @Override
  public boolean incrementToken() throws IOException {
    clearAttributes();
    if (length == 0)
      refill();
    while (!incrementTokenBuffer()) {
      refill();
      if (length <= 0) // no more bytes to read;
        return false;
    }
    return true;
  }
  
  @Override
  public void reset() throws IOException {
    super.reset();
    breaker.setText(buffer, 0, 0);
    length = usableLength = offset = 0;
  }
  
  @Override
  public void end() throws IOException {
    super.end();
    final int finalOffset = (length < 0) ? offset : offset + length;
    offsetAtt.setOffset(correctOffset(finalOffset), correctOffset(finalOffset));
  }  

  /*
   * This tokenizes text based upon the longest matching rule, and because of 
   * this, isn't friendly to a Reader.
   * 
   * Text is read from the input stream in 4kB chunks. Within a 4kB chunk of
   * text, the last unambiguous break point is found (in this implementation:
   * white space character) Any remaining characters represent possible partial
   * words, so are appended to the front of the next chunk.
   * 
   * There is the possibility that there are no unambiguous break points within
   * an entire 4kB chunk of text (binary data). So there is a maximum word limit
   * of 4kB since it will not try to grow the buffer in this case.
   */

  /**
   * Returns the last unambiguous break position in the text.
   * 
   * @return position of character, or -1 if one does not exist
   */
  private int findSafeEnd() {
    for (int i = length - 1; i >= 0; i--)
      if (UCharacter.isWhitespace(buffer[i]))
        return i + 1;
    return -1;
  }

  /**
   * Refill the buffer, accumulating the offset and setting usableLength to the
   * last unambiguous break position
   * 
   * @throws IOException If there is a low-level I/O error.
   */
  private void refill() throws IOException {
    offset += usableLength;
    int leftover = length - usableLength;
    System.arraycopy(buffer, usableLength, buffer, 0, leftover);
    int requested = buffer.length - leftover;
    int returned = read(input, buffer, leftover, requested);
    length = returned + leftover;
    if (returned < requested) /* reader has been emptied, process the rest */
      usableLength = length;
    else { /* still more data to be read, find a safe-stopping place */
      usableLength = findSafeEnd();
      if (usableLength < 0)
        usableLength = length; /*
                                * more than IOBUFFER of text without space,
                                * gonna possibly truncate tokens
                                */
    }

    breaker.setText(buffer, 0, Math.max(0, usableLength));
  }

  // TODO: refactor to a shared readFully somewhere
  // (NGramTokenizer does this too):
  /** commons-io's readFully, but without bugs if offset != 0 */
  private static int read(Reader input, char[] buffer, int offset, int length) throws IOException {
    assert length >= 0 : "length must not be negative: " + length;
 
    int remaining = length;
    while ( remaining > 0 ) {
      int location = length - remaining;
      int count = input.read( buffer, offset + location, remaining );
      if ( -1 == count ) { // EOF
        break;
      }
      remaining -= count;
    }
    return length - remaining;
  }

  /*
   * return true if there is a token from the buffer, or null if it is
   * exhausted.
   */
  private boolean incrementTokenBuffer() {
    int start = breaker.current();
    if (start == BreakIterator.DONE)
      return false; // BreakIterator exhausted

    // find the next set of boundaries, skipping over non-tokens (rule status 0)
    int end = breaker.next();
    while (start != BreakIterator.DONE && breaker.getRuleStatus() == 0) {
      start = end;
      end = breaker.next();
    }

    if (start == BreakIterator.DONE)
      return false; // BreakIterator exhausted

    termAtt.copyBuffer(buffer, start, end - start);
    offsetAtt.setOffset(correctOffset(offset + start), correctOffset(offset + end));
    typeAtt.setType(config.getType(breaker.getScriptCode(), breaker.getRuleStatus()));
    scriptAtt.setCode(breaker.getScriptCode());

    return true;
  }
}
