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
package org.apache.lucene.analysis.core;

import static org.apache.lucene.analysis.standard.StandardTokenizer.MAX_TOKEN_LENGTH_LIMIT;

import java.io.IOException;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.util.AttributeFactory;

/** Emits the entire input as a single token. */
public final class KeywordTokenizer extends Tokenizer {
  /** Default read buffer size */
  public static final int DEFAULT_BUFFER_SIZE = 256;

  private boolean done = false;
  private int finalOffset;
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);

  public KeywordTokenizer() {
    this(DEFAULT_BUFFER_SIZE);
  }

  public KeywordTokenizer(int bufferSize) {
    if (bufferSize > MAX_TOKEN_LENGTH_LIMIT || bufferSize <= 0) {
      throw new IllegalArgumentException(
          "maxTokenLen must be greater than 0 and less than "
              + MAX_TOKEN_LENGTH_LIMIT
              + " passed: "
              + bufferSize);
    }
    termAtt.resizeBuffer(bufferSize);
  }

  public KeywordTokenizer(AttributeFactory factory, int bufferSize) {
    super(factory);
    if (bufferSize > MAX_TOKEN_LENGTH_LIMIT || bufferSize <= 0) {
      throw new IllegalArgumentException(
          "maxTokenLen must be greater than 0 and less than "
              + MAX_TOKEN_LENGTH_LIMIT
              + " passed: "
              + bufferSize);
    }
    termAtt.resizeBuffer(bufferSize);
  }

  @Override
  public final boolean incrementToken() throws IOException {
    if (!done) {
      clearAttributes();
      done = true;
      int upto = 0;
      char[] buffer = termAtt.buffer();
      while (true) {
        final int length = input.read(buffer, upto, buffer.length - upto);
        if (length == -1) break;
        upto += length;
        if (upto == buffer.length) buffer = termAtt.resizeBuffer(1 + buffer.length);
      }
      termAtt.setLength(upto);
      finalOffset = correctOffset(upto);
      offsetAtt.setOffset(correctOffset(0), finalOffset);
      return true;
    }
    return false;
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
    this.done = false;
  }
}
