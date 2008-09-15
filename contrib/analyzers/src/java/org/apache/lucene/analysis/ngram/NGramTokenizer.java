package org.apache.lucene.analysis.ngram;

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

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.Tokenizer;

import java.io.IOException;
import java.io.Reader;

/**
 * Tokenizes the input into n-grams of the given size(s).
 */
public class NGramTokenizer extends Tokenizer {
  public static final int DEFAULT_MIN_NGRAM_SIZE = 1;
  public static final int DEFAULT_MAX_NGRAM_SIZE = 2;

  private int minGram, maxGram;
  private int gramSize;
  private int pos = 0;
  private int inLen;
  private String inStr;
  private boolean started = false;

  /**
   * Creates NGramTokenizer with given min and max n-grams.
   * @param input Reader holding the input to be tokenized
   * @param minGram the smallest n-gram to generate
   * @param maxGram the largest n-gram to generate
   */
  public NGramTokenizer(Reader input, int minGram, int maxGram) {
    super(input);
    if (minGram < 1) {
      throw new IllegalArgumentException("minGram must be greater than zero");
    }
    if (minGram > maxGram) {
      throw new IllegalArgumentException("minGram must not be greater than maxGram");
    }
    this.minGram = minGram;
    this.maxGram = maxGram;
  }
  /**
   * Creates NGramTokenizer with default min and max n-grams.
   * @param input Reader holding the input to be tokenized
   */
  public NGramTokenizer(Reader input) {
    this(input, DEFAULT_MIN_NGRAM_SIZE, DEFAULT_MAX_NGRAM_SIZE);
  }

  /** Returns the next token in the stream, or null at EOS. */
  public final Token next(final Token reusableToken) throws IOException {
    assert reusableToken != null;
    if (!started) {
      started = true;
      gramSize = minGram;
      char[] chars = new char[1024];
      input.read(chars);
      inStr = new String(chars).trim();  // remove any trailing empty strings 
      inLen = inStr.length();
    }

    if (pos+gramSize > inLen) {            // if we hit the end of the string
      pos = 0;                           // reset to beginning of string
      gramSize++;                        // increase n-gram size
      if (gramSize > maxGram)            // we are done
        return null;
      if (pos+gramSize > inLen)
        return null;
    }

    int oldPos = pos;
    pos++;
    return reusableToken.reinit(inStr, oldPos, gramSize, oldPos, oldPos+gramSize);
  }
}
