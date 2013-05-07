package org.apache.lucene.analysis.ngram;

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
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Version;

/**
 * Tokenizes the input from an edge into n-grams of given size(s).
 * <p>
 * This {@link Tokenizer} create n-grams from the beginning edge of a input token.
 */
public final class EdgeNGramTokenizer extends Tokenizer {
  public static final int DEFAULT_MAX_GRAM_SIZE = 1;
  public static final int DEFAULT_MIN_GRAM_SIZE = 1;

  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
  private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);

  private int minGram;
  private int maxGram;
  private int gramSize;
  private boolean started;
  private int inLen; // length of the input AFTER trim()
  private int charsRead; // length of the input
  private String inStr;

  /**
   * Creates EdgeNGramTokenizer that can generate n-grams in the sizes of the given range
   *
   * @param version the Lucene match version
   * @param input {@link Reader} holding the input to be tokenized
   * @param minGram the smallest n-gram to generate
   * @param maxGram the largest n-gram to generate
   */
  public EdgeNGramTokenizer(Version version, Reader input, int minGram, int maxGram) {
    super(input);
    init(version, minGram, maxGram);
  }

  /**
   * Creates EdgeNGramTokenizer that can generate n-grams in the sizes of the given range
   *
   * @param version the Lucene match version
   * @param factory {@link org.apache.lucene.util.AttributeSource.AttributeFactory} to use
   * @param input {@link Reader} holding the input to be tokenized
   * @param minGram the smallest n-gram to generate
   * @param maxGram the largest n-gram to generate
   */
  public EdgeNGramTokenizer(Version version, AttributeFactory factory, Reader input, int minGram, int maxGram) {
    super(factory, input);
    init(version, minGram, maxGram);
  }

  private void init(Version version, int minGram, int maxGram) {
    if (version == null) {
      throw new IllegalArgumentException("version must not be null");
    }

    if (minGram < 1) {
      throw new IllegalArgumentException("minGram must be greater than zero");
    }

    if (minGram > maxGram) {
      throw new IllegalArgumentException("minGram must not be greater than maxGram");
    }

    this.minGram = minGram;
    this.maxGram = maxGram;
  }

  /** Returns the next token in the stream, or null at EOS. */
  @Override
  public boolean incrementToken() throws IOException {
    clearAttributes();
    // if we are just starting, read the whole input
    if (!started) {
      started = true;
      gramSize = minGram;
      char[] chars = new char[Math.min(1024, maxGram)];
      charsRead = 0;
      // TODO: refactor to a shared readFully somewhere:
      boolean exhausted = false;
      while (charsRead < maxGram) {
        final int inc = input.read(chars, charsRead, chars.length-charsRead);
        if (inc == -1) {
          exhausted = true;
          break;
        }
        charsRead += inc;
        if (charsRead == chars.length && charsRead < maxGram) {
          chars = ArrayUtil.grow(chars);
        }
      }

      inStr = new String(chars, 0, charsRead);

      if (!exhausted) {
        // Read extra throwaway chars so that on end() we
        // report the correct offset:
        char[] throwaway = new char[1024];
        while(true) {
          final int inc = input.read(throwaway, 0, throwaway.length);
          if (inc == -1) {
            break;
          }
          charsRead += inc;
        }
      }

      inLen = inStr.length();
      if (inLen == 0) {
        return false;
      }
      posIncrAtt.setPositionIncrement(1);
    } else {
      posIncrAtt.setPositionIncrement(1);
    }

    // if the remaining input is too short, we can't generate any n-grams
    if (gramSize > inLen) {
      return false;
    }

    // if we have hit the end of our n-gram size range, quit
    if (gramSize > maxGram || gramSize > inLen) {
      return false;
    }

    // grab gramSize chars from front or back
    termAtt.setEmpty().append(inStr, 0, gramSize);
    offsetAtt.setOffset(correctOffset(0), correctOffset(gramSize));
    gramSize++;
    return true;
  }
  
  @Override
  public void end() {
    // set final offset
    final int finalOffset = correctOffset(charsRead);
    this.offsetAtt.setOffset(finalOffset, finalOffset);
  }    

  @Override
  public void reset() throws IOException {
    super.reset();
    started = false;
  }
}
