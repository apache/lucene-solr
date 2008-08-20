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
import org.apache.lucene.analysis.ngram.EdgeNGramTokenFilter.Side;

import java.io.IOException;
import java.io.Reader;

/**
 * Tokenizes the input from an edge into n-grams of given size(s).
 *
 * This tokenizer create n-grams from the beginning edge or ending edge of a input token.
 * MaxGram can't be larger than 1024 because of limitation.
 *
 */
public class EdgeNGramTokenizer extends Tokenizer {
  public static final Side DEFAULT_SIDE = Side.FRONT;
  public static final int DEFAULT_MAX_GRAM_SIZE = 1;
  public static final int DEFAULT_MIN_GRAM_SIZE = 1;

  // Replace this with an enum when the Java 1.5 upgrade is made, the impl will be simplified
  /** Specifies which side of the input the n-gram should be generated from */
  public static class Side {
    private String label;

    /** Get the n-gram from the front of the input */
    public static Side FRONT = new Side("front");

    /** Get the n-gram from the end of the input */
    public static Side BACK = new Side("back");

    // Private ctor
    private Side(String label) { this.label = label; }


    public String getLabel() { return label; }

    // Get the appropriate Side from a string
    public static Side getSide(String sideName) {
      if (FRONT.getLabel().equals(sideName)) {
        return FRONT;
      }
      else if (BACK.getLabel().equals(sideName)) {
        return BACK;
      }
      return null;
    }
  }

  private int minGram;
  private int maxGram;
  private int gramSize;
  private Side side;
  private boolean started = false;
  private int inLen;
  private String inStr;


  /**
   * Creates EdgeNGramTokenizer that can generate n-grams in the sizes of the given range
   *
   * @param input Reader holding the input to be tokenized
   * @param side the {@link Side} from which to chop off an n-gram
   * @param minGram the smallest n-gram to generate
   * @param maxGram the largest n-gram to generate
   */
  public EdgeNGramTokenizer(Reader input, Side side, int minGram, int maxGram) {
    super(input);

    if (side == null) {
      throw new IllegalArgumentException("sideLabel must be either front or back");
    }

    if (minGram < 1) {
      throw new IllegalArgumentException("minGram must be greater than zero");
    }

    if (minGram > maxGram) {
      throw new IllegalArgumentException("minGram must not be greater than maxGram");
    }

    this.minGram = minGram;
    this.maxGram = maxGram;
    this.side = side;
  }
  /**
   * Creates EdgeNGramTokenizer that can generate n-grams in the sizes of the given range
   *
   * @param input Reader holding the input to be tokenized
   * @param sideLabel the name of the {@link Side} from which to chop off an n-gram
   * @param minGram the smallest n-gram to generate
   * @param maxGram the largest n-gram to generate
   */
  public EdgeNGramTokenizer(Reader input, String sideLabel, int minGram, int maxGram) {
    this(input, Side.getSide(sideLabel), minGram, maxGram);
  }

  /** Returns the next token in the stream, or null at EOS. */
  public final Token next(final Token reusableToken) throws IOException {
    assert reusableToken != null;
    // if we are just starting, read the whole input
    if (!started) {
      started = true;
      char[] chars = new char[1024];
      input.read(chars);
      inStr = new String(chars).trim();  // remove any leading or trailing spaces
      inLen = inStr.length();
      gramSize = minGram;
    }

    // if the remaining input is too short, we can't generate any n-grams
    if (gramSize > inLen) {
      return null;
    }

    // if we have hit the end of our n-gram size range, quit
    if (gramSize > maxGram) {
      return null;
    }

    // grab gramSize chars from front or back
    int start = side == Side.FRONT ? 0 : inLen - gramSize;
    int end = start + gramSize;
    reusableToken.setTermBuffer(inStr, start, gramSize);
    reusableToken.setStartOffset(start);
    reusableToken.setEndOffset(end);
    gramSize++;
    return reusableToken;
  }
}
