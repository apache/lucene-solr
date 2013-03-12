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

/**
 * Tokenizes the input from an edge into n-grams of given size(s).
 * <p>
 * This {@link Tokenizer} create n-grams from the beginning edge or ending edge of a input token.
 * MaxGram can't be larger than 1024 because of limitation.
 * </p>
 */
public final class EdgeNGramTokenizer extends Tokenizer {
  public static final Side DEFAULT_SIDE = Side.FRONT;
  public static final int DEFAULT_MAX_GRAM_SIZE = 1;
  public static final int DEFAULT_MIN_GRAM_SIZE = 1;
  
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
  private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);

  /** Specifies which side of the input the n-gram should be generated from */
  public static enum Side {

    /** Get the n-gram from the front of the input */
    FRONT {
      @Override
      public String getLabel() { return "front"; }
    },

    /** Get the n-gram from the end of the input */
    BACK  {
      @Override
      public String getLabel() { return "back"; }
    };

    public abstract String getLabel();

    // Get the appropriate Side from a string
    public static Side getSide(String sideName) {
      if (FRONT.getLabel().equals(sideName)) {
        return FRONT;
      }
      if (BACK.getLabel().equals(sideName)) {
        return BACK;
      }
      return null;
    }
  }

  private int minGram;
  private int maxGram;
  private int gramSize;
  private Side side;
  private boolean started;
  private int inLen; // length of the input AFTER trim()
  private int charsRead; // length of the input
  private String inStr;


  /**
   * Creates EdgeNGramTokenizer that can generate n-grams in the sizes of the given range
   *
   * @param input {@link Reader} holding the input to be tokenized
   * @param side the {@link Side} from which to chop off an n-gram
   * @param minGram the smallest n-gram to generate
   * @param maxGram the largest n-gram to generate
   */
  public EdgeNGramTokenizer(Reader input, Side side, int minGram, int maxGram) {
    super(input);
    init(side, minGram, maxGram);
  }

  /**
   * Creates EdgeNGramTokenizer that can generate n-grams in the sizes of the given range
   * 
   * @param factory {@link org.apache.lucene.util.AttributeSource.AttributeFactory} to use
   * @param input {@link Reader} holding the input to be tokenized
   * @param side the {@link Side} from which to chop off an n-gram
   * @param minGram the smallest n-gram to generate
   * @param maxGram the largest n-gram to generate
   */
  public EdgeNGramTokenizer(AttributeFactory factory, Reader input, Side side, int minGram, int maxGram) {
    super(factory, input);
    init(side, minGram, maxGram);
  }
  
  /**
   * Creates EdgeNGramTokenizer that can generate n-grams in the sizes of the given range
   *
   * @param input {@link Reader} holding the input to be tokenized
   * @param sideLabel the name of the {@link Side} from which to chop off an n-gram
   * @param minGram the smallest n-gram to generate
   * @param maxGram the largest n-gram to generate
   */
  public EdgeNGramTokenizer(Reader input, String sideLabel, int minGram, int maxGram) {
    this(input, Side.getSide(sideLabel), minGram, maxGram);
  }

  /**
   * Creates EdgeNGramTokenizer that can generate n-grams in the sizes of the given range
   * 
   * @param factory {@link org.apache.lucene.util.AttributeSource.AttributeFactory} to use
   * @param input {@link Reader} holding the input to be tokenized
   * @param sideLabel the name of the {@link Side} from which to chop off an n-gram
   * @param minGram the smallest n-gram to generate
   * @param maxGram the largest n-gram to generate
   */
  public EdgeNGramTokenizer(AttributeFactory factory, Reader input, String sideLabel, int minGram, int maxGram) {
    this(factory, input, Side.getSide(sideLabel), minGram, maxGram);
  }
  
  private void init(Side side, int minGram, int maxGram) {
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

  /** Returns the next token in the stream, or null at EOS. */
  @Override
  public boolean incrementToken() throws IOException {
    clearAttributes();
    // if we are just starting, read the whole input
    if (!started) {
      started = true;
      gramSize = minGram;
      char[] chars = new char[1024];
      charsRead = 0;
      // TODO: refactor to a shared readFully somewhere:
      while (charsRead < chars.length) {
        final int inc = input.read(chars, charsRead, chars.length-charsRead);
        if (inc == -1) {
          break;
        }
        charsRead += inc;
      }

      inStr = new String(chars, 0, charsRead).trim();  // remove any trailing empty strings 

      if (charsRead == chars.length) {
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
      posIncrAtt.setPositionIncrement(0);
    }

    // if the remaining input is too short, we can't generate any n-grams
    if (gramSize > inLen) {
      return false;
    }

    // if we have hit the end of our n-gram size range, quit
    if (gramSize > maxGram) {
      return false;
    }

    // grab gramSize chars from front or back
    int start = side == Side.FRONT ? 0 : inLen - gramSize;
    int end = start + gramSize;
    termAtt.setEmpty().append(inStr, start, end);
    offsetAtt.setOffset(correctOffset(start), correctOffset(end));
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
