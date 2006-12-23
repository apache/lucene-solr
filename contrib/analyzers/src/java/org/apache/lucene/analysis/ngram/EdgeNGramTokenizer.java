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
 * Tokenizes the input into n-grams of the given size.
 * @author Otis Gospodnetic
 */
public class EdgeNGramTokenizer extends Tokenizer {
  // which side to get the n-gram from
  // TODO: switch to using this enum when we move to 1.5+
//  public enum Side {
//    FRONT (),
//    BACK ();
//  }
  /** Specifies which side of the input the n-gram should be generated from */
  public static class Side {
    /** Get the n-gram from the front of the input */
    public static Side FRONT = new Side("front");
    /** Get the n-gram from the end of the input */
    public static Side BACK = new Side("back");
    private Side(String label) {}
  }
  private int gramSize;
  private Side side;
  private int inLen;
  private String inStr;
  private boolean started = false;

  /**
   * Creates EdgeNGramTokenizer that can generate an n-gram of the given size.
   * @param input Reader holding the input to be tokenized
   * @param side the {@link Side} from which to chop off an n-gram 
   * @param gramSize the size of the n-gram to generate
   */
  public EdgeNGramTokenizer(Reader input, Side side, int gramSize) {
    super(input);
    if (gramSize < 1) {
      throw new IllegalArgumentException("gramSize must be greater than zero");
    }
    this.gramSize = gramSize;
    this.side = side;
  }
  public EdgeNGramTokenizer(Reader input, String side, int gramSize) {

  }

  /** Returns the next token in the stream, or null at EOS. */
  public final Token next() throws IOException {
    // if we already returned the edge n-gram, we are done
    if (started)
      return null;
    if (!started) {
      started = true;
      char[] chars = new char[1024];
      input.read(chars);
      inStr = new String(chars).trim();  // remove any trailing empty strings 
      inLen = inStr.length();
    }
    // if the input is too short, we can't generate any n-grams
    if (gramSize > inLen)
      return null;
    if (side == Side.FRONT)
      return new Token(inStr.substring(0, gramSize), 0, gramSize);
    else
      return new Token(inStr.substring(inLen-gramSize), inLen-gramSize, inLen);            
  }

  static Side side(String label) {
    if (label == null || label.isEmpty())
      throw new IllegalArgumentException("Label must be either 'front' or 'back'");
    if (label.equals("front"))
      return Side.FRONT;
    else
      return Side.BACK;
  }
}
