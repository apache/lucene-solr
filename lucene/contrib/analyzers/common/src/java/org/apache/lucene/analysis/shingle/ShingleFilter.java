package org.apache.lucene.analysis.shingle;

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

import java.io.IOException;
import java.util.LinkedList;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;


/**
 * <p>A ShingleFilter constructs shingles (token n-grams) from a token stream.
 * In other words, it creates combinations of tokens as a single token.
 *
 * <p>For example, the sentence "please divide this sentence into shingles"
 * might be tokenized into shingles "please divide", "divide this",
 * "this sentence", "sentence into", and "into shingles".
 *
 * <p>This filter handles position increments > 1 by inserting filler tokens
 * (tokens with termtext "_"). It does not handle a position increment of 0.
 */
public final class ShingleFilter extends TokenFilter {

  /**
   * filler token for when positionIncrement is more than 1
   */
  public static final char[] FILLER_TOKEN = { '_' };

  /**
   * default maximum shingle size is 2.
   */
  public static final int DEFAULT_MAX_SHINGLE_SIZE = 2;

  /**
   * default minimum shingle size is 2.
   */
  public static final int DEFAULT_MIN_SHINGLE_SIZE = 2;

  /**
   * default token type attribute value is "shingle" 
   */
  public static final String DEFAULT_TOKEN_TYPE = "shingle";
  
  /**
   * The default string to use when joining adjacent tokens to form a shingle
   */
  public static final String TOKEN_SEPARATOR = " ";


  /**
   * The sequence of input stream tokens (or filler tokens, if necessary)
   * that will be composed to form output shingles.
   */
  private LinkedList<State> inputWindow = new LinkedList<State>();
  
  /**
   * The number of input tokens in the next output token.  This is the "n" in
   * "token n-grams".
   */
  private CircularSequence gramSize;

  /**
   * Shingle text is composed here.
   */
  private StringBuilder shingleBuilder = new StringBuilder();

  /**
   * The token type attribute value to use - default is "shingle"
   */
  private String tokenType = DEFAULT_TOKEN_TYPE;

  /**
   * The string to use when joining adjacent tokens to form a shingle
   */
  private String tokenSeparator = TOKEN_SEPARATOR;

  /**
   * By default, we output unigrams (individual tokens) as well as shingles
   * (token n-grams).
   */
  private boolean outputUnigrams = true;

  /**
   * maximum shingle size (number of tokens)
   */
  private int maxShingleSize;

  /**
   * minimum shingle size (number of tokens)
   */
  private int minShingleSize;

  /**
   * The remaining number of filler tokens inserted into the input stream
   * from which shingles are composed, to handle position increments greater
   * than one.
   */
  private int numFillerTokensToInsert;

  /**
   * The next input stream token.
   */
  private State nextInputStreamToken;
  
  private final TermAttribute termAtt;
  private final OffsetAttribute offsetAtt;
  private final PositionIncrementAttribute posIncrAtt;
  private final TypeAttribute typeAtt;


  /**
   * Constructs a ShingleFilter with the specified shingle size from the
   * {@link TokenStream} <code>input</code>
   *
   * @param input input stream
   * @param minShingleSize minimum shingle size produced by the filter.
   * @param maxShingleSize maximum shingle size produced by the filter.
   */
  public ShingleFilter(TokenStream input, int minShingleSize, int maxShingleSize) {
    super(input);
    setMaxShingleSize(maxShingleSize);
    setMinShingleSize(minShingleSize);
    this.termAtt = addAttribute(TermAttribute.class);
    this.offsetAtt = addAttribute(OffsetAttribute.class);
    this.posIncrAtt = addAttribute(PositionIncrementAttribute.class);
    this.typeAtt = addAttribute(TypeAttribute.class);
  }

  /**
   * Constructs a ShingleFilter with the specified shingle size from the
   * {@link TokenStream} <code>input</code>
   *
   * @param input input stream
   * @param maxShingleSize maximum shingle size produced by the filter.
   */
  public ShingleFilter(TokenStream input, int maxShingleSize) {
    this(input, DEFAULT_MIN_SHINGLE_SIZE, maxShingleSize);
  }
  
  /**
   * Construct a ShingleFilter with default shingle size: 2.
   *
   * @param input input stream
   */
  public ShingleFilter(TokenStream input) {
    this(input, DEFAULT_MIN_SHINGLE_SIZE, DEFAULT_MAX_SHINGLE_SIZE);
  }

  /**
   * Construct a ShingleFilter with the specified token type for shingle tokens
   * and the default shingle size: 2
   *
   * @param input input stream
   * @param tokenType token type for shingle tokens
   */
  public ShingleFilter(TokenStream input, String tokenType) {
    this(input, DEFAULT_MIN_SHINGLE_SIZE, DEFAULT_MAX_SHINGLE_SIZE);
    setTokenType(tokenType);
  }

  /**
   * Set the type of the shingle tokens produced by this filter.
   * (default: "shingle")
   *
   * @param tokenType token tokenType
   */
  public void setTokenType(String tokenType) {
    this.tokenType = tokenType;
  }

  /**
   * Shall the output stream contain the input tokens (unigrams) as well as
   * shingles? (default: true.)
   *
   * @param outputUnigrams Whether or not the output stream shall contain
   * the input tokens (unigrams)
   */
  public void setOutputUnigrams(boolean outputUnigrams) {
    this.outputUnigrams = outputUnigrams;
    gramSize = new CircularSequence();
  }

  /**
   * Set the max shingle size (default: 2)
   *
   * @param maxShingleSize max size of output shingles
   */
  public void setMaxShingleSize(int maxShingleSize) {
    if (maxShingleSize < 2) {
      throw new IllegalArgumentException("Max shingle size must be >= 2");
    }
    this.maxShingleSize = maxShingleSize;
  }

  /**
   * <p>Set the min shingle size (default: 2).
   * <p>This method requires that the passed in minShingleSize is not greater
   * than maxShingleSize, so make sure that maxShingleSize is set before
   * calling this method.
   * <p>The unigram output option is independent of the min shingle size.
   *
   * @param minShingleSize min size of output shingles
   */
  public void setMinShingleSize(int minShingleSize) {
    if (minShingleSize < 2) {
      throw new IllegalArgumentException("Min shingle size must be >= 2");
    }
    if (minShingleSize > maxShingleSize) {
      throw new IllegalArgumentException
        ("Min shingle size must be <= max shingle size");
    }
    this.minShingleSize = minShingleSize;
    gramSize = new CircularSequence();
  }

  /**
   * Sets the string to use when joining adjacent tokens to form a shingle
   * @param tokenSeparator used to separate input stream tokens in output shingles
   */
  public void setTokenSeparator(String tokenSeparator) {
    this.tokenSeparator = null == tokenSeparator ? "" : tokenSeparator;
  }

  /* (non-Javadoc)
   * @see org.apache.lucene.analysis.TokenStream#next()
   */
  @Override
  public final boolean incrementToken() throws IOException {
    boolean tokenAvailable = false; 
    if (gramSize.atMinValue() || inputWindow.size() < gramSize.getValue()) {
      shiftInputWindow();
    }
    if ( ! inputWindow.isEmpty()) {
      restoreState(inputWindow.getFirst());
      if (1 == gramSize.getValue()) {
        posIncrAtt.setPositionIncrement(1);
        gramSize.advance();
        tokenAvailable = true;
      } else if (inputWindow.size() >= gramSize.getValue()) {
        getNextShingle();
        gramSize.advance();
        tokenAvailable = true;
      }
    }
    return tokenAvailable;
  }

  /**
   * <p>Makes the next token a shingle of length {@link #gramSize}, 
   * composed of tokens taken from {@link #inputWindow}.
   * <p>Callers of this method must first insure that there are at least 
   * <code>gramSize</code> tokens available in <code>inputWindow</code>.
   */
  private void getNextShingle() {
    int startOffset = offsetAtt.startOffset();

    int minTokNum = gramSize.getValue() - 1; // zero-based inputWindow position
    if (gramSize.getValue() == minShingleSize) {
      // Clear the shingle text buffer if this is the first shingle
      // at the current position in the input stream.
      shingleBuilder.setLength(0);
      minTokNum = 0;
    }
    for (int tokNum = minTokNum ; tokNum < gramSize.getValue() ; ++tokNum) {
      if (tokNum > 0) {
        shingleBuilder.append(tokenSeparator);
      }
      restoreState(inputWindow.get(tokNum));
      shingleBuilder.append(termAtt.termBuffer(), 0, termAtt.termLength());
    }
    char[] termBuffer = termAtt.termBuffer();
    int termLength = shingleBuilder.length();
    if (termBuffer.length < termLength) {
      termBuffer = termAtt.resizeTermBuffer(termLength);
    }
    shingleBuilder.getChars(0, termLength, termBuffer, 0);
    termAtt.setTermLength(termLength);
    posIncrAtt.setPositionIncrement(gramSize.atMinValue() ? 1 : 0);
    typeAtt.setType(tokenType);
    offsetAtt.setOffset(startOffset, offsetAtt.endOffset());
  }
  
  /**
   * <p>Get the next token from the input stream.
   * <p>If the next token has <code>positionIncrement > 1</code>,
   * <code>positionIncrement - 1</code> {@link #FILLER_TOKEN}s are
   * inserted first.
   * @return false for end of stream; true otherwise
   * @throws IOException if the input stream has a problem
   */
  private boolean getNextToken() throws IOException {
    boolean success = false;
    if (numFillerTokensToInsert > 0) {
      insertFillerToken();
      success = true;
    } else if (null != nextInputStreamToken) {
      restoreState(nextInputStreamToken);
      nextInputStreamToken = null;
      success = true;
    } else if (input.incrementToken()) {
      if (posIncrAtt.getPositionIncrement() > 1) {
        numFillerTokensToInsert = posIncrAtt.getPositionIncrement() - 1;
        insertFillerToken();
      }
      success = true;
    }
    return success;
	}

  /**
   * Inserts a {@link #FILLER_TOKEN} and decrements
   * {@link #numFillerTokensToInsert}.
   */
  private void insertFillerToken() {
    if (null == nextInputStreamToken) {
      nextInputStreamToken = captureState();
    } else {
      restoreState(nextInputStreamToken);
    }
    --numFillerTokensToInsert;
    // A filler token occupies no space
    offsetAtt.setOffset(offsetAtt.startOffset(), offsetAtt.startOffset());
    termAtt.setTermBuffer(FILLER_TOKEN, 0, FILLER_TOKEN.length);
  }

  /**
   * <p>Fills {@link #inputWindow} with input stream tokens, if available, 
   * shifting to the right if the window was previously full.
   * <p>Resets {@link #gramSize} to its minimum value.
   *
   * @throws IOException if there's a problem getting the next token
   */
  private void shiftInputWindow() throws IOException {
    if (inputWindow.size() > 0) {
      inputWindow.removeFirst();
    }
    while (getNextToken()) {
      inputWindow.add(captureState());
      if (inputWindow.size() == maxShingleSize) {
        break;
      }
    }
    gramSize.reset();
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    gramSize.reset();
    inputWindow.clear();
    numFillerTokensToInsert = 0;
  }


  /**
   * <p>An instance of this class is used to maintain the number of input
   * stream tokens that will be used to compose the next unigram or shingle:
   * {@link #gramSize}.
   * <p><code>gramSize</code> will take on values from the circular sequence
   * <b>{ [ 1, ] {@link #minShingleSize} [ , ... , {@link #maxShingleSize} ] }</b>.
   * <p>1 is included in the circular sequence only if 
   * {@link #outputUnigrams} = true.
   */
  private class CircularSequence {
    private int value;
    private int minValue;
    
    public CircularSequence() {
      minValue = outputUnigrams ? 1 : minShingleSize;
      reset();
    }

    /**
     * @return the current value.  
     * @see #advance()
     */
    public int getValue() {
      return value;
    }
    
    /**
     * <p>Increments this circular number's value to the next member in the
     * circular sequence
     * <code>gramSize</code> will take on values from the circular sequence
     * <b>{ [ 1, ] {@link #minShingleSize} [ , ... , {@link #maxShingleSize} ] }</b>.
     * <p>1 is included in the circular sequence only if 
     * {@link #outputUnigrams} = true.
     * 
     * @return the next member in the circular sequence
     */
    public int advance() {
      if (value == 1) {
        value = minShingleSize;
      } else if (value == maxShingleSize) {
        reset();
      } else {
        ++value;
      }
      return value;
    }

    /**
     * <p>Sets this circular number's value to the first member of the 
     * circular sequence
     * <p><code>gramSize</code> will take on values from the circular sequence
     * <b>{ [ 1, ] {@link #minShingleSize} [ , ... , {@link #maxShingleSize} ] }</b>.
     * <p>1 is included in the circular sequence only if 
     * {@link #outputUnigrams} = true.
     */
    public void reset() {
      value = minValue;
    }

    /**
     * <p>Returns true if the current value is the first member of the circular
     * sequence.
     * <p>If {@link #outputUnigrams} = true, the first member of the circular
     * sequence will be 1; otherwise, it will be {@link #minShingleSize}.
     * 
     * @return true if the current value is the first member of the circular
     *  sequence; false otherwise
     */
    public boolean atMinValue() {
      return value == minValue;
    }
  }
}
