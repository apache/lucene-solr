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
package org.apache.lucene.analysis.shingle;


import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.util.AttributeSource;


/**
 * <p>A ShingleFilter constructs shingles (token n-grams) from a token stream.
 * In other words, it creates combinations of tokens as a single token.
 *
 * <p>For example, the sentence "please divide this sentence into shingles"
 * might be tokenized into shingles "please divide", "divide this",
 * "this sentence", "sentence into", and "into shingles".
 *
 * <p>This filter handles position increments &gt; 1 by inserting filler tokens
 * (tokens with termtext "_"). It does not handle a position increment of 0.
 */
public final class ShingleFilter extends TokenFilter {

  /**
   * filler token for when positionIncrement is more than 1
   */
  public static final String DEFAULT_FILLER_TOKEN = "_";

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
  public static final String DEFAULT_TOKEN_SEPARATOR = " ";

  /**
   * The sequence of input stream tokens (or filler tokens, if necessary)
   * that will be composed to form output shingles.
   */
  private LinkedList<InputWindowToken> inputWindow
    = new LinkedList<>();
  
  /**
   * The number of input tokens in the next output token.  This is the "n" in
   * "token n-grams".
   */
  private CircularSequence gramSize;

  /**
   * Shingle and unigram text is composed here.
   */
  private StringBuilder gramBuilder = new StringBuilder();

  /**
   * The token type attribute value to use - default is "shingle"
   */
  private String tokenType = DEFAULT_TOKEN_TYPE;

  /**
   * The string to use when joining adjacent tokens to form a shingle
   */
  private String tokenSeparator = DEFAULT_TOKEN_SEPARATOR;

  /**
   * The string to insert for each position at which there is no token
   * (i.e., when position increment is greater than one).
   */
  private char[] fillerToken = DEFAULT_FILLER_TOKEN.toCharArray();

  /**
   * By default, we output unigrams (individual tokens) as well as shingles
   * (token n-grams).
   */
  private boolean outputUnigrams = true;

  /**
   * By default, we don't override behavior of outputUnigrams.
   */
  private boolean outputUnigramsIfNoShingles = false;
 
  /**
   * maximum shingle size (number of tokens)
   */
  private int maxShingleSize;

  /**
   * minimum shingle size (number of tokens)
   */
  private int minShingleSize;

  /**
   * The remaining number of filler tokens to be inserted into the input stream
   * from which shingles are composed, to handle position increments greater
   * than one.
   */
  private int numFillerTokensToInsert;

  /**
   * When the next input stream token has a position increment greater than
   * one, it is stored in this field until sufficient filler tokens have been
   * inserted to account for the position increment. 
   */
  private AttributeSource nextInputStreamToken;

  /**
   * Whether or not there is a next input stream token.
   */
  private boolean isNextInputStreamToken = false;

  /**
   * Whether at least one unigram or shingle has been output at the current 
   * position.
   */
  private boolean isOutputHere = false;

  /**
   * true if no shingles have been output yet (for outputUnigramsIfNoShingles).
   */
  boolean noShingleOutput = true;

  /**
   * Holds the State after input.end() was called, so we can
   * restore it in our end() impl.
   */
  private State endState;
  
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
  private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);
  private final PositionLengthAttribute posLenAtt = addAttribute(PositionLengthAttribute.class);
  private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);


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
   * <p>Shall we override the behavior of outputUnigrams==false for those
   * times when no shingles are available (because there are fewer than
   * minShingleSize tokens in the input stream)? (default: false.)
   * <p>Note that if outputUnigrams==true, then unigrams are always output,
   * regardless of whether any shingles are available.
   *
   * @param outputUnigramsIfNoShingles Whether or not to output a single
   * unigram when no shingles are available.
   */
  public void setOutputUnigramsIfNoShingles(boolean outputUnigramsIfNoShingles) {
    this.outputUnigramsIfNoShingles = outputUnigramsIfNoShingles;
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

  /**
   * Sets the string to insert for each position at which there is no token
   * (i.e., when position increment is greater than one).
   *
   * @param fillerToken string to insert at each position where there is no token
   */
  public void setFillerToken(String fillerToken) {
    this.fillerToken = null == fillerToken ? new char[0] : fillerToken.toCharArray();
  }

  @Override
  public boolean incrementToken() throws IOException {
    boolean tokenAvailable = false;
    int builtGramSize = 0;
    if (gramSize.atMinValue() || inputWindow.size() < gramSize.getValue()) {
      shiftInputWindow();
      gramBuilder.setLength(0);
    } else {
      builtGramSize = gramSize.getPreviousValue();
    }
    if (inputWindow.size() >= gramSize.getValue()) {
      boolean isAllFiller = true;
      InputWindowToken nextToken = null;
      Iterator<InputWindowToken> iter = inputWindow.iterator();
      for (int gramNum = 1 ;
           iter.hasNext() && builtGramSize < gramSize.getValue() ;
           ++gramNum) {
        nextToken = iter.next();
        if (builtGramSize < gramNum) {
          if (builtGramSize > 0) {
            gramBuilder.append(tokenSeparator);
          }
          gramBuilder.append(nextToken.termAtt.buffer(), 0, 
                             nextToken.termAtt.length());
          ++builtGramSize;
        }
        if (isAllFiller && nextToken.isFiller) {
          if (gramNum == gramSize.getValue()) {
            gramSize.advance();
          }
        } else { 
          isAllFiller = false;
        }
      }
      if ( ! isAllFiller && builtGramSize == gramSize.getValue()) {
        inputWindow.getFirst().attSource.copyTo(this);
        posIncrAtt.setPositionIncrement(isOutputHere ? 0 : 1);
        termAtt.setEmpty().append(gramBuilder);
        if (gramSize.getValue() > 1) {
          typeAtt.setType(tokenType);
          noShingleOutput = false;
        }
        offsetAtt.setOffset(offsetAtt.startOffset(), nextToken.offsetAtt.endOffset());
        posLenAtt.setPositionLength(builtGramSize);
        isOutputHere = true;
        gramSize.advance();
        tokenAvailable = true;
      }
    }
    return tokenAvailable;
  }

  private boolean exhausted;

  /**
   * <p>Get the next token from the input stream.
   * <p>If the next token has <code>positionIncrement &gt; 1</code>,
   * <code>positionIncrement - 1</code> {@link #fillerToken}s are
   * inserted first.
   * @param target Where to put the new token; if null, a new instance is created.
   * @return On success, the populated token; null otherwise
   * @throws IOException if the input stream has a problem
   */
  private InputWindowToken getNextToken(InputWindowToken target) 
    throws IOException {
    InputWindowToken newTarget = target;
    if (numFillerTokensToInsert > 0) {
      if (null == target) {
        newTarget = new InputWindowToken(nextInputStreamToken.cloneAttributes());
      } else {
        nextInputStreamToken.copyTo(target.attSource);
      }
      // A filler token occupies no space
      newTarget.offsetAtt.setOffset(newTarget.offsetAtt.startOffset(), 
                                    newTarget.offsetAtt.startOffset());
      newTarget.termAtt.copyBuffer(fillerToken, 0, fillerToken.length);
      newTarget.isFiller = true;
      --numFillerTokensToInsert;
    } else if (isNextInputStreamToken) {
      if (null == target) {
        newTarget = new InputWindowToken(nextInputStreamToken.cloneAttributes());
      } else {
        nextInputStreamToken.copyTo(target.attSource);
      }
      isNextInputStreamToken = false;
      newTarget.isFiller = false;
    } else if (!exhausted) {
      if (input.incrementToken()) {
        if (null == target) {
          newTarget = new InputWindowToken(cloneAttributes());
        } else {
          this.copyTo(target.attSource);
        }
        if (posIncrAtt.getPositionIncrement() > 1) {
          // Each output shingle must contain at least one input token, 
          // so no more than (maxShingleSize - 1) filler tokens will be inserted.
          numFillerTokensToInsert = Math.min(posIncrAtt.getPositionIncrement() - 1, maxShingleSize - 1);
          // Save the current token as the next input stream token
          if (null == nextInputStreamToken) {
            nextInputStreamToken = cloneAttributes();
          } else {
            this.copyTo(nextInputStreamToken);
          }
          isNextInputStreamToken = true;
          // A filler token occupies no space
          newTarget.offsetAtt.setOffset(offsetAtt.startOffset(), offsetAtt.startOffset());
          newTarget.termAtt.copyBuffer(fillerToken, 0, fillerToken.length);
          newTarget.isFiller = true;
          --numFillerTokensToInsert;
        } else {
          newTarget.isFiller = false;
        }
      } else {
        exhausted = true;
        input.end();
        endState = captureState();
        numFillerTokensToInsert = Math.min(posIncrAtt.getPositionIncrement(), maxShingleSize - 1);
        if (numFillerTokensToInsert > 0) {
          nextInputStreamToken = new AttributeSource(getAttributeFactory());
          nextInputStreamToken.addAttribute(CharTermAttribute.class);
          OffsetAttribute newOffsetAtt = nextInputStreamToken.addAttribute(OffsetAttribute.class);
          newOffsetAtt.setOffset(offsetAtt.endOffset(), offsetAtt.endOffset());
          // Recurse/loop just once:
          return getNextToken(target);
        } else {
          newTarget = null;
        }
      }
    } else {
      newTarget = null;
    }
    return newTarget;
  }

  @Override
  public void end() throws IOException {
    if (!exhausted) {
      super.end();
    } else {
      restoreState(endState);
    }
  }

  /**
   * <p>Fills {@link #inputWindow} with input stream tokens, if available, 
   * shifting to the right if the window was previously full.
   * <p>Resets {@link #gramSize} to its minimum value.
   *
   * @throws IOException if there's a problem getting the next token
   */
  private void shiftInputWindow() throws IOException {
    InputWindowToken firstToken = null;
    if (inputWindow.size() > 0) {
      firstToken = inputWindow.removeFirst();
    }
    while (inputWindow.size() < maxShingleSize) {
      if (null != firstToken) {  // recycle the firstToken, if available
        if (null != getNextToken(firstToken)) {
          inputWindow.add(firstToken); // the firstToken becomes the last
          firstToken = null;
        } else {
          break; // end of input stream
        }
      } else {
        InputWindowToken nextToken = getNextToken(null);
        if (null != nextToken) {
          inputWindow.add(nextToken);
        } else {
          break; // end of input stream
        }
      }
    }
    if (outputUnigramsIfNoShingles && noShingleOutput 
        && gramSize.minValue > 1 && inputWindow.size() < minShingleSize) {
      gramSize.minValue = 1;
    }
    gramSize.reset();
    isOutputHere = false;
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    gramSize.reset();
    inputWindow.clear();
    nextInputStreamToken = null;
    isNextInputStreamToken = false;
    numFillerTokensToInsert = 0;
    isOutputHere = false;
    noShingleOutput = true;
    exhausted = false;
    endState = null;
    if (outputUnigramsIfNoShingles && ! outputUnigrams) {
      // Fix up gramSize if minValue was reset for outputUnigramsIfNoShingles
      gramSize.minValue = minShingleSize;
    }
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
    private int previousValue;
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
     */
    public void advance() {
      previousValue = value;
      if (value == 1) {
        value = minShingleSize;
      } else if (value == maxShingleSize) {
        reset();
      } else {
        ++value;
      }
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
      previousValue = value = minValue;
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

    /**
     * @return the value this instance had before the last advance() call
     */
    public int getPreviousValue() {
      return previousValue;
    }
  }
    
  private class InputWindowToken {
    final AttributeSource attSource;
    final CharTermAttribute termAtt;
    final OffsetAttribute offsetAtt;
    boolean isFiller = false;
      
    public InputWindowToken(AttributeSource attSource) {
      this.attSource = attSource;
      this.termAtt = attSource.getAttribute(CharTermAttribute.class);
      this.offsetAtt = attSource.getAttribute(OffsetAttribute.class);
    }
  }
}
