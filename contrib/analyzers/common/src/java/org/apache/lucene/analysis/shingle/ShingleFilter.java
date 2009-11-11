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
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.AttributeSource.State;

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

  private LinkedList<State> shingleBuf = new LinkedList<State>();
  private StringBuilder[] shingles;
  private String tokenType = "shingle";

  /**
   * filler token for when positionIncrement is more than 1
   */
  public static final char[] FILLER_TOKEN = { '_' };


  /**
   * default maximum shingle size is 2.
   */
  public static final int DEFAULT_MAX_SHINGLE_SIZE = 2;

  /**
   * The string to use when joining adjacent tokens to form a shingle
   */
  public static final String TOKEN_SEPARATOR = " ";

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
   * Constructs a ShingleFilter with the specified single size from the
   * {@link TokenStream} <code>input</code>
   *
   * @param input input stream
   * @param maxShingleSize maximum shingle size produced by the filter.
   */
  public ShingleFilter(TokenStream input, int maxShingleSize) {
    super(input);
    setMaxShingleSize(maxShingleSize);
    this.termAtt = addAttribute(TermAttribute.class);
    this.offsetAtt = addAttribute(OffsetAttribute.class);
    this.posIncrAtt = addAttribute(PositionIncrementAttribute.class);
    this.typeAtt = addAttribute(TypeAttribute.class);
  }

  /**
   * Construct a ShingleFilter with default shingle size.
   *
   * @param input input stream
   */
  public ShingleFilter(TokenStream input) {
    this(input, DEFAULT_MAX_SHINGLE_SIZE);
  }

  /**
   * Construct a ShingleFilter with the specified token type for shingle tokens.
   *
   * @param input input stream
   * @param tokenType token type for shingle tokens
   */
  public ShingleFilter(TokenStream input, String tokenType) {
    this(input, DEFAULT_MAX_SHINGLE_SIZE);
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
    shingles = new StringBuilder[maxShingleSize];
    for (int i = 0; i < shingles.length; i++) {
      shingles[i] = new StringBuilder();
    }
    this.maxShingleSize = maxShingleSize;
  }

  /**
   * Clear the StringBuilders that are used for storing the output shingles.
   */
  private void clearShingles() {
    for (int i = 0; i < shingles.length; i++) {
      shingles[i].setLength(0);
    }
  }
  
  private AttributeSource.State nextToken;
  private int shingleBufferPosition;
  private int[] endOffsets;

  /* (non-Javadoc)
   * @see org.apache.lucene.analysis.TokenStream#next()
   */
  @Override
  public final boolean incrementToken() throws IOException {
    while (true) {
      if (nextToken == null) {
        if (!fillShingleBuffer()) {
          return false;
        }
      }
      
      nextToken = shingleBuf.getFirst();
      
      if (outputUnigrams) {
        if (shingleBufferPosition == 0) {
          restoreState(nextToken);
          posIncrAtt.setPositionIncrement(1);
          shingleBufferPosition++;
          return true;
        }
      } else {
        shingleBufferPosition++;
      }
  
      if (shingleBufferPosition < shingleBuf.size()) {
        restoreState(nextToken);
        typeAtt.setType(tokenType);
        offsetAtt.setOffset(offsetAtt.startOffset(), endOffsets[shingleBufferPosition]);
        StringBuilder buf = shingles[shingleBufferPosition];
        int termLength = buf.length();
        char[] termBuffer = termAtt.termBuffer();
        if (termBuffer.length < termLength)
          termBuffer = termAtt.resizeTermBuffer(termLength);
        buf.getChars(0, termLength, termBuffer, 0);
        termAtt.setTermLength(termLength);
        if ((! outputUnigrams) && shingleBufferPosition == 1) {
          posIncrAtt.setPositionIncrement(1);
        } else {
          posIncrAtt.setPositionIncrement(0);
        }
        shingleBufferPosition++;
        if (shingleBufferPosition == shingleBuf.size()) {
          nextToken = null;
          shingleBufferPosition = 0;
        }
        return true;
      } else {
        nextToken = null;
        shingleBufferPosition = 0;
      }
    }
  }
  
  private int numFillerTokensToInsert;
  private AttributeSource.State currentToken;
  private boolean hasCurrentToken;
   
  private TermAttribute termAtt;
  private OffsetAttribute offsetAtt;
  private PositionIncrementAttribute posIncrAtt;
  private TypeAttribute typeAtt;
  
  /**
   * Get the next token from the input stream and push it on the token buffer.
   * If we encounter a token with position increment > 1, we put filler tokens
   * on the token buffer.
   * <p/>
   * Returns null when the end of the input stream is reached.
   * @return the next token, or null if at end of input stream
   * @throws IOException if the input stream has a problem
   */
  private boolean getNextToken() throws IOException {
    
    while (true) {
  	  if (numFillerTokensToInsert > 0) {
  	    if (currentToken == null) {
  	      currentToken = captureState();
  	    } else {
  	      restoreState(currentToken);
  	    }
  	    numFillerTokensToInsert--;
        // A filler token occupies no space
  	    offsetAtt.setOffset(offsetAtt.startOffset(), offsetAtt.startOffset());
  	    termAtt.setTermBuffer(FILLER_TOKEN, 0, FILLER_TOKEN.length);
        return true;
  	  } 
  	  
  	  if (hasCurrentToken) {
  	    if (currentToken != null) {
  	      restoreState(currentToken);
  	      currentToken = null;
  	    }
  	    hasCurrentToken = false;
  	    return true;
  	  }
  	  
  	  if (!input.incrementToken()) return false;
  	  hasCurrentToken = true;
  	  
  	  if (posIncrAtt.getPositionIncrement() > 1) {
  	    numFillerTokensToInsert = posIncrAtt.getPositionIncrement() - 1;
  	  }
    }
	}

  /**
   * Fill the output buffer with new shingles.
   *
   * @throws IOException if there's a problem getting the next token
   */
  private boolean fillShingleBuffer() throws IOException {
    boolean addedToken = false;
    /*
     * Try to fill the shingle buffer.
     */
    do {
      if (getNextToken()) {
        shingleBuf.add(captureState());
        if (shingleBuf.size() > maxShingleSize)
        {
          shingleBuf.removeFirst();
        }
        addedToken = true;
      } else {
        break;
      }
    } while (shingleBuf.size() < maxShingleSize);

    if (shingleBuf.isEmpty()) {
      return false;
    }
    
    /*
     * If no new token could be added to the shingle buffer, we have reached
     * the end of the input stream and have to discard the least recent token.
     */
    if (! addedToken) {
      shingleBuf.removeFirst();
    }
    
    if (shingleBuf.isEmpty()) {
      return false;
    }

    clearShingles();

    endOffsets = new int[shingleBuf.size()];
    for (int i = 0; i < endOffsets.length; i++) {
      endOffsets[i] = 0;
    }

    int i = 0;
    for (Iterator<State> it = shingleBuf.iterator(); it.hasNext(); ) {
      restoreState(it.next());
      for (int j = i; j < shingles.length; j++) {
        if (shingles[j].length() != 0) {
          shingles[j].append(TOKEN_SEPARATOR);
        }
        shingles[j].append(termAtt.termBuffer(), 0, termAtt.termLength());
      }

      endOffsets[i] = offsetAtt.endOffset();
      i++;
    }
    
    return true;
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    nextToken = null;
    shingleBufferPosition = 0;
    shingleBuf.clear();
    numFillerTokensToInsert = 0;
    currentToken = null;
    hasCurrentToken = false;
  }
}
