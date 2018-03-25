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
import java.util.ArrayDeque;
import java.util.Deque;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.util.AttributeSource;

/**
 * A FixedShingleFilter constructs shingles (token n-grams) from a token stream.
 * In other words, it creates combinations of tokens as a single token.
 * <p>
 * Unlike the {@link ShingleFilter}, FixedShingleFilter only emits shingles of a
 * fixed size, and never emits unigrams, even at the end of a TokenStream. In
 * addition, if the filter encounters stacked tokens (eg synonyms), then it will
 * output stacked shingles
 * <p>
 * For example, the sentence "please divide this sentence into shingles"
 * might be tokenized into shingles "please divide", "divide this",
 * "this sentence", "sentence into", and "into shingles".
 * <p>
 * This filter handles position increments &gt; 1 by inserting filler tokens
 * (tokens with termtext "_").
 *
 * @lucene.experimental
 */
public final class FixedShingleFilter extends TokenFilter {

  private final Deque<Token> tokenPool = new ArrayDeque<>();

  private static final int MAX_SHINGLE_STACK_SIZE = 1000;
  private static final int MAX_SHINGLE_SIZE = 4;

  private final int shingleSize;
  private final String tokenSeparator;

  private final Token gapToken = new Token(new AttributeSource());
  private final Token endToken = new Token(new AttributeSource());

  private final PositionIncrementAttribute incAtt = addAttribute(PositionIncrementAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);

  private Token[] currentShingleTokens;
  private int currentShingleStackSize;
  private boolean inputStreamExhausted = false;

  /**
   * Creates a FixedShingleFilter over an input token stream
   *
   * @param input       the input stream
   * @param shingleSize the shingle size
   */
  public FixedShingleFilter(TokenStream input, int shingleSize) {
    this(input, shingleSize, " ", "_");
  }

  /**
   * Creates a FixedShingleFilter over an input token stream
   *
   * @param input          the input tokenstream
   * @param shingleSize    the shingle size
   * @param tokenSeparator a String to use as a token separator
   * @param fillerToken    a String to use to represent gaps in the input stream (due to eg stopwords)
   */
  public FixedShingleFilter(TokenStream input, int shingleSize, String tokenSeparator, String fillerToken) {
    super(input);
    if (shingleSize <= 1 || shingleSize > MAX_SHINGLE_SIZE) {
      throw new IllegalArgumentException("Shingle size must be between 2 and " + MAX_SHINGLE_SIZE + ", got " + shingleSize);
    }
    this.shingleSize = shingleSize;
    this.tokenSeparator = tokenSeparator;
    this.gapToken.termAtt.setEmpty().append(fillerToken);
    this.currentShingleTokens = new Token[shingleSize];
  }

  @Override
  public boolean incrementToken() throws IOException {
    int posInc = 0;
    if (nextShingle() == false) {
      Token nextRoot = nextTokenInStream(currentShingleTokens[0]);
      if (nextRoot == endToken)
        return false;
      recycleToken(currentShingleTokens[0]);
      if (resetShingleRoot(nextRoot) == false) {
        return false;
      }
      posInc = currentShingleTokens[0].posInc();
    }
    clearAttributes();
    incAtt.setPositionIncrement(posInc);
    offsetAtt.setOffset(currentShingleTokens[0].startOffset(), lastTokenInShingle().endOffset());
    termAtt.setEmpty();
    termAtt.append(currentShingleTokens[0].term());
    typeAtt.setType("shingle");
    for (int i = 1; i < shingleSize; i++) {
      termAtt.append(tokenSeparator).append(currentShingleTokens[i].term());
    }
    return true;
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    this.tokenPool.clear();
    this.currentShingleTokens[0] = null;
    this.inputStreamExhausted = false;
    this.currentShingleStackSize = 0;
  }

  @Override
  public void end() throws IOException {
    if (inputStreamExhausted == false) {
      finishInnerStream();
    }
    clearAttributes();
    this.offsetAtt.setOffset(0, endToken.endOffset());
  }

  private void finishInnerStream() throws IOException {
    input.end();
    inputStreamExhausted = true;
    // check for gaps at the end of the tokenstream
    endToken.posIncAtt.setPositionIncrement(this.incAtt.getPositionIncrement());
    OffsetAttribute inputOffsets = input.getAttribute(OffsetAttribute.class);
    endToken.offsetAtt.setOffset(inputOffsets.startOffset(), inputOffsets.endOffset());
  }

  private Token lastTokenInShingle() {
    int lastTokenIndex = shingleSize - 1;
    while (currentShingleTokens[lastTokenIndex] == gapToken) {
      lastTokenIndex--;
    }
    return currentShingleTokens[lastTokenIndex];
  }

  private boolean resetShingleRoot(Token token) throws IOException {
    this.currentShingleTokens[0] = token;
    for (int i = 1; i < shingleSize; i++) {
      Token current = nextTokenInGraph(this.currentShingleTokens[i - 1]);
      if (current == endToken) {
        if (endToken.posInc() + i >= shingleSize) {
          // end tokens are a special case, because their posIncs are always
          // due to stopwords.  Therefore, we can happily append gap tokens
          // to the end of the current shingle
          for (int j = i; j < shingleSize; j++) {
            this.currentShingleTokens[i] = gapToken;
            i++;
          }
          return true;
        }
        return false;
      }
      if (current.posInc() > 1) {
        // insert gaps into the shingle list
        for (int j = 1; j < current.posInc(); j++) {
          this.currentShingleTokens[i] = gapToken;
          i++;
          if (i >= shingleSize)
            return true;
        }
      }
      this.currentShingleTokens[i] = current;
    }
    return true;
  }

  private boolean nextShingle() throws IOException {
    return currentShingleTokens[0] != null && advanceStack();
  }

  // check if the next token in the tokenstream is at the same position as this one
  private boolean lastInStack(Token token) throws IOException {
    Token next = nextTokenInStream(token);
    return next == endToken || next.posInc() != 0;
  }

  private boolean advanceStack() throws IOException {
    for (int i = shingleSize - 1; i >= 1; i--) {
      if (currentShingleTokens[i] != gapToken && lastInStack(currentShingleTokens[i]) == false) {
        currentShingleTokens[i] = nextTokenInStream(currentShingleTokens[i]);
        for (int j = i + 1; j < shingleSize; j++) {
          currentShingleTokens[j] = nextTokenInGraph(currentShingleTokens[j - 1]);
        }
        if (currentShingleStackSize++ > MAX_SHINGLE_STACK_SIZE) {
          throw new IllegalStateException("Too many shingles (> " + MAX_SHINGLE_STACK_SIZE + ") at term [" + currentShingleTokens[0].term() + "]");
        }
        return true;
      }
    }
    currentShingleStackSize = 0;
    return false;
  }

  private Token newToken() {
    Token token = tokenPool.size() == 0 ? new Token(this.cloneAttributes()) : tokenPool.removeFirst();
    token.reset(this);
    return token;
  }

  private void recycleToken(Token token) {
    if (token == null)
      return;
    token.nextToken = null;
    tokenPool.add(token);
  }

  // for testing
  int instantiatedTokenCount() {
    int tokenCount = tokenPool.size() + 1;
    if (currentShingleTokens[0] == endToken || currentShingleTokens[0] == null)
      return tokenCount;
    for (Token t = currentShingleTokens[0]; t != endToken && t != null; t = t.nextToken) {
      tokenCount++;
    }
    return tokenCount;
  }

  private Token nextTokenInGraph(Token token) throws IOException {
    do {
      token = nextTokenInStream(token);
      if (token == endToken) {
        return endToken;
      }
    } while (token.posInc() == 0);
    return token;
  }

  private Token nextTokenInStream(Token token) throws IOException {
    if (token != null && token.nextToken != null) {
      return token.nextToken;
    }
    if (input.incrementToken() == false) {
      finishInnerStream();
      if (token == null) {
        return endToken;
      } else {
        token.nextToken = endToken;
        return endToken;
      }
    }
    if (token == null) {
      return newToken();
    }
    token.nextToken = newToken();
    return token.nextToken;
  }

  private static class Token {
    final AttributeSource attSource;
    final PositionIncrementAttribute posIncAtt;
    final CharTermAttribute termAtt;
    final OffsetAttribute offsetAtt;

    Token nextToken;

    Token(AttributeSource attSource) {
      this.attSource = attSource;
      this.posIncAtt = attSource.addAttribute(PositionIncrementAttribute.class);
      this.termAtt = attSource.addAttribute(CharTermAttribute.class);
      this.offsetAtt = attSource.addAttribute(OffsetAttribute.class);
    }

    int posInc() {
      return this.posIncAtt.getPositionIncrement();
    }

    CharSequence term() {
      return this.termAtt;
    }

    int startOffset() {
      return this.offsetAtt.startOffset();
    }

    int endOffset() {
      return this.offsetAtt.endOffset();
    }

    void reset(AttributeSource attSource) {
      attSource.copyTo(this.attSource);
      this.nextToken = null;
    }

    @Override
    public String toString() {
      return term() + "(" + startOffset() + "," + endOffset() + ") " + posInc();
    }
  }

}
