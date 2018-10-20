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
package org.apache.lucene.analysis.miscellaneous;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;

import java.io.IOException;

/**
 * This TokenFilter limits its emitted tokens to those with positions that
 * are not greater than the configured limit.
 * <p>
 * By default, this filter ignores any tokens in the wrapped {@code TokenStream}
 * once the limit has been exceeded, which can result in {@code reset()} being 
 * called prior to {@code incrementToken()} returning {@code false}.  For most 
 * {@code TokenStream} implementations this should be acceptable, and faster 
 * then consuming the full stream. If you are wrapping a {@code TokenStream}
 * which requires that the full stream of tokens be exhausted in order to 
 * function properly, use the 
 * {@link #LimitTokenPositionFilter(TokenStream,int,boolean) consumeAllTokens}
 * option.
 */
public final class LimitTokenPositionFilter extends TokenFilter {

  private final int maxTokenPosition;
  private final boolean consumeAllTokens;
  private int tokenPosition = 0;
  private boolean exhausted = false;
  private final PositionIncrementAttribute posIncAtt = addAttribute(PositionIncrementAttribute.class);

  /**
   * Build a filter that only accepts tokens up to and including the given maximum position.
   * This filter will not consume any tokens with position greater than the maxTokenPosition limit.

   * @param in the stream to wrap
   * @param maxTokenPosition max position of tokens to produce (1st token always has position 1)
   *                         
   * @see #LimitTokenPositionFilter(TokenStream,int,boolean)
   */
  public LimitTokenPositionFilter(TokenStream in, int maxTokenPosition) {
    this(in, maxTokenPosition, false);
  }

  /**
   * Build a filter that limits the maximum position of tokens to emit.
   * 
   * @param in the stream to wrap
   * @param maxTokenPosition max position of tokens to produce (1st token always has position 1)
   * @param consumeAllTokens whether all tokens from the wrapped input stream must be consumed
   *                         even if maxTokenPosition is exceeded.
   */
  public LimitTokenPositionFilter(TokenStream in, int maxTokenPosition, boolean consumeAllTokens) {
    super(in);
    if (maxTokenPosition < 1) {
      throw new IllegalArgumentException("maxTokenPosition must be greater than zero");
    }
    this.maxTokenPosition = maxTokenPosition;
    this.consumeAllTokens = consumeAllTokens;
  }

  @Override
  public boolean incrementToken() throws IOException {
    if (exhausted) {
      return false;
    }
    if (input.incrementToken()) {
      tokenPosition += posIncAtt.getPositionIncrement();
      if (tokenPosition <= maxTokenPosition) {
        return true;
      } else {
        while (consumeAllTokens && input.incrementToken()) { /* NOOP */ }
        exhausted = true;
        return false;
      }
    } else {
      exhausted = true;
      return false;
    }
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    tokenPosition = 0;
    exhausted = false;
  }
}
