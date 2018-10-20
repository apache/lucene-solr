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

import java.io.IOException;

/**
 * This TokenFilter limits the number of tokens while indexing. It is
 * a replacement for the maximum field length setting inside {@link org.apache.lucene.index.IndexWriter}.
 * <p>
 * By default, this filter ignores any tokens in the wrapped {@code TokenStream}
 * once the limit has been reached, which can result in {@code reset()} being 
 * called prior to {@code incrementToken()} returning {@code false}.  For most 
 * {@code TokenStream} implementations this should be acceptable, and faster 
 * then consuming the full stream. If you are wrapping a {@code TokenStream} 
 * which requires that the full stream of tokens be exhausted in order to 
 * function properly, use the 
 * {@link #LimitTokenCountFilter(TokenStream,int,boolean) consumeAllTokens} 
 * option.
 */
public final class LimitTokenCountFilter extends TokenFilter {

  private final int maxTokenCount;
  private final boolean consumeAllTokens;
  private int tokenCount = 0;
  private boolean exhausted = false;

  /**
   * Build a filter that only accepts tokens up to a maximum number.
   * This filter will not consume any tokens beyond the maxTokenCount limit
   *
   * @see #LimitTokenCountFilter(TokenStream,int,boolean)
   */
  public LimitTokenCountFilter(TokenStream in, int maxTokenCount) {
    this(in, maxTokenCount, false);
  }

  /**
   * Build an filter that limits the maximum number of tokens per field.
   * @param in the stream to wrap
   * @param maxTokenCount max number of tokens to produce
   * @param consumeAllTokens whether all tokens from the input must be consumed even if maxTokenCount is reached.
   */
  public LimitTokenCountFilter(TokenStream in, int maxTokenCount, boolean consumeAllTokens) {
    super(in);
    if (maxTokenCount < 1) {
      throw new IllegalArgumentException("maxTokenCount must be greater than zero");
    }
    this.maxTokenCount = maxTokenCount;
    this.consumeAllTokens = consumeAllTokens;
  }
  
  @Override
  public boolean incrementToken() throws IOException {
    if (exhausted) {
      return false;
    } else if (tokenCount < maxTokenCount) {
      if (input.incrementToken()) {
        tokenCount++;
        return true;
      } else {
        exhausted = true;
        return false;
      }
    } else {
      while (consumeAllTokens && input.incrementToken()) { /* NOOP */ }
      return false;
    }
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    tokenCount = 0;
    exhausted = false;
  }
}
