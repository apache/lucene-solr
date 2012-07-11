package org.apache.lucene.analysis.util;

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

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;

/**
 * Abstract base class for TokenFilters that may remove tokens.
 * You have to implement {@link #accept} and return a boolean if the current
 * token should be preserved. {@link #incrementToken} uses this method
 * to decide if a token should be passed to the caller.
 */
public abstract class FilteringTokenFilter extends TokenFilter {

  private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);
  private boolean enablePositionIncrements; // no init needed, as ctor enforces setting value!
  private boolean first = true; // only used when not preserving gaps

  public FilteringTokenFilter(boolean enablePositionIncrements, TokenStream input){
    super(input);
    this.enablePositionIncrements = enablePositionIncrements;
  }

  /** Override this method and return if the current input token should be returned by {@link #incrementToken}. */
  protected abstract boolean accept() throws IOException;

  @Override
  public final boolean incrementToken() throws IOException {
    if (enablePositionIncrements) {
      int skippedPositions = 0;
      while (input.incrementToken()) {
        if (accept()) {
          if (skippedPositions != 0) {
            posIncrAtt.setPositionIncrement(posIncrAtt.getPositionIncrement() + skippedPositions);
          }
          return true;
        }
        skippedPositions += posIncrAtt.getPositionIncrement();
      }
    } else {
      while (input.incrementToken()) {
        if (accept()) {
          if (first) {
            // first token having posinc=0 is illegal.
            if (posIncrAtt.getPositionIncrement() == 0) {
              posIncrAtt.setPositionIncrement(1);
            }
            first = false;
          }
          return true;
        }
      }
    }
    // reached EOS -- return false
    return false;
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    first = true;
  }

  /**
   * @see #setEnablePositionIncrements(boolean)
   */
  public boolean getEnablePositionIncrements() {
    return enablePositionIncrements;
  }

  /**
   * If <code>true</code>, this TokenFilter will preserve
   * positions of the incoming tokens (ie, accumulate and
   * set position increments of the removed tokens).
   * Generally, <code>true</code> is best as it does not
   * lose information (positions of the original tokens)
   * during indexing.
   * 
   * <p> When set, when a token is stopped
   * (omitted), the position increment of the following
   * token is incremented.
   *
   * <p> <b>NOTE</b>: be sure to also
   * set org.apache.lucene.queryparser.classic.QueryParser#setEnablePositionIncrements if
   * you use QueryParser to create queries.
   */
  public void setEnablePositionIncrements(boolean enable) {
    this.enablePositionIncrements = enable;
  }
}
