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
package org.apache.lucene.analysis;

import java.io.IOException;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;

/**
 * Abstract base class for TokenFilters that may remove tokens. You have to implement {@link
 * #accept} and return a boolean if the current token should be preserved. {@link #incrementToken}
 * uses this method to decide if a token should be passed to the caller.
 */
public abstract class FilteringTokenFilter extends TokenFilter {

  private final PositionIncrementAttribute posIncrAtt =
      addAttribute(PositionIncrementAttribute.class);
  private int skippedPositions;

  /**
   * Create a new {@link FilteringTokenFilter}.
   *
   * @param in the {@link TokenStream} to consume
   */
  public FilteringTokenFilter(TokenStream in) {
    super(in);
  }

  /**
   * Override this method and return if the current input token should be returned by {@link
   * #incrementToken}.
   */
  protected abstract boolean accept() throws IOException;

  @Override
  public final boolean incrementToken() throws IOException {
    skippedPositions = 0;
    while (input.incrementToken()) {
      if (accept()) {
        if (skippedPositions != 0) {
          posIncrAtt.setPositionIncrement(posIncrAtt.getPositionIncrement() + skippedPositions);
        }
        return true;
      }
      skippedPositions += posIncrAtt.getPositionIncrement();
    }

    // reached EOS -- return false
    return false;
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    skippedPositions = 0;
  }

  @Override
  public void end() throws IOException {
    super.end();
    posIncrAtt.setPositionIncrement(posIncrAtt.getPositionIncrement() + skippedPositions);
  }
}
