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
package org.apache.lucene.analysis.util;

import java.io.IOException;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;

/**
 * Backcompat FilteringTokenFilter for versions 4.3 and before.
 * @deprecated Use {@link org.apache.lucene.analysis.util.FilteringTokenFilter}
 */
@Deprecated
public abstract class Lucene43FilteringTokenFilter extends TokenFilter {

  private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);
  private boolean enablePositionIncrements; // no init needed, as ctor enforces setting value!
  private boolean first = true; // only used when not preserving gaps

  public Lucene43FilteringTokenFilter(boolean enablePositionIncrements, TokenStream input){
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
}
