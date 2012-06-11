package org.apache.lucene.analysis.position;

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

/** Set the positionIncrement of all tokens to the "positionIncrement",
 * except the first return token which retains its original positionIncrement value.
 * The default positionIncrement value is zero.
 */
public final class PositionFilter extends TokenFilter {

  /** Position increment to assign to all but the first token - default = 0 */
  private final int positionIncrement;
  
  /** The first token must have non-zero positionIncrement **/
  private boolean firstTokenPositioned = false;
  
  private PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);

  /**
   * Constructs a PositionFilter that assigns a position increment of zero to
   * all but the first token from the given input stream.
   * 
   * @param input the input stream
   */
  public PositionFilter(final TokenStream input) {
    this(input, 0);
  }

  /**
   * Constructs a PositionFilter that assigns the given position increment to
   * all but the first token from the given input stream.
   * 
   * @param input the input stream
   * @param positionIncrement position increment to assign to all but the first
   *  token from the input stream
   */
  public PositionFilter(final TokenStream input, final int positionIncrement) {
    super(input);
    if (positionIncrement < 0) {
      throw new IllegalArgumentException("positionIncrement may not be negative");
    }
    this.positionIncrement = positionIncrement;
  }

  @Override
  public final boolean incrementToken() throws IOException {
    if (input.incrementToken()) {
      if (firstTokenPositioned) {
        posIncrAtt.setPositionIncrement(positionIncrement);
      } else {
        firstTokenPositioned = true;
      }
      return true;
    } else {
      return false;
    }
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    firstTokenPositioned = false;
  }
}
