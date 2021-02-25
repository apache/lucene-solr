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

import org.apache.lucene.analysis.FilteringTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

/**
 * Removes words that are too long or too short from the stream.
 *
 * <p>Note: Length is calculated as the number of Unicode codepoints.
 */
public final class CodepointCountFilter extends FilteringTokenFilter {

  private final int min;
  private final int max;

  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

  /**
   * Create a new {@link CodepointCountFilter}. This will filter out tokens whose {@link
   * CharTermAttribute} is either too short ({@link Character#codePointCount(char[], int, int)} &lt;
   * min) or too long ({@link Character#codePointCount(char[], int, int)} &gt; max).
   *
   * @param in the {@link TokenStream} to consume
   * @param min the minimum length
   * @param max the maximum length
   */
  public CodepointCountFilter(TokenStream in, int min, int max) {
    super(in);
    if (min < 0) {
      throw new IllegalArgumentException("minimum length must be greater than or equal to zero");
    }
    if (min > max) {
      throw new IllegalArgumentException("maximum length must not be greater than minimum length");
    }
    this.min = min;
    this.max = max;
  }

  @Override
  public boolean accept() {
    final int max32 = termAtt.length();
    final int min32 = max32 >> 1;
    if (min32 >= min && max32 <= max) {
      // definitely within range
      return true;
    } else if (min32 > max || max32 < min) {
      // definitely not
      return false;
    } else {
      // we must count to be sure
      int len = Character.codePointCount(termAtt.buffer(), 0, termAtt.length());
      return (len >= min && len <= max);
    }
  }
}
