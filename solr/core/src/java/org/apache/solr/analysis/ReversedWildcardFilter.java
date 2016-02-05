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
package org.apache.solr.analysis;
import java.io.IOException;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

/**
 * This class produces a special form of reversed tokens, suitable for
 * better handling of leading wildcards. Tokens from the input TokenStream
 * are reversed and prepended with a special "reversed" marker character.
 * If <code>withOriginal</code> argument is <code>true</code> then first the
 * original token is returned, and then the reversed token (with
 * <code>positionIncrement == 0</code>) is returned. Otherwise only reversed
 * tokens are returned.
 * <p>Note: this filter doubles the number of tokens in the input stream when
 * <code>withOriginal == true</code>, which proportionally increases the size
 * of postings and term dictionary in the index.
 */
public final class ReversedWildcardFilter extends TokenFilter {
  
  private final boolean withOriginal;
  private final char markerChar;
  private final CharTermAttribute termAtt;
  private final PositionIncrementAttribute posAtt;
  private State save = null;

  protected ReversedWildcardFilter(TokenStream input, boolean withOriginal, char markerChar) {
    super(input);
    this.termAtt = addAttribute(CharTermAttribute.class);
    this.posAtt = addAttribute(PositionIncrementAttribute.class);
    this.withOriginal = withOriginal;
    this.markerChar = markerChar;
  }

  @Override
  public boolean incrementToken() throws IOException {
    if( save != null ) {
      // clearAttributes();  // not currently necessary
      restoreState(save);
      save = null;
      return true;
    }

    if (!input.incrementToken()) return false;

    // pass through zero-length terms
    int oldLen = termAtt.length();
    if (oldLen ==0) return true;
    int origOffset = posAtt.getPositionIncrement();
    if (withOriginal == true){
      posAtt.setPositionIncrement(0);
      save = captureState();
    }
    char [] buffer = termAtt.resizeBuffer(oldLen + 1);
    buffer[oldLen] = markerChar;
    reverse(buffer, 0, oldLen + 1);

    posAtt.setPositionIncrement(origOffset);
    termAtt.copyBuffer(buffer, 0, oldLen +1);
    return true;
  }
  

  /**
   * Partially reverses the given input buffer in-place from the given offset
   * up to the given length, keeping surrogate pairs in the correct (non-reversed) order.
   * @param buffer the input char array to reverse
   * @param start the offset from where to reverse the buffer
   * @param len the length in the buffer up to where the
   *        buffer should be reversed
   */
  public static void reverse(final char[] buffer, final int start, final int len) {
    /* modified version of Apache Harmony AbstractStringBuilder reverse0() */
    if (len < 2)
      return;
    int end = (start + len) - 1;
    char frontHigh = buffer[start];
    char endLow = buffer[end];
    boolean allowFrontSur = true, allowEndSur = true;
    final int mid = start + (len >> 1);
    for (int i = start; i < mid; ++i, --end) {
      final char frontLow = buffer[i + 1];
      final char endHigh = buffer[end - 1];
      final boolean surAtFront = allowFrontSur
          && Character.isSurrogatePair(frontHigh, frontLow);
      if (surAtFront && (len < 3)) {
        // nothing to do since surAtFront is allowed and 1 char left
        return;
      }
      final boolean surAtEnd = allowEndSur
          && Character.isSurrogatePair(endHigh, endLow);
      allowFrontSur = allowEndSur = true;
      if (surAtFront == surAtEnd) {
        if (surAtFront) {
          // both surrogates
          buffer[end] = frontLow;
          buffer[--end] = frontHigh;
          buffer[i] = endHigh;
          buffer[++i] = endLow;
          frontHigh = buffer[i + 1];
          endLow = buffer[end - 1];
        } else {
          // neither surrogates
          buffer[end] = frontHigh;
          buffer[i] = endLow;
          frontHigh = frontLow;
          endLow = endHigh;
        }
      } else {
        if (surAtFront) {
          // surrogate only at the front
          buffer[end] = frontLow;
          buffer[i] = endLow;
          endLow = endHigh;
          allowFrontSur = false;
        } else {
          // surrogate only at the end
          buffer[end] = frontHigh;
          buffer[i] = endHigh;
          frontHigh = frontLow;
          allowEndSur = false;
        }
      }
    }
    if ((len & 0x01) == 1 && !(allowFrontSur && allowEndSur)) {
      // only if odd length
      buffer[end] = allowFrontSur ? endLow : frontHigh;
    }
  }
  
  @Override
  public void reset() throws IOException {
    super.reset();
    save = null;
  }

}
