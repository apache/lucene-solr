package org.apache.solr.analysis;
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

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.reverse.ReverseStringFilter;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;

/**
 * This class produces a special form of reversed tokens, suitable for
 * better handling of leading wildcards. Tokens from the input TokenStream
 * are reversed and prepended with a special "reversed" marker character.
 * If <code>withOriginal<code> argument is <code>true</code> then first the
 * original token is returned, and then the reversed token (with
 * <code>positionIncrement == 0</code>) is returned. Otherwise only reversed
 * tokens are returned.
 * <p>Note: this filter doubles the number of tokens in the input stream when
 * <code>withOriginal == true</code>, which proportionally increases the size
 * of postings and term dictionary in the index.
 */
public class ReversedWildcardFilter extends TokenFilter {
  
  private boolean withOriginal;
  private char markerChar;
  private State save;
  private TermAttribute termAtt;
  private PositionIncrementAttribute posAtt;

  protected ReversedWildcardFilter(TokenStream input, boolean withOriginal, char markerChar) {
    super(input);
    this.termAtt = (TermAttribute)addAttribute(TermAttribute.class);
    this.posAtt = (PositionIncrementAttribute) addAttribute(PositionIncrementAttribute.class);
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
    int oldLen = termAtt.termLength();
    if (oldLen ==0) return true;
    int origOffset = posAtt.getPositionIncrement();
    if (withOriginal == true){
      posAtt.setPositionIncrement(0);
      save = captureState();
    }
    char [] buffer = termAtt.resizeTermBuffer(oldLen + 1);
    buffer[oldLen] = markerChar;
    //String reversed = reverseAndMark(value, markerChar);
    ReverseStringFilter.reverse(buffer, oldLen + 1);

    posAtt.setPositionIncrement(origOffset);
    termAtt.setTermBuffer(buffer, 0, oldLen +1);
    return true;
  }
  
   
}
