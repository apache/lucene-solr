package org.apache.lucene.search.highlight;

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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.index.TermPositionVector;
import org.apache.lucene.index.TermVectorOffsetInfo;

/**
 * @author CMorris
 */
public class TokenStreamFromTermPositionVector extends TokenStream {

  private final List<Token> positionedTokens = new ArrayList<Token>();

  private Iterator<Token> tokensAtCurrentPosition;

  private TermAttribute termAttribute;

  private PositionIncrementAttribute positionIncrementAttribute;

  private OffsetAttribute offsetAttribute;

  /**
   * Constructor.
   * 
   * @param termPositionVector TermPositionVector that contains the data for
   *        creating the TokenStream. Must have positions and offsets.
   */
  public TokenStreamFromTermPositionVector(
      final TermPositionVector termPositionVector) {
    termAttribute = addAttribute(TermAttribute.class);
    positionIncrementAttribute = addAttribute(PositionIncrementAttribute.class);
    offsetAttribute = addAttribute(OffsetAttribute.class);
    final String[] terms = termPositionVector.getTerms();
    for (int i = 0; i < terms.length; i++) {
      final TermVectorOffsetInfo[] offsets = termPositionVector.getOffsets(i);
      final int[] termPositions = termPositionVector.getTermPositions(i);
      for (int j = 0; j < termPositions.length; j++) {
        Token token;
        if (offsets != null) {
          token = new Token(terms[i].toCharArray(), 0, terms[i].length(),
              offsets[j].getStartOffset(), offsets[j].getEndOffset());
        } else {
          token = new Token();
          token.setTermBuffer(terms[i]);
        }
        // Yes - this is the position, not the increment! This is for
        // sorting. This value
        // will be corrected before use.
        token.setPositionIncrement(termPositions[j]);
        this.positionedTokens.add(token);
      }
    }
    final Comparator<Token> tokenComparator = new Comparator<Token>() {
      public int compare(final Token o1, final Token o2) {
        if (o1.getPositionIncrement() < o2.getPositionIncrement()) {
          return -1;
        }
        if (o1.getPositionIncrement() > o2.getPositionIncrement()) {
          return 1;
        }
        return 0;
      }
    };
    Collections.sort(this.positionedTokens, tokenComparator);
    int lastPosition = -1;
    for (final Token token : this.positionedTokens) {
      int thisPosition = token.getPositionIncrement();
      token.setPositionIncrement(thisPosition - lastPosition);
      lastPosition = thisPosition;
    }
    this.tokensAtCurrentPosition = this.positionedTokens.iterator();
  }

  @Override
  public boolean incrementToken() throws IOException {
    if (this.tokensAtCurrentPosition.hasNext()) {
      final Token next = this.tokensAtCurrentPosition.next();
      termAttribute.setTermBuffer(next.term());
      positionIncrementAttribute.setPositionIncrement(next
          .getPositionIncrement());
      offsetAttribute.setOffset(next.startOffset(), next.endOffset());
      return true;
    }
    return false;
  }

  @Override
  public void reset() throws IOException {
    this.tokensAtCurrentPosition = this.positionedTokens.iterator();
  }
}
