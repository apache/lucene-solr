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

package org.apache.solr.analysis;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.solr.util.CharArrayMap;

import java.io.IOException;

/**
 * A TokenFilter which filters out Tokens at the same position and Term text as the previous token in the stream.
 */
public final class RemoveDuplicatesTokenFilter extends TokenFilter {

  private final TermAttribute termAttribute = (TermAttribute) addAttribute(TermAttribute.class);
  private final PositionIncrementAttribute posIncAttribute =  (PositionIncrementAttribute) addAttribute(PositionIncrementAttribute.class);
  
  // keep a seen 'set' after each term with posInc > 0
  // for now use CharArrayMap vs CharArraySet, as it has clear()
  private final CharArrayMap<Boolean> previous = new CharArrayMap<Boolean>(8, false);

  /**
   * Creates a new RemoveDuplicatesTokenFilter
   *
   * @param in TokenStream that will be filtered
   */
  public RemoveDuplicatesTokenFilter(TokenStream in) {
    super(in);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean incrementToken() throws IOException {
    while (input.incrementToken()) {
      final char term[] = termAttribute.termBuffer();
      final int length = termAttribute.termLength();
      final int posIncrement = posIncAttribute.getPositionIncrement();
      
      if (posIncrement > 0) {
        previous.clear();
      }
      
      boolean duplicate = (posIncrement == 0 && previous.get(term, 0, length) != null);
      
      // clone the term, and add to the set of seen terms.
      char saved[] = new char[length];
      System.arraycopy(term, 0, saved, 0, length);
      previous.put(saved, Boolean.TRUE);
      
      if (!duplicate) {
        return true;
      }
    }
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void reset() throws IOException {
    super.reset();
    previous.clear();
  }
} 
