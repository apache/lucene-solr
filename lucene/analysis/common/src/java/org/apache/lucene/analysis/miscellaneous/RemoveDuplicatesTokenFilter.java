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
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.CharArraySet;

import java.io.IOException;

/**
 * A TokenFilter which filters out Tokens at the same position and Term text as the previous token in the stream.
 */
public final class RemoveDuplicatesTokenFilter extends TokenFilter {

  private final CharTermAttribute termAttribute = addAttribute(CharTermAttribute.class);
  private final PositionIncrementAttribute posIncAttribute =  addAttribute(PositionIncrementAttribute.class);
  
  private final CharArraySet previous = new CharArraySet(8, false);

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
      final char term[] = termAttribute.buffer();
      final int length = termAttribute.length();
      final int posIncrement = posIncAttribute.getPositionIncrement();
      
      if (posIncrement > 0) {
        previous.clear();
      }
      
      boolean duplicate = (posIncrement == 0 && previous.contains(term, 0, length));
      
      // clone the term, and add to the set of seen terms.
      char saved[] = new char[length];
      System.arraycopy(term, 0, saved, 0, length);
      previous.add(saved);
      
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
