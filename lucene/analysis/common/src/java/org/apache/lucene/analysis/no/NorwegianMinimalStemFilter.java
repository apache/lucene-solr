package org.apache.lucene.analysis.no;

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
import org.apache.lucene.analysis.miscellaneous.SetKeywordMarkerFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.KeywordAttribute;

/**
 * A {@link TokenFilter} that applies {@link NorwegianMinimalStemmer} to stem Norwegian
 * words.
 * <p>
 * To prevent terms from being stemmed use an instance of
 * {@link SetKeywordMarkerFilter} or a custom {@link TokenFilter} that sets
 * the {@link KeywordAttribute} before this {@link TokenStream}.
 * </p>
 */
public final class NorwegianMinimalStemFilter extends TokenFilter {
  private final NorwegianMinimalStemmer stemmer;
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final KeywordAttribute keywordAttr = addAttribute(KeywordAttribute.class);

  /** 
   * Calls {@link #NorwegianMinimalStemFilter(TokenStream, int) 
   * NorwegianMinimalStemFilter(input, BOKMAAL)}
   */
  public NorwegianMinimalStemFilter(TokenStream input) {
    this(input, NorwegianLightStemmer.BOKMAAL);
  }
  
  /** 
   * Creates a new NorwegianLightStemFilter
   * @param flags set to {@link NorwegianLightStemmer#BOKMAAL}, 
   *                     {@link NorwegianLightStemmer#NYNORSK}, or both.
   */
  public NorwegianMinimalStemFilter(TokenStream input, int flags) {
    super(input);
    this.stemmer = new NorwegianMinimalStemmer(flags);
  }
  
  @Override
  public boolean incrementToken() throws IOException {
    if (input.incrementToken()) {
      if (!keywordAttr.isKeyword()) {
        final int newlen = stemmer.stem(termAtt.buffer(), termAtt.length());
        termAtt.setLength(newlen);
      }
      return true;
    } else {
      return false;
    }
  }
}
