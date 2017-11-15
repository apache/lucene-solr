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
package org.apache.lucene.analysis.ar;


import java.io.IOException;

import org.apache.lucene.analysis.miscellaneous.SetKeywordMarkerFilter; // javadoc @link
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.KeywordAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

/**
 * A {@link TokenFilter} that applies {@link ArabicStemmer} to stem Arabic words..
 * <p>
 * To prevent terms from being stemmed use an instance of
 * {@link SetKeywordMarkerFilter} or a custom {@link TokenFilter} that sets
 * the {@link KeywordAttribute} before this {@link TokenStream}.
 * </p>
 * @see SetKeywordMarkerFilter */

public final class ArabicStemFilter extends TokenFilter {
  private final ArabicStemmer stemmer = new ArabicStemmer();
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final KeywordAttribute keywordAttr = addAttribute(KeywordAttribute.class);
  private boolean useEnhancedStemmer = false;
  private boolean highAccuracy = false;


  public ArabicStemFilter(TokenStream input) {
    super(input);
  }
  
  public ArabicStemFilter(TokenStream input, CharArraySet validatedStemList) {
    super(input);
    stemmer.setValidatedStemList(validatedStemList);
  }
  
  public boolean isUseEnhancedStemmer() {
    return useEnhancedStemmer;
  }

  public void setUseEnhancedStemmer(boolean useEnhancedStemmer) {
    this.useEnhancedStemmer = useEnhancedStemmer;
    stemmer.setUseEnhancedStemmer(useEnhancedStemmer);
  }
  
  public boolean isHighAccuracy() {
    return highAccuracy;
  }

  public void setHighAccuracy(boolean highAccuracy) {
    this.highAccuracy = highAccuracy;
    stemmer.setHighAccuracy(highAccuracy);
  }

  @Override
  public boolean incrementToken() throws IOException {
    if (input.incrementToken()) {
      if(!keywordAttr.isKeyword()) {
        final int newlen = stemmer.stem(termAtt.buffer(), termAtt.length());
        if(useEnhancedStemmer)
          termAtt.copyBuffer(stemmer.getStr(), 0, newlen);
        termAtt.setLength(newlen);
      }
      return true;
    } else {
      return false;
    }
  }
  
}
