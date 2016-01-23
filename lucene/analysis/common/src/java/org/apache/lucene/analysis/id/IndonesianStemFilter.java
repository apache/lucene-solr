package org.apache.lucene.analysis.id;

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
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.KeywordAttribute;

/**
 * A {@link TokenFilter} that applies {@link IndonesianStemmer} to stem Indonesian words.
 */
public final class IndonesianStemFilter extends TokenFilter {
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final KeywordAttribute keywordAtt = addAttribute(KeywordAttribute.class);
  private final IndonesianStemmer stemmer = new IndonesianStemmer();
  private final boolean stemDerivational;

  /**
   * Calls {@link #IndonesianStemFilter(TokenStream, boolean) IndonesianStemFilter(input, true)}
   */
  public IndonesianStemFilter(TokenStream input) {
    this(input, true);
  }
  
  /**
   * Create a new IndonesianStemFilter.
   * <p>
   * If <code>stemDerivational</code> is false, 
   * only inflectional suffixes (particles and possessive pronouns) are stemmed.
   */
  public IndonesianStemFilter(TokenStream input, boolean stemDerivational) {
    super(input);
    this.stemDerivational = stemDerivational;
  }

  @Override
  public boolean incrementToken() throws IOException {
    if (input.incrementToken()) {
      if(!keywordAtt.isKeyword()) {
        final int newlen = 
          stemmer.stem(termAtt.buffer(), termAtt.length(), stemDerivational);
        termAtt.setLength(newlen);
      }
      return true;
    } else {
      return false;
    }
  }
}
