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
package org.apache.lucene.analysis.stempel;

import java.io.IOException;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.KeywordAttribute;

/**
 * Transforms the token stream as per the stemming algorithm.
 * <p>
 * Note: the input to the stemming filter must already be in lower case, so you
 * will need to use LowerCaseFilter or LowerCaseTokenizer farther down the
 * Tokenizer chain in order for this to work properly!
 */
public final class StempelFilter extends TokenFilter {
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final KeywordAttribute keywordAtt = addAttribute(KeywordAttribute.class);
  private final StempelStemmer stemmer;
  private final int minLength;
  
  /**
   * Minimum length of input words to be processed. Shorter words are returned
   * unchanged.
   */
  public static final int DEFAULT_MIN_LENGTH = 3;
  
  /**
   * Create filter using the supplied stemming table.
   * 
   * @param in input token stream
   * @param stemmer stemmer
   */
  public StempelFilter(TokenStream in, StempelStemmer stemmer) {
    this(in, stemmer, DEFAULT_MIN_LENGTH);
  }
  
  /**
   * Create filter using the supplied stemming table.
   * 
   * @param in input token stream
   * @param stemmer stemmer
   * @param minLength For performance reasons words shorter than minLength
   * characters are not processed, but simply returned.
   */
  public StempelFilter(TokenStream in, StempelStemmer stemmer, int minLength) {
    super(in);
    this.stemmer = stemmer;
    this.minLength = minLength;
  }
  
  /** Returns the next input Token, after being stemmed */
  @Override
  public boolean incrementToken() throws IOException {
    if (input.incrementToken()) {
      if (!keywordAtt.isKeyword() && termAtt.length() > minLength) {
        StringBuilder sb = stemmer.stem(termAtt);
        if (sb != null) // if we can't stem it, return unchanged
          termAtt.setEmpty().append(sb);
      }
      return true;
    } else {
      return false;
    }
  }
}
