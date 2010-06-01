package org.apache.lucene.analysis.miscellaneous;

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
import java.util.Map;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.KeywordAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.CharArrayMap;
import org.apache.lucene.util.Version;

/**
 * Provides the ability to override any {@link KeywordAttribute} aware stemmer
 * with custom dictionary-based stemming.
 */
public final class StemmerOverrideFilter extends TokenFilter {
  private final CharArrayMap<String> dictionary;
  
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final KeywordAttribute keywordAtt = addAttribute(KeywordAttribute.class);
  
  /**
   * Create a new StemmerOverrideFilter, performing dictionary-based stemming
   * with the provided <code>dictionary</code>.
   * <p>
   * Any dictionary-stemmed terms will be marked with {@link KeywordAttribute}
   * so that they will not be stemmed with stemmers down the chain.
   * </p>
   */
  public StemmerOverrideFilter(Version matchVersion, TokenStream input,
      Map<?,String> dictionary) {
    super(input);
    this.dictionary = dictionary instanceof CharArrayMap ? 
        (CharArrayMap<String>) dictionary : CharArrayMap.copy(matchVersion, dictionary);
  }

  @Override
  public boolean incrementToken() throws IOException {
    if (input.incrementToken()) {
      if (!keywordAtt.isKeyword()) { // don't muck with already-keyworded terms
        String stem = dictionary.get(termAtt.buffer(), 0, termAtt.length());
        if (stem != null) {
          termAtt.setEmpty().append(stem);
          keywordAtt.setKeyword(true);
        }
      }
      return true;
    } else {
      return false;
    }
  }
}
