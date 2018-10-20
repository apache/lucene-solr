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
package org.apache.lucene.analysis.ja;


import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.KeywordAttribute;

import java.io.IOException;

/**
 * A {@link TokenFilter} that normalizes common katakana spelling variations
 * ending in a long sound character by removing this character (U+30FC).  Only
 * katakana words longer than a minimum length are stemmed (default is four).
 * <p>
 * Note that only full-width katakana characters are supported.  Please use a
 * {@link org.apache.lucene.analysis.cjk.CJKWidthFilter} to convert half-width
 * katakana to full-width before using this filter.
 * </p>
 * <p>
 * In order to prevent terms from being stemmed, use an instance of
 * {@link org.apache.lucene.analysis.miscellaneous.SetKeywordMarkerFilter}
 * or a custom {@link TokenFilter} that sets the {@link KeywordAttribute}
 * before this {@link TokenStream}.
 * </p>
 */

public final class JapaneseKatakanaStemFilter extends TokenFilter {
  public final static int DEFAULT_MINIMUM_LENGTH = 4;
  private final static char HIRAGANA_KATAKANA_PROLONGED_SOUND_MARK = '\u30fc';

  private final CharTermAttribute termAttr = addAttribute(CharTermAttribute.class);
  private final KeywordAttribute keywordAttr = addAttribute(KeywordAttribute.class);
  private final int minimumKatakanaLength;

  public JapaneseKatakanaStemFilter(TokenStream input, int minimumLength) {
    super(input);
    this.minimumKatakanaLength = minimumLength;
  }

  public JapaneseKatakanaStemFilter(TokenStream input) {
    this(input, DEFAULT_MINIMUM_LENGTH);
  }

  @Override
  public boolean incrementToken() throws IOException {
    if (input.incrementToken()) {
      if (!keywordAttr.isKeyword()) {
        termAttr.setLength(stem(termAttr.buffer(), termAttr.length()));
      }
      return true;
    } else {
      return false;
    }
  }

  private int stem(char[] term, int length) {
    if (length < minimumKatakanaLength) {
      return length;
    }

    if (! isKatakana(term, length)) {
      return length;
    }

    if (term[length - 1] == HIRAGANA_KATAKANA_PROLONGED_SOUND_MARK) {
      return length - 1;
    }

    return length;
  }

  private boolean isKatakana(char[] term, int length) {
    for (int i = 0; i < length; i++) {
      // NOTE: Test only identifies full-width characters -- half-widths are supported
      if (Character.UnicodeBlock.of(term[i]) != Character.UnicodeBlock.KATAKANA) {
        return false;
      }
    }
    return true;
  }
}
