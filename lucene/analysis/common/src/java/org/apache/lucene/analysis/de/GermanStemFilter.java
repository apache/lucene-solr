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
package org.apache.lucene.analysis.de;

import java.io.IOException;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.SetKeywordMarkerFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.KeywordAttribute;

/**
 * A {@link TokenFilter} that stems German words.
 *
 * <p>It supports a table of words that should not be stemmed at all. The stemmer used can be
 * changed at runtime after the filter object is created (as long as it is a {@link GermanStemmer}).
 *
 * <p>To prevent terms from being stemmed use an instance of {@link SetKeywordMarkerFilter} or a
 * custom {@link TokenFilter} that sets the {@link KeywordAttribute} before this {@link
 * TokenStream}.
 *
 * @see SetKeywordMarkerFilter
 */
public final class GermanStemFilter extends TokenFilter {
  /** The actual token in the input stream. */
  private GermanStemmer stemmer = new GermanStemmer();

  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final KeywordAttribute keywordAttr = addAttribute(KeywordAttribute.class);

  /**
   * Creates a {@link GermanStemFilter} instance
   *
   * @param in the source {@link TokenStream}
   */
  public GermanStemFilter(TokenStream in) {
    super(in);
  }

  /** @return Returns true for next token in the stream, or false at EOS */
  @Override
  public boolean incrementToken() throws IOException {
    if (input.incrementToken()) {
      String term = termAtt.toString();

      if (!keywordAttr.isKeyword()) {
        String s = stemmer.stem(term);
        // If not stemmed, don't waste the time adjusting the token.
        if ((s != null) && !s.equals(term)) termAtt.setEmpty().append(s);
      }
      return true;
    } else {
      return false;
    }
  }

  /** Set a alternative/custom {@link GermanStemmer} for this filter. */
  public void setStemmer(GermanStemmer stemmer) {
    if (stemmer != null) {
      this.stemmer = stemmer;
    }
  }
}
