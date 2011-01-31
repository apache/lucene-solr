package org.apache.lucene.analysis;

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
import java.util.Set;

import org.apache.lucene.analysis.tokenattributes.KeywordAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;

/**
 * Marks terms as keywords via the {@link KeywordAttribute}. Each token
 * contained in the provided is marked as a keyword by setting
 * {@link KeywordAttribute#setKeyword(boolean)} to <code>true</code>.
 * 
 * @see KeywordAttribute
 */
public final class KeywordMarkerFilter extends TokenFilter {

  private final KeywordAttribute keywordAttr = addAttribute(KeywordAttribute.class);
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final CharArraySet keywordSet;

  /**
   * Create a new KeywordMarkerFilter, that marks the current token as a
   * keyword if the tokens term buffer is contained in the given set via the
   * {@link KeywordAttribute}.
   * 
   * @param in
   *          TokenStream to filter
   * @param keywordSet
   *          the keywords set to lookup the current termbuffer
   */
  public KeywordMarkerFilter(final TokenStream in,
      final CharArraySet keywordSet) {
    super(in);
    this.keywordSet = keywordSet;
  }

  /**
   * Create a new KeywordMarkerFilter, that marks the current token as a
   * keyword if the tokens term buffer is contained in the given set via the
   * {@link KeywordAttribute}.
   * 
   * @param in
   *          TokenStream to filter
   * @param keywordSet
   *          the keywords set to lookup the current termbuffer
   */
  public KeywordMarkerFilter(final TokenStream in, final Set<?> keywordSet) {
    this(in, keywordSet instanceof CharArraySet ? (CharArraySet) keywordSet
        : CharArraySet.copy(Version.LUCENE_31, keywordSet));
  }

  @Override
  public final boolean incrementToken() throws IOException {
    if (input.incrementToken()) {
      if (keywordSet.contains(termAtt.buffer(), 0, termAtt.length())) { 
        keywordAttr.setKeyword(true);
      }
      return true;
    } else {
      return false;
    }
  }
}
