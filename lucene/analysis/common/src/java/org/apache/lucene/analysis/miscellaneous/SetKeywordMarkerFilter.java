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
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.KeywordAttribute;
import org.apache.lucene.analysis.util.CharArraySet;

/**
 * Marks terms as keywords via the {@link KeywordAttribute}. Each token
 * contained in the provided set is marked as a keyword by setting
 * {@link KeywordAttribute#setKeyword(boolean)} to <code>true</code>.
 */
public final class SetKeywordMarkerFilter extends KeywordMarkerFilter {
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final CharArraySet keywordSet;

  /**
   * Create a new KeywordSetMarkerFilter, that marks the current token as a
   * keyword if the tokens term buffer is contained in the given set via the
   * {@link KeywordAttribute}.
   * 
   * @param in
   *          TokenStream to filter
   * @param keywordSet
   *          the keywords set to lookup the current termbuffer
   */
  public SetKeywordMarkerFilter(final TokenStream in, final CharArraySet keywordSet) {
    super(in);
    this.keywordSet = keywordSet;
  }

  @Override
  protected boolean isKeyword() {
    return keywordSet.contains(termAtt.buffer(), 0, termAtt.length());
  }
  
}
