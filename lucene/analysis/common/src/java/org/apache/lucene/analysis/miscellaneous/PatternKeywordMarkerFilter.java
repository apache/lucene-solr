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

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.KeywordAttribute;

/**
 * Marks terms as keywords via the {@link KeywordAttribute}. Each token that matches the provided
 * pattern is marked as a keyword by setting {@link KeywordAttribute#setKeyword(boolean)} to <code>
 * true</code>.
 */
public final class PatternKeywordMarkerFilter extends KeywordMarkerFilter {
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final Matcher matcher;

  /**
   * Create a new {@link PatternKeywordMarkerFilter}, that marks the current token as a keyword if
   * the tokens term buffer matches the provided {@link Pattern} via the {@link KeywordAttribute}.
   *
   * @param in TokenStream to filter
   * @param pattern the pattern to apply to the incoming term buffer
   */
  public PatternKeywordMarkerFilter(TokenStream in, Pattern pattern) {
    super(in);
    this.matcher = pattern.matcher("");
  }

  @Override
  protected boolean isKeyword() {
    matcher.reset(termAtt);
    return matcher.matches();
  }
}
