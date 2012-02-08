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

import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.CharacterUtils;
import org.apache.lucene.util.Version;

/**
 * Normalizes token text to lower case.
 * <a name="version"/>
 * <p>You must specify the required {@link Version}
 * compatibility when creating LowerCaseFilter:
 * <ul>
 *   <li> As of 3.1, supplementary characters are properly lowercased.
 * </ul>
 */
public final class LowerCaseFilter extends TokenFilter {
  private final CharacterUtils charUtils;
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  
  /**
   * Create a new LowerCaseFilter, that normalizes token text to lower case.
   * 
   * @param matchVersion See <a href="#version">above</a>
   * @param in TokenStream to filter
   */
  public LowerCaseFilter(Version matchVersion, TokenStream in) {
    super(in);
    charUtils = CharacterUtils.getInstance(matchVersion);
  }
  
  /**
   * @deprecated Use {@link #LowerCaseFilter(Version, TokenStream)} instead.
   */
  @Deprecated
  public LowerCaseFilter(TokenStream in) {
    this(Version.LUCENE_30, in);
  }

  @Override
  public final boolean incrementToken() throws IOException {
    if (input.incrementToken()) {
      final char[] buffer = termAtt.buffer();
      final int length = termAtt.length();
      for (int i = 0; i < length;) {
       i += Character.toChars(
               Character.toLowerCase(
                   charUtils.codePointAt(buffer, i)), buffer, i);
      }
      return true;
    } else
      return false;
  }
}
