package org.apache.lucene.analysis.en;

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
import org.apache.lucene.util.Version;

/**
 * TokenFilter that removes possessives (trailing 's) from words.
 * <a name="version"/>
 * <p>You must specify the required {@link Version}
 * compatibility when creating EnglishPossessiveFilter:
 * <ul>
 *    <li> As of 3.6, U+2019 RIGHT SINGLE QUOTATION MARK and 
 *         U+FF07 FULLWIDTH APOSTROPHE are also treated as
 *         quotation marks.
 * </ul>
 */
public final class EnglishPossessiveFilter extends TokenFilter {
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private Version matchVersion;

  /**
   * @deprecated Use {@link #EnglishPossessiveFilter(Version, TokenStream)} instead.
   */
  @Deprecated
  public EnglishPossessiveFilter(TokenStream input) {
    this(Version.LUCENE_35, input);
  }

  public EnglishPossessiveFilter(Version version, TokenStream input) {
    super(input);
    this.matchVersion = version;
  }

  @Override
  public boolean incrementToken() throws IOException {
    if (!input.incrementToken()) {
      return false;
    }
    
    final char[] buffer = termAtt.buffer();
    final int bufferLength = termAtt.length();
    
    if (bufferLength >= 2 && 
        (buffer[bufferLength-2] == '\'' || 
         (matchVersion.onOrAfter(Version.LUCENE_36) && (buffer[bufferLength-2] == '\u2019' || buffer[bufferLength-2] == '\uFF07'))) &&
        (buffer[bufferLength-1] == 's' || buffer[bufferLength-1] == 'S')) {
      termAtt.setLength(bufferLength - 2); // Strip last 2 characters off
    }

    return true;
  }
}
