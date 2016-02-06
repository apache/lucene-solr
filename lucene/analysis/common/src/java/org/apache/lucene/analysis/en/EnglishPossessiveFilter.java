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
package org.apache.lucene.analysis.en;


import java.io.IOException;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

/**
 * TokenFilter that removes possessives (trailing 's) from words.
 */
public final class EnglishPossessiveFilter extends TokenFilter {
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

  public EnglishPossessiveFilter(TokenStream input) {
    super(input);
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
         buffer[bufferLength-2] == '\u2019' || 
         buffer[bufferLength-2] == '\uFF07') &&
        (buffer[bufferLength-1] == 's' || buffer[bufferLength-1] == 'S')) {
      termAtt.setLength(bufferLength - 2); // Strip last 2 characters off
    }

    return true;
  }
}
