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
package org.apache.lucene.analysis.util;


import java.io.IOException;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

/**
 * Removes elisions from a {@link TokenStream}. For example, "l'avion" (the plane) will be
 * tokenized as "avion" (plane).
 * 
 * @see <a href="http://fr.wikipedia.org/wiki/%C3%89lision">Elision in Wikipedia</a>
 */
public final class ElisionFilter extends TokenFilter {
  private final CharArraySet articles;
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  
  /**
   * Constructs an elision filter with a Set of stop words
   * @param input the source {@link TokenStream}
   * @param articles a set of stopword articles
   */
  public ElisionFilter(TokenStream input, CharArraySet articles) {
    super(input);
    this.articles = articles;
  }

  /**
   * Increments the {@link TokenStream} with a {@link CharTermAttribute} without elisioned start
   */
  @Override
  public final boolean incrementToken() throws IOException {
    if (input.incrementToken()) {
      char[] termBuffer = termAtt.buffer();
      int termLength = termAtt.length();

      int index = -1;
      for (int i = 0; i < termLength; i++) {
        char ch = termBuffer[i];
        if (ch == '\'' || ch == '\u2019') {
          index = i;
          break;
        }
      }

      // An apostrophe has been found. If the prefix is an article strip it off.
      if (index >= 0 && articles.contains(termBuffer, 0, index)) {
        termAtt.copyBuffer(termBuffer, index + 1, termLength - (index + 1));
      }

      return true;
    } else {
      return false;
    }
  }
}
