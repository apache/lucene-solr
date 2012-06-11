package org.apache.lucene.analysis.fr;

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
import java.util.Arrays;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.util.Version;

/**
 * Removes elisions from a {@link TokenStream}. For example, "l'avion" (the plane) will be
 * tokenized as "avion" (plane).
 * 
 * @see <a href="http://fr.wikipedia.org/wiki/%C3%89lision">Elision in Wikipedia</a>
 */
public final class ElisionFilter extends TokenFilter {
  private CharArraySet articles = CharArraySet.EMPTY_SET;
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private static final CharArraySet DEFAULT_ARTICLES = CharArraySet.unmodifiableSet(
      new CharArraySet(Version.LUCENE_CURRENT, Arrays.asList(
          "l", "m", "t", "qu", "n", "s", "j"), true));
  
  private static char[] apostrophes = {'\'', '\u2019'};
  
  /**
   * Constructs an elision filter with standard stop words
   */
  public ElisionFilter(Version matchVersion, TokenStream input) {
    this(matchVersion, input, DEFAULT_ARTICLES);
  }

  /**
   * Constructs an elision filter with a Set of stop words
   * @param matchVersion the lucene backwards compatibility version
   * @param input the source {@link TokenStream}
   * @param articles a set of stopword articles
   */
  public ElisionFilter(Version matchVersion, TokenStream input, CharArraySet articles) {
    super(input);
    this.articles = CharArraySet.unmodifiableSet(
        new CharArraySet(matchVersion, articles, true));
  }

  /**
   * Increments the {@link TokenStream} with a {@link CharTermAttribute} without elisioned start
   */
  @Override
  public final boolean incrementToken() throws IOException {
    if (input.incrementToken()) {
      char[] termBuffer = termAtt.buffer();
      int termLength = termAtt.length();

      int minPoz = Integer.MAX_VALUE;
      for (int i = 0; i < apostrophes.length; i++) {
        char apos = apostrophes[i];
        // The equivalent of String.indexOf(ch)
        for (int poz = 0; poz < termLength ; poz++) {
          if (termBuffer[poz] == apos) {
            minPoz = Math.min(poz, minPoz);
            break;
          }
        }
      }

      // An apostrophe has been found. If the prefix is an article strip it off.
      if (minPoz != Integer.MAX_VALUE
          && articles.contains(termAtt.buffer(), 0, minPoz)) {
        termAtt.copyBuffer(termAtt.buffer(), minPoz + 1, termAtt.length() - (minPoz + 1));
      }

      return true;
    } else {
      return false;
    }
  }
}
