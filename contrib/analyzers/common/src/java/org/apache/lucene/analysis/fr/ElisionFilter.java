package org.apache.lucene.analysis.fr;

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
import java.util.Arrays;
import org.apache.lucene.analysis.standard.StandardTokenizer; // for javadocs
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;

/**
 * Removes elisions from a {@link TokenStream}. For example, "l'avion" (the plane) will be
 * tokenized as "avion" (plane).
 * <p>
 * Note that {@link StandardTokenizer} sees " ' " as a space, and cuts it out.
 * 
 * @see <a href="http://fr.wikipedia.org/wiki/%C3%89lision">Elision in Wikipedia</a>
 */
public final class ElisionFilter extends TokenFilter {
  private CharArraySet articles = null;
  private TermAttribute termAtt;
  
  private static char[] apostrophes = {'\'', '’'};

  public void setArticles(Set<?> articles) {
    if (articles instanceof CharArraySet)
      this.articles = (CharArraySet) articles;
    else
      this.articles = new CharArraySet(articles, true);
  }

  /**
   * Constructs an elision filter with standard stop words
   */
  protected ElisionFilter(TokenStream input) {
    super(input);
    this.articles = new CharArraySet(Arrays.asList(
        "l", "m", "t", "qu", "n", "s", "j"), true);
    termAtt = addAttribute(TermAttribute.class);
  }

  /**
   * Constructs an elision filter with a Set of stop words
   */
  public ElisionFilter(TokenStream input, Set<?> articles) {
    super(input);
    setArticles(articles);
    termAtt = addAttribute(TermAttribute.class);
  }

  /**
   * Constructs an elision filter with an array of stop words
   */
  public ElisionFilter(TokenStream input, String[] articles) {
    super(input);
    this.articles = new CharArraySet(Arrays.asList(articles), true);
    termAtt = addAttribute(TermAttribute.class);
  }

  /**
   * Increments the {@link TokenStream} with a {@link TermAttribute} without elisioned start
   */
  @Override
  public final boolean incrementToken() throws IOException {
    if (input.incrementToken()) {
      char[] termBuffer = termAtt.termBuffer();
      int termLength = termAtt.termLength();

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
          && articles.contains(termAtt.termBuffer(), 0, minPoz)) {
        termAtt.setTermBuffer(termAtt.termBuffer(), minPoz + 1, termAtt.termLength() - (minPoz + 1));
      }

      return true;
    } else {
      return false;
    }
  }
}
