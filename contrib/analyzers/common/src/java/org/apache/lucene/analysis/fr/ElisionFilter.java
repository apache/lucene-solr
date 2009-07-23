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
import java.util.HashSet;
import java.util.Arrays;
import java.util.Iterator;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.TokenFilter;

/**
 * Removes elisions from a token stream. For example, "l'avion" (the plane) will be
 * tokenized as "avion" (plane).
 * <p>
 * Note that StandardTokenizer sees " ' " as a space, and cuts it out.
 * 
 * @see <a href="http://fr.wikipedia.org/wiki/%C3%89lision">Elision in Wikipedia</a>
 */
public class ElisionFilter extends TokenFilter {
  private Set articles = null;

  private static char[] apostrophes = {'\'', '’'};

  public void setArticles(Set articles) {
    this.articles = new HashSet();
    Iterator iter = articles.iterator();
    while (iter.hasNext()) {
      this.articles.add(((String) iter.next()).toLowerCase());
    }
  }

  /**
   * Constructs an elision filter with standard stop words
   */
  protected ElisionFilter(TokenStream input) {
    super(input);
    this.articles = new HashSet(Arrays.asList(new String[] { "l", "m", "t",
        "qu", "n", "s", "j" }));
  }

  /**
   * Constructs an elision filter with a Set of stop words
   */
  public ElisionFilter(TokenStream input, Set articles) {
    super(input);
    setArticles(articles);
  }

  /**
   * Constructs an elision filter with an array of stop words
   */
  public ElisionFilter(TokenStream input, String[] articles) {
    super(input);
    setArticles(new HashSet(Arrays.asList(articles)));
  }

  /**
   * Returns the next input Token with term() without elisioned start
   */
  public Token next(final Token reusableToken) throws IOException {
    assert reusableToken != null;
    Token nextToken = input.next(reusableToken);
    if (nextToken == null)
      return null;

    char[] termBuffer = nextToken.termBuffer();
    int termLength = nextToken.termLength();

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
        && articles.contains(new String(nextToken.termBuffer(), 0, minPoz).toLowerCase())) {
      nextToken.setTermBuffer(nextToken.termBuffer(), minPoz + 1, nextToken.termLength() - (minPoz + 1));
    }

    return nextToken;
  }

}
