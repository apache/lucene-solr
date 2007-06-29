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
 * 
 * @author Mathieu Lecarme<mlecarme@openwide.fr>
 * @see{http://fr.wikipedia.org/wiki/%C3%89lision}
 * 
 * Note that StandardTokenizer sees "’" as a space, and cuts it out.
 */
public class ElisionFilter extends TokenFilter {
  private Set articles = null;

  private static String apostrophes = "'’";

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
   * Returns the next input Token whith termText() without elisioned start
   */
  public Token next() throws IOException {
    Token t = input.next();
    if (t == null)
      return null;
    String text = t.termText();
    System.out.println(text);
    int minPoz = -1;
    int poz;
    for (int i = 0; i < apostrophes.length(); i++) {
      poz = text.indexOf(apostrophes.charAt(i));
      if (poz != -1)
        minPoz = (minPoz == -1) ? poz : Math.min(poz, minPoz);
    }
    if (minPoz != -1
        && articles.contains(text.substring(0, minPoz).toLowerCase()))
      text = text.substring(minPoz + 1);
    return new Token(text, t.startOffset(), t.endOffset(), t.type());
  }

}
