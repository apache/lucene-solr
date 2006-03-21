package org.apache.lucene.analysis.br;

/**
 * Copyright 2004-2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;

import java.io.IOException;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;

/**
 * Based on GermanStemFilter
 *
 * @author Jo&atilde;o Kramer
 */
public final class BrazilianStemFilter extends TokenFilter {

  /**
   * The actual token in the input stream.
   */
  private Token token = null;
  private BrazilianStemmer stemmer = null;
  private Set exclusions = null;

  public BrazilianStemFilter(TokenStream in) {
    super(in);
    stemmer = new BrazilianStemmer();
  }

  public BrazilianStemFilter(TokenStream in, Set exclusiontable) {
    this(in);
    this.exclusions = exclusiontable;
  }

  /**
   * @return Returns the next token in the stream, or null at EOS.
   */
  public final Token next()
      throws IOException {
    if ((token = input.next()) == null) {
      return null;
    }
    // Check the exclusiontable.
    else if (exclusions != null && exclusions.contains(token.termText())) {
      return token;
    } else {
      String s = stemmer.stem(token.termText());
      // If not stemmed, dont waste the time creating a new token.
      if ((s != null) && !s.equals(token.termText())) {
        return new Token(s, token.startOffset(), token.endOffset(), token.type());
      }
      return token;
    }
  }
}


