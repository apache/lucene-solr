package org.apache.lucene.analysis.br;

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

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Based on GermanStemFilter
 *
 */
public final class BrazilianStemFilter extends TokenFilter {

  /**
   * The actual token in the input stream.
   */
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
  public final Token next(final Token reusableToken)
      throws IOException {
    assert reusableToken != null;
    Token nextToken = input.next(reusableToken);
    if (nextToken == null)
      return null;

    String term = nextToken.term();

    // Check the exclusion table.
    if (exclusions == null || !exclusions.contains(term)) {
      String s = stemmer.stem(term);
      // If not stemmed, don't waste the time adjusting the token.
      if ((s != null) && !s.equals(term))
        nextToken.setTermBuffer(s);
    }
    return nextToken;
  }
}


