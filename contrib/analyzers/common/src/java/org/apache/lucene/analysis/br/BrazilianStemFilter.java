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

import java.io.IOException;
import java.util.Set;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;

/**
 * A {@link TokenFilter} that applies {@link BrazilianStemmer}.
 *
 */
public final class BrazilianStemFilter extends TokenFilter {

  /**
   * {@link BrazilianStemmer} in use by this filter.
   */
  private BrazilianStemmer stemmer = null;
  private Set exclusions = null;
  private TermAttribute termAtt;
  
  public BrazilianStemFilter(TokenStream in) {
    super(in);
    stemmer = new BrazilianStemmer();
    termAtt = addAttribute(TermAttribute.class);
  }

  public BrazilianStemFilter(TokenStream in, Set exclusiontable) {
    this(in);
    this.exclusions = exclusiontable;
  }

  @Override
  public boolean incrementToken() throws IOException {
    if (input.incrementToken()) {
      String term = termAtt.term();
      // Check the exclusion table.
      if (exclusions == null || !exclusions.contains(term)) {
        String s = stemmer.stem(term);
        // If not stemmed, don't waste the time adjusting the token.
        if ((s != null) && !s.equals(term))
          termAtt.setTermBuffer(s);
      }
      return true;
    } else {
      return false;
    }
  }
}


