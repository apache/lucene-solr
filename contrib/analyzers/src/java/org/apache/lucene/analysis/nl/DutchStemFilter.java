package org.apache.lucene.analysis.nl;

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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Map;

/**
 * A filter that stems Dutch words. It supports a table of words that should
 * not be stemmed at all. The stemmer used can be changed at runtime after the
 * filter object is created (as long as it is a DutchStemmer).
 *
 */
public final class DutchStemFilter extends TokenFilter {
  /**
   * The actual token in the input stream.
   */
  private DutchStemmer stemmer = null;
  private Set exclusions = null;

  public DutchStemFilter(TokenStream _in) {
    super(_in);
    stemmer = new DutchStemmer();
  }

  /**
   * Builds a DutchStemFilter that uses an exclusion table.
   */
  public DutchStemFilter(TokenStream _in, Set exclusiontable) {
    this(_in);
    exclusions = exclusiontable;
  }

  /**
   * @param stemdictionary Dictionary of word stem pairs, that overrule the algorithm
   */
  public DutchStemFilter(TokenStream _in, Set exclusiontable, Map stemdictionary) {
    this(_in, exclusiontable);
    stemmer.setStemDictionary(stemdictionary);
  }

  /**
   * @return Returns the next token in the stream, or null at EOS
   */
  public Token next(Token reusableToken) throws IOException {
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

  /**
   * Set a alternative/custom DutchStemmer for this filter.
   */
  public void setStemmer(DutchStemmer stemmer) {
    if (stemmer != null) {
      this.stemmer = stemmer;
    }
  }

  /**
   * Set an alternative exclusion list for this filter.
   */
  public void setExclusionTable(HashSet exclusiontable) {
    exclusions = exclusiontable;
  }

  /**
   * Set dictionary for stemming, this dictionary overrules the algorithm,
   * so you can correct for a particular unwanted word-stem pair.
   */
  public void setStemDictionary(HashMap dict) {
    if (stemmer != null)
      stemmer.setStemDictionary(dict);
  }
}