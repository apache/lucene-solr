package org.apache.lucene.search;

/**
 * Copyright 2004 The Apache Software Foundation
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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import java.io.IOException;

/** Implements the fuzzy search query. The similiarity measurement
 * is based on the Levenshtein (edit distance) algorithm.
 */
public final class FuzzyQuery extends MultiTermQuery {
  
  public final static float defaultMinSimilarity = 0.5f;
  private float minimumSimilarity;
  private int prefixLength;
  
  /**
   * Create a new FuzzyQuery that will match terms with a similarity 
   * of at least <code>minimumSimilarity</code> to <code>term</code>.
   * If a <code>prefixLength</code> &gt; 0 is specified, a common prefix
   * of that length is also required.
   * 
   * @param term the term to search for
   * @param minimumSimilarity a value between 0 and 1 to set the required similarity
   *  between the query term and the matching terms. For example, for a
   *  <code>minimumSimilarity</code> of <code>0.5</code> a term of the same length
   *  as the query term is considered similar to the query term if the edit distance
   *  between both terms is less than <code>length(term)*0.5</code>.
   * @param prefixLength length of common prefix.
   * @throws IllegalArgumentException if minimumSimilarity is &gt; 1 or &lt; 0
   * or if prefixLength &lt; 0 or &gt; <code>term.text().length()</code>.
   */
  public FuzzyQuery(Term term, float minimumSimilarity, int prefixLength) throws IllegalArgumentException {
    super(term);
    
    if (minimumSimilarity > 1.0f)
      throw new IllegalArgumentException("minimumSimilarity > 1");
    else if (minimumSimilarity < 0.0f)
      throw new IllegalArgumentException("minimumSimilarity < 0");
    this.minimumSimilarity = minimumSimilarity;
    
    if(prefixLength < 0)
        throw new IllegalArgumentException("prefixLength < 0");
    else if(prefixLength >= term.text().length())
        throw new IllegalArgumentException("prefixLength >= term.text().length()");
    this.prefixLength = prefixLength;
  }
  
  /**
   * Calls {@link #FuzzyQuery(Term, float) FuzzyQuery(term, minimumSimilarity, 0)}.
   */
  public FuzzyQuery(Term term, float minimumSimilarity) throws IllegalArgumentException {
      this(term, minimumSimilarity, 0);
  }

  /**
   * Calls {@link #FuzzyQuery(Term, float) FuzzyQuery(term, 0.5f, 0)}.
   */
  public FuzzyQuery(Term term) {
    this(term, defaultMinSimilarity, 0);
  }
    
  protected FilteredTermEnum getEnum(IndexReader reader) throws IOException {
    return new FuzzyTermEnum(reader, getTerm(), minimumSimilarity, prefixLength);
  }
    
  public String toString(String field) {
    return super.toString(field) + '~' + Float.toString(minimumSimilarity);
  }
}
