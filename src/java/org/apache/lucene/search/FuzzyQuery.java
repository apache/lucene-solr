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
  
  private float minimumSimilarity;
  
  /**
   * Create a new FuzzyQuery that will match terms with a similarity 
   * of at least <code>minimumSimilarity</code> to <code>term</code>.
   * 
   * @param term the term to search for
   * @param minimumSimilarity a value between 0 and 1 to set the required similarity
   *  between the query term and the matching terms. For example, for a
   *  <code>minimumSimilarity</code> of <code>0.5</code> a term of the same length
   *  as the query term is considered similar to the query term if the edit distance
   *  between both terms is less than <code>length(term)*0.5</code>.
   * @throws IllegalArgumentException if minimumSimilarity is &gt; 1 or &lt; 0
   */
  public FuzzyQuery(Term term, float minimumSimilarity) throws IllegalArgumentException {
    super(term);
    if (minimumSimilarity > 1.0f)
      throw new IllegalArgumentException("minimumSimilarity > 1");
    else if (minimumSimilarity < 0.0f)
      throw new IllegalArgumentException("minimumSimilarity < 0");
    this.minimumSimilarity = minimumSimilarity;
  }

  /**
   * Calls {@link #FuzzyQuery(Term, float) FuzzyQuery(term, 0.5f)}.
   */
  public FuzzyQuery(Term term) {
    this(term, 0.5f);
  }
    
  protected FilteredTermEnum getEnum(IndexReader reader) throws IOException {
    return new FuzzyTermEnum(reader, getTerm(), minimumSimilarity);
  }
    
  public String toString(String field) {
    return super.toString(field) + '~';
  }
}
