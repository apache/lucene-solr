package org.apache.lucene.search;

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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.ToStringUtils;

import java.io.IOException;

/** Implements the fuzzy search query. The similarity measurement
 * is based on the Levenshtein (edit distance) algorithm.
 * 
 * Warning: this query is not very scalable with its default prefix
 * length of 0 - in this case, *every* term will be enumerated and
 * cause an edit score calculation.
 * 
 */
public class FuzzyQuery extends MultiTermQuery {
  
  public final static float defaultMinSimilarity = 0.5f;
  public final static int defaultPrefixLength = 0;
  
  private float minimumSimilarity;
  private int prefixLength;
  private boolean termLongEnough = false;
  
  protected Term term;
  
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
   *  between both terms is less than <code>length(term)*0.5</code>
   * @param prefixLength length of common (non-fuzzy) prefix
   * @throws IllegalArgumentException if minimumSimilarity is &gt;= 1 or &lt; 0
   * or if prefixLength &lt; 0
   */
  public FuzzyQuery(Term term, float minimumSimilarity, int prefixLength) throws IllegalArgumentException {
    super(term); // will be removed in 3.0
    this.term = term;
    
    if (minimumSimilarity >= 1.0f)
      throw new IllegalArgumentException("minimumSimilarity >= 1");
    else if (minimumSimilarity < 0.0f)
      throw new IllegalArgumentException("minimumSimilarity < 0");
    if (prefixLength < 0)
      throw new IllegalArgumentException("prefixLength < 0");
    
    if (term.text().length() > 1.0f / (1.0f - minimumSimilarity)) {
      this.termLongEnough = true;
    }
    
    this.minimumSimilarity = minimumSimilarity;
    this.prefixLength = prefixLength;
    rewriteMethod = SCORING_BOOLEAN_QUERY_REWRITE;
  }
  
  /**
   * Calls {@link #FuzzyQuery(Term, float) FuzzyQuery(term, minimumSimilarity, 0)}.
   */
  public FuzzyQuery(Term term, float minimumSimilarity) throws IllegalArgumentException {
      this(term, minimumSimilarity, defaultPrefixLength);
  }

  /**
   * Calls {@link #FuzzyQuery(Term, float) FuzzyQuery(term, 0.5f, 0)}.
   */
  public FuzzyQuery(Term term) {
    this(term, defaultMinSimilarity, defaultPrefixLength);
  }
  
  /**
   * Returns the minimum similarity that is required for this query to match.
   * @return float value between 0.0 and 1.0
   */
  public float getMinSimilarity() {
    return minimumSimilarity;
  }
    
  /**
   * Returns the non-fuzzy prefix length. This is the number of characters at the start
   * of a term that must be identical (not fuzzy) to the query term if the query
   * is to match that term. 
   */
  public int getPrefixLength() {
    return prefixLength;
  }

  protected FilteredTermEnum getEnum(IndexReader reader) throws IOException {
    return new FuzzyTermEnum(reader, getTerm(), minimumSimilarity, prefixLength);
  }
  
  /**
   * Returns the pattern term.
   */
  public Term getTerm() {
    return term;
  }

  public void setRewriteMethod(RewriteMethod method) {
    throw new UnsupportedOperationException("FuzzyQuery cannot change rewrite method");
  }
  
  public Query rewrite(IndexReader reader) throws IOException {
    if(!termLongEnough) {  // can only match if it's exact
      return new TermQuery(term);
    }

    FilteredTermEnum enumerator = getEnum(reader);
    int maxClauseCount = BooleanQuery.getMaxClauseCount();
    ScoreTermQueue stQueue = new ScoreTermQueue(maxClauseCount);
    ScoreTerm reusableST = null;

    try {
      do {
        float score = 0.0f;
        Term t = enumerator.term();
        if (t != null) {
          score = enumerator.difference();
          if (reusableST == null) {
            reusableST = new ScoreTerm(t, score);
          } else if (score >= reusableST.score) {
            // reusableST holds the last "rejected" entry, so, if
            // this new score is not better than that, there's no
            // need to try inserting it
            reusableST.score = score;
            reusableST.term = t;
          } else {
            continue;
          }

          reusableST = (ScoreTerm) stQueue.insertWithOverflow(reusableST);
        }
      } while (enumerator.next());
    } finally {
      enumerator.close();
    }
    
    BooleanQuery query = new BooleanQuery(true);
    int size = stQueue.size();
    for(int i = 0; i < size; i++){
      ScoreTerm st = (ScoreTerm) stQueue.pop();
      TermQuery tq = new TermQuery(st.term);      // found a match
      tq.setBoost(getBoost() * st.score); // set the boost
      query.add(tq, BooleanClause.Occur.SHOULD);          // add to query
    }

    return query;
  }
    
  public String toString(String field) {
    StringBuffer buffer = new StringBuffer();
    if (!term.field().equals(field)) {
        buffer.append(term.field());
        buffer.append(":");
    }
    buffer.append(term.text());
    buffer.append('~');
    buffer.append(Float.toString(minimumSimilarity));
    buffer.append(ToStringUtils.boost(getBoost()));
    return buffer.toString();
  }
  
  protected static class ScoreTerm {
    public Term term;
    public float score;
    
    public ScoreTerm(Term term, float score){
      this.term = term;
      this.score = score;
    }
  }
  
  protected static class ScoreTermQueue extends PriorityQueue {
    
    public ScoreTermQueue(int size){
      initialize(size);
    }
    
    /* (non-Javadoc)
     * @see org.apache.lucene.util.PriorityQueue#lessThan(java.lang.Object, java.lang.Object)
     */
    protected boolean lessThan(Object a, Object b) {
      ScoreTerm termA = (ScoreTerm)a;
      ScoreTerm termB = (ScoreTerm)b;
      if (termA.score == termB.score)
        return termA.term.compareTo(termB.term) > 0;
      else
        return termA.score < termB.score;
    }
    
  }

  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + Float.floatToIntBits(minimumSimilarity);
    result = prime * result + prefixLength;
    result = prime * result + ((term == null) ? 0 : term.hashCode());
    return result;
  }

  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (!super.equals(obj))
      return false;
    if (getClass() != obj.getClass())
      return false;
    FuzzyQuery other = (FuzzyQuery) obj;
    if (Float.floatToIntBits(minimumSimilarity) != Float
        .floatToIntBits(other.minimumSimilarity))
      return false;
    if (prefixLength != other.prefixLength)
      return false;
    if (term == null) {
      if (other.term != null)
        return false;
    } else if (!term.equals(other.term))
      return false;
    return true;
  }


}
