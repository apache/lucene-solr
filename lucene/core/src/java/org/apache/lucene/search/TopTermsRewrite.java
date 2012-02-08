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

import java.io.IOException;
import java.util.PriorityQueue;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;

/**
 * Base rewrite method for collecting only the top terms
 * via a priority queue.
 * @lucene.internal Only public to be accessible by spans package.
 */
public abstract class TopTermsRewrite<Q extends Query> extends TermCollectingRewrite<Q> {

  private final int size;
  
  /** 
   * Create a TopTermsBooleanQueryRewrite for 
   * at most <code>size</code> terms.
   * <p>
   * NOTE: if {@link BooleanQuery#getMaxClauseCount} is smaller than 
   * <code>size</code>, then it will be used instead. 
   */
  public TopTermsRewrite(int size) {
    this.size = size;
  }
  
  /** return the maximum priority queue size */
  public int getSize() {
    return size;
  }
  
  /** return the maximum size of the priority queue (for boolean rewrites this is BooleanQuery#getMaxClauseCount). */
  protected abstract int getMaxSize();
  
  @Override
  public Q rewrite(final IndexReader reader, final MultiTermQuery query) throws IOException {
    final int maxSize = Math.min(size, getMaxSize());
    final PriorityQueue<ScoreTerm> stQueue = new PriorityQueue<ScoreTerm>();
    collectTerms(reader, query, new TermCollector() {
      public boolean collect(Term t, float boost) {
        // ignore uncompetitive hits
        if (stQueue.size() >= maxSize && boost <= stQueue.peek().boost)
          return true;
        // add new entry in PQ
        st.term = t;
        st.boost = boost;
        stQueue.offer(st);
        // possibly drop entries from queue
        st = (stQueue.size() > maxSize) ? stQueue.poll() : new ScoreTerm();
        return true;
      }
      
      // reusable instance
      private ScoreTerm st = new ScoreTerm();
    });
    
    final Q q = getTopLevelQuery();
    for (final ScoreTerm st : stQueue) {
      addClause(q, st.term, query.getBoost() * st.boost); // add to query
    }
    query.incTotalNumberOfTerms(stQueue.size());
    
    return q;
  }

  @Override
  public int hashCode() {
    return 31 * size;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    final TopTermsRewrite other = (TopTermsRewrite) obj;
    if (size != other.size) return false;
    return true;
  }
  
  private static class ScoreTerm implements Comparable<ScoreTerm> {
    public Term term;
    public float boost;
    
    public int compareTo(ScoreTerm other) {
      if (this.boost == other.boost)
        return other.term.compareTo(this.term);
      else
        return Float.compare(this.boost, other.boost);
    }
  }
  
}
