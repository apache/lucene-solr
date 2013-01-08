package org.apache.lucene.search;

/*
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
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Comparator;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;

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
  public final Q rewrite(final IndexReader reader, final MultiTermQuery query) throws IOException {
    final int maxSize = Math.min(size, getMaxSize());
    final PriorityQueue<ScoreTerm> stQueue = new PriorityQueue<ScoreTerm>();
    collectTerms(reader, query, new TermCollector() {
      private final MaxNonCompetitiveBoostAttribute maxBoostAtt =
        attributes.addAttribute(MaxNonCompetitiveBoostAttribute.class);
      
      private final Map<BytesRef,ScoreTerm> visitedTerms = new HashMap<BytesRef,ScoreTerm>();
      
      private TermsEnum termsEnum;
      private Comparator<BytesRef> termComp;
      private BoostAttribute boostAtt;        
      private ScoreTerm st;
      
      @Override
      public void setNextEnum(TermsEnum termsEnum) {
        this.termsEnum = termsEnum;
        this.termComp = termsEnum.getComparator();
        
        assert compareToLastTerm(null);

        // lazy init the initial ScoreTerm because comparator is not known on ctor:
        if (st == null)
          st = new ScoreTerm(this.termComp, new TermContext(topReaderContext));
        boostAtt = termsEnum.attributes().addAttribute(BoostAttribute.class);
      }
    
      // for assert:
      private BytesRef lastTerm;
      private boolean compareToLastTerm(BytesRef t) {
        if (lastTerm == null && t != null) {
          lastTerm = BytesRef.deepCopyOf(t);
        } else if (t == null) {
          lastTerm = null;
        } else {
          assert termsEnum.getComparator().compare(lastTerm, t) < 0: "lastTerm=" + lastTerm + " t=" + t;
          lastTerm.copyBytes(t);
        }
        return true;
      }
  
      @Override
      public boolean collect(BytesRef bytes) throws IOException {
        final float boost = boostAtt.getBoost();

        // make sure within a single seg we always collect
        // terms in order
        assert compareToLastTerm(bytes);

        //System.out.println("TTR.collect term=" + bytes.utf8ToString() + " boost=" + boost + " ord=" + readerContext.ord);
        // ignore uncompetitive hits
        if (stQueue.size() == maxSize) {
          final ScoreTerm t = stQueue.peek();
          if (boost < t.boost)
            return true;
          if (boost == t.boost && termComp.compare(bytes, t.bytes) > 0)
            return true;
        }
        ScoreTerm t = visitedTerms.get(bytes);
        final TermState state = termsEnum.termState();
        assert state != null;
        if (t != null) {
          // if the term is already in the PQ, only update docFreq of term in PQ
          assert t.boost == boost : "boost should be equal in all segment TermsEnums";
          t.termState.register(state, readerContext.ord, termsEnum.docFreq(), termsEnum.totalTermFreq());
        } else {
          // add new entry in PQ, we must clone the term, else it may get overwritten!
          st.bytes.copyBytes(bytes);
          st.boost = boost;
          visitedTerms.put(st.bytes, st);
          assert st.termState.docFreq() == 0;
          st.termState.register(state, readerContext.ord, termsEnum.docFreq(), termsEnum.totalTermFreq());
          stQueue.offer(st);
          // possibly drop entries from queue
          if (stQueue.size() > maxSize) {
            st = stQueue.poll();
            visitedTerms.remove(st.bytes);
            st.termState.clear(); // reset the termstate! 
          } else {
            st = new ScoreTerm(termComp, new TermContext(topReaderContext));
          }
          assert stQueue.size() <= maxSize : "the PQ size must be limited to maxSize";
          // set maxBoostAtt with values to help FuzzyTermsEnum to optimize
          if (stQueue.size() == maxSize) {
            t = stQueue.peek();
            maxBoostAtt.setMaxNonCompetitiveBoost(t.boost);
            maxBoostAtt.setCompetitiveTerm(t.bytes);
          }
        }
       
        return true;
      }
    });
    
    final Q q = getTopLevelQuery();
    final ScoreTerm[] scoreTerms = stQueue.toArray(new ScoreTerm[stQueue.size()]);
    ArrayUtil.mergeSort(scoreTerms, scoreTermSortByTermComp);
    
    for (final ScoreTerm st : scoreTerms) {
      final Term term = new Term(query.field, st.bytes);
      assert reader.docFreq(term) == st.termState.docFreq() : "reader DF is " + reader.docFreq(term) + " vs " + st.termState.docFreq() + " term=" + term;
      addClause(q, term, st.termState.docFreq(), query.getBoost() * st.boost, st.termState); // add to query
    }
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
    final TopTermsRewrite<?> other = (TopTermsRewrite<?>) obj;
    if (size != other.size) return false;
    return true;
  }
  
  private static final Comparator<ScoreTerm> scoreTermSortByTermComp = 
    new Comparator<ScoreTerm>() {
      @Override
      public int compare(ScoreTerm st1, ScoreTerm st2) {
        assert st1.termComp == st2.termComp :
          "term comparator should not change between segments";
        return st1.termComp.compare(st1.bytes, st2.bytes);
      }
    };

  static final class ScoreTerm implements Comparable<ScoreTerm> {
    public final Comparator<BytesRef> termComp;
    public final BytesRef bytes = new BytesRef();
    public float boost;
    public final TermContext termState;
    public ScoreTerm(Comparator<BytesRef> termComp, TermContext termState) {
      this.termComp = termComp;
      this.termState = termState;
    }
    
    @Override
    public int compareTo(ScoreTerm other) {
      if (this.boost == other.boost)
        return termComp.compare(other.bytes, this.bytes);
      else
        return Float.compare(this.boost, other.boost);
    }
  }
}
