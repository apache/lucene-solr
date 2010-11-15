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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Comparator;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.MultiTermQuery.RewriteMethod;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.BytesRefHash.DirectBytesStartArray;

/** @lucene.internal Only public to be accessible by spans package. */
public abstract class ScoringRewrite<Q extends Query> extends TermCollectingRewrite<Q> {

  /** A rewrite method that first translates each term into
   *  {@link BooleanClause.Occur#SHOULD} clause in a
   *  BooleanQuery, and keeps the scores as computed by the
   *  query.  Note that typically such scores are
   *  meaningless to the user, and require non-trivial CPU
   *  to compute, so it's almost always better to use {@link
   *  MultiTermQuery#CONSTANT_SCORE_AUTO_REWRITE_DEFAULT} instead.
   *
   *  <p><b>NOTE</b>: This rewrite method will hit {@link
   *  BooleanQuery.TooManyClauses} if the number of terms
   *  exceeds {@link BooleanQuery#getMaxClauseCount}.
   *
   *  @see #setRewriteMethod */
  public final static ScoringRewrite<BooleanQuery> SCORING_BOOLEAN_QUERY_REWRITE = new ScoringRewrite<BooleanQuery>() {
    @Override
    protected BooleanQuery getTopLevelQuery() {
      return new BooleanQuery(true);
    }
    
    @Override
    protected void addClause(BooleanQuery topLevel, Term term, int docCount, float boost) {
      final TermQuery tq = new TermQuery(term, docCount);
      tq.setBoost(boost);
      topLevel.add(tq, BooleanClause.Occur.SHOULD);
    }
    
    @Override
    protected void checkMaxClauseCount(int count) {
      if (count > BooleanQuery.getMaxClauseCount())
        throw new BooleanQuery.TooManyClauses();
    }
    
    // Make sure we are still a singleton even after deserializing
    protected Object readResolve() {
      return SCORING_BOOLEAN_QUERY_REWRITE;
    }    
  };
  
  /** Like {@link #SCORING_BOOLEAN_QUERY_REWRITE} except
   *  scores are not computed.  Instead, each matching
   *  document receives a constant score equal to the
   *  query's boost.
   * 
   *  <p><b>NOTE</b>: This rewrite method will hit {@link
   *  BooleanQuery.TooManyClauses} if the number of terms
   *  exceeds {@link BooleanQuery#getMaxClauseCount}.
   *
   *  @see #setRewriteMethod */
  public final static RewriteMethod CONSTANT_SCORE_BOOLEAN_QUERY_REWRITE = new RewriteMethod() {
    @Override
    public Query rewrite(IndexReader reader, MultiTermQuery query) throws IOException {
      final BooleanQuery bq = SCORING_BOOLEAN_QUERY_REWRITE.rewrite(reader, query);
      // TODO: if empty boolean query return NullQuery?
      if (bq.clauses().isEmpty())
        return bq;
      // strip the scores off
      final Query result = new ConstantScoreQuery(new QueryWrapperFilter(bq));
      result.setBoost(query.getBoost());
      return result;
    }

    // Make sure we are still a singleton even after deserializing
    protected Object readResolve() {
      return CONSTANT_SCORE_BOOLEAN_QUERY_REWRITE;
    }
  };

  /** This method is called after every new term to check if the number of max clauses
   * (e.g. in BooleanQuery) is not exceeded. Throws the corresponding {@link RuntimeException}. */
  protected abstract void checkMaxClauseCount(int count) throws IOException;
  
  @Override
  public final Q rewrite(final IndexReader reader, final MultiTermQuery query) throws IOException {
    final Q result = getTopLevelQuery();
    final ParallelArraysTermCollector col = new ParallelArraysTermCollector();
    collectTerms(reader, query, col);
    
    final Term placeholderTerm = new Term(query.field);
    final int size = col.terms.size();
    if (size > 0) {
      final int sort[] = col.terms.sort(col.termsEnum.getComparator());
      final int[] docFreq = col.array.docFreq;
      final float[] boost = col.array.boost;
      for (int i = 0; i < size; i++) {
        final int pos = sort[i];
        final Term term = placeholderTerm.createTerm(col.terms.get(pos, new BytesRef()));
        assert reader.docFreq(term) == docFreq[pos];
        addClause(result, term, docFreq[pos], query.getBoost() * boost[pos]);
      }
    }
    query.incTotalNumberOfTerms(size);
    return result;
  }

  final class ParallelArraysTermCollector extends TermCollector {
    final TermFreqBoostByteStart array = new TermFreqBoostByteStart(16);
    final BytesRefHash terms = new BytesRefHash(new ByteBlockPool(new ByteBlockPool.DirectAllocator()), 16, array);
    TermsEnum termsEnum;

    private BoostAttribute boostAtt;
    
    @Override
    public void setNextEnum(TermsEnum termsEnum) throws IOException {
      this.termsEnum = termsEnum;
      this.boostAtt = termsEnum.attributes().addAttribute(BoostAttribute.class);
    }
  
    @Override
    public boolean collect(BytesRef bytes) throws IOException {
      final int e = terms.add(bytes);
      if (e < 0 ) {
        // duplicate term: update docFreq
        final int pos = (-e)-1;
        array.docFreq[pos] += termsEnum.docFreq();
        assert array.boost[pos] == boostAtt.getBoost() : "boost should be equal in all segment TermsEnums";
      } else {
        // new entry: we populate the entry initially
        array.docFreq[e] = termsEnum.docFreq();
        array.boost[e] = boostAtt.getBoost();
        ScoringRewrite.this.checkMaxClauseCount(terms.size());
      }
      return true;
    }
  }
  
  /** Special implementation of BytesStartArray that keeps parallel arrays for boost and docFreq */
  static final class TermFreqBoostByteStart extends DirectBytesStartArray  {
    int[] docFreq;
    float[] boost;
    
    public TermFreqBoostByteStart(int initSize) {
      super(initSize);
    }

    @Override
    public int[] init() {
      final int[] ord = super.init();
      boost = new float[ArrayUtil.oversize(ord.length, RamUsageEstimator.NUM_BYTES_FLOAT)];
      docFreq = new int[ArrayUtil.oversize(ord.length, RamUsageEstimator.NUM_BYTES_INT)];
      assert boost.length >= ord.length && docFreq.length >= ord.length;
      return ord;
    }

    @Override
    public int[] grow() {
      final int[] ord = super.grow();
      docFreq = ArrayUtil.grow(docFreq, ord.length);
      boost = ArrayUtil.grow(boost, ord.length);
      assert boost.length >= ord.length && docFreq.length >= ord.length;
      return ord;
    }

    @Override
    public int[] clear() {
     boost = null;
     docFreq = null;
     return super.clear();
    }
    
  }
}
