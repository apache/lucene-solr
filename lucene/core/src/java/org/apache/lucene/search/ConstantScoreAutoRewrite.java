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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.BytesRefHash.DirectBytesStartArray;

class ConstantScoreAutoRewrite extends TermCollectingRewrite<BooleanQuery> {

  // Defaults derived from rough tests with a 20.0 million
  // doc Wikipedia index.  With more than 350 terms in the
  // query, the filter method is fastest:
  public static int DEFAULT_TERM_COUNT_CUTOFF = 350;

  // If the query will hit more than 1 in 1000 of the docs
  // in the index (0.1%), the filter method is fastest:
  public static double DEFAULT_DOC_COUNT_PERCENT = 0.1;

  private int termCountCutoff = DEFAULT_TERM_COUNT_CUTOFF;
  private double docCountPercent = DEFAULT_DOC_COUNT_PERCENT;

  /** If the number of terms in this query is equal to or
   *  larger than this setting then {@link
   *  MultiTermQuery#CONSTANT_SCORE_FILTER_REWRITE} is used. */
  public void setTermCountCutoff(int count) {
    termCountCutoff = count;
  }

  /** @see #setTermCountCutoff */
  public int getTermCountCutoff() {
    return termCountCutoff;
  }

  /** If the number of documents to be visited in the
   *  postings exceeds this specified percentage of the
   *  maxDoc() for the index, then {@link
   *  MultiTermQuery#CONSTANT_SCORE_FILTER_REWRITE} is used.
   *  @param percent 0.0 to 100.0 */
  public void setDocCountPercent(double percent) {
    docCountPercent = percent;
  }

  /** @see #setDocCountPercent */
  public double getDocCountPercent() {
    return docCountPercent;
  }

  @Override
  protected BooleanQuery getTopLevelQuery() {
    return new BooleanQuery(true);
  }
  
  @Override
  protected void addClause(BooleanQuery topLevel, Term term, int docFreq, float boost /*ignored*/, TermContext states) {
    topLevel.add(new TermQuery(term, states), BooleanClause.Occur.SHOULD);
  }

  @Override
  public Query rewrite(final IndexReader reader, final MultiTermQuery query) throws IOException {

    // Get the enum and start visiting terms.  If we
    // exhaust the enum before hitting either of the
    // cutoffs, we use ConstantBooleanQueryRewrite; else,
    // ConstantFilterRewrite:
    final int docCountCutoff = (int) ((docCountPercent / 100.) * reader.maxDoc());
    final int termCountLimit = Math.min(BooleanQuery.getMaxClauseCount(), termCountCutoff);

    final CutOffTermCollector col = new CutOffTermCollector(docCountCutoff, termCountLimit);
    collectTerms(reader, query, col);
    final int size = col.pendingTerms.size();
    if (col.hasCutOff) {
      return MultiTermQuery.CONSTANT_SCORE_FILTER_REWRITE.rewrite(reader, query);
    } else if (size == 0) {
      return getTopLevelQuery();
    } else {
      final BooleanQuery bq = getTopLevelQuery();
      final BytesRefHash pendingTerms = col.pendingTerms;
      final int sort[] = pendingTerms.sort(col.termsEnum.getComparator());
      for(int i = 0; i < size; i++) {
        final int pos = sort[i];
        // docFreq is not used for constant score here, we pass 1
        // to explicitely set a fake value, so it's not calculated
        addClause(bq, new Term(query.field, pendingTerms.get(pos, new BytesRef())), 1, 1.0f, col.array.termState[pos]);
      }
      // Strip scores
      final Query result = new ConstantScoreQuery(bq);
      result.setBoost(query.getBoost());
      return result;
    }
  }
  
  static final class CutOffTermCollector extends TermCollector {
    CutOffTermCollector(int docCountCutoff, int termCountLimit) {
      this.docCountCutoff = docCountCutoff;
      this.termCountLimit = termCountLimit;
    }
  
    @Override
    public void setNextEnum(TermsEnum termsEnum) {
      this.termsEnum = termsEnum;
    }
      
    @Override
    public boolean collect(BytesRef bytes) throws IOException {
      int pos = pendingTerms.add(bytes);
      docVisitCount += termsEnum.docFreq();
      if (pendingTerms.size() >= termCountLimit || docVisitCount >= docCountCutoff) {
        hasCutOff = true;
        return false;
      }
      
      final TermState termState = termsEnum.termState();
      assert termState != null;
      if (pos < 0) {
        pos = (-pos)-1;
        array.termState[pos].register(termState, readerContext.ord, termsEnum.docFreq(), termsEnum.totalTermFreq());
      } else {
        array.termState[pos] = new TermContext(topReaderContext, termState, readerContext.ord, termsEnum.docFreq(), termsEnum.totalTermFreq());
      }
      return true;
    }
    
    int docVisitCount = 0;
    boolean hasCutOff = false;
    TermsEnum termsEnum;

    final int docCountCutoff, termCountLimit;
    final TermStateByteStart array = new TermStateByteStart(16);
    final BytesRefHash pendingTerms = new BytesRefHash(new ByteBlockPool(new ByteBlockPool.DirectAllocator()), 16, array);
  }

  @Override
  public int hashCode() {
    final int prime = 1279;
    return (int) (prime * termCountCutoff + Double.doubleToLongBits(docCountPercent));
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;

    ConstantScoreAutoRewrite other = (ConstantScoreAutoRewrite) obj;
    if (other.termCountCutoff != termCountCutoff) {
      return false;
    }

    if (Double.doubleToLongBits(other.docCountPercent) != Double.doubleToLongBits(docCountPercent)) {
      return false;
    }
    
    return true;
  }
  
  /** Special implementation of BytesStartArray that keeps parallel arrays for {@link TermContext} */
  static final class TermStateByteStart extends DirectBytesStartArray  {
    TermContext[] termState;
    
    public TermStateByteStart(int initSize) {
      super(initSize);
    }

    @Override
    public int[] init() {
      final int[] ord = super.init();
      termState = new TermContext[ArrayUtil.oversize(ord.length, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
      assert termState.length >= ord.length;
      return ord;
    }

    @Override
    public int[] grow() {
      final int[] ord = super.grow();
      if (termState.length < ord.length) {
        TermContext[] tmpTermState = new TermContext[ArrayUtil.oversize(ord.length, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
        System.arraycopy(termState, 0, tmpTermState, 0, termState.length);
        termState = tmpTermState;
      }      
      assert termState.length >= ord.length;
      return ord;
    }

    @Override
    public int[] clear() {
     termState = null;
     return super.clear();
    }
    
  }
}
