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
import java.util.ArrayList;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;

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
   *  #CONSTANT_SCORE_FILTER_REWRITE} is used. */
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
   *  #CONSTANT_SCORE_FILTER_REWRITE} is used.
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
  protected void addClause(BooleanQuery topLevel, Term term, float boost /*ignored*/) {
    topLevel.add(new TermQuery(term), BooleanClause.Occur.SHOULD);
  }

  @Override
  public Query rewrite(final IndexReader reader, final MultiTermQuery query) throws IOException {

    // Get the enum and start visiting terms.  If we
    // exhaust the enum before hitting either of the
    // cutoffs, we use ConstantBooleanQueryRewrite; else,
    // ConstantFilterRewrite:
    final int docCountCutoff = (int) ((docCountPercent / 100.) * reader.maxDoc());
    final int termCountLimit = Math.min(BooleanQuery.getMaxClauseCount(), termCountCutoff);

    final CutOffTermCollector col = new CutOffTermCollector(reader, docCountCutoff, termCountLimit);
    collectTerms(reader, query, col);
    
    if (col.hasCutOff) {
      return MultiTermQuery.CONSTANT_SCORE_FILTER_REWRITE.rewrite(reader, query);
    } else {
      final Query result;
      if (col.pendingTerms.isEmpty()) {
        result = getTopLevelQuery();
      } else {
        BooleanQuery bq = getTopLevelQuery();
        for(Term term : col.pendingTerms) {
          addClause(bq, term, 1.0f);
        }
        // Strip scores
        result = new ConstantScoreQuery(bq);
        result.setBoost(query.getBoost());
      }
      query.incTotalNumberOfTerms(col.pendingTerms.size());
      return result;
    }
  }
  
  private static final class CutOffTermCollector implements TermCollector {
    CutOffTermCollector(IndexReader reader, int docCountCutoff, int termCountLimit) {
      this.reader = reader;
      this.docCountCutoff = docCountCutoff;
      this.termCountLimit = termCountLimit;
    }
  
    public boolean collect(Term t, float boost) throws IOException {
      pendingTerms.add(t);
      // Loading the TermInfo from the terms dict here
      // should not be costly, because 1) the
      // query/filter will load the TermInfo when it
      // runs, and 2) the terms dict has a cache:
      docVisitCount += reader.docFreq(t);
      if (pendingTerms.size() >= termCountLimit || docVisitCount >= docCountCutoff) {
        hasCutOff = true;
        return false;
      }
      return true;
    }
    
    int docVisitCount = 0;
    boolean hasCutOff = false;
    
    final IndexReader reader;
    final int docCountCutoff, termCountLimit;
    final ArrayList<Term> pendingTerms = new ArrayList<Term>();
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
}
