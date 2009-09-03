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
import java.util.Collection;
import java.util.Iterator;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.ToStringUtils;
import org.apache.lucene.queryParser.QueryParser; // for javadoc

/**
 * An abstract {@link Query} that matches documents
 * containing a subset of terms provided by a {@link
 * FilteredTermEnum} enumeration.
 *
 * <p>This query cannot be used directly; you must subclass
 * it and define {@link #getEnum} to provide a {@link
 * FilteredTermEnum} that iterates through the terms to be
 * matched.
 *
 * <p><b>NOTE</b>: if {@link #setRewriteMethod} is either
 * {@link #CONSTANT_SCORE_BOOLEAN_QUERY_REWRITE} or {@link
 * #SCORING_BOOLEAN_QUERY_REWRITE}, you may encounter a
 * {@link BooleanQuery.TooManyClauses} exception during
 * searching, which happens when the number of terms to be
 * searched exceeds {@link
 * BooleanQuery#getMaxClauseCount()}.  Setting {@link
 * #setRewriteMethod} to {@link #CONSTANT_SCORE_FILTER_REWRITE}
 * prevents this.
 *
 * <p>The recommended rewrite method is {@link
 * #CONSTANT_SCORE_AUTO_REWRITE_DEFAULT}: it doesn't spend CPU
 * computing unhelpful scores, and it tries to pick the most
 * performant rewrite method given the query.
 *
 * Note that {@link QueryParser} produces
 * MultiTermQueries using {@link
 * #CONSTANT_SCORE_AUTO_REWRITE_DEFAULT} by default.
 */
public abstract class MultiTermQuery extends Query {
  /* @deprecated move to sub class */
  protected Term term;
  protected RewriteMethod rewriteMethod = CONSTANT_SCORE_AUTO_REWRITE_DEFAULT;
  transient int numberOfTerms = 0;

  /** Abstract class that defines how the query is rewritten. */
  public static abstract class RewriteMethod implements Serializable {
    public abstract Query rewrite(IndexReader reader, MultiTermQuery query) throws IOException;
  }

  private static final class ConstantScoreFilterRewrite extends RewriteMethod implements Serializable {
    public Query rewrite(IndexReader reader, MultiTermQuery query) {
      Query result = new ConstantScoreQuery(new MultiTermQueryWrapperFilter(query));
      result.setBoost(query.getBoost());
      return result;
    }

    // Make sure we are still a singleton even after deserializing
    protected Object readResolve() {
      return CONSTANT_SCORE_FILTER_REWRITE;
    }
  }

  /** A rewrite method that first creates a private Filter,
   *  by visiting each term in sequence and marking all docs
   *  for that term.  Matching documents are assigned a
   *  constant score equal to the query's boost.
   * 
   *  <p> This method is faster than the BooleanQuery
   *  rewrite methods when the number of matched terms or
   *  matched documents is non-trivial. Also, it will never
   *  hit an errant {@link BooleanQuery.TooManyClauses}
   *  exception.
   *
   *  @see #setRewriteMethod */
  public final static RewriteMethod CONSTANT_SCORE_FILTER_REWRITE = new ConstantScoreFilterRewrite();

  private static class ScoringBooleanQueryRewrite extends RewriteMethod implements Serializable {
    public Query rewrite(IndexReader reader, MultiTermQuery query) throws IOException {

      FilteredTermEnum enumerator = query.getEnum(reader);
      BooleanQuery result = new BooleanQuery(true);
      int count = 0;
      try {
        do {
          Term t = enumerator.term();
          if (t != null) {
            TermQuery tq = new TermQuery(t); // found a match
            tq.setBoost(query.getBoost() * enumerator.difference()); // set the boost
            result.add(tq, BooleanClause.Occur.SHOULD); // add to query
            count++;
          }
        } while (enumerator.next());    
      } finally {
        enumerator.close();
      }
      query.incTotalNumberOfTerms(count);
      return result;
    }

    // Make sure we are still a singleton even after deserializing
    protected Object readResolve() {
      return SCORING_BOOLEAN_QUERY_REWRITE;
    }
  }

  /** A rewrite method that first translates each term into
   *  {@link BooleanClause.Occur#SHOULD} clause in a
   *  BooleanQuery, and keeps the scores as computed by the
   *  query.  Note that typically such scores are
   *  meaningless to the user, and require non-trivial CPU
   *  to compute, so it's almost always better to use {@link
   *  #CONSTANT_SCORE_AUTO_REWRITE_DEFAULT} instead.
   *
   *  <p><b>NOTE</b>: This rewrite method will hit {@link
   *  BooleanQuery.TooManyClauses} if the number of terms
   *  exceeds {@link BooleanQuery#getMaxClauseCount}.
   *
   *  @see #setRewriteMethod */
  public final static RewriteMethod SCORING_BOOLEAN_QUERY_REWRITE = new ScoringBooleanQueryRewrite();

  private static class ConstantScoreBooleanQueryRewrite extends ScoringBooleanQueryRewrite implements Serializable {
    public Query rewrite(IndexReader reader, MultiTermQuery query) throws IOException {
      // strip the scores off
      Query result = new ConstantScoreQuery(new QueryWrapperFilter(super.rewrite(reader, query)));
      result.setBoost(query.getBoost());
      return result;
    }

    // Make sure we are still a singleton even after deserializing
    protected Object readResolve() {
      return CONSTANT_SCORE_BOOLEAN_QUERY_REWRITE;
    }
  }

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
  public final static RewriteMethod CONSTANT_SCORE_BOOLEAN_QUERY_REWRITE = new ConstantScoreBooleanQueryRewrite();


  /** A rewrite method that tries to pick the best
   *  constant-score rewrite method based on term and
   *  document counts from the query.  If both the number of
   *  terms and documents is small enough, then {@link
   *  #CONSTANT_SCORE_BOOLEAN_QUERY_REWRITE} is used.
   *  Otherwise, {@link #CONSTANT_SCORE_FILTER_REWRITE} is
   *  used.
   */
  public static class ConstantScoreAutoRewrite extends RewriteMethod implements Serializable {

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

    public Query rewrite(IndexReader reader, MultiTermQuery query) throws IOException {
      // Get the enum and start visiting terms.  If we
      // exhaust the enum before hitting either of the
      // cutoffs, we use ConstantBooleanQueryRewrite; else,
      // ConstantFilterRewrite:
      final Collection pendingTerms = new ArrayList();
      final int docCountCutoff = (int) ((docCountPercent / 100.) * reader.maxDoc());
      final int termCountLimit = Math.min(BooleanQuery.getMaxClauseCount(), termCountCutoff);
      int docVisitCount = 0;

      FilteredTermEnum enumerator = query.getEnum(reader);
      try {
        while(true) {
          Term t = enumerator.term();
          if (t != null) {
            pendingTerms.add(t);
            // Loading the TermInfo from the terms dict here
            // should not be costly, because 1) the
            // query/filter will load the TermInfo when it
            // runs, and 2) the terms dict has a cache:
            docVisitCount += reader.docFreq(t);
          }

          if (pendingTerms.size() >= termCountLimit || docVisitCount >= docCountCutoff) {
            // Too many terms -- make a filter.
            Query result = new ConstantScoreQuery(new MultiTermQueryWrapperFilter(query));
            result.setBoost(query.getBoost());
            return result;
          } else  if (!enumerator.next()) {
            // Enumeration is done, and we hit a small
            // enough number of terms & docs -- just make a
            // BooleanQuery, now
            Iterator it = pendingTerms.iterator();
            BooleanQuery bq = new BooleanQuery(true);
            while(it.hasNext()) {
              TermQuery tq = new TermQuery((Term) it.next());
              bq.add(tq, BooleanClause.Occur.SHOULD);
            }
            // Strip scores
            Query result = new ConstantScoreQuery(new QueryWrapperFilter(bq));
            result.setBoost(query.getBoost());
            query.incTotalNumberOfTerms(pendingTerms.size());
            return result;
          }
        }
      } finally {
        enumerator.close();
      }
    }
    
    public int hashCode() {
      final int prime = 1279;
      return (int) (prime * termCountCutoff + Double.doubleToLongBits(docCountPercent));
    }

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

  /** Read-only default instance of {@link
   *  ConstantScoreAutoRewrite}, with {@link
   *  ConstantScoreAutoRewrite#setTermCountCutoff} set to
   *  {@link
   *  ConstantScoreAutoRewrite#DEFAULT_TERM_COUNT_CUTOFF}
   *  and {@link
   *  ConstantScoreAutoRewrite#setDocCountPercent} set to
   *  {@link
   *  ConstantScoreAutoRewrite#DEFAULT_DOC_COUNT_PERCENT}.
   *  Note that you cannot alter the configuration of this
   *  instance; you'll need to create a private instance
   *  instead. */
  public final static RewriteMethod CONSTANT_SCORE_AUTO_REWRITE_DEFAULT = new ConstantScoreAutoRewrite() {
    public void setTermCountCutoff(int count) {
      throw new UnsupportedOperationException("Please create a private instance");
    }

    public void setDocCountPercent(double percent) {
      throw new UnsupportedOperationException("Please create a private instance");
    }

    // Make sure we are still a singleton even after deserializing
    protected Object readResolve() {
      return CONSTANT_SCORE_AUTO_REWRITE_DEFAULT;
    }
  };

  /**
   * Constructs a query for terms matching <code>term</code>.
   * @deprecated check sub class for possible term access - the Term does not
   * make sense for all MultiTermQuerys and will be removed.
   */
  public MultiTermQuery(Term term) {
    this.term = term;
  }

  /**
   * Constructs a query matching terms that cannot be represented with a single
   * Term.
   */
  public MultiTermQuery() {
  }

  /**
   * Returns the pattern term.
   * @deprecated check sub class for possible term access - getTerm does not
   * make sense for all MultiTermQuerys and will be removed.
   */
  public Term getTerm() {
    return term;
  }

  /** Construct the enumeration to be used, expanding the pattern term. */
  protected abstract FilteredTermEnum getEnum(IndexReader reader)
      throws IOException;

  /**
   * Expert: Return the number of unique terms visited during execution of the query.
   * If there are many of them, you may consider using another query type
   * or optimize your total term count in index.
   * <p>This method is not thread safe, be sure to only call it when no query is running!
   * If you re-use the same query instance for another
   * search, be sure to first reset the term counter
   * with {@link #clearTotalNumberOfTerms}.
   * <p>On optimized indexes / no MultiReaders, you get the correct number of
   * unique terms for the whole index. Use this number to compare different queries.
   * For non-optimized indexes this number can also be achieved in
   * non-constant-score mode. In constant-score mode you get the total number of
   * terms seeked for all segments / sub-readers.
   * @see #clearTotalNumberOfTerms
   */
  public int getTotalNumberOfTerms() {
    return numberOfTerms;
  }
  
  /**
   * Expert: Resets the counting of unique terms.
   * Do this before executing the query/filter.
   * @see #getTotalNumberOfTerms
   */
  public void clearTotalNumberOfTerms() {
    numberOfTerms = 0;
  }
  
  protected void incTotalNumberOfTerms(int inc) {
    numberOfTerms += inc;
  }

  public Query rewrite(IndexReader reader) throws IOException {
    return rewriteMethod.rewrite(reader, this);
  }


  /* Prints a user-readable version of this query.
   * Implemented for back compat in case MultiTermQuery
   * subclasses do no implement.
   */
  public String toString(String field) {
    StringBuffer buffer = new StringBuffer();
    if (term != null) {
      if (!term.field().equals(field)) {
        buffer.append(term.field());
        buffer.append(":");
      }
      buffer.append(term.text());
    } else {
      buffer.append("termPattern:unknown");
    }
    buffer.append(ToStringUtils.boost(getBoost()));
    return buffer.toString();
  }

  /**
   * @see #setRewriteMethod
   */
  public RewriteMethod getRewriteMethod() {
    return rewriteMethod;
  }

  /**
   * Sets the rewrite method to be used when executing the
   * query.  You can use one of the four core methods, or
   * implement your own subclass of {@link RewriteMethod}. */
  public void setRewriteMethod(RewriteMethod method) {
    rewriteMethod = method;
  }

  //@Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Float.floatToIntBits(getBoost());
    result = prime * result;
    result += rewriteMethod.hashCode();
    return result;
  }

  //@Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    MultiTermQuery other = (MultiTermQuery) obj;
    if (Float.floatToIntBits(getBoost()) != Float.floatToIntBits(other.getBoost()))
      return false;
    if (!rewriteMethod.equals(other.rewriteMethod)) {
      return false;
    }
    return true;
  }
 
}
