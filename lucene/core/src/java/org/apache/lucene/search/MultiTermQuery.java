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
package org.apache.lucene.search;

import java.io.IOException;
import java.util.Objects;
import org.apache.lucene.index.FilteredTermsEnum; // javadocs
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SingleTermsEnum; // javadocs
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermStates;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanQuery.Builder;
import org.apache.lucene.util.AttributeSource;

/**
 * An abstract {@link Query} that matches documents containing a subset of terms provided by a
 * {@link FilteredTermsEnum} enumeration.
 *
 * <p>This query cannot be used directly; you must subclass it and define {@link
 * #getTermsEnum(Terms,AttributeSource)} to provide a {@link FilteredTermsEnum} that iterates
 * through the terms to be matched.
 *
 * <p><b>NOTE</b>: if {@link #setRewriteMethod} is either {@link #CONSTANT_SCORE_BOOLEAN_REWRITE} or
 * {@link #SCORING_BOOLEAN_REWRITE}, you may encounter a {@link IndexSearcher.TooManyClauses}
 * exception during searching, which happens when the number of terms to be searched exceeds {@link
 * IndexSearcher#getMaxClauseCount()}. Setting {@link #setRewriteMethod} to {@link
 * #CONSTANT_SCORE_REWRITE} prevents this.
 *
 * <p>The recommended rewrite method is {@link #CONSTANT_SCORE_REWRITE}: it doesn't spend CPU
 * computing unhelpful scores, and is the most performant rewrite method given the query. If you
 * need scoring (like {@link FuzzyQuery}, use {@link TopTermsScoringBooleanQueryRewrite} which uses
 * a priority queue to only collect competitive terms and not hit this limitation.
 *
 * <p>Note that org.apache.lucene.queryparser.classic.QueryParser produces MultiTermQueries using
 * {@link #CONSTANT_SCORE_REWRITE} by default.
 */
public abstract class MultiTermQuery extends Query {
  protected final String field;
  protected RewriteMethod rewriteMethod = CONSTANT_SCORE_REWRITE;

  /** Abstract class that defines how the query is rewritten. */
  public abstract static class RewriteMethod {
    public abstract Query rewrite(IndexReader reader, MultiTermQuery query) throws IOException;
    /**
     * Returns the {@link MultiTermQuery}s {@link TermsEnum}
     *
     * @see MultiTermQuery#getTermsEnum(Terms, AttributeSource)
     */
    protected TermsEnum getTermsEnum(MultiTermQuery query, Terms terms, AttributeSource atts)
        throws IOException {
      return query.getTermsEnum(
          terms, atts); // allow RewriteMethod subclasses to pull a TermsEnum from the MTQ
    }
  }

  /**
   * A rewrite method that first creates a private Filter, by visiting each term in sequence and
   * marking all docs for that term. Matching documents are assigned a constant score equal to the
   * query's boost.
   *
   * <p>This method is faster than the BooleanQuery rewrite methods when the number of matched terms
   * or matched documents is non-trivial. Also, it will never hit an errant {@link
   * IndexSearcher.TooManyClauses} exception.
   *
   * @see #setRewriteMethod
   */
  public static final RewriteMethod CONSTANT_SCORE_REWRITE =
      new RewriteMethod() {
        @Override
        public Query rewrite(IndexReader reader, MultiTermQuery query) {
          return new MultiTermQueryConstantScoreWrapper<>(query);
        }
      };

  /**
   * A rewrite method that first translates each term into {@link BooleanClause.Occur#SHOULD} clause
   * in a BooleanQuery, and keeps the scores as computed by the query. Note that typically such
   * scores are meaningless to the user, and require non-trivial CPU to compute, so it's almost
   * always better to use {@link #CONSTANT_SCORE_REWRITE} instead.
   *
   * <p><b>NOTE</b>: This rewrite method will hit {@link IndexSearcher.TooManyClauses} if the number
   * of terms exceeds {@link IndexSearcher#getMaxClauseCount}.
   *
   * @see #setRewriteMethod
   */
  public static final RewriteMethod SCORING_BOOLEAN_REWRITE =
      ScoringRewrite.SCORING_BOOLEAN_REWRITE;

  /**
   * Like {@link #SCORING_BOOLEAN_REWRITE} except scores are not computed. Instead, each matching
   * document receives a constant score equal to the query's boost.
   *
   * <p><b>NOTE</b>: This rewrite method will hit {@link IndexSearcher.TooManyClauses} if the number
   * of terms exceeds {@link IndexSearcher#getMaxClauseCount}.
   *
   * @see #setRewriteMethod
   */
  public static final RewriteMethod CONSTANT_SCORE_BOOLEAN_REWRITE =
      ScoringRewrite.CONSTANT_SCORE_BOOLEAN_REWRITE;

  /**
   * A rewrite method that first translates each term into {@link BooleanClause.Occur#SHOULD} clause
   * in a BooleanQuery, and keeps the scores as computed by the query.
   *
   * <p>This rewrite method only uses the top scoring terms so it will not overflow the boolean max
   * clause count. It is the default rewrite method for {@link FuzzyQuery}.
   *
   * @see #setRewriteMethod
   */
  public static final class TopTermsScoringBooleanQueryRewrite
      extends TopTermsRewrite<BooleanQuery.Builder> {

    /**
     * Create a TopTermsScoringBooleanQueryRewrite for at most <code>size</code> terms.
     *
     * <p>NOTE: if {@link IndexSearcher#getMaxClauseCount} is smaller than <code>size</code>, then
     * it will be used instead.
     */
    public TopTermsScoringBooleanQueryRewrite(int size) {
      super(size);
    }

    @Override
    protected int getMaxSize() {
      return IndexSearcher.getMaxClauseCount();
    }

    @Override
    protected BooleanQuery.Builder getTopLevelBuilder() {
      return new BooleanQuery.Builder();
    }

    @Override
    protected Query build(Builder builder) {
      return builder.build();
    }

    @Override
    protected void addClause(
        BooleanQuery.Builder topLevel, Term term, int docCount, float boost, TermStates states) {
      final TermQuery tq = new TermQuery(term, states);
      topLevel.add(new BoostQuery(tq, boost), BooleanClause.Occur.SHOULD);
    }
  }

  /**
   * A rewrite method that first translates each term into {@link BooleanClause.Occur#SHOULD} clause
   * in a BooleanQuery, but adjusts the frequencies used for scoring to be blended across the terms,
   * otherwise the rarest term typically ranks highest (often not useful eg in the set of expanded
   * terms in a FuzzyQuery).
   *
   * <p>This rewrite method only uses the top scoring terms so it will not overflow the boolean max
   * clause count.
   *
   * @see #setRewriteMethod
   */
  public static final class TopTermsBlendedFreqScoringRewrite
      extends TopTermsRewrite<BlendedTermQuery.Builder> {

    /**
     * Create a TopTermsBlendedScoringBooleanQueryRewrite for at most <code>size</code> terms.
     *
     * <p>NOTE: if {@link IndexSearcher#getMaxClauseCount} is smaller than <code>size</code>, then
     * it will be used instead.
     */
    public TopTermsBlendedFreqScoringRewrite(int size) {
      super(size);
    }

    @Override
    protected int getMaxSize() {
      return IndexSearcher.getMaxClauseCount();
    }

    @Override
    protected BlendedTermQuery.Builder getTopLevelBuilder() {
      BlendedTermQuery.Builder builder = new BlendedTermQuery.Builder();
      builder.setRewriteMethod(BlendedTermQuery.BOOLEAN_REWRITE);
      return builder;
    }

    @Override
    protected Query build(BlendedTermQuery.Builder builder) {
      return builder.build();
    }

    @Override
    protected void addClause(
        BlendedTermQuery.Builder topLevel,
        Term term,
        int docCount,
        float boost,
        TermStates states) {
      topLevel.add(term, boost, states);
    }
  }

  /**
   * A rewrite method that first translates each term into {@link BooleanClause.Occur#SHOULD} clause
   * in a BooleanQuery, but the scores are only computed as the boost.
   *
   * <p>This rewrite method only uses the top scoring terms so it will not overflow the boolean max
   * clause count.
   *
   * @see #setRewriteMethod
   */
  public static final class TopTermsBoostOnlyBooleanQueryRewrite
      extends TopTermsRewrite<BooleanQuery.Builder> {

    /**
     * Create a TopTermsBoostOnlyBooleanQueryRewrite for at most <code>size</code> terms.
     *
     * <p>NOTE: if {@link IndexSearcher#getMaxClauseCount} is smaller than <code>size</code>, then
     * it will be used instead.
     */
    public TopTermsBoostOnlyBooleanQueryRewrite(int size) {
      super(size);
    }

    @Override
    protected int getMaxSize() {
      return IndexSearcher.getMaxClauseCount();
    }

    @Override
    protected BooleanQuery.Builder getTopLevelBuilder() {
      return new BooleanQuery.Builder();
    }

    @Override
    protected Query build(BooleanQuery.Builder builder) {
      return builder.build();
    }

    @Override
    protected void addClause(
        BooleanQuery.Builder topLevel, Term term, int docFreq, float boost, TermStates states) {
      final Query q = new ConstantScoreQuery(new TermQuery(term, states));
      topLevel.add(new BoostQuery(q, boost), BooleanClause.Occur.SHOULD);
    }
  }

  /** Constructs a query matching terms that cannot be represented with a single Term. */
  public MultiTermQuery(final String field) {
    this.field = Objects.requireNonNull(field, "field must not be null");
  }

  /** Returns the field name for this query */
  public final String getField() {
    return field;
  }

  /**
   * Construct the enumeration to be used, expanding the pattern term. This method should only be
   * called if the field exists (ie, implementations can assume the field does exist). This method
   * should not return null (should instead return {@link TermsEnum#EMPTY} if no terms match). The
   * TermsEnum must already be positioned to the first matching term. The given {@link
   * AttributeSource} is passed by the {@link RewriteMethod} to share information between segments,
   * for example {@link TopTermsRewrite} uses it to share maximum competitive boosts
   */
  protected abstract TermsEnum getTermsEnum(Terms terms, AttributeSource atts) throws IOException;

  /**
   * Constructs an enumeration that expands the pattern term. This method should only be called if
   * the field exists (ie, implementations can assume the field does exist). This method never
   * returns null. The returned TermsEnum is positioned to the first matching term.
   */
  public final TermsEnum getTermsEnum(Terms terms) throws IOException {
    return getTermsEnum(terms, new AttributeSource());
  }

  /**
   * To rewrite to a simpler form, instead return a simpler enum from {@link #getTermsEnum(Terms,
   * AttributeSource)}. For example, to rewrite to a single term, return a {@link SingleTermsEnum}
   */
  @Override
  public final Query rewrite(IndexReader reader) throws IOException {
    return rewriteMethod.rewrite(reader, this);
  }

  /** @see #setRewriteMethod */
  public RewriteMethod getRewriteMethod() {
    return rewriteMethod;
  }

  /**
   * Sets the rewrite method to be used when executing the query. You can use one of the four core
   * methods, or implement your own subclass of {@link RewriteMethod}.
   */
  public void setRewriteMethod(RewriteMethod method) {
    rewriteMethod = method;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = classHash();
    result = prime * result + rewriteMethod.hashCode();
    result = prime * result + field.hashCode();
    return result;
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) && equalsTo(getClass().cast(other));
  }

  private boolean equalsTo(MultiTermQuery other) {
    return rewriteMethod.equals(other.rewriteMethod) && field.equals(other.field);
  }
}
