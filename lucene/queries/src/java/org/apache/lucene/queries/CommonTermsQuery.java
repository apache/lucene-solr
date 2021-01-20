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
package org.apache.lucene.queries;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermStates;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.TermQuery;

/**
 * A query that executes high-frequency terms in a optional sub-query to prevent slow queries due to
 * "common" terms like stopwords. This query builds 2 queries off the {@link #add(Term) added}
 * terms: low-frequency terms are added to a required boolean clause and high-frequency terms are
 * added to an optional boolean clause. The optional clause is only executed if the required
 * "low-frequency" clause matches. In most cases, high-frequency terms are unlikely to significantly
 * contribute to the document score unless at least one of the low-frequency terms are matched. This
 * query can improve query execution times significantly if applicable.
 *
 * <p>{@link CommonTermsQuery} has several advantages over stopword filtering at index or query time
 * since a term can be "classified" based on the actual document frequency in the index and can
 * prevent slow queries even across domains without specialized stopword files.
 *
 * <p><b>Note:</b> if the query only contains high-frequency terms the query is rewritten into a
 * plain conjunction query ie. all high-frequency terms need to match in order to match a document.
 */
public class CommonTermsQuery extends Query {
  /*
   * TODO maybe it would make sense to abstract this even further and allow to
   * rewrite to dismax rather than boolean. Yet, this can already be subclassed
   * to do so.
   */
  protected final List<Term> terms = new ArrayList<>();
  protected final float maxTermFrequency;
  protected final Occur lowFreqOccur;
  protected final Occur highFreqOccur;
  protected float lowFreqBoost = 1.0f;
  protected float highFreqBoost = 1.0f;
  protected float lowFreqMinNrShouldMatch = 0;
  protected float highFreqMinNrShouldMatch = 0;

  /**
   * Creates a new {@link CommonTermsQuery}
   *
   * @param highFreqOccur {@link Occur} used for high frequency terms
   * @param lowFreqOccur {@link Occur} used for low frequency terms
   * @param maxTermFrequency a value in [0..1) (or absolute number &gt;=1) representing the maximum
   *     threshold of a terms document frequency to be considered a low frequency term.
   * @throws IllegalArgumentException if {@link Occur#MUST_NOT} is pass as lowFreqOccur or
   *     highFreqOccur
   */
  public CommonTermsQuery(Occur highFreqOccur, Occur lowFreqOccur, float maxTermFrequency) {
    if (highFreqOccur == Occur.MUST_NOT) {
      throw new IllegalArgumentException("highFreqOccur should be MUST or SHOULD but was MUST_NOT");
    }
    if (lowFreqOccur == Occur.MUST_NOT) {
      throw new IllegalArgumentException("lowFreqOccur should be MUST or SHOULD but was MUST_NOT");
    }
    this.highFreqOccur = highFreqOccur;
    this.lowFreqOccur = lowFreqOccur;
    this.maxTermFrequency = maxTermFrequency;
  }

  /**
   * Adds a term to the {@link CommonTermsQuery}
   *
   * @param term the term to add
   */
  public void add(Term term) {
    if (term == null) {
      throw new IllegalArgumentException("Term must not be null");
    }
    this.terms.add(term);
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    if (this.terms.isEmpty()) {
      return new MatchNoDocsQuery("CommonTermsQuery with no terms");
    } else if (this.terms.size() == 1) {
      return newTermQuery(this.terms.get(0), null);
    }
    final List<LeafReaderContext> leaves = reader.leaves();
    final int maxDoc = reader.maxDoc();
    final TermStates[] contextArray = new TermStates[terms.size()];
    final Term[] queryTerms = this.terms.toArray(new Term[0]);
    collectTermStates(reader, leaves, contextArray, queryTerms);
    return buildQuery(maxDoc, contextArray, queryTerms);
  }

  @Override
  public void visit(QueryVisitor visitor) {
    Term[] selectedTerms =
        terms.stream().filter(t -> visitor.acceptField(t.field())).toArray(Term[]::new);
    if (selectedTerms.length > 0) {
      QueryVisitor v = visitor.getSubVisitor(Occur.SHOULD, this);
      v.consumeTerms(this, selectedTerms);
    }
  }

  protected int calcLowFreqMinimumNumberShouldMatch(int numOptional) {
    return minNrShouldMatch(lowFreqMinNrShouldMatch, numOptional);
  }

  protected int calcHighFreqMinimumNumberShouldMatch(int numOptional) {
    return minNrShouldMatch(highFreqMinNrShouldMatch, numOptional);
  }

  private final int minNrShouldMatch(float minNrShouldMatch, int numOptional) {
    if (minNrShouldMatch >= 1.0f || minNrShouldMatch == 0.0f) {
      return (int) minNrShouldMatch;
    }
    return Math.round(minNrShouldMatch * numOptional);
  }

  protected Query buildQuery(
      final int maxDoc, final TermStates[] contextArray, final Term[] queryTerms) {
    List<Query> lowFreqQueries = new ArrayList<>();
    List<Query> highFreqQueries = new ArrayList<>();
    for (int i = 0; i < queryTerms.length; i++) {
      TermStates termStates = contextArray[i];
      if (termStates == null) {
        lowFreqQueries.add(newTermQuery(queryTerms[i], null));
      } else {
        if ((maxTermFrequency >= 1f && termStates.docFreq() > maxTermFrequency)
            || (termStates.docFreq() > (int) Math.ceil(maxTermFrequency * (float) maxDoc))) {
          highFreqQueries.add(newTermQuery(queryTerms[i], termStates));
        } else {
          lowFreqQueries.add(newTermQuery(queryTerms[i], termStates));
        }
      }
    }
    final int numLowFreqClauses = lowFreqQueries.size();
    final int numHighFreqClauses = highFreqQueries.size();
    Occur lowFreqOccur = this.lowFreqOccur;
    Occur highFreqOccur = this.highFreqOccur;
    int lowFreqMinShouldMatch = 0;
    int highFreqMinShouldMatch = 0;
    if (lowFreqOccur == Occur.SHOULD && numLowFreqClauses > 0) {
      lowFreqMinShouldMatch = calcLowFreqMinimumNumberShouldMatch(numLowFreqClauses);
    }
    if (highFreqOccur == Occur.SHOULD && numHighFreqClauses > 0) {
      highFreqMinShouldMatch = calcHighFreqMinimumNumberShouldMatch(numHighFreqClauses);
    }
    if (lowFreqQueries.isEmpty()) {
      /*
       * if lowFreq is empty we rewrite the high freq terms in a conjunction to
       * prevent slow queries.
       */
      if (highFreqMinShouldMatch == 0 && highFreqOccur != Occur.MUST) {
        highFreqOccur = Occur.MUST;
      }
    }
    BooleanQuery.Builder builder = new BooleanQuery.Builder();

    if (lowFreqQueries.isEmpty() == false) {
      BooleanQuery.Builder lowFreq = new BooleanQuery.Builder();
      for (Query query : lowFreqQueries) {
        lowFreq.add(query, lowFreqOccur);
      }
      lowFreq.setMinimumNumberShouldMatch(lowFreqMinShouldMatch);
      Query lowFreqQuery = lowFreq.build();
      builder.add(new BoostQuery(lowFreqQuery, lowFreqBoost), Occur.MUST);
    }
    if (highFreqQueries.isEmpty() == false) {
      BooleanQuery.Builder highFreq = new BooleanQuery.Builder();
      for (Query query : highFreqQueries) {
        highFreq.add(query, highFreqOccur);
      }
      highFreq.setMinimumNumberShouldMatch(highFreqMinShouldMatch);
      Query highFreqQuery = highFreq.build();
      builder.add(new BoostQuery(highFreqQuery, highFreqBoost), Occur.SHOULD);
    }
    return builder.build();
  }

  public void collectTermStates(
      IndexReader reader,
      List<LeafReaderContext> leaves,
      TermStates[] contextArray,
      Term[] queryTerms)
      throws IOException {
    TermsEnum termsEnum = null;
    for (LeafReaderContext context : leaves) {
      for (int i = 0; i < queryTerms.length; i++) {
        Term term = queryTerms[i];
        TermStates termStates = contextArray[i];
        final Terms terms = context.reader().terms(term.field());
        if (terms == null) {
          // field does not exist
          continue;
        }
        termsEnum = terms.iterator();
        assert termsEnum != null;

        if (termsEnum == TermsEnum.EMPTY) continue;
        if (termsEnum.seekExact(term.bytes())) {
          if (termStates == null) {
            contextArray[i] =
                new TermStates(
                    reader.getContext(),
                    termsEnum.termState(),
                    context.ord,
                    termsEnum.docFreq(),
                    termsEnum.totalTermFreq());
          } else {
            termStates.register(
                termsEnum.termState(), context.ord, termsEnum.docFreq(), termsEnum.totalTermFreq());
          }
        }
      }
    }
  }

  /**
   * Specifies a minimum number of the low frequent optional BooleanClauses which must be satisfied
   * in order to produce a match on the low frequency terms query part. This method accepts a float
   * value in the range [0..1) as a fraction of the actual query terms in the low frequent clause or
   * a number <code>&gt;=1</code> as an absolut number of clauses that need to match.
   *
   * <p>By default no optional clauses are necessary for a match (unless there are no required
   * clauses). If this method is used, then the specified number of clauses is required.
   *
   * @param min the number of optional clauses that must match
   */
  public void setLowFreqMinimumNumberShouldMatch(float min) {
    this.lowFreqMinNrShouldMatch = min;
  }

  /**
   * Gets the minimum number of the optional low frequent BooleanClauses which must be satisfied.
   */
  public float getLowFreqMinimumNumberShouldMatch() {
    return lowFreqMinNrShouldMatch;
  }

  /**
   * Specifies a minimum number of the high frequent optional BooleanClauses which must be satisfied
   * in order to produce a match on the low frequency terms query part. This method accepts a float
   * value in the range [0..1) as a fraction of the actual query terms in the low frequent clause or
   * a number <code>&gt;=1</code> as an absolut number of clauses that need to match.
   *
   * <p>By default no optional clauses are necessary for a match (unless there are no required
   * clauses). If this method is used, then the specified number of clauses is required.
   *
   * @param min the number of optional clauses that must match
   */
  public void setHighFreqMinimumNumberShouldMatch(float min) {
    this.highFreqMinNrShouldMatch = min;
  }

  /**
   * Gets the minimum number of the optional high frequent BooleanClauses which must be satisfied.
   */
  public float getHighFreqMinimumNumberShouldMatch() {
    return highFreqMinNrShouldMatch;
  }

  /** Gets the list of terms. */
  public List<Term> getTerms() {
    return Collections.unmodifiableList(terms);
  }

  /**
   * Gets the maximum threshold of a terms document frequency to be considered a low frequency term.
   */
  public float getMaxTermFrequency() {
    return maxTermFrequency;
  }

  /** Gets the {@link Occur} used for low frequency terms. */
  public Occur getLowFreqOccur() {
    return lowFreqOccur;
  }

  /** Gets the {@link Occur} used for high frequency terms. */
  public Occur getHighFreqOccur() {
    return highFreqOccur;
  }

  /** Gets the boost used for low frequency terms. */
  public float getLowFreqBoost() {
    return lowFreqBoost;
  }

  /** Gets the boost used for high frequency terms. */
  public float getHighFreqBoost() {
    return highFreqBoost;
  }

  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    boolean needParens = (getLowFreqMinimumNumberShouldMatch() > 0);
    if (needParens) {
      buffer.append("(");
    }
    for (int i = 0; i < terms.size(); i++) {
      Term t = terms.get(i);
      buffer.append(newTermQuery(t, null).toString());

      if (i != terms.size() - 1) buffer.append(", ");
    }
    if (needParens) {
      buffer.append(")");
    }
    if (getLowFreqMinimumNumberShouldMatch() > 0 || getHighFreqMinimumNumberShouldMatch() > 0) {
      buffer.append('~');
      buffer.append("(");
      buffer.append(getLowFreqMinimumNumberShouldMatch());
      buffer.append(getHighFreqMinimumNumberShouldMatch());
      buffer.append(")");
    }
    return buffer.toString();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = classHash();
    result = prime * result + Float.floatToIntBits(highFreqBoost);
    result = prime * result + Objects.hashCode(highFreqOccur);
    result = prime * result + Objects.hashCode(lowFreqOccur);
    result = prime * result + Float.floatToIntBits(lowFreqBoost);
    result = prime * result + Float.floatToIntBits(maxTermFrequency);
    result = prime * result + Float.floatToIntBits(lowFreqMinNrShouldMatch);
    result = prime * result + Float.floatToIntBits(highFreqMinNrShouldMatch);
    result = prime * result + Objects.hashCode(terms);
    return result;
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) && equalsTo(getClass().cast(other));
  }

  private boolean equalsTo(CommonTermsQuery other) {
    return Float.floatToIntBits(highFreqBoost) == Float.floatToIntBits(other.highFreqBoost)
        && highFreqOccur == other.highFreqOccur
        && lowFreqOccur == other.lowFreqOccur
        && Float.floatToIntBits(lowFreqBoost) == Float.floatToIntBits(other.lowFreqBoost)
        && Float.floatToIntBits(maxTermFrequency) == Float.floatToIntBits(other.maxTermFrequency)
        && lowFreqMinNrShouldMatch == other.lowFreqMinNrShouldMatch
        && highFreqMinNrShouldMatch == other.highFreqMinNrShouldMatch
        && terms.equals(other.terms);
  }

  /**
   * Builds a new TermQuery instance.
   *
   * <p>This is intended for subclasses that wish to customize the generated queries.
   *
   * @param term term
   * @param termStates the TermStates to be used to create the low level term query. Can be <code>
   *     null</code>.
   * @return new TermQuery instance
   */
  protected Query newTermQuery(Term term, TermStates termStates) {
    return termStates == null ? new TermQuery(term) : new TermQuery(term, termStates);
  }
}
