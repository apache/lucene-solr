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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.function.FunctionQuery;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.FilterScorer;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;

/**
 * Query that sets document score as a programmatic function of several (sub) scores:
 * <ol>
 *    <li>the score of its subQuery (any query)</li>
 *    <li>(optional) the score of its {@link FunctionQuery} (or queries).</li>
 * </ol>
 * Subclasses can modify the computation by overriding {@link #getCustomScoreProvider}.
 * 
 * @lucene.experimental
 */
public class CustomScoreQuery extends Query implements Cloneable {

  private Query subQuery;
  private Query[] scoringQueries; // never null (empty array if there are no valSrcQueries).
  private boolean strict = false; // if true, valueSource part of query does not take part in weights normalization.

  /**
   * Create a CustomScoreQuery over input subQuery.
   * @param subQuery the sub query whose scored is being customized. Must not be null. 
   */
  public CustomScoreQuery(Query subQuery) {
    this(subQuery, new FunctionQuery[0]);
  }

  /**
   * Create a CustomScoreQuery over input subQuery and a {@link org.apache.lucene.queries.function.FunctionQuery}.
   * @param subQuery the sub query whose score is being customized. Must not be null.
   * @param scoringQuery a value source query whose scores are used in the custom score
   * computation.  This parameter is optional - it can be null.
   */
  public CustomScoreQuery(Query subQuery, FunctionQuery scoringQuery) {
    this(subQuery, scoringQuery!=null ? // don't want an array that contains a single null..
        new FunctionQuery[] {scoringQuery} : new FunctionQuery[0]);
  }

  /**
   * Create a CustomScoreQuery over input subQuery and a {@link org.apache.lucene.queries.function.FunctionQuery}.
   * @param subQuery the sub query whose score is being customized. Must not be null.
   * @param scoringQueries value source queries whose scores are used in the custom score
   * computation.  This parameter is optional - it can be null or even an empty array.
   */
  public CustomScoreQuery(Query subQuery, FunctionQuery... scoringQueries) {
    this.subQuery = subQuery;
    this.scoringQueries = scoringQueries !=null?
        scoringQueries : new Query[0];
    if (subQuery == null) throw new IllegalArgumentException("<subquery> must not be null!");
  }

  /*(non-Javadoc) @see org.apache.lucene.search.Query#rewrite(org.apache.lucene.index.IndexReader) */
  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    CustomScoreQuery clone = null;
    
    final Query sq = subQuery.rewrite(reader);
    if (sq != subQuery) {
      clone = clone();
      clone.subQuery = sq;
    }

    for(int i = 0; i < scoringQueries.length; i++) {
      final Query v = scoringQueries[i].rewrite(reader);
      if (v != scoringQueries[i]) {
        if (clone == null) clone = clone();
        clone.scoringQueries[i] = v;
      }
    }
    
    return (clone == null) ? this : clone;
  }

  /*(non-Javadoc) @see org.apache.lucene.search.Query#clone() */
  @Override
  public CustomScoreQuery clone() {
    CustomScoreQuery clone;
    try {
      clone = (CustomScoreQuery)super.clone();
    } catch (CloneNotSupportedException bogus) {
      // cannot happen
      throw new Error(bogus);
    }
    clone.subQuery = subQuery;
    clone.scoringQueries = new Query[scoringQueries.length];
    for(int i = 0; i < scoringQueries.length; i++) {
      clone.scoringQueries[i] = scoringQueries[i];
    }
    return clone;
  }

  /* (non-Javadoc) @see org.apache.lucene.search.Query#toString(java.lang.String) */
  @Override
  public String toString(String field) {
    StringBuilder sb = new StringBuilder(name()).append("(");
    sb.append(subQuery.toString(field));
    for (Query scoringQuery : scoringQueries) {
      sb.append(", ").append(scoringQuery.toString(field));
    }
    sb.append(")");
    sb.append(strict?" STRICT" : "");
    return sb.toString();
  }

  /** Returns true if <code>o</code> is equal to this. */
  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) &&
           equalsTo(getClass().cast(other));
  }

  private boolean equalsTo(CustomScoreQuery other) {
    return subQuery.equals(other.subQuery) &&
           strict == other.strict &&
           scoringQueries.length == other.scoringQueries.length &&
           Arrays.equals(scoringQueries, other.scoringQueries);
  }

  /** Returns a hash code value for this object. */
  @Override
  public int hashCode() {
    // Didn't change this hashcode, but it looks suspicious.
    return (classHash() + 
        subQuery.hashCode() + 
        Arrays.hashCode(scoringQueries)) ^ (strict ? 1234 : 4321);
  }
  
  /**
   * Returns a {@link CustomScoreProvider} that calculates the custom scores
   * for the given {@link IndexReader}. The default implementation returns a default
   * implementation as specified in the docs of {@link CustomScoreProvider}.
   * @since 2.9.2
   */
  protected CustomScoreProvider getCustomScoreProvider(LeafReaderContext context) throws IOException {
    return new CustomScoreProvider(context);
  }

  //=========================== W E I G H T ============================
  
  private class CustomWeight extends Weight {
    Weight subQueryWeight;
    Weight[] valSrcWeights;
    boolean qStrict;
    float queryWeight;

    public CustomWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
      super(CustomScoreQuery.this);
      this.subQueryWeight = subQuery.createWeight(searcher, needsScores);
      this.valSrcWeights = new Weight[scoringQueries.length];
      for(int i = 0; i < scoringQueries.length; i++) {
        this.valSrcWeights[i] = scoringQueries[i].createWeight(searcher, needsScores);
      }
      this.qStrict = strict;
    }

    @Override
    public void extractTerms(Set<Term> terms) {
      subQueryWeight.extractTerms(terms);
      for (Weight scoringWeight : valSrcWeights) {
        scoringWeight.extractTerms(terms);
      }
    }

    @Override
    public float getValueForNormalization() throws IOException {
      float sum = subQueryWeight.getValueForNormalization();
      for (Weight valSrcWeight : valSrcWeights) {
        if (qStrict == false) { // otherwise do not include ValueSource part in the query normalization
          sum += valSrcWeight.getValueForNormalization();
        }
      }
      return sum;
    }

    /*(non-Javadoc) @see org.apache.lucene.search.Weight#normalize(float) */
    @Override
    public void normalize(float norm, float boost) {
      // note we DONT incorporate our boost, nor pass down any boost 
      // (e.g. from outer BQ), as there is no guarantee that the CustomScoreProvider's 
      // function obeys the distributive law... it might call sqrt() on the subQuery score
      // or some other arbitrary function other than multiplication.
      // so, instead boosts are applied directly in score()
      subQueryWeight.normalize(norm, 1f);
      for (Weight valSrcWeight : valSrcWeights) {
        if (qStrict) {
          valSrcWeight.normalize(1, 1); // do not normalize the ValueSource part
        } else {
          valSrcWeight.normalize(norm, 1f);
        }
      }
      queryWeight = boost;
    }

    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
      Scorer subQueryScorer = subQueryWeight.scorer(context);
      if (subQueryScorer == null) {
        return null;
      }
      Scorer[] valSrcScorers = new Scorer[valSrcWeights.length];
      for(int i = 0; i < valSrcScorers.length; i++) {
         valSrcScorers[i] = valSrcWeights[i].scorer(context);
      }
      return new CustomScorer(CustomScoreQuery.this.getCustomScoreProvider(context), this, queryWeight, subQueryScorer, valSrcScorers);
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      Explanation explain = doExplain(context, doc);
      return explain == null ? Explanation.noMatch("no matching docs") : explain;
    }
    
    private Explanation doExplain(LeafReaderContext info, int doc) throws IOException {
      Explanation subQueryExpl = subQueryWeight.explain(info, doc);
      if (!subQueryExpl.isMatch()) {
        return subQueryExpl;
      }
      // match
      Explanation[] valSrcExpls = new Explanation[valSrcWeights.length];
      for(int i = 0; i < valSrcWeights.length; i++) {
        valSrcExpls[i] = valSrcWeights[i].explain(info, doc);
      }
      Explanation customExp = CustomScoreQuery.this.getCustomScoreProvider(info).customExplain(doc,subQueryExpl,valSrcExpls);
      float sc = queryWeight * customExp.getValue();
      return Explanation.match(
        sc, CustomScoreQuery.this.toString() + ", product of:",
        customExp, Explanation.match(queryWeight, "queryWeight"));
    }
    
  }


  //=========================== S C O R E R ============================
  
  /**
   * A scorer that applies a (callback) function on scores of the subQuery.
   */
  private class CustomScorer extends FilterScorer {
    private final float qWeight;
    private final Scorer subQueryScorer;
    private final Scorer[] valSrcScorers;
    private final CustomScoreProvider provider;
    private final float[] vScores; // reused in score() to avoid allocating this array for each doc
    private int valSrcDocID = -1; // we lazily advance subscorers.

    // constructor
    private CustomScorer(CustomScoreProvider provider, CustomWeight w, float qWeight,
        Scorer subQueryScorer, Scorer[] valSrcScorers) {
      super(subQueryScorer, w);
      this.qWeight = qWeight;
      this.subQueryScorer = subQueryScorer;
      this.valSrcScorers = valSrcScorers;
      this.vScores = new float[valSrcScorers.length];
      this.provider = provider;
    }
    
    @Override
    public float score() throws IOException {
      // lazily advance to current doc.
      int doc = docID();
      if (doc > valSrcDocID) {
        for (Scorer valSrcScorer : valSrcScorers) {
          valSrcScorer.iterator().advance(doc);
        }
        valSrcDocID = doc;
      }
      // TODO: this thing technically takes any Query, so what about when subs don't match?
      for (int i = 0; i < valSrcScorers.length; i++) {
        vScores[i] = valSrcScorers[i].score();
      }
      return qWeight * provider.customScore(subQueryScorer.docID(), subQueryScorer.score(), vScores);
    }

    @Override
    public Collection<ChildScorer> getChildren() {
      return Collections.singleton(new ChildScorer(subQueryScorer, "CUSTOM"));
    }
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
    return new CustomWeight(searcher, needsScores);
  }

  /**
   * Checks if this is strict custom scoring.
   * In strict custom scoring, the {@link ValueSource} part does not participate in weight normalization.
   * This may be useful when one wants full control over how scores are modified, and does 
   * not care about normalizing by the {@link ValueSource} part.
   * One particular case where this is useful if for testing this query.   
   * <P>
   * Note: only has effect when the {@link ValueSource} part is not null.
   */
  public boolean isStrict() {
    return strict;
  }

  /**
   * Set the strict mode of this query. 
   * @param strict The strict mode to set.
   * @see #isStrict()
   */
  public void setStrict(boolean strict) {
    this.strict = strict;
  }

  /** The sub-query that CustomScoreQuery wraps, affecting both the score and which documents match. */
  public Query getSubQuery() {
    return subQuery;
  }

  /** The scoring queries that only affect the score of CustomScoreQuery. */
  public Query[] getScoringQueries() {
    return scoringQueries;
  }

  /**
   * A short name of this query, used in {@link #toString(String)}.
   */
  public String name() {
    return "custom";
  }

}
