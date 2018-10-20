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
package org.apache.lucene.search.similarities;


import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.search.Explanation;

/**
 * Axiomatic approaches for IR. From Hui Fang and Chengxiang Zhai
 * 2005. An Exploration of Axiomatic Approaches to Information Retrieval.
 * In Proceedings of the 28th annual international ACM SIGIR
 * conference on Research and development in information retrieval
 * (SIGIR '05). ACM, New York, NY, USA, 480-487.
 * <p>
 * There are a family of models. All of them are based on BM25,
 * Pivoted Document Length Normalization and Language model with
 * Dirichlet prior. Some components (e.g. Term Frequency,
 * Inverted Document Frequency) in the original models are modified
 * so that they follow some axiomatic constraints.
 * </p>
 *
 * @lucene.experimental
 */
public abstract class Axiomatic extends SimilarityBase {
  /**
   * hyperparam for the growth function
   */
  protected final float s;

  /**
   * hyperparam for the primitive weighthing function
   */
  protected final float k;

  /**
   * the query length
   */
  protected final int queryLen;

  /**
   * Constructor setting all Axiomatic hyperparameters
   * @param s hyperparam for the growth function
   * @param queryLen the query length
   * @param k hyperparam for the primitive weighting function
   */
  public Axiomatic(float s, int queryLen, float k) {
    if (Float.isFinite(s) == false || Float.isNaN(s) || s < 0 || s > 1) {
      throw new IllegalArgumentException("illegal s value: " + s + ", must be between 0 and 1");
    }
    if (Float.isFinite(k) == false || Float.isNaN(k) || k < 0 || k > 1) {
      throw new IllegalArgumentException("illegal k value: " + k + ", must be between 0 and 1");
    }
    if (queryLen < 0 || queryLen > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("illegal query length value: "
          + queryLen + ", must be larger 0 and smaller than MAX_INT");
    }
    this.s = s;
    this.queryLen = queryLen;
    this.k = k;
  }

  /**
   * Constructor setting only s, letting k and queryLen to default
   * @param s hyperparam for the growth function
   */
  public Axiomatic(float s) {
    this(s, 1, 0.35f);
  }

  /**
   * Constructor setting s and queryLen, letting k to default
   * @param s hyperparam for the growth function
   * @param queryLen the query length
   */
  public Axiomatic(float s, int queryLen) {
    this(s, queryLen, 0.35f);
  }

  /**
   * Default constructor
   */
  public Axiomatic() {
    this(0.25f, 1, 0.35f);
  }

  @Override
  public double score(BasicStats stats, double freq, double docLen) {
    double score = tf(stats, freq, docLen)
        * ln(stats, freq, docLen)
        * tfln(stats, freq, docLen)
        * idf(stats, freq, docLen)
        - gamma(stats, freq, docLen);
    score *= stats.boost;
    // AxiomaticF3 similarities might produce negative scores due to their gamma component
    return Math.max(0, score);
  }

  @Override
  protected Explanation explain(
      BasicStats stats, Explanation freq, double docLen) {    
    List<Explanation> subs = new ArrayList<>();
    double f = freq.getValue().doubleValue();
    explain(subs, stats, f, docLen);
    
    double score = tf(stats, f, docLen)
        * ln(stats, f, docLen)
        * tfln(stats, f, docLen)
        * idf(stats, f, docLen)
        - gamma(stats, f, docLen);

    Explanation explanation = Explanation.match((float) score,
        "score(" + getClass().getSimpleName() + ", freq=" + freq.getValue() +"), computed from:",
        subs);
    if (stats.boost != 1f) {
      explanation = Explanation.match((float) (score * stats.boost), "Boosted score, computed as (score * boost) from:",
          explanation,
          Explanation.match((float) stats.boost, "Query boost"));
    }
    if (score < 0) {
      explanation = Explanation.match(0, "max of:",
          Explanation.match(0, "Minimum legal score"),
          explanation);
    }
    return explanation;
  }

  @Override
  protected void explain(List<Explanation> subs, BasicStats stats,
                         double freq, double docLen) {
    if (stats.getBoost() != 1.0d) {
      subs.add(Explanation.match((float) stats.getBoost(),
          "boost, query boost"));
    }

    subs.add(Explanation.match(this.k,
        "k, hyperparam for the primitive weighting function"));
    subs.add(Explanation.match(this.s,
        "s, hyperparam for the growth function"));
    subs.add(Explanation.match(this.queryLen, "queryLen, query length"));
    subs.add(tfExplain(stats, freq, docLen));
    subs.add(lnExplain(stats, freq, docLen));
    subs.add(tflnExplain(stats, freq, docLen));
    subs.add(idfExplain(stats, freq, docLen));
    subs.add(Explanation.match((float) gamma(stats, freq, docLen), "gamma"));
    super.explain(subs, stats, freq, docLen);
  }

  /**
   * Name of the axiomatic method.
   */
  @Override
  public abstract String toString();

  /**
   * compute the term frequency component
   */
  protected abstract double tf(BasicStats stats, double freq, double docLen);

  /**
   * compute the document length component
   */
  protected abstract double ln(BasicStats stats, double freq, double docLen);

  /**
   * compute the mixed term frequency and document length component
   */
  protected abstract double tfln(BasicStats stats, double freq, double docLen);

  /**
   * compute the inverted document frequency component
   */
  protected abstract double idf(BasicStats stats, double freq, double docLen);

  /**
   * compute the gamma component (only for F3EXp and F3LOG)
   */
  protected abstract double gamma(BasicStats stats, double freq, double docLen);


  /**
   * Explain the score of the term frequency component for a single document
   * @param stats the corpus level statistics
   * @param freq number of occurrences of term in the document
   * @param docLen the document length
   * @return Explanation of how the tf component was computed
   */
  protected abstract Explanation tfExplain(BasicStats stats,
                                           double freq, double docLen);

  /**
   * Explain the score of the document length component for a single document
   * @param stats the corpus level statistics
   * @param freq number of occurrences of term in the document
   * @param docLen the document length
   * @return Explanation of how the ln component was computed
   */
  protected abstract Explanation lnExplain(BasicStats stats,
                                           double freq, double docLen);

  /**
   * Explain the score of the mixed term frequency and
   * document length component for a single document
   * @param stats the corpus level statistics
   * @param freq number of occurrences of term in the document
   * @param docLen the document length
   * @return Explanation of how the tfln component was computed
   */
  protected abstract Explanation tflnExplain(BasicStats stats,
                                             double freq, double docLen);

  /**
   * Explain the score of the inverted document frequency component
   * for a single document
   * @param stats the corpus level statistics
   * @param freq number of occurrences of term in the document
   * @param docLen the document length
   * @return Explanation of how the idf component was computed
   */
  protected abstract Explanation idfExplain(BasicStats stats, double freq, double docLen);

}