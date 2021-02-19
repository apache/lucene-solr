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

import java.util.List;
import java.util.Locale;
import org.apache.lucene.search.Explanation;

/**
 * Bayesian smoothing using Dirichlet priors as implemented in the Indri Search engine
 * (http://www.lemurproject.org/indri.php). Indri Dirichelet Smoothing!
 *
 * <pre class="prettyprint">
 * tf_E + mu*P(t|D) P(t|E)= documentLength + documentMu
 * mu*P(t|C) + tf_D where P(t|D)= doclen + mu
 * </pre>
 *
 * <p>A larger value for mu, produces more smoothing. Smoothing is most important for short
 * documents where the probabilities are more granular.
 */
public class IndriDirichletSimilarity extends LMSimilarity {

  /** The &mu; parameter. */
  private final float mu;

  /** Instantiates the similarity with the provided &mu; parameter. */
  public IndriDirichletSimilarity(CollectionModel collectionModel, float mu) {
    super(collectionModel);
    this.mu = mu;
  }

  /** Instantiates the similarity with the provided &mu; parameter. */
  public IndriDirichletSimilarity(float mu) {
    this.mu = mu;
  }

  /** Instantiates the similarity with the default &mu; value of 2000. */
  public IndriDirichletSimilarity(CollectionModel collectionModel) {
    this(collectionModel, 2000);
  }

  /** Instantiates the similarity with the default &mu; value of 2000. */
  public IndriDirichletSimilarity() {
    this(new IndriCollectionModel(), 2000);
  }

  @Override
  protected double score(BasicStats stats, double freq, double docLen) {
    double collectionProbability = ((LMStats) stats).getCollectionProbability();
    double score = (freq + (mu * collectionProbability)) / (docLen + mu);
    return (Math.log(score));
  }

  @Override
  protected void explain(List<Explanation> subs, BasicStats stats, double freq, double docLen) {
    if (stats.getBoost() != 1.0f) {
      subs.add(Explanation.match(stats.getBoost(), "boost"));
    }

    subs.add(Explanation.match(mu, "mu"));
    double collectionProbability = ((LMStats) stats).getCollectionProbability();
    Explanation weightExpl =
        Explanation.match(
            (float) Math.log((freq + (mu * collectionProbability)) / (docLen + mu)), "term weight");
    subs.add(weightExpl);
    subs.add(Explanation.match((float) Math.log(mu / (docLen + mu)), "document norm"));
    super.explain(subs, stats, freq, docLen);
  }

  /** Returns the &mu; parameter. */
  public float getMu() {
    return mu;
  }

  public String getName() {
    return String.format(Locale.ROOT, "IndriDirichlet(%f)", getMu());
  }

  /**
   * Models {@code p(w|C)} as the number of occurrences of the term in the collection, divided by
   * the total number of tokens {@code + 1}.
   */
  public static class IndriCollectionModel implements CollectionModel {

    /** Sole constructor: parameter-free */
    public IndriCollectionModel() {}

    @Override
    public double computeProbability(BasicStats stats) {
      return ((double) stats.getTotalTermFreq()) / ((double) stats.getNumberOfFieldTokens());
    }

    @Override
    public String getName() {
      return null;
    }
  }
}
