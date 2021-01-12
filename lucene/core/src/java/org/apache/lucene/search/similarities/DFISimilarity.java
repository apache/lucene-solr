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

import org.apache.lucene.search.Explanation;

/**
 * Implements the <em>Divergence from Independence (DFI)</em> model based on Chi-square statistics
 * (i.e., standardized Chi-squared distance from independence in term frequency tf).
 *
 * <p>DFI is both parameter-free and non-parametric:
 *
 * <ul>
 *   <li>parameter-free: it does not require any parameter tuning or training.
 *   <li>non-parametric: it does not make any assumptions about word frequency distributions on
 *       document collections.
 * </ul>
 *
 * <p>It is highly recommended <b>not</b> to remove stopwords (very common terms: the, of, and, to,
 * a, in, for, is, on, that, etc) with this similarity.
 *
 * <p>For more information see: <a href="http://dx.doi.org/10.1007/s10791-013-9225-4">A
 * nonparametric term weighting method for information retrieval based on measuring the divergence
 * from independence</a>
 *
 * @lucene.experimental
 * @see org.apache.lucene.search.similarities.IndependenceStandardized
 * @see org.apache.lucene.search.similarities.IndependenceSaturated
 * @see org.apache.lucene.search.similarities.IndependenceChiSquared
 */
public class DFISimilarity extends SimilarityBase {
  private final Independence independence;

  /**
   * Create DFI with the specified divergence from independence measure
   *
   * @param independenceMeasure measure of divergence from independence
   */
  public DFISimilarity(Independence independenceMeasure) {
    this.independence = independenceMeasure;
  }

  @Override
  protected double score(BasicStats stats, double freq, double docLen) {

    final double expected =
        (stats.getTotalTermFreq() + 1) * docLen / (stats.getNumberOfFieldTokens() + 1);

    // if the observed frequency is less than or equal to the expected value, then return zero.
    if (freq <= expected) return 0;

    final double measure = independence.score(freq, expected);

    return stats.getBoost() * log2(measure + 1);
  }

  /** Returns the measure of independence */
  public Independence getIndependence() {
    return independence;
  }

  @Override
  protected Explanation explain(BasicStats stats, Explanation freq, double docLen) {
    final double expected =
        (stats.getTotalTermFreq() + 1) * docLen / (stats.getNumberOfFieldTokens() + 1);
    if (freq.getValue().doubleValue() <= expected) {
      return Explanation.match(
          (float) 0,
          "score(" + getClass().getSimpleName() + ", freq=" + freq.getValue() + "), equals to 0");
    }
    Explanation explExpected =
        Explanation.match(
            (float) expected,
            "expected, computed as (F + 1) * dl / (T + 1) from:",
            Explanation.match(
                stats.getTotalTermFreq(), "F, total number of occurrences of term across all docs"),
            Explanation.match((float) docLen, "dl, length of field"),
            Explanation.match(
                stats.getNumberOfFieldTokens(), "T, total number of tokens in the field"));

    final double measure = independence.score(freq.getValue().doubleValue(), expected);
    Explanation explMeasure =
        Explanation.match(
            (float) measure,
            "measure, computed as independence.score(freq, expected) from:",
            freq,
            explExpected);

    return Explanation.match(
        (float) score(stats, freq.getValue().doubleValue(), docLen),
        "score("
            + getClass().getSimpleName()
            + ", freq="
            + freq.getValue()
            + "), computed as boost * log2(measure + 1) from:",
        Explanation.match((float) stats.getBoost(), "boost, query boost"),
        explMeasure);
  }

  @Override
  public String toString() {
    return "DFI(" + independence + ")";
  }
}
