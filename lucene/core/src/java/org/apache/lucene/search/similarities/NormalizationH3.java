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
 * Dirichlet Priors normalization
 *
 * @lucene.experimental
 */
public class NormalizationH3 extends Normalization {
  private final float mu;

  /** Calls {@link #NormalizationH3(float) NormalizationH3(800)} */
  public NormalizationH3() {
    this(800F);
  }

  /**
   * Creates NormalizationH3 with the supplied parameter <code>&mu;</code>.
   *
   * @param mu smoothing parameter <code>&mu;</code>
   */
  public NormalizationH3(float mu) {
    if (Float.isFinite(mu) == false || mu < 0) {
      throw new IllegalArgumentException(
          "illegal mu value: " + mu + ", must be a non-negative finite value");
    }
    this.mu = mu;
  }

  @Override
  public double tfn(BasicStats stats, double tf, double len) {
    return (tf + mu * ((stats.getTotalTermFreq() + 1F) / (stats.getNumberOfFieldTokens() + 1F)))
        / (len + mu)
        * mu;
  }

  @Override
  public Explanation explain(BasicStats stats, double tf, double len) {
    return Explanation.match(
        (float) tfn(stats, tf, len),
        getClass().getSimpleName()
            + ", computed as (tf + mu * ((F+1) / (T+1))) / (fl + mu) * mu from:",
        Explanation.match((float) tf, "tf, number of occurrences of term in the document"),
        Explanation.match(mu, "mu, smoothing parameter"),
        Explanation.match(
            (float) stats.getTotalTermFreq(),
            "F,  total number of occurrences of term across all documents"),
        Explanation.match(
            (float) stats.getNumberOfFieldTokens(),
            "T, total number of tokens of the field across all documents"),
        Explanation.match((float) len, "fl, field length of the document"));
  }

  @Override
  public String toString() {
    return "3(" + mu + ")";
  }

  /**
   * Returns the parameter <code>&mu;</code>
   *
   * @see #NormalizationH3(float)
   */
  public float getMu() {
    return mu;
  }
}
