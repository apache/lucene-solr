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

package org.apache.lucene.luwak.termextractor.weights;

import java.util.Map;

import org.apache.lucene.luwak.termextractor.QueryTerm;

/**
 * Weights more infrequent terms more highly
 */
public class TermFrequencyWeightNorm extends WeightNorm {

  private final Map<String, Integer> frequencies;
  private final float n;
  private final float k;

  /**
   * Creates a TermFrequencyNorm
   *
   * @param frequencies map of terms to term frequencies
   * @param n           scaling factor to use for frequencies
   * @param k           minimum weight to scale to
   */
  public TermFrequencyWeightNorm(Map<String, Integer> frequencies, float n, float k) {
    this.frequencies = frequencies;
    this.n = n;
    this.k = k;
  }

  @Override
  public float norm(QueryTerm term) {
    Integer mapVal = this.frequencies.get(term.term.text());
    if (mapVal != null)
      return (n / mapVal) + k;
    return 1;
  }

}
