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

import org.apache.lucene.luwak.termextractor.QueryTerm;

/**
 * A combination of {@link WeightNorm} classes that calculates the weight
 * of a {@link QueryTerm}
 * <p>
 * Individual {@link WeightNorm} results are combined by multiplication.
 */
public final class TermWeightor {

  private final WeightNorm[] norms;

  /**
   * Create a new TermWeightor from a combination of WeightNorms
   * <p>
   * Note that passing an empty list will result in all terms being given
   * a weight of {@code 1f}
   */
  public TermWeightor(WeightNorm... norms) {
    this.norms = norms;
  }

  /**
   * Calculates the weight of a specific term
   */
  public float weigh(QueryTerm term) {
    float termweight = 1;
    for (WeightNorm norm : norms) {
      termweight *= norm.norm(term);
    }
    return termweight;
  }

}
