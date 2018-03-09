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


/**
 * The smoothed power-law (SPL) distribution for the information-based framework
 * that is described in the original paper.
 * <p>Unlike for DFR, the natural logarithm is used, as
 * it is faster to compute and the original paper does not express any
 * preference to a specific base.</p>
 * WARNING: this model currently returns infinite scores for very small
 * tf values and negative scores for very large tf values
 * @lucene.experimental
 */
public class DistributionSPL extends Distribution {
  
  /** Sole constructor: parameter-free */
  public DistributionSPL() {}

  @Override
  public final double score(BasicStats stats, double tfn, double lambda) {
    assert lambda != 1;

    // tfn/(tfn+1) -> 1 - 1/(tfn+1), guaranteed to be non decreasing when tfn increases
    double q = 1 - 1 / (tfn + 1);
    if (q == 1) {
      q = Math.nextDown(1.0);
    }

    double pow = Math.pow(lambda, q);
    if (pow == lambda) {
      // this can happen because of floating-point rounding
      // but then we return infinity when taking the log, so we enforce
      // that pow is different from lambda
      if (lambda < 1) {
        // x^y > x when x < 1 and y < 1
        pow = Math.nextUp(lambda);
      } else {
        // x^y < x when x > 1 and y < 1
        pow = Math.nextDown(lambda);
      }
    }

    return -Math.log((pow - lambda) / (1 - lambda));
  }
  
  @Override
  public String toString() {
    return "SPL";
  }
}
