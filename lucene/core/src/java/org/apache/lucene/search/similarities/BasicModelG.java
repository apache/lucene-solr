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

import static org.apache.lucene.search.similarities.SimilarityBase.log2;

import org.apache.lucene.search.Explanation;

/**
 * Geometric as limiting form of the Bose-Einstein model. The formula used in Lucene differs
 * slightly from the one in the original paper: {@code F} is increased by {@code 1} and {@code N} is
 * increased by {@code F}.
 *
 * @lucene.experimental
 */
public class BasicModelG extends BasicModel {

  /** Sole constructor: parameter-free */
  public BasicModelG() {}

  @Override
  public final double score(BasicStats stats, double tfn, double aeTimes1pTfn) {
    // just like in BE, approximation only holds true when F << N, so we use lambda = F / (N + F)
    double F = stats.getTotalTermFreq() + 1;
    double N = stats.getNumberOfDocuments();
    double lambda = F / (N + F);
    // -log(1 / (lambda + 1)) -> log(lambda + 1)
    double A = log2(lambda + 1);
    double B = log2((1 + lambda) / lambda);

    // basic model G should return (A + B * tfn)
    // which we rewrite to B * (1 + tfn) - (B - A)
    // so that it can be combined with the after effect while still guaranteeing
    // that the result is non-decreasing with tfn since B >= A

    return (B - (B - A) / (1 + tfn)) * aeTimes1pTfn;
  }

  @Override
  public Explanation explain(BasicStats stats, double tfn, double aeTimes1pTfn) {
    double F = stats.getTotalTermFreq() + 1;
    double N = stats.getNumberOfDocuments();
    double lambda = F / (N + F);
    Explanation explLambda =
        Explanation.match(
            (float) lambda,
            "lambda, computed as F / (N + F) from:",
            Explanation.match(
                (float) F, "F, total number of occurrences of term across all docs + 1"),
            Explanation.match((float) N, "N, total number of documents with field"));

    return Explanation.match(
        (float) (score(stats, tfn, aeTimes1pTfn) * (1 + tfn) / aeTimes1pTfn),
        getClass().getSimpleName()
            + ", computed as "
            + "log2(lambda + 1) + tfn * log2((1 + lambda) / lambda) from:",
        Explanation.match((float) tfn, "tfn, normalized term frequency"),
        explLambda);
  }

  @Override
  public String toString() {
    return "G";
  }
}
