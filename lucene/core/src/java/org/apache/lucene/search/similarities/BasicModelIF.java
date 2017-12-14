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

import static org.apache.lucene.search.similarities.SimilarityBase.log2;

/**
 * An approximation of the <em>I(n<sub>e</sub>)</em> model.
 * @lucene.experimental
 */ 
public class BasicModelIF extends BasicModel {
  
  /** Sole constructor: parameter-free */
  public BasicModelIF() {}

  @Override
  public final double score(BasicStats stats, double tfn, double aeTimes1pTfn) {
    long N = stats.getNumberOfDocuments();
    long F = stats.getTotalTermFreq();
    double A = log2(1 + (N + 1) / (F + 0.5));
    
    // basic model IF should return A * tfn
    // which we rewrite to A * (1 + tfn) - A
    // so that it can be combined with the after effect while still guaranteeing
    // that the result is non-decreasing with tfn
    
    return A * aeTimes1pTfn * (1 - 1 / (1 + tfn));
  }

  @Override
    public Explanation explain(BasicStats stats, double tfn, double aeTimes1pTfn) {
    return Explanation.match(
        (float) (score(stats, tfn, aeTimes1pTfn) * (1 + tfn) / aeTimes1pTfn),
        getClass().getSimpleName() + ", computed as " +
            "tfn * log2(1 + (N + 1) / (F + 0.5)) from:",
        Explanation.match((float) tfn, "tfn, normalized term frequency"),
        Explanation.match(stats.getNumberOfDocuments(),
            "N, total number of documents with field"),
        Explanation.match(stats.getTotalTermFreq(),
            "F, total number of occurrences of term across all documents"));
  }

  @Override
  public String toString() {
    return "I(F)";
  }
}
