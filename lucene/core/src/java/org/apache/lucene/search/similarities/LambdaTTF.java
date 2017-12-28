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
 * Computes lambda as {@code totalTermFreq+1 / numberOfDocuments+1}.
 * @lucene.experimental
 */
public class LambdaTTF extends Lambda {  
  
  /** Sole constructor: parameter-free */
  public LambdaTTF() {}

  @Override
  public final float lambda(BasicStats stats) {
    float lambda = (float) ((stats.getTotalTermFreq() + 1.0) / (stats.getNumberOfDocuments() + 1.0));
    if (lambda == 1) {
      // Distribution SPL cannot work with values of lambda that are equal to 1
      lambda = Math.nextUp(lambda);
    }
    return lambda;
  }

  public final Explanation explain(BasicStats stats) {
    return Explanation.match(
        lambda(stats),
        getClass().getSimpleName()
            + ", computed as (F + 1) / (N + 1) from:",
        Explanation.match(stats.getTotalTermFreq(),
            "F, total number of occurrences of term across all documents"),
        Explanation.match(stats.getNumberOfDocuments(),
            "N, total number of documents with field"));
  }

  @Override
  public String toString() {
    return "L";
  }
}
