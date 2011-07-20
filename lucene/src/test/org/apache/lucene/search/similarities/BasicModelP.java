package org.apache.lucene.search.similarities;

/**
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

import static org.apache.lucene.search.similarities.EasySimilarity.log2;

/**
 * Implements the Poisson approximation for the binomial model for DFR.
 * @lucene.experimental
 */
public class BasicModelP extends BasicModel {
  @Override
  public final float score(EasyStats stats, float tfn) {
    float lambda = (float)stats.getTotalTermFreq() / stats.getNumberOfDocuments();
    return (float)(tfn * log2(tfn / lambda)
        + (lambda + 1 / 12 / tfn - tfn) * log2(Math.E)
        + 0.5 * log2(2 * Math.PI * tfn));
  }
}
