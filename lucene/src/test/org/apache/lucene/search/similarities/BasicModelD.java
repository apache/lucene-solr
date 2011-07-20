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
 * Implements the approximation of the binomial model with the divergence
 * for DFR.
 * @lucene.experimental
 */
public class BasicModelD extends BasicModel {
  @Override
  public final float score(EasyStats stats, float tfn) {
    long F = stats.getTotalTermFreq();
    double phi = (double)tfn / F;
    double nphi = 1 - phi;
    double p = 1.0 / stats.getNumberOfDocuments();
    double D = phi * log2(phi / p) + nphi * log2(nphi / (1 - p));
    return (float)(D * F + 0.5 * log2(2 * Math.PI * tfn * nphi));
  }
}
