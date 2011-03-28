package org.apache.lucene.search;

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

/**
 * SimilarityProvider for {@link MockLMSimilarity}
 * <ul>
 *   <li> disables coord, because its already factored into the formula
 *   <li> disables queryNorm, because we (currently) shove part of the formula in there as "idf"
 * </ul>
 */
public class MockLMSimilarityProvider implements SimilarityProvider {
  private static final Similarity impl = new MockLMSimilarity();
  
  public float coord(int overlap, int maxOverlap) {
    return 1f;
  }

  public float queryNorm(float sumOfSquaredWeights) {
    return 1f;
  }

  public Similarity get(String field) {
    return impl;
  }
}
