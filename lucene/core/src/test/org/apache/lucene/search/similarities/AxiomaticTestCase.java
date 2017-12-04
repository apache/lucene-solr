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

import java.util.Random;

public abstract class AxiomaticTestCase extends BaseSimilarityTestCase {
  
  @Override
  protected final Similarity getSimilarity(Random random) {
    // axiomatic parameter s
    final float s;
    switch (random.nextInt(4)) {
      case 0:
        // minimum value
        s = 0;
        break;
      case 1:
        // tiny value
        s = Float.MIN_VALUE;
        break;
      case 2:
        // maximum value
        s = 1;
        break;
      default:
        // random value
        s = random.nextFloat();
        break;
    }
    // axiomatic query length
    final int queryLen;
    switch (random.nextInt(4)) {
      case 0:
        // minimum value
        queryLen = 0;
        break;
      case 1:
        // tiny value
        queryLen = 1;
        break;
      case 2:
        // maximum value
        queryLen = Integer.MAX_VALUE;
        break;
      default:
        // random value
        queryLen = random.nextInt(Integer.MAX_VALUE);
        break;
    }
    // axiomatic parameter k
    final float k;
    switch (random.nextInt(4)) {
      case 0:
        // minimum value
        k = 0;
        break;
      case 1:
        // tiny value
        k = Float.MIN_VALUE;
        break;
      case 2:
        // maximum value
        k = 1;
        break;
      default:
        // random value
        k = random.nextFloat();
        break;
    }
    
    return getAxiomaticModel(s, queryLen, k);
  }
  
  protected abstract Similarity getAxiomaticModel(float s, int queryLen, float k);
}
