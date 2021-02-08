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

public class TestIndriDirichletSimilarity extends BaseSimilarityTestCase {

  @Override
  protected Similarity getSimilarity(Random random) {
    // smoothing parameter mu, unbounded
    final float mu;
    switch (random.nextInt(4)) {
      case 0:
        // minimum value
        mu = 0;
        break;
      case 1:
        // tiny value
        mu = Float.MIN_VALUE;
        break;
      case 2:
        // maximum value
        // we just limit the test to "reasonable" mu values but don't enforce
        // this anywhere.
        mu = Integer.MAX_VALUE;
        break;
      default:
        // random value
        mu = Integer.MAX_VALUE * random.nextFloat();
        break;
    }
    return new IndriDirichletSimilarity(mu);
  }
}
