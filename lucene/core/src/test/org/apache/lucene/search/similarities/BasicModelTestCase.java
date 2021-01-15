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

public abstract class BasicModelTestCase extends BaseSimilarityTestCase {

  @Override
  protected final Similarity getSimilarity(Random random) {
    final AfterEffect afterEffect;
    switch (random.nextInt(2)) {
      case 0:
        afterEffect = new AfterEffectL();
        break;
      default:
        afterEffect = new AfterEffectB();
        break;
    }
    // normalization hyper-parameter c
    final float c;
    switch (random.nextInt(4)) {
      case 0:
        // minimum value
        c = 0;
        break;
      case 1:
        // tiny value
        c = Float.MIN_VALUE;
        break;
      case 2:
        // maximum value
        // we just limit the test to "reasonable" c values but don't enforce this anywhere.
        c = Integer.MAX_VALUE;
        break;
      default:
        // random value
        c = Integer.MAX_VALUE * random.nextFloat();
        break;
    }
    // normalization hyper-parameter z
    final float z;
    switch (random.nextInt(3)) {
      case 0:
        // minimum value
        z = Float.MIN_VALUE;
        break;
      case 1:
        // maximum value
        z = Math.nextDown(0.5f);
        break;
      default:
        // random value
        float zcand = random.nextFloat() / 2;
        if (zcand == 0f) {
          // nextFloat returns 0 inclusive, we have to avoid it.
          z = Math.nextUp(zcand);
        } else {
          z = zcand;
        }
    }
    // dirichlet parameter mu
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
        // we just limit the test to "reasonable" mu values but don't enforce this anywhere.
        mu = Integer.MAX_VALUE;
        break;
      default:
        // random value
        mu = Integer.MAX_VALUE * random.nextFloat();
        break;
    }
    final Normalization normalization;
    switch (random.nextInt(5)) {
      case 0:
        normalization = new Normalization.NoNormalization();
        break;
      case 1:
        normalization = new NormalizationH1(c);
        break;
      case 2:
        normalization = new NormalizationH2(c);
        break;
      case 3:
        normalization = new NormalizationH3(mu);
        break;
      default:
        normalization = new NormalizationZ(z);
        break;
    }
    return new DFRSimilarity(getBasicModel(), afterEffect, normalization);
  }

  /** return BasicModel under test */
  protected abstract BasicModel getBasicModel();
}
